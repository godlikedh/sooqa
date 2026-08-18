use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{CommandError, DEFAULT_MAX_OUTPUT_BYTES, ExternalCommand, ExternalCommandRunner};

#[derive(Clone)]
pub struct FfprobeAdapter {
    executable: PathBuf,
    timeout: Duration,
    max_output_bytes: usize,
    runner: Arc<dyn ExternalCommandRunner>,
}

impl FfprobeAdapter {
    pub fn new(executable: impl Into<PathBuf>, timeout: Duration) -> Self {
        Self::with_runner(
            executable,
            timeout,
            DEFAULT_MAX_OUTPUT_BYTES,
            Arc::new(crate::ProcessCommandRunner),
        )
    }

    pub fn with_runner(
        executable: impl Into<PathBuf>,
        timeout: Duration,
        max_output_bytes: usize,
        runner: Arc<dyn ExternalCommandRunner>,
    ) -> Self {
        Self { executable: executable.into(), timeout, max_output_bytes, runner }
    }

    pub async fn probe(&self, input: impl AsRef<Path>) -> Result<MediaProbe, ProbeError> {
        let input = input.as_ref();
        let metadata = tokio::fs::metadata(input)
            .await
            .map_err(|source| ProbeError::InputFile { path: input.to_owned(), source })?;
        if !metadata.is_file() {
            return Err(ProbeError::InputNotFile { path: input.to_owned() });
        }

        let command = ExternalCommand::new(self.executable.clone())
            .arg("-v")
            .arg("error")
            .arg("-show_format")
            .arg("-show_streams")
            .arg("-of")
            .arg("json")
            .arg(input.as_os_str())
            .timeout(self.timeout)
            .max_output_bytes(self.max_output_bytes);
        let output = self.runner.run(command).await.map_err(ProbeError::Command)?;
        if output.stdout_truncated || output.stderr_truncated {
            return Err(ProbeError::OutputLimitExceeded { limit: self.max_output_bytes });
        }
        if !output.success {
            return Err(ProbeError::ProcessFailed {
                exit_code: output.exit_code,
                stderr: bounded_text(&output.stderr),
            });
        }

        let mut probe = parse_probe_json(&output.stdout, metadata.len())?;
        if probe.streams.iter().any(needs_targeted_avcc_probe) {
            // Debian Bookworm's ffprobe does not expose mime_codec_string.
            // Keep the regular probe bounded and obtain only the first video
            // stream's avcC declaration in a separate bounded command. Any
            // failure of this optional metadata probe is fail-closed: the
            // stream remains non-remuxable and normalization will transcode.
            if let Some(output) = self.targeted_avcc_probe(input).await {
                merge_targeted_avcc(&mut probe, &output);
            }
        }
        Ok(probe)
    }

    async fn targeted_avcc_probe(&self, input: &Path) -> Option<Vec<u8>> {
        let command = ExternalCommand::new(self.executable.clone())
            .arg("-v")
            .arg("error")
            .arg("-select_streams")
            .arg("v:0")
            .arg("-show_entries")
            .arg("stream=index,extradata")
            .arg("-show_data")
            .arg("-of")
            .arg("json")
            .arg(input.as_os_str())
            .timeout(self.timeout)
            .max_output_bytes(self.max_output_bytes);
        let output = self.runner.run(command).await.ok()?;
        if !output.success || output.stdout_truncated || output.stderr_truncated {
            return None;
        }
        Some(output.stdout)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct MediaProbe {
    pub container_format: Option<String>,
    pub duration_ms: Option<u64>,
    pub size_bytes: u64,
    pub bit_rate: Option<u64>,
    pub streams: Vec<MediaStream>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct MediaStream {
    pub index: u32,
    pub kind: MediaStreamKind,
    pub codec: Option<String>,
    /// Container codec tag, such as `avc1` or `avc3` for MP4/H.264.
    #[serde(default)]
    pub codec_tag: Option<String>,
    /// The codec MIME string contains the MP4 avcC declaration, for example
    /// `avc1.640028`; it is kept separate from `level`, which is authoritative
    /// SPS metadata.
    #[serde(default)]
    pub codec_mime: Option<String>,
    /// H.264 level reported from the stream's SPS.
    #[serde(default)]
    pub level: Option<u32>,
    pub profile: Option<String>,
    pub pixel_format: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub display_aspect_ratio: Option<String>,
    pub frame_rate: Option<FrameRate>,
    pub rotation_degrees: Option<i32>,
    pub sample_rate_hz: Option<u32>,
    pub channels: Option<u16>,
    pub bit_rate: Option<u64>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaStreamKind {
    Video,
    Audio,
    Subtitle,
    Data,
    Attachment,
    Other(String),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub struct FrameRate {
    pub numerator: u64,
    pub denominator: u64,
}

pub fn parse_probe_json(input: &[u8], file_size_bytes: u64) -> Result<MediaProbe, ProbeError> {
    let raw: RawProbe = serde_json::from_slice(input)
        .map_err(|source| ProbeError::InvalidJson { message: source.to_string() })?;
    let format = raw.format.ok_or_else(|| ProbeError::InvalidField {
        field: "format".to_owned(),
        message: "ffprobe output did not contain a format object".to_owned(),
    })?;

    let streams = raw.streams.into_iter().map(parse_stream).collect::<Result<Vec<_>, _>>()?;

    Ok(MediaProbe {
        container_format: optional_value(format.format_name),
        duration_ms: parse_duration(format.duration.as_deref(), "format.duration")?,
        size_bytes: parse_optional_u64(format.size.as_deref(), "format.size")?
            .unwrap_or(file_size_bytes),
        bit_rate: parse_optional_u64(format.bit_rate.as_deref(), "format.bit_rate")?,
        streams,
    })
}

#[derive(Debug, Error)]
pub enum ProbeError {
    #[error("could not read media input {path}: {source}")]
    InputFile { path: PathBuf, source: std::io::Error },
    #[error("media input is not a regular file: {path}")]
    InputNotFile { path: PathBuf },
    #[error("ffprobe command failed: {0}")]
    Command(CommandError),
    #[error("ffprobe output exceeded the {limit}-byte capture limit")]
    OutputLimitExceeded { limit: usize },
    #[error("ffprobe exited unsuccessfully with status {exit_code:?}: {stderr}")]
    ProcessFailed { exit_code: Option<i32>, stderr: String },
    #[error("ffprobe returned invalid JSON: {message}")]
    InvalidJson { message: String },
    #[error("ffprobe returned an invalid field {field}: {message}")]
    InvalidField { field: String, message: String },
}

impl ProbeError {
    pub fn class(&self) -> &'static str {
        match self {
            Self::InputFile { .. } | Self::InputNotFile { .. } => "probe_input",
            Self::Command(error) if error.is_timeout() => "probe_timeout",
            Self::Command(_) => "probe_command",
            Self::OutputLimitExceeded { .. } => "probe_output_limit",
            Self::ProcessFailed { .. } => "probe_process",
            Self::InvalidJson { .. } | Self::InvalidField { .. } => "probe_invalid_output",
        }
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Command(error) if error.is_timeout())
    }
}

#[derive(Debug, Deserialize)]
struct RawProbe {
    #[serde(default)]
    streams: Vec<RawStream>,
    format: Option<RawFormat>,
}

#[derive(Debug, Deserialize)]
struct RawFormat {
    format_name: Option<String>,
    duration: Option<String>,
    size: Option<String>,
    bit_rate: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawStream {
    index: Option<u32>,
    codec_type: Option<String>,
    codec_name: Option<String>,
    codec_tag_string: Option<String>,
    mime_codec_string: Option<String>,
    extradata: Option<String>,
    // Some ffprobe builds report an unknown H.264 level as -99. Keep the raw
    // JSON value permissive so that insufficient metadata falls back to a
    // transcode instead of making the whole probe unreadable.
    level: Option<serde_json::Value>,
    profile: Option<String>,
    pix_fmt: Option<String>,
    width: Option<u32>,
    height: Option<u32>,
    display_aspect_ratio: Option<String>,
    avg_frame_rate: Option<String>,
    r_frame_rate: Option<String>,
    tags: Option<HashMap<String, String>>,
    side_data_list: Option<Vec<RawSideData>>,
    sample_rate: Option<String>,
    channels: Option<u16>,
    bit_rate: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawAvccProbe {
    #[serde(default)]
    streams: Vec<RawAvccStream>,
}

#[derive(Debug, Deserialize)]
struct RawAvccStream {
    index: Option<u32>,
    extradata: Option<String>,
}

fn needs_targeted_avcc_probe(stream: &MediaStream) -> bool {
    stream.kind == MediaStreamKind::Video
        && stream.codec_mime.is_none()
        && stream.codec.as_deref().is_some_and(|codec| codec.eq_ignore_ascii_case("h264"))
        && stream
            .codec_tag
            .as_deref()
            .is_some_and(|tag| tag.eq_ignore_ascii_case("avc1") || tag.eq_ignore_ascii_case("avc3"))
}

fn merge_targeted_avcc(probe: &mut MediaProbe, input: &[u8]) {
    let Ok(raw) = serde_json::from_slice::<RawAvccProbe>(input) else { return };
    for stream in &mut probe.streams {
        if !needs_targeted_avcc_probe(stream) {
            continue;
        }
        let Some(extradata) = raw
            .streams
            .iter()
            .find(|candidate| candidate.index == Some(stream.index))
            .and_then(|candidate| candidate.extradata.as_deref())
        else {
            continue;
        };
        stream.codec_mime = codec_mime_from_metadata(
            stream.codec.as_deref(),
            stream.codec_tag.as_deref(),
            None,
            Some(extradata),
        );
    }
}

#[derive(Debug, Deserialize)]
struct RawSideData {
    rotation: Option<f64>,
}

fn parse_stream(stream: RawStream) -> Result<MediaStream, ProbeError> {
    let index = stream.index.ok_or_else(|| missing_field("stream.index"))?;
    let kind_value =
        stream.codec_type.as_deref().ok_or_else(|| missing_field("stream.codec_type"))?;
    let kind = match kind_value.to_ascii_lowercase().as_str() {
        "video" => MediaStreamKind::Video,
        "audio" => MediaStreamKind::Audio,
        "subtitle" => MediaStreamKind::Subtitle,
        "data" => MediaStreamKind::Data,
        "attachment" => MediaStreamKind::Attachment,
        other => MediaStreamKind::Other(other.to_owned()),
    };
    let frame_rate = stream
        .avg_frame_rate
        .as_deref()
        .filter(|value| *value != "0/0")
        .or(stream.r_frame_rate.as_deref().filter(|value| *value != "0/0"))
        .map(|value| parse_frame_rate(value, "stream.frame_rate"))
        .transpose()?;
    let rotation_degrees = parse_rotation(&stream);

    let codec = optional_value(stream.codec_name);
    let codec_tag = optional_value(stream.codec_tag_string);
    let codec_mime = codec_mime_from_metadata(
        codec.as_deref(),
        codec_tag.as_deref(),
        optional_value(stream.mime_codec_string),
        stream.extradata.as_deref(),
    );

    Ok(MediaStream {
        index,
        kind,
        codec,
        codec_tag,
        codec_mime,
        level: stream
            .level
            .and_then(|value| value.as_i64().and_then(|level| u32::try_from(level).ok())),
        profile: optional_value(stream.profile),
        pixel_format: optional_value(stream.pix_fmt),
        width: stream.width,
        height: stream.height,
        display_aspect_ratio: optional_value(stream.display_aspect_ratio),
        frame_rate,
        rotation_degrees,
        sample_rate_hz: parse_optional_u32(stream.sample_rate.as_deref(), "stream.sample_rate")?,
        channels: stream.channels,
        bit_rate: parse_optional_u64(stream.bit_rate.as_deref(), "stream.bit_rate")?,
    })
}

const MAX_AVCC_EXTRADATA_BYTES: usize = 4096;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct AvcCDeclaration {
    profile_idc: u8,
    profile_compatibility: u8,
    level_idc: u8,
}

/// ffprobe's `-show_data` stream dump is a bounded hexdump, not a raw hex
/// string. Decode only the contiguous four-hex-digit words before each line's
/// ASCII column and stop once the small avcC envelope has been collected.
fn parse_avcc_extradata(value: &str) -> Option<AvcCDeclaration> {
    let mut bytes = Vec::new();
    for line in value.lines() {
        let Some((_, payload)) = line.split_once(':') else { continue };
        for token in payload.split_whitespace() {
            if token.len() != 4 || !token.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                break;
            }
            let word = u16::from_str_radix(token, 16).ok()?;
            bytes.extend_from_slice(&word.to_be_bytes());
            if bytes.len() >= MAX_AVCC_EXTRADATA_BYTES {
                return parse_avcc_bytes(&bytes);
            }
        }
    }
    parse_avcc_bytes(&bytes)
}

fn parse_avcc_bytes(bytes: &[u8]) -> Option<AvcCDeclaration> {
    if bytes.len() < 8 || bytes[0] != 1 || bytes[4] & 0x03 != 3 {
        return None;
    }
    let declaration = AvcCDeclaration {
        profile_idc: bytes[1],
        profile_compatibility: bytes[2],
        level_idc: bytes[3],
    };
    let sps_count = usize::from(bytes[5] & 0x1f);
    if sps_count == 0 {
        return None;
    }
    let mut offset = 6;
    for _ in 0..sps_count {
        let length =
            usize::from(u16::from_be_bytes([*bytes.get(offset)?, *bytes.get(offset + 1)?]));
        offset = offset.checked_add(2)?.checked_add(length)?;
        if offset > bytes.len() {
            return None;
        }
    }
    let pps_count = usize::from(*bytes.get(offset)?);
    offset = offset.checked_add(1)?;
    if pps_count == 0 {
        return None;
    }
    for _ in 0..pps_count {
        let length =
            usize::from(u16::from_be_bytes([*bytes.get(offset)?, *bytes.get(offset + 1)?]));
        offset = offset.checked_add(2)?.checked_add(length)?;
        if offset > bytes.len() {
            return None;
        }
    }
    Some(declaration)
}

fn codec_mime_from_metadata(
    codec: Option<&str>,
    codec_tag: Option<&str>,
    reported_mime: Option<String>,
    extradata: Option<&str>,
) -> Option<String> {
    let avcc = extradata.and_then(parse_avcc_extradata);
    let is_h264_avc = codec.is_some_and(|value| value.eq_ignore_ascii_case("h264"))
        && codec_tag.is_some_and(|value| {
            value.eq_ignore_ascii_case("avc1") || value.eq_ignore_ascii_case("avc3")
        });

    // An avc1/avc3 stream with malformed avcC bytes must not inherit a
    // contradictory newer-field declaration and become remuxable.
    if is_h264_avc && extradata.is_some_and(|value| !value.trim().is_empty()) && avcc.is_none() {
        return None;
    }

    match (reported_mime, avcc) {
        (Some(reported), Some(avcc)) => {
            let suffix = reported.split_once('.')?.1;
            if suffix.len() != 6 || !suffix.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return None;
            }
            let profile = u8::from_str_radix(&suffix[..2], 16).ok()?;
            let compatibility = u8::from_str_radix(&suffix[2..4], 16).ok()?;
            let level = u8::from_str_radix(&suffix[4..], 16).ok()?;
            (profile == avcc.profile_idc
                && compatibility == avcc.profile_compatibility
                && level == avcc.level_idc)
                .then_some(reported)
        }
        (Some(reported), None) => Some(reported),
        (None, Some(avcc)) => {
            let codec_tag = codec_tag?;
            Some(format!(
                "{codec_tag}.{:02x}{:02x}{:02x}",
                avcc.profile_idc, avcc.profile_compatibility, avcc.level_idc
            ))
        }
        (None, None) => None,
    }
}

fn parse_rotation(stream: &RawStream) -> Option<i32> {
    let side_data_rotation = stream
        .side_data_list
        .as_deref()
        .and_then(|items| items.iter().find_map(|item| item.rotation));
    side_data_rotation
        .or_else(|| {
            stream
                .tags
                .as_ref()
                .and_then(|tags| tags.get("rotate").and_then(|value| value.parse().ok()))
        })
        .filter(|value: &f64| {
            value.is_finite() && *value >= i32::MIN as f64 && *value <= i32::MAX as f64
        })
        .map(|value| value.round() as i32)
}

fn parse_duration(value: Option<&str>, field: &str) -> Result<Option<u64>, ProbeError> {
    let Some(value) = usable_value(value) else {
        return Ok(None);
    };
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    let whole = whole.parse::<u64>().map_err(|_| invalid_number(field, value))?;
    if !fraction.chars().all(|character| character.is_ascii_digit()) {
        return Err(invalid_number(field, value));
    }
    let mut milliseconds = fraction.chars().take(3).collect::<String>();
    while milliseconds.len() < 3 {
        milliseconds.push('0');
    }
    let milliseconds = milliseconds.parse::<u64>().map_err(|_| invalid_number(field, value))?;
    whole
        .checked_mul(1000)
        .and_then(|value| value.checked_add(milliseconds))
        .map(Some)
        .ok_or_else(|| invalid_number(field, value))
}

fn parse_frame_rate(value: &str, field: &str) -> Result<FrameRate, ProbeError> {
    let (numerator, denominator) =
        value.split_once('/').ok_or_else(|| invalid_number(field, value))?;
    let numerator = numerator.parse::<u64>().map_err(|_| invalid_number(field, value))?;
    let denominator = denominator.parse::<u64>().map_err(|_| invalid_number(field, value))?;
    if denominator == 0 {
        return Err(invalid_number(field, value));
    }
    Ok(FrameRate { numerator, denominator })
}

fn parse_optional_u32(value: Option<&str>, field: &str) -> Result<Option<u32>, ProbeError> {
    usable_value(value)
        .map(|value| value.parse().map_err(|_| invalid_number(field, value)))
        .transpose()
}

fn parse_optional_u64(value: Option<&str>, field: &str) -> Result<Option<u64>, ProbeError> {
    usable_value(value)
        .map(|value| value.parse().map_err(|_| invalid_number(field, value)))
        .transpose()
}

fn usable_value(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty() && !value.eq_ignore_ascii_case("n/a"))
}

fn optional_value(value: Option<String>) -> Option<String> {
    value.and_then(|value| usable_value(Some(&value)).map(str::to_owned))
}

fn missing_field(field: &str) -> ProbeError {
    ProbeError::InvalidField { field: field.to_owned(), message: "field was missing".to_owned() }
}

fn invalid_number(field: &str, value: &str) -> ProbeError {
    ProbeError::InvalidField {
        field: field.to_owned(),
        message: format!("expected a non-negative number, got {value:?}"),
    }
}

fn bounded_text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).trim().to_owned()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        ffi::OsString,
        sync::{Arc, Mutex},
    };

    use async_trait::async_trait;

    use super::*;
    use crate::ExternalCommandOutput;

    const PROBE_JSON: &str = r#"
    {
      "streams": [
        {
          "index": 0,
          "codec_type": "video",
          "codec_name": "h264",
          "codec_tag_string": "avc1",
          "mime_codec_string": "avc1.640028",
          "level": 40,
          "profile": "High",
          "pix_fmt": "yuv420p",
          "width": 1920,
          "height": 1080,
          "display_aspect_ratio": "16:9",
          "avg_frame_rate": "30000/1001",
          "r_frame_rate": "30000/1001",
          "tags": {"rotate": "90"},
          "side_data_list": [{"rotation": 90.0}],
          "bit_rate": "500000"
        },
        {
          "index": 1,
          "codec_type": "audio",
          "codec_name": "aac",
          "sample_rate": "48000",
          "channels": 2,
          "bit_rate": "128000"
        }
      ],
      "format": {
        "format_name": "matroska,webm",
        "duration": "2.500000",
        "size": "123456",
        "bit_rate": "628000"
      }
    }
    "#;

    #[derive(Clone)]
    struct RecordingRunner {
        commands: Arc<Mutex<Vec<ExternalCommand>>>,
        outputs: Arc<Mutex<VecDeque<ExternalCommandOutput>>>,
    }

    #[async_trait]
    impl ExternalCommandRunner for RecordingRunner {
        async fn run(
            &self,
            command: ExternalCommand,
        ) -> Result<ExternalCommandOutput, CommandError> {
            self.commands.lock().expect("test mutex should not be poisoned").push(command);
            self.outputs.lock().expect("test mutex should not be poisoned").pop_front().ok_or_else(
                || CommandError::Spawn {
                    program: PathBuf::from("ffprobe"),
                    source: std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "recording runner ran out of outputs",
                    ),
                },
            )
        }
    }

    #[test]
    fn parses_project_owned_probe_model() {
        let probe = parse_probe_json(PROBE_JSON.as_bytes(), 999)
            .expect("fixture should parse into a media probe");

        assert_eq!(probe.container_format.as_deref(), Some("matroska,webm"));
        assert_eq!(probe.duration_ms, Some(2500));
        assert_eq!(probe.size_bytes, 123456);
        assert_eq!(probe.bit_rate, Some(628000));
        assert_eq!(probe.streams.len(), 2);
        assert_eq!(probe.streams[0].kind, MediaStreamKind::Video);
        assert_eq!(probe.streams[0].codec_tag.as_deref(), Some("avc1"));
        assert_eq!(probe.streams[0].codec_mime.as_deref(), Some("avc1.640028"));
        assert_eq!(probe.streams[0].level, Some(40));
        assert_eq!(probe.streams[0].rotation_degrees, Some(90));
        assert_eq!(
            probe.streams[0].frame_rate,
            Some(FrameRate { numerator: 30000, denominator: 1001 })
        );
        assert_eq!(probe.streams[1].sample_rate_hz, Some(48000));
        assert_eq!(probe.streams[1].channels, Some(2));
    }

    #[test]
    fn parses_bookworm_avcc_dump_when_mime_codec_string_is_absent() {
        let json = PROBE_JSON
            .replace("          \"mime_codec_string\": \"avc1.640028\",\n", "")
            .replace(
                "          \"level\": 40,",
                "          \"extradata\": \"\\n00000000: 0164 0028 ffe1 0002 6701 0100 0168  .d.(....gd...h\\n\",\n          \"level\": 40,",
            );
        let probe = parse_probe_json(json.as_bytes(), 999)
            .expect("Bookworm-style fixture should parse into a media probe");

        assert_eq!(probe.streams[0].codec_tag.as_deref(), Some("avc1"));
        assert_eq!(probe.streams[0].codec_mime.as_deref(), Some("avc1.640028"));
    }

    #[test]
    fn contradictory_reported_mime_and_avcc_dump_are_rejected() {
        let json = PROBE_JSON.replace(
            "          \"level\": 40,",
            "          \"extradata\": \"\\n00000000: 0164 0028 ffe1 0002 6701 0100 0168\\n\",\n          \"level\": 40,",
        );
        let json = json.replace("avc1.640028", "avc1.420028");
        let probe = parse_probe_json(json.as_bytes(), 999)
            .expect("contradictory metadata should remain parseable");

        assert_eq!(probe.streams[0].codec_mime, None);
    }

    #[test]
    fn malformed_avcc_dump_is_not_used_for_remux_metadata() {
        let json = PROBE_JSON.replace(
            "          \"level\": 40,",
            "          \"extradata\": \"\\n00000000: 0164 0028\\n\",\n          \"level\": 40,",
        );
        let probe = parse_probe_json(json.as_bytes(), 999)
            .expect("malformed metadata should remain parseable");

        assert_eq!(probe.streams[0].codec_mime, None);
    }

    #[test]
    fn uses_file_size_when_ffprobe_does_not_report_one() {
        let json = br#"{"streams":[],"format":{"format_name":"wav","duration":"N/A","size":"N/A","bit_rate":"N/A"}}"#;
        let probe = parse_probe_json(json, 17).expect("fixture should parse");
        assert_eq!(probe.size_bytes, 17);
        assert_eq!(probe.duration_ms, None);
        assert_eq!(probe.bit_rate, None);
    }

    #[test]
    fn unknown_negative_h264_level_remains_insufficient_metadata() {
        let json = PROBE_JSON.replace("\"level\": 40", "\"level\": -99");
        let probe =
            parse_probe_json(json.as_bytes(), 999).expect("fixture should remain parseable");
        assert_eq!(probe.streams[0].level, None);
    }

    #[tokio::test]
    async fn passes_safe_ffprobe_arguments_and_parses_output() {
        let commands = Arc::new(Mutex::new(Vec::new()));
        let runner = Arc::new(RecordingRunner {
            commands: Arc::clone(&commands),
            outputs: Arc::new(Mutex::new(VecDeque::from([ExternalCommandOutput {
                success: true,
                exit_code: Some(0),
                stdout: PROBE_JSON.as_bytes().to_vec(),
                stderr: Vec::new(),
                stdout_truncated: false,
                stderr_truncated: false,
            }]))),
        });
        let adapter = FfprobeAdapter::with_runner(
            "/usr/local/bin/ffprobe",
            Duration::from_secs(4),
            4096,
            runner,
        );
        let path = std::env::temp_dir().join(format!("sooqa-probe-{}.bin", uuid::Uuid::new_v4()));
        tokio::fs::write(&path, b"fixture").await.expect("fixture should be writable");

        let probe = adapter.probe(&path).await.expect("probe should succeed");
        assert_eq!(probe.size_bytes, 123456);
        let command = commands
            .lock()
            .expect("test mutex should not be poisoned")
            .first()
            .cloned()
            .expect("runner should record command");
        assert_eq!(command.program(), Path::new("/usr/local/bin/ffprobe"));
        assert_eq!(
            command.args()[..6],
            [
                OsString::from("-v"),
                OsString::from("error"),
                OsString::from("-show_format"),
                OsString::from("-show_streams"),
                OsString::from("-of"),
                OsString::from("json"),
            ]
        );
        assert!(!command.args().iter().any(|argument| argument == "-show_data"));
        assert_eq!(
            command.args().last().map(|argument| argument.as_os_str()),
            Some(path.as_os_str())
        );
        tokio::fs::remove_file(path).await.expect("fixture should be removed");
    }

    #[tokio::test]
    async fn targeted_bookworm_probe_is_bounded_and_fills_missing_codec_mime() {
        let commands = Arc::new(Mutex::new(Vec::new()));
        let base_json =
            PROBE_JSON.replace("          \"mime_codec_string\": \"avc1.640028\",\n", "");
        let runner = Arc::new(RecordingRunner {
            commands: Arc::clone(&commands),
            outputs: Arc::new(Mutex::new(VecDeque::from([
                ExternalCommandOutput {
                    success: true,
                    exit_code: Some(0),
                    stdout: base_json.into_bytes(),
                    stderr: Vec::new(),
                    stdout_truncated: false,
                    stderr_truncated: false,
                },
                ExternalCommandOutput {
                    success: true,
                    exit_code: Some(0),
                    stdout: br#"{"streams":[{"index":0,"extradata":"\n00000000: 0164 0028 ffe1 0002 6701 0100 0168  .d.(....gd...h\n"}]}"#.to_vec(),
                    stderr: Vec::new(),
                    stdout_truncated: false,
                    stderr_truncated: false,
                },
            ]))),
        });
        let adapter = FfprobeAdapter::with_runner(
            "/usr/local/bin/ffprobe",
            Duration::from_secs(4),
            4096,
            runner,
        );
        let path = std::env::temp_dir().join(format!("sooqa-probe-{}.bin", uuid::Uuid::new_v4()));
        tokio::fs::write(&path, b"fixture").await.expect("fixture should be writable");

        let probe = adapter.probe(&path).await.expect("probe should succeed");
        assert_eq!(probe.streams[0].codec_mime.as_deref(), Some("avc1.640028"));
        {
            let commands = commands.lock().expect("test mutex should not be poisoned");
            assert_eq!(commands.len(), 2);
            assert_eq!(
                commands[1].args(),
                &[
                    OsString::from("-v"),
                    OsString::from("error"),
                    OsString::from("-select_streams"),
                    OsString::from("v:0"),
                    OsString::from("-show_entries"),
                    OsString::from("stream=index,extradata"),
                    OsString::from("-show_data"),
                    OsString::from("-of"),
                    OsString::from("json"),
                    path.as_os_str().to_owned(),
                ]
            );
        }
        tokio::fs::remove_file(path).await.expect("fixture should be removed");
    }

    #[tokio::test]
    async fn targeted_probe_failure_fails_closed_without_rejecting_base_probe() {
        let commands = Arc::new(Mutex::new(Vec::new()));
        let base_json =
            PROBE_JSON.replace("          \"mime_codec_string\": \"avc1.640028\",\n", "");
        let runner = Arc::new(RecordingRunner {
            commands,
            outputs: Arc::new(Mutex::new(VecDeque::from([
                ExternalCommandOutput {
                    success: true,
                    exit_code: Some(0),
                    stdout: base_json.into_bytes(),
                    stderr: Vec::new(),
                    stdout_truncated: false,
                    stderr_truncated: false,
                },
                ExternalCommandOutput {
                    success: false,
                    exit_code: Some(1),
                    stdout: Vec::new(),
                    stderr: b"unsupported option".to_vec(),
                    stdout_truncated: false,
                    stderr_truncated: false,
                },
            ]))),
        });
        let adapter = FfprobeAdapter::with_runner("ffprobe", Duration::from_secs(1), 4096, runner);
        let path = std::env::temp_dir().join(format!("sooqa-probe-{}.bin", uuid::Uuid::new_v4()));
        tokio::fs::write(&path, b"fixture").await.expect("fixture should be writable");

        let probe = adapter.probe(&path).await.expect("base probe should remain usable");
        assert_eq!(probe.streams[0].codec_mime, None);
        tokio::fs::remove_file(path).await.expect("fixture should be removed");
    }

    #[tokio::test]
    async fn classifies_nonzero_ffprobe_exit_as_terminal() {
        let commands = Arc::new(Mutex::new(Vec::new()));
        let runner = Arc::new(RecordingRunner {
            commands,
            outputs: Arc::new(Mutex::new(VecDeque::from([ExternalCommandOutput {
                success: false,
                exit_code: Some(1),
                stdout: Vec::new(),
                stderr: b"Invalid data found when processing input".to_vec(),
                stdout_truncated: false,
                stderr_truncated: false,
            }]))),
        });
        let adapter = FfprobeAdapter::with_runner("ffprobe", Duration::from_secs(1), 4096, runner);
        let path = std::env::temp_dir().join(format!("sooqa-probe-{}.bin", uuid::Uuid::new_v4()));
        tokio::fs::write(&path, b"fixture").await.expect("fixture should be writable");

        let error = adapter.probe(&path).await.expect_err("probe should fail");
        assert_eq!(error.class(), "probe_process");
        assert!(!error.is_retryable());
        tokio::fs::remove_file(path).await.expect("fixture should be removed");
    }

    #[tokio::test]
    async fn classifies_ffprobe_timeout_as_retryable() {
        struct TimeoutRunner;

        #[async_trait]
        impl ExternalCommandRunner for TimeoutRunner {
            async fn run(
                &self,
                _command: ExternalCommand,
            ) -> Result<ExternalCommandOutput, CommandError> {
                Err(CommandError::TimedOut {
                    program: PathBuf::from("ffprobe"),
                    timeout: Duration::from_secs(1),
                })
            }
        }

        let adapter = FfprobeAdapter::with_runner(
            "ffprobe",
            Duration::from_secs(1),
            4096,
            Arc::new(TimeoutRunner),
        );
        let path = std::env::temp_dir().join(format!("sooqa-probe-{}.bin", uuid::Uuid::new_v4()));
        tokio::fs::write(&path, b"fixture").await.expect("fixture should be writable");

        let error = adapter.probe(&path).await.expect_err("probe should time out");
        assert_eq!(error.class(), "probe_timeout");
        assert!(error.is_retryable());
        tokio::fs::remove_file(path).await.expect("fixture should be removed");
    }

    #[tokio::test]
    #[ignore = "requires ffprobe installed on the test host"]
    async fn probes_generated_wav_fixture_with_real_ffprobe() {
        let path =
            std::env::temp_dir().join(format!("sooqa-generated-{}.wav", uuid::Uuid::new_v4()));
        tokio::fs::write(&path, generated_wav()).await.expect("fixture should be writable");
        let adapter = FfprobeAdapter::new("ffprobe", Duration::from_secs(5));

        let probe = adapter.probe(&path).await.expect("ffprobe should parse generated WAV");
        assert!(probe.container_format.as_deref().is_some_and(|format| format.contains("wav")));
        assert!(probe.streams.iter().any(|stream| stream.kind == MediaStreamKind::Audio));
        assert!(probe.duration_ms.is_some_and(|duration| duration > 0));
        tokio::fs::remove_file(path).await.expect("fixture should be removed");
    }

    fn generated_wav() -> Vec<u8> {
        let sample_rate = 8_000_u32;
        let channels = 1_u16;
        let bits_per_sample = 16_u16;
        let samples = vec![0_u8; sample_rate as usize * 2];
        let data_size = samples.len() as u32;
        let riff_size = 36 + data_size;
        let byte_rate = sample_rate * channels as u32 * bits_per_sample as u32 / 8;
        let block_align = channels * bits_per_sample / 8;
        let mut wav = Vec::with_capacity(riff_size as usize + 8);
        wav.extend_from_slice(b"RIFF");
        wav.extend_from_slice(&riff_size.to_le_bytes());
        wav.extend_from_slice(b"WAVEfmt ");
        wav.extend_from_slice(&16_u32.to_le_bytes());
        wav.extend_from_slice(&1_u16.to_le_bytes());
        wav.extend_from_slice(&channels.to_le_bytes());
        wav.extend_from_slice(&sample_rate.to_le_bytes());
        wav.extend_from_slice(&byte_rate.to_le_bytes());
        wav.extend_from_slice(&block_align.to_le_bytes());
        wav.extend_from_slice(&bits_per_sample.to_le_bytes());
        wav.extend_from_slice(b"data");
        wav.extend_from_slice(&data_size.to_le_bytes());
        wav.extend_from_slice(&samples);
        wav
    }
}
