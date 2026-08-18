use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{ExternalCommand, FrameRate, MediaProbe, MediaStream, MediaStreamKind};

/// Version of the project-owned canonical MP4/H.264 profile. Changing codec,
/// adaptation, or metadata rules requires a new version before any explicit
/// reprocessing of existing media is considered.
pub const CANONICAL_VIDEO_PROFILE_VERSION: &str = "canonical_video_v2";

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanonicalContainer {
    Mp4,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VideoCodec {
    H264,
}

impl VideoCodec {
    fn ffmpeg_name(self) -> &'static str {
        match self {
            Self::H264 => "libx264",
        }
    }

    pub(crate) fn probe_name(self) -> &'static str {
        match self {
            Self::H264 => "h264",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PixelFormat {
    Yuv420p,
}

impl PixelFormat {
    pub(crate) fn ffmpeg_name(self) -> &'static str {
        match self {
            Self::Yuv420p => "yuv420p",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AudioCodec {
    Aac,
}

impl AudioCodec {
    fn ffmpeg_name(self) -> &'static str {
        match self {
            Self::Aac => "aac",
        }
    }

    pub(crate) fn probe_name(self) -> &'static str {
        match self {
            Self::Aac => "aac",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VideoPreset {
    Medium,
}

impl VideoPreset {
    fn ffmpeg_name(self) -> &'static str {
        match self {
            Self::Medium => "medium",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub struct CanonicalVideoProfile {
    pub container: CanonicalContainer,
    pub video_codec: VideoCodec,
    pub pixel_format: PixelFormat,
    pub audio_codec: AudioCodec,
    pub max_width: u32,
    pub max_height: u32,
    pub max_frame_rate: FrameRate,
    pub video_preset: VideoPreset,
    pub target_max_bytes: u64,
    pub preferred_crf: u8,
    pub maximum_crf: u8,
    pub minimum_short_edge: u32,
    pub audio_bitrate_kbps: u32,
    pub fast_start: bool,
    pub strip_metadata: bool,
}

impl Default for CanonicalVideoProfile {
    fn default() -> Self {
        Self {
            container: CanonicalContainer::Mp4,
            video_codec: VideoCodec::H264,
            pixel_format: PixelFormat::Yuv420p,
            audio_codec: AudioCodec::Aac,
            max_width: 1920,
            max_height: 1080,
            max_frame_rate: FrameRate { numerator: 60, denominator: 1 },
            video_preset: VideoPreset::Medium,
            target_max_bytes: 14 * 1024 * 1024,
            preferred_crf: 23,
            maximum_crf: 27,
            minimum_short_edge: 480,
            audio_bitrate_kbps: 128,
            fast_start: true,
            strip_metadata: true,
        }
    }
}

impl CanonicalVideoProfile {
    pub fn validate(self) -> Result<(), ProfileError> {
        if self.max_width == 0 || self.max_height == 0 {
            return Err(ProfileError::InvalidDimensions {
                width: self.max_width,
                height: self.max_height,
            });
        }
        if self.max_frame_rate.numerator == 0 || self.max_frame_rate.denominator == 0 {
            return Err(ProfileError::InvalidFrameRate(self.max_frame_rate));
        }
        if self.preferred_crf > 51 {
            return Err(ProfileError::InvalidPreferredCrf(self.preferred_crf));
        }
        if self.maximum_crf > 51 {
            return Err(ProfileError::InvalidMaximumCrf(self.maximum_crf));
        }
        if self.preferred_crf > self.maximum_crf {
            return Err(ProfileError::CrfRangeReversed {
                preferred: self.preferred_crf,
                maximum: self.maximum_crf,
            });
        }
        if self.target_max_bytes == 0 {
            return Err(ProfileError::InvalidTargetBytes);
        }
        if self.minimum_short_edge == 0 {
            return Err(ProfileError::InvalidMinimumShortEdge);
        }
        if self.minimum_short_edge > self.max_width.min(self.max_height) {
            return Err(ProfileError::MinimumShortEdgeExceedsProfile {
                minimum: self.minimum_short_edge,
                max_short_edge: self.max_width.min(self.max_height),
            });
        }
        if self.audio_bitrate_kbps == 0 {
            return Err(ProfileError::InvalidAudioBitrate);
        }
        Ok(())
    }
}

/// An even, aspect-preserving output size selected by the bounded adaptation
/// planner.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct VideoDimensions {
    pub width: u32,
    pub height: u32,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum NormalizationMode {
    Remux,
    Transcode,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NormalizationPlan {
    mode: NormalizationMode,
    command: ExternalCommand,
    output: PathBuf,
    profile: CanonicalVideoProfile,
}

impl NormalizationPlan {
    pub fn mode(&self) -> NormalizationMode {
        self.mode
    }

    pub fn command(&self) -> &ExternalCommand {
        &self.command
    }

    pub fn command_with_progress(&self) -> ExternalCommand {
        self.command_with_progress_for_output(&self.output)
    }

    pub fn command_with_progress_for_output(&self, output: impl AsRef<Path>) -> ExternalCommand {
        let args = self.command.args();
        let Some(_) = args.last() else {
            return self.command.clone();
        };
        let mut command = ExternalCommand::new(self.command.program().to_owned())
            .timeout(self.command.timeout_duration())
            .max_output_bytes(self.command.max_output_bytes_limit());
        for arg in &args[..args.len() - 1] {
            command = command.arg(arg.clone());
        }
        command.arg("-progress").arg("pipe:1").arg(output.as_ref().as_os_str())
    }

    /// Returns an equivalent plan that writes to a caller-owned attempt path.
    /// The worker uses this for remux decisions so a candidate can be inspected
    /// before it is published to the canonical destination.
    pub fn with_output(&self, output: impl AsRef<Path>) -> Self {
        let args = self.command.args();
        let mut command = ExternalCommand::new(self.command.program().to_owned())
            .timeout(self.command.timeout_duration())
            .max_output_bytes(self.command.max_output_bytes_limit());
        for arg in &args[..args.len().saturating_sub(1)] {
            command = command.arg(arg.clone());
        }
        command = command.arg(output.as_ref().as_os_str());
        Self { mode: self.mode, command, output: output.as_ref().to_owned(), profile: self.profile }
    }

    pub fn output(&self) -> &Path {
        &self.output
    }

    pub(crate) fn profile(&self) -> CanonicalVideoProfile {
        self.profile
    }
}

#[derive(Debug, Clone)]
pub struct NormalizationPlanner {
    ffmpeg_executable: PathBuf,
    profile: CanonicalVideoProfile,
}

impl NormalizationPlanner {
    pub fn new(
        ffmpeg_executable: impl Into<PathBuf>,
        profile: CanonicalVideoProfile,
    ) -> Result<Self, NormalizationError> {
        profile.validate().map_err(NormalizationError::InvalidProfile)?;
        Ok(Self { ffmpeg_executable: ffmpeg_executable.into(), profile })
    }

    pub fn profile(&self) -> CanonicalVideoProfile {
        self.profile
    }

    pub fn plan(
        &self,
        input: impl AsRef<Path>,
        output: impl AsRef<Path>,
        probe: &MediaProbe,
    ) -> Result<NormalizationPlan, NormalizationError> {
        let video = first_video_stream(probe).ok_or(NormalizationError::NoVideoStream)?;
        let mode = if is_remux_compatible(probe, video, self.profile) {
            NormalizationMode::Remux
        } else {
            NormalizationMode::Transcode
        };
        let command = match mode {
            NormalizationMode::Remux => self.remux_command(input.as_ref(), output.as_ref()),
            NormalizationMode::Transcode => {
                let dimensions = self
                    .canonical_dimensions(video)
                    .ok_or(NormalizationError::MissingVideoDimensions)?;
                self.transcode_command(
                    input.as_ref(),
                    output.as_ref(),
                    dimensions,
                    self.profile.preferred_crf,
                    video,
                )
            }
        };
        Ok(NormalizationPlan {
            mode,
            command,
            output: output.as_ref().to_owned(),
            profile: self.profile,
        })
    }

    /// Builds one bounded adaptation candidate. The caller owns the candidate
    /// execution and must validate its actual output size before selecting it.
    pub fn plan_candidate(
        &self,
        input: impl AsRef<Path>,
        output: impl AsRef<Path>,
        probe: &MediaProbe,
        dimensions: VideoDimensions,
        crf: u8,
    ) -> Result<NormalizationPlan, NormalizationError> {
        let video = first_video_stream(probe).ok_or(NormalizationError::NoVideoStream)?;
        validate_candidate_dimensions(dimensions, video, self.profile)?;
        if crf < self.profile.preferred_crf || crf > self.profile.maximum_crf {
            return Err(NormalizationError::CrfOutsideAdaptationRange {
                crf,
                preferred: self.profile.preferred_crf,
                maximum: self.profile.maximum_crf,
            });
        }
        Ok(NormalizationPlan {
            mode: NormalizationMode::Transcode,
            command: self.transcode_command(
                input.as_ref(),
                output.as_ref(),
                dimensions,
                crf,
                video,
            ),
            output: output.as_ref().to_owned(),
            profile: self.profile,
        })
    }

    /// Returns the highest non-upscaled even dimensions for the source.
    pub fn canonical_dimensions(&self, video: &MediaStream) -> Option<VideoDimensions> {
        let (width, height) = video.width.zip(video.height)?;
        bounded_dimensions(width, height, self.profile.max_width, self.profile.max_height)
    }

    /// Returns the effective lower bound for adaptation. A source whose aspect
    /// ratio forces its highest non-upscaled canonical dimensions below the
    /// configured floor must retain that native canonical size rather than be
    /// rejected for falling below the floor.
    pub fn effective_minimum_short_edge(&self, video: &MediaStream) -> Option<u32> {
        self.canonical_dimensions(video).map(|dimensions| {
            self.profile.minimum_short_edge.min(dimensions.width.min(dimensions.height))
        })
    }

    /// Returns a bounded descending resolution ladder. The first size is the
    /// highest canonical size and the last is the configured quality floor.
    pub fn resolution_ladder(&self, video: &MediaStream) -> Vec<VideoDimensions> {
        let Some(initial) = self.canonical_dimensions(video) else { return Vec::new() };
        let minimum = self.profile.minimum_short_edge;
        let initial_short = initial.width.min(initial.height);
        if initial_short <= minimum {
            return vec![initial];
        }

        let mut ladder = vec![initial];
        let mut short_edge = initial_short;
        while short_edge > minimum {
            let next = ((u64::from(short_edge) * 3 + 2) / 4) as u32;
            let next_short = next.max(minimum).min(short_edge.saturating_sub(2));
            let scale = next_short as f64 / initial_short as f64;
            let width = rounded_even_dimension(initial.width as f64 * scale);
            let height = rounded_even_dimension(initial.height as f64 * scale);
            let candidate = VideoDimensions { width: width.max(2), height: height.max(2) };
            if candidate.width.min(candidate.height) < minimum {
                let scale = minimum as f64 / initial_short as f64;
                let candidate = VideoDimensions {
                    width: rounded_even_dimension(initial.width as f64 * scale).max(2),
                    height: rounded_even_dimension(initial.height as f64 * scale).max(2),
                };
                if ladder.last() != Some(&candidate) {
                    ladder.push(candidate);
                }
                break;
            }
            if ladder.last() == Some(&candidate) {
                break;
            }
            ladder.push(candidate);
            short_edge = candidate.width.min(candidate.height);
        }
        ladder
    }

    fn remux_command(&self, input: &Path, output: &Path) -> ExternalCommand {
        let mut command = base_command(&self.ffmpeg_executable)
            .arg("-i")
            .arg(input.as_os_str())
            .arg("-map")
            .arg("0:v:0")
            .arg("-map")
            .arg("0:a:0?")
            .arg("-c")
            .arg("copy");
        command = self.append_output_options(command);
        command.arg(output.as_os_str())
    }

    fn transcode_command(
        &self,
        input: &Path,
        output: &Path,
        dimensions: VideoDimensions,
        crf: u8,
        video: &MediaStream,
    ) -> ExternalCommand {
        let mut command = base_command(&self.ffmpeg_executable)
            .arg("-i")
            .arg(input.as_os_str())
            .arg("-map")
            .arg("0:v:0")
            .arg("-map")
            .arg("0:a:0?")
            .arg("-vf")
            .arg(scale_filter(dimensions, self.profile.pixel_format))
            .arg("-c:v")
            .arg(self.profile.video_codec.ffmpeg_name())
            .arg("-preset")
            .arg(self.profile.video_preset.ffmpeg_name())
            .arg("-crf")
            .arg(crf.to_string())
            .arg("-pix_fmt")
            .arg(self.profile.pixel_format.ffmpeg_name())
            .arg("-c:a")
            .arg(self.profile.audio_codec.ffmpeg_name())
            .arg("-b:a")
            .arg(format!("{}k", self.profile.audio_bitrate_kbps));

        if requires_frame_rate_cap(video, self.profile.max_frame_rate) {
            command = command.arg("-r").arg(frame_rate_argument(self.profile.max_frame_rate));
        }

        command = self.append_output_options(command);
        command.arg(output.as_os_str())
    }

    fn append_output_options(&self, mut command: ExternalCommand) -> ExternalCommand {
        if self.profile.fast_start {
            command = command.arg("-movflags").arg("+faststart");
        }
        if self.profile.strip_metadata {
            command = command.arg("-map_metadata").arg("-1");
        }
        command
    }
}

fn scale_filter(dimensions: VideoDimensions, pixel_format: PixelFormat) -> String {
    format!(
        "scale='{}':'{}':force_original_aspect_ratio=decrease:force_divisible_by=2,format={}",
        dimensions.width,
        dimensions.height,
        pixel_format.ffmpeg_name()
    )
}

fn base_command(executable: &Path) -> ExternalCommand {
    ExternalCommand::new(executable.to_owned())
        .arg("-hide_banner")
        .arg("-loglevel")
        .arg("error")
        .arg("-nostdin")
        .arg("-y")
}

fn first_video_stream(probe: &MediaProbe) -> Option<&MediaStream> {
    probe.streams.iter().find(|stream| stream.kind == MediaStreamKind::Video)
}

fn is_remux_compatible(
    probe: &MediaProbe,
    video: &MediaStream,
    profile: CanonicalVideoProfile,
) -> bool {
    is_mp4_container(probe.container_format.as_deref())
        && video.codec.as_deref() == Some(profile.video_codec.probe_name())
        && h264_metadata_is_compatible(video)
        && video.pixel_format.as_deref() == Some(profile.pixel_format.ffmpeg_name())
        && dimensions_within_profile(video, profile)
        && !requires_frame_rate_cap(video, profile.max_frame_rate)
        && video.rotation_degrees.unwrap_or_default() == 0
        && probe
            .streams
            .iter()
            .filter(|stream| stream.kind == MediaStreamKind::Audio)
            .all(|stream| stream.codec.as_deref() == Some(profile.audio_codec.probe_name()))
}

fn h264_metadata_is_compatible(video: &MediaStream) -> bool {
    // MP4's avcC declaration and the SPS are not interchangeable. ffprobe's
    // stream level comes from the authoritative SPS; only avc1/avc3 tags are
    // accepted and a missing level is treated as insufficient evidence for a
    // quality-preserving remux.
    if !video
        .codec_tag
        .as_deref()
        .is_some_and(|tag| tag.eq_ignore_ascii_case("avc1") || tag.eq_ignore_ascii_case("avc3"))
    {
        return false;
    }
    let Some(codec_mime) = video.codec_mime.as_deref() else { return false };
    let Some(codec_tag) = video.codec_tag.as_deref() else { return false };
    let Some((declared_codec, profile_level_id)) = codec_mime.split_once('.') else {
        return false;
    };
    if !declared_codec.eq_ignore_ascii_case(codec_tag)
        || profile_level_id.len() != 6
        || !profile_level_id.is_ascii()
        || !profile_level_id.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return false;
    }
    let Some(declared_profile) =
        profile_level_id.get(..2).and_then(|value| u8::from_str_radix(value, 16).ok())
    else {
        return false;
    };
    let Some(authoritative_profile) = video.profile.as_deref().and_then(h264_profile_idc) else {
        return false;
    };
    if declared_profile != authoritative_profile {
        return false;
    }
    let Some(declared_level) =
        profile_level_id.get(4..).and_then(|value| u32::from_str_radix(value, 16).ok())
    else {
        return false;
    };
    let Some(level) = video.level else { return false };
    if declared_level != level {
        return false;
    }
    let Some((width, height)) = video.width.zip(video.height) else { return false };
    let macroblocks = u64::from(width.div_ceil(16)) * u64::from(height.div_ceil(16));
    let frame_rate = video.frame_rate.map_or(0, |rate| {
        if rate.denominator == 0 { 0 } else { rate.numerator.div_ceil(rate.denominator) }
    });
    let macroblocks_per_second = macroblocks.saturating_mul(frame_rate);
    let Some((max_macroblocks, max_macroblocks_per_second)) = h264_level_limits(level) else {
        return false;
    };
    macroblocks <= max_macroblocks && macroblocks_per_second <= max_macroblocks_per_second
}

fn h264_profile_idc(profile: &str) -> Option<u8> {
    match profile.to_ascii_lowercase().as_str() {
        "baseline" | "constrained baseline" => Some(66),
        "main" => Some(77),
        "extended" => Some(88),
        "high" | "constrained high" => Some(100),
        "high 10" => Some(110),
        "high 4:2:2" => Some(122),
        "high 4:4:4 predictive" => Some(244),
        "cavlc 4:4:4 intra" => Some(44),
        _ => None,
    }
}

fn h264_level_limits(level: u32) -> Option<(u64, u64)> {
    // H.264 Annex A limits, expressed as level codes used by ffprobe.
    Some(match level {
        10 => (99, 1_485),
        11 => (396, 3_000),
        12 => (396, 6_000),
        13 => (396, 11_880),
        20 => (396, 11_880),
        21 => (792, 19_800),
        22 => (1_620, 20_250),
        30 => (1_620, 40_500),
        31 => (3_600, 108_000),
        32 => (5_120, 216_000),
        40 | 41 => (8_192, 245_760),
        42 => (8_704, 522_240),
        50 => (22_080, 589_824),
        51 => (36_864, 983_040),
        52 => (36_864, 2_073_600),
        60 => (36_864, 2_073_600),
        61 => (69_120, 4_177_920),
        62 => (69_120, 4_177_920),
        _ => return None,
    })
}

fn is_mp4_container(container: Option<&str>) -> bool {
    container
        .map(|value| {
            value.split(',').any(|format| {
                let format = format.trim();
                format.eq_ignore_ascii_case("mp4") || format.eq_ignore_ascii_case("mov")
            })
        })
        .unwrap_or(false)
}

fn dimensions_within_profile(video: &MediaStream, profile: CanonicalVideoProfile) -> bool {
    video
        .width
        .zip(video.height)
        .is_some_and(|(width, height)| width <= profile.max_width && height <= profile.max_height)
}

fn bounded_dimensions(
    width: u32,
    height: u32,
    max_width: u32,
    max_height: u32,
) -> Option<VideoDimensions> {
    if width < 2 || height < 2 {
        return None;
    }
    let scale = (max_width as f64 / width as f64).min(max_height as f64 / height as f64).min(1.0);
    Some(VideoDimensions {
        width: rounded_even_dimension(width as f64 * scale).min(even_dimension(width)).max(2),
        height: rounded_even_dimension(height as f64 * scale).min(even_dimension(height)).max(2),
    })
}

fn validate_candidate_dimensions(
    dimensions: VideoDimensions,
    source: &MediaStream,
    profile: CanonicalVideoProfile,
) -> Result<(), NormalizationError> {
    let effective_minimum_short_edge = source
        .width
        .zip(source.height)
        .and_then(|(width, height)| {
            bounded_dimensions(width, height, profile.max_width, profile.max_height)
        })
        .map(|canonical| profile.minimum_short_edge.min(canonical.width.min(canonical.height)));
    if dimensions.width == 0
        || dimensions.height == 0
        || !dimensions.width.is_multiple_of(2)
        || !dimensions.height.is_multiple_of(2)
        || dimensions.width > profile.max_width
        || dimensions.height > profile.max_height
        || effective_minimum_short_edge
            .is_some_and(|minimum| dimensions.width.min(dimensions.height) < minimum)
    {
        return Err(NormalizationError::InvalidCandidateDimensions { dimensions });
    }
    if let Some((width, height)) = source.width.zip(source.height)
        && (dimensions.width > width || dimensions.height > height)
    {
        return Err(NormalizationError::InvalidCandidateDimensions { dimensions });
    }
    Ok(())
}

fn even_dimension(value: u32) -> u32 {
    value.saturating_sub(value % 2)
}

fn rounded_even_dimension(value: f64) -> u32 {
    ((value / 2.0).round() * 2.0).max(2.0) as u32
}

pub(crate) fn requires_frame_rate_cap(video: &MediaStream, maximum: FrameRate) -> bool {
    let Some(rate) = video.frame_rate else {
        return true;
    };
    if rate.denominator == 0 {
        return true;
    }
    u128::from(rate.numerator) * u128::from(maximum.denominator)
        > u128::from(maximum.numerator) * u128::from(rate.denominator)
}

fn frame_rate_argument(rate: FrameRate) -> String {
    if rate.denominator == 1 {
        rate.numerator.to_string()
    } else {
        format!("{}/{}", rate.numerator, rate.denominator)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Error)]
pub enum ProfileError {
    #[error("canonical profile dimensions must be greater than zero, got {width}x{height}")]
    InvalidDimensions { width: u32, height: u32 },
    #[error("canonical profile frame rate must be greater than zero")]
    InvalidFrameRate(FrameRate),
    #[error("canonical profile preferred CRF must be between 0 and 51, got {0}")]
    InvalidPreferredCrf(u8),
    #[error("canonical profile maximum CRF must be between 0 and 51, got {0}")]
    InvalidMaximumCrf(u8),
    #[error("canonical profile CRF range is reversed: preferred {preferred}, maximum {maximum}")]
    CrfRangeReversed { preferred: u8, maximum: u8 },
    #[error("canonical profile target byte ceiling must be greater than zero")]
    InvalidTargetBytes,
    #[error("canonical profile minimum short edge must be greater than zero")]
    InvalidMinimumShortEdge,
    #[error(
        "canonical profile minimum short edge {minimum} exceeds maximum short edge {max_short_edge}"
    )]
    MinimumShortEdgeExceedsProfile { minimum: u32, max_short_edge: u32 },
    #[error("canonical profile audio bitrate must be greater than zero")]
    InvalidAudioBitrate,
}

#[derive(Debug, Error)]
pub enum NormalizationError {
    #[error("invalid canonical video profile: {0}")]
    InvalidProfile(ProfileError),
    #[error("media probe contains no video stream")]
    NoVideoStream,
    #[error("media probe does not contain video dimensions")]
    MissingVideoDimensions,
    #[error("candidate CRF {crf} is outside the adaptation range {preferred}..={maximum}")]
    CrfOutsideAdaptationRange { crf: u8, preferred: u8, maximum: u8 },
    #[error("candidate dimensions {dimensions:?} are outside the bounded profile")]
    InvalidCandidateDimensions { dimensions: VideoDimensions },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn video_probe(
        container: &str,
        codec: &str,
        pixel_format: &str,
        width: u32,
        height: u32,
        frame_rate: Option<FrameRate>,
        audio_codec: Option<&str>,
    ) -> MediaProbe {
        let mut streams = vec![MediaStream {
            index: 0,
            kind: MediaStreamKind::Video,
            codec: Some(codec.to_owned()),
            codec_tag: (codec == "h264").then(|| "avc1".to_owned()),
            codec_mime: (codec == "h264").then(|| "avc1.640028".to_owned()),
            level: (codec == "h264").then_some(40),
            profile: Some("High".to_owned()),
            pixel_format: Some(pixel_format.to_owned()),
            width: Some(width),
            height: Some(height),
            display_aspect_ratio: None,
            frame_rate,
            rotation_degrees: Some(0),
            sample_rate_hz: None,
            channels: None,
            bit_rate: Some(1_000_000),
        }];
        if let Some(codec) = audio_codec {
            streams.push(MediaStream {
                index: 1,
                kind: MediaStreamKind::Audio,
                codec: Some(codec.to_owned()),
                codec_tag: None,
                codec_mime: None,
                level: None,
                profile: None,
                pixel_format: None,
                width: None,
                height: None,
                display_aspect_ratio: None,
                frame_rate: None,
                rotation_degrees: None,
                sample_rate_hz: Some(48_000),
                channels: Some(2),
                bit_rate: Some(128_000),
            });
        }
        MediaProbe {
            container_format: Some(container.to_owned()),
            duration_ms: Some(2_000),
            size_bytes: 10_000,
            bit_rate: Some(1_128_000),
            streams,
        }
    }

    fn args(plan: &NormalizationPlan) -> Vec<String> {
        plan.command().args().iter().map(|arg| arg.to_string_lossy().into_owned()).collect()
    }

    #[test]
    fn default_profile_matches_canonical_video_v2() {
        let profile = CanonicalVideoProfile::default();
        assert_eq!(profile.container, CanonicalContainer::Mp4);
        assert_eq!(profile.video_codec, VideoCodec::H264);
        assert_eq!(profile.pixel_format, PixelFormat::Yuv420p);
        assert_eq!(profile.audio_codec, AudioCodec::Aac);
        assert_eq!(profile.max_width, 1920);
        assert_eq!(profile.max_height, 1080);
        assert_eq!(profile.max_frame_rate, FrameRate { numerator: 60, denominator: 1 });
        assert_eq!(profile.target_max_bytes, 14 * 1024 * 1024);
        assert_eq!(profile.preferred_crf, 23);
        assert_eq!(profile.maximum_crf, 27);
        assert_eq!(profile.minimum_short_edge, 480);
        assert!(profile.validate().is_ok());
    }

    #[test]
    fn compatible_mp4_uses_remux_without_reencoding() {
        let planner = NormalizationPlanner::new("/usr/local/bin/ffmpeg", Default::default())
            .expect("default profile should be valid");
        let plan = planner
            .plan(
                "/tmp/input with spaces.mp4",
                "/tmp/output.mp4",
                &video_probe(
                    "mov,mp4,m4a,3gp,3g2,mj2",
                    "h264",
                    "yuv420p",
                    1920,
                    1080,
                    Some(FrameRate { numerator: 30, denominator: 1 }),
                    Some("aac"),
                ),
            )
            .expect("compatible input should produce a plan");

        assert_eq!(plan.mode(), NormalizationMode::Remux);
        assert_eq!(plan.command().program(), Path::new("/usr/local/bin/ffmpeg"));
        assert_eq!(
            args(&plan),
            vec![
                "-hide_banner",
                "-loglevel",
                "error",
                "-nostdin",
                "-y",
                "-i",
                "/tmp/input with spaces.mp4",
                "-map",
                "0:v:0",
                "-map",
                "0:a:0?",
                "-c",
                "copy",
                "-movflags",
                "+faststart",
                "-map_metadata",
                "-1",
                "/tmp/output.mp4",
            ]
        );
    }

    #[test]
    fn portrait_or_oversized_video_transcodes_with_aspect_preserving_scale() {
        let planner = NormalizationPlanner::new("ffmpeg", Default::default())
            .expect("default profile should be valid");
        let plan = planner
            .plan(
                "/tmp/portrait.webm",
                "/tmp/canonical.mp4",
                &video_probe(
                    "matroska,webm",
                    "vp9",
                    "yuv420p",
                    720,
                    1280,
                    Some(FrameRate { numerator: 30, denominator: 1 }),
                    Some("opus"),
                ),
            )
            .expect("unsupported input should produce a transcode plan");
        let command_args = args(&plan);

        assert_eq!(plan.mode(), NormalizationMode::Transcode);
        assert!(command_args.windows(2).any(|pair| pair == ["-c:v", "libx264"]));
        assert!(command_args.windows(2).any(|pair| pair == ["-c:a", "aac"]));
        assert!(command_args.windows(2).any(|pair| {
                pair == [
                    "-vf",
                "scale='608':'1080':force_original_aspect_ratio=decrease:force_divisible_by=2,format=yuv420p",
            ]
        }));
        assert!(!command_args.contains(&"-r".to_owned()));
        assert_eq!(command_args.last(), Some(&"/tmp/canonical.mp4".to_owned()));
    }

    #[test]
    fn excessive_frame_rate_adds_a_cap_without_changing_the_profile() {
        let planner = NormalizationPlanner::new("ffmpeg", Default::default())
            .expect("default profile should be valid");
        let plan = planner
            .plan(
                "/tmp/high-fps.mp4",
                "/tmp/canonical.mp4",
                &video_probe(
                    "mp4",
                    "h264",
                    "yuv420p",
                    1280,
                    720,
                    Some(FrameRate { numerator: 120, denominator: 1 }),
                    Some("aac"),
                ),
            )
            .expect("high frame rate input should produce a plan");
        let command_args = args(&plan);

        assert_eq!(plan.mode(), NormalizationMode::Transcode);
        assert!(command_args.windows(2).any(|pair| pair == ["-r", "60"]));
    }

    #[test]
    fn oversized_compatible_mp4_still_tries_remux_before_adaptation() {
        let planner = NormalizationPlanner::new("ffmpeg", Default::default())
            .expect("default profile should be valid");
        let mut probe = video_probe(
            "mp4",
            "h264",
            "yuv420p",
            1920,
            1080,
            Some(FrameRate { numerator: 30, denominator: 1 }),
            Some("aac"),
        );
        probe.size_bytes = 15 * 1024 * 1024;
        let plan = planner
            .plan("input.mp4", "output.mp4", &probe)
            .expect("oversized input should still produce a remux candidate");
        assert_eq!(plan.mode(), NormalizationMode::Remux);
        let command_args = args(&plan);
        assert!(command_args.windows(2).any(|pair| pair == ["-c", "copy"]));
        assert!(!command_args.iter().any(|argument| argument == "-crf"));
    }

    #[test]
    fn effective_floor_accepts_portrait_canonical_size_below_configured_floor() {
        let planner = NormalizationPlanner::new("ffmpeg", Default::default())
            .expect("default profile should be valid");
        let probe = video_probe(
            "matroska,webm",
            "vp9",
            "yuv420p",
            1080,
            2560,
            Some(FrameRate { numerator: 30, denominator: 1 }),
            Some("opus"),
        );
        assert_eq!(
            planner.canonical_dimensions(&probe.streams[0]),
            Some(VideoDimensions { width: 456, height: 1080 })
        );
        assert_eq!(planner.effective_minimum_short_edge(&probe.streams[0]), Some(456));
        planner
            .plan_candidate(
                "input.webm",
                "output.mp4",
                &probe,
                VideoDimensions { width: 456, height: 1080 },
                23,
            )
            .expect("highest portrait canonical dimensions should remain valid");
    }

    #[test]
    fn effective_floor_accepts_aspect_limited_size_with_higher_configured_floor() {
        let profile = CanonicalVideoProfile { minimum_short_edge: 720, ..Default::default() };
        let planner = NormalizationPlanner::new("ffmpeg", profile)
            .expect("higher floor profile should be valid");
        let probe = video_probe(
            "matroska,webm",
            "vp9",
            "yuv420p",
            1080,
            1920,
            Some(FrameRate { numerator: 30, denominator: 1 }),
            Some("opus"),
        );
        assert_eq!(
            planner.canonical_dimensions(&probe.streams[0]),
            Some(VideoDimensions { width: 608, height: 1080 })
        );
        assert_eq!(planner.effective_minimum_short_edge(&probe.streams[0]), Some(608));
        planner
            .plan_candidate(
                "input.webm",
                "output.mp4",
                &probe,
                VideoDimensions { width: 608, height: 1080 },
                23,
            )
            .expect("aspect-limited canonical dimensions should remain valid");
    }

    #[test]
    fn contradictory_avcc_level_and_sps_level_cannot_remux() {
        let planner = NormalizationPlanner::new("ffmpeg", Default::default())
            .expect("default profile should be valid");
        let mut probe = video_probe(
            "mp4",
            "h264",
            "yuv420p",
            1920,
            1080,
            Some(FrameRate { numerator: 30, denominator: 1 }),
            Some("aac"),
        );
        probe.streams[0].codec_mime = Some("avc1.64001e".to_owned());
        let plan = planner
            .plan("input.mp4", "output.mp4", &probe)
            .expect("contradictory metadata should fall back to transcode");
        assert_eq!(plan.mode(), NormalizationMode::Transcode);
    }

    #[test]
    fn contradictory_avcc_codec_tag_cannot_remux() {
        let planner = NormalizationPlanner::new("ffmpeg", Default::default())
            .expect("default profile should be valid");
        let mut probe = video_probe(
            "mp4",
            "h264",
            "yuv420p",
            1920,
            1080,
            Some(FrameRate { numerator: 30, denominator: 1 }),
            Some("aac"),
        );
        probe.streams[0].codec_mime = Some("hev1.640028".to_owned());
        let plan = planner
            .plan("input.mp4", "output.mp4", &probe)
            .expect("contradictory metadata should fall back to transcode");
        assert_eq!(plan.mode(), NormalizationMode::Transcode);
    }

    #[test]
    fn contradictory_avcc_profile_cannot_remux() {
        let planner = NormalizationPlanner::new("ffmpeg", Default::default())
            .expect("default profile should be valid");
        let mut probe = video_probe(
            "mp4",
            "h264",
            "yuv420p",
            1920,
            1080,
            Some(FrameRate { numerator: 30, denominator: 1 }),
            Some("aac"),
        );
        probe.streams[0].codec_mime = Some("avc1.420028".to_owned());
        let plan = planner
            .plan("input.mp4", "output.mp4", &probe)
            .expect("contradictory metadata should fall back to transcode");
        assert_eq!(plan.mode(), NormalizationMode::Transcode);
    }

    #[test]
    fn resolution_ladder_stops_at_floor_without_upscaling() {
        let planner = NormalizationPlanner::new("ffmpeg", Default::default())
            .expect("default profile should be valid");
        let probe = video_probe(
            "matroska,webm",
            "vp9",
            "yuv420p",
            1920,
            1080,
            Some(FrameRate { numerator: 30, denominator: 1 }),
            Some("opus"),
        );
        let ladder = planner.resolution_ladder(&probe.streams[0]);
        assert_eq!(
            ladder,
            vec![
                VideoDimensions { width: 1920, height: 1080 },
                VideoDimensions { width: 1440, height: 810 },
                VideoDimensions { width: 1080, height: 608 },
                VideoDimensions { width: 854, height: 480 },
            ]
        );

        let small = video_probe(
            "matroska,webm",
            "vp9",
            "yuv420p",
            320,
            240,
            Some(FrameRate { numerator: 30, denominator: 1 }),
            Some("opus"),
        );
        assert_eq!(
            planner.resolution_ladder(&small.streams[0]),
            vec![VideoDimensions { width: 320, height: 240 }]
        );
        planner
            .plan_candidate(
                "input.webm",
                "output.mp4",
                &small,
                VideoDimensions { width: 320, height: 240 },
                23,
            )
            .expect("native sub-floor source should not be rejected or upscaled");
    }

    #[test]
    fn missing_video_stream_is_rejected_before_command_construction() {
        let planner = NormalizationPlanner::new("ffmpeg", Default::default())
            .expect("default profile should be valid");
        let probe = MediaProbe {
            container_format: Some("mp4".to_owned()),
            duration_ms: Some(2_000),
            size_bytes: 10,
            bit_rate: None,
            streams: vec![MediaStream {
                index: 0,
                kind: MediaStreamKind::Audio,
                codec: Some("aac".to_owned()),
                codec_tag: None,
                codec_mime: None,
                level: None,
                profile: None,
                pixel_format: None,
                width: None,
                height: None,
                display_aspect_ratio: None,
                frame_rate: None,
                rotation_degrees: None,
                sample_rate_hz: Some(48_000),
                channels: Some(2),
                bit_rate: Some(128_000),
            }],
        };

        assert!(matches!(
            planner.plan("input", "output", &probe),
            Err(NormalizationError::NoVideoStream)
        ));
    }

    #[test]
    fn missing_video_dimensions_are_rejected_before_transcode_command_construction() {
        let planner = NormalizationPlanner::new("ffmpeg", Default::default())
            .expect("default profile should be valid");
        let mut probe = video_probe(
            "matroska,webm",
            "vp9",
            "yuv420p",
            320,
            240,
            Some(FrameRate { numerator: 30, denominator: 1 }),
            Some("opus"),
        );
        probe.streams[0].width = None;
        let error = planner
            .plan("input.webm", "output.mp4", &probe)
            .expect_err("missing dimensions must not silently become a 2x2 encode");
        assert!(matches!(error, NormalizationError::MissingVideoDimensions));
    }

    #[test]
    fn invalid_profile_values_are_rejected() {
        let profile = CanonicalVideoProfile { max_width: 0, ..Default::default() };
        assert!(matches!(
            NormalizationPlanner::new("ffmpeg", profile),
            Err(NormalizationError::InvalidProfile(ProfileError::InvalidDimensions { .. }))
        ));

        let profile = CanonicalVideoProfile { preferred_crf: 52, ..Default::default() };
        assert!(matches!(
            NormalizationPlanner::new("ffmpeg", profile),
            Err(NormalizationError::InvalidProfile(ProfileError::InvalidPreferredCrf(52)))
        ));
    }
}
