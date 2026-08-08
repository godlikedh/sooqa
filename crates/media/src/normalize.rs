use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{ExternalCommand, FrameRate, MediaProbe, MediaStream, MediaStreamKind};

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
    pub video_crf: u8,
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
            video_crf: 23,
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
        if self.video_crf > 51 {
            return Err(ProfileError::InvalidCrf(self.video_crf));
        }
        if self.audio_bitrate_kbps == 0 {
            return Err(ProfileError::InvalidAudioBitrate);
        }
        Ok(())
    }
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
                self.transcode_command(input.as_ref(), output.as_ref(), video)
            }
        };
        Ok(NormalizationPlan {
            mode,
            command,
            output: output.as_ref().to_owned(),
            profile: self.profile,
        })
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
            .arg(self.scale_filter())
            .arg("-c:v")
            .arg(self.profile.video_codec.ffmpeg_name())
            .arg("-preset")
            .arg(self.profile.video_preset.ffmpeg_name())
            .arg("-crf")
            .arg(self.profile.video_crf.to_string())
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

    fn scale_filter(&self) -> String {
        format!(
            "scale='min(iw,{})':'min(ih,{})':force_original_aspect_ratio=decrease:force_divisible_by=2,format={}",
            self.profile.max_width,
            self.profile.max_height,
            self.profile.pixel_format.ffmpeg_name()
        )
    }
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
    #[error("canonical profile CRF must be between 0 and 51, got {0}")]
    InvalidCrf(u8),
    #[error("canonical profile audio bitrate must be greater than zero")]
    InvalidAudioBitrate,
}

#[derive(Debug, Error)]
pub enum NormalizationError {
    #[error("invalid canonical video profile: {0}")]
    InvalidProfile(ProfileError),
    #[error("media probe contains no video stream")]
    NoVideoStream,
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
    fn default_profile_matches_canonical_video_v1() {
        let profile = CanonicalVideoProfile::default();
        assert_eq!(profile.container, CanonicalContainer::Mp4);
        assert_eq!(profile.video_codec, VideoCodec::H264);
        assert_eq!(profile.pixel_format, PixelFormat::Yuv420p);
        assert_eq!(profile.audio_codec, AudioCodec::Aac);
        assert_eq!(profile.max_width, 1920);
        assert_eq!(profile.max_height, 1080);
        assert_eq!(profile.max_frame_rate, FrameRate { numerator: 60, denominator: 1 });
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
                "scale='min(iw,1920)':'min(ih,1080)':force_original_aspect_ratio=decrease:force_divisible_by=2,format=yuv420p",
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
    fn invalid_profile_values_are_rejected() {
        let profile = CanonicalVideoProfile { max_width: 0, ..Default::default() };
        assert!(matches!(
            NormalizationPlanner::new("ffmpeg", profile),
            Err(NormalizationError::InvalidProfile(ProfileError::InvalidDimensions { .. }))
        ));

        let profile = CanonicalVideoProfile { video_crf: 52, ..Default::default() };
        assert!(matches!(
            NormalizationPlanner::new("ffmpeg", profile),
            Err(NormalizationError::InvalidProfile(ProfileError::InvalidCrf(52)))
        ));
    }
}
