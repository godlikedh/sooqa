use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use image::{DynamicImage, ImageDecoder, ImageReader, Limits};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    CommandError, DEFAULT_MAX_OUTPUT_BYTES, ExternalCommand, ExternalCommandRunner, MediaWorkspace,
    VideoSequenceFingerprint, WorkspaceArea, WorkspaceError, select_video_sequence_timestamps,
    sha256_bytes, video_sequence_interval_ms,
};

const DEFAULT_MAX_FRAME_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_MAX_FRAME_PIXELS: u64 = 16_000_000;
const DEFAULT_MAX_FRAME_WORKING_BYTES: u64 = 128 * 1024 * 1024;

pub const VIDEO_SEQUENCE_V1: &str = "video_sequence_v1";

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct FrameTimestamp {
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum FingerprintVersion {
    #[serde(rename = "video_sequence_v1")]
    VideoSequenceV1,
}

impl FingerprintVersion {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::VideoSequenceV1 => VIDEO_SEQUENCE_V1,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct FrameDecodeLimits {
    pub max_bytes: u64,
    pub max_pixels: u64,
    pub max_working_bytes: u64,
}

impl Default for FrameDecodeLimits {
    fn default() -> Self {
        Self {
            max_bytes: DEFAULT_MAX_FRAME_BYTES,
            max_pixels: DEFAULT_MAX_FRAME_PIXELS,
            max_working_bytes: DEFAULT_MAX_FRAME_WORKING_BYTES,
        }
    }
}

#[derive(Clone)]
pub struct FrameExtractor {
    ffmpeg_executable: PathBuf,
    runner: Arc<dyn ExternalCommandRunner>,
    timeout: Duration,
    max_output_bytes: usize,
    decode_limits: FrameDecodeLimits,
}

impl FrameExtractor {
    pub fn new(ffmpeg_executable: impl Into<PathBuf>, timeout: Duration) -> Self {
        Self::with_runner(
            ffmpeg_executable,
            timeout,
            DEFAULT_MAX_OUTPUT_BYTES,
            Arc::new(crate::ProcessCommandRunner),
        )
    }

    pub fn with_runner(
        ffmpeg_executable: impl Into<PathBuf>,
        timeout: Duration,
        max_output_bytes: usize,
        runner: Arc<dyn ExternalCommandRunner>,
    ) -> Self {
        Self {
            ffmpeg_executable: ffmpeg_executable.into(),
            runner,
            timeout,
            max_output_bytes,
            decode_limits: FrameDecodeLimits::default(),
        }
    }

    pub fn with_decode_limits(mut self, decode_limits: FrameDecodeLimits) -> Self {
        self.decode_limits = decode_limits;
        self
    }

    pub async fn extract_video_sequence_from_area_with_cache_key(
        &self,
        workspace: &MediaWorkspace,
        area: WorkspaceArea,
        input_name: &str,
        cache_key: &str,
        duration_ms: u64,
    ) -> Result<VideoSequenceFingerprint, FrameExtractionError> {
        if duration_ms == 0 {
            return Err(FrameExtractionError::InvalidDuration);
        }
        workspace.validate()?;
        let input_path = workspace.path(area, input_name)?;
        let input_metadata = tokio::fs::symlink_metadata(&input_path).await.map_err(|source| {
            FrameExtractionError::InputFile { path: input_path.clone(), source }
        })?;
        if input_metadata.file_type().is_symlink() || !input_metadata.is_file() {
            return Err(FrameExtractionError::InputNotFile { path: input_path });
        }
        let interval_ms = video_sequence_interval_ms(duration_ms)
            .ok_or(FrameExtractionError::VideoSequenceIntervalTooLarge)?;
        let frame_cache_key =
            sha256_bytes(format!("video-sequence-v1:{cache_key}:{duration_ms}").as_bytes()).sha256;
        let timestamps = select_video_sequence_timestamps(duration_ms);
        let mut frame_paths = Vec::with_capacity(timestamps.len());
        for (index, timestamp_ms) in timestamps.iter().copied().enumerate() {
            let frame_name = format!("frame-video-sequence-v1-{frame_cache_key}-{index:04}.png");
            let output_path = workspace.path(WorkspaceArea::Frames, &frame_name)?;
            let cached = match tokio::fs::symlink_metadata(&output_path).await {
                Ok(metadata) => {
                    if metadata.file_type().is_symlink() || !metadata.is_file() {
                        return Err(FrameExtractionError::InvalidOutput { path: output_path });
                    }
                    if metadata.len() == 0 {
                        tokio::fs::remove_file(&output_path).await.map_err(|source| {
                            FrameExtractionError::OutputFile { path: output_path.clone(), source }
                        })?;
                        false
                    } else {
                        existing_frame_is_valid(&output_path, self.decode_limits).await?
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
                Err(source) => {
                    return Err(FrameExtractionError::OutputFile { path: output_path, source });
                }
            };
            if !cached {
                self.extract_frame(&input_path, &output_path, FrameTimestamp { timestamp_ms })
                    .await?;
            }
            frame_paths.push(output_path);
        }

        let paths_for_decode = frame_paths.clone();
        let decode_limits = self.decode_limits;
        let images = tokio::task::spawn_blocking(move || {
            paths_for_decode
                .iter()
                .map(|path| decode_frame(path, decode_limits))
                .collect::<Result<Vec<_>, _>>()
        })
        .await
        .map_err(FrameExtractionError::TaskJoin)??;
        Ok(VideoSequenceFingerprint::from_images(duration_ms, interval_ms, &images)?)
    }

    async fn extract_frame(
        &self,
        input_path: &Path,
        output_path: &Path,
        timestamp: FrameTimestamp,
    ) -> Result<(), FrameExtractionError> {
        match tokio::fs::symlink_metadata(output_path).await {
            Ok(_) => {
                return Err(FrameExtractionError::OutputExists { path: output_path.to_owned() });
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(FrameExtractionError::OutputFile {
                    path: output_path.to_owned(),
                    source,
                });
            }
        }
        let file_name =
            output_path.file_name().ok_or_else(|| FrameExtractionError::OutputFile {
                path: output_path.to_owned(),
                source: std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "output has no file name",
                ),
            })?;
        let temporary_path = output_path.with_file_name(format!(
            ".{}-{}.png",
            file_name.to_string_lossy(),
            Uuid::new_v4()
        ));
        let _temporary_path_guard = TemporaryPath(temporary_path.clone());
        let command = ExternalCommand::new(self.ffmpeg_executable.clone())
            .arg("-hide_banner")
            .arg("-loglevel")
            .arg("error")
            .arg("-nostdin")
            .arg("-i")
            .arg(input_path.as_os_str())
            .arg("-map")
            .arg("0:v:0")
            .arg("-ss")
            .arg(format_timestamp(timestamp.timestamp_ms))
            .arg("-frames:v")
            .arg("1")
            .arg("-an")
            .arg("-f")
            .arg("image2")
            .arg(temporary_path.as_os_str())
            .timeout(self.timeout)
            .max_output_bytes(self.max_output_bytes);
        let result = async {
            let output = self.runner.run(command).await?;
            if output.stdout_truncated || output.stderr_truncated {
                return Err(FrameExtractionError::OutputLimitExceeded {
                    limit: self.max_output_bytes,
                });
            }
            if !output.success {
                return Err(FrameExtractionError::ProcessFailed {
                    exit_code: output.exit_code,
                    stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
                });
            }
            let metadata =
                tokio::fs::symlink_metadata(&temporary_path).await.map_err(|source| {
                    FrameExtractionError::OutputFile { path: temporary_path.clone(), source }
                })?;
            if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() == 0 {
                return Err(FrameExtractionError::InvalidOutput { path: temporary_path.clone() });
            }
            match tokio::fs::hard_link(&temporary_path, output_path).await {
                Ok(()) => {
                    let _ = tokio::fs::remove_file(&temporary_path).await;
                    Ok(())
                }
                Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
                    if existing_frame_is_valid(output_path, self.decode_limits).await? {
                        let _ = tokio::fs::remove_file(&temporary_path).await;
                        Ok(())
                    } else {
                        Err(FrameExtractionError::OutputExists { path: output_path.to_owned() })
                    }
                }
                Err(source) => {
                    Err(FrameExtractionError::OutputFile { path: output_path.to_owned(), source })
                }
            }
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&temporary_path).await;
        }
        result
    }
}

struct TemporaryPath(PathBuf);

impl Drop for TemporaryPath {
    fn drop(&mut self) {
        // This also runs when the async extraction future is dropped while
        // ffmpeg is still running. The operation is one unlink, not media I/O.
        let _ = std::fs::remove_file(&self.0);
    }
}

fn format_timestamp(timestamp_ms: u64) -> String {
    format!("{}.{:03}", timestamp_ms / 1_000, timestamp_ms % 1_000)
}

async fn existing_frame_is_valid(
    path: &Path,
    limits: FrameDecodeLimits,
) -> Result<bool, FrameExtractionError> {
    let path = path.to_owned();
    let decode_path = path.clone();
    match tokio::task::spawn_blocking(move || decode_frame(&decode_path, limits).map(|_| ())).await
    {
        Ok(Ok(())) => Ok(true),
        Ok(Err(_)) => {
            tokio::fs::remove_file(&path).await.map_err(|source| {
                FrameExtractionError::OutputFile { path: path.clone(), source }
            })?;
            Ok(false)
        }
        Err(error) => Err(FrameExtractionError::TaskJoin(error)),
    }
}

fn decode_frame(
    path: &Path,
    limits: FrameDecodeLimits,
) -> Result<DynamicImage, FrameExtractionError> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|source| FrameExtractionError::InputFile { path: path.to_owned(), source })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(FrameExtractionError::InputNotFile { path: path.to_owned() });
    }
    if metadata.len() > limits.max_bytes {
        return Err(FrameExtractionError::FrameTooLarge {
            path: path.to_owned(),
            limit: limits.max_bytes,
        });
    }
    let mut reader = ImageReader::open(path)
        .map_err(|source| FrameExtractionError::InputFile { path: path.to_owned(), source })?
        .with_guessed_format()
        .map_err(|source| FrameExtractionError::InputFile { path: path.to_owned(), source })?;
    let mut image_limits = Limits::default();
    image_limits.max_alloc = Some(limits.max_working_bytes.saturating_div(4).max(1));
    reader.limits(image_limits);
    let decoder = reader
        .into_decoder()
        .map_err(|source| FrameExtractionError::Decode { path: path.to_owned(), source })?;
    let (width, height) = decoder.dimensions();
    let pixels = u64::from(width).checked_mul(u64::from(height)).ok_or(
        FrameExtractionError::FrameTooManyPixels {
            path: path.to_owned(),
            limit: limits.max_pixels,
        },
    )?;
    if pixels > limits.max_pixels {
        return Err(FrameExtractionError::FrameTooManyPixels {
            path: path.to_owned(),
            limit: limits.max_pixels,
        });
    }
    let estimated_working_bytes =
        pixels.checked_mul(12).ok_or(FrameExtractionError::FrameWorkingSetTooLarge {
            path: path.to_owned(),
            limit: limits.max_working_bytes,
        })?;
    if estimated_working_bytes > limits.max_working_bytes {
        return Err(FrameExtractionError::FrameWorkingSetTooLarge {
            path: path.to_owned(),
            limit: limits.max_working_bytes,
        });
    }
    DynamicImage::from_decoder(decoder)
        .map_err(|source| FrameExtractionError::Decode { path: path.to_owned(), source })
}

#[derive(Debug, Error)]
pub enum FrameExtractionError {
    #[error("video duration must be greater than zero")]
    InvalidDuration,
    #[error("video sequence interval does not fit in a 32-bit millisecond value")]
    VideoSequenceIntervalTooLarge,
    #[error("workspace path is invalid: {0}")]
    Workspace(#[from] WorkspaceError),
    #[error("could not run ffmpeg for frame extraction: {0}")]
    Command(#[from] CommandError),
    #[error("ffmpeg frame extraction output exceeded the {limit}-byte capture limit")]
    OutputLimitExceeded { limit: usize },
    #[error("ffmpeg frame extraction failed with status {exit_code:?}: {stderr}")]
    ProcessFailed { exit_code: Option<i32>, stderr: String },
    #[error("frame output already exists: {path}")]
    OutputExists { path: PathBuf },
    #[error("could not inspect frame output {path}: {source}")]
    OutputFile { path: PathBuf, source: std::io::Error },
    #[error("frame output is missing, empty, or a symlink: {path}")]
    InvalidOutput { path: PathBuf },
    #[error("could not read extracted frame {path}: {source}")]
    InputFile { path: PathBuf, source: std::io::Error },
    #[error("video input is not a regular file: {path}")]
    InputNotFile { path: PathBuf },
    #[error("extracted frame exceeds the {limit}-byte limit: {path}")]
    FrameTooLarge { path: PathBuf, limit: u64 },
    #[error("extracted frame exceeds the {limit}-pixel limit: {path}")]
    FrameTooManyPixels { path: PathBuf, limit: u64 },
    #[error("extracted frame estimated working set exceeds the {limit}-byte limit: {path}")]
    FrameWorkingSetTooLarge { path: PathBuf, limit: u64 },
    #[error("could not decode extracted frame {path}: {source}")]
    Decode { path: PathBuf, source: image::ImageError },
    #[error("could not build video sequence fingerprint: {0}")]
    VideoSequence(#[from] crate::VideoSequenceError),
    #[error("video sequence frame decoding task failed: {0}")]
    TaskJoin(#[source] tokio::task::JoinError),
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    use async_trait::async_trait;
    use image::{DynamicImage, ImageBuffer, Rgb};
    use uuid::Uuid;

    use super::*;
    use crate::ExternalCommandOutput;

    #[derive(Clone, Default)]
    struct RecordingRunner {
        commands: Arc<Mutex<VecDeque<ExternalCommand>>>,
    }

    #[async_trait]
    impl ExternalCommandRunner for RecordingRunner {
        async fn run(
            &self,
            command: ExternalCommand,
        ) -> Result<ExternalCommandOutput, CommandError> {
            let output = PathBuf::from(command.args().last().expect("output argument exists"));
            let image = ImageBuffer::from_fn(18, 16, |x, y| {
                Rgb([x.saturating_mul(10) as u8, y.saturating_mul(10) as u8, 80])
            });
            DynamicImage::ImageRgb8(image)
                .save_with_format(&output, image::ImageFormat::Png)
                .expect("fake ffmpeg should write a frame");
            self.commands.lock().expect("runner lock should not be poisoned").push_back(command);
            Ok(ExternalCommandOutput {
                success: true,
                exit_code: Some(0),
                stdout: Vec::new(),
                stderr: Vec::new(),
                stdout_truncated: false,
                stderr_truncated: false,
            })
        }
    }

    fn temp_path(stem: &str) -> PathBuf {
        let temp_dir =
            std::fs::canonicalize(std::env::temp_dir()).expect("test temp directory exists");
        temp_dir.join(format!("sooqa-{stem}-{}", Uuid::new_v4()))
    }

    #[tokio::test]
    async fn sequence_extraction_uses_deterministic_sampling_and_cache() {
        let workspace = MediaWorkspace::create(temp_path("sequence-workspace"), Uuid::new_v4())
            .await
            .expect("workspace should be created");
        let input = workspace.path(WorkspaceArea::Normalized, "canonical.mp4").unwrap();
        std::fs::write(&input, b"fake video").expect("fake input should be written");
        let runner = RecordingRunner::default();
        let extractor = FrameExtractor::with_runner(
            "/usr/bin/ffmpeg",
            Duration::from_secs(5),
            1024,
            Arc::new(runner.clone()),
        );

        let first = extractor
            .extract_video_sequence_from_area_with_cache_key(
                &workspace,
                WorkspaceArea::Normalized,
                "canonical.mp4",
                "digest-a",
                10_000,
            )
            .await
            .expect("sequence should be extracted");
        assert_eq!(first.version.as_str(), VIDEO_SEQUENCE_V1);
        assert_eq!(first.interval_ms, 500);
        assert_eq!(first.samples.len(), 20);
        assert_eq!(runner.commands.lock().unwrap().len(), 20);

        let second = extractor
            .extract_video_sequence_from_area_with_cache_key(
                &workspace,
                WorkspaceArea::Normalized,
                "canonical.mp4",
                "digest-a",
                10_000,
            )
            .await
            .expect("cached sequence should be decoded");
        assert_eq!(second, first);
        assert_eq!(runner.commands.lock().unwrap().len(), 20);
        workspace.cleanup().await.expect("workspace should be removed");
    }
}
