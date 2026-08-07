use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use image::{DynamicImage, ImageDecoder, ImageReader, Limits, imageops::FilterType};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    CommandError, DEFAULT_MAX_OUTPUT_BYTES, ExternalCommand, ExternalCommandRunner, MediaProbe,
    MediaWorkspace, WorkspaceArea, WorkspaceError,
};

const FRAME_RATIOS_BPS: [u16; 7] = [500, 1500, 3000, 5000, 7000, 8500, 9500];
const FRAME_DHASH_WIDTH: u32 = 9;
const FRAME_DHASH_HEIGHT: u32 = 8;
const DEFAULT_MAX_FRAME_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_MAX_FRAME_PIXELS: u64 = 16_000_000;
const DEFAULT_MAX_FRAME_WORKING_BYTES: u64 = 128 * 1024 * 1024;

pub const FRAME_DHASH_V1: &str = "frame_dhash_v1";

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub struct FrameTimestamp {
    pub timestamp_ms: u64,
    pub ratio_bps: u16,
}

/// Select stable relative positions while collapsing duplicate timestamps in
/// very short videos.
pub fn select_fingerprint_timestamps(duration_ms: u64) -> Vec<FrameTimestamp> {
    let mut timestamps = Vec::with_capacity(FRAME_RATIOS_BPS.len());
    for ratio_bps in FRAME_RATIOS_BPS {
        let timestamp_ms = (u128::from(duration_ms) * u128::from(ratio_bps) / 10_000) as u64;
        if timestamps.last().is_none_or(|last: &FrameTimestamp| last.timestamp_ms != timestamp_ms) {
            timestamps.push(FrameTimestamp { timestamp_ms, ratio_bps });
        }
    }
    timestamps
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum FingerprintVersion {
    #[serde(rename = "frame_dhash_v1")]
    FrameDHashV1,
}

impl FingerprintVersion {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FrameDHashV1 => FRAME_DHASH_V1,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct FrameFingerprint {
    pub timestamp_ms: u64,
    pub ratio_bps: u16,
    pub hash: u64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct VideoFingerprint {
    pub version: FingerprintVersion,
    pub duration_ms: u64,
    pub frames: Vec<FrameFingerprint>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FrameExtractionResult {
    pub fingerprint: VideoFingerprint,
    pub frame_paths: Vec<PathBuf>,
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

    pub async fn extract(
        &self,
        workspace: &MediaWorkspace,
        input_name: &str,
        duration_ms: u64,
    ) -> Result<FrameExtractionResult, FrameExtractionError> {
        if duration_ms == 0 {
            return Err(FrameExtractionError::InvalidDuration);
        }
        workspace.validate()?;
        let input_path = workspace.path(WorkspaceArea::Source, input_name)?;
        let input_metadata = tokio::fs::symlink_metadata(&input_path).await.map_err(|source| {
            FrameExtractionError::InputFile { path: input_path.clone(), source }
        })?;
        if input_metadata.file_type().is_symlink() || !input_metadata.is_file() {
            return Err(FrameExtractionError::InputNotFile { path: input_path });
        }
        let timestamps = select_fingerprint_timestamps(duration_ms);
        let mut frame_paths = Vec::with_capacity(timestamps.len());

        for (index, timestamp) in timestamps.iter().enumerate() {
            let frame_name = format!("frame-dhash-v1-{index:02}.png");
            let output_path = match workspace.path(WorkspaceArea::Frames, &frame_name) {
                Ok(path) => path,
                Err(error) => return Err(error.into()),
            };
            let needs_extraction = match tokio::fs::symlink_metadata(&output_path).await {
                Ok(metadata) => {
                    if metadata.file_type().is_symlink() || !metadata.is_file() {
                        return Err(FrameExtractionError::InvalidOutput { path: output_path });
                    }
                    if metadata.len() == 0 {
                        tokio::fs::remove_file(&output_path).await.map_err(|source| {
                            FrameExtractionError::OutputFile { path: output_path.clone(), source }
                        })?;
                        true
                    } else {
                        if existing_frame_hash(&output_path, self.decode_limits).await?.is_some() {
                            frame_paths.push(output_path);
                            continue;
                        }
                        true
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
                Err(source) => {
                    return Err(FrameExtractionError::OutputFile { path: output_path, source });
                }
            };
            if needs_extraction {
                self.extract_frame(&input_path, &output_path, *timestamp).await?;
            }
            frame_paths.push(output_path);
        }

        let paths_for_hashing = frame_paths.clone();
        let decode_limits = self.decode_limits;
        let frames = match tokio::task::spawn_blocking(move || {
            paths_for_hashing
                .iter()
                .map(|path| decode_and_hash(path, decode_limits))
                .collect::<Result<Vec<_>, _>>()
        })
        .await
        {
            Ok(result) => match result {
                Ok(frames) => frames,
                Err(error) => return Err(error),
            },
            Err(error) => {
                return Err(FrameExtractionError::TaskJoin(error));
            }
        };

        let frames = timestamps
            .into_iter()
            .zip(frames)
            .map(|(timestamp, hash)| FrameFingerprint {
                timestamp_ms: timestamp.timestamp_ms,
                ratio_bps: timestamp.ratio_bps,
                hash,
            })
            .collect();
        Ok(FrameExtractionResult {
            fingerprint: VideoFingerprint {
                version: FingerprintVersion::FrameDHashV1,
                duration_ms,
                frames,
            },
            frame_paths,
        })
    }

    pub async fn extract_for_probe(
        &self,
        workspace: &MediaWorkspace,
        input_name: &str,
        probe: &MediaProbe,
    ) -> Result<FrameExtractionResult, FrameExtractionError> {
        let duration_ms = probe.duration_ms.ok_or(FrameExtractionError::MissingDuration)?;
        self.extract(workspace, input_name, duration_ms).await
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
                    Err(FrameExtractionError::OutputExists { path: output_path.to_owned() })
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

async fn existing_frame_hash(
    path: &Path,
    limits: FrameDecodeLimits,
) -> Result<Option<u64>, FrameExtractionError> {
    let path = path.to_owned();
    let decode_path = path.clone();
    match tokio::task::spawn_blocking(move || decode_and_hash(&decode_path, limits)).await {
        Ok(Ok(hash)) => Ok(Some(hash)),
        Ok(Err(_)) => {
            tokio::fs::remove_file(&path).await.map_err(|source| {
                FrameExtractionError::OutputFile { path: path.clone(), source }
            })?;
            Ok(None)
        }
        Err(error) => Err(FrameExtractionError::TaskJoin(error)),
    }
}

fn decode_and_hash(path: &Path, limits: FrameDecodeLimits) -> Result<u64, FrameExtractionError> {
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
    let image = DynamicImage::from_decoder(decoder)
        .map_err(|source| FrameExtractionError::Decode { path: path.to_owned(), source })?;
    Ok(frame_dhash_v1(&image))
}

/// Compute the versioned 64-bit dHash used by G1.
///
/// The image is converted to grayscale, resized to 9×8, then each row's eight
/// adjacent horizontal comparisons are packed in row-major order. A set bit
/// means the left pixel is darker than the right pixel.
pub fn frame_dhash_v1(image: &DynamicImage) -> u64 {
    let grayscale = image.grayscale();
    let resized = grayscale
        .resize_exact(FRAME_DHASH_WIDTH, FRAME_DHASH_HEIGHT, FilterType::Triangle)
        .to_luma8();
    let mut hash = 0_u64;
    for y in 0..FRAME_DHASH_HEIGHT {
        for x in 0..(FRAME_DHASH_WIDTH - 1) {
            if resized.get_pixel(x, y).0[0] < resized.get_pixel(x + 1, y).0[0] {
                let bit = u64::from(y * (FRAME_DHASH_WIDTH - 1) + x);
                hash |= 1 << bit;
            }
        }
    }
    hash
}

#[derive(Debug, Error)]
pub enum FrameExtractionError {
    #[error("video duration must be greater than zero")]
    InvalidDuration,
    #[error("video probe did not contain a duration")]
    MissingDuration,
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
    #[error("frame hashing task failed: {0}")]
    TaskJoin(#[source] tokio::task::JoinError),
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    use image::{ImageBuffer, Luma, Rgb};
    use uuid::Uuid;

    use super::*;

    #[derive(Clone, Default)]
    struct RecordingRunner {
        commands: Arc<Mutex<VecDeque<ExternalCommand>>>,
    }

    use crate::ExternalCommandOutput;
    use async_trait::async_trait;

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

    #[test]
    fn timestamps_are_stable_and_short_inputs_collapse_duplicates() {
        assert_eq!(
            select_fingerprint_timestamps(1_000),
            vec![
                FrameTimestamp { timestamp_ms: 50, ratio_bps: 500 },
                FrameTimestamp { timestamp_ms: 150, ratio_bps: 1500 },
                FrameTimestamp { timestamp_ms: 300, ratio_bps: 3000 },
                FrameTimestamp { timestamp_ms: 500, ratio_bps: 5000 },
                FrameTimestamp { timestamp_ms: 700, ratio_bps: 7000 },
                FrameTimestamp { timestamp_ms: 850, ratio_bps: 8500 },
                FrameTimestamp { timestamp_ms: 950, ratio_bps: 9500 },
            ]
        );
        assert_eq!(select_fingerprint_timestamps(1).len(), 1);
    }

    #[test]
    fn dhash_v1_is_stable_for_a_horizontal_gradient() {
        let image = ImageBuffer::from_fn(9, 8, |x, _| Luma([(x * 20) as u8]));
        assert_eq!(frame_dhash_v1(&DynamicImage::ImageLuma8(image)), u64::MAX);
        assert_eq!(FingerprintVersion::FrameDHashV1.as_str(), FRAME_DHASH_V1);
        let encoded = serde_json::to_string(&FingerprintVersion::FrameDHashV1)
            .expect("fingerprint version should serialize");
        assert_eq!(encoded, "\"frame_dhash_v1\"");
    }

    #[tokio::test]
    async fn extraction_writes_frames_and_returns_versioned_hashes() {
        let workspace = MediaWorkspace::create(temp_path("fingerprint-workspace"), Uuid::new_v4())
            .await
            .expect("workspace should be created");
        let input =
            workspace.path(WorkspaceArea::Source, "input.mp4").expect("input path should be valid");
        std::fs::write(&input, b"fake video").expect("fake input should be written");
        let runner = RecordingRunner::default();
        let extractor = FrameExtractor::with_runner(
            "/usr/bin/ffmpeg",
            Duration::from_secs(5),
            1024,
            Arc::new(runner.clone()),
        );

        let result = extractor
            .extract(&workspace, "input.mp4", 10_000)
            .await
            .expect("frames should be extracted");
        assert_eq!(result.fingerprint.version, FingerprintVersion::FrameDHashV1);
        assert_eq!(result.fingerprint.duration_ms, 10_000);
        assert_eq!(result.fingerprint.frames.len(), 7);
        assert_eq!(result.frame_paths.len(), 7);
        assert!(result.fingerprint.frames.iter().all(|frame| frame.hash != 0));
        assert_eq!(runner.commands.lock().expect("runner lock should not be poisoned").len(), 7);
        let first_command = runner
            .commands
            .lock()
            .expect("runner lock should not be poisoned")
            .front()
            .cloned()
            .expect("first command should be recorded");
        let args = first_command.args();
        assert!(args.windows(2).any(|pair| {
            pair[0].to_string_lossy() == "-map" && pair[1].to_string_lossy() == "0:v:0"
        }));
        assert!(args.windows(2).any(|pair| {
            pair[0].to_string_lossy() == "-ss" && pair[1].to_string_lossy() == "0.500"
        }));
        for path in &result.frame_paths {
            assert!(path.is_file());
        }
        let rerun = extractor
            .extract(&workspace, "input.mp4", 10_000)
            .await
            .expect("existing valid frames should be reused");
        assert_eq!(rerun.fingerprint, result.fingerprint);
        assert_eq!(runner.commands.lock().expect("runner lock should not be poisoned").len(), 7);

        std::fs::write(&result.frame_paths[0], b"corrupt frame")
            .expect("corrupt frame should be written");
        let repaired = extractor
            .extract(&workspace, "input.mp4", 10_000)
            .await
            .expect("corrupt frame should be replaced");
        assert_eq!(repaired.fingerprint, result.fingerprint);
        assert_eq!(runner.commands.lock().expect("runner lock should not be poisoned").len(), 8);

        let bounded_extractor = extractor
            .with_decode_limits(FrameDecodeLimits { max_bytes: 1, ..FrameDecodeLimits::default() });
        let error = bounded_extractor
            .extract(&workspace, "input.mp4", 10_000)
            .await
            .expect_err("frame byte limit should be enforced");
        assert!(matches!(error, FrameExtractionError::FrameTooLarge { limit: 1, .. }));

        workspace.cleanup().await.expect("workspace should be removed");
    }
}
