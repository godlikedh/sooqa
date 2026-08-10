use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use image::{DynamicImage, ImageDecoder, ImageReader, Limits};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    CommandError, DEFAULT_MAX_OUTPUT_BYTES, ExternalCommand, ExternalCommandRunner, MediaWorkspace,
    VideoSequenceBuilder, VideoSequenceFingerprint, WorkspaceArea, WorkspaceError,
    command::sequence_directory_size, select_video_sequence_timestamps, video_sequence_interval_ms,
};

const DEFAULT_MAX_FRAME_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_MAX_FRAME_PIXELS: u64 = 16_000_000;
const DEFAULT_MAX_FRAME_WORKING_BYTES: u64 = 128 * 1024 * 1024;
pub const DEFAULT_MAX_FRAME_SEQUENCE_BYTES: u64 = 4 * 1024 * 1024 * 1024;

pub const VIDEO_SEQUENCE_V1: &str = "video_sequence_v1";

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
    max_sequence_bytes: u64,
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
            max_sequence_bytes: DEFAULT_MAX_FRAME_SEQUENCE_BYTES,
            decode_limits: FrameDecodeLimits::default(),
        }
    }

    pub fn timeout_duration(&self) -> Duration {
        self.timeout
    }

    pub fn with_decode_limits(mut self, decode_limits: FrameDecodeLimits) -> Self {
        self.decode_limits = decode_limits;
        self
    }

    pub fn with_max_sequence_bytes(mut self, max_sequence_bytes: u64) -> Self {
        self.max_sequence_bytes = max_sequence_bytes;
        self
    }

    pub async fn extract_video_sequence_from_area(
        &self,
        workspace: &MediaWorkspace,
        area: WorkspaceArea,
        input_name: &str,
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
        let timestamps = select_video_sequence_timestamps(duration_ms);
        let expected_count = timestamps.len();
        let extraction_name = format!(".sooqa-video-sequence-{}", Uuid::new_v4());
        let extraction_dir = workspace.path(WorkspaceArea::Frames, &extraction_name)?;
        tokio::fs::create_dir(&extraction_dir).await.map_err(|source| {
            FrameExtractionError::OutputDirectory { path: extraction_dir.clone(), source }
        })?;
        let extraction_guard = ExtractionDirectoryGuard::new(extraction_dir.clone());
        let output_pattern = extraction_dir.join("frame-%04d.png");
        let command = ExternalCommand::new(self.ffmpeg_executable.clone())
            .arg("-hide_banner")
            .arg("-loglevel")
            .arg("error")
            .arg("-nostdin")
            .arg("-i")
            .arg(input_path.as_os_str())
            .arg("-map")
            .arg("0:v:0")
            .arg("-vf")
            .arg(format!(
                "setpts=PTS-STARTPTS,select='isnan(prev_selected_pts)+gte(t,selected_n*{interval_ms}/1000)'"
            ))
            .arg("-frames:v")
            .arg(expected_count.to_string())
            .arg("-an")
            .arg("-fps_mode")
            .arg("vfr")
            .arg("-start_number")
            .arg("0")
            .arg("-f")
            .arg("image2")
            .arg(output_pattern.as_os_str())
            .timeout(self.timeout)
            .max_output_bytes(self.max_output_bytes);
        let decode_limits = self.decode_limits;
        let max_sequence_bytes = self.max_sequence_bytes;
        let sequence_config = SequenceExtractionConfig {
            duration_ms,
            interval_ms,
            expected_count,
            decode_limits,
            max_sequence_bytes,
            max_output_bytes: self.max_output_bytes,
        };
        let result = async {
            let actual = run_and_consume_sequence(
                Arc::clone(&self.runner),
                command,
                extraction_dir.clone(),
                sequence_config,
            )
            .await?;
            Ok(actual)
        }
        .await;
        match (result, extraction_guard.cleanup().await) {
            (Ok(fingerprint), Ok(())) => Ok(fingerprint),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Err(error), Err(_cleanup_error)) => Err(error),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct SequenceExtractionConfig {
    duration_ms: u64,
    interval_ms: u32,
    expected_count: usize,
    decode_limits: FrameDecodeLimits,
    max_sequence_bytes: u64,
    max_output_bytes: usize,
}

async fn run_and_consume_sequence(
    runner: Arc<dyn ExternalCommandRunner>,
    command: ExternalCommand,
    extraction_dir: PathBuf,
    config: SequenceExtractionConfig,
) -> Result<VideoSequenceFingerprint, FrameExtractionError> {
    let producer_done = Arc::new(AtomicBool::new(false));
    let producer_done_signal = Arc::clone(&producer_done);
    let producer_directory = extraction_dir.clone();
    let producer = async move {
        let result =
            runner.run_sequence(command, &producer_directory, config.max_sequence_bytes).await;
        producer_done_signal.store(true, Ordering::Release);
        result
    };
    let consumer = consume_sequence_frames(
        &extraction_dir,
        config.duration_ms,
        config.interval_ms,
        config.expected_count,
        config.decode_limits,
        config.max_sequence_bytes,
        Arc::clone(&producer_done),
    );
    tokio::pin!(producer);
    tokio::pin!(consumer);

    let (output, builder) = tokio::select! {
        output = &mut producer => {
            let output = output?;
            let builder = consumer.await?;
            (output, builder)
        }
        builder = &mut consumer => {
            let builder = builder?;
            let output = producer.await?;
            (output, builder)
        }
    };
    if output.stdout_truncated || output.stderr_truncated {
        return Err(FrameExtractionError::OutputLimitExceeded { limit: config.max_output_bytes });
    }
    if !output.success {
        return Err(FrameExtractionError::ProcessFailed {
            exit_code: output.exit_code,
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }
    Ok(builder.finish()?)
}

async fn consume_sequence_frames(
    directory: &Path,
    duration_ms: u64,
    interval_ms: u32,
    expected_count: usize,
    decode_limits: FrameDecodeLimits,
    max_sequence_bytes: u64,
    producer_done: Arc<AtomicBool>,
) -> Result<VideoSequenceBuilder, FrameExtractionError> {
    let mut builder = VideoSequenceBuilder::new(duration_ms, interval_ms)?;
    let mut index = 0;
    let mut finished_entries_validated = false;
    while index < expected_count {
        let producer_finished = producer_done.load(Ordering::Acquire);
        if producer_finished {
            if !finished_entries_validated {
                if sequence_directory_size(directory).await.map_err(|source| {
                    FrameExtractionError::OutputDirectory { path: directory.to_owned(), source }
                })? > max_sequence_bytes
                {
                    return Err(FrameExtractionError::SequenceOutputLimitExceeded {
                        limit: max_sequence_bytes,
                    });
                }
                reject_unexpected_sequence_entries(directory, expected_count).await?;
                finished_entries_validated = true;
            }
        } else {
            reject_unexpected_sequence_entries(directory, expected_count).await?;
            if sequence_directory_size(directory).await.map_err(|source| {
                FrameExtractionError::OutputDirectory { path: directory.to_owned(), source }
            })? > max_sequence_bytes
            {
                return Err(FrameExtractionError::SequenceOutputLimitExceeded {
                    limit: max_sequence_bytes,
                });
            }
        }
        let path = directory.join(format!("frame-{index:04}.png"));
        match consume_one_sequence_frame(&path, decode_limits, producer_finished).await? {
            Some(image) => {
                builder.push_image(&image)?;
                tokio::fs::remove_file(&path).await.map_err(|source| {
                    FrameExtractionError::OutputFile { path: path.clone(), source }
                })?;
                index += 1;
            }
            None => {
                if producer_finished {
                    return Err(FrameExtractionError::InvalidFrameCount {
                        expected: expected_count,
                        actual: index,
                    });
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
    }

    while !producer_done.load(Ordering::Acquire) {
        reject_unexpected_sequence_entries(directory, expected_count).await?;
        if sequence_directory_size(directory).await.map_err(|source| {
            FrameExtractionError::OutputDirectory { path: directory.to_owned(), source }
        })? > max_sequence_bytes
        {
            return Err(FrameExtractionError::SequenceOutputLimitExceeded {
                limit: max_sequence_bytes,
            });
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    if !finished_entries_validated {
        reject_unexpected_sequence_entries(directory, expected_count).await?;
    }
    if sequence_directory_size(directory).await.map_err(|source| {
        FrameExtractionError::OutputDirectory { path: directory.to_owned(), source }
    })? > max_sequence_bytes
    {
        return Err(FrameExtractionError::SequenceOutputLimitExceeded {
            limit: max_sequence_bytes,
        });
    }
    Ok(builder)
}

async fn consume_one_sequence_frame(
    path: &Path,
    decode_limits: FrameDecodeLimits,
    producer_done: bool,
) -> Result<Option<DynamicImage>, FrameExtractionError> {
    let first_metadata = match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(FrameExtractionError::OutputFile { path: path.to_owned(), source });
        }
    };
    if first_metadata.file_type().is_symlink() || !first_metadata.is_file() {
        return Err(FrameExtractionError::InvalidOutput { path: path.to_owned() });
    }
    if first_metadata.len() == 0 {
        return if producer_done {
            Err(FrameExtractionError::InvalidOutput { path: path.to_owned() })
        } else {
            Ok(None)
        };
    }
    if !producer_done {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let second_metadata = match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(FrameExtractionError::OutputFile { path: path.to_owned(), source });
        }
    };
    if second_metadata.file_type().is_symlink() || !second_metadata.is_file() {
        return Err(FrameExtractionError::InvalidOutput { path: path.to_owned() });
    }
    if first_metadata.len() != second_metadata.len() {
        return Ok(None);
    }
    let decode_path = path.to_owned();
    match tokio::task::spawn_blocking(move || decode_frame(&decode_path, decode_limits)).await {
        Ok(Ok(image)) => Ok(Some(image)),
        Ok(Err(error)) if !producer_done && is_retryable_incomplete_frame(&error) => Ok(None),
        Ok(Err(error)) => Err(error),
        Err(error) => Err(FrameExtractionError::TaskJoin(error)),
    }
}

fn is_retryable_incomplete_frame(error: &FrameExtractionError) -> bool {
    matches!(error, FrameExtractionError::InputFile { .. } | FrameExtractionError::Decode { .. })
}

async fn reject_unexpected_sequence_entries(
    directory: &Path,
    expected_count: usize,
) -> Result<(), FrameExtractionError> {
    let mut entries = tokio::fs::read_dir(directory).await.map_err(|source| {
        FrameExtractionError::OutputDirectory { path: directory.to_owned(), source }
    })?;
    while let Some(entry) = entries.next_entry().await.map_err(|source| {
        FrameExtractionError::OutputDirectory { path: directory.to_owned(), source }
    })? {
        let path = entry.path();
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(FrameExtractionError::UnexpectedOutput { path });
        };
        let Some(index_text) =
            name.strip_prefix("frame-").and_then(|name| name.strip_suffix(".png"))
        else {
            return Err(FrameExtractionError::UnexpectedOutput { path });
        };
        let Ok(index) = index_text.parse::<usize>() else {
            return Err(FrameExtractionError::UnexpectedOutput { path });
        };
        if index >= expected_count || format!("frame-{index:04}.png") != name {
            return Err(FrameExtractionError::UnexpectedOutput { path });
        }
    }
    Ok(())
}

struct ExtractionDirectoryGuard {
    path: PathBuf,
    armed: bool,
}

impl ExtractionDirectoryGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    async fn cleanup(mut self) -> Result<(), FrameExtractionError> {
        let result = match tokio::fs::symlink_metadata(&self.path).await {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                Err(FrameExtractionError::Workspace(WorkspaceError::Symlink(self.path.clone())))
            }
            Ok(metadata) if !metadata.is_dir() => Err(FrameExtractionError::Workspace(
                WorkspaceError::NotDirectory(self.path.clone()),
            )),
            Ok(_) => tokio::fs::remove_dir_all(&self.path).await.map_err(|source| {
                FrameExtractionError::OutputDirectory { path: self.path.clone(), source }
            }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => {
                Err(FrameExtractionError::OutputDirectory { path: self.path.clone(), source })
            }
        };
        if result.is_ok() {
            self.armed = false;
        }
        result
    }
}

impl Drop for ExtractionDirectoryGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Ok(metadata) = fs::symlink_metadata(&self.path) else { return };
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return;
        }
        let _ = fs::remove_dir_all(&self.path);
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
    #[error("video fingerprint frame sequence exceeded the {limit}-byte temporary-disk limit")]
    SequenceOutputLimitExceeded { limit: u64 },
    #[error("ffmpeg frame extraction failed with status {exit_code:?}: {stderr}")]
    ProcessFailed { exit_code: Option<i32>, stderr: String },
    #[error("could not inspect frame output {path}: {source}")]
    OutputFile { path: PathBuf, source: std::io::Error },
    #[error("could not inspect frame output directory {path}: {source}")]
    OutputDirectory { path: PathBuf, source: std::io::Error },
    #[error("frame output is missing, empty, or a symlink: {path}")]
    InvalidOutput { path: PathBuf },
    #[error("frame output sequence has {actual} files; expected {expected}")]
    InvalidFrameCount { expected: usize, actual: usize },
    #[error("frame output sequence contains an unexpected entry: {path}")]
    UnexpectedOutput { path: PathBuf },
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
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
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

    fn command_frame_count(command: &ExternalCommand) -> usize {
        command
            .args()
            .windows(2)
            .find_map(|args| (args[0] == "-frames:v").then(|| args[1].to_string_lossy()))
            .expect("frame count argument exists")
            .parse::<usize>()
            .expect("frame count should be numeric")
    }

    fn command_frame_path(command: &ExternalCommand, index: usize) -> PathBuf {
        let output_pattern = command.args().last().expect("output argument exists");
        PathBuf::from(output_pattern.to_string_lossy().replace("%04d", &format!("{index:04}")))
    }

    fn write_fake_frames(command: &ExternalCommand, count: usize) {
        let image = ImageBuffer::from_fn(18, 16, |x, y| {
            Rgb([x.saturating_mul(10) as u8, y.saturating_mul(10) as u8, 80])
        });
        for index in 0..count {
            let output = command_frame_path(command, index);
            DynamicImage::ImageRgb8(image.clone())
                .save_with_format(&output, image::ImageFormat::Png)
                .expect("fake ffmpeg should write a frame");
        }
    }

    #[async_trait]
    impl ExternalCommandRunner for RecordingRunner {
        async fn run(
            &self,
            command: ExternalCommand,
        ) -> Result<ExternalCommandOutput, CommandError> {
            write_fake_frames(&command, command_frame_count(&command));
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

    #[cfg(unix)]
    fn write_executable(path: &Path, contents: &str) {
        use std::os::unix::fs::PermissionsExt;

        std::fs::write(path, contents).expect("test executable should be written");
        let mut permissions = std::fs::metadata(path)
            .expect("test executable metadata should be readable")
            .permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(path, permissions).expect("test executable should be executable");
    }

    #[tokio::test]
    async fn sequence_extraction_is_single_pass_and_bounded() {
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
            .extract_video_sequence_from_area(
                &workspace,
                WorkspaceArea::Normalized,
                "canonical.mp4",
                10_000,
            )
            .await
            .expect("sequence should be extracted");
        assert_eq!(first.version.as_str(), VIDEO_SEQUENCE_V1);
        assert_eq!(first.interval_ms, 500);
        assert_eq!(first.samples.len(), 20);
        assert_eq!(runner.commands.lock().unwrap().len(), 1);
        assert_eq!(
            runner.commands.lock().unwrap()[0]
                .args()
                .windows(2)
                .find(|args| args[0] == "-vf")
                .expect("timestamp selector should be configured")[1],
            "setpts=PTS-STARTPTS,select='isnan(prev_selected_pts)+gte(t,selected_n*500/1000)'"
        );
        assert_eq!(
            runner.commands.lock().unwrap()[0]
                .args()
                .windows(2)
                .find(|args| args[0] == "-fps_mode")
                .expect("variable frame-rate output should be configured")[1],
            "vfr"
        );
        assert_eq!(
            runner.commands.lock().unwrap()[0]
                .args()
                .windows(2)
                .find(|args| args[0] == "-frames:v")
                .expect("frame cap should be configured")[1],
            "20"
        );
        assert_eq!(
            std::fs::read_dir(workspace.root().join("frames")).unwrap().count(),
            0,
            "the extraction directory should be removed after success"
        );

        let second = extractor
            .extract_video_sequence_from_area(
                &workspace,
                WorkspaceArea::Normalized,
                "canonical.mp4",
                10_000,
            )
            .await
            .expect("cached sequence should be decoded");
        assert_eq!(second, first);
        assert_eq!(runner.commands.lock().unwrap().len(), 2);
        assert_eq!(
            std::fs::read_dir(workspace.root().join("frames")).unwrap().count(),
            0,
            "the second extraction should also clean its directory"
        );
        workspace.cleanup().await.expect("workspace should be removed");
    }

    #[derive(Clone, Copy)]
    struct MissingFrameRunner;

    #[async_trait]
    impl ExternalCommandRunner for MissingFrameRunner {
        async fn run(
            &self,
            command: ExternalCommand,
        ) -> Result<ExternalCommandOutput, CommandError> {
            let count = command_frame_count(&command);
            write_fake_frames(&command, count.saturating_sub(1));
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

    #[derive(Clone, Copy)]
    struct UnexpectedOutputRunner;

    #[async_trait]
    impl ExternalCommandRunner for UnexpectedOutputRunner {
        async fn run(
            &self,
            command: ExternalCommand,
        ) -> Result<ExternalCommandOutput, CommandError> {
            write_fake_frames(&command, command_frame_count(&command));
            std::fs::write(
                command_frame_path(&command, command_frame_count(&command))
                    .with_file_name("unexpected.txt"),
                b"unexpected",
            )
            .expect("fake ffmpeg should write its unexpected output");
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

    #[derive(Clone, Copy)]
    struct TimedOutRunner;

    #[async_trait]
    impl ExternalCommandRunner for TimedOutRunner {
        async fn run(
            &self,
            command: ExternalCommand,
        ) -> Result<ExternalCommandOutput, CommandError> {
            Err(CommandError::TimedOut {
                program: command.program().to_owned(),
                timeout: command.timeout_duration(),
            })
        }
    }

    #[derive(Clone)]
    struct BlockingRunner {
        started: Arc<AtomicBool>,
    }

    #[async_trait]
    impl ExternalCommandRunner for BlockingRunner {
        async fn run(
            &self,
            _command: ExternalCommand,
        ) -> Result<ExternalCommandOutput, CommandError> {
            self.started.store(true, Ordering::Release);
            std::future::pending().await
        }
    }

    async fn workspace_with_input(stem: &str) -> (MediaWorkspace, PathBuf) {
        let workspace = MediaWorkspace::create(temp_path(stem), Uuid::new_v4())
            .await
            .expect("workspace should be created");
        let input = workspace.path(WorkspaceArea::Normalized, "canonical.mp4").unwrap();
        std::fs::write(&input, b"fake video").expect("fake input should be written");
        (workspace, input)
    }

    #[tokio::test]
    async fn malformed_sequence_output_is_rejected_and_cleaned() {
        for (stem, runner, expected_error) in [
            (
                "missing-frame",
                Arc::new(MissingFrameRunner) as Arc<dyn ExternalCommandRunner>,
                "frame output sequence has",
            ),
            (
                "unexpected-frame",
                Arc::new(UnexpectedOutputRunner) as Arc<dyn ExternalCommandRunner>,
                "frame output sequence contains",
            ),
        ] {
            let (workspace, _) = workspace_with_input(stem).await;
            let extractor = FrameExtractor::with_runner(
                "/usr/bin/ffmpeg",
                Duration::from_secs(5),
                1024,
                runner,
            );
            let error = extractor
                .extract_video_sequence_from_area(
                    &workspace,
                    WorkspaceArea::Normalized,
                    "canonical.mp4",
                    1_000,
                )
                .await
                .expect_err("malformed output should fail");
            assert!(error.to_string().starts_with(expected_error));
            assert_eq!(std::fs::read_dir(workspace.root().join("frames")).unwrap().count(), 0);
            workspace.cleanup().await.expect("workspace should be removed");
        }
    }

    #[tokio::test]
    async fn cancelled_sequence_extraction_removes_its_temporary_directory() {
        let (workspace, _) = workspace_with_input("cancelled-sequence").await;
        let started = Arc::new(AtomicBool::new(false));
        let extractor = FrameExtractor::with_runner(
            "/usr/bin/ffmpeg",
            Duration::from_secs(5),
            1024,
            Arc::new(BlockingRunner { started: Arc::clone(&started) }),
        );
        let workspace_for_task = workspace.clone();
        let task = tokio::spawn(async move {
            extractor
                .extract_video_sequence_from_area(
                    &workspace_for_task,
                    WorkspaceArea::Normalized,
                    "canonical.mp4",
                    1_000,
                )
                .await
        });
        for _ in 0..100 {
            if started.load(Ordering::Acquire) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(started.load(Ordering::Acquire), "runner should have started");
        task.abort();
        assert!(task.await.expect_err("task should be cancelled").is_cancelled());
        assert_eq!(
            std::fs::read_dir(workspace.root().join("frames")).unwrap().count(),
            0,
            "cancellation should remove the extraction directory"
        );
        workspace.cleanup().await.expect("workspace should be removed");
    }

    #[tokio::test]
    async fn timed_out_sequence_extraction_removes_its_temporary_directory() {
        let (workspace, _) = workspace_with_input("timed-out-sequence").await;
        let extractor = FrameExtractor::with_runner(
            "/usr/bin/ffmpeg",
            Duration::from_secs(5),
            1024,
            Arc::new(TimedOutRunner),
        );
        let error = extractor
            .extract_video_sequence_from_area(
                &workspace,
                WorkspaceArea::Normalized,
                "canonical.mp4",
                1_000,
            )
            .await
            .expect_err("timeout should fail");
        assert!(matches!(error, FrameExtractionError::Command(error) if error.is_timeout()));
        assert_eq!(std::fs::read_dir(workspace.root().join("frames")).unwrap().count(), 0);
        workspace.cleanup().await.expect("workspace should be removed");
    }

    #[tokio::test]
    async fn sequence_extraction_honors_the_2048_sample_cap() {
        let (workspace, _) = workspace_with_input("maximum-sequence").await;
        let runner = RecordingRunner::default();
        let extractor = FrameExtractor::with_runner(
            "/usr/bin/ffmpeg",
            Duration::from_secs(30),
            1024,
            Arc::new(runner.clone()),
        );
        let fingerprint = extractor
            .extract_video_sequence_from_area(
                &workspace,
                WorkspaceArea::Normalized,
                "canonical.mp4",
                2_000_000,
            )
            .await
            .expect("maximum sequence should be extracted");
        assert_eq!(fingerprint.samples.len(), 2_048);
        assert_eq!(runner.commands.lock().unwrap().len(), 1);
        assert_eq!(command_frame_count(&runner.commands.lock().unwrap()[0]), 2_048);
        assert_eq!(std::fs::read_dir(workspace.root().join("frames")).unwrap().count(), 0);
        workspace.cleanup().await.expect("workspace should be removed");
    }

    #[cfg(unix)]
    #[tokio::test]
    #[ignore = "requires ffmpeg; validates the video_sequence_v1 timestamp contract"]
    async fn non_aligned_rate_and_non_zero_pts_match_legacy_grid_sampling() {
        let workspace = MediaWorkspace::create(temp_path("non-aligned-golden"), Uuid::new_v4())
            .await
            .expect("workspace should be created");
        let input = workspace.path(WorkspaceArea::Normalized, "canonical.nut").unwrap();
        let raw_input = workspace.root().join("fixture.rgb");
        let (width, height) = (96_u32, 64_u32);
        let mut raw = Vec::with_capacity((width * height * 3 * 14) as usize);
        for frame in 0..14_u32 {
            for y in 0..height {
                for x in 0..width {
                    raw.extend_from_slice(&[
                        (frame.wrapping_mul(29).wrapping_add(x.wrapping_mul(3))) as u8,
                        (frame.wrapping_mul(17).wrapping_add(y.wrapping_mul(5))) as u8,
                        (x.wrapping_mul(7).wrapping_add(y.wrapping_mul(11))) as u8,
                    ]);
                }
            }
        }
        std::fs::write(&raw_input, &raw).expect("raw fixture should be written");
        let fixture = std::process::Command::new("ffmpeg")
            .args([
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "rawvideo",
                "-pix_fmt",
                "rgb24",
                "-s",
                "96x64",
                "-r",
                "7",
                "-i",
            ])
            .arg(&raw_input)
            .args([
                "-frames:v",
                "14",
                "-vf",
                "setpts=PTS+2/TB",
                "-c:v",
                "rawvideo",
                "-pix_fmt",
                "rgb24",
                "-f",
                "nut",
            ])
            .arg(&input)
            .status()
            .expect("ffmpeg fixture command should start");
        assert!(fixture.success(), "ffmpeg fixture command should succeed");

        let legacy_dir = workspace.root().join("legacy");
        std::fs::create_dir(&legacy_dir).expect("legacy output directory should be created");
        let mut legacy_builder = VideoSequenceBuilder::new(2_000, 500).unwrap();
        let raw_frame_bytes = (width * height * 3) as usize;
        for (index, (timestamp_ms, expected_frame)) in
            [(0_u64, 0_usize), (500, 4), (1_000, 7), (1_500, 11)].into_iter().enumerate()
        {
            let output = legacy_dir.join(format!("frame-{index:04}.png"));
            let seek = format!("{:.3}", timestamp_ms as f64 / 1_000.0);
            let result = std::process::Command::new("ffmpeg")
                .args(["-hide_banner", "-loglevel", "error", "-nostdin", "-i"])
                .arg(&input)
                .args(["-map", "0:v:0", "-ss"])
                .arg(seek)
                .args(["-frames:v", "1", "-an", "-f", "image2"])
                .arg(&output)
                .status()
                .expect("legacy ffmpeg sample command should start");
            assert!(result.success(), "legacy ffmpeg sample command should succeed");
            let image = decode_frame(&output, FrameDecodeLimits::default())
                .expect("legacy sample should decode");
            let expected_start = expected_frame * raw_frame_bytes;
            let expected_hash =
                crate::sha256_bytes(&raw[expected_start..expected_start + raw_frame_bytes]);
            let actual_hash = crate::sha256_bytes(&image.to_rgb8().into_raw());
            assert_eq!(actual_hash.sha256, expected_hash.sha256);
            legacy_builder.push_image(&image).unwrap();
        }
        let legacy = legacy_builder.finish().unwrap();

        let actual = FrameExtractor::new("ffmpeg", Duration::from_secs(30))
            .extract_video_sequence_from_area(
                &workspace,
                WorkspaceArea::Normalized,
                "canonical.nut",
                2_000,
            )
            .await
            .expect("single-pass extraction should succeed");

        // Every sample carries content-derived pHash, dHash, colour, information,
        // and transition values. Equality with the legacy per-grid extraction is
        // therefore a frame-content golden check at every timestamp.
        assert_eq!(actual.samples, legacy.samples);
        assert_eq!(actual.interval_ms, 500);
        assert_eq!(actual.samples.len(), 4);
        assert_eq!(std::fs::read_dir(workspace.root().join("frames")).unwrap().count(), 0);
        workspace.cleanup().await.expect("workspace should be removed");
    }

    #[cfg(unix)]
    #[tokio::test]
    #[ignore = "requires a POSIX shell; validates the producer-side sequence byte bound"]
    async fn real_process_runner_stops_a_sequence_when_output_limit_is_exceeded() {
        let workspace = MediaWorkspace::create(temp_path("producer-output-limit"), Uuid::new_v4())
            .await
            .expect("workspace should be created");
        let input = workspace.path(WorkspaceArea::Normalized, "canonical.mp4").unwrap();
        std::fs::write(&input, b"fake video").expect("fake input should be written");
        let script = temp_path("producer-output-limit-script");
        write_executable(
            &script,
            "#!/bin/sh\noutput=\nfor arg in \"$@\"; do output=\"$arg\"; done\ndirectory=$(dirname \"$output\")\ndd if=/dev/zero of=\"$directory/frame-0000.png\" bs=2048 count=1 2>/dev/null\nsleep 30\n",
        );

        let extractor =
            FrameExtractor::new(&script, Duration::from_secs(10)).with_max_sequence_bytes(1_024);
        let error = extractor
            .extract_video_sequence_from_area(
                &workspace,
                WorkspaceArea::Normalized,
                "canonical.mp4",
                1_000,
            )
            .await
            .expect_err("producer output should be stopped at the aggregate limit");
        assert!(matches!(
            error,
            FrameExtractionError::SequenceOutputLimitExceeded { limit: 1_024 }
                | FrameExtractionError::Command(CommandError::OutputLimitExceeded {
                    limit: 1_024,
                    ..
                })
        ));
        assert_eq!(std::fs::read_dir(workspace.root().join("frames")).unwrap().count(), 0);
        let _ = tokio::fs::remove_file(&script).await;
        workspace.cleanup().await.expect("workspace should be removed");
    }
}
