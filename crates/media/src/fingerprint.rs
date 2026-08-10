use std::{
    fs,
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
    VideoSequenceBuilder, VideoSequenceFingerprint, WorkspaceArea, WorkspaceError,
    select_video_sequence_timestamps, video_sequence_interval_ms,
};

const DEFAULT_MAX_FRAME_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_MAX_FRAME_PIXELS: u64 = 16_000_000;
const DEFAULT_MAX_FRAME_WORKING_BYTES: u64 = 128 * 1024 * 1024;

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

    pub fn timeout_duration(&self) -> Duration {
        self.timeout
    }

    pub fn with_decode_limits(mut self, decode_limits: FrameDecodeLimits) -> Self {
        self.decode_limits = decode_limits;
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
            .arg(format!("fps=1000/{interval_ms}:round=near"))
            .arg("-frames:v")
            .arg(expected_count.to_string())
            .arg("-an")
            .arg("-start_number")
            .arg("0")
            .arg("-f")
            .arg("image2")
            .arg(output_pattern.as_os_str())
            .timeout(self.timeout)
            .max_output_bytes(self.max_output_bytes);
        let decode_limits = self.decode_limits;
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
            let frame_paths = list_extracted_frames(&extraction_dir, expected_count).await?;
            let mut builder = VideoSequenceBuilder::new(duration_ms, interval_ms)?;
            for path in frame_paths {
                let decode_path = path.clone();
                let image =
                    tokio::task::spawn_blocking(move || decode_frame(&decode_path, decode_limits))
                        .await
                        .map_err(FrameExtractionError::TaskJoin)??;
                builder.push_image(&image)?;
            }
            Ok(builder.finish()?)
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

async fn list_extracted_frames(
    directory: &Path,
    expected_count: usize,
) -> Result<Vec<PathBuf>, FrameExtractionError> {
    let mut paths = vec![None; expected_count];
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
        let metadata = tokio::fs::symlink_metadata(&path)
            .await
            .map_err(|source| FrameExtractionError::OutputFile { path: path.clone(), source })?;
        if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() == 0 {
            return Err(FrameExtractionError::InvalidOutput { path });
        }
        if paths[index].replace(path.clone()).is_some() {
            return Err(FrameExtractionError::UnexpectedOutput { path });
        }
    }

    let actual_count = paths.iter().filter(|path| path.is_some()).count();
    if actual_count != expected_count {
        return Err(FrameExtractionError::InvalidFrameCount {
            expected: expected_count,
            actual: actual_count,
        });
    }
    Ok(paths.into_iter().map(Option::unwrap).collect())
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
                .expect("fps filter should be configured")[1],
            "fps=1000/500:round=near"
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
}
