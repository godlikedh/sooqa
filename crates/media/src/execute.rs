use std::{
    future::Future,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::normalize::requires_frame_rate_cap;
use crate::publication::{TempArtifact, publish_or_reuse};
use crate::{
    AudioCodec, CanonicalContainer, CommandError, DEFAULT_MAX_OUTPUT_BYTES, ExternalCommandRunner,
    FfprobeAdapter, FileDigest, HashError, MediaProbe, MediaStreamKind, NormalizationPlan,
    PixelFormat, ProbeError, VideoCodec, sha256_file,
};

const MAX_PROGRESS_LINE_BYTES: usize = 4096;
const MAX_PROGRESS_LINES: usize = 4096;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FfmpegProgressState {
    Continue,
    End,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub struct FfmpegProgress {
    pub frame: Option<u64>,
    pub out_time_ms: Option<i64>,
    pub state: FfmpegProgressState,
}

pub fn parse_ffmpeg_progress(output: &[u8]) -> Result<FfmpegProgress, ProgressError> {
    let output = std::str::from_utf8(output).map_err(|_| ProgressError::InvalidUtf8)?;
    let mut frame = None;
    let mut out_time_ms = None;
    let mut state = None;
    let mut line_count = 0;

    for line in output.lines() {
        line_count += 1;
        if line_count > MAX_PROGRESS_LINES {
            return Err(ProgressError::TooManyLines { limit: MAX_PROGRESS_LINES });
        }
        if line.len() > MAX_PROGRESS_LINE_BYTES {
            return Err(ProgressError::LineTooLong { limit: MAX_PROGRESS_LINE_BYTES });
        }
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            return Err(ProgressError::InvalidLine(line.to_owned()));
        };
        match key {
            "frame" => frame = parse_optional_u64(value, "frame")?,
            "out_time_ms" => out_time_ms = parse_optional_i64(value, "out_time_ms")?,
            "progress" => {
                state = Some(match value {
                    "continue" => FfmpegProgressState::Continue,
                    "end" => FfmpegProgressState::End,
                    _ => {
                        return Err(ProgressError::InvalidValue {
                            field: "progress",
                            value: value.to_owned(),
                        });
                    }
                });
            }
            _ => {}
        }
    }

    match state {
        Some(state) => Ok(FfmpegProgress { frame, out_time_ms, state }),
        None => Err(ProgressError::MissingState),
    }
}

fn parse_optional_u64(value: &str, field: &'static str) -> Result<Option<u64>, ProgressError> {
    if value == "N/A" {
        return Ok(None);
    }
    value
        .parse()
        .map(Some)
        .map_err(|_| ProgressError::InvalidValue { field, value: value.to_owned() })
}

fn parse_optional_i64(value: &str, field: &'static str) -> Result<Option<i64>, ProgressError> {
    if value == "N/A" {
        return Ok(None);
    }
    value
        .parse()
        .map(Some)
        .map_err(|_| ProgressError::InvalidValue { field, value: value.to_owned() })
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NormalizationResult {
    pub output_path: PathBuf,
    pub progress: FfmpegProgress,
    pub probe: MediaProbe,
    pub digest: FileDigest,
}

#[derive(Clone)]
pub struct FfmpegExecutor {
    runner: Arc<dyn ExternalCommandRunner>,
    ffprobe: FfprobeAdapter,
    timeout: Duration,
    max_output_bytes: usize,
}

impl FfmpegExecutor {
    pub fn new(
        runner: Arc<dyn ExternalCommandRunner>,
        ffprobe_executable: impl Into<PathBuf>,
        timeout: Duration,
    ) -> Self {
        let max_output_bytes = DEFAULT_MAX_OUTPUT_BYTES;
        let ffprobe = FfprobeAdapter::with_runner(
            ffprobe_executable,
            timeout,
            max_output_bytes,
            Arc::clone(&runner),
        );
        Self { runner, ffprobe, timeout, max_output_bytes }
    }

    pub fn with_runner(
        runner: Arc<dyn ExternalCommandRunner>,
        ffprobe: FfprobeAdapter,
        timeout: Duration,
        max_output_bytes: usize,
    ) -> Self {
        Self { runner, ffprobe, timeout, max_output_bytes }
    }

    pub fn timeout_duration(&self) -> Duration {
        self.timeout
    }

    pub async fn execute<F>(
        &self,
        plan: &NormalizationPlan,
        cancellation: F,
    ) -> Result<NormalizationResult, NormalizationExecutionError>
    where
        F: Future<Output = ()> + Send,
    {
        let output_path = plan.output().to_owned();
        let temporary_path = temporary_output_path(&output_path);
        let mut temporary = TempArtifact::reserve(temporary_path).await.map_err(|source| {
            NormalizationExecutionError::TemporaryOutput { path: output_path.clone(), source }
        })?;

        let result = self.execute_inner(plan, temporary.path(), cancellation).await;
        match result {
            Ok((progress, probe, digest)) => {
                publish_or_reuse(temporary.path(), &output_path).await.map_err(|error| {
                    NormalizationExecutionError::OutputPublish {
                        path: output_path.clone(),
                        message: error.to_string(),
                    }
                })?;
                temporary.remove().await;
                Ok(NormalizationResult { output_path, progress, probe, digest })
            }
            Err(error) => Err(error),
        }
    }

    async fn execute_inner<F>(
        &self,
        plan: &NormalizationPlan,
        output_path: &Path,
        cancellation: F,
    ) -> Result<(FfmpegProgress, MediaProbe, FileDigest), NormalizationExecutionError>
    where
        F: Future<Output = ()> + Send,
    {
        let command = plan
            .command_with_progress_for_output(output_path)
            .timeout(self.timeout)
            .max_output_bytes(self.max_output_bytes);
        tokio::pin!(cancellation);
        let command_output = tokio::select! {
            result = self.runner.run(command) => result.map_err(NormalizationExecutionError::Command)?,
            _ = &mut cancellation => return Err(NormalizationExecutionError::Cancelled),
        };

        if !command_output.success {
            return Err(NormalizationExecutionError::ProcessFailed {
                exit_code: command_output.exit_code,
                stderr: bounded_text(&command_output.stderr),
            });
        }
        if command_output.stderr_truncated {
            return Err(NormalizationExecutionError::OutputLimitExceeded {
                limit: self.max_output_bytes,
            });
        }
        // Progress is advisory. The stream remains bounded and fully drained by
        // the command runner; a successful process with truncated progress is
        // accepted only after the output is validated below.
        let progress = if command_output.stdout_truncated {
            FfmpegProgress { frame: None, out_time_ms: None, state: FfmpegProgressState::End }
        } else {
            let progress = parse_ffmpeg_progress(&command_output.stdout)?;
            if progress.state != FfmpegProgressState::End {
                return Err(NormalizationExecutionError::ProgressDidNotEnd);
            }
            progress
        };

        let metadata = tokio::fs::metadata(output_path).await.map_err(|source| {
            NormalizationExecutionError::OutputFile { path: output_path.to_owned(), source }
        })?;
        if !metadata.is_file() || metadata.len() == 0 {
            return Err(NormalizationExecutionError::InvalidOutput {
                path: output_path.to_owned(),
            });
        }

        let probe = self.ffprobe.probe(output_path).await?;
        validate_output_probe(&probe, plan.profile())?;
        let digest = sha256_file(output_path).await?;
        Ok((progress, probe, digest))
    }
}

fn temporary_output_path(output: &Path) -> PathBuf {
    let file_name = match output.extension().and_then(|extension| extension.to_str()) {
        Some(extension) => format!(".sooqa-normalize-{}.{}", Uuid::new_v4(), extension),
        None => format!(".sooqa-normalize-{}.tmp", Uuid::new_v4()),
    };
    output.with_file_name(file_name)
}

fn validate_output_probe(
    probe: &MediaProbe,
    profile: crate::CanonicalVideoProfile,
) -> Result<(), NormalizationExecutionError> {
    let is_mp4 = matches!(profile.container, CanonicalContainer::Mp4)
        && probe.container_format.as_deref().is_some_and(|value| {
            value.split(',').any(|format| format.trim().eq_ignore_ascii_case("mp4"))
        });
    if !is_mp4 {
        return Err(NormalizationExecutionError::InvalidOutputFormat {
            format: probe.container_format.clone(),
        });
    }
    let video = probe
        .streams
        .iter()
        .find(|stream| stream.kind == MediaStreamKind::Video)
        .ok_or(NormalizationExecutionError::OutputHasNoVideo)?;
    if video.codec.as_deref() != Some(VideoCodec::H264.probe_name()) {
        return Err(invalid_output_profile("video codec is not H.264"));
    }
    if video.pixel_format.as_deref() != Some(PixelFormat::Yuv420p.ffmpeg_name()) {
        return Err(invalid_output_profile("video pixel format is not yuv420p"));
    }
    if video
        .width
        .zip(video.height)
        .is_none_or(|(width, height)| width > profile.max_width || height > profile.max_height)
    {
        return Err(invalid_output_profile("video dimensions exceed the canonical profile"));
    }
    if video.rotation_degrees.unwrap_or_default() != 0
        || requires_frame_rate_cap(video, profile.max_frame_rate)
    {
        return Err(invalid_output_profile(
            "video rotation or frame rate exceeds the canonical profile",
        ));
    }
    if probe
        .streams
        .iter()
        .filter(|stream| stream.kind == MediaStreamKind::Audio)
        .any(|stream| stream.codec.as_deref() != Some(AudioCodec::Aac.probe_name()))
    {
        return Err(invalid_output_profile("audio codec is not AAC"));
    }
    Ok(())
}

fn invalid_output_profile(message: &'static str) -> NormalizationExecutionError {
    NormalizationExecutionError::InvalidOutputProfile { message }
}

fn bounded_text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).trim().to_owned()
}

#[derive(Debug, Clone, Eq, PartialEq, Error)]
pub enum ProgressError {
    #[error("ffmpeg progress output was not valid UTF-8")]
    InvalidUtf8,
    #[error("ffmpeg progress output line exceeded the {limit}-byte limit")]
    LineTooLong { limit: usize },
    #[error("ffmpeg progress output exceeded the {limit}-line limit")]
    TooManyLines { limit: usize },
    #[error("ffmpeg progress output line is invalid: {0}")]
    InvalidLine(String),
    #[error("ffmpeg progress field {field} contained invalid value {value:?}")]
    InvalidValue { field: &'static str, value: String },
    #[error("ffmpeg progress output did not contain a progress state")]
    MissingState,
}

#[derive(Debug, Error)]
pub enum NormalizationExecutionError {
    #[error("ffmpeg command failed: {0}")]
    Command(#[source] CommandError),
    #[error("normalization was cancelled")]
    Cancelled,
    #[error("ffmpeg output exceeded the {limit}-byte capture limit")]
    OutputLimitExceeded { limit: usize },
    #[error("ffmpeg exited unsuccessfully with status {exit_code:?}: {stderr}")]
    ProcessFailed { exit_code: Option<i32>, stderr: String },
    #[error("could not parse ffmpeg progress: {0}")]
    Progress(#[from] ProgressError),
    #[error("ffmpeg progress did not end successfully")]
    ProgressDidNotEnd,
    #[error("could not inspect normalized output {path}: {source}")]
    OutputFile { path: PathBuf, source: std::io::Error },
    #[error("could not reserve temporary normalized output {path}: {source}")]
    TemporaryOutput { path: PathBuf, source: std::io::Error },
    #[error("could not publish normalized output {path}: {message}")]
    OutputPublish { path: PathBuf, message: String },
    #[error("normalized output is missing or empty: {path}")]
    InvalidOutput { path: PathBuf },
    #[error("normalized output has an unsupported container: {format:?}")]
    InvalidOutputFormat { format: Option<String> },
    #[error("normalized output contains no video stream")]
    OutputHasNoVideo,
    #[error("normalized output does not satisfy the canonical profile: {message}")]
    InvalidOutputProfile { message: &'static str },
    #[error("could not validate normalized output: {0}")]
    Probe(#[from] ProbeError),
    #[error("could not hash normalized output: {0}")]
    Hash(#[from] HashError),
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    use async_trait::async_trait;
    use tokio::time::sleep;

    use super::*;
    use crate::{
        CanonicalVideoProfile, ExternalCommand, ExternalCommandOutput, FrameRate, MediaStream,
        NormalizationPlanner,
    };

    const PROBE_JSON: &[u8] = br#"{
      "streams": [{
        "index": 0,
        "codec_type": "video",
        "codec_name": "h264",
        "profile": "High",
        "pix_fmt": "yuv420p",
        "width": 320,
        "height": 240,
        "avg_frame_rate": "25/1",
        "bit_rate": "100000"
      }],
      "format": {
        "format_name": "mov,mp4,m4a,3gp,3g2,mj2",
        "duration": "1.000000",
        "size": "8",
        "bit_rate": "100000"
      }
    }"#;

    #[derive(Clone)]
    struct SequenceRunner {
        commands: Arc<Mutex<Vec<ExternalCommand>>>,
        outputs: Arc<Mutex<VecDeque<Result<ExternalCommandOutput, CommandError>>>>,
    }

    impl SequenceRunner {
        fn new(outputs: Vec<Result<ExternalCommandOutput, CommandError>>) -> Self {
            Self {
                commands: Arc::new(Mutex::new(Vec::new())),
                outputs: Arc::new(Mutex::new(outputs.into_iter().collect())),
            }
        }
    }

    #[async_trait]
    impl ExternalCommandRunner for SequenceRunner {
        async fn run(
            &self,
            command: ExternalCommand,
        ) -> Result<ExternalCommandOutput, CommandError> {
            self.commands.lock().expect("commands mutex should not be poisoned").push(command);
            let output = self
                .outputs
                .lock()
                .expect("outputs mutex should not be poisoned")
                .pop_front()
                .expect("test runner should have an output")?;
            let output_path = {
                let commands = self.commands.lock().expect("commands mutex should not be poisoned");
                let command = commands.last().expect("command should have been recorded");
                command.args().iter().any(|argument| argument == "-progress").then(|| {
                    PathBuf::from(command.args().last().expect("ffmpeg output should be present"))
                })
            };
            if let Some(output_path) = output_path {
                tokio::fs::write(output_path, b"canonical output")
                    .await
                    .expect("fake ffmpeg should write its output");
            }
            Ok(output)
        }
    }

    #[derive(Clone, Copy)]
    struct BlockingRunner;

    #[async_trait]
    impl ExternalCommandRunner for BlockingRunner {
        async fn run(
            &self,
            _command: ExternalCommand,
        ) -> Result<ExternalCommandOutput, CommandError> {
            std::future::pending().await
        }
    }

    fn planner() -> NormalizationPlanner {
        NormalizationPlanner::new("ffmpeg", CanonicalVideoProfile::default())
            .expect("default profile should be valid")
    }

    fn probe() -> MediaProbe {
        MediaProbe {
            container_format: Some("mp4".to_owned()),
            duration_ms: Some(1_000),
            size_bytes: 8,
            bit_rate: Some(100_000),
            streams: vec![MediaStream {
                index: 0,
                kind: MediaStreamKind::Video,
                codec: Some("h264".to_owned()),
                codec_tag: Some("avc1".to_owned()),
                codec_mime: Some("avc1.640028".to_owned()),
                level: Some(40),
                profile: Some("High".to_owned()),
                pixel_format: Some("yuv420p".to_owned()),
                width: Some(320),
                height: Some(240),
                display_aspect_ratio: Some("4:3".to_owned()),
                frame_rate: Some(FrameRate { numerator: 25, denominator: 1 }),
                rotation_degrees: Some(0),
                sample_rate_hz: None,
                channels: None,
                bit_rate: Some(100_000),
            }],
        }
    }

    fn success(stdout: &[u8]) -> Result<ExternalCommandOutput, CommandError> {
        Ok(ExternalCommandOutput {
            success: true,
            exit_code: Some(0),
            stdout: stdout.to_vec(),
            stderr: Vec::new(),
            stdout_truncated: false,
            stderr_truncated: false,
        })
    }

    #[test]
    fn parses_bounded_progress_and_keeps_last_values() {
        let progress = parse_ffmpeg_progress(
            b"frame=12\nout_time_ms=400000\nprogress=continue\nframe=24\nout_time_ms=800000\nprogress=end\n",
        )
        .expect("progress should parse");
        assert_eq!(progress.frame, Some(24));
        assert_eq!(progress.out_time_ms, Some(800000));
        assert_eq!(progress.state, FfmpegProgressState::End);
    }

    #[test]
    fn rejects_unbounded_progress_lines_and_missing_final_state() {
        assert!(matches!(
            parse_ffmpeg_progress(&vec![b'x'; MAX_PROGRESS_LINE_BYTES + 1]),
            Err(ProgressError::LineTooLong { .. })
        ));
        assert_eq!(parse_ffmpeg_progress(b"frame=1\n"), Err(ProgressError::MissingState));
    }

    #[tokio::test]
    async fn executes_plan_probes_output_and_hashes_it() {
        let output_path =
            std::env::temp_dir().join(format!("sooqa-normalized-{}.mp4", uuid::Uuid::new_v4()));
        let runner = Arc::new(SequenceRunner::new(vec![
            success(b"frame=25\nout_time_ms=1000000\nprogress=end\n"),
            success(PROBE_JSON),
            success(b"frame=25\nout_time_ms=1000000\nprogress=end\n"),
            success(PROBE_JSON),
        ]));
        let ffprobe = FfprobeAdapter::with_runner(
            "ffprobe",
            Duration::from_secs(10),
            DEFAULT_MAX_OUTPUT_BYTES,
            Arc::clone(&runner) as Arc<dyn ExternalCommandRunner>,
        );
        let executor = FfmpegExecutor::with_runner(
            Arc::clone(&runner) as Arc<dyn ExternalCommandRunner>,
            ffprobe,
            Duration::from_secs(10),
            DEFAULT_MAX_OUTPUT_BYTES,
        );
        let plan = planner().plan("input.mp4", &output_path, &probe()).expect("plan should build");

        let result = executor
            .execute(&plan, std::future::pending())
            .await
            .expect("execution should succeed");
        assert_eq!(result.output_path, output_path);
        assert_eq!(result.progress.state, FfmpegProgressState::End);
        assert_eq!(result.probe.container_format.as_deref(), Some("mov,mp4,m4a,3gp,3g2,mj2"));
        assert_eq!(result.digest.bytes, b"canonical output".len() as u64);
        let replayed = executor
            .execute(&plan, std::future::pending())
            .await
            .expect("retry should reuse the validated published output");
        assert_eq!(replayed.digest, result.digest);
        assert_eq!(runner.commands.lock().expect("commands mutex should not be poisoned").len(), 4);
        let ffmpeg_args = runner.commands.lock().expect("commands mutex should not be poisoned")[0]
            .args()
            .iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(ffmpeg_args.windows(2).any(|pair| pair == ["-progress", "pipe:1"]));
        tokio::fs::remove_file(output_path).await.expect("output should be removed");
    }

    #[tokio::test]
    async fn truncated_progress_is_non_fatal_for_a_successful_valid_output() {
        let output_path = std::env::temp_dir()
            .join(format!("sooqa-truncated-progress-{}.mp4", uuid::Uuid::new_v4()));
        let runner = Arc::new(SequenceRunner::new(vec![
            Ok(ExternalCommandOutput {
                success: true,
                exit_code: Some(0),
                stdout: vec![b'x'; DEFAULT_MAX_OUTPUT_BYTES + 1],
                stderr: Vec::new(),
                stdout_truncated: true,
                stderr_truncated: false,
            }),
            success(PROBE_JSON),
        ]));
        let ffprobe = FfprobeAdapter::with_runner(
            "ffprobe",
            Duration::from_secs(10),
            DEFAULT_MAX_OUTPUT_BYTES,
            Arc::clone(&runner) as Arc<dyn ExternalCommandRunner>,
        );
        let executor = FfmpegExecutor::with_runner(
            Arc::clone(&runner) as Arc<dyn ExternalCommandRunner>,
            ffprobe,
            Duration::from_secs(10),
            DEFAULT_MAX_OUTPUT_BYTES,
        );
        let plan = planner().plan("input.mp4", &output_path, &probe()).expect("plan should build");

        let result = executor
            .execute(&plan, std::future::pending())
            .await
            .expect("valid output should survive truncated progress capture");
        assert_eq!(result.progress.state, FfmpegProgressState::End);
        assert_eq!(result.digest.bytes, b"canonical output".len() as u64);
        tokio::fs::remove_file(output_path).await.expect("output should be removed");
    }

    #[tokio::test]
    async fn truncated_stderr_remains_fatal_after_a_successful_process() {
        let output_path = std::env::temp_dir()
            .join(format!("sooqa-truncated-stderr-{}.mp4", uuid::Uuid::new_v4()));
        let runner = Arc::new(SequenceRunner::new(vec![Ok(ExternalCommandOutput {
            success: true,
            exit_code: Some(0),
            stdout: b"frame=1\nout_time_ms=1000\nprogress=end\n".to_vec(),
            stderr: vec![b'x'; DEFAULT_MAX_OUTPUT_BYTES + 1],
            stdout_truncated: false,
            stderr_truncated: true,
        })]));
        let ffprobe = FfprobeAdapter::with_runner(
            "ffprobe",
            Duration::from_secs(10),
            DEFAULT_MAX_OUTPUT_BYTES,
            Arc::clone(&runner) as Arc<dyn ExternalCommandRunner>,
        );
        let executor = FfmpegExecutor::with_runner(
            Arc::clone(&runner) as Arc<dyn ExternalCommandRunner>,
            ffprobe,
            Duration::from_secs(10),
            DEFAULT_MAX_OUTPUT_BYTES,
        );
        let plan = planner().plan("input.mp4", &output_path, &probe()).expect("plan should build");

        let error = executor
            .execute(&plan, std::future::pending())
            .await
            .expect_err("truncated stderr should remain fatal");
        assert!(matches!(error, NormalizationExecutionError::OutputLimitExceeded { .. }));
        assert!(!output_path.exists());
    }

    #[tokio::test]
    async fn rejects_output_that_does_not_match_the_canonical_profile() {
        let output_path = std::env::temp_dir()
            .join(format!("sooqa-invalid-normalized-{}.mp4", uuid::Uuid::new_v4()));
        let invalid_probe = br#"{
          "streams": [{
            "index": 0,
            "codec_type": "video",
            "codec_name": "vp9",
            "pix_fmt": "yuv420p",
            "width": 320,
            "height": 240,
            "avg_frame_rate": "25/1"
          }],
          "format": {"format_name": "mp4", "duration": "1.0", "size": "16"}
        }"#;
        let runner = Arc::new(SequenceRunner::new(vec![
            success(b"frame=25\nout_time_ms=1000000\nprogress=end\n"),
            success(invalid_probe),
        ]));
        let ffprobe = FfprobeAdapter::with_runner(
            "ffprobe",
            Duration::from_secs(10),
            DEFAULT_MAX_OUTPUT_BYTES,
            Arc::clone(&runner) as Arc<dyn ExternalCommandRunner>,
        );
        let executor = FfmpegExecutor::with_runner(
            Arc::clone(&runner) as Arc<dyn ExternalCommandRunner>,
            ffprobe,
            Duration::from_secs(10),
            DEFAULT_MAX_OUTPUT_BYTES,
        );
        let plan = planner().plan("input.mp4", &output_path, &probe()).expect("plan should build");

        let error = executor
            .execute(&plan, std::future::pending())
            .await
            .expect_err("invalid canonical output should be rejected");
        assert!(matches!(error, NormalizationExecutionError::InvalidOutputProfile { .. }));
        let _ = tokio::fs::remove_file(output_path).await;
    }

    #[tokio::test]
    async fn cancellation_stops_waiting_for_ffmpeg() {
        let runner: Arc<dyn ExternalCommandRunner> = Arc::new(BlockingRunner);
        let ffprobe = FfprobeAdapter::with_runner(
            "ffprobe",
            Duration::from_secs(10),
            DEFAULT_MAX_OUTPUT_BYTES,
            Arc::clone(&runner),
        );
        let executor = FfmpegExecutor::with_runner(
            Arc::clone(&runner),
            ffprobe,
            Duration::from_secs(10),
            DEFAULT_MAX_OUTPUT_BYTES,
        );
        let root = std::env::temp_dir().join(format!("sooqa-cancel-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&root).await.expect("test root should be created");
        let output = root.join("canonical.mp4");
        let plan = planner().plan("input.mp4", &output, &probe()).expect("plan should build");

        let error = executor
            .execute(&plan, sleep(Duration::from_millis(10)))
            .await
            .expect_err("cancellation should stop execution");
        assert!(matches!(error, NormalizationExecutionError::Cancelled));
        assert!(!output.exists());
        let mut entries = tokio::fs::read_dir(&root).await.expect("test root should be readable");
        while let Some(entry) = entries.next_entry().await.expect("directory should be readable") {
            let name = entry.file_name().to_string_lossy().into_owned();
            assert!(
                !name.starts_with(".sooqa-normalize-"),
                "ffmpeg temporary output was left behind"
            );
        }
        tokio::fs::remove_dir_all(root).await.expect("test root should be removed");
    }

    #[tokio::test]
    async fn process_failure_is_reported_without_probing_or_hashing() {
        let runner = Arc::new(SequenceRunner::new(vec![Ok(ExternalCommandOutput {
            success: false,
            exit_code: Some(1),
            stdout: Vec::new(),
            stderr: b"encoder failed".to_vec(),
            stdout_truncated: false,
            stderr_truncated: false,
        })]));
        let ffprobe = FfprobeAdapter::with_runner(
            "ffprobe",
            Duration::from_secs(10),
            DEFAULT_MAX_OUTPUT_BYTES,
            Arc::clone(&runner) as Arc<dyn ExternalCommandRunner>,
        );
        let executor = FfmpegExecutor::with_runner(
            Arc::clone(&runner) as Arc<dyn ExternalCommandRunner>,
            ffprobe,
            Duration::from_secs(10),
            DEFAULT_MAX_OUTPUT_BYTES,
        );
        let output =
            std::env::temp_dir().join(format!("sooqa-failure-{}.mp4", uuid::Uuid::new_v4()));
        let plan = planner().plan("input.mp4", &output, &probe()).expect("plan should build");

        let error = executor
            .execute(&plan, std::future::pending())
            .await
            .expect_err("failure should be reported");
        assert!(matches!(
            error,
            NormalizationExecutionError::ProcessFailed { exit_code: Some(1), .. }
        ));
        assert!(!output.exists());
    }

    #[tokio::test]
    #[ignore = "requires ffmpeg and ffprobe installed on the test host"]
    async fn executes_generated_mp4_with_real_ffmpeg_and_ffprobe() {
        let input =
            std::env::temp_dir().join(format!("sooqa-generated-{}.mp4", uuid::Uuid::new_v4()));
        let output =
            std::env::temp_dir().join(format!("sooqa-canonical-{}.mp4", uuid::Uuid::new_v4()));
        let runner: Arc<dyn ExternalCommandRunner> = Arc::new(crate::ProcessCommandRunner);
        let generated = ExternalCommand::new("ffmpeg")
            .arg("-hide_banner")
            .arg("-loglevel")
            .arg("error")
            .arg("-nostdin")
            .arg("-y")
            .arg("-f")
            .arg("lavfi")
            .arg("-i")
            .arg("testsrc=size=320x240:rate=25")
            .arg("-t")
            .arg("1")
            .arg("-c:v")
            .arg("libx264")
            .arg("-pix_fmt")
            .arg("yuv420p")
            .arg("-an")
            .arg(&input);
        runner.run(generated).await.expect("generated fixture should be created");

        let ffprobe = FfprobeAdapter::with_runner(
            "ffprobe",
            Duration::from_secs(30),
            DEFAULT_MAX_OUTPUT_BYTES,
            Arc::clone(&runner),
        );
        let input_probe = ffprobe.probe(&input).await.expect("generated input should probe");
        let planner = NormalizationPlanner::new("ffmpeg", CanonicalVideoProfile::default())
            .expect("default profile should be valid");
        let plan = planner.plan(&input, &output, &input_probe).expect("plan should build");
        let executor = FfmpegExecutor::with_runner(
            Arc::clone(&runner),
            ffprobe,
            Duration::from_secs(30),
            DEFAULT_MAX_OUTPUT_BYTES,
        );
        let result = executor
            .execute(&plan, std::future::pending())
            .await
            .expect("generated media should normalize");
        assert!(result.digest.bytes > 0);
        assert_eq!(result.progress.state, FfmpegProgressState::End);
        assert!(result.probe.streams.iter().any(|stream| stream.kind == MediaStreamKind::Video));

        let _ = tokio::fs::remove_file(input).await;
        let _ = tokio::fs::remove_file(output).await;
    }
}
