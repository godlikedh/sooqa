use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;
use uuid::Uuid;

use crate::publication::{PublishOutcome, TempDirectory, publish_or_reuse};
use crate::{
    CommandError, DEFAULT_COMMAND_PATH, DEFAULT_MAX_OUTPUT_BYTES, DownloadError, DownloadLimits,
    DownloadedSource, ExternalCommand, ExternalCommandOutput, ExternalCommandRunner,
    SourceDownloader, SourceInput, SourceInspection, SourceMediaKind,
};

const DEFAULT_DENO_PATH: &str = "deno";
const MIN_SUPPORTED_DENO_VERSION: (u32, u32, u32) = (2, 3, 0);
const YTDLP_ATTEMPT_MAX_BYTES_MULTIPLIER: u64 = 3;
pub const DEFAULT_YTDLP_METADATA_MAX_OUTPUT_BYTES: usize = 4 * 1024 * 1024;
const RUNTIME_FIXTURE: &[u8] = br#"{
  "id": "sooqa-runtime-fixture",
  "title": "sooqa runtime fixture",
  "extractor": "generic",
  "webpage_url": "https://example.test/sooqa-runtime-fixture",
  "url": "https://example.test/sooqa-runtime-fixture.mp4",
  "ext": "mp4",
  "vcodec": "h264",
  "acodec": "none"
}
"#;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct YtDlpConfig {
    executable: PathBuf,
    format_selection: String,
    deno_path: PathBuf,
    max_output_bytes: usize,
    metadata_max_output_bytes: usize,
}

impl YtDlpConfig {
    pub fn new(
        executable: impl Into<PathBuf>,
        format_selection: impl Into<String>,
    ) -> Result<Self, YtDlpConfigError> {
        let format_selection = format_selection.into();
        validate_format_selection(&format_selection)?;
        Ok(Self {
            executable: executable.into(),
            format_selection,
            deno_path: PathBuf::from(DEFAULT_DENO_PATH),
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
            metadata_max_output_bytes: DEFAULT_YTDLP_METADATA_MAX_OUTPUT_BYTES,
        })
    }

    pub fn with_deno_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.deno_path = path.into();
        self
    }

    pub fn deno_path(&self) -> &Path {
        &self.deno_path
    }

    pub fn with_max_output_bytes(mut self, max_output_bytes: usize) -> Self {
        self.max_output_bytes = max_output_bytes;
        self
    }

    pub fn with_metadata_max_output_bytes(mut self, max_output_bytes: usize) -> Self {
        self.metadata_max_output_bytes = max_output_bytes;
        self
    }
}

pub fn is_supported_deno_version(version_line: &str) -> bool {
    let Some(version) = version_line
        .split_whitespace()
        .find(|part| part.chars().next().is_some_and(|character| character.is_ascii_digit()))
    else {
        return false;
    };
    let mut components = version.split('.').map(|component| component.parse::<u32>().ok());
    let Some(major) = components.next().flatten() else { return false };
    let Some(minor) = components.next().flatten() else { return false };
    let patch = components.next().flatten().unwrap_or(0);
    (major, minor, patch) >= MIN_SUPPORTED_DENO_VERSION
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum YtDlpConfigError {
    #[error("yt-dlp format selection must not be empty")]
    EmptyFormatSelection,
    #[error("yt-dlp format selection contains an unsafe control character or option prefix")]
    UnsafeFormatSelection,
}

#[derive(Clone)]
pub struct YtDlpDownloader {
    config: YtDlpConfig,
    inspection_limits: DownloadLimits,
    runner: Arc<dyn ExternalCommandRunner>,
}

impl YtDlpDownloader {
    pub fn new(config: YtDlpConfig) -> Self {
        Self::with_limits(config, DownloadLimits::default())
    }

    pub fn with_limits(config: YtDlpConfig, inspection_limits: DownloadLimits) -> Self {
        Self::with_runner(config, inspection_limits, Arc::new(crate::ProcessCommandRunner))
    }

    pub fn with_runner(
        config: YtDlpConfig,
        inspection_limits: DownloadLimits,
        runner: Arc<dyn ExternalCommandRunner>,
    ) -> Self {
        Self { config, inspection_limits, runner }
    }

    async fn run_with_output_limit(
        &self,
        args: Vec<String>,
        timeout: Duration,
        source_url: &str,
        max_output_bytes: usize,
    ) -> Result<ExternalCommandOutput, DownloadError> {
        if timeout.is_zero() {
            return Err(DownloadError::terminal(
                "invalid_download_limits",
                "download timeout must be greater than zero",
            ));
        }
        let command = args
            .into_iter()
            .fold(ExternalCommand::new(self.config.executable.clone()), |command, arg| {
                command.arg(arg)
            })
            .clear_environment()
            .env("PATH", DEFAULT_COMMAND_PATH);
        let command = command.timeout(timeout).max_output_bytes(max_output_bytes);
        let output = self
            .runner
            .run(command)
            .await
            .map_err(|error| map_command_error(error, "ytdlp_output_limit"))?;
        if output.stdout_truncated || output.stderr_truncated {
            return Err(DownloadError::terminal(
                "ytdlp_output_limit",
                format!("yt-dlp output exceeded the {}-byte capture limit", max_output_bytes),
            ));
        }
        if !output.success {
            return Err(map_process_failure(&output, source_url));
        }
        Ok(output)
    }

    async fn run_sequence(
        &self,
        args: Vec<String>,
        timeout: Duration,
        source_url: &str,
        output_directory: &Path,
        max_bytes: u64,
    ) -> Result<ExternalCommandOutput, DownloadError> {
        if timeout.is_zero() {
            return Err(DownloadError::terminal(
                "invalid_download_limits",
                "download timeout must be greater than zero",
            ));
        }
        let command = args
            .into_iter()
            .fold(ExternalCommand::new(self.config.executable.clone()), |command, arg| {
                command.arg(arg)
            })
            .clear_environment()
            .env("PATH", DEFAULT_COMMAND_PATH)
            .current_dir(output_directory)
            .timeout(timeout)
            .max_output_bytes(self.config.max_output_bytes);
        let output = self
            .runner
            .run_sequence(command, output_directory, max_bytes)
            .await
            .map_err(|error| map_command_error(error, "source_too_large"))?;
        if output.stdout_truncated || output.stderr_truncated {
            return Err(DownloadError::terminal(
                "ytdlp_output_limit",
                format!(
                    "yt-dlp output exceeded the {}-byte capture limit",
                    self.config.max_output_bytes
                ),
            ));
        }
        if !output.success {
            return Err(map_process_failure(&output, source_url));
        }
        Ok(output)
    }

    /// Verifies the pinned standalone runtime without contacting a provider.
    ///
    /// The local info fixture makes yt-dlp initialize its normal extractor
    /// process with the configured EJS/runtime flags, while the verbose
    /// diagnostics confirm that the bundled EJS package and Deno provider are
    /// discoverable. A separate Deno eval proves that the configured runtime
    /// can actually execute code before the worker accepts page jobs.
    pub async fn verify_runtime(&self, timeout: Duration) -> Result<(), String> {
        if timeout.is_zero() {
            return Err("yt-dlp runtime check timeout must be greater than zero".to_owned());
        }

        let fixture_path =
            std::env::temp_dir().join(format!(".sooqa-ytdlp-runtime-{}.json", Uuid::new_v4()));
        if let Err(error) = tokio::fs::write(&fixture_path, RUNTIME_FIXTURE).await {
            return Err(format!("could not write yt-dlp runtime fixture: {error}"));
        }

        let result = self.verify_runtime_with_fixture(&fixture_path, timeout).await;
        let _ = tokio::fs::remove_file(&fixture_path).await;
        result
    }

    async fn verify_runtime_with_fixture(
        &self,
        fixture_path: &Path,
        timeout: Duration,
    ) -> Result<(), String> {
        let mut args = vec![
            "--verbose".to_owned(),
            "--dump-single-json".to_owned(),
            "--skip-download".to_owned(),
            "--no-playlist".to_owned(),
        ];
        args.extend(self.security_args());
        args.extend(["--load-info-json".to_owned(), fixture_path.display().to_string()]);
        let command = args
            .into_iter()
            .fold(ExternalCommand::new(self.config.executable.clone()), |command, arg| {
                command.arg(arg)
            })
            .clear_environment()
            .env("PATH", DEFAULT_COMMAND_PATH)
            .timeout(timeout)
            .max_output_bytes(DEFAULT_YTDLP_METADATA_MAX_OUTPUT_BYTES);
        let output = self
            .runner
            .run(command)
            .await
            .map_err(|error| format!("yt-dlp runtime probe failed: {error}"))?;
        if !output.success {
            return Err(format!(
                "yt-dlp runtime probe exited unsuccessfully: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        if output.stdout_truncated || output.stderr_truncated {
            return Err(format!(
                "yt-dlp runtime probe exceeded the {DEFAULT_YTDLP_METADATA_MAX_OUTPUT_BYTES}-byte output limit"
            ));
        }
        let diagnostics = format!(
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        if !diagnostics.contains("yt_dlp_ejs") {
            return Err("yt-dlp runtime probe did not discover bundled yt-dlp-ejs".to_owned());
        }
        if !diagnostics.contains("JS runtimes: deno-") {
            return Err(
                "yt-dlp runtime probe did not discover the configured Deno runtime".to_owned()
            );
        }

        let deno_command = ExternalCommand::new(self.config.deno_path.clone())
            .arg("eval")
            .arg("--no-config")
            .arg("console.log('sooqa-deno-runtime-ok')")
            .clear_environment()
            .env("PATH", DEFAULT_COMMAND_PATH)
            .timeout(timeout)
            .max_output_bytes(1024);
        let deno_output = self
            .runner
            .run(deno_command)
            .await
            .map_err(|error| format!("Deno runtime probe failed: {error}"))?;
        if !deno_output.success
            || deno_output.stdout_truncated
            || deno_output.stderr_truncated
            || !String::from_utf8_lossy(&deno_output.stdout).contains("sooqa-deno-runtime-ok")
        {
            return Err(format!(
                "configured Deno runtime probe failed: {}",
                String::from_utf8_lossy(&deno_output.stderr).trim()
            ));
        }
        Ok(())
    }

    fn security_args(&self) -> [String; 5] {
        [
            "--ignore-config".to_owned(),
            "--no-plugin-dirs".to_owned(),
            "--no-remote-components".to_owned(),
            "--js-runtimes".to_owned(),
            format!("deno:{}", self.config.deno_path.display()),
        ]
    }
}

#[async_trait]
impl SourceDownloader for YtDlpDownloader {
    async fn inspect(&self, source: &SourceInput) -> Result<SourceInspection, DownloadError> {
        validate_limits(&self.inspection_limits, self.inspection_limits.timeout)?;
        let source_url = validate_source_url(&source.source_url)?;
        let mut args = vec![
            "--dump-single-json".to_owned(),
            "--skip-download".to_owned(),
            "--no-playlist".to_owned(),
            "--no-warnings".to_owned(),
            "--no-progress".to_owned(),
        ];
        args.extend(self.security_args());
        args.extend([
            "--format".to_owned(),
            self.config.format_selection.clone(),
            "--".to_owned(),
            source_url.clone(),
        ]);
        let output = self
            .run_with_output_limit(
                args,
                self.inspection_limits.timeout,
                &source_url,
                self.config.metadata_max_output_bytes,
            )
            .await?;
        let metadata = parse_metadata(&output.stdout, &source_url)?;
        if metadata.filesize_bytes.is_some_and(|bytes| bytes > self.inspection_limits.max_bytes) {
            return Err(DownloadError::terminal(
                "source_too_large",
                "yt-dlp metadata exceeds the configured byte limit",
            ));
        }

        let content_length_bytes = metadata.filesize_bytes;
        let mime_type = metadata.mime_type.clone();
        let title = metadata.title.clone();
        let media_kind = metadata.media_kind();
        let resolved_url = metadata.webpage_url.clone().or_else(|| Some(source_url.clone()));
        let metadata = serde_json::to_value(&metadata).map_err(|error| {
            DownloadError::terminal(
                "ytdlp_metadata",
                format!("could not serialize metadata: {error}"),
            )
        })?;

        Ok(SourceInspection {
            adapter: "yt_dlp".to_owned(),
            source_url: source.source_url.clone(),
            resolved_url,
            media_kind,
            mime_type,
            content_length_bytes,
            title,
            metadata,
        })
    }

    async fn download(
        &self,
        inspection: &SourceInspection,
        destination: &Path,
        limits: &DownloadLimits,
    ) -> Result<DownloadedSource, DownloadError> {
        validate_limits(limits, limits.timeout)?;
        let source_url = validate_source_url(&inspection.source_url)?;
        let attempt_path = destination.with_file_name(format!(".sooqa-ytdlp-{}", Uuid::new_v4()));
        let attempt = TempDirectory::reserve(attempt_path).await.map_err(|source| {
            DownloadError::terminal(
                "destination_io",
                format!("could not reserve yt-dlp attempt directory: {source}"),
            )
        })?;
        let attempt_max_bytes =
            limits.max_bytes.checked_mul(YTDLP_ATTEMPT_MAX_BYTES_MULTIPLIER).ok_or_else(|| {
                DownloadError::terminal(
                    "invalid_download_limits",
                    "yt-dlp aggregate attempt byte limit overflowed",
                )
            })?;

        let mut args = vec![
            "--no-playlist".to_owned(),
            "--no-warnings".to_owned(),
            "--no-progress".to_owned(),
            "--no-part".to_owned(),
            "--force-overwrites".to_owned(),
            "--no-cache-dir".to_owned(),
        ];
        args.extend(self.security_args());
        args.extend([
            "--paths".to_owned(),
            "home:.".to_owned(),
            "--paths".to_owned(),
            "temp:.".to_owned(),
            "--max-filesize".to_owned(),
            limits.max_bytes.to_string(),
            "--format".to_owned(),
            self.config.format_selection.clone(),
            "--output".to_owned(),
            "final.%(ext)s".to_owned(),
            "--".to_owned(),
            source_url.clone(),
        ]);
        let result = self
            .run_sequence(args, limits.timeout, &source_url, attempt.path(), attempt_max_bytes)
            .await;
        result?;

        let final_path = single_regular_file(attempt.path()).await.map_err(|source| {
            DownloadError::terminal(
                "destination_io",
                format!("yt-dlp attempt did not produce exactly one final media file: {source}"),
            )
        })?;
        let metadata = tokio::fs::metadata(&final_path).await.map_err(|source| {
            DownloadError::terminal(
                "destination_io",
                format!("could not inspect yt-dlp final media file: {source}"),
            )
        })?;
        if metadata.len() > limits.max_bytes {
            return Err(DownloadError::terminal(
                "source_too_large",
                "downloaded source exceeds the configured byte limit",
            ));
        }

        let published = publish_or_reuse(&final_path, destination).await.map_err(|error| {
            DownloadError::terminal(
                "destination_io",
                format!("could not publish yt-dlp output: {error}"),
            )
        })?;
        let bytes = match published {
            PublishOutcome::Published => metadata.len(),
            PublishOutcome::Reused => tokio::fs::metadata(destination)
                .await
                .map_err(|error| {
                    DownloadError::terminal(
                        "destination_io",
                        format!("could not inspect reused yt-dlp output: {error}"),
                    )
                })?
                .len(),
        };

        Ok(DownloadedSource {
            path: destination.to_owned(),
            bytes,
            mime_type: inspection.mime_type.clone(),
        })
    }
}

async fn single_regular_file(directory: &Path) -> Result<PathBuf, std::io::Error> {
    let mut entries = tokio::fs::read_dir(directory).await?;
    let mut files = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let metadata = tokio::fs::symlink_metadata(&path).await?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("yt-dlp attempt contains a non-regular entry: {}", path.display()),
            ));
        }
        files.push(path);
    }
    match files.as_slice() {
        [path] => Ok(path.clone()),
        [] => Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "yt-dlp attempt directory is empty",
        )),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "yt-dlp attempt contains multiple final media files",
        )),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct YtDlpMetadata {
    pub id: Option<String>,
    pub title: Option<String>,
    pub webpage_url: Option<String>,
    pub extractor: Option<String>,
    pub duration_ms: Option<u64>,
    pub filesize_bytes: Option<u64>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub ext: Option<String>,
    pub mime_type: Option<String>,
    pub vcodec: Option<String>,
    pub acodec: Option<String>,
    pub format_id: Option<String>,
    pub format: Option<String>,
    pub thumbnail: Option<String>,
    pub uploader: Option<String>,
}

impl YtDlpMetadata {
    fn media_kind(&self) -> SourceMediaKind {
        if is_present_codec(self.vcodec.as_deref()) {
            return SourceMediaKind::Video;
        }
        if is_present_codec(self.acodec.as_deref()) {
            return SourceMediaKind::Audio;
        }
        match self.ext.as_deref().map(str::to_ascii_lowercase).as_deref() {
            Some("jpg" | "jpeg" | "png" | "gif" | "webp" | "avif") => SourceMediaKind::Image,
            _ => SourceMediaKind::Unknown,
        }
    }
}

#[derive(Debug, Deserialize)]
struct RawYtDlpMetadata {
    id: Option<String>,
    title: Option<String>,
    webpage_url: Option<String>,
    original_url: Option<String>,
    extractor_key: Option<String>,
    extractor: Option<String>,
    duration: Option<f64>,
    filesize: Option<u64>,
    filesize_approx: Option<u64>,
    width: Option<u32>,
    height: Option<u32>,
    ext: Option<String>,
    mime_type: Option<String>,
    vcodec: Option<String>,
    acodec: Option<String>,
    format_id: Option<String>,
    format: Option<String>,
    thumbnail: Option<String>,
    uploader: Option<String>,
}

fn parse_metadata(input: &[u8], source_url: &str) -> Result<YtDlpMetadata, DownloadError> {
    let raw: RawYtDlpMetadata = serde_json::from_slice(input).map_err(|error| {
        DownloadError::terminal(
            "ytdlp_invalid_output",
            format!("yt-dlp returned invalid JSON: {error}"),
        )
    })?;
    let mime_type = raw.mime_type.or_else(|| mime_type_for_ext(raw.ext.as_deref()));
    Ok(YtDlpMetadata {
        id: raw.id,
        title: raw.title,
        webpage_url: raw.webpage_url.or(raw.original_url).or_else(|| Some(source_url.to_owned())),
        extractor: raw.extractor_key.or(raw.extractor),
        duration_ms: raw.duration.and_then(duration_to_ms),
        filesize_bytes: raw.filesize.or(raw.filesize_approx),
        width: raw.width,
        height: raw.height,
        ext: raw.ext,
        mime_type,
        vcodec: raw.vcodec,
        acodec: raw.acodec,
        format_id: raw.format_id,
        format: raw.format,
        thumbnail: raw.thumbnail,
        uploader: raw.uploader,
    })
}

fn validate_format_selection(value: &str) -> Result<(), YtDlpConfigError> {
    if value.trim().is_empty() {
        return Err(YtDlpConfigError::EmptyFormatSelection);
    }
    if value.starts_with('-') || value.chars().any(char::is_control) {
        return Err(YtDlpConfigError::UnsafeFormatSelection);
    }
    Ok(())
}

fn validate_source_url(value: &str) -> Result<String, DownloadError> {
    let url = Url::parse(value).map_err(|_| {
        DownloadError::terminal("invalid_source_url", "source URL could not be parsed")
    })?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(DownloadError::terminal(
            "unsupported_scheme",
            "yt-dlp source URL must use http or https",
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(DownloadError::terminal(
            "source_credentials_forbidden",
            "source URL must not contain embedded credentials",
        ));
    }
    let default_port = match url.scheme() {
        "http" => 80,
        "https" => 443,
        _ => unreachable!("scheme was checked above"),
    };
    if url.port().is_some_and(|port| port != default_port) {
        return Err(DownloadError::terminal(
            "source_port_not_allowed",
            "yt-dlp page URLs must use the default HTTP or HTTPS port",
        ));
    }
    let Some(host) = url.host() else {
        return Err(DownloadError::terminal("missing_source_host", "source URL has no host"));
    };
    if !matches!(host, url::Host::Domain(_)) {
        return Err(DownloadError::terminal(
            "source_host_not_allowed",
            "yt-dlp page URL host must be a configured DNS hostname",
        ));
    }
    Ok(url.to_string())
}

fn validate_limits(limits: &DownloadLimits, timeout: Duration) -> Result<(), DownloadError> {
    if limits.max_bytes == 0 || timeout.is_zero() {
        return Err(DownloadError::terminal(
            "invalid_download_limits",
            "download byte limit and timeout must be greater than zero",
        ));
    }
    Ok(())
}

fn map_command_error(error: CommandError, output_limit_class: &str) -> DownloadError {
    if error.is_timeout() {
        DownloadError::retryable("ytdlp_timeout", error.to_string())
    } else if error.is_output_limit_exceeded() {
        DownloadError::terminal(output_limit_class, error.to_string())
    } else {
        DownloadError::terminal("ytdlp_command", error.to_string())
    }
}

fn map_process_failure(output: &ExternalCommandOutput, source_url: &str) -> DownloadError {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().replace(source_url, "<source-url>");
    let message = if stderr.is_empty() {
        format!("yt-dlp exited unsuccessfully with status {:?}", output.exit_code)
    } else {
        format!("yt-dlp exited unsuccessfully with status {:?}: {stderr}", output.exit_code)
    };
    if is_transient_failure(&stderr) {
        DownloadError::retryable("ytdlp_upstream", message)
    } else {
        DownloadError::terminal("ytdlp_process", message)
    }
}

fn is_transient_failure(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    [
        "timed out",
        "timeout",
        "temporary failure",
        "connection reset",
        "connection refused",
        "network is unreachable",
        "name or service not known",
        "http error 429",
        "http error 502",
        "http error 503",
        "http error 504",
    ]
    .iter()
    .any(|marker| message.contains(marker))
}

fn duration_to_ms(value: f64) -> Option<u64> {
    if !value.is_finite() || value < 0.0 {
        return None;
    }
    let milliseconds = value * 1000.0;
    if milliseconds > u64::MAX as f64 {
        return None;
    }
    Some(milliseconds.round() as u64)
}

fn is_present_codec(codec: Option<&str>) -> bool {
    codec.is_some_and(|codec| !codec.is_empty() && codec != "none")
}

fn mime_type_for_ext(ext: Option<&str>) -> Option<String> {
    let mime_type = match ext?.to_ascii_lowercase().as_str() {
        "mp4" => "video/mp4",
        "webm" => "video/webm",
        "mkv" => "video/x-matroska",
        "mov" => "video/quicktime",
        "mp3" => "audio/mpeg",
        "m4a" => "audio/mp4",
        "opus" => "audio/opus",
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "avif" => "image/avif",
        _ => return None,
    };
    Some(mime_type.to_owned())
}

#[cfg(unix)]
#[cfg(test)]
mod tests {
    use std::{
        os::unix::fs::PermissionsExt,
        path::PathBuf,
        process::{Command, Stdio},
        sync::{Arc, Mutex},
    };

    use super::*;

    struct RuntimeProbeRunner {
        calls: Mutex<Vec<ExternalCommand>>,
    }

    #[async_trait]
    impl ExternalCommandRunner for RuntimeProbeRunner {
        async fn run(
            &self,
            command: ExternalCommand,
        ) -> Result<ExternalCommandOutput, CommandError> {
            let is_deno =
                command.args().iter().any(|argument| argument.to_string_lossy() == "eval");
            self.calls.lock().expect("test mutex should not be poisoned").push(command);
            Ok(if is_deno {
                ExternalCommandOutput {
                    success: true,
                    exit_code: Some(0),
                    stdout: b"sooqa-deno-runtime-ok\n".to_vec(),
                    stderr: Vec::new(),
                    stdout_truncated: false,
                    stderr_truncated: false,
                }
            } else {
                ExternalCommandOutput {
                    success: true,
                    exit_code: Some(0),
                    stdout: b"{}\n".to_vec(),
                    stderr: b"[debug] Optional libraries: yt_dlp_ejs-0.8.0\n[debug] JS runtimes: deno-2.8.1\n".to_vec(),
                    stdout_truncated: false,
                    stderr_truncated: false,
                }
            })
        }
    }

    #[tokio::test]
    async fn runtime_probe_requires_bundled_ejs_and_executes_deno() {
        let runner = Arc::new(RuntimeProbeRunner { calls: Mutex::new(Vec::new()) });
        let downloader = YtDlpDownloader::with_runner(
            YtDlpConfig::new("yt-dlp", "best").expect("format selection should be valid"),
            DownloadLimits::default(),
            Arc::clone(&runner) as Arc<dyn ExternalCommandRunner>,
        );

        downloader
            .verify_runtime(Duration::from_secs(1))
            .await
            .expect("runtime fixture should pass");

        let calls = runner.calls.lock().expect("test mutex should not be poisoned");
        assert_eq!(calls.len(), 2);
        assert!(calls[0].clears_environment());
        assert_eq!(calls[0].max_output_bytes_limit(), DEFAULT_YTDLP_METADATA_MAX_OUTPUT_BYTES);
        assert!(calls[0].args().iter().any(|arg| arg == "--load-info-json"));
        assert!(calls[1].clears_environment());
        assert_eq!(calls[1].args()[0], "eval");
    }

    #[tokio::test]
    async fn fake_executable_inspects_and_downloads_without_shell_interpolation() {
        let root = std::env::temp_dir().join(format!("sooqa-ytdlp-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&root).await.expect("test root should be created");
        let executable = root.join("fake-yt-dlp.sh");
        let args_log = root.join("args.log");
        let script = format!(
            "#!/bin/sh\nset -eu\nif [ -n \"${{DATABASE_URL-}}\" ] || [ -n \"${{SOOQA_API_TOKEN-}}\" ] || [ -n \"${{SOOQA_TELEGRAM_BOT_TOKEN-}}\" ]; then exit 91; fi\nlog={args_log:?}\n: > \"$log\"\nfor arg in \"$@\"; do printf '%s\\n' \"$arg\" >> \"$log\"; done\nif [ \"$1\" = \"--dump-single-json\" ]; then printf '%s\\n' '{{\"id\":\"abc\",\"title\":\"Example\",\"webpage_url\":\"https://example.test/watch?v=abc\",\"extractor\":\"generic\",\"duration\":3.5,\"filesize\":17,\"width\":1280,\"height\":720,\"ext\":\"mp4\",\"vcodec\":\"h264\",\"acodec\":\"aac\",\"format_id\":\"best\"}}'; else printf 'fake-media' > final.mp4; fi\n",
            args_log = args_log.display()
        );
        tokio::fs::write(&executable, script).await.expect("fake executable should be written");
        let mut permissions = tokio::fs::metadata(&executable)
            .await
            .expect("fake executable metadata should be available")
            .permissions();
        permissions.set_mode(0o700);
        tokio::fs::set_permissions(&executable, permissions)
            .await
            .expect("fake executable should be executable");

        let config = YtDlpConfig::new(&executable, "bestvideo*+bestaudio/best")
            .expect("format selection should be accepted");
        let limits =
            DownloadLimits { max_bytes: 1024, max_redirects: 0, timeout: Duration::from_secs(5) };
        let downloader = YtDlpDownloader::with_limits(config, limits);
        let source = SourceInput {
            ingest_request_id: uuid::Uuid::new_v4(),
            source_url: "https://example.test/watch?v=abc&title=hello%20world".to_owned(),
            page_url: None,
        };

        let inspection = downloader.inspect(&source).await.expect("inspection should succeed");
        assert_eq!(inspection.adapter, "yt_dlp");
        assert_eq!(inspection.media_kind, SourceMediaKind::Video);
        assert_eq!(inspection.mime_type.as_deref(), Some("video/mp4"));
        assert_eq!(inspection.content_length_bytes, Some(17));
        assert_eq!(inspection.title.as_deref(), Some("Example"));
        assert_eq!(inspection.metadata["duration_ms"], 3500);

        let inspect_args =
            tokio::fs::read_to_string(&args_log).await.expect("inspect arguments should be logged");
        assert!(inspect_args.lines().any(|line| line == "bestvideo*+bestaudio/best"));
        for argument in [
            "--ignore-config",
            "--no-plugin-dirs",
            "--no-remote-components",
            "--js-runtimes",
            "deno:deno",
        ] {
            assert!(
                inspect_args.lines().any(|line| line == argument),
                "missing argument: {argument}"
            );
        }
        assert!(
            inspect_args
                .lines()
                .any(|line| line == "https://example.test/watch?v=abc&title=hello%20world")
        );

        let destination = root.join("source.webm");
        let downloaded = downloader
            .download(&inspection, &destination, &limits)
            .await
            .expect("download should succeed");
        assert_eq!(downloaded.bytes, 10);
        assert_eq!(
            tokio::fs::read(&destination).await.expect("download should be readable"),
            b"fake-media"
        );
        let download_args = tokio::fs::read_to_string(&args_log)
            .await
            .expect("download arguments should be logged");
        assert!(download_args.lines().any(|line| line == "--output"));
        assert!(download_args.lines().any(|line| line == "--max-filesize"));
        assert!(download_args.lines().any(|line| line == "--paths"));
        assert!(download_args.lines().any(|line| line == "home:."));
        assert!(download_args.lines().any(|line| line == "temp:."));
        let output_argument = download_args
            .lines()
            .skip_while(|line| *line != "--output")
            .nth(1)
            .expect("download output argument should be logged");
        assert_eq!(output_argument, "final.%(ext)s");
        assert!(
            download_args
                .lines()
                .any(|line| line == "https://example.test/watch?v=abc&title=hello%20world")
        );

        let replayed = downloader
            .download(&inspection, &destination, &limits)
            .await
            .expect("retry should reuse the validated published output");
        assert_eq!(replayed.bytes, downloaded.bytes);
        assert_eq!(
            tokio::fs::read(&destination).await.expect("reused output should be readable"),
            b"fake-media"
        );

        tokio::fs::remove_dir_all(root).await.expect("test root should be removed");
    }

    #[tokio::test]
    async fn download_enforces_the_aggregate_attempt_limit_while_yt_dlp_is_running() {
        let root =
            std::env::temp_dir().join(format!("sooqa-ytdlp-size-limit-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&root).await.expect("test root should be created");
        let executable = root.join("fake-yt-dlp-size-limit.sh");
        let pid_file = root.join("yt-dlp.pid");
        let script = r#"#!/bin/sh
set -eu
printf '%s' "$$" > "PID_FILE"
printf 'fake-media' > final.mp4
while :; do
    printf '0123456789' >> intermediate.part
    sleep 0.01
done
"#;
        let script = script.replace("PID_FILE", &pid_file.display().to_string());
        tokio::fs::write(&executable, script).await.expect("fake executable should be written");
        let mut permissions = tokio::fs::metadata(&executable)
            .await
            .expect("fake executable metadata should be available")
            .permissions();
        permissions.set_mode(0o700);
        tokio::fs::set_permissions(&executable, permissions)
            .await
            .expect("fake executable should be executable");

        let downloader = YtDlpDownloader::new(
            YtDlpConfig::new(&executable, "best").expect("format selection should be valid"),
        );
        let destination = root.join("source.mp4");
        let inspection = SourceInspection {
            adapter: "yt_dlp".to_owned(),
            source_url: "https://example.test/video".to_owned(),
            resolved_url: None,
            media_kind: SourceMediaKind::Video,
            mime_type: Some("video/mp4".to_owned()),
            content_length_bytes: None,
            title: None,
            metadata: serde_json::json!({}),
        };
        let limits =
            DownloadLimits { max_bytes: 32, max_redirects: 0, timeout: Duration::from_secs(5) };
        let error = downloader
            .download(&inspection, &destination, &limits)
            .await
            .expect_err("yt-dlp attempt should be stopped at the aggregate byte limit");
        assert_eq!(error.class(), "source_too_large");
        assert!(!destination.exists());
        let pid = tokio::fs::read_to_string(&pid_file)
            .await
            .expect("fake executable should have recorded its PID");
        let status = Command::new("/bin/kill")
            .arg("-0")
            .arg(pid.trim())
            .stderr(Stdio::null())
            .status()
            .expect("kill probe should run");
        assert!(!status.success(), "oversized yt-dlp process should be terminated");
        let mut entries = tokio::fs::read_dir(&root).await.expect("test root should be readable");
        while let Some(entry) = entries.next_entry().await.expect("directory should be readable") {
            let name = entry.file_name().to_string_lossy().into_owned();
            assert!(!name.starts_with(".sooqa-ytdlp-"), "attempt directory was left behind");
        }
        tokio::fs::remove_dir_all(root).await.expect("test root should be removed");
    }

    #[tokio::test]
    async fn failed_download_removes_partial_yt_dlp_output() {
        let root =
            std::env::temp_dir().join(format!("sooqa-ytdlp-failure-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&root).await.expect("test root should be created");
        let executable = root.join("fake-yt-dlp-failure.sh");
        let script = r#"#!/bin/sh
set -eu
printf 'partial-media' > final.mp4
printf 'leftover-sidecar' > intermediate.part
exit 1
"#;
        tokio::fs::write(&executable, script).await.expect("fake executable should be written");
        let mut permissions = tokio::fs::metadata(&executable)
            .await
            .expect("fake executable metadata should be available")
            .permissions();
        permissions.set_mode(0o700);
        tokio::fs::set_permissions(&executable, permissions)
            .await
            .expect("fake executable should be executable");

        let downloader = YtDlpDownloader::new(
            YtDlpConfig::new(&executable, "best").expect("format selection should be valid"),
        );
        let destination = root.join("source.mp4");
        let inspection = SourceInspection {
            adapter: "yt_dlp".to_owned(),
            source_url: "https://example.test/video".to_owned(),
            resolved_url: None,
            media_kind: SourceMediaKind::Video,
            mime_type: Some("video/mp4".to_owned()),
            content_length_bytes: None,
            title: None,
            metadata: serde_json::json!({}),
        };
        let error = downloader
            .download(&inspection, &destination, &DownloadLimits::default())
            .await
            .expect_err("failed yt-dlp process should be reported");
        assert_eq!(error.class(), "ytdlp_process");
        assert!(!destination.exists());
        let mut entries = tokio::fs::read_dir(&root).await.expect("test root should be readable");
        while let Some(entry) = entries.next_entry().await.expect("directory should be readable") {
            let name = entry.file_name().to_string_lossy().into_owned();
            assert!(!name.starts_with(".sooqa-ytdlp-"), "attempt directory was left behind");
        }
        tokio::fs::remove_dir_all(root).await.expect("test root should be removed");
    }

    #[derive(Clone, Copy)]
    struct PendingRunner;

    #[async_trait]
    impl ExternalCommandRunner for PendingRunner {
        async fn run(
            &self,
            _command: ExternalCommand,
        ) -> Result<ExternalCommandOutput, CommandError> {
            std::future::pending().await
        }

        async fn run_sequence(
            &self,
            _command: ExternalCommand,
            output_directory: &Path,
            _max_bytes: u64,
        ) -> Result<ExternalCommandOutput, CommandError> {
            tokio::fs::write(output_directory.join("leftover-sidecar.part"), b"sidecar")
                .await
                .expect("pending runner should write its sidecar");
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn dropped_download_removes_the_yt_dlp_attempt_directory_and_sidecars() {
        let root =
            std::env::temp_dir().join(format!("sooqa-ytdlp-cancel-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&root).await.expect("test root should be created");
        let config = YtDlpConfig::new("yt-dlp", "best").expect("format selection should be valid");
        let downloader = YtDlpDownloader::with_runner(
            config,
            DownloadLimits::default(),
            Arc::new(PendingRunner),
        );
        let destination = root.join("source.mp4");
        let inspection = SourceInspection {
            adapter: "yt_dlp".to_owned(),
            source_url: "https://example.test/video".to_owned(),
            resolved_url: None,
            media_kind: SourceMediaKind::Video,
            mime_type: Some("video/mp4".to_owned()),
            content_length_bytes: None,
            title: None,
            metadata: serde_json::json!({}),
        };
        let download = tokio::spawn(async move {
            downloader.download(&inspection, &destination, &DownloadLimits::default()).await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        download.abort();
        let _ = download.await;

        let mut entries = tokio::fs::read_dir(&root).await.expect("test root should be readable");
        while let Some(entry) = entries.next_entry().await.expect("directory should be readable") {
            let name = entry.file_name().to_string_lossy().into_owned();
            assert!(!name.starts_with(".sooqa-ytdlp-"), "yt-dlp attempt directory was left behind");
        }
        tokio::fs::remove_dir_all(root).await.expect("test root should be removed");
    }

    #[test]
    fn rejects_format_values_that_could_become_options() {
        assert_eq!(
            YtDlpConfig::new(PathBuf::from("yt-dlp"), "--exec=whoami")
                .expect_err("option-looking format must be rejected"),
            YtDlpConfigError::UnsafeFormatSelection
        );
        assert_eq!(
            YtDlpConfig::new(PathBuf::from("yt-dlp"), "   ")
                .expect_err("empty format must be rejected"),
            YtDlpConfigError::EmptyFormatSelection
        );
    }

    #[test]
    fn deno_version_must_meet_yt_dlp_runtime_minimum() {
        assert!(!is_supported_deno_version("deno 2.2.9"));
        assert!(is_supported_deno_version("deno 2.3.0 (stable, release)"));
        assert!(is_supported_deno_version("deno 2.8.1"));
        assert!(!is_supported_deno_version("deno unknown"));
    }
}
