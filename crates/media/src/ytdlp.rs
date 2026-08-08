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

use crate::publication::{PublishOutcome, TempArtifact, publish_or_reuse};
use crate::{
    CommandError, DEFAULT_MAX_OUTPUT_BYTES, DownloadError, DownloadLimits, DownloadedSource,
    ExternalCommand, ExternalCommandOutput, ExternalCommandRunner, SourceDownloader, SourceInput,
    SourceInspection, SourceMediaKind,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct YtDlpConfig {
    executable: PathBuf,
    format_selection: String,
    max_output_bytes: usize,
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
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
        })
    }

    pub fn with_max_output_bytes(mut self, max_output_bytes: usize) -> Self {
        self.max_output_bytes = max_output_bytes;
        self
    }
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

    async fn run(
        &self,
        args: Vec<String>,
        timeout: Duration,
        source_url: &str,
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
            });
        let command = command.timeout(timeout).max_output_bytes(self.config.max_output_bytes);
        let output = self.runner.run(command).await.map_err(map_command_error)?;
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
}

#[async_trait]
impl SourceDownloader for YtDlpDownloader {
    async fn inspect(&self, source: &SourceInput) -> Result<SourceInspection, DownloadError> {
        validate_limits(&self.inspection_limits, self.inspection_limits.timeout)?;
        let source_url = validate_source_url(&source.source_url)?;
        let output = self
            .run(
                vec![
                    "--dump-single-json".to_owned(),
                    "--skip-download".to_owned(),
                    "--no-playlist".to_owned(),
                    "--no-warnings".to_owned(),
                    "--no-progress".to_owned(),
                    "--format".to_owned(),
                    self.config.format_selection.clone(),
                    "--".to_owned(),
                    source_url.clone(),
                ],
                self.inspection_limits.timeout,
                &source_url,
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
        let source_url = inspection.resolved_url.as_deref().unwrap_or(&inspection.source_url);
        let source_url = validate_source_url(source_url)?;
        let temporary = destination.with_file_name(format!(".sooqa-ytdlp-{}.tmp", Uuid::new_v4()));
        let mut temporary = TempArtifact::reserve(temporary).await.map_err(|source| {
            DownloadError::terminal(
                "destination_io",
                format!("could not reserve yt-dlp temporary output: {source}"),
            )
        })?;

        let result = self
            .run(
                vec![
                    "--no-playlist".to_owned(),
                    "--no-warnings".to_owned(),
                    "--no-progress".to_owned(),
                    "--no-part".to_owned(),
                    "--force-overwrites".to_owned(),
                    "--max-filesize".to_owned(),
                    limits.max_bytes.to_string(),
                    "--format".to_owned(),
                    self.config.format_selection.clone(),
                    "--output".to_owned(),
                    temporary.path().to_string_lossy().into_owned(),
                    "--".to_owned(),
                    source_url.clone(),
                ],
                limits.timeout,
                &source_url,
            )
            .await;
        result?;

        let metadata = match tokio::fs::metadata(temporary.path()).await {
            Ok(metadata) => metadata,
            Err(source) => {
                return Err(DownloadError::terminal(
                    "destination_io",
                    format!("yt-dlp did not produce the requested destination: {source}"),
                ));
            }
        };
        if !metadata.is_file() {
            return Err(DownloadError::terminal(
                "destination_io",
                "yt-dlp destination is not a regular file",
            ));
        }
        if metadata.len() > limits.max_bytes {
            return Err(DownloadError::terminal(
                "source_too_large",
                "downloaded source exceeds the configured byte limit",
            ));
        }

        let published = publish_or_reuse(temporary.path(), destination).await.map_err(|error| {
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
        temporary.remove().await;

        Ok(DownloadedSource {
            path: destination.to_owned(),
            bytes,
            mime_type: inspection.mime_type.clone(),
        })
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
    if url.host_str().is_none() {
        return Err(DownloadError::terminal("missing_source_host", "source URL has no host"));
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

fn map_command_error(error: CommandError) -> DownloadError {
    if error.is_timeout() {
        DownloadError::retryable("ytdlp_timeout", error.to_string())
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
    use std::{os::unix::fs::PermissionsExt, path::PathBuf, sync::Arc};

    use super::*;

    #[tokio::test]
    async fn fake_executable_inspects_and_downloads_without_shell_interpolation() {
        let root = std::env::temp_dir().join(format!("sooqa-ytdlp-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&root).await.expect("test root should be created");
        let executable = root.join("fake-yt-dlp.sh");
        let args_log = root.join("args.log");
        let script = format!(
            "#!/bin/sh\nset -eu\nlog={args_log:?}\n: > \"$log\"\nfor arg in \"$@\"; do printf '%s\\n' \"$arg\" >> \"$log\"; done\nif [ \"$1\" = \"--dump-single-json\" ]; then printf '%s\\n' '{{\"id\":\"abc\",\"title\":\"Example\",\"webpage_url\":\"https://example.test/watch?v=abc\",\"extractor\":\"generic\",\"duration\":3.5,\"filesize\":17,\"width\":1280,\"height\":720,\"ext\":\"mp4\",\"vcodec\":\"h264\",\"acodec\":\"aac\",\"format_id\":\"best\"}}'; else output=; previous=; for arg in \"$@\"; do if [ \"$previous\" = \"--output\" ]; then output=\"$arg\"; fi; previous=\"$arg\"; done; printf 'fake-media' > \"$output\"; fi\n",
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
        let output_argument = download_args
            .lines()
            .skip_while(|line| *line != "--output")
            .nth(1)
            .expect("download output argument should be logged");
        assert_ne!(output_argument, destination.display().to_string());
        assert!(
            Path::new(output_argument)
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(".sooqa-ytdlp-") && name.ends_with(".tmp"))
        );
        assert!(download_args.lines().any(|line| line == "https://example.test/watch?v=abc"));

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
    async fn failed_download_removes_partial_yt_dlp_output() {
        let root =
            std::env::temp_dir().join(format!("sooqa-ytdlp-failure-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&root).await.expect("test root should be created");
        let executable = root.join("fake-yt-dlp-failure.sh");
        let script = r#"#!/bin/sh
set -eu
output=
previous=
for arg in "$@"; do
    if [ "$previous" = "--output" ]; then output="$arg"; fi
    previous="$arg"
done
printf 'partial-media' > "$output"
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
            assert!(!name.starts_with(".sooqa-ytdlp-"), "temporary output was left behind");
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
    }

    #[tokio::test]
    async fn dropped_download_removes_the_yt_dlp_temporary_artifact() {
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
            assert!(!name.starts_with(".sooqa-ytdlp-"), "yt-dlp temporary output was left behind");
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
}
