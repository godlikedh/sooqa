use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;
use uuid::Uuid;

use crate::direct_http::{self, HostResolver};
use crate::publication::{PublishOutcome, TempDirectory, publish_or_reuse};
use crate::{
    CommandError, DEFAULT_COMMAND_PATH, DEFAULT_MAX_OUTPUT_BYTES, DownloadError, DownloadLimits,
    DownloadedSource, ExternalCommand, ExternalCommandOutput, ExternalCommandRunner,
    SourceDownloader, SourceInput, SourceInspection, SourceMediaKind,
};

const DEFAULT_DENO_PATH: &str = "deno";
const DEFAULT_YTDLP_POT_PROVIDER_URL: &str = "http://127.0.0.1:4416";
const MIN_SUPPORTED_DENO_VERSION: (u32, u32, u32) = (2, 3, 0);
const MAX_POT_PROVIDER_RESPONSE_BYTES: usize = 64 * 1024;
const YTDLP_ATTEMPT_MAX_BYTES_MULTIPLIER: u64 = 3;
pub const YTDLP_POT_PROVIDER_VERSION: &str = "1.3.1";
pub const YTDLP_PLUGIN_DIRECTORY: &str = "/usr/local/share/sooqa/yt-dlp-plugins";
pub const YTDLP_PLUGIN_ARCHIVE_PATH: &str =
    "/usr/local/share/sooqa/yt-dlp-plugins/bgutil-ytdlp-pot-provider-1.3.1.zip";
pub const MAX_YTDLP_FORMAT_SELECTION_BYTES: usize = 1024;
pub const YTDLP_PROGRESSIVE_FALLBACK_FORMAT: &str =
    "best[ext=mp4][vcodec!=none][acodec!=none]/best[vcodec!=none][acodec!=none]";
pub const DEFAULT_YTDLP_METADATA_MAX_OUTPUT_BYTES: usize = 4 * 1024 * 1024;
const MAX_YTDLP_METADATA_ID_BYTES: usize = 256;
const MAX_YTDLP_METADATA_TEXT_BYTES: usize = 4 * 1024;
const MAX_YTDLP_METADATA_URL_BYTES: usize = 2 * 1024;
const MAX_YTDLP_METADATA_EXTRACTOR_BYTES: usize = 128;
const MAX_YTDLP_METADATA_FORMAT_BYTES: usize = 1024;
const MAX_YTDLP_METADATA_MIME_BYTES: usize = 128;
const MAX_YTDLP_METADATA_EXTENSION_BYTES: usize = 32;
const MAX_TCO_REDIRECTS: u32 = 5;
const MAX_TCO_LOCATION_BYTES: usize = 2 * 1024;
const TCO_PREFLIGHT_USER_AGENT: &str = "sooqa-tco-preflight/1";
type TcoClientFactory = Arc<
    dyn Fn(&direct_http::ResolvedTarget, Duration) -> Result<reqwest::Client, DownloadError>
        + Send
        + Sync,
>;
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

/// The provider families that the page adapter may handle. This is
/// deliberately a closed set: configuring an arbitrary hostname must not
/// turn yt-dlp into a general-purpose downloader.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum YtDlpProviderFamily {
    Youtube,
    Tiktok,
    Instagram,
    X,
}

impl YtDlpProviderFamily {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Youtube => "youtube",
            Self::Tiktok => "tiktok",
            Self::Instagram => "instagram",
            Self::X => "x",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct YtDlpConfig {
    executable: PathBuf,
    format_selection: String,
    deno_path: PathBuf,
    max_output_bytes: usize,
    metadata_max_output_bytes: usize,
    pot_provider_url: String,
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
            pot_provider_url: DEFAULT_YTDLP_POT_PROVIDER_URL.to_owned(),
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

    pub fn with_pot_provider_url(mut self, url: impl Into<String>) -> Self {
        self.pot_provider_url = url.into();
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
    #[error("yt-dlp format selection exceeds the {MAX_YTDLP_FORMAT_SELECTION_BYTES}-byte limit")]
    FormatSelectionTooLong,
    #[error("yt-dlp format selection contains an unsafe control character or option prefix")]
    UnsafeFormatSelection,
}

#[derive(Clone)]
pub struct YtDlpDownloader {
    config: YtDlpConfig,
    inspection_limits: DownloadLimits,
    runner: Arc<dyn ExternalCommandRunner>,
    resolver: Arc<dyn HostResolver>,
    tco_client_factory: TcoClientFactory,
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
        Self::with_runner_and_resolver(
            config,
            inspection_limits,
            runner,
            direct_http::default_host_resolver(),
        )
    }

    fn with_runner_and_resolver(
        config: YtDlpConfig,
        inspection_limits: DownloadLimits,
        runner: Arc<dyn ExternalCommandRunner>,
        resolver: Arc<dyn HostResolver>,
    ) -> Self {
        Self::with_runner_resolver_and_client(
            config,
            inspection_limits,
            runner,
            resolver,
            Arc::new(direct_http::client_for),
        )
    }

    fn with_runner_resolver_and_client(
        config: YtDlpConfig,
        inspection_limits: DownloadLimits,
        runner: Arc<dyn ExternalCommandRunner>,
        resolver: Arc<dyn HostResolver>,
        tco_client_factory: TcoClientFactory,
    ) -> Self {
        Self { config, inspection_limits, runner, resolver, tco_client_factory }
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
            return Err(map_inspection_failure(&output, source_url));
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
        provider_family: Option<YtDlpProviderFamily>,
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
            return Err(map_download_failure(&output, source_url, provider_family));
        }
        Ok(output)
    }

    /// Verifies the pinned standalone runtime and plugin without contacting a provider.
    ///
    /// The local info fixture makes yt-dlp initialize its normal extractor
    /// process with the configured EJS/runtime flags, while the verbose
    /// diagnostics confirm that the bundled EJS package, pinned PO-token
    /// plugin, and Deno provider are discoverable. A separate Deno eval proves
    /// that the configured runtime can actually execute code before the worker
    /// accepts page jobs.
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
        args.extend(self.security_args(None));
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
        let expected_plugin_directory = format!("{YTDLP_PLUGIN_ARCHIVE_PATH}/yt_dlp_plugins");
        if !diagnostics.contains(&expected_plugin_directory) {
            return Err(format!(
                "yt-dlp runtime probe did not discover the pinned PO-token provider plugin archive ({YTDLP_PLUGIN_ARCHIVE_PATH})"
            ));
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

    /// Verifies the private HTTP provider and its pinned version before page
    /// jobs can enter the queue.
    pub async fn verify_pot_provider(&self, timeout: Duration) -> Result<(), String> {
        if timeout.is_zero() {
            return Err("PO-token provider check timeout must be greater than zero".to_owned());
        }
        let ping_url = pot_provider_ping_url(&self.config.pot_provider_url)?;
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(timeout)
            .build()
            .map_err(|error| format!("could not build PO-token provider client: {error}"))?;
        let response = client
            .get(ping_url)
            .send()
            .await
            .map_err(|error| format!("PO-token provider is unavailable: {error}"))?;
        let status = response.status();
        if !status.is_success() {
            return Err(format!("PO-token provider returned HTTP {status}"));
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_POT_PROVIDER_RESPONSE_BYTES as u64)
        {
            return Err("PO-token provider returned an oversized health response".to_owned());
        }
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| {
                format!("could not read PO-token provider health response: {error}")
            })?;
            if chunk.len() > MAX_POT_PROVIDER_RESPONSE_BYTES.saturating_sub(body.len()) {
                return Err("PO-token provider returned an oversized health response".to_owned());
            }
            body.extend_from_slice(&chunk);
        }
        let payload: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|error| format!("PO-token provider returned invalid health JSON: {error}"))?;
        if payload.get("version").and_then(serde_json::Value::as_str)
            != Some(YTDLP_POT_PROVIDER_VERSION)
        {
            return Err(format!(
                "PO-token provider version mismatch: expected {YTDLP_POT_PROVIDER_VERSION}"
            ));
        }
        Ok(())
    }

    fn security_args(&self, provider_family: Option<YtDlpProviderFamily>) -> Vec<String> {
        let mut args = vec![
            "--ignore-config".to_owned(),
            "--no-plugin-dirs".to_owned(),
            "--plugin-dirs".to_owned(),
            YTDLP_PLUGIN_DIRECTORY.to_owned(),
            "--no-remote-components".to_owned(),
            "--no-cookies".to_owned(),
            "--no-cookies-from-browser".to_owned(),
            "--js-runtimes".to_owned(),
            format!("deno:{}", self.config.deno_path.display()),
        ];
        if provider_family == Some(YtDlpProviderFamily::Youtube) {
            args.extend([
                "--extractor-args".to_owned(),
                format!("youtubepot-bgutilhttp:base_url={}", self.config.pot_provider_url),
            ]);
        }
        args
    }

    async fn resolve_tco_target(&self, source_url: &str) -> Result<String, DownloadError> {
        resolve_tco_target_with_resolver(
            source_url,
            self.resolver.as_ref(),
            self.inspection_limits.timeout,
            self.inspection_limits.max_redirects.min(MAX_TCO_REDIRECTS),
            &self.tco_client_factory,
        )
        .await
    }
}

impl YtDlpDownloader {
    async fn download_with_format(
        &self,
        inspection: &SourceInspection,
        destination: &Path,
        limits: &DownloadLimits,
        attempt_max_bytes: u64,
        format_selection: &str,
        provider_family: Option<YtDlpProviderFamily>,
    ) -> Result<DownloadedSource, DownloadError> {
        let source_url = inspection.resolved_url.as_deref().unwrap_or(&inspection.source_url);
        let source_url = validate_source_url(source_url)?;
        validate_format_selection(format_selection)
            .map_err(|error| DownloadError::terminal("ytdlp_format", error.to_string()))?;
        let attempt_path = destination.with_file_name(format!(".sooqa-ytdlp-{}", Uuid::new_v4()));
        let attempt = TempDirectory::reserve(attempt_path).await.map_err(|source| {
            DownloadError::terminal(
                "destination_io",
                format!("could not reserve yt-dlp attempt directory: {source}"),
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
        args.extend(self.security_args(provider_family));
        args.extend([
            "--paths".to_owned(),
            "home:.".to_owned(),
            "--paths".to_owned(),
            "temp:.".to_owned(),
            "--max-filesize".to_owned(),
            limits.max_bytes.to_string(),
            "--format".to_owned(),
            format_selection.to_owned(),
            "--output".to_owned(),
            "final.%(ext)s".to_owned(),
            "--".to_owned(),
            source_url.to_owned(),
        ]);
        self.run_sequence(
            args,
            limits.timeout,
            &source_url,
            attempt.path(),
            attempt_max_bytes,
            provider_family,
        )
        .await?;

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
            selected_format: Some(format_selection.to_owned()),
        })
    }
}

#[async_trait]
impl SourceDownloader for YtDlpDownloader {
    async fn inspect(&self, source: &SourceInput) -> Result<SourceInspection, DownloadError> {
        validate_limits(&self.inspection_limits, self.inspection_limits.timeout)?;
        let submitted_url = validate_source_url(&source.source_url)?;
        let extraction_url = if is_tco_url(&submitted_url)? {
            self.resolve_tco_target(&submitted_url).await?
        } else {
            submitted_url.clone()
        };
        let mut args = vec![
            "--dump-single-json".to_owned(),
            "--skip-download".to_owned(),
            "--no-playlist".to_owned(),
            "--no-warnings".to_owned(),
            "--no-progress".to_owned(),
        ];
        let submitted_provider_family = provider_family_for_source_url(&submitted_url)?;
        if submitted_provider_family == Some(YtDlpProviderFamily::X)
            || provider_family_for_canonical_url(&extraction_url)? == Some(YtDlpProviderFamily::X)
        {
            validate_public_video_path(YtDlpProviderFamily::X, &extraction_url)?;
        }
        args.extend(self.security_args(submitted_provider_family));
        args.extend([
            "--format".to_owned(),
            self.config.format_selection.clone(),
            "--".to_owned(),
            extraction_url.clone(),
        ]);
        let output = self
            .run_with_output_limit(
                args,
                self.inspection_limits.timeout,
                &extraction_url,
                self.config.metadata_max_output_bytes,
            )
            .await?;
        let mut metadata = parse_metadata(&output.stdout, &extraction_url)?;
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
        let resolved_url = metadata
            .webpage_url
            .as_deref()
            .map(validate_source_url)
            .transpose()?
            .or_else(|| Some(extraction_url.clone()));
        if submitted_provider_family.is_some()
            || is_tco_url(&submitted_url)?
            || provider_family_for_canonical_url(
                resolved_url.as_deref().unwrap_or(&extraction_url),
            )?
            .is_some()
        {
            let family = validate_provider_metadata(
                &submitted_url,
                resolved_url.as_deref().unwrap_or(&extraction_url),
                media_kind,
                Some(&metadata),
            )?;
            metadata.platform = Some(family.as_str().to_owned());
        }
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
        let provider_family = provider_family_for_source_url(&inspection.source_url)
            .ok()
            .flatten()
            .or_else(|| provider_family_for_canonical_url(&source_url).ok().flatten());
        if provider_family == Some(YtDlpProviderFamily::X) {
            validate_public_video_path(YtDlpProviderFamily::X, &source_url)?;
        }
        let attempt_max_bytes =
            limits.max_bytes.checked_mul(YTDLP_ATTEMPT_MAX_BYTES_MULTIPLIER).ok_or_else(|| {
                DownloadError::terminal(
                    "invalid_download_limits",
                    "yt-dlp aggregate attempt byte limit overflowed",
                )
            })?;

        let high_quality_format = self.config.format_selection.clone();
        let first_attempt = self
            .download_with_format(
                inspection,
                destination,
                limits,
                attempt_max_bytes,
                &high_quality_format,
                provider_family,
            )
            .await;
        match first_attempt {
            Ok(downloaded) => return Ok(downloaded),
            Err(error) if is_media_data_forbidden(&error) => {}
            Err(error) => return Err(error),
        }

        // A second process gets fresh extractor/plugin state. Only the
        // specific media-byte 403 is eligible for this bounded recovery path;
        // private, removed, account-required, and other extractor outcomes
        // remain terminal.
        let second_attempt = self
            .download_with_format(
                inspection,
                destination,
                limits,
                attempt_max_bytes,
                &high_quality_format,
                provider_family,
            )
            .await;
        match second_attempt {
            Ok(downloaded) => Ok(downloaded),
            Err(error) if is_media_data_forbidden(&error) => self
                .download_with_format(
                    inspection,
                    destination,
                    limits,
                    attempt_max_bytes,
                    YTDLP_PROGRESSIVE_FALLBACK_FORMAT,
                    provider_family,
                )
                .await
                .map_err(|fallback_error| {
                    if is_media_data_forbidden(&fallback_error) {
                        DownloadError::retryable(
                            "ytdlp_media_forbidden",
                            format!(
                                "high-quality and progressive yt-dlp attempts were rejected: {}",
                                fallback_error
                            ),
                        )
                    } else {
                        fallback_error
                    }
                }),
            Err(error) => Err(error),
        }
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
    pub platform: Option<String>,
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
    #[serde(rename = "_type")]
    item_type: Option<String>,
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
    #[serde(default)]
    entries: Option<serde_json::Value>,
    playlist_count: Option<u64>,
    n_entries: Option<u64>,
    is_live: Option<bool>,
    live_status: Option<String>,
}

fn parse_metadata(input: &[u8], source_url: &str) -> Result<YtDlpMetadata, DownloadError> {
    let raw: RawYtDlpMetadata = serde_json::from_slice(input).map_err(|error| {
        DownloadError::terminal(
            "ytdlp_invalid_output",
            format!("yt-dlp returned invalid JSON: {error}"),
        )
    })?;
    if raw.entries.as_ref().is_some_and(|entries| !entries.is_null())
        || raw.playlist_count.is_some_and(|count| count > 1)
        || raw.n_entries.is_some_and(|count| count > 1)
        || raw.item_type.as_deref().is_some_and(|item_type| {
            matches!(item_type.to_ascii_lowercase().as_str(), "playlist" | "multi_video")
        })
    {
        return Err(DownloadError::terminal(
            "ytdlp_unsupported_surface",
            "yt-dlp returned a playlist or multiple media entries; submit one public video URL",
        ));
    }
    if raw.is_live == Some(true)
        || raw.live_status.as_deref().is_some_and(|status| {
            matches!(status.to_ascii_lowercase().as_str(), "is_live" | "post_live" | "is_upcoming")
        })
    {
        return Err(DownloadError::terminal(
            "ytdlp_unsupported_surface",
            "live streams and live-event pages are not supported",
        ));
    }

    let id = bounded_metadata_string(raw.id, MAX_YTDLP_METADATA_ID_BYTES, "content id")?;
    let title = bounded_metadata_string(raw.title, MAX_YTDLP_METADATA_TEXT_BYTES, "title")?;
    let webpage_url =
        bounded_metadata_string(raw.webpage_url, MAX_YTDLP_METADATA_URL_BYTES, "canonical URL")?;
    let original_url =
        bounded_metadata_string(raw.original_url, MAX_YTDLP_METADATA_URL_BYTES, "original URL")?;
    let extractor_key = bounded_metadata_string(
        raw.extractor_key,
        MAX_YTDLP_METADATA_EXTRACTOR_BYTES,
        "extractor",
    )?;
    let extractor =
        bounded_metadata_string(raw.extractor, MAX_YTDLP_METADATA_EXTRACTOR_BYTES, "extractor")?;
    let ext = bounded_metadata_string(raw.ext, MAX_YTDLP_METADATA_EXTENSION_BYTES, "extension")?;
    let mime_type_value =
        bounded_metadata_string(raw.mime_type, MAX_YTDLP_METADATA_MIME_BYTES, "MIME type")?;
    let format_id =
        bounded_metadata_string(raw.format_id, MAX_YTDLP_METADATA_ID_BYTES, "format id")?;
    let format = bounded_metadata_string(raw.format, MAX_YTDLP_METADATA_FORMAT_BYTES, "format")?;
    let thumbnail =
        bounded_metadata_string(raw.thumbnail, MAX_YTDLP_METADATA_URL_BYTES, "thumbnail URL")?;
    let uploader =
        bounded_metadata_string(raw.uploader, MAX_YTDLP_METADATA_TEXT_BYTES, "uploader")?;
    let vcodec = bounded_metadata_string(raw.vcodec, MAX_YTDLP_METADATA_ID_BYTES, "video codec")?;
    let acodec = bounded_metadata_string(raw.acodec, MAX_YTDLP_METADATA_ID_BYTES, "audio codec")?;
    let mime_type = mime_type_value.or_else(|| mime_type_for_ext(ext.as_deref()));
    Ok(YtDlpMetadata {
        id,
        title,
        webpage_url: webpage_url.or(original_url).or_else(|| Some(source_url.to_owned())),
        extractor: extractor_key.or(extractor),
        platform: None,
        duration_ms: raw.duration.and_then(duration_to_ms),
        filesize_bytes: raw.filesize.or(raw.filesize_approx),
        width: raw.width,
        height: raw.height,
        ext,
        mime_type,
        vcodec,
        acodec,
        format_id,
        format,
        thumbnail,
        uploader,
    })
}

fn bounded_metadata_string(
    value: Option<String>,
    max_bytes: usize,
    field: &str,
) -> Result<Option<String>, DownloadError> {
    let Some(value) = value else { return Ok(None) };
    if value.len() > max_bytes || value.chars().any(char::is_control) {
        return Err(DownloadError::terminal(
            "ytdlp_metadata",
            format!("yt-dlp {field} metadata exceeds its bounded text policy"),
        ));
    }
    Ok(Some(value))
}

const TIKTOK_SUBMITTED_HOSTS: &[&str] =
    &["tiktok.com", "www.tiktok.com", "vm.tiktok.com", "vt.tiktok.com", "m.tiktok.com"];
const INSTAGRAM_SUBMITTED_HOSTS: &[&str] = &["instagram.com", "www.instagram.com"];
const X_SUBMITTED_HOSTS: &[&str] = &["x.com", "www.x.com", "twitter.com", "www.twitter.com"];
const TIKTOK_CANONICAL_HOSTS: &[&str] =
    &["tiktok.com", "www.tiktok.com", "vm.tiktok.com", "vt.tiktok.com", "m.tiktok.com"];
const INSTAGRAM_CANONICAL_HOSTS: &[&str] = &["instagram.com", "www.instagram.com"];
const X_CANONICAL_HOSTS: &[&str] = &["x.com", "www.x.com", "twitter.com", "www.twitter.com"];

pub fn ytdlp_allowed_hosts_include_youtube(allowed_hosts: &[String]) -> bool {
    allowed_hosts
        .iter()
        .any(|host| provider_family_for_host(host, false) == Some(YtDlpProviderFamily::Youtube))
}

fn provider_family_for_source_url(
    value: &str,
) -> Result<Option<YtDlpProviderFamily>, DownloadError> {
    let normalized = validate_source_url(value)?;
    let url = Url::parse(&normalized).expect("validated source URL should parse");
    let host = url.host_str().expect("validated source URL has a host");
    Ok(provider_family_for_host(host, false))
}

fn provider_family_for_canonical_url(
    value: &str,
) -> Result<Option<YtDlpProviderFamily>, DownloadError> {
    let normalized = validate_source_url(value)?;
    let url = Url::parse(&normalized).expect("validated canonical URL should parse");
    let host = url.host_str().expect("validated source URL has a host");
    Ok(provider_family_for_host(host, true))
}

pub(crate) fn provider_family_for_host(host: &str, canonical: bool) -> Option<YtDlpProviderFamily> {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    if is_domain_or_subdomain(&host, "youtube.com") || is_domain_or_subdomain(&host, "youtu.be") {
        return Some(YtDlpProviderFamily::Youtube);
    }
    let tiktok_hosts = if canonical { TIKTOK_CANONICAL_HOSTS } else { TIKTOK_SUBMITTED_HOSTS };
    if tiktok_hosts.iter().any(|candidate| *candidate == host) {
        return Some(YtDlpProviderFamily::Tiktok);
    }
    let instagram_hosts =
        if canonical { INSTAGRAM_CANONICAL_HOSTS } else { INSTAGRAM_SUBMITTED_HOSTS };
    if instagram_hosts.iter().any(|candidate| *candidate == host) {
        return Some(YtDlpProviderFamily::Instagram);
    }
    let x_hosts = if canonical { X_CANONICAL_HOSTS } else { X_SUBMITTED_HOSTS };
    if x_hosts.iter().any(|candidate| *candidate == host) {
        return Some(YtDlpProviderFamily::X);
    }
    None
}

fn is_domain_or_subdomain(host: &str, domain: &str) -> bool {
    host == domain || host.ends_with(&format!(".{domain}"))
}

fn is_tco_url(value: &str) -> Result<bool, DownloadError> {
    let normalized = validate_source_url(value)?;
    let url = Url::parse(&normalized).expect("validated source URL should parse");
    Ok(url.host_str().is_some_and(|host| host.eq_ignore_ascii_case("t.co")))
}

async fn resolve_tco_target_with_resolver(
    source_url: &str,
    resolver: &dyn HostResolver,
    timeout: Duration,
    max_redirects: u32,
    client_factory: &TcoClientFactory,
) -> Result<String, DownloadError> {
    let source_url = validate_source_url(source_url)?;
    let mut current_url = Url::parse(&source_url).expect("validated source URL should parse");
    if !is_tco_url(&source_url)? {
        return Err(DownloadError::terminal(
            "source_redirect_not_allowed",
            "the t.co preflight requires a t.co source URL",
        ));
    }

    for redirect_number in 0..=max_redirects {
        let target = direct_http::resolve_target(resolver, &current_url).await?;
        let client = client_factory(&target, timeout)?;
        let response = client
            .get(target.url)
            .header(reqwest::header::RANGE, "bytes=0-0")
            .header(reqwest::header::USER_AGENT, TCO_PREFLIGHT_USER_AGENT)
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() || error.is_connect() {
                    DownloadError::retryable(
                        "tco_resolution",
                        "the t.co redirect preflight could not reach its target",
                    )
                } else {
                    DownloadError::terminal(
                        "tco_resolution",
                        "the t.co redirect preflight request failed",
                    )
                }
            })?;

        if response.status().is_redirection() {
            if redirect_number >= max_redirects {
                return Err(DownloadError::terminal(
                    "redirect_limit",
                    "the t.co redirect chain exceeded the configured limit",
                ));
            }
            let location = response
                .headers()
                .get(reqwest::header::LOCATION)
                .ok_or_else(|| {
                    DownloadError::terminal(
                        "source_redirect_not_allowed",
                        "the t.co redirect response did not include a Location header",
                    )
                })?
                .to_str()
                .map_err(|_| {
                    DownloadError::terminal(
                        "source_redirect_not_allowed",
                        "the t.co redirect Location header was not valid UTF-8",
                    )
                })?;
            current_url = validate_tco_redirect_target(&current_url, location)?;
            continue;
        }

        if provider_family_for_host(
            current_url.host_str().expect("validated URL has a host"),
            false,
        ) == Some(YtDlpProviderFamily::X)
        {
            validate_public_video_path(YtDlpProviderFamily::X, current_url.as_str())?;
            return Ok(current_url.to_string());
        }

        return Err(DownloadError::terminal(
            "source_redirect_not_allowed",
            "the t.co URL did not resolve to an X/Twitter page",
        ));
    }

    Err(DownloadError::terminal(
        "redirect_limit",
        "the t.co redirect chain exceeded the configured limit",
    ))
}

fn validate_tco_redirect_target(current_url: &Url, location: &str) -> Result<Url, DownloadError> {
    if location.len() > MAX_TCO_LOCATION_BYTES || location.chars().any(char::is_control) {
        return Err(DownloadError::terminal(
            "source_redirect_not_allowed",
            "the t.co redirect Location header exceeded its safety limit",
        ));
    }

    let target = current_url.join(location).map_err(|_| {
        DownloadError::terminal(
            "source_redirect_not_allowed",
            "the t.co redirect Location header was not a valid URL",
        )
    })?;
    let normalized = validate_source_url(target.as_str()).map_err(|_| {
        DownloadError::terminal(
            "source_redirect_not_allowed",
            "the t.co redirect target failed URL validation",
        )
    })?;
    let target = Url::parse(&normalized).expect("validated redirect URL should parse");
    let is_x_target = target
        .host_str()
        .is_some_and(|host| provider_family_for_host(host, false) == Some(YtDlpProviderFamily::X));
    if !is_x_target {
        return Err(DownloadError::terminal(
            "source_redirect_not_allowed",
            "the t.co redirect target is not an X/Twitter host",
        ));
    }
    Ok(target)
}

fn extractor_matches_provider(family: YtDlpProviderFamily, extractor: &str) -> bool {
    let extractor = extractor.trim().to_ascii_lowercase();
    match family {
        YtDlpProviderFamily::Youtube => {
            matches!(extractor.as_str(), "youtube" | "youtubeshorts" | "youtube:shorts")
        }
        YtDlpProviderFamily::Tiktok => extractor == "tiktok",
        YtDlpProviderFamily::Instagram => extractor == "instagram",
        YtDlpProviderFamily::X => matches!(extractor.as_str(), "twitter" | "x"),
    }
}

fn validate_provider_metadata(
    source_url: &str,
    canonical_url: &str,
    media_kind: SourceMediaKind,
    metadata: Option<&YtDlpMetadata>,
) -> Result<YtDlpProviderFamily, DownloadError> {
    let submitted_family = provider_family_for_source_url(source_url)?;
    let canonical_family = provider_family_for_canonical_url(canonical_url)?;
    let is_short_link = is_tco_url(source_url)?;
    let family = match (submitted_family, canonical_family, is_short_link) {
        (Some(submitted), Some(canonical), false) if submitted == canonical => submitted,
        (None, Some(YtDlpProviderFamily::X), true) => YtDlpProviderFamily::X,
        (Some(_), Some(_), false) => {
            return Err(DownloadError::terminal(
                "source_provider_mismatch",
                "yt-dlp canonical URL belongs to a different provider family",
            ));
        }
        (None, _, true) => {
            return Err(DownloadError::terminal(
                "source_provider_mismatch",
                "t.co short links must resolve to an X/Twitter video post",
            ));
        }
        (None, _, false) => {
            return Err(DownloadError::terminal(
                "source_provider_not_supported",
                "yt-dlp returned a URL outside the supported provider families",
            ));
        }
        (Some(_), None, false) => {
            return Err(DownloadError::terminal(
                "source_provider_mismatch",
                "yt-dlp canonical URL is not a supported host for the submitted provider",
            ));
        }
        (Some(_), _, true) => {
            return Err(DownloadError::terminal(
                "source_provider_mismatch",
                "short-link handling is only available for t.co submissions",
            ));
        }
    };

    if matches!(
        family,
        YtDlpProviderFamily::Tiktok | YtDlpProviderFamily::Instagram | YtDlpProviderFamily::X
    ) && media_kind != SourceMediaKind::Video
    {
        return Err(DownloadError::terminal(
            "source_media_kind_not_supported",
            "the submitted provider URL did not resolve to a video",
        ));
    }
    validate_public_video_path(family, canonical_url)?;

    if let Some(metadata) = metadata {
        if let Some(metadata_url) = metadata.webpage_url.as_deref() {
            let metadata_url = validate_source_url(metadata_url)?;
            let canonical_url = validate_source_url(canonical_url)?;
            if metadata_url != canonical_url {
                return Err(DownloadError::terminal(
                    "source_provider_mismatch",
                    "yt-dlp metadata canonical URL disagrees with the inspected URL",
                ));
            }
        }
        let Some(extractor) = metadata.extractor.as_deref() else {
            return Err(DownloadError::terminal(
                "source_extractor_not_allowed",
                "yt-dlp did not report a provider extractor identity",
            ));
        };
        if !extractor_matches_provider(family, extractor) {
            return Err(DownloadError::terminal(
                "source_extractor_not_allowed",
                "yt-dlp extractor identity does not match the submitted provider family",
            ));
        }
        if metadata.platform.as_deref().is_some_and(|platform| platform != family.as_str()) {
            return Err(DownloadError::terminal(
                "source_provider_mismatch",
                "yt-dlp metadata provider identity does not match its canonical URL",
            ));
        }
        if matches!(
            family,
            YtDlpProviderFamily::Tiktok | YtDlpProviderFamily::Instagram | YtDlpProviderFamily::X
        ) && metadata.media_kind() != SourceMediaKind::Video
        {
            return Err(DownloadError::terminal(
                "source_media_kind_not_supported",
                "yt-dlp metadata describes a non-video result",
            ));
        }
    }

    Ok(family)
}

fn validate_public_video_path(
    family: YtDlpProviderFamily,
    canonical_url: &str,
) -> Result<(), DownloadError> {
    let url = Url::parse(canonical_url).map_err(|_| {
        DownloadError::terminal("invalid_source_url", "yt-dlp canonical URL could not be parsed")
    })?;
    let segments = url.path().split('/').filter(|segment| !segment.is_empty()).collect::<Vec<_>>();
    let supported = match family {
        YtDlpProviderFamily::Youtube => true,
        YtDlpProviderFamily::Tiktok => {
            segments.len() == 3
                && segments[0].starts_with('@')
                && segments[1].eq_ignore_ascii_case("video")
                && !segments[2].is_empty()
        }
        YtDlpProviderFamily::Instagram => {
            segments.len() == 2
                && matches!(segments[0].to_ascii_lowercase().as_str(), "reel" | "p")
                && !segments[1].is_empty()
        }
        YtDlpProviderFamily::X => {
            segments.len() == 3
                && segments[1].eq_ignore_ascii_case("status")
                && !segments[0].is_empty()
                && !segments[2].is_empty()
        }
    };
    if supported {
        Ok(())
    } else {
        Err(DownloadError::terminal(
            "ytdlp_unsupported_surface",
            "only a single public video post/reel URL is supported for this provider",
        ))
    }
}

pub(crate) fn validate_provider_inspection(
    inspection: &SourceInspection,
) -> Result<YtDlpProviderFamily, DownloadError> {
    let canonical_url = inspection.resolved_url.as_deref().unwrap_or(&inspection.source_url);
    let metadata = match inspection.metadata.as_object() {
        Some(object) if !object.is_empty() => {
            Some(serde_json::from_value::<YtDlpMetadata>(inspection.metadata.clone()).map_err(
                |error| {
                    DownloadError::terminal(
                        "ytdlp_metadata",
                        format!("yt-dlp inspection metadata is invalid: {error}"),
                    )
                },
            )?)
        }
        _ => None,
    };
    validate_provider_metadata(
        &inspection.source_url,
        canonical_url,
        inspection.media_kind,
        metadata.as_ref(),
    )
}

fn validate_format_selection(value: &str) -> Result<(), YtDlpConfigError> {
    if value.trim().is_empty() {
        return Err(YtDlpConfigError::EmptyFormatSelection);
    }
    if value.len() > MAX_YTDLP_FORMAT_SELECTION_BYTES {
        return Err(YtDlpConfigError::FormatSelectionTooLong);
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

fn pot_provider_ping_url(value: &str) -> Result<Url, String> {
    let mut url =
        Url::parse(value).map_err(|_| "PO-token provider URL could not be parsed".to_owned())?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !matches!(url.path(), "" | "/")
    {
        return Err(
            "PO-token provider URL must be an HTTP(S) origin without credentials, path, query, or fragment"
                .to_owned(),
        );
    }
    url.set_path("/ping");
    Ok(url)
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

fn map_process_failure(
    output: &ExternalCommandOutput,
    source_url: &str,
    allow_media_data_recovery: bool,
    provider_family: Option<YtDlpProviderFamily>,
) -> DownloadError {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().replace(source_url, "<source-url>");
    let message = if stderr.is_empty() {
        format!("yt-dlp exited unsuccessfully with status {:?}", output.exit_code)
    } else {
        format!("yt-dlp exited unsuccessfully with status {:?}: {stderr}", output.exit_code)
    };
    if allow_media_data_recovery
        && provider_family == Some(YtDlpProviderFamily::Youtube)
        && is_media_data_forbidden_message(&stderr)
    {
        DownloadError::retryable("ytdlp_media_forbidden", message)
    } else if is_auth_required_message(&stderr) {
        DownloadError::terminal(
            "ytdlp_auth_required",
            "the provider requires an account, login, or private-content access",
        )
    } else if is_content_unavailable_message(&stderr) {
        DownloadError::terminal(
            "ytdlp_content_unavailable",
            "the requested provider video is removed or unavailable",
        )
    } else if is_unsupported_surface_message(&stderr) {
        DownloadError::terminal(
            "ytdlp_unsupported_surface",
            "the provider URL is not a supported single public video surface",
        )
    } else if is_transient_failure(&stderr) {
        DownloadError::retryable("ytdlp_upstream", message)
    } else {
        DownloadError::terminal("ytdlp_process", message)
    }
}

fn map_inspection_failure(output: &ExternalCommandOutput, source_url: &str) -> DownloadError {
    map_process_failure(output, source_url, false, None)
}

fn map_download_failure(
    output: &ExternalCommandOutput,
    source_url: &str,
    provider_family: Option<YtDlpProviderFamily>,
) -> DownloadError {
    map_process_failure(output, source_url, true, provider_family)
}

fn is_media_data_forbidden(error: &DownloadError) -> bool {
    matches!(error, DownloadError::Retryable { class, .. } if class == "ytdlp_media_forbidden")
}

fn is_media_data_forbidden_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("unable to download video data")
        && (message.contains("http error 403") || message.contains("http 403"))
}

fn is_auth_required_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    [
        "sign in",
        "log in",
        "login required",
        "private video",
        "private post",
        "private account",
        "private content",
        "followers-only",
        "followers only",
        "only followers",
        "account required",
        "age-restricted",
        "age restricted",
        "confirm your age",
    ]
    .iter()
    .any(|marker| message.contains(marker))
}

fn is_content_unavailable_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    [
        "video unavailable",
        "this video is unavailable",
        "content is unavailable",
        "content isn't available",
        "content is not available",
        "post not found",
        "does not exist",
        "has been removed",
        "was removed",
        "has been deleted",
        "was deleted",
        "no longer available",
    ]
    .iter()
    .any(|marker| message.contains(marker))
}

fn is_unsupported_surface_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    [
        "unsupported url",
        "unsupported page",
        "playlist",
        "profile",
        "user page",
        "feed",
        "story",
        "stories",
        "live stream",
        "live event",
        "space",
        "spaces",
        "carousel",
        "gallery",
        "image-only",
        "not a video",
        "multiple entries",
    ]
    .iter()
    .any(|marker| message.contains(marker))
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
        collections::HashMap,
        net::{IpAddr, Ipv4Addr},
        os::unix::fs::PermissionsExt,
        path::PathBuf,
        process::{Command, Stdio},
        sync::{Arc, Mutex},
    };

    use super::*;
    use crate::{HostResolver, ResolvedAddress};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    #[derive(Clone)]
    struct StaticResolver {
        addresses: HashMap<String, Vec<ResolvedAddress>>,
    }

    #[async_trait]
    impl HostResolver for StaticResolver {
        async fn resolve(
            &self,
            host: &str,
            _port: u16,
        ) -> Result<Vec<ResolvedAddress>, DownloadError> {
            self.addresses.get(host).cloned().ok_or_else(|| {
                DownloadError::terminal("dns_resolution", "test resolver has no such host")
            })
        }
    }

    struct RecordingRunner {
        calls: Mutex<Vec<ExternalCommand>>,
    }

    #[async_trait]
    impl ExternalCommandRunner for RecordingRunner {
        async fn run(
            &self,
            command: ExternalCommand,
        ) -> Result<ExternalCommandOutput, CommandError> {
            self.calls.lock().expect("test mutex should not be poisoned").push(command);
            Ok(ExternalCommandOutput {
                success: true,
                exit_code: Some(0),
                stdout: br#"{"id":"abc","title":"Example","webpage_url":"http://x.com/creator/status/123","extractor":"Twitter","duration":3.5,"filesize":17,"width":1280,"height":720,"ext":"mp4","vcodec":"h264","acodec":"aac","format_id":"best"}"#.to_vec(),
                stderr: Vec::new(),
                stdout_truncated: false,
                stderr_truncated: false,
            })
        }
    }

    struct RuntimeProbeRunner {
        calls: Mutex<Vec<ExternalCommand>>,
        plugin_discovered: bool,
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
                    stderr: format!(
                        "[debug] Optional libraries: yt_dlp_ejs-0.8.0\n[debug] JS runtimes: deno-2.8.1\n{}",
                        if self.plugin_discovered {
                            "[debug] Plugin directories: /usr/local/share/sooqa/yt-dlp-plugins/bgutil-ytdlp-pot-provider-1.3.1.zip/yt_dlp_plugins\n"
                        } else {
                            ""
                        }
                    )
                    .into_bytes(),
                    stdout_truncated: false,
                    stderr_truncated: false,
                }
            })
        }
    }

    #[tokio::test]
    async fn runtime_probe_requires_bundled_ejs_and_executes_deno() {
        let runner =
            Arc::new(RuntimeProbeRunner { calls: Mutex::new(Vec::new()), plugin_discovered: true });
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

    #[test]
    fn provider_argument_does_not_force_youtube_client_selection() {
        let downloader = YtDlpDownloader::new(
            YtDlpConfig::new("yt-dlp", "best").expect("format selection should be valid"),
        );
        let args = downloader.security_args(Some(YtDlpProviderFamily::Youtube));

        assert!(args.iter().any(|argument| {
            argument == "youtubepot-bgutilhttp:base_url=http://127.0.0.1:4416"
        }));
        assert!(!args.iter().any(|argument| argument == "youtube:player-client=mweb"));

        let social_args = downloader.security_args(Some(YtDlpProviderFamily::Tiktok));
        assert!(!social_args.iter().any(|argument| argument == "--extractor-args"));
    }

    #[test]
    fn metadata_parser_rejects_playlists_live_pages_and_oversized_text() {
        for input in [
            br#"{"id":"playlist","entries":[],"extractor":"TikTok"}"# as &[u8],
            br#"{"id":"live","is_live":true,"extractor":"Instagram"}"# as &[u8],
        ] {
            let error = parse_metadata(input, "https://www.tiktok.com/@creator/video/123")
                .expect_err("unsupported surfaces must be terminal");
            assert_eq!(error.class(), "ytdlp_unsupported_surface");
        }

        let oversized_title = format!(
            r#"{{"id":"id","title":"{}","extractor":"TikTok"}}"#,
            "x".repeat(MAX_YTDLP_METADATA_TEXT_BYTES + 1)
        );
        let error =
            parse_metadata(oversized_title.as_bytes(), "https://www.tiktok.com/@creator/video/123")
                .expect_err("oversized metadata must be terminal");
        assert_eq!(error.class(), "ytdlp_metadata");
    }

    #[test]
    fn media_byte_403_recovery_is_youtube_only() {
        let output = ExternalCommandOutput {
            success: false,
            exit_code: Some(1),
            stdout: Vec::new(),
            stderr: b"ERROR: unable to download video data: HTTP Error 403: Forbidden".to_vec(),
            stdout_truncated: false,
            stderr_truncated: false,
        };
        let youtube = map_download_failure(
            &output,
            "https://www.youtube.com/watch?v=video",
            Some(YtDlpProviderFamily::Youtube),
        );
        assert_eq!(youtube.class(), "ytdlp_media_forbidden");
        assert!(youtube.is_retryable());

        let tiktok = map_download_failure(
            &output,
            "https://www.tiktok.com/@creator/video/123",
            Some(YtDlpProviderFamily::Tiktok),
        );
        assert_eq!(tiktok.class(), "ytdlp_process");
        assert!(!tiktok.is_retryable());
    }

    #[test]
    fn unsupported_surface_process_errors_are_terminal_and_explicit() {
        let error = map_download_failure(
            &ExternalCommandOutput {
                success: false,
                exit_code: Some(1),
                stdout: Vec::new(),
                stderr: b"ERROR: profile pages are not supported with --no-playlist".to_vec(),
                stdout_truncated: false,
                stderr_truncated: false,
            },
            "https://www.instagram.com/creator/",
            Some(YtDlpProviderFamily::Instagram),
        );
        assert_eq!(error.class(), "ytdlp_unsupported_surface");
        assert!(!error.is_retryable());
    }

    #[tokio::test]
    async fn runtime_probe_rejects_a_missing_pinned_plugin() {
        let runner = Arc::new(RuntimeProbeRunner {
            calls: Mutex::new(Vec::new()),
            plugin_discovered: false,
        });
        let downloader = YtDlpDownloader::with_runner(
            YtDlpConfig::new("yt-dlp", "best").expect("format selection should be valid"),
            DownloadLimits::default(),
            Arc::clone(&runner) as Arc<dyn ExternalCommandRunner>,
        );

        let error = downloader
            .verify_runtime(Duration::from_secs(1))
            .await
            .expect_err("missing plugin should fail the runtime preflight");
        assert!(error.contains("pinned PO-token provider plugin"));
    }

    async fn provider_fixture(status: &str, body: &str) -> (String, tokio::task::JoinHandle<()>) {
        provider_fixture_with_headers(status, "", body).await
    }

    async fn provider_fixture_with_headers(
        status: &str,
        headers: &str,
        body: &str,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener =
            TcpListener::bind("127.0.0.1:0").await.expect("provider fixture should bind");
        let address = listener.local_addr().expect("provider fixture address should be known");
        let status = status.to_owned();
        let headers = headers.to_owned();
        let body = body.to_owned();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("provider request should arrive");
            let mut request = [0_u8; 1024];
            let read =
                stream.read(&mut request).await.expect("provider request should be readable");
            assert!(String::from_utf8_lossy(&request[..read]).starts_with("GET /ping"));
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{headers}Connection: close\r\n\r\n{body}",
                body.len(),
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("provider response should be writable");
        });
        (format!("http://{address}"), server)
    }

    #[tokio::test]
    async fn provider_preflight_requires_the_pinned_provider_version() {
        let (provider_url, server) =
            provider_fixture("200 OK", r#"{"server_uptime":1.0,"version":"1.3.1"}"#).await;
        let downloader = YtDlpDownloader::new(
            YtDlpConfig::new("yt-dlp", "best")
                .expect("format selection should be valid")
                .with_pot_provider_url(provider_url),
        );

        downloader
            .verify_pot_provider(Duration::from_secs(1))
            .await
            .expect("pinned provider fixture should pass");
        server.await.expect("provider fixture should finish");
    }

    #[tokio::test]
    async fn provider_preflight_rejects_redirects() {
        let (provider_url, server) = provider_fixture_with_headers(
            "302 Found",
            "Location: http://example.invalid/ping\r\n",
            r#"{"server_uptime":1.0,"version":"1.3.1"}"#,
        )
        .await;
        let downloader = YtDlpDownloader::new(
            YtDlpConfig::new("yt-dlp", "best")
                .expect("format selection should be valid")
                .with_pot_provider_url(provider_url),
        );

        let error = downloader
            .verify_pot_provider(Duration::from_secs(1))
            .await
            .expect_err("provider redirects should fail preflight");
        assert!(error.contains("PO-token provider returned HTTP 302"));
        server.await.expect("provider fixture should finish");
    }

    #[tokio::test]
    async fn provider_preflight_does_not_echo_untrusted_version_text() {
        let (provider_url, server) =
            provider_fixture("200 OK", r#"{"version":"unexpected-secret-text"}"#).await;
        let downloader = YtDlpDownloader::new(
            YtDlpConfig::new("yt-dlp", "best")
                .expect("format selection should be valid")
                .with_pot_provider_url(provider_url),
        );

        let error = downloader
            .verify_pot_provider(Duration::from_secs(1))
            .await
            .expect_err("wrong provider version should fail preflight");
        assert!(error.contains("PO-token provider version mismatch"));
        assert!(!error.contains("unexpected-secret-text"));
        server.await.expect("provider fixture should finish");
    }

    #[tokio::test]
    async fn provider_preflight_reports_unavailable_provider_without_media_details() {
        let (provider_url, server) = provider_fixture("503 Service Unavailable", "{}").await;
        let downloader = YtDlpDownloader::new(
            YtDlpConfig::new("yt-dlp", "best")
                .expect("format selection should be valid")
                .with_pot_provider_url(provider_url),
        );

        let error = downloader
            .verify_pot_provider(Duration::from_secs(1))
            .await
            .expect_err("unavailable provider should fail preflight");
        assert!(error.contains("PO-token provider returned HTTP 503"));
        server.await.expect("provider fixture should finish");
    }

    #[tokio::test]
    async fn provider_preflight_bounds_health_response_bytes() {
        let oversized_body = "x".repeat(MAX_POT_PROVIDER_RESPONSE_BYTES + 1);
        let (provider_url, server) = provider_fixture("200 OK", &oversized_body).await;
        let downloader = YtDlpDownloader::new(
            YtDlpConfig::new("yt-dlp", "best")
                .expect("format selection should be valid")
                .with_pot_provider_url(provider_url),
        );

        let error = downloader
            .verify_pot_provider(Duration::from_secs(1))
            .await
            .expect_err("oversized provider response should fail preflight");
        assert!(error.contains("oversized health response"));
        server.await.expect("provider fixture should finish");
    }

    fn tco_redirect_response(location: &str) -> String {
        format!(
            "HTTP/1.1 302 Found\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        )
    }

    async fn tco_redirect_fixture(
        responses: Vec<String>,
    ) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let listener =
            TcpListener::bind("127.0.0.1:0").await.expect("t.co redirect fixture should bind");
        let address = listener.local_addr().expect("t.co redirect fixture address should be known");
        let server = tokio::spawn(async move {
            for response in responses {
                let (mut stream, _) =
                    listener.accept().await.expect("t.co redirect request should arrive");
                let mut request = [0_u8; 4096];
                let read = stream
                    .read(&mut request)
                    .await
                    .expect("t.co redirect request should be readable");
                assert!(read > 0, "t.co redirect request should not be empty");
                stream
                    .write_all(response.as_bytes())
                    .await
                    .expect("t.co redirect response should be writable");
            }
        });
        (address, server)
    }

    fn tco_resolver(address: std::net::SocketAddr, x_ip: IpAddr) -> StaticResolver {
        let tco_ip = IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34));
        StaticResolver {
            addresses: HashMap::from([
                ("t.co".to_owned(), vec![ResolvedAddress { ip: tco_ip, connect_ip: address.ip() }]),
                ("x.com".to_owned(), vec![ResolvedAddress { ip: x_ip, connect_ip: address.ip() }]),
            ]),
        }
    }

    fn tco_test_client_factory(address: std::net::SocketAddr) -> TcoClientFactory {
        Arc::new(move |target, timeout| {
            let host = target.url.host_str().expect("test target should have a host");
            reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(timeout)
                .connect_timeout(timeout)
                .resolve(host, address)
                .build()
                .map_err(|error| {
                    DownloadError::terminal(
                        "http_client",
                        format!("could not build test HTTP client: {error}"),
                    )
                })
        })
    }

    #[tokio::test]
    async fn tco_inspection_pins_safe_dns_and_preserves_submitted_provenance() {
        let (address, server) = tco_redirect_fixture(vec![
            tco_redirect_response("http://x.com/creator/status/123"),
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_owned(),
        ])
        .await;
        let runner = Arc::new(RecordingRunner { calls: Mutex::new(Vec::new()) });
        let downloader = YtDlpDownloader::with_runner_resolver_and_client(
            YtDlpConfig::new("yt-dlp", "best").expect("format selection should be valid"),
            DownloadLimits { max_bytes: 1024, max_redirects: 2, timeout: Duration::from_secs(1) },
            Arc::clone(&runner) as Arc<dyn ExternalCommandRunner>,
            Arc::new(tco_resolver(address, IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34)))),
            tco_test_client_factory(address),
        );
        let inspection = downloader
            .inspect(&SourceInput {
                ingest_request_id: Uuid::new_v4(),
                source_url: "http://t.co/short-link".to_owned(),
                page_url: None,
            })
            .await
            .expect("t.co should resolve to the X status URL");

        assert_eq!(inspection.source_url, "http://t.co/short-link");
        assert_eq!(inspection.resolved_url.as_deref(), Some("http://x.com/creator/status/123"));
        {
            let calls = runner.calls.lock().expect("test mutex should not be poisoned");
            assert_eq!(calls.len(), 1);
            assert_eq!(
                calls[0].args().last().expect("yt-dlp URL argument should exist").to_string_lossy(),
                "http://x.com/creator/status/123"
            );
        }
        server.await.expect("t.co redirect fixture should finish");
    }

    #[tokio::test]
    async fn tco_inspection_rejects_forbidden_dns_before_starting_ytdlp() {
        let (address, server) =
            tco_redirect_fixture(vec![tco_redirect_response("http://x.com/creator/status/123")])
                .await;
        let runner = Arc::new(RecordingRunner { calls: Mutex::new(Vec::new()) });
        let downloader = YtDlpDownloader::with_runner_resolver_and_client(
            YtDlpConfig::new("yt-dlp", "best").expect("format selection should be valid"),
            DownloadLimits { max_bytes: 1024, max_redirects: 2, timeout: Duration::from_secs(1) },
            Arc::clone(&runner) as Arc<dyn ExternalCommandRunner>,
            Arc::new(tco_resolver(address, IpAddr::V4(Ipv4Addr::LOCALHOST))),
            tco_test_client_factory(address),
        );
        let error = downloader
            .inspect(&SourceInput {
                ingest_request_id: Uuid::new_v4(),
                source_url: "http://t.co/short-link".to_owned(),
                page_url: None,
            })
            .await
            .expect_err("a forbidden X DNS result must be blocked");

        assert!(matches!(
            error,
            DownloadError::Terminal { ref class, .. } if class == "ssrf_blocked"
        ));
        assert!(runner.calls.lock().expect("test mutex should not be poisoned").is_empty());
        server.await.expect("t.co redirect fixture should finish");
    }

    #[tokio::test]
    async fn tco_inspection_rejects_non_status_x_pages_before_starting_ytdlp() {
        let (address, server) = tco_redirect_fixture(vec![
            tco_redirect_response("http://x.com/home"),
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_owned(),
        ])
        .await;
        let runner = Arc::new(RecordingRunner { calls: Mutex::new(Vec::new()) });
        let downloader = YtDlpDownloader::with_runner_resolver_and_client(
            YtDlpConfig::new("yt-dlp", "best").expect("format selection should be valid"),
            DownloadLimits { max_bytes: 1024, max_redirects: 2, timeout: Duration::from_secs(1) },
            Arc::clone(&runner) as Arc<dyn ExternalCommandRunner>,
            Arc::new(tco_resolver(address, IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34)))),
            tco_test_client_factory(address),
        );
        let error = downloader
            .inspect(&SourceInput {
                ingest_request_id: Uuid::new_v4(),
                source_url: "http://t.co/short-link".to_owned(),
                page_url: None,
            })
            .await
            .expect_err("non-status X pages must be rejected during preflight");

        assert_eq!(error.class(), "ytdlp_unsupported_surface");
        assert!(runner.calls.lock().expect("test mutex should not be poisoned").is_empty());
        server.await.expect("t.co redirect fixture should finish");
    }

    #[tokio::test]
    async fn direct_non_status_x_pages_are_rejected_before_starting_ytdlp() {
        let runner = Arc::new(RecordingRunner { calls: Mutex::new(Vec::new()) });
        let downloader = YtDlpDownloader::with_runner_and_resolver(
            YtDlpConfig::new("yt-dlp", "best").expect("format selection should be valid"),
            DownloadLimits::default(),
            Arc::clone(&runner) as Arc<dyn ExternalCommandRunner>,
            Arc::new(StaticResolver { addresses: HashMap::new() }),
        );
        let error = downloader
            .inspect(&SourceInput {
                ingest_request_id: Uuid::new_v4(),
                source_url: "https://x.com/home".to_owned(),
                page_url: None,
            })
            .await
            .expect_err("non-status X pages must be rejected before inspection");

        assert_eq!(error.class(), "ytdlp_unsupported_surface");
        assert!(runner.calls.lock().expect("test mutex should not be poisoned").is_empty());
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
            source_url: "https://example.test/watch?v=abc&list=playlist&index=2".to_owned(),
            page_url: None,
        };

        let inspection = downloader.inspect(&source).await.expect("inspection should succeed");
        assert_eq!(inspection.adapter, "yt_dlp");
        assert_eq!(inspection.media_kind, SourceMediaKind::Video);
        assert_eq!(inspection.mime_type.as_deref(), Some("video/mp4"));
        assert_eq!(inspection.content_length_bytes, Some(17));
        assert_eq!(inspection.title.as_deref(), Some("Example"));
        assert_eq!(inspection.resolved_url.as_deref(), Some("https://example.test/watch?v=abc"));
        assert_eq!(inspection.metadata["duration_ms"], 3500);

        let inspect_args =
            tokio::fs::read_to_string(&args_log).await.expect("inspect arguments should be logged");
        assert!(inspect_args.lines().any(|line| line == "bestvideo*+bestaudio/best"));
        for argument in [
            "--ignore-config",
            "--no-plugin-dirs",
            "--plugin-dirs",
            YTDLP_PLUGIN_DIRECTORY,
            "--no-remote-components",
            "--no-cookies",
            "--no-cookies-from-browser",
            "--js-runtimes",
            "deno:deno",
        ] {
            assert!(
                inspect_args.lines().any(|line| line == argument),
                "missing argument: {argument}"
            );
        }
        assert!(!inspect_args.lines().any(|line| line == "--extractor-args"));
        assert!(
            !inspect_args
                .lines()
                .any(|line| line == "youtubepot-bgutilhttp:base_url=http://127.0.0.1:4416")
        );
        assert!(!inspect_args.lines().any(|line| line == "youtube:player-client=mweb"));
        assert!(
            inspect_args
                .lines()
                .any(|line| line == "https://example.test/watch?v=abc&list=playlist&index=2")
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
    async fn inspection_media_data_403_remains_terminal() {
        let root = std::env::temp_dir()
            .join(format!("sooqa-ytdlp-inspection-403-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&root).await.expect("test root should be created");
        let executable = root.join("fake-yt-dlp.sh");
        let script = "#!/bin/sh\nset -eu\nprintf '%s\\n' 'ERROR: unable to download video data: HTTP Error 403: Forbidden' >&2\nexit 1\n";
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
        let error = downloader
            .inspect(&SourceInput {
                ingest_request_id: uuid::Uuid::new_v4(),
                source_url: "https://www.youtube.com/watch?v=inspection-403".to_owned(),
                page_url: None,
            })
            .await
            .expect_err("inspection 403 should fail");
        assert_eq!(error.class(), "ytdlp_process");
        assert!(!error.is_retryable());

        tokio::fs::remove_dir_all(root).await.expect("test root should be removed");
    }

    #[tokio::test]
    async fn download_uses_the_inspected_canonical_url_for_shorts() {
        let root =
            std::env::temp_dir().join(format!("sooqa-ytdlp-shorts-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&root).await.expect("test root should be created");
        let executable = root.join("fake-yt-dlp.sh");
        let args_log = root.join("args.log");
        let script = format!(
            "#!/bin/sh\nset -eu\nlog={args_log:?}\n: > \"$log\"\nfor arg in \"$@\"; do printf '%s\\n' \"$arg\" >> \"$log\"; done\nif [ \"$1\" = \"--dump-single-json\" ]; then printf '%s\\n' '{{\"id\":\"short-id\",\"title\":\"Short\",\"webpage_url\":\"https://www.youtube.com/watch?v=short-id\",\"extractor\":\"youtube\",\"ext\":\"mp4\",\"vcodec\":\"h264\",\"acodec\":\"aac\"}}'; else printf 'short-media' > final.mp4; fi\n",
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

        let downloader = YtDlpDownloader::new(
            YtDlpConfig::new(&executable, "bestvideo*+bestaudio/best")
                .expect("format selection should be valid"),
        );
        let source = SourceInput {
            ingest_request_id: uuid::Uuid::new_v4(),
            source_url: "https://www.youtube.com/shorts/short-id".to_owned(),
            page_url: None,
        };
        let inspection =
            downloader.inspect(&source).await.expect("Shorts inspection should succeed");
        assert_eq!(inspection.source_url, source.source_url);
        assert_eq!(
            inspection.resolved_url.as_deref(),
            Some("https://www.youtube.com/watch?v=short-id")
        );
        let destination = root.join("source.mp4");

        let downloaded = downloader
            .download(&inspection, &destination, &DownloadLimits::default())
            .await
            .expect("canonical Shorts URL should download");
        assert_eq!(downloaded.selected_format.as_deref(), Some("bestvideo*+bestaudio/best"));
        let args = tokio::fs::read_to_string(&args_log).await.expect("arguments should be logged");
        assert!(args.lines().any(|line| line == "https://www.youtube.com/watch?v=short-id"));
        assert!(!args.lines().any(|line| line == "https://www.youtube.com/shorts/short-id"));

        tokio::fs::remove_dir_all(root).await.expect("test root should be removed");
    }

    #[tokio::test]
    async fn media_data_403_refreshes_high_quality_extraction_before_success() {
        let root =
            std::env::temp_dir().join(format!("sooqa-ytdlp-refresh-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&root).await.expect("test root should be created");
        let executable = root.join("fake-yt-dlp.sh");
        let count_file = root.join("attempts");
        let script = format!(
            "#!/bin/sh\nset -eu\ncount=0\nif [ -f {count_file:?} ]; then count=$(cat {count_file:?}); fi\ncount=$((count + 1))\nprintf '%s' \"$count\" > {count_file:?}\nif [ \"$count\" -eq 1 ]; then printf '%s\\n' 'ERROR: unable to download video data: HTTP Error 403: Forbidden' >&2; exit 1; fi\nprintf 'fresh-media' > final.mp4\n",
            count_file = count_file.display()
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

        let downloader = YtDlpDownloader::new(
            YtDlpConfig::new(&executable, "bestvideo*+bestaudio/best")
                .expect("format selection should be valid"),
        );
        let inspection = SourceInspection {
            adapter: "yt_dlp".to_owned(),
            source_url: "https://www.youtube.com/watch?v=refresh".to_owned(),
            resolved_url: Some("https://www.youtube.com/watch?v=refresh".to_owned()),
            media_kind: SourceMediaKind::Video,
            mime_type: Some("video/mp4".to_owned()),
            content_length_bytes: None,
            title: None,
            metadata: serde_json::json!({}),
        };
        let destination = root.join("source.mp4");

        let downloaded = downloader
            .download(&inspection, &destination, &DownloadLimits::default())
            .await
            .expect("a fresh high-quality extraction should recover");
        assert_eq!(downloaded.selected_format.as_deref(), Some("bestvideo*+bestaudio/best"));
        assert_eq!(tokio::fs::read_to_string(&count_file).await.unwrap(), "2");
        assert_eq!(tokio::fs::read(&destination).await.unwrap(), b"fresh-media");

        tokio::fs::remove_dir_all(root).await.expect("test root should be removed");
    }

    #[tokio::test]
    async fn repeated_media_data_403_uses_one_clean_progressive_fallback() {
        let root =
            std::env::temp_dir().join(format!("sooqa-ytdlp-fallback-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&root).await.expect("test root should be created");
        let executable = root.join("fake-yt-dlp.sh");
        let formats_log = root.join("formats.log");
        let script = format!(
            "#!/bin/sh\nset -eu\nformat=''\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = '--format' ]; then shift; format=$1; fi\n  shift\ndone\nprintf '%s\\n' \"$format\" >> {formats_log:?}\nif [ \"$format\" = 'bestvideo*+bestaudio/best' ]; then printf '%s\\n' 'ERROR: unable to download video data: HTTP Error 403: Forbidden' >&2; exit 1; fi\nprintf 'progressive-media' > final.mp4\n",
            formats_log = formats_log.display()
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

        let high_quality = "bestvideo*+bestaudio/best";
        let downloader = YtDlpDownloader::new(
            YtDlpConfig::new(&executable, high_quality).expect("format selection should be valid"),
        );
        let inspection = SourceInspection {
            adapter: "yt_dlp".to_owned(),
            source_url: "https://www.youtube.com/watch?v=fallback".to_owned(),
            resolved_url: Some("https://www.youtube.com/watch?v=fallback".to_owned()),
            media_kind: SourceMediaKind::Video,
            mime_type: Some("video/mp4".to_owned()),
            content_length_bytes: None,
            title: None,
            metadata: serde_json::json!({}),
        };
        let destination = root.join("source.mp4");

        let downloaded = downloader
            .download(&inspection, &destination, &DownloadLimits::default())
            .await
            .expect("progressive fallback should succeed");
        assert_eq!(downloaded.selected_format.as_deref(), Some(YTDLP_PROGRESSIVE_FALLBACK_FORMAT));
        let formats = tokio::fs::read_to_string(&formats_log).await.unwrap();
        assert_eq!(
            formats.lines().collect::<Vec<_>>(),
            [high_quality, high_quality, YTDLP_PROGRESSIVE_FALLBACK_FORMAT]
        );
        assert_eq!(tokio::fs::read(&destination).await.unwrap(), b"progressive-media");

        tokio::fs::remove_dir_all(root).await.expect("test root should be removed");
    }

    #[test]
    fn private_and_account_required_outcomes_are_not_media_data_retries() {
        for stderr in [
            "ERROR: [youtube] Private video. Sign in to watch this video.",
            "ERROR: [youtube] Sign in to confirm your age.",
            "ERROR: [youtube] This video is unavailable.",
        ] {
            let error = map_download_failure(
                &ExternalCommandOutput {
                    success: false,
                    exit_code: Some(1),
                    stdout: Vec::new(),
                    stderr: stderr.as_bytes().to_vec(),
                    stdout_truncated: false,
                    stderr_truncated: false,
                },
                "https://www.youtube.com/watch?v=terminal",
                Some(YtDlpProviderFamily::Youtube),
            );
            let expected_class = if stderr.contains("unavailable") {
                "ytdlp_content_unavailable"
            } else {
                "ytdlp_auth_required"
            };
            assert_eq!(error.class(), expected_class);
            assert!(!error.is_retryable());
        }
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
        assert_eq!(
            YtDlpConfig::new(
                PathBuf::from("yt-dlp"),
                "x".repeat(MAX_YTDLP_FORMAT_SELECTION_BYTES + 1)
            )
            .expect_err("an oversized format must be rejected"),
            YtDlpConfigError::FormatSelectionTooLong
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
