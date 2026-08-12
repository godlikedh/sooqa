//! Download, inspection, normalization, and fingerprinting boundaries for sooqa.

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

mod command;
mod diagnostics;
mod direct_http;
mod execute;
mod ffprobe;
mod fingerprint;
mod hashing;
mod image_normalize;
mod normalize;
mod publication;
mod sequence_alignment;
mod two_ch;
mod video_sequence;
mod workspace;
mod ytdlp;

pub use command::{
    CommandError, DEFAULT_COMMAND_PATH, DEFAULT_COMMAND_TIMEOUT, DEFAULT_MAX_OUTPUT_BYTES,
    ExternalCommand, ExternalCommandOutput, ExternalCommandRunner, ProcessCommandRunner,
};
pub use diagnostics::{BinaryCheck, BinaryDiagnostic, diagnose_binaries};
pub use direct_http::{DirectHttpDownloader, HostResolver, ResolvedAddress};
pub use execute::{
    FfmpegExecutor, FfmpegProgress, FfmpegProgressState, NormalizationExecutionError,
    NormalizationResult, ProgressError, parse_ffmpeg_progress,
};
pub use ffprobe::{
    FfprobeAdapter, FrameRate, MediaProbe, MediaStream, MediaStreamKind, ProbeError,
    parse_probe_json,
};
pub use fingerprint::{
    DEFAULT_MAX_FRAME_SEQUENCE_BYTES, FingerprintVersion, FrameDecodeLimits, FrameExtractionError,
    FrameExtractor, VIDEO_SEQUENCE_V1,
};
pub use hashing::{FileDigest, HashError, sha256_file};
pub(crate) use hashing::{sha256_bytes, sha256_file_sync};
pub use image_normalize::{
    CanonicalImageFormat, CanonicalImageProfile, ImageNormalizationError, ImageNormalizationPlan,
    ImageNormalizationResult, ImageNormalizer, ImageProfileError,
};
pub use normalize::{
    AudioCodec, CanonicalContainer, CanonicalVideoProfile, NormalizationError, NormalizationMode,
    NormalizationPlan, NormalizationPlanner, PixelFormat, ProfileError, VideoCodec, VideoPreset,
};
pub use sequence_alignment::{
    SequenceAlignment, SequenceAlignmentConfig, SequenceAlignmentError, SequenceClassification,
    SequenceEvidence, align_video_sequences,
};
pub use sooqa_inbox::{SourceInspection, SourceMediaKind};
pub use two_ch::{TWO_CH_MIRROR_HOSTS, TwoChMirrorDownloader};
pub use video_sequence::{
    VIDEO_SEQUENCE_BASE_INTERVAL_MS, VIDEO_SEQUENCE_CODEC_V1, VIDEO_SEQUENCE_MAGIC,
    VIDEO_SEQUENCE_MAX_ANCHORS, VIDEO_SEQUENCE_MAX_SAMPLES, VIDEO_SEQUENCE_MAX_TOKENS,
    VideoSequenceBuilder, VideoSequenceError, VideoSequenceFingerprint, VideoSequenceSample,
    derive_search_tokens, select_video_sequence_timestamps, video_sequence_interval_ms,
};
pub use workspace::{
    ManifestEntry, MediaWorkspace, WorkspaceArea, WorkspaceError, WorkspaceManifest,
};
pub use ytdlp::{
    YtDlpConfig, YtDlpConfigError, YtDlpDownloader, YtDlpMetadata, is_supported_deno_version,
};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceInput {
    pub ingest_request_id: Uuid,
    pub source_url: String,
    pub page_url: Option<String>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DownloadLimits {
    pub max_bytes: u64,
    pub max_redirects: u32,
    pub timeout: Duration,
}

impl Default for DownloadLimits {
    fn default() -> Self {
        Self {
            max_bytes: 2 * 1024 * 1024 * 1024,
            max_redirects: 5,
            timeout: Duration::from_secs(300),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DownloadedSource {
    pub path: PathBuf,
    pub bytes: u64,
    pub mime_type: Option<String>,
}

#[async_trait]
pub trait SourceDownloader: Send + Sync {
    async fn inspect(&self, source: &SourceInput) -> Result<SourceInspection, DownloadError>;

    async fn download(
        &self,
        _inspection: &SourceInspection,
        _destination: &Path,
        _limits: &DownloadLimits,
    ) -> Result<DownloadedSource, DownloadError> {
        Err(DownloadError::NotImplemented)
    }
}

#[derive(Debug, Error)]
pub enum ArtifactPublicationError {
    #[error("media artifact destination already contains different content")]
    DestinationConflict,
    #[error("media artifact publication failed: {0}")]
    Failed(String),
}

/// Publishes a completed artifact into a workspace without overwriting a
/// different existing file. Identical content is safely reused, which makes
/// lease-expiry retries converge on one destination.
pub async fn publish_artifact(
    temporary: impl AsRef<Path>,
    destination: impl AsRef<Path>,
) -> Result<(), ArtifactPublicationError> {
    publication::publish_or_reuse(temporary.as_ref(), destination.as_ref())
        .await
        .map(|_| ())
        .map_err(|error| match error {
            publication::PublishError::DestinationConflict(_) => {
                ArtifactPublicationError::DestinationConflict
            }
            error => ArtifactPublicationError::Failed(error.to_string()),
        })
}

/// Selects the direct HTTP adapter for recognizable media responses and, when
/// configured, uses yt-dlp for page-like URLs or upstream responses that
/// direct HTTP cannot handle. The selected adapter is recorded in
/// `SourceInspection` so the later download job uses the same boundary.
#[derive(Clone)]
pub struct SourceDownloaderRouter {
    direct_http: Arc<dyn SourceDownloader>,
    ytdlp: Option<Arc<dyn SourceDownloader>>,
    ytdlp_allowed_hosts: Vec<String>,
}

impl SourceDownloaderRouter {
    pub fn new(
        direct_http: Arc<dyn SourceDownloader>,
        ytdlp: Arc<dyn SourceDownloader>,
        ytdlp_allowed_hosts: Vec<String>,
    ) -> Self {
        Self { direct_http, ytdlp: Some(ytdlp), ytdlp_allowed_hosts }
    }

    /// Builds a production-safe router until the yt-dlp process has an
    /// equivalent egress/SSRF boundary.
    pub fn direct_only(direct_http: Arc<dyn SourceDownloader>) -> Self {
        Self { direct_http, ytdlp: None, ytdlp_allowed_hosts: Vec::new() }
    }
}

#[async_trait]
impl SourceDownloader for SourceDownloaderRouter {
    async fn inspect(&self, source: &SourceInput) -> Result<SourceInspection, DownloadError> {
        match self.direct_http.inspect(source).await {
            Ok(inspection) if inspection.media_kind != SourceMediaKind::Unknown => Ok(inspection),
            Ok(_) => self.inspect_with_ytdlp_policy(source).await,
            Err(error) if should_try_ytdlp(&error) => self.inspect_with_ytdlp_policy(source).await,
            Err(error) => Err(error),
        }
    }

    async fn download(
        &self,
        inspection: &SourceInspection,
        destination: &Path,
        limits: &DownloadLimits,
    ) -> Result<DownloadedSource, DownloadError> {
        match inspection.adapter.as_str() {
            "direct_http" => self.direct_http.download(inspection, destination, limits).await,
            "yt_dlp" => match &self.ytdlp {
                Some(ytdlp) => {
                    if !is_allowed_ytdlp_source(&inspection.source_url, &self.ytdlp_allowed_hosts)?
                    {
                        return Err(DownloadError::terminal(
                            "source_host_not_allowed",
                            "page URL host is not enabled for yt-dlp",
                        ));
                    }
                    ytdlp.download(inspection, destination, limits).await
                }
                None => Err(DownloadError::terminal(
                    "source_adapter_disabled",
                    "yt-dlp source adapter is not enabled in this worker",
                )),
            },
            adapter => Err(DownloadError::terminal(
                "unknown_source_adapter",
                format!("source inspection selected unsupported adapter {adapter:?}"),
            )),
        }
    }
}

impl SourceDownloaderRouter {
    async fn inspect_with_ytdlp_policy(
        &self,
        source: &SourceInput,
    ) -> Result<SourceInspection, DownloadError> {
        if !is_allowed_ytdlp_source(&source.source_url, &self.ytdlp_allowed_hosts)? {
            return Err(DownloadError::terminal(
                "source_host_not_allowed",
                "page URL host is not enabled for yt-dlp",
            ));
        }
        if self.ytdlp.is_none() {
            return Err(DownloadError::terminal(
                "unsupported_source",
                "direct HTTP did not recognize a supported media response",
            ));
        }

        self.ytdlp
            .as_ref()
            .expect("yt-dlp was checked before applying its host policy")
            .inspect(source)
            .await
    }
}

fn is_allowed_ytdlp_source(
    source_url: &str,
    allowed_hosts: &[String],
) -> Result<bool, DownloadError> {
    let url = Url::parse(source_url).map_err(|_| {
        DownloadError::terminal("invalid_source_url", "source URL could not be parsed")
    })?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(DownloadError::terminal(
            "unsupported_scheme",
            "source URL must use http or https",
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(DownloadError::terminal(
            "source_credentials_forbidden",
            "source URL must not contain embedded credentials",
        ));
    }
    let Some(host) = url.host_str() else {
        return Err(DownloadError::terminal("missing_source_host", "source URL has no host"));
    };
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

    let host = host.strip_suffix('.').unwrap_or(host);
    if host.is_empty() || host.ends_with('.') {
        return Ok(false);
    }
    let host = host.to_ascii_lowercase();
    if !matches!(url.host(), Some(url::Host::Domain(_))) {
        return Ok(false);
    }
    Ok(allowed_hosts
        .iter()
        .any(|allowed| host == *allowed || host.ends_with(&format!(".{allowed}"))))
}

fn should_try_ytdlp(error: &DownloadError) -> bool {
    matches!(error, DownloadError::Terminal { class, .. } if class == "upstream_http_status")
}

#[derive(Debug, Clone, Eq, PartialEq, Error)]
pub enum DownloadError {
    #[error("{message}")]
    Retryable { class: String, message: String },
    #[error("{message}")]
    Terminal { class: String, message: String },
    #[error("source downloader is not implemented")]
    NotImplemented,
}

impl DownloadError {
    pub fn retryable(class: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Retryable { class: class.into(), message: message.into() }
    }

    pub fn terminal(class: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Terminal { class: class.into(), message: message.into() }
    }

    pub fn class(&self) -> &str {
        match self {
            Self::Retryable { class, .. } | Self::Terminal { class, .. } => class,
            Self::NotImplemented => "downloader_not_implemented",
        }
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable { .. })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[derive(Clone)]
    struct StubDownloader {
        inspection: Result<SourceInspection, DownloadError>,
        downloads: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl SourceDownloader for StubDownloader {
        async fn inspect(&self, _source: &SourceInput) -> Result<SourceInspection, DownloadError> {
            self.inspection.clone()
        }

        async fn download(
            &self,
            _inspection: &SourceInspection,
            destination: &Path,
            _limits: &DownloadLimits,
        ) -> Result<DownloadedSource, DownloadError> {
            self.downloads.fetch_add(1, Ordering::Relaxed);
            Ok(DownloadedSource {
                path: destination.to_owned(),
                bytes: 1,
                mime_type: Some("video/mp4".to_owned()),
            })
        }
    }

    fn inspection(adapter: &str, media_kind: SourceMediaKind) -> SourceInspection {
        SourceInspection {
            adapter: adapter.to_owned(),
            source_url: "https://example.test/watch".to_owned(),
            resolved_url: None,
            media_kind,
            mime_type: Some("video/mp4".to_owned()),
            content_length_bytes: Some(1),
            title: None,
            metadata: serde_json::json!({}),
        }
    }

    fn source() -> SourceInput {
        SourceInput {
            ingest_request_id: Uuid::from_u128(1),
            source_url: "https://example.test/watch".to_owned(),
            page_url: None,
        }
    }

    #[test]
    fn ytdlp_host_policy_matches_exact_and_dot_delimited_subdomains_only() {
        let allowed = vec!["youtube.com".to_owned(), "youtu.be".to_owned()];
        for source_url in [
            "https://youtube.com/watch?v=abc",
            "https://WWW.YouTube.COM./shorts/abc",
            "https://music.youtube.com/watch?v=abc",
            "https://youtu.be/abc",
        ] {
            assert!(
                is_allowed_ytdlp_source(source_url, &allowed).expect("URL should be valid"),
                "host should be allowed: {source_url}"
            );
        }
        for source_url in [
            "https://notyoutube.com/watch?v=abc",
            "https://youtube.com.attacker.example/watch?v=abc",
            "https://127.0.0.1/watch?v=abc",
            "https://[::1]/watch?v=abc",
        ] {
            assert!(
                !is_allowed_ytdlp_source(source_url, &allowed).expect("URL should be valid"),
                "host should be denied: {source_url}"
            );
        }
        assert!(
            !is_allowed_ytdlp_source("https://youtube.com../watch?v=abc", &allowed)
                .unwrap_or(false)
        );
        assert!(matches!(
            is_allowed_ytdlp_source("https://user:password@youtube.com/watch?v=abc", &allowed),
            Err(DownloadError::Terminal { class, .. }) if class == "source_credentials_forbidden"
        ));
        assert!(matches!(
            is_allowed_ytdlp_source("https://youtube.com:8443/watch?v=abc", &allowed),
            Err(DownloadError::Terminal { class, .. }) if class == "source_port_not_allowed"
        ));
    }

    #[test]
    fn source_inspection_round_trips_as_job_payload() {
        let inspection = SourceInspection {
            adapter: "fake".to_owned(),
            source_url: "https://example.com/video".to_owned(),
            resolved_url: Some("https://cdn.example.com/video.mp4".to_owned()),
            media_kind: SourceMediaKind::Video,
            mime_type: Some("video/mp4".to_owned()),
            content_length_bytes: Some(42),
            title: Some("Example".to_owned()),
            metadata: serde_json::json!({"duration_seconds": 2}),
        };

        let value = serde_json::to_value(&inspection).expect("inspection should serialize");
        let decoded: SourceInspection =
            serde_json::from_value(value).expect("inspection should deserialize");
        assert_eq!(decoded, inspection);
    }

    #[tokio::test]
    async fn router_falls_back_to_ytdlp_for_page_like_direct_responses() {
        let direct_downloads = Arc::new(AtomicUsize::new(0));
        let ytdlp_downloads = Arc::new(AtomicUsize::new(0));
        let router = SourceDownloaderRouter::new(
            Arc::new(StubDownloader {
                inspection: Ok(inspection("direct_http", SourceMediaKind::Unknown)),
                downloads: Arc::clone(&direct_downloads),
            }),
            Arc::new(StubDownloader {
                inspection: Ok(inspection("yt_dlp", SourceMediaKind::Video)),
                downloads: Arc::clone(&ytdlp_downloads),
            }),
            vec!["example.test".to_owned()],
        );

        let inspection = router.inspect(&source()).await.expect("yt-dlp inspection should win");
        assert_eq!(inspection.adapter, "yt_dlp");
        assert_eq!(inspection.media_kind, SourceMediaKind::Video);
        let destination = PathBuf::from("source.bin");
        router
            .download(&inspection, &destination, &DownloadLimits::default())
            .await
            .expect("download should use the selected adapter");
        assert_eq!(direct_downloads.load(Ordering::Relaxed), 0);
        assert_eq!(ytdlp_downloads.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn router_rejects_page_hosts_outside_the_allowlist_without_starting_ytdlp() {
        let ytdlp_downloads = Arc::new(AtomicUsize::new(0));
        let router = SourceDownloaderRouter::new(
            Arc::new(StubDownloader {
                inspection: Ok(inspection("direct_http", SourceMediaKind::Unknown)),
                downloads: Arc::new(AtomicUsize::new(0)),
            }),
            Arc::new(StubDownloader {
                inspection: Ok(inspection("yt_dlp", SourceMediaKind::Video)),
                downloads: Arc::clone(&ytdlp_downloads),
            }),
            vec!["youtube.com".to_owned()],
        );
        let source =
            SourceInput { source_url: "https://notyoutube.com/watch?v=abc".to_owned(), ..source() };

        assert!(matches!(
            router.inspect(&source).await,
            Err(DownloadError::Terminal { class, .. }) if class == "source_host_not_allowed"
        ));
        assert_eq!(ytdlp_downloads.load(Ordering::Relaxed), 0);

        let mut selected = inspection("yt_dlp", SourceMediaKind::Video);
        selected.source_url = source.source_url;
        assert!(matches!(
            router
                .download(&selected, Path::new("source.bin"), &DownloadLimits::default())
                .await,
            Err(DownloadError::Terminal { class, .. }) if class == "source_host_not_allowed"
        ));
        assert_eq!(ytdlp_downloads.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn router_keeps_recognized_direct_media_on_http_adapter() {
        let direct_downloads = Arc::new(AtomicUsize::new(0));
        let ytdlp_downloads = Arc::new(AtomicUsize::new(0));
        let router = SourceDownloaderRouter::new(
            Arc::new(StubDownloader {
                inspection: Ok(inspection("direct_http", SourceMediaKind::Video)),
                downloads: Arc::clone(&direct_downloads),
            }),
            Arc::new(StubDownloader {
                inspection: Ok(inspection("yt_dlp", SourceMediaKind::Video)),
                downloads: Arc::clone(&ytdlp_downloads),
            }),
            vec!["example.test".to_owned()],
        );

        let inspection = router.inspect(&source()).await.expect("direct inspection should win");
        assert_eq!(inspection.adapter, "direct_http");
        let destination = PathBuf::from("source.bin");
        router
            .download(&inspection, &destination, &DownloadLimits::default())
            .await
            .expect("download should use the selected adapter");
        assert_eq!(direct_downloads.load(Ordering::Relaxed), 1);
        assert_eq!(ytdlp_downloads.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn router_falls_back_for_upstream_http_statuses_but_not_security_errors() {
        let ytdlp_downloads = Arc::new(AtomicUsize::new(0));
        let ytdlp = Arc::new(StubDownloader {
            inspection: Ok(inspection("yt_dlp", SourceMediaKind::Video)),
            downloads: Arc::clone(&ytdlp_downloads),
        });
        let fallback = SourceDownloaderRouter::new(
            Arc::new(StubDownloader {
                inspection: Err(DownloadError::terminal(
                    "upstream_http_status",
                    "source returned HTTP status 403",
                )),
                downloads: Arc::new(AtomicUsize::new(0)),
            }),
            Arc::clone(&ytdlp) as Arc<dyn SourceDownloader>,
            vec!["example.test".to_owned()],
        );
        assert_eq!(
            fallback.inspect(&source()).await.expect("yt-dlp should be tried").adapter,
            "yt_dlp"
        );

        let blocked = SourceDownloaderRouter::new(
            Arc::new(StubDownloader {
                inspection: Err(DownloadError::terminal("ssrf_blocked", "private address")),
                downloads: Arc::new(AtomicUsize::new(0)),
            }),
            ytdlp,
            vec!["example.test".to_owned()],
        );
        assert!(matches!(
            blocked.inspect(&source()).await,
            Err(DownloadError::Terminal { class, .. }) if class == "ssrf_blocked"
        ));
        assert_eq!(ytdlp_downloads.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn direct_only_router_rejects_unrecognized_sources() {
        let router = SourceDownloaderRouter::direct_only(Arc::new(StubDownloader {
            inspection: Ok(inspection("direct_http", SourceMediaKind::Unknown)),
            downloads: Arc::new(AtomicUsize::new(0)),
        }));

        assert!(matches!(
            router.inspect(&source()).await,
            Err(DownloadError::Terminal { class, .. }) if class == "source_host_not_allowed"
        ));
    }
}
