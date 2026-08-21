pub(crate) use std::{
    collections::HashMap,
    fmt::Display,
    fs,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

pub(crate) use async_trait::async_trait;
pub(crate) use sooqa_inbox::{
    AssetNormalization, AssetThumbnailNormalization, IngestFinalization, IngestKind, IngestStatus,
    SourceDownload, SourceMediaKind,
};
pub(crate) use sooqa_jobs::{Job, JobCommand, JobLease, JobStatus, JobType};
pub(crate) use sooqa_library::{
    MAX_MEDIA_PREVIEW_BYTES, MediaIngest, MediaKind, MediaMetadata, MediaPreviewInput,
    MediaSourceInput, NewMedia, SourceKind, StorageUploadStore, VideoDuplicateClassification,
    VideoDuplicateEvidence, VideoDuplicateMatch, VideoFingerprintCandidate, VideoFingerprintInput,
    VideoIdentityDecision,
};
pub(crate) use sooqa_media::{
    ArtifactPublicationError, CANONICAL_VIDEO_PROFILE_VERSION, DiskAdmissionError, DownloadError,
    DownloadLimits, DownloadedSource, FfmpegExecutor, FfprobeAdapter, FrameExtractionError,
    FrameExtractor, ImageNormalizer, MediaProbe, MediaStreamKind, MediaWorkspace,
    NormalizationExecutionError, NormalizationPlanner, SequenceAlignmentConfig,
    SequenceClassification, SourceDownloader, SourceInput, VideoSequenceFingerprint, WorkspaceArea,
    WorkspaceError, align_video_sequences, check_disk_space, decode_first_preview_frame,
    encode_bounded_preview, publish_artifact, sha256_file, validate_bounded_preview_for_mime,
};
pub(crate) use sooqa_persistence::{
    AssetNormalizationStart, AssetProbeStart, InboxRepository, InboxRepositoryError,
    IngestFinalizationStart, IngestFingerprintStart, IngestVideoIdentityStart, JobRepository,
    JobRepositoryError, LibraryRepository, LibraryRepositoryError, SourceDownloadStart,
    SourceInspectionStart, WorkspaceCleanupStart,
};
pub(crate) use sooqa_telegram::StorageUploadError;
pub(crate) use sooqa_telegram::{
    StorageCaptionEditRequest, StorageUploadCancellation, StorageUploadInput,
    StorageUploadProvider, TelegramApi, TelegramPublicationApi, TelegramPublicationRequest,
    TelegramStorageApi, TelegramStorageCaptionApi, storage_caption,
};
pub(crate) use thiserror::Error;
pub(crate) use time::{Duration as TimeDuration, OffsetDateTime};
pub(crate) use tokio::{
    sync::{oneshot, watch},
    task::JoinError,
    time::sleep,
};
pub(crate) use tracing::{debug, error, info, warn};
pub(crate) use uuid::Uuid;

pub type HandlerFuture = Pin<Box<dyn Future<Output = Result<(), HandlerFailure>> + Send + 'static>>;
pub type HandlerFn = Arc<dyn Fn(Job) -> HandlerFuture + Send + Sync>;
pub type CancellableHandlerFn =
    Arc<dyn Fn(Job, HandlerCancellation) -> HandlerFuture + Send + Sync>;

#[derive(Clone, Debug)]
pub struct HandlerCancellation {
    storage_upload: StorageUploadCancellation,
}

impl HandlerCancellation {
    pub(crate) fn new() -> Self {
        Self { storage_upload: StorageUploadCancellation::new() }
    }

    pub(crate) fn cancel(&self) {
        self.storage_upload.cancel();
    }

    pub fn storage_upload(&self) -> StorageUploadCancellation {
        self.storage_upload.clone()
    }
}
pub type IdentityAlignmentHook = Arc<dyn Fn() + Send + Sync>;

pub(crate) const DISK_ADMISSION_RETRY_DELAY: TimeDuration = TimeDuration::minutes(1);
pub(crate) const DEFAULT_TELEGRAM_SOURCE_DOWNLOAD_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Admission policy for operations that may create large workspace artifacts.
/// The policy is deliberately per operation: the filesystem check observes
/// bytes already consumed by other workers, while the operation amount is
/// expanded to the bounded two-worker worst-case before it is checked.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct WorkspaceAdmission {
    reserve_bytes: u64,
    enabled: bool,
}

impl WorkspaceAdmission {
    pub const fn disabled() -> Self {
        Self { reserve_bytes: 0, enabled: false }
    }

    pub const fn new(reserve_bytes: u64) -> Self {
        Self { reserve_bytes, enabled: true }
    }

    pub const fn reserve_bytes(self) -> u64 {
        self.reserve_bytes
    }

    pub(crate) fn admit(self, work_root: &Path, required_bytes: u64) -> Result<(), HandlerFailure> {
        if !self.enabled {
            return Ok(());
        }
        let concurrent_budget = sooqa_media::concurrent_operation_budget(
            required_bytes,
            sooqa_media::MAX_CONCURRENT_WORKSPACE_OPERATIONS,
        );
        match check_disk_space(work_root, self.reserve_bytes, concurrent_budget) {
            Ok(_) => Ok(()),
            Err(error) => Self::defer_for_space_error(work_root, required_bytes, error),
        }
    }

    pub(crate) fn defer_for_space_error(
        work_root: &Path,
        operation_bytes: u64,
        error: DiskAdmissionError,
    ) -> Result<(), HandlerFailure> {
        let message = error.to_string();
        match &error {
            DiskAdmissionError::Insufficient {
                available_bytes,
                reserve_bytes,
                required_bytes,
                ..
            } => warn!(
                work_root = %work_root.display(),
                available_bytes,
                reserve_bytes,
                required_bytes,
                operation_bytes,
                concurrent_budget_bytes = required_bytes,
                "work volume is below the configured two-worker admission reserve; deferring job"
            ),
            DiskAdmissionError::Stat { .. } => warn!(
                work_root = %work_root.display(),
                error = %message,
                "work volume free-space check failed; deferring job"
            ),
        }
        Err(HandlerFailure::defer_without_consuming_attempt(
            "work_disk_low",
            message,
            OffsetDateTime::now_utc() + DISK_ADMISSION_RETRY_DELAY,
        ))
    }
}

pub fn media_processing_components(
    ffmpeg_executable: impl Into<PathBuf>,
    ffprobe_executable: impl Into<PathBuf>,
    timeout: Duration,
) -> (FfmpegExecutor, FrameExtractor) {
    (
        FfmpegExecutor::new(
            Arc::new(sooqa_media::ProcessCommandRunner),
            ffprobe_executable,
            timeout,
        ),
        FrameExtractor::new(ffmpeg_executable, timeout),
    )
}

#[async_trait]
pub trait TelegramSourceDownloader: Send + Sync {
    async fn download_file(&self, file_id: &str, destination: &Path) -> Result<(), HandlerFailure>;
}

/// A best-effort startup probe for optional Telegram storage.  It is
/// intentionally separate from [`Worker::run`]: a remote outage must not
/// prevent non-storage jobs from being claimed, and storage jobs continue to
/// use their durable handler retry policy.
#[async_trait]
pub trait StoragePreflight: Send + 'static {
    type Error: Display + Send + Sync + 'static;

    async fn verify_storage_chat(&self) -> Result<(), Self::Error>;

    fn is_terminal_configuration(error: &Self::Error) -> bool;
}

#[async_trait]
impl<A, S> StoragePreflight for StorageUploadProvider<A, S>
where
    A: sooqa_telegram::TelegramStorageApi,
    S: StorageUploadStore,
{
    type Error = StorageUploadError;

    async fn verify_storage_chat(&self) -> Result<(), Self::Error> {
        StorageUploadProvider::verify_storage_chat(self).await
    }

    fn is_terminal_configuration(error: &Self::Error) -> bool {
        error.is_terminal_configuration()
    }
}

pub fn spawn_storage_preflight<P>(preflight: P, storage_chat_id: i64) -> tokio::task::JoinHandle<()>
where
    P: StoragePreflight,
{
    tokio::spawn(async move {
        match preflight.verify_storage_chat().await {
            Ok(()) => info!(
                target: "sooqa.telegram",
                status = "ready",
                phase = "worker_storage_preflight",
                storage_chat_id,
                "Telegram storage chat preflight passed"
            ),
            Err(error) if P::is_terminal_configuration(&error) => error!(
                target: "sooqa.telegram",
                status = "terminally_misconfigured",
                phase = "worker_storage_preflight",
                storage_chat_id,
                error = %error,
                "Telegram storage preflight found invalid remote permissions or authentication; storage jobs remain enabled"
            ),
            Err(error) => warn!(
                target: "sooqa.telegram",
                status = "degraded",
                phase = "worker_storage_preflight",
                storage_chat_id,
                error = %error,
                "Telegram storage preflight unavailable; storage jobs will use normal retry policy"
            ),
        }
    })
}

#[async_trait]
impl TelegramSourceDownloader for sooqa_telegram::TeloxideApi {
    async fn download_file(&self, file_id: &str, destination: &Path) -> Result<(), HandlerFailure> {
        TelegramApi::download_file(self, file_id, destination).await.map_err(|error| {
            if <sooqa_telegram::TeloxideApi as TelegramApi>::is_retryable_error(&error) {
                HandlerFailure::retryable("telegram_source_download", error.to_string())
            } else {
                HandlerFailure::permanent("telegram_source_download", error.to_string())
            }
        })
    }
}

pub(crate) struct DownloadAttemptArtifact {
    path: PathBuf,
}

impl DownloadAttemptArtifact {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for DownloadAttemptArtifact {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[derive(Debug, Clone)]
pub struct HandlerFailure {
    pub retryable: bool,
    pub class: String,
    pub message: String,
    pub defer_until: Option<OffsetDateTime>,
    pub defer_without_consuming_attempt: bool,
    pub retry_without_consuming_attempt: bool,
    pub requires_storage_reconciliation: bool,
}

impl HandlerFailure {
    pub fn retryable(class: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            retryable: true,
            class: class.into(),
            message: message.into(),
            defer_until: None,
            defer_without_consuming_attempt: false,
            retry_without_consuming_attempt: false,
            requires_storage_reconciliation: false,
        }
    }

    pub fn retryable_without_consuming_attempt(
        class: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            retryable: true,
            class: class.into(),
            message: message.into(),
            defer_until: None,
            defer_without_consuming_attempt: false,
            retry_without_consuming_attempt: true,
            requires_storage_reconciliation: false,
        }
    }

    pub fn permanent(class: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            retryable: false,
            class: class.into(),
            message: message.into(),
            defer_until: None,
            defer_without_consuming_attempt: false,
            retry_without_consuming_attempt: false,
            requires_storage_reconciliation: false,
        }
    }

    pub fn defer(
        class: impl Into<String>,
        message: impl Into<String>,
        defer_until: OffsetDateTime,
    ) -> Self {
        Self {
            retryable: true,
            class: class.into(),
            message: message.into(),
            defer_until: Some(defer_until),
            defer_without_consuming_attempt: false,
            retry_without_consuming_attempt: false,
            requires_storage_reconciliation: false,
        }
    }

    pub fn defer_without_consuming_attempt(
        class: impl Into<String>,
        message: impl Into<String>,
        defer_until: OffsetDateTime,
    ) -> Self {
        Self {
            retryable: true,
            class: class.into(),
            message: message.into(),
            defer_until: Some(defer_until),
            defer_without_consuming_attempt: true,
            retry_without_consuming_attempt: false,
            requires_storage_reconciliation: false,
        }
    }

    pub fn storage_reconciliation_required(message: impl Into<String>) -> Self {
        Self {
            retryable: false,
            class: "storage_upload_unknown".to_owned(),
            message: message.into(),
            defer_until: None,
            defer_without_consuming_attempt: false,
            retry_without_consuming_attempt: false,
            requires_storage_reconciliation: true,
        }
    }
}

#[derive(Clone, Default)]
pub struct HandlerRegistry {
    handlers: HashMap<JobType, HandlerEntry>,
}

#[derive(Clone)]
pub(crate) struct HandlerEntry {
    pub(crate) handler: Option<HandlerFn>,
    pub(crate) cancellable_handler: Option<CancellableHandlerFn>,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<F>(&mut self, job_type: JobType, handler: F)
    where
        F: Fn(Job) -> HandlerFuture + Send + Sync + 'static,
    {
        self.handlers.insert(
            job_type,
            HandlerEntry { handler: Some(Arc::new(handler)), cancellable_handler: None },
        );
    }

    pub fn register_cancellable<F>(&mut self, job_type: JobType, handler: F)
    where
        F: Fn(Job, HandlerCancellation) -> HandlerFuture + Send + Sync + 'static,
    {
        self.handlers.insert(
            job_type,
            HandlerEntry { handler: None, cancellable_handler: Some(Arc::new(handler)) },
        );
    }

    pub fn contains(&self, job_type: JobType) -> bool {
        self.handlers.contains_key(&job_type)
    }

    pub fn job_types(&self) -> Vec<JobType> {
        let mut job_types = self.handlers.keys().copied().collect::<Vec<_>>();
        job_types.sort_by_key(|job_type| job_type.as_str());
        job_types
    }

    pub(crate) fn handler(&self, job_type: JobType) -> Option<HandlerEntry> {
        self.handlers.get(&job_type).cloned()
    }
}

pub(crate) fn map_workspace_error(error: WorkspaceError) -> HandlerFailure {
    HandlerFailure::permanent("workspace_error", error.to_string())
}

pub(crate) async fn load_ingest_for_admission(
    inbox: &InboxRepository,
    ingest_request_id: Uuid,
) -> Result<sooqa_inbox::Ingest, HandlerFailure> {
    inbox
        .find(ingest_request_id)
        .await
        .map_err(map_inbox_error)?
        .ok_or_else(|| HandlerFailure::permanent("ingest_missing", "ingest request was not found"))
}

pub(crate) fn probe_stage_may_run(status: IngestStatus) -> bool {
    matches!(
        status,
        IngestStatus::Queued
            | IngestStatus::Downloading
            | IngestStatus::Probing
            | IngestStatus::FailedRetryable
    )
}

pub(crate) fn download_stage_may_run(request: &sooqa_inbox::Ingest) -> bool {
    matches!(request.status, IngestStatus::Downloading | IngestStatus::FailedRetryable)
        && request.input_data().map(|data| data.download.is_none()).unwrap_or(false)
}

pub(crate) fn normalization_stage_may_run(status: IngestStatus) -> bool {
    matches!(status, IngestStatus::Normalizing | IngestStatus::FailedRetryable)
}

pub(crate) fn fingerprint_stage_may_run(status: IngestStatus) -> bool {
    matches!(status, IngestStatus::Fingerprinting | IngestStatus::FailedRetryable)
}

pub(crate) async fn source_artifact_exists(path: &Path) -> Result<bool, HandlerFailure> {
    let metadata = match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(HandlerFailure::permanent(
                "source_reconstruction",
                format!("could not inspect source artifact: {error}"),
            ));
        }
    };
    if metadata.file_type().is_symlink() {
        return Err(HandlerFailure::permanent(
            "source_reconstruction",
            "source artifact is a symlink",
        ));
    }
    Ok(metadata.is_file() && metadata.len() > 0)
}

pub(crate) fn workspace_input(
    request: &sooqa_inbox::Ingest,
) -> Result<(Uuid, &'static str), HandlerFailure> {
    if request.kind == IngestKind::Url {
        return Ok((request.workspace_id, "source.bin"));
    }
    Ok((request.workspace_id, "telegram-input.bin"))
}

pub(crate) fn request_media_kind(request: &sooqa_inbox::Ingest) -> Option<SourceMediaKind> {
    request.input_data().ok()?.media_kind()
}

pub(crate) fn map_library_error(error: LibraryRepositoryError) -> HandlerFailure {
    let message = error.to_string();
    match error {
        LibraryRepositoryError::Database(_) => HandlerFailure::retryable("database_error", message),
        _ => HandlerFailure::permanent("library_error", message),
    }
}

pub(crate) fn map_inbox_error(error: InboxRepositoryError) -> HandlerFailure {
    let message = error.to_string();
    match error {
        InboxRepositoryError::Library(error) => map_library_error(error),
        InboxRepositoryError::InputEnvelope(_) => {
            HandlerFailure::permanent("invalid_ingest_state", message)
        }
        InboxRepositoryError::ResourceMissing(_)
        | InboxRepositoryError::MissingSourceUrl(_)
        | InboxRepositoryError::InvalidFailureStatus(_)
        | InboxRepositoryError::InvalidStateTransition(_)
        | InboxRepositoryError::UnknownIngestKind(_)
        | InboxRepositoryError::UnknownIngestStatus(_)
        | InboxRepositoryError::UnknownSubmittedVia(_)
        | InboxRepositoryError::VideoFinalizationNotAllowed
        | InboxRepositoryError::DuplicateEvidenceTooManyMatches { .. }
        | InboxRepositoryError::DuplicateEvidenceTooLarge { .. } => {
            HandlerFailure::permanent("invalid_ingest_state", message)
        }
        _ => HandlerFailure::retryable("database_error", message),
    }
}
