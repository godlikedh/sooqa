//! Bounded durable-job worker loop for sooqa.

use std::{
    collections::HashMap,
    fs,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use sooqa_inbox::{
    AssetNormalization, AssetThumbnailNormalization, IngestFinalization, IngestKind, IngestStatus,
    SourceDownload, SourceMediaKind,
};
use sooqa_jobs::{Job, JobCommand, JobLease, JobStatus, JobType};
use sooqa_library::{
    MediaIngest, MediaKind, MediaMetadata, MediaSourceInput, NewMedia, SourceKind,
    StorageUploadStore,
};
use sooqa_media::{
    ArtifactPublicationError, DownloadError, DownloadLimits, DownloadedSource, FfmpegExecutor,
    FfprobeAdapter, FrameExtractionError, FrameExtractor, ImageNormalizer, MediaProbe,
    MediaStreamKind, MediaWorkspace, NormalizationExecutionError, NormalizationPlanner,
    SequenceAlignmentConfig, SourceDownloader, SourceInput, WorkspaceArea, WorkspaceError,
    publish_artifact, sha256_file,
};
use sooqa_persistence::{
    AssetNormalizationStart, AssetProbeStart, InboxRepository, InboxRepositoryError,
    IngestFinalizationStart, IngestFingerprintStart, JobRepository, JobRepositoryError,
    LibraryRepository, LibraryRepositoryError, SourceDownloadStart, SourceInspectionStart,
    WorkspaceCleanupStart,
};
use sooqa_telegram::StorageUploadError;
use sooqa_telegram::{
    StorageUploadInput, StorageUploadProvider, TelegramApi, TelegramPublicationApi,
    TelegramPublicationRequest, TelegramStorageApi,
};
use thiserror::Error;
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::{
    sync::{oneshot, watch},
    task::JoinError,
    time::sleep,
};
use tracing::{debug, info, warn};
use uuid::Uuid;

pub type HandlerFuture = Pin<Box<dyn Future<Output = Result<(), HandlerFailure>> + Send + 'static>>;
pub type HandlerFn = Arc<dyn Fn(Job) -> HandlerFuture + Send + Sync>;

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

struct DownloadAttemptArtifact {
    path: PathBuf,
}

impl DownloadAttemptArtifact {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    fn path(&self) -> &Path {
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
}

impl HandlerFailure {
    pub fn retryable(class: impl Into<String>, message: impl Into<String>) -> Self {
        Self { retryable: true, class: class.into(), message: message.into(), defer_until: None }
    }

    pub fn permanent(class: impl Into<String>, message: impl Into<String>) -> Self {
        Self { retryable: false, class: class.into(), message: message.into(), defer_until: None }
    }

    pub fn defer(
        class: impl Into<String>,
        message: impl Into<String>,
        available_at: OffsetDateTime,
    ) -> Self {
        Self {
            retryable: true,
            class: class.into(),
            message: message.into(),
            defer_until: Some(available_at),
        }
    }
}

#[derive(Clone, Default)]
pub struct HandlerRegistry {
    handlers: HashMap<JobType, HandlerFn>,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<F>(&mut self, job_type: JobType, handler: F)
    where
        F: Fn(Job) -> HandlerFuture + Send + Sync + 'static,
    {
        self.handlers.insert(job_type, Arc::new(handler));
    }

    pub fn contains(&self, job_type: JobType) -> bool {
        self.handlers.contains_key(&job_type)
    }

    pub fn job_types(&self) -> Vec<JobType> {
        let mut job_types = self.handlers.keys().copied().collect::<Vec<_>>();
        job_types.sort_by_key(|job_type| job_type.as_str());
        job_types
    }

    fn handler(&self, job_type: JobType) -> Option<HandlerFn> {
        self.handlers.get(&job_type).cloned()
    }
}

pub fn inspect_source_handler(
    inbox: InboxRepository,
    downloader: Arc<dyn SourceDownloader>,
) -> HandlerFn {
    Arc::new(move |job| {
        let inbox = inbox.clone();
        let downloader = Arc::clone(&downloader);
        Box::pin(async move { inspect_source(&inbox, downloader.as_ref(), job).await })
    })
}

pub fn download_source_handler(
    inbox: InboxRepository,
    work_root: impl Into<PathBuf>,
    downloader: Arc<dyn SourceDownloader>,
    limits: DownloadLimits,
) -> HandlerFn {
    let work_root = work_root.into();
    Arc::new(move |job| {
        let inbox = inbox.clone();
        let work_root = work_root.clone();
        let downloader = Arc::clone(&downloader);
        Box::pin(async move {
            download_source(&inbox, &work_root, downloader.as_ref(), &limits, job).await
        })
    })
}

pub fn upload_storage_asset_handler<A, S>(
    inbox: InboxRepository,
    provider: StorageUploadProvider<A, S>,
) -> HandlerFn
where
    A: TelegramStorageApi,
    S: StorageUploadStore,
{
    Arc::new(move |job| {
        let inbox = inbox.clone();
        let provider = provider.clone();
        Box::pin(async move { upload_storage_asset(&inbox, &provider, job).await })
    })
}

pub fn publish_post_handler<A>(
    publisher: sooqa_persistence::PublisherRepository,
    library: LibraryRepository,
    telegram: A,
) -> HandlerFn
where
    A: TelegramPublicationApi,
{
    Arc::new(move |job| {
        let publisher = publisher.clone();
        let library = library.clone();
        let telegram = telegram.clone();
        Box::pin(async move { publish_post(&publisher, &library, &telegram, job).await })
    })
}

pub fn materialize_publication_handler(
    publisher: sooqa_persistence::PublisherRepository,
) -> HandlerFn {
    Arc::new(move |job| {
        let publisher = publisher.clone();
        Box::pin(async move { materialize_publication(&publisher, job).await })
    })
}

pub fn probe_asset_handler(
    inbox: InboxRepository,
    work_root: impl Into<std::path::PathBuf>,
    ffprobe: FfprobeAdapter,
) -> HandlerFn {
    probe_asset_handler_with_telegram_source(inbox, work_root, ffprobe, None)
}

pub fn probe_asset_handler_with_telegram_source(
    inbox: InboxRepository,
    work_root: impl Into<std::path::PathBuf>,
    ffprobe: FfprobeAdapter,
    telegram_source: Option<Arc<dyn TelegramSourceDownloader>>,
) -> HandlerFn {
    let work_root = work_root.into();
    Arc::new(move |job| {
        let inbox = inbox.clone();
        let work_root = work_root.clone();
        let ffprobe = ffprobe.clone();
        let telegram_source = telegram_source.clone();
        Box::pin(async move {
            probe_asset(&inbox, &work_root, &ffprobe, telegram_source.as_deref(), job).await
        })
    })
}

pub fn normalize_asset_handler(
    inbox: InboxRepository,
    work_root: impl Into<std::path::PathBuf>,
    planner: NormalizationPlanner,
    executor: FfmpegExecutor,
    image_normalizer: ImageNormalizer,
    max_normalized_storage_bytes: u64,
) -> HandlerFn {
    let work_root = work_root.into();
    Arc::new(move |job| {
        let inbox = inbox.clone();
        let work_root = work_root.clone();
        let planner = planner.clone();
        let executor = executor.clone();
        Box::pin(async move {
            normalize_asset(
                &inbox,
                &work_root,
                &planner,
                &executor,
                image_normalizer,
                max_normalized_storage_bytes,
                job,
            )
            .await
        })
    })
}

pub fn finalize_ingest_handler(inbox: InboxRepository, library: LibraryRepository) -> HandlerFn {
    Arc::new(move |job| {
        let inbox = inbox.clone();
        let library = library.clone();
        Box::pin(async move { finalize_ingest(&inbox, &library, job).await })
    })
}

pub fn compute_fingerprint_handler(
    inbox: InboxRepository,
    library: LibraryRepository,
    work_root: impl Into<PathBuf>,
    extractor: FrameExtractor,
) -> HandlerFn {
    let work_root = work_root.into();
    Arc::new(move |job| {
        let inbox = inbox.clone();
        let library = library.clone();
        let work_root = work_root.clone();
        let extractor = extractor.clone();
        Box::pin(
            async move { compute_fingerprint(&inbox, &library, &work_root, &extractor, job).await },
        )
    })
}

pub fn cleanup_workspace_handler(
    inbox: InboxRepository,
    work_root: impl Into<PathBuf>,
) -> HandlerFn {
    let work_root = work_root.into();
    Arc::new(move |job| {
        let inbox = inbox.clone();
        let work_root = work_root.clone();
        Box::pin(async move { cleanup_workspace(&inbox, &work_root, job).await })
    })
}

async fn cleanup_workspace(
    inbox: &InboxRepository,
    work_root: &Path,
    job: Job,
) -> Result<(), HandlerFailure> {
    let (ingest_id, workspace_id) = match &job.command {
        JobCommand::CleanupWorkspace(payload) => (payload.ingest_id, payload.workspace_id),
        _ => {
            return Err(HandlerFailure::permanent(
                "invalid_payload",
                "cleanup_workspace handler received a different job command",
            ));
        }
    };
    let job_attempt = job.attempt().ok_or_else(|| {
        HandlerFailure::permanent(
            "invalid_job_state",
            "cleanup_workspace handler requires a running job lease",
        )
    })?;
    let start = inbox
        .begin_workspace_cleanup(&job_attempt, ingest_id, workspace_id)
        .await
        .map_err(map_inbox_error)?;
    match start {
        WorkspaceCleanupStart::Deferred => {
            return Err(HandlerFailure::defer(
                "workspace_protected",
                "workspace is still protected by durable ingest or storage state",
                OffsetDateTime::now_utc() + TimeDuration::minutes(1),
            ));
        }
        WorkspaceCleanupStart::AlreadyAdvanced => return Ok(()),
        WorkspaceCleanupStart::Ready => {}
    }

    if let Err(error) = MediaWorkspace::cleanup_existing(work_root, workspace_id).await {
        let message = error.to_string();
        return Err(if matches!(error, WorkspaceError::Io { .. }) {
            HandlerFailure::retryable("workspace_cleanup", message)
        } else {
            HandlerFailure::permanent("workspace_cleanup", message)
        });
    }
    Ok(())
}

async fn probe_asset(
    inbox: &InboxRepository,
    work_root: &std::path::Path,
    ffprobe: &FfprobeAdapter,
    telegram_source: Option<&dyn TelegramSourceDownloader>,
    job: Job,
) -> Result<(), HandlerFailure> {
    let ingest_request_id = match &job.command {
        JobCommand::ProbeAsset(payload) => payload.ingest_id,
        _ => {
            return Err(HandlerFailure::permanent(
                "invalid_payload",
                "probe_asset handler received a different job command",
            ));
        }
    };
    let job_attempt = job.attempt().ok_or_else(|| {
        HandlerFailure::permanent(
            "invalid_job_state",
            "probe_asset handler requires a running job lease",
        )
    })?;

    let request = match inbox.begin_asset_probe(ingest_request_id, &job_attempt).await {
        Ok(AssetProbeStart::Ready(request)) => request,
        Ok(AssetProbeStart::AlreadyAdvanced(_)) => return Ok(()),
        Err(error) => return Err(map_inbox_error(error)),
    };
    let (workspace_id, input_name) = if request.kind == IngestKind::Url {
        (request.workspace_id, "source.bin")
    } else {
        (request.workspace_id, "telegram-input.bin")
    };
    let workspace = match MediaWorkspace::create(work_root, workspace_id).await {
        Ok(workspace) => workspace,
        Err(error) => {
            return fail_probe(inbox, ingest_request_id, &job_attempt, map_workspace_error(error))
                .await;
        }
    };
    if let Err(error) = workspace.validate() {
        return fail_probe(inbox, ingest_request_id, &job_attempt, map_workspace_error(error))
            .await;
    }
    let input_path = match workspace.path(WorkspaceArea::Source, input_name) {
        Ok(path) => path,
        Err(error) => {
            return fail_probe(inbox, ingest_request_id, &job_attempt, map_workspace_error(error))
                .await;
        }
    };

    if request.kind == IngestKind::TelegramMessage
        && !source_artifact_exists(&input_path).await?
        && let Err(failure) = ensure_telegram_source(&workspace, &request, telegram_source).await
    {
        let terminal = !failure.retryable || job.attempt_count >= job.max_attempts;
        let failure = if terminal {
            HandlerFailure::permanent(failure.class, failure.message)
        } else {
            failure
        };
        return fail_probe(inbox, ingest_request_id, &job_attempt, failure).await;
    }

    let probe = match ffprobe.probe(&input_path).await {
        Ok(probe) => probe,
        Err(error) => {
            let terminal = !error.is_retryable() || job.attempt_count >= job.max_attempts;
            let class = error.class().to_owned();
            let message = error.to_string();
            inbox
                .fail_asset_probe(
                    ingest_request_id,
                    &job_attempt,
                    if terminal {
                        IngestStatus::FailedTerminal
                    } else {
                        IngestStatus::FailedRetryable
                    },
                    &class,
                    &message,
                )
                .await
                .map_err(map_inbox_error)?;
            return Err(if terminal {
                HandlerFailure::permanent(class, message)
            } else {
                HandlerFailure::retryable(class, message)
            });
        }
    };

    inbox
        .complete_asset_probe(
            ingest_request_id,
            &job_attempt,
            serde_json::to_value(probe).expect("probe is serializable"),
        )
        .await
        .map_err(map_inbox_error)?;
    Ok(())
}

fn map_workspace_error(error: WorkspaceError) -> HandlerFailure {
    HandlerFailure::permanent("workspace_error", error.to_string())
}

async fn source_artifact_exists(path: &Path) -> Result<bool, HandlerFailure> {
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

async fn ensure_telegram_source(
    workspace: &MediaWorkspace,
    request: &sooqa_inbox::Ingest,
    telegram_source: Option<&dyn TelegramSourceDownloader>,
) -> Result<(), HandlerFailure> {
    let file_id = request
        .original_input
        .get("telegram_file_id")
        .and_then(serde_json::Value::as_str)
        .filter(|file_id| !file_id.is_empty())
        .ok_or_else(|| {
            HandlerFailure::permanent(
                "source_reconstruction",
                "Telegram ingest has no durable file ID",
            )
        })?;
    let telegram_source = telegram_source.ok_or_else(|| {
        HandlerFailure::permanent(
            "source_reconstruction",
            "Telegram source reconstruction is not configured in this worker",
        )
    })?;
    let source_path =
        workspace.path(WorkspaceArea::Source, "telegram-input.bin").map_err(map_workspace_error)?;
    let temporary_path = workspace
        .path(WorkspaceArea::Source, &format!(".sooqa-telegram-source-{}.tmp", Uuid::new_v4()))
        .map_err(map_workspace_error)?;
    let temporary = DownloadAttemptArtifact::new(temporary_path);
    telegram_source.download_file(file_id, temporary.path()).await?;
    let downloaded = read_source_artifact(workspace, temporary.path(), None).await?;
    if downloaded.bytes == 0 {
        return Err(HandlerFailure::permanent(
            "source_reconstruction",
            "Telegram source reconstruction produced an empty file",
        ));
    }
    match publish_artifact(temporary.path(), &source_path).await {
        Ok(()) | Err(ArtifactPublicationError::DestinationConflict) => {
            let existing = read_source_artifact(workspace, &source_path, None).await?;
            if existing.bytes == 0 {
                return Err(HandlerFailure::permanent(
                    "source_reconstruction",
                    "Telegram source artifact is empty after reconstruction",
                ));
            }
        }
        Err(error) => {
            return Err(HandlerFailure::permanent("source_reconstruction", error.to_string()));
        }
    }
    Ok(())
}

async fn fail_probe(
    inbox: &InboxRepository,
    ingest_request_id: uuid::Uuid,
    job_attempt: &sooqa_jobs::JobAttempt,
    failure: HandlerFailure,
) -> Result<(), HandlerFailure> {
    let status = if failure.retryable {
        IngestStatus::FailedRetryable
    } else {
        IngestStatus::FailedTerminal
    };
    inbox
        .fail_asset_probe(ingest_request_id, job_attempt, status, &failure.class, &failure.message)
        .await
        .map_err(map_inbox_error)?;
    Err(failure)
}

async fn normalize_asset(
    inbox: &InboxRepository,
    work_root: &std::path::Path,
    planner: &NormalizationPlanner,
    executor: &FfmpegExecutor,
    image_normalizer: ImageNormalizer,
    max_normalized_storage_bytes: u64,
    job: Job,
) -> Result<(), HandlerFailure> {
    let ingest_request_id = match &job.command {
        JobCommand::NormalizeAsset(payload) => payload.ingest_id,
        _ => {
            return Err(HandlerFailure::permanent(
                "invalid_payload",
                "normalize_asset handler received a different job command",
            ));
        }
    };
    let job_attempt = job.attempt().ok_or_else(|| {
        HandlerFailure::permanent(
            "invalid_job_state",
            "normalize_asset handler requires a running job lease",
        )
    })?;
    let request = match inbox.begin_asset_normalization(ingest_request_id, &job_attempt).await {
        Ok(AssetNormalizationStart::Ready(request)) => request,
        Ok(AssetNormalizationStart::AlreadyAdvanced(_)) => return Ok(()),
        Err(error) => return Err(map_inbox_error(error)),
    };
    let probe = match request.original_input.get("probe").cloned() {
        Some(probe) => match serde_json::from_value::<MediaProbe>(probe) {
            Ok(probe) => probe,
            Err(error) => {
                return fail_normalization(
                    inbox,
                    ingest_request_id,
                    &job_attempt,
                    HandlerFailure::permanent(
                        "invalid_ingest_state",
                        format!("stored media probe could not be decoded: {error}"),
                    ),
                )
                .await;
            }
        },
        None => {
            return fail_normalization(
                inbox,
                ingest_request_id,
                &job_attempt,
                HandlerFailure::permanent(
                    "invalid_ingest_state",
                    "ingest request has no stored media probe",
                ),
            )
            .await;
        }
    };
    let media_kind = match probe_media_kind(&probe).or_else(|| request_media_kind(&request)) {
        Some(media_kind) => media_kind,
        None => {
            return fail_normalization(
                inbox,
                ingest_request_id,
                &job_attempt,
                HandlerFailure::permanent(
                    "invalid_ingest_state",
                    "ingest request has no stored source media kind",
                ),
            )
            .await;
        }
    };
    if media_kind == SourceMediaKind::Image {
        return normalize_image_asset(
            inbox,
            work_root,
            image_normalizer,
            &request,
            ingest_request_id,
            &job_attempt,
            max_normalized_storage_bytes,
        )
        .await;
    }
    if matches!(media_kind, SourceMediaKind::Animation | SourceMediaKind::Audio) {
        return normalize_exact_asset(
            inbox,
            work_root,
            &request,
            ingest_request_id,
            &job_attempt,
            ExactNormalizationSpec { media_kind, probe: &probe, max_normalized_storage_bytes },
        )
        .await;
    }
    if media_kind != SourceMediaKind::Video {
        return fail_normalization(
            inbox,
            ingest_request_id,
            &job_attempt,
            HandlerFailure::permanent(
                "unsupported_media_kind",
                format!("asset media kind {media_kind:?} is not supported by the video normalizer"),
            ),
        )
        .await;
    }
    let (workspace_id, input_name) = match workspace_input(&request) {
        Ok(value) => value,
        Err(failure) => {
            return fail_normalization(inbox, ingest_request_id, &job_attempt, failure).await;
        }
    };
    let workspace = match MediaWorkspace::create(work_root, workspace_id).await {
        Ok(workspace) => workspace,
        Err(error) => {
            return fail_normalization(
                inbox,
                ingest_request_id,
                &job_attempt,
                map_workspace_error(error),
            )
            .await;
        }
    };
    if let Err(error) = workspace.validate() {
        return fail_normalization(
            inbox,
            ingest_request_id,
            &job_attempt,
            map_workspace_error(error),
        )
        .await;
    }
    let input_path = match workspace.path(WorkspaceArea::Source, input_name) {
        Ok(path) => path,
        Err(error) => {
            return fail_normalization(
                inbox,
                ingest_request_id,
                &job_attempt,
                map_workspace_error(error),
            )
            .await;
        }
    };
    let output_path = match workspace.path(WorkspaceArea::Normalized, "canonical.mp4") {
        Ok(path) => path,
        Err(error) => {
            return fail_normalization(
                inbox,
                ingest_request_id,
                &job_attempt,
                map_workspace_error(error),
            )
            .await;
        }
    };
    let plan = match planner.plan(&input_path, &output_path, &probe) {
        Ok(plan) => plan,
        Err(error) => {
            return fail_normalization(
                inbox,
                ingest_request_id,
                &job_attempt,
                HandlerFailure::permanent("normalize_plan", error.to_string()),
            )
            .await;
        }
    };
    let result = match executor.execute(&plan, std::future::pending()).await {
        Ok(result) => result,
        Err(error) => {
            let retryable = normalization_error_is_retryable(&error);
            let terminal = !retryable || job.attempt_count >= job.max_attempts;
            let failure = if terminal {
                HandlerFailure::permanent("normalize", error.to_string())
            } else {
                HandlerFailure::retryable("normalize_timeout", error.to_string())
            };
            return fail_normalization(inbox, ingest_request_id, &job_attempt, failure).await;
        }
    };
    if let Some(failure) =
        normalized_storage_limit_failure(result.digest.bytes, max_normalized_storage_bytes)
    {
        return fail_normalization(inbox, ingest_request_id, &job_attempt, failure).await;
    }
    let normalization = normalization_metadata(result);
    inbox
        .complete_asset_normalization(ingest_request_id, &job_attempt, normalization)
        .await
        .map_err(map_inbox_error)?;
    Ok(())
}

async fn normalize_image_asset(
    inbox: &InboxRepository,
    work_root: &std::path::Path,
    image_normalizer: ImageNormalizer,
    request: &sooqa_inbox::Ingest,
    ingest_request_id: Uuid,
    job_attempt: &sooqa_jobs::JobAttempt,
    max_normalized_storage_bytes: u64,
) -> Result<(), HandlerFailure> {
    let (workspace_id, input_name) = match workspace_input(request) {
        Ok(value) => value,
        Err(failure) => {
            return fail_normalization(inbox, ingest_request_id, job_attempt, failure).await;
        }
    };
    let workspace = match MediaWorkspace::create(work_root, workspace_id).await {
        Ok(workspace) => workspace,
        Err(error) => {
            return fail_normalization(
                inbox,
                ingest_request_id,
                job_attempt,
                map_workspace_error(error),
            )
            .await;
        }
    };
    if let Err(error) = workspace.validate() {
        return fail_normalization(
            inbox,
            ingest_request_id,
            job_attempt,
            map_workspace_error(error),
        )
        .await;
    }
    let plan = match image_normalizer.plan(&workspace, input_name, "canonical", "thumbnail") {
        Ok(plan) => plan,
        Err(error) => {
            return fail_normalization(
                inbox,
                ingest_request_id,
                job_attempt,
                HandlerFailure::permanent("normalize_plan", error.to_string()),
            )
            .await;
        }
    };
    let result = match image_normalizer.execute(&plan).await {
        Ok(result) => result,
        Err(error) => {
            return fail_normalization(
                inbox,
                ingest_request_id,
                job_attempt,
                HandlerFailure::permanent("normalize_image", error.to_string()),
            )
            .await;
        }
    };
    if let Some(failure) = normalized_storage_limit_failure(
        result.canonical_digest.bytes,
        max_normalized_storage_bytes,
    ) {
        return fail_normalization(inbox, ingest_request_id, job_attempt, failure).await;
    }
    inbox
        .complete_asset_normalization(
            ingest_request_id,
            job_attempt,
            image_normalization_metadata(result),
        )
        .await
        .map_err(map_inbox_error)?;
    Ok(())
}

struct ExactNormalizationSpec<'a> {
    media_kind: SourceMediaKind,
    probe: &'a MediaProbe,
    max_normalized_storage_bytes: u64,
}

async fn normalize_exact_asset(
    inbox: &InboxRepository,
    work_root: &Path,
    request: &sooqa_inbox::Ingest,
    ingest_request_id: Uuid,
    job_attempt: &sooqa_jobs::JobAttempt,
    spec: ExactNormalizationSpec<'_>,
) -> Result<(), HandlerFailure> {
    let ExactNormalizationSpec { media_kind, probe, max_normalized_storage_bytes } = spec;
    let (workspace_id, input_name) = match workspace_input(request) {
        Ok(value) => value,
        Err(failure) => {
            return fail_normalization(inbox, ingest_request_id, job_attempt, failure).await;
        }
    };
    let workspace = match MediaWorkspace::create(work_root, workspace_id).await {
        Ok(workspace) => workspace,
        Err(error) => {
            return fail_normalization(
                inbox,
                ingest_request_id,
                job_attempt,
                map_workspace_error(error),
            )
            .await;
        }
    };
    if let Err(error) = workspace.validate() {
        return fail_normalization(
            inbox,
            ingest_request_id,
            job_attempt,
            map_workspace_error(error),
        )
        .await;
    }
    let input_path = match workspace.path(WorkspaceArea::Source, input_name) {
        Ok(path) => path,
        Err(error) => {
            return fail_normalization(
                inbox,
                ingest_request_id,
                job_attempt,
                map_workspace_error(error),
            )
            .await;
        }
    };
    let canonical_name = match media_kind {
        SourceMediaKind::Animation => "canonical.animation",
        SourceMediaKind::Audio => "canonical.audio",
        SourceMediaKind::Video | SourceMediaKind::Image | SourceMediaKind::Unknown => {
            "canonical.media"
        }
    };
    let canonical_path = match workspace.path(WorkspaceArea::Normalized, canonical_name) {
        Ok(path) => path,
        Err(error) => {
            return fail_normalization(
                inbox,
                ingest_request_id,
                job_attempt,
                map_workspace_error(error),
            )
            .await;
        }
    };
    match source_artifact_exists(&canonical_path).await {
        Ok(true) => {}
        Ok(false) => match publish_artifact(&input_path, &canonical_path).await {
            Ok(()) | Err(ArtifactPublicationError::DestinationConflict) => {}
            Err(error) => {
                return fail_normalization(
                    inbox,
                    ingest_request_id,
                    job_attempt,
                    HandlerFailure::permanent("normalize_exact", error.to_string()),
                )
                .await;
            }
        },
        Err(failure) => {
            return fail_normalization(inbox, ingest_request_id, job_attempt, failure).await;
        }
    }
    let digest = match sha256_file(&canonical_path).await {
        Ok(digest) => digest,
        Err(error) => {
            return fail_normalization(
                inbox,
                ingest_request_id,
                job_attempt,
                HandlerFailure::permanent("normalize_exact", error.to_string()),
            )
            .await;
        }
    };
    if let Some(failure) =
        normalized_storage_limit_failure(digest.bytes, max_normalized_storage_bytes)
    {
        return fail_normalization(inbox, ingest_request_id, job_attempt, failure).await;
    }
    let video = probe.streams.iter().find(|stream| stream.kind == MediaStreamKind::Video);
    let audio = probe.streams.iter().find(|stream| stream.kind == MediaStreamKind::Audio);
    let normalization = AssetNormalization {
        local_work_path: canonical_path.to_string_lossy().into_owned(),
        file_size_bytes: digest.bytes,
        sha256: digest.sha256,
        media_kind,
        mime_type: source_mime_type(request),
        container: probe.container_format.clone(),
        video_codec: video.and_then(|stream| stream.codec.clone()),
        audio_codec: audio.and_then(|stream| stream.codec.clone()),
        width: video.and_then(|stream| stream.width),
        height: video.and_then(|stream| stream.height),
        duration_ms: probe.duration_ms,
        bit_rate: probe.bit_rate,
        thumbnail: None,
    };
    inbox
        .complete_asset_normalization(ingest_request_id, job_attempt, normalization)
        .await
        .map_err(map_inbox_error)?;
    Ok(())
}

fn source_mime_type(request: &sooqa_inbox::Ingest) -> Option<String> {
    let value = if request.kind == IngestKind::Url {
        request.original_input.get("download").and_then(|value| value.get("mime_type"))
    } else {
        request.original_input.get("mime_type")
    };
    value.and_then(serde_json::Value::as_str).map(ToOwned::to_owned)
}

fn normalization_error_is_retryable(error: &NormalizationExecutionError) -> bool {
    match error {
        NormalizationExecutionError::Command(error) => error.is_timeout(),
        NormalizationExecutionError::Probe(error) => error.is_retryable(),
        _ => false,
    }
}

fn request_media_kind(request: &sooqa_inbox::Ingest) -> Option<SourceMediaKind> {
    if let Some(value) = request.original_input.get("probed_media_kind")
        && let Ok(media_kind) = serde_json::from_value(value.clone())
    {
        return Some(media_kind);
    }
    let value = if request.kind == IngestKind::Url {
        request.original_input.get("download")?.get("media_kind")?
    } else {
        request.original_input.get("media_kind")?
    };
    serde_json::from_value(value.clone()).ok()
}

fn probe_media_kind(probe: &MediaProbe) -> Option<SourceMediaKind> {
    let container = probe.container_format.as_deref().map(str::to_ascii_lowercase);
    let video_streams = probe
        .streams
        .iter()
        .filter(|stream| matches!(&stream.kind, MediaStreamKind::Video))
        .collect::<Vec<_>>();
    let codecs =
        video_streams.iter().filter_map(|stream| stream.codec.as_deref()).collect::<Vec<_>>();
    let is_gif = container.as_deref().is_some_and(|value| value.contains("gif"))
        || codecs.iter().any(|value| value.to_ascii_lowercase().contains("gif"));
    if is_gif {
        return Some(SourceMediaKind::Animation);
    }

    let is_image_container = container.as_deref().is_some_and(|value| {
        ["image2", "png", "jpeg", "jpg", "webp", "avif", "mjpeg"]
            .iter()
            .any(|format| value.contains(format))
    });
    let is_image_codec = codecs
        .iter()
        .any(|value| ["png", "webp"].iter().any(|format| value.eq_ignore_ascii_case(format)))
        || (container.is_none() && codecs.iter().any(|value| value.eq_ignore_ascii_case("mjpeg")));
    if is_image_container || is_image_codec {
        return Some(SourceMediaKind::Image);
    }
    if !video_streams.is_empty() {
        return Some(SourceMediaKind::Video);
    }
    if probe.streams.iter().any(|stream| matches!(&stream.kind, MediaStreamKind::Audio)) {
        return Some(SourceMediaKind::Audio);
    }
    None
}

fn workspace_input(request: &sooqa_inbox::Ingest) -> Result<(Uuid, &'static str), HandlerFailure> {
    if request.kind == IngestKind::Url {
        return Ok((request.workspace_id, "source.bin"));
    }
    Ok((request.workspace_id, "telegram-input.bin"))
}

fn normalization_metadata(result: sooqa_media::NormalizationResult) -> AssetNormalization {
    let video =
        result.probe.streams.iter().find(|stream| matches!(&stream.kind, MediaStreamKind::Video));
    let audio =
        result.probe.streams.iter().find(|stream| matches!(&stream.kind, MediaStreamKind::Audio));
    AssetNormalization {
        local_work_path: result.output_path.to_string_lossy().into_owned(),
        file_size_bytes: result.digest.bytes,
        sha256: result.digest.sha256,
        media_kind: SourceMediaKind::Video,
        mime_type: Some("video/mp4".to_owned()),
        container: result.probe.container_format,
        video_codec: video.and_then(|stream| stream.codec.clone()),
        audio_codec: audio.and_then(|stream| stream.codec.clone()),
        width: video.and_then(|stream| stream.width),
        height: video.and_then(|stream| stream.height),
        duration_ms: result.probe.duration_ms,
        bit_rate: result.probe.bit_rate,
        thumbnail: None,
    }
}

fn image_normalization_metadata(
    result: sooqa_media::ImageNormalizationResult,
) -> AssetNormalization {
    AssetNormalization {
        local_work_path: result.canonical_path.to_string_lossy().into_owned(),
        file_size_bytes: result.canonical_digest.bytes,
        sha256: result.canonical_digest.sha256,
        media_kind: SourceMediaKind::Image,
        mime_type: Some(result.format.mime_type().to_owned()),
        container: Some(result.format.extension().to_owned()),
        video_codec: None,
        audio_codec: None,
        width: Some(result.width),
        height: Some(result.height),
        duration_ms: None,
        bit_rate: None,
        thumbnail: Some(AssetThumbnailNormalization {
            local_work_path: result.thumbnail_path.to_string_lossy().into_owned(),
            file_size_bytes: result.thumbnail_digest.bytes,
            sha256: result.thumbnail_digest.sha256,
            mime_type: Some(result.format.mime_type().to_owned()),
            width: Some(result.thumbnail_width),
            height: Some(result.thumbnail_height),
        }),
    }
}

fn normalized_storage_limit_failure(bytes: u64, limit: u64) -> Option<HandlerFailure> {
    (bytes > limit).then(|| {
        HandlerFailure::permanent(
            "normalized_storage_too_large",
            format!(
                "canonical normalized media is {bytes} bytes, above the configured storage ceiling of {limit} bytes"
            ),
        )
    })
}

async fn fail_normalization(
    inbox: &InboxRepository,
    ingest_request_id: uuid::Uuid,
    job_attempt: &sooqa_jobs::JobAttempt,
    failure: HandlerFailure,
) -> Result<(), HandlerFailure> {
    let status = if failure.retryable {
        IngestStatus::FailedRetryable
    } else {
        IngestStatus::FailedTerminal
    };
    inbox
        .fail_asset_normalization(
            ingest_request_id,
            job_attempt,
            status,
            &failure.class,
            &failure.message,
        )
        .await
        .map_err(map_inbox_error)?;
    Err(failure)
}

async fn finalize_ingest(
    inbox: &InboxRepository,
    library: &LibraryRepository,
    job: Job,
) -> Result<(), HandlerFailure> {
    let ingest_request_id = match &job.command {
        JobCommand::FinalizeIngest(payload) => payload.ingest_id,
        _ => {
            return Err(HandlerFailure::permanent(
                "invalid_payload",
                "finalize_ingest handler received a different job command",
            ));
        }
    };
    let job_attempt = job.attempt().ok_or_else(|| {
        HandlerFailure::permanent(
            "invalid_job_state",
            "finalize_ingest handler requires a running job lease",
        )
    })?;
    let request = match inbox.begin_ingest_finalization(ingest_request_id, &job_attempt).await {
        Ok(IngestFinalizationStart::Ready(request)) => request,
        Ok(IngestFinalizationStart::AlreadyAdvanced(_)) => return Ok(()),
        Err(error) => return Err(map_inbox_error(error)),
    };
    let normalization = match request.original_input.get("normalization").cloned() {
        Some(value) => match serde_json::from_value::<AssetNormalization>(value) {
            Ok(normalization) => normalization,
            Err(error) => {
                return fail_finalization(
                    inbox,
                    ingest_request_id,
                    &job_attempt,
                    HandlerFailure::permanent(
                        "invalid_ingest_state",
                        format!("stored normalization metadata could not be decoded: {error}"),
                    ),
                )
                .await;
            }
        },
        None => {
            return fail_finalization(
                inbox,
                ingest_request_id,
                &job_attempt,
                HandlerFailure::permanent(
                    "invalid_ingest_state",
                    "ingest request has no stored normalization metadata",
                ),
            )
            .await;
        }
    };
    let metadata = match normalization_to_media_metadata(&normalization) {
        Ok(metadata) => metadata,
        Err(failure) => {
            return fail_finalization(inbox, ingest_request_id, &job_attempt, failure).await;
        }
    };
    if normalization.media_kind == SourceMediaKind::Video {
        return fail_finalization(
            inbox,
            ingest_request_id,
            &job_attempt,
            HandlerFailure::permanent(
                "invalid_ingest_state",
                "video finalization is handled by the pre-storage identity gate",
            ),
        )
        .await;
    }
    let source = source_record_for_request(&request);
    let resolution = match library
        .resolve_media(MediaIngest {
            media: NewMedia {
                kind: metadata.kind,
                title: request.page_title.clone(),
                description: request.supplied_description.clone(),
                notes: None,
            },
            metadata,
            source,
            tags: request.supplied_tags.clone(),
        })
        .await
    {
        Ok(resolution) => resolution,
        Err(error) => {
            return fail_finalization(
                inbox,
                ingest_request_id,
                &job_attempt,
                map_library_error(error),
            )
            .await;
        }
    };
    inbox
        .complete_ingest_finalization(
            ingest_request_id,
            &job_attempt,
            IngestFinalization { media_id: resolution.media.id },
        )
        .await
        .map_err(map_inbox_error)?;
    Ok(())
}

async fn compute_fingerprint(
    inbox: &InboxRepository,
    library: &LibraryRepository,
    work_root: &Path,
    extractor: &FrameExtractor,
    job: Job,
) -> Result<(), HandlerFailure> {
    let ingest_request_id = match &job.command {
        JobCommand::ComputeFingerprint(payload) => payload.ingest_id,
        _ => {
            return Err(HandlerFailure::permanent(
                "invalid_payload",
                "compute_fingerprint handler received a different job command",
            ));
        }
    };
    let job_attempt = job.attempt().ok_or_else(|| {
        HandlerFailure::permanent(
            "invalid_job_state",
            "compute_fingerprint handler requires a running job lease",
        )
    })?;
    let request = match inbox.begin_ingest_fingerprinting(ingest_request_id, &job_attempt).await {
        Ok(IngestFingerprintStart::Ready(request)) => request,
        Ok(IngestFingerprintStart::AlreadyAdvanced(_)) => return Ok(()),
        Err(error) => return Err(map_inbox_error(error)),
    };
    let normalization = match request.original_input.get("normalization").cloned() {
        Some(value) => match serde_json::from_value::<AssetNormalization>(value) {
            Ok(normalization) => normalization,
            Err(error) => {
                return fail_fingerprint(
                    inbox,
                    ingest_request_id,
                    &job_attempt,
                    HandlerFailure::permanent(
                        "invalid_ingest_state",
                        format!("stored normalization metadata could not be decoded: {error}"),
                    ),
                )
                .await;
            }
        },
        None => {
            return fail_fingerprint(
                inbox,
                ingest_request_id,
                &job_attempt,
                HandlerFailure::permanent(
                    "invalid_ingest_state",
                    "ingest request has no stored normalization metadata",
                ),
            )
            .await;
        }
    };

    if normalization.media_kind != SourceMediaKind::Video {
        return fail_fingerprint(
            inbox,
            ingest_request_id,
            &job_attempt,
            HandlerFailure::permanent(
                "invalid_ingest_state",
                "video fingerprinting was queued for a non-video normalization",
            ),
        )
        .await;
    }

    let metadata = match normalization_to_media_metadata(&normalization) {
        Ok(metadata) => metadata,
        Err(failure) => {
            return fail_fingerprint(inbox, ingest_request_id, &job_attempt, failure).await;
        }
    };
    let media_ingest = MediaIngest {
        media: NewMedia {
            kind: MediaKind::Video,
            title: request.page_title.clone(),
            description: request.supplied_description.clone(),
            notes: None,
        },
        metadata,
        source: source_record_for_request(&request),
        tags: request.supplied_tags.clone(),
    };
    let exact_media_exists = match library.resolve_exact_sha(&media_ingest).await {
        Ok(media_id) => media_id.is_some(),
        Err(error) => {
            return fail_fingerprint(
                inbox,
                ingest_request_id,
                &job_attempt,
                map_library_error(error),
            )
            .await;
        }
    };

    let fingerprint = if exact_media_exists {
        None
    } else {
        let duration_ms = match normalization.duration_ms {
            Some(duration_ms) if duration_ms > 0 => duration_ms,
            _ => {
                return fail_fingerprint(
                    inbox,
                    ingest_request_id,
                    &job_attempt,
                    HandlerFailure::permanent(
                        "invalid_ingest_state",
                        "video normalization has no valid canonical duration",
                    ),
                )
                .await;
            }
        };
        let workspace_id = match workspace_input(&request) {
            Ok((workspace_id, _)) => workspace_id,
            Err(failure) => {
                return fail_fingerprint(inbox, ingest_request_id, &job_attempt, failure).await;
            }
        };
        let workspace = match MediaWorkspace::create(work_root, workspace_id).await {
            Ok(workspace) => workspace,
            Err(error) => {
                return fail_fingerprint(
                    inbox,
                    ingest_request_id,
                    &job_attempt,
                    map_workspace_error(error),
                )
                .await;
            }
        };
        if let Err(error) = workspace.validate() {
            return fail_fingerprint(
                inbox,
                ingest_request_id,
                &job_attempt,
                map_workspace_error(error),
            )
            .await;
        }
        match extractor
            .extract_video_sequence_from_area(
                &workspace,
                WorkspaceArea::Normalized,
                "canonical.mp4",
                duration_ms,
            )
            .await
        {
            Ok(result) => Some(result),
            Err(error) => {
                return fail_fingerprint(
                    inbox,
                    ingest_request_id,
                    &job_attempt,
                    map_fingerprint_error(&job, error),
                )
                .await;
            }
        }
    };

    inbox
        .finalize_video_identity(
            ingest_request_id,
            &job_attempt,
            media_ingest,
            fingerprint.as_ref(),
            SequenceAlignmentConfig::default(),
        )
        .await
        .map_err(map_inbox_error)?;
    Ok(())
}

fn map_fingerprint_error(job: &Job, error: FrameExtractionError) -> HandlerFailure {
    let message = error.to_string();
    let retryable = matches!(&error, FrameExtractionError::Command(error) if error.is_timeout())
        && job.attempt_count < job.max_attempts;
    if retryable {
        HandlerFailure::retryable("fingerprint_timeout", message)
    } else {
        HandlerFailure::permanent("fingerprint_failed", message)
    }
}

async fn fail_fingerprint(
    inbox: &InboxRepository,
    ingest_request_id: uuid::Uuid,
    job_attempt: &sooqa_jobs::JobAttempt,
    failure: HandlerFailure,
) -> Result<(), HandlerFailure> {
    let status = if failure.retryable {
        IngestStatus::FailedRetryable
    } else {
        IngestStatus::FailedTerminal
    };
    inbox
        .fail_ingest_fingerprint(
            ingest_request_id,
            job_attempt,
            status,
            &failure.class,
            &failure.message,
        )
        .await
        .map_err(map_inbox_error)?;
    Err(failure)
}

fn normalization_to_media_metadata(
    normalization: &AssetNormalization,
) -> Result<MediaMetadata, HandlerFailure> {
    Ok(MediaMetadata {
        kind: media_kind_for_normalization(normalization.media_kind)?,
        mime_type: normalization.mime_type.clone(),
        container: normalization.container.clone(),
        video_codec: normalization.video_codec.clone(),
        audio_codec: normalization.audio_codec.clone(),
        width: to_database_dimension(normalization.width, "width")?,
        height: to_database_dimension(normalization.height, "height")?,
        duration_ms: normalization.duration_ms,
        bit_rate: normalization.bit_rate,
        file_size_bytes: Some(normalization.file_size_bytes),
        sha256: Some(decode_sha256(&normalization.sha256)?),
        local_work_path: Some(normalization.local_work_path.clone()),
    })
}

fn media_kind_for_normalization(media_kind: SourceMediaKind) -> Result<MediaKind, HandlerFailure> {
    match media_kind {
        SourceMediaKind::Video => Ok(MediaKind::Video),
        SourceMediaKind::Image => Ok(MediaKind::Image),
        SourceMediaKind::Audio => Ok(MediaKind::Audio),
        SourceMediaKind::Animation => Ok(MediaKind::Animation),
        SourceMediaKind::Unknown => Err(HandlerFailure::permanent(
            "invalid_normalization",
            "normalized media kind is unknown",
        )),
    }
}

fn to_database_dimension(
    value: Option<u32>,
    field: &'static str,
) -> Result<Option<i32>, HandlerFailure> {
    value
        .map(|value| {
            i32::try_from(value).map_err(|_| {
                HandlerFailure::permanent(
                    "invalid_normalization",
                    format!("normalized {field} does not fit the library schema"),
                )
            })
        })
        .transpose()
}

fn decode_sha256(value: &str) -> Result<Vec<u8>, HandlerFailure> {
    let bytes = value.as_bytes();
    if bytes.len() != 64 || !bytes.is_ascii() {
        return Err(HandlerFailure::permanent(
            "invalid_normalization",
            "normalized SHA-256 digest must contain 64 hexadecimal characters",
        ));
    }
    let mut digest = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let high = decode_hex_digit(pair[0]).ok_or_else(|| {
            HandlerFailure::permanent(
                "invalid_normalization",
                "normalized SHA-256 digest is not hexadecimal",
            )
        })?;
        let low = decode_hex_digit(pair[1]).ok_or_else(|| {
            HandlerFailure::permanent(
                "invalid_normalization",
                "normalized SHA-256 digest is not hexadecimal",
            )
        })?;
        digest.push((high << 4) | low);
    }
    Ok(digest)
}

fn decode_hex_digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn source_record_for_request(request: &sooqa_inbox::Ingest) -> MediaSourceInput {
    let (kind, normalized_url, platform, platform_content_id) = match request.kind {
        IngestKind::Url => (SourceKind::DirectUrl, Some(request.source_url.clone()), None, None),
        IngestKind::TelegramMessage => (
            SourceKind::Telegram,
            None,
            Some("telegram".to_owned()),
            Some(request.source_url.clone()),
        ),
        IngestKind::Upload => (
            SourceKind::Upload,
            None,
            Some("sooqa_ingest".to_owned()),
            Some(request.id.to_string()),
        ),
    };
    MediaSourceInput {
        ingest_id: Some(request.id),
        kind,
        original_url: Some(request.source_url.clone()),
        normalized_url,
        platform,
        platform_content_id,
        author_name: None,
        title: request.page_title.clone(),
        description: request.supplied_description.clone().or_else(|| {
            if request.kind == IngestKind::TelegramMessage {
                request.supplied_caption.clone()
            } else {
                None
            }
        }),
        published_at: None,
        metadata: source_provenance_for_request(request),
    }
}

#[derive(Debug, serde::Serialize)]
struct SourceProvenance {
    #[serde(skip_serializing_if = "Option::is_none")]
    page_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    media_kind: Option<SourceMediaKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    telegram_update_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    telegram_chat_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    telegram_message_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    telegram_file_unique_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    two_ch_mirror: Option<serde_json::Value>,
}

fn source_provenance_for_request(request: &sooqa_inbox::Ingest) -> serde_json::Value {
    let input = &request.original_input;
    let download = input.get("download");
    let media_kind = request_media_kind(request);
    let mime_type = if request.kind == IngestKind::Url {
        download
            .and_then(|value| value.get("mime_type"))
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned)
    } else {
        input.get("mime_type").and_then(serde_json::Value::as_str).map(ToOwned::to_owned)
    };
    let source_size_bytes = if request.kind == IngestKind::Url {
        download.and_then(|value| value.get("bytes")).and_then(serde_json::Value::as_u64)
    } else {
        input.get("file_size").and_then(serde_json::Value::as_u64)
    };
    let two_ch_mirror = input
        .get("inspection")
        .and_then(|value| value.get("metadata"))
        .and_then(|value| value.get("two_ch_mirror"))
        .cloned();
    let provenance = SourceProvenance {
        page_url: request.page_url.clone(),
        media_kind,
        mime_type,
        source_size_bytes,
        telegram_update_id: input.get("telegram_update_id").and_then(serde_json::Value::as_i64),
        telegram_chat_id: input.get("telegram_chat_id").and_then(serde_json::Value::as_i64),
        telegram_message_id: input.get("telegram_message_id").and_then(serde_json::Value::as_i64),
        telegram_file_unique_id: input
            .get("telegram_file_unique_id")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
        two_ch_mirror,
    };
    serde_json::to_value(provenance).expect("source provenance is serializable")
}

fn map_library_error(error: LibraryRepositoryError) -> HandlerFailure {
    let message = error.to_string();
    match error {
        LibraryRepositoryError::Database(_) => HandlerFailure::retryable("database_error", message),
        _ => HandlerFailure::permanent("library_error", message),
    }
}

async fn fail_finalization(
    inbox: &InboxRepository,
    ingest_request_id: uuid::Uuid,
    job_attempt: &sooqa_jobs::JobAttempt,
    failure: HandlerFailure,
) -> Result<(), HandlerFailure> {
    let status = if failure.retryable {
        IngestStatus::FailedRetryable
    } else {
        IngestStatus::FailedTerminal
    };
    inbox
        .fail_ingest_finalization(
            ingest_request_id,
            job_attempt,
            status,
            &failure.class,
            &failure.message,
        )
        .await
        .map_err(map_inbox_error)?;
    Err(failure)
}

async fn upload_storage_asset<A, S>(
    inbox: &InboxRepository,
    provider: &StorageUploadProvider<A, S>,
    job: Job,
) -> Result<(), HandlerFailure>
where
    A: TelegramStorageApi,
    S: StorageUploadStore,
{
    let (media_id, generation) = match &job.command {
        JobCommand::UploadStorageAsset(payload) => (payload.media_id, payload.generation),
        _ => {
            return Err(HandlerFailure::permanent(
                "invalid_payload",
                "upload_storage_asset handler received a different job command",
            ));
        }
    };

    match provider.upload(StorageUploadInput { media_id, generation }).await {
        Ok(_) => {
            inbox.complete_storage_for_media(media_id).await.map_err(map_inbox_error)?;
            Ok(())
        }
        Err(error) => {
            let message = error.to_string();
            if matches!(&error, StorageUploadError::StaleGeneration { .. }) {
                return Ok(());
            }
            if let StorageUploadError::InProgress { retry_at: Some(retry_at) } = &error
                && job.attempt_count < job.max_attempts
            {
                return Err(HandlerFailure::defer(
                    "storage_upload_in_progress",
                    message,
                    *retry_at,
                ));
            }
            let terminal = !error.is_retryable() || job.attempt_count >= job.max_attempts;
            let status =
                if terminal { IngestStatus::FailedTerminal } else { IngestStatus::FailedRetryable };
            inbox
                .fail_storage_for_media(media_id, status, "storage_upload", &message)
                .await
                .map_err(map_inbox_error)?;
            if terminal {
                Err(HandlerFailure::permanent("storage_upload", message))
            } else {
                Err(HandlerFailure::retryable("storage_upload", message))
            }
        }
    }
}

async fn publish_post<A>(
    publisher: &sooqa_persistence::PublisherRepository,
    library: &LibraryRepository,
    telegram: &A,
    job: Job,
) -> Result<(), HandlerFailure>
where
    A: TelegramPublicationApi,
{
    let (post_id, expected_revision) = match &job.command {
        JobCommand::PublishPost(payload) => (payload.post_id, payload.expected_revision),
        _ => {
            return Err(HandlerFailure::permanent(
                "invalid_payload",
                "publish_post handler received a different job command",
            ));
        }
    };
    let attempt = job.attempt().ok_or_else(|| {
        HandlerFailure::permanent(
            "invalid_job_state",
            "publish_post handler requires a running job lease",
        )
    })?;
    let claim = match publisher.claim_publish(post_id, expected_revision, &attempt).await {
        Ok(claim) => claim,
        Err(sooqa_persistence::PublisherRepositoryError::PostNotClaimable {
            state: sooqa_publisher::PostState::Sending,
            ..
        }) => match publisher.reconcile_interrupted_publish(post_id, &attempt).await {
            Ok(_) | Err(sooqa_persistence::PublisherRepositoryError::PublishLeaseLost(_)) => {
                return Ok(());
            }
            Err(error) => return Err(map_publisher_error(error)),
        },
        Err(
            sooqa_persistence::PublisherRepositoryError::PostNotClaimable { .. }
            | sooqa_persistence::PublisherRepositoryError::StalePublicationJob { .. }
            | sooqa_persistence::PublisherRepositoryError::PublishLeaseLost(_),
        ) => return Ok(()),
        Err(error) => return Err(map_publisher_error(error)),
    };
    let token = claim.post.send_token.ok_or_else(|| {
        HandlerFailure::permanent(
            "invalid_publication_state",
            "publication claim has no send token",
        )
    })?;
    let lease = sooqa_persistence::PublishLease {
        generation: claim.post.send_generation,
        token,
        attempt: attempt.clone(),
    };
    let receipt = match library.find_storage_receipt(claim.post.media_id).await {
        Ok(Some(receipt)) => receipt,
        Ok(None) => {
            return record_publication_failure(
                publisher,
                &claim,
                &lease,
                sooqa_publisher::PostState::Failed,
                "storage_receipt_missing",
                "media has no ready Telegram storage receipt",
            )
            .await;
        }
        Err(error) if matches!(error, LibraryRepositoryError::Database(_)) => {
            return settle_pre_send_retryable_error(
                publisher,
                &claim,
                &lease,
                "storage_receipt_lookup",
                &error.to_string(),
            )
            .await;
        }
        Err(error) => {
            return record_publication_failure(
                publisher,
                &claim,
                &lease,
                sooqa_publisher::PostState::Failed,
                "storage_receipt_invalid",
                &error.to_string(),
            )
            .await;
        }
    };
    let request = TelegramPublicationRequest {
        target_chat_id: claim.channel_chat_id,
        storage_chat_id: receipt.storage_chat_id,
        storage_message_id: receipt.storage_message_id,
        telegram_file_id: receipt.telegram_file_id,
        media_kind: receipt.media_kind,
        caption: claim.post.caption.clone(),
        parse_mode: claim.post.parse_mode.clone(),
        disable_notification: claim.post.disable_notification,
    };
    let message_id = match telegram.copy_from_storage(&request).await {
        Ok(message_id) => message_id,
        Err(error) if A::is_copy_unavailable(&error) => {
            if request.telegram_file_id.is_none() {
                return record_publication_failure(
                    publisher,
                    &claim,
                    &lease,
                    sooqa_publisher::PostState::Failed,
                    "storage_file_reference_missing",
                    "copyMessage was unavailable and the storage receipt has no file ID",
                )
                .await;
            }
            match telegram.send_storage_file(&request).await {
                Ok(message_id) => message_id,
                Err(error) => {
                    return settle_publication_transport_error::<A>(
                        publisher, &claim, &lease, error,
                    )
                    .await;
                }
            }
        }
        Err(error) => {
            return settle_publication_transport_error::<A>(publisher, &claim, &lease, error).await;
        }
    };

    match publisher.complete_publish(post_id, &lease, message_id).await {
        Ok(_) | Err(sooqa_persistence::PublisherRepositoryError::PublishConflict(_)) => Ok(()),
        Err(sooqa_persistence::PublisherRepositoryError::PublishLeaseLost(_)) => Ok(()),
        Err(error) => {
            let message = error.to_string();
            match publisher
                .fail_publish(
                    post_id,
                    &lease,
                    sooqa_publisher::PostState::Unknown,
                    "publication_commit",
                    &message,
                )
                .await
            {
                Ok(_) => Err(HandlerFailure::permanent("publication_commit", message)),
                Err(sooqa_persistence::PublisherRepositoryError::PublishLeaseLost(_)) => Ok(()),
                Err(failure) => Err(HandlerFailure::permanent(
                    "publication_commit",
                    format!("{message}; could not fence the publication as unknown: {failure}"),
                )),
            }
        }
    }
}

async fn materialize_publication(
    publisher: &sooqa_persistence::PublisherRepository,
    job: Job,
) -> Result<(), HandlerFailure> {
    let ingest_id = match &job.command {
        JobCommand::MaterializePublication(payload) => payload.ingest_id,
        _ => {
            return Err(HandlerFailure::permanent(
                "invalid_payload",
                "materialize_publication handler received a different job command",
            ));
        }
    };
    publisher.materialize_ingest(ingest_id).await.map(|_| ()).map_err(map_publisher_error)
}

async fn settle_publication_transport_error<A>(
    publisher: &sooqa_persistence::PublisherRepository,
    claim: &sooqa_publisher::PublishClaim,
    lease: &sooqa_persistence::PublishLease,
    error: A::Error,
) -> Result<(), HandlerFailure>
where
    A: TelegramPublicationApi,
{
    let message = error.to_string();
    if A::is_retryable_no_effect(&error) {
        return match publisher
            .retry_publish(claim.post.id, lease, "telegram_retryable", &message)
            .await
        {
            Ok(result) if result.terminal => {
                Err(HandlerFailure::permanent("telegram_retryable", message))
            }
            Ok(_) => Err(HandlerFailure::retryable("telegram_retryable", message)),
            Err(sooqa_persistence::PublisherRepositoryError::PublishLeaseLost(_)) => Ok(()),
            Err(error) => Err(HandlerFailure::permanent(
                "publication_state",
                format!("could not requeue a safe publication retry: {error}"),
            )),
        };
    }
    let (state, class) = if A::is_known_caption_error(&error) {
        (sooqa_publisher::PostState::Failed, "caption_rejected")
    } else if A::is_ambiguous_error(&error) {
        (sooqa_publisher::PostState::Unknown, "publication_unknown")
    } else {
        (sooqa_publisher::PostState::Failed, "publication_rejected")
    };
    record_publication_failure(publisher, claim, lease, state, class, &message).await
}

async fn settle_pre_send_retryable_error(
    publisher: &sooqa_persistence::PublisherRepository,
    claim: &sooqa_publisher::PublishClaim,
    lease: &sooqa_persistence::PublishLease,
    error_class: &str,
    error_message: &str,
) -> Result<(), HandlerFailure> {
    match publisher.retry_publish(claim.post.id, lease, error_class, error_message).await {
        Ok(result) if result.terminal => Err(HandlerFailure::permanent(error_class, error_message)),
        Ok(_) => Err(HandlerFailure::retryable(error_class, error_message)),
        Err(sooqa_persistence::PublisherRepositoryError::PublishLeaseLost(_)) => Ok(()),
        Err(error) => Err(HandlerFailure::permanent(
            "publication_state",
            format!("could not requeue a safe publication retry: {error}"),
        )),
    }
}

async fn record_publication_failure(
    publisher: &sooqa_persistence::PublisherRepository,
    claim: &sooqa_publisher::PublishClaim,
    lease: &sooqa_persistence::PublishLease,
    state: sooqa_publisher::PostState,
    error_class: &str,
    error_message: &str,
) -> Result<(), HandlerFailure> {
    match publisher.fail_publish(claim.post.id, lease, state, error_class, error_message).await {
        Ok(_) => Err(HandlerFailure::permanent(error_class, error_message)),
        Err(sooqa_persistence::PublisherRepositoryError::PublishLeaseLost(_)) => Ok(()),
        Err(error) => Err(HandlerFailure::permanent(
            "publication_state",
            format!("could not record publication outcome: {error}"),
        )),
    }
}

fn map_publisher_error(error: sooqa_persistence::PublisherRepositoryError) -> HandlerFailure {
    let message = error.to_string();
    match error {
        sooqa_persistence::PublisherRepositoryError::Database(_) => {
            HandlerFailure::retryable("database_error", message)
        }
        sooqa_persistence::PublisherRepositoryError::ChannelDisabled(_)
        | sooqa_persistence::PublisherRepositoryError::MediaNotReady { .. }
        | sooqa_persistence::PublisherRepositoryError::MaterializationNotReady { .. } => {
            HandlerFailure::retryable("publication_dependency", message)
        }
        _ => HandlerFailure::permanent("publication_state", message),
    }
}

async fn inspect_source(
    inbox: &InboxRepository,
    downloader: &dyn SourceDownloader,
    job: Job,
) -> Result<(), HandlerFailure> {
    let ingest_request_id = match &job.command {
        JobCommand::InspectSource(payload) => payload.ingest_id,
        _ => {
            return Err(HandlerFailure::permanent(
                "invalid_payload",
                "inspect_source handler received a different job command",
            ));
        }
    };

    let job_attempt = job.attempt().ok_or_else(|| {
        HandlerFailure::permanent(
            "invalid_job_state",
            "inspect_source handler requires a running job lease",
        )
    })?;
    let request = match inbox.begin_source_inspection(ingest_request_id, &job_attempt).await {
        Ok(SourceInspectionStart::Ready(request)) => request,
        Ok(SourceInspectionStart::AlreadyAdvanced(_)) => return Ok(()),
        Err(error) => return Err(map_inbox_error(error)),
    };

    let source = SourceInput {
        ingest_request_id,
        source_url: request.source_url,
        page_url: request.page_url,
    };
    let inspection = match downloader.inspect(&source).await {
        Ok(inspection) => inspection,
        Err(error) => {
            let terminal = !error.is_retryable() || job.attempt_count >= job.max_attempts;
            let status =
                if terminal { IngestStatus::FailedTerminal } else { IngestStatus::FailedRetryable };
            let class = error.class().to_owned();
            let message = error.to_string();
            inbox
                .fail_source_inspection(ingest_request_id, &job_attempt, status, &class, &message)
                .await
                .map_err(map_inbox_error)?;
            return Err(if terminal {
                HandlerFailure::permanent(class, message)
            } else {
                HandlerFailure::retryable(class, message)
            });
        }
    };

    inbox
        .complete_source_inspection(ingest_request_id, &job_attempt, inspection)
        .await
        .map_err(map_inbox_error)?;
    Ok(())
}

async fn download_source(
    inbox: &InboxRepository,
    work_root: &Path,
    downloader: &dyn SourceDownloader,
    limits: &DownloadLimits,
    job: Job,
) -> Result<(), HandlerFailure> {
    let (ingest_request_id, inspection) = match &job.command {
        JobCommand::DownloadSource(payload) => (payload.ingest_id, payload.inspection.clone()),
        _ => {
            return Err(HandlerFailure::permanent(
                "invalid_payload",
                "download_source handler received a different job command",
            ));
        }
    };
    let job_attempt = job.attempt().ok_or_else(|| {
        HandlerFailure::permanent(
            "invalid_job_state",
            "download_source handler requires a running job lease",
        )
    })?;

    match inbox.begin_source_download(ingest_request_id, &job_attempt).await {
        Ok(SourceDownloadStart::Ready(_)) => {}
        Ok(SourceDownloadStart::AlreadyAdvanced(_)) => return Ok(()),
        Err(error) => return Err(map_inbox_error(error)),
    }

    let workspace = match MediaWorkspace::create(work_root, ingest_request_id).await {
        Ok(workspace) => workspace,
        Err(error) => {
            return fail_download(
                inbox,
                ingest_request_id,
                &job_attempt,
                map_workspace_error(error),
            )
            .await;
        }
    };
    if let Err(error) = workspace.validate() {
        return fail_download(inbox, ingest_request_id, &job_attempt, map_workspace_error(error))
            .await;
    }
    let source_path = match workspace.path(WorkspaceArea::Source, "source.bin") {
        Ok(path) => path,
        Err(error) => {
            return fail_download(
                inbox,
                ingest_request_id,
                &job_attempt,
                map_workspace_error(error),
            )
            .await;
        }
    };
    let attempt_path = match workspace
        .path(WorkspaceArea::Source, &format!(".sooqa-source-{}.tmp", Uuid::new_v4()))
    {
        Ok(path) => path,
        Err(error) => {
            return fail_download(
                inbox,
                ingest_request_id,
                &job_attempt,
                map_workspace_error(error),
            )
            .await;
        }
    };
    let attempt = DownloadAttemptArtifact::new(attempt_path);

    let downloaded = match downloader.download(&inspection, attempt.path(), limits).await {
        Ok(downloaded) => downloaded,
        Err(error) => {
            let terminal = !error.is_retryable() || job.attempt_count >= job.max_attempts;
            let failure = download_failure(error, terminal);
            return fail_download(inbox, ingest_request_id, &job_attempt, failure).await;
        }
    };

    if let Err(failure) = validate_downloaded_source(&workspace, attempt.path(), &downloaded).await
    {
        return fail_download(inbox, ingest_request_id, &job_attempt, failure).await;
    }
    let downloaded = match publish_artifact(attempt.path(), &source_path).await {
        Ok(()) => {
            let published =
                match read_source_artifact(&workspace, &source_path, downloaded.mime_type.clone())
                    .await
                {
                    Ok(published) => published,
                    Err(failure) => {
                        return fail_download(inbox, ingest_request_id, &job_attempt, failure)
                            .await;
                    }
                };
            if published.bytes != downloaded.bytes {
                return fail_download(
                    inbox,
                    ingest_request_id,
                    &job_attempt,
                    HandlerFailure::permanent(
                        "download_artifact",
                        "published source size does not match adapter metadata",
                    ),
                )
                .await;
            }
            published
        }
        Err(ArtifactPublicationError::DestinationConflict) => {
            match read_source_artifact(&workspace, &source_path, downloaded.mime_type.clone()).await
            {
                Ok(published) => published,
                Err(failure) => {
                    return fail_download(inbox, ingest_request_id, &job_attempt, failure).await;
                }
            }
        }
        Err(error) => {
            return fail_download(
                inbox,
                ingest_request_id,
                &job_attempt,
                HandlerFailure::permanent("download_artifact", error.to_string()),
            )
            .await;
        }
    };

    inbox
        .complete_source_download(
            ingest_request_id,
            &job_attempt,
            SourceDownload {
                bytes: downloaded.bytes,
                mime_type: downloaded.mime_type,
                media_kind: inspection.media_kind,
            },
        )
        .await
        .map_err(map_inbox_error)?;
    Ok(())
}

fn download_failure(error: DownloadError, terminal: bool) -> HandlerFailure {
    let class = error.class().to_owned();
    let message = error.to_string();
    if terminal {
        HandlerFailure::permanent(class, message)
    } else {
        HandlerFailure::retryable(class, message)
    }
}

async fn validate_downloaded_source(
    workspace: &MediaWorkspace,
    source_path: &Path,
    downloaded: &DownloadedSource,
) -> Result<(), HandlerFailure> {
    workspace.validate().map_err(map_workspace_error)?;
    if downloaded.path != source_path {
        return Err(HandlerFailure::permanent(
            "download_artifact",
            "source downloader returned a path outside the requested workspace destination",
        ));
    }
    let stored = read_source_artifact(workspace, source_path, downloaded.mime_type.clone()).await?;
    if stored.bytes != downloaded.bytes {
        return Err(HandlerFailure::permanent(
            "download_artifact",
            "downloaded source size does not match adapter metadata",
        ));
    }
    Ok(())
}

async fn read_source_artifact(
    workspace: &MediaWorkspace,
    source_path: &Path,
    mime_type: Option<String>,
) -> Result<DownloadedSource, HandlerFailure> {
    workspace.validate().map_err(map_workspace_error)?;
    let metadata = tokio::fs::symlink_metadata(source_path).await.map_err(|error| {
        HandlerFailure::permanent(
            "download_artifact",
            format!("downloaded source is not accessible: {error}"),
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(HandlerFailure::permanent(
            "download_artifact",
            "downloaded source is not a regular file",
        ));
    }
    Ok(DownloadedSource { path: source_path.to_owned(), bytes: metadata.len(), mime_type })
}

async fn fail_download(
    inbox: &InboxRepository,
    ingest_request_id: uuid::Uuid,
    job_attempt: &sooqa_jobs::JobAttempt,
    failure: HandlerFailure,
) -> Result<(), HandlerFailure> {
    let status = if failure.retryable {
        IngestStatus::FailedRetryable
    } else {
        IngestStatus::FailedTerminal
    };
    inbox
        .fail_source_download(
            ingest_request_id,
            job_attempt,
            status,
            &failure.class,
            &failure.message,
        )
        .await
        .map_err(map_inbox_error)?;
    Err(failure)
}

fn map_inbox_error(error: InboxRepositoryError) -> HandlerFailure {
    let message = error.to_string();
    match error {
        InboxRepositoryError::Library(error) => map_library_error(error),
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

#[derive(Debug, Default)]
pub struct WorkerMetrics {
    polls: AtomicU64,
    claimed: AtomicU64,
    succeeded: AtomicU64,
    retried: AtomicU64,
    failed: AtomicU64,
    shutdown_requeued: AtomicU64,
}

impl WorkerMetrics {
    pub fn snapshot(&self) -> WorkerMetricsSnapshot {
        WorkerMetricsSnapshot {
            polls: self.polls.load(Ordering::Relaxed),
            claimed: self.claimed.load(Ordering::Relaxed),
            succeeded: self.succeeded.load(Ordering::Relaxed),
            retried: self.retried.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            shutdown_requeued: self.shutdown_requeued.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct WorkerMetricsSnapshot {
    pub polls: u64,
    pub claimed: u64,
    pub succeeded: u64,
    pub retried: u64,
    pub failed: u64,
    pub shutdown_requeued: u64,
}

pub struct Worker {
    repository: JobRepository,
    registry: HandlerRegistry,
    worker_id: String,
    poll_interval: Duration,
    lease_duration: Duration,
    metrics: Arc<WorkerMetrics>,
}

impl Worker {
    pub fn new(
        repository: JobRepository,
        registry: HandlerRegistry,
        worker_id: impl Into<String>,
        poll_interval: Duration,
        lease_duration: Duration,
    ) -> Result<Self, WorkerError> {
        validate_timing(poll_interval, lease_duration)?;

        Ok(Self {
            repository,
            registry,
            worker_id: worker_id.into(),
            poll_interval,
            lease_duration,
            metrics: Arc::new(WorkerMetrics::default()),
        })
    }

    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    pub fn metrics(&self) -> Arc<WorkerMetrics> {
        Arc::clone(&self.metrics)
    }

    pub async fn run<F>(&self, shutdown: F) -> Result<(), WorkerError>
    where
        F: Future<Output = ()> + Send,
    {
        let capabilities = self.registry.job_types();
        let recovered = self.repository.recover_stale_leases().await?;
        if recovered > 0 {
            info!(worker_id = %self.worker_id, recovered, "recovered stale job leases before polling");
        }

        let (stop_recovery, recovery_signal) = watch::channel(false);
        let recovery_repository = self.repository.clone();
        let recovery_interval = recovery_interval(self.lease_duration);
        let recovery_task = tokio::spawn(async move {
            recover_stale_leases_periodically(
                recovery_repository,
                recovery_interval,
                recovery_signal,
            )
            .await;
        });
        let result = self.run_loop(shutdown, &capabilities).await;
        let _ = stop_recovery.send(true);
        if let Err(error) = recovery_task.await {
            warn!(?error, worker_id = %self.worker_id, "stale-lease recovery task stopped unexpectedly");
        }
        result
    }

    async fn run_loop<F>(&self, shutdown: F, capabilities: &[JobType]) -> Result<(), WorkerError>
    where
        F: Future<Output = ()> + Send,
    {
        tokio::pin!(shutdown);
        info!(worker_id = %self.worker_id, "worker loop started");

        loop {
            self.metrics.polls.fetch_add(1, Ordering::Relaxed);
            debug!(worker_id = %self.worker_id, "polling for a job");
            let claimed = tokio::select! {
                _ = &mut shutdown => break,
                result = self.repository.claim_next(
                    &self.worker_id,
                    self.lease_duration,
                    capabilities,
                ) => result?,
            };

            let Some(job) = claimed else {
                tokio::select! {
                    _ = &mut shutdown => break,
                    _ = sleep(self.poll_interval) => continue,
                }
            };

            self.metrics.claimed.fetch_add(1, Ordering::Relaxed);
            info!(worker_id = %self.worker_id, job_id = %job.id, job_type = %job.job_type(), "job claimed");
            let lease = job.lease().ok_or(JobRepositoryError::LeaseLost)?;

            let Some(handler) = self.registry.handler(job.job_type()) else {
                self.repository
                    .fail_lease(
                        &lease,
                        "handler_not_registered",
                        "no handler is registered for this job type",
                    )
                    .await?;
                self.metrics.failed.fetch_add(1, Ordering::Relaxed);
                warn!(worker_id = %self.worker_id, job_id = %job.id, job_type = %job.job_type(), "job failed because no handler is registered");
                continue;
            };

            let outcome = self.execute_handler(&job, &lease, handler, &mut shutdown).await?;
            let stop_after_shutdown = match outcome {
                HandlerRunOutcome::Completed(result) => {
                    self.finish_job(&job, &lease, result).await?;
                    false
                }
                HandlerRunOutcome::Shutdown => {
                    self.repository
                        .retry_lease(
                            &lease,
                            OffsetDateTime::now_utc(),
                            "worker_shutdown",
                            "worker stopped while the job was active",
                        )
                        .await?;
                    self.metrics.shutdown_requeued.fetch_add(1, Ordering::Relaxed);
                    true
                }
            };

            if stop_after_shutdown {
                break;
            }
        }

        info!(worker_id = %self.worker_id, "worker loop stopped");
        Ok(())
    }

    async fn execute_handler<F>(
        &self,
        job: &Job,
        lease: &JobLease,
        handler: HandlerFn,
        shutdown: &mut std::pin::Pin<&mut F>,
    ) -> Result<HandlerRunOutcome, WorkerError>
    where
        F: Future<Output = ()> + Send,
    {
        let (stop_heartbeat, heartbeat_signal) = oneshot::channel();
        let heartbeat_repository = self.repository.clone();
        let heartbeat_lease = lease.clone();
        let heartbeat_lease_duration = self.lease_duration;
        let mut heartbeat_task = tokio::spawn(async move {
            heartbeat_loop(
                heartbeat_repository,
                heartbeat_lease,
                heartbeat_lease_duration,
                heartbeat_signal,
            )
            .await
        });
        let handler_future = handler(job.clone());
        tokio::pin!(handler_future);

        tokio::select! {
            _ = shutdown.as_mut() => {
                let _ = stop_heartbeat.send(());
                await_heartbeat(&mut heartbeat_task).await?;
                Ok(HandlerRunOutcome::Shutdown)
            }
            result = &mut handler_future => {
                let _ = stop_heartbeat.send(());
                await_heartbeat(&mut heartbeat_task).await?;
                Ok(HandlerRunOutcome::Completed(result))
            }
            result = &mut heartbeat_task => {
                let result = result.map_err(map_heartbeat_join_error)?;
                match result {
                    Ok(()) => Err(WorkerError::HeartbeatTask(
                        "heartbeat loop stopped while the handler was active".to_owned(),
                    )),
                    Err(error) => Err(WorkerError::Repository(error)),
                }
            }
        }
    }

    async fn finish_job(
        &self,
        job: &Job,
        lease: &JobLease,
        result: Result<(), HandlerFailure>,
    ) -> Result<(), WorkerError> {
        match result {
            Ok(()) => {
                self.repository.complete_lease(lease).await?;
                self.metrics.succeeded.fetch_add(1, Ordering::Relaxed);
                info!(worker_id = %self.worker_id, job_id = %job.id, "job completed");
            }
            Err(failure) if failure.defer_until.is_some() => {
                self.repository
                    .defer_lease(
                        lease,
                        failure.defer_until.expect("defer timestamp was checked"),
                        &failure.class,
                        &failure.message,
                    )
                    .await?;
                info!(worker_id = %self.worker_id, job_id = %job.id, "job deferred until dependent lease expires");
            }
            Err(failure) if failure.retryable => {
                let updated = self
                    .repository
                    .retry_lease(
                        lease,
                        OffsetDateTime::now_utc() + TimeDuration::seconds(1),
                        &failure.class,
                        &failure.message,
                    )
                    .await?;
                if updated.status == JobStatus::Queued {
                    self.metrics.retried.fetch_add(1, Ordering::Relaxed);
                    info!(worker_id = %self.worker_id, job_id = %job.id, "job scheduled for retry");
                } else {
                    self.metrics.failed.fetch_add(1, Ordering::Relaxed);
                    warn!(worker_id = %self.worker_id, job_id = %job.id, "job exhausted its retry attempts");
                }
            }
            Err(failure) => {
                self.repository.fail_lease(lease, &failure.class, &failure.message).await?;
                self.metrics.failed.fetch_add(1, Ordering::Relaxed);
                warn!(worker_id = %self.worker_id, job_id = %job.id, error_class = %failure.class, "job failed");
            }
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error("worker poll interval must be greater than zero")]
    InvalidPollInterval,
    #[error("worker lease duration must be greater than zero")]
    InvalidLeaseDuration,
    #[error("job repository error: {0}")]
    Repository(#[from] JobRepositoryError),
    #[error("worker heartbeat task failed: {0}")]
    HeartbeatTask(String),
}

enum HandlerRunOutcome {
    Completed(Result<(), HandlerFailure>),
    Shutdown,
}

async fn heartbeat_loop(
    repository: JobRepository,
    lease: JobLease,
    lease_duration: Duration,
    mut stop: oneshot::Receiver<()>,
) -> Result<(), JobRepositoryError> {
    let interval = heartbeat_interval(lease_duration);
    loop {
        tokio::select! {
            _ = &mut stop => return Ok(()),
            _ = sleep(interval) => {
                repository.heartbeat_lease(&lease, lease_duration).await?;
            }
        }
    }
}

async fn await_heartbeat(
    task: &mut tokio::task::JoinHandle<Result<(), JobRepositoryError>>,
) -> Result<(), WorkerError> {
    task.await.map_err(map_heartbeat_join_error)??;
    Ok(())
}

fn map_heartbeat_join_error(error: JoinError) -> WorkerError {
    WorkerError::HeartbeatTask(error.to_string())
}

async fn recover_stale_leases_periodically(
    repository: JobRepository,
    interval: Duration,
    mut stop: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    return;
                }
            }
            _ = sleep(interval) => {
                match repository.recover_stale_leases().await {
                    Ok(recovered) if recovered > 0 => {
                        info!(recovered, "recovered stale job leases");
                    }
                    Ok(_) => {}
                    Err(error) => {
                        warn!(?error, "periodic stale-lease recovery failed");
                    }
                }
            }
        }
    }
}

fn heartbeat_interval(lease_duration: Duration) -> Duration {
    let interval = lease_duration / 3;
    if interval.is_zero() { Duration::from_millis(1) } else { interval }
}

fn recovery_interval(lease_duration: Duration) -> Duration {
    let interval = lease_duration / 2;
    if interval.is_zero() { Duration::from_millis(1) } else { interval }
}

fn validate_timing(poll_interval: Duration, lease_duration: Duration) -> Result<(), WorkerError> {
    if poll_interval.is_zero() {
        return Err(WorkerError::InvalidPollInterval);
    }
    if lease_duration.is_zero() {
        return Err(WorkerError::InvalidLeaseDuration);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use sooqa_inbox::{Ingest, IngestSubmission, IngestSubmissionInput, SubmittedVia};
    use sooqa_media::CommandError;

    use super::*;

    fn running_compute_job(attempt_count: i32, max_attempts: i32) -> Job {
        let now = OffsetDateTime::now_utc();
        Job {
            id: Uuid::new_v4(),
            command: JobCommand::ComputeFingerprint(sooqa_jobs::IngestJobPayload {
                ingest_id: Uuid::new_v4(),
            }),
            status: JobStatus::Running,
            priority: 0,
            run_at: now,
            attempt_count,
            max_attempts,
            lease_token: Some(Uuid::new_v4()),
            lease_owner: Some("test-worker".to_owned()),
            lease_expires_at: None,
            last_heartbeat_at: None,
            last_error_class: None,
            last_error_message: None,
            dedupe_key: None,
            created_at: now,
            updated_at: now,
            completed_at: None,
        }
    }

    fn test_handler(_job: Job) -> HandlerFuture {
        Box::pin(async { Ok(()) })
    }

    #[test]
    fn registry_returns_registered_handler() {
        let mut registry = HandlerRegistry::new();
        registry.register(JobType::CleanupWorkspace, test_handler);

        assert!(registry.contains(JobType::CleanupWorkspace));
        assert!(!registry.contains(JobType::PublishPost));
    }

    #[test]
    fn worker_rejects_unbounded_timing_values() {
        assert!(matches!(
            validate_timing(Duration::ZERO, Duration::from_secs(1)),
            Err(WorkerError::InvalidPollInterval)
        ));
        assert!(matches!(
            validate_timing(Duration::from_secs(1), Duration::ZERO),
            Err(WorkerError::InvalidLeaseDuration)
        ));
    }

    #[test]
    fn media_processing_components_use_configured_timeout() {
        let timeout = Duration::from_secs(301);
        let (normalization, fingerprint) =
            media_processing_components("ffmpeg", "ffprobe", timeout);

        assert_eq!(normalization.timeout_duration(), timeout);
        assert_eq!(fingerprint.timeout_duration(), timeout);
    }

    #[test]
    fn sha256_decoder_accepts_hex_and_rejects_malformed_utf8_without_panicking() {
        let digest = decode_sha256(&"ab".repeat(32)).expect("hex digest should decode");
        assert_eq!(digest, vec![0xab; 32]);

        let malformed = "é".repeat(32);
        let error = decode_sha256(&malformed).expect_err("non-ASCII digest should be rejected");
        assert_eq!(error.class, "invalid_normalization");

        let error = decode_sha256(&format!("{}g", "0".repeat(63)))
            .expect_err("non-hex digest should be rejected");
        assert_eq!(error.class, "invalid_normalization");
    }

    #[test]
    fn fingerprint_timeout_becomes_terminal_on_the_last_attempt() {
        let timeout_error = || {
            FrameExtractionError::Command(CommandError::TimedOut {
                program: PathBuf::from("ffmpeg"),
                timeout: Duration::from_secs(1),
            })
        };
        let retry = map_fingerprint_error(&running_compute_job(1, 5), timeout_error());
        assert!(retry.retryable);
        assert_eq!(retry.class, "fingerprint_timeout");

        let exhausted = map_fingerprint_error(&running_compute_job(5, 5), timeout_error());
        assert!(!exhausted.retryable);
        assert_eq!(exhausted.class, "fingerprint_failed");
    }

    #[test]
    fn normalized_storage_limit_failure_is_terminal_and_descriptive() {
        let failure = normalized_storage_limit_failure(101, 100)
            .expect("an oversized canonical artifact should fail");
        assert!(!failure.retryable);
        assert_eq!(failure.class, "normalized_storage_too_large");
        assert!(failure.message.contains("101 bytes"));
        assert!(failure.message.contains("100 bytes"));
        assert!(normalized_storage_limit_failure(100, 100).is_none());
    }

    #[test]
    fn source_provenance_keeps_page_context_and_selected_2ch_mirror() {
        let mut input =
            IngestSubmissionInput::new("https://2ch.life/b/src/clip.webm", SubmittedVia::Companion);
        input.page_url = Some("https://2ch.life/b/res/123".to_owned());
        let submission = IngestSubmission::try_new(input).expect("submission should validate");
        let mut request = Ingest::from_submission(Uuid::new_v4(), &submission);
        request.original_input["inspection"] = serde_json::json!({
            "metadata": {
                "two_ch_mirror": {
                    "submitted_host": "2ch.life",
                    "selected_host": "2ch.org",
                    "selected_url": "https://2ch.org/b/src/clip.webm"
                }
            }
        });

        let metadata = source_provenance_for_request(&request);
        assert_eq!(metadata["page_url"], "https://2ch.life/b/res/123");
        assert_eq!(metadata["two_ch_mirror"]["submitted_host"], "2ch.life");
        assert_eq!(metadata["two_ch_mirror"]["selected_host"], "2ch.org");
    }
}
