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

use sooqa_inbox::{
    AssetNormalization, AssetThumbnailNormalization, IngestFinalization, IngestKind, IngestStatus,
    SourceDownload, SourceMediaKind,
};
use sooqa_jobs::{Job, JobCommand, JobStatus, JobType};
use sooqa_library::{
    AssetRole, ContentKind, ExactDuplicateRequest, MediaKind, NewContentItem, NewMediaAssetDraft,
    NewSourceRecordDraft, SourceType, StorageState, StorageUploadStore,
};
use sooqa_media::{
    ArtifactPublicationError, DownloadError, DownloadLimits, DownloadedSource, FfmpegExecutor,
    FfprobeAdapter, ImageNormalizer, MediaProbe, MediaStreamKind, MediaWorkspace,
    NormalizationExecutionError, NormalizationPlanner, SourceDownloader, SourceInput,
    WorkspaceArea, WorkspaceError, publish_artifact,
};
use sooqa_persistence::{
    AssetNormalizationStart, AssetProbeStart, InboxRepository, InboxRepositoryError,
    IngestFinalizationStart, JobRepository, JobRepositoryError, LibraryRepository,
    LibraryRepositoryError, SourceDownloadStart, SourceInspectionStart,
};
use sooqa_telegram::StorageUploadError;
use sooqa_telegram::{StorageUploadInput, StorageUploadProvider, TelegramStorageApi};
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

pub fn upload_storage_asset_handler<A, S>(provider: StorageUploadProvider<A, S>) -> HandlerFn
where
    A: TelegramStorageApi,
    S: StorageUploadStore,
{
    Arc::new(move |job| {
        let provider = provider.clone();
        Box::pin(async move { upload_storage_asset(&provider, job).await })
    })
}

pub fn probe_asset_handler(
    inbox: InboxRepository,
    work_root: impl Into<std::path::PathBuf>,
    ffprobe: FfprobeAdapter,
) -> HandlerFn {
    let work_root = work_root.into();
    Arc::new(move |job| {
        let inbox = inbox.clone();
        let work_root = work_root.clone();
        let ffprobe = ffprobe.clone();
        Box::pin(async move { probe_asset(&inbox, &work_root, &ffprobe, job).await })
    })
}

pub fn normalize_asset_handler(
    inbox: InboxRepository,
    work_root: impl Into<std::path::PathBuf>,
    planner: NormalizationPlanner,
    executor: FfmpegExecutor,
    image_normalizer: ImageNormalizer,
) -> HandlerFn {
    let work_root = work_root.into();
    Arc::new(move |job| {
        let inbox = inbox.clone();
        let work_root = work_root.clone();
        let planner = planner.clone();
        let executor = executor.clone();
        Box::pin(async move {
            normalize_asset(&inbox, &work_root, &planner, &executor, image_normalizer, job).await
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

async fn probe_asset(
    inbox: &InboxRepository,
    work_root: &std::path::Path,
    ffprobe: &FfprobeAdapter,
    job: Job,
) -> Result<(), HandlerFailure> {
    let ingest_request_id = match &job.command {
        JobCommand::ProbeAsset(payload) => payload.ingest_request_id,
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
        (request.id, "source.bin")
    } else {
        let workspace_id = match request.original_input["telegram_workspace_id"]
            .as_str()
            .and_then(|value| value.parse().ok())
        {
            Some(workspace_id) => workspace_id,
            None => {
                return fail_probe(
                    inbox,
                    ingest_request_id,
                    &job_attempt,
                    HandlerFailure::permanent(
                        "invalid_ingest_state",
                        "Telegram ingest request has no valid workspace ID",
                    ),
                )
                .await;
            }
        };
        (workspace_id, "telegram-input.bin")
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
    job: Job,
) -> Result<(), HandlerFailure> {
    let ingest_request_id = match &job.command {
        JobCommand::NormalizeAsset(payload) => payload.ingest_request_id,
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
    let media_kind = match request_media_kind(&request) {
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
    request: &sooqa_inbox::IngestRequest,
    ingest_request_id: Uuid,
    job_attempt: &sooqa_jobs::JobAttempt,
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

fn normalization_error_is_retryable(error: &NormalizationExecutionError) -> bool {
    match error {
        NormalizationExecutionError::Command(error) => error.is_timeout(),
        NormalizationExecutionError::Probe(error) => error.is_retryable(),
        _ => false,
    }
}

fn request_media_kind(request: &sooqa_inbox::IngestRequest) -> Option<SourceMediaKind> {
    let value = if request.kind == IngestKind::Url {
        request.original_input.get("download")?.get("media_kind")?
    } else {
        request.original_input.get("media_kind")?
    };
    serde_json::from_value(value.clone()).ok()
}

fn workspace_input(
    request: &sooqa_inbox::IngestRequest,
) -> Result<(Uuid, &'static str), HandlerFailure> {
    if request.kind == IngestKind::Url {
        return Ok((request.id, "source.bin"));
    }
    request.original_input["telegram_workspace_id"]
        .as_str()
        .and_then(|value| value.parse().ok())
        .map(|workspace_id| (workspace_id, "telegram-input.bin"))
        .ok_or_else(|| {
            HandlerFailure::permanent(
                "invalid_ingest_state",
                "Telegram ingest request has no valid workspace ID",
            )
        })
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
        JobCommand::FinalizeIngest(payload) => payload.ingest_request_id,
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
    let asset_draft = match normalization_to_library_asset(&normalization) {
        Ok(asset) => asset,
        Err(failure) => {
            return fail_finalization(inbox, ingest_request_id, &job_attempt, failure).await;
        }
    };
    let source = source_record_for_request(&request);
    let content_kind = match content_kind_for_normalization(&normalization) {
        Ok(content_kind) => content_kind,
        Err(failure) => {
            return fail_finalization(inbox, ingest_request_id, &job_attempt, failure).await;
        }
    };
    let resolution = match library
        .resolve_exact_duplicate(ExactDuplicateRequest {
            content_item: NewContentItem {
                kind: content_kind,
                preferred_title: request
                    .page_title
                    .clone()
                    .or_else(|| request.supplied_caption.clone()),
                editorial_description: None,
                notes: None,
            },
            asset: asset_draft.clone(),
            source,
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
    let canonical_asset = match library
        .record_canonical_asset(
            resolution.content_item.id,
            asset_draft.for_content_item(resolution.content_item.id),
        )
        .await
    {
        Ok(asset) => asset,
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
    if let Some(thumbnail) = normalization.thumbnail.as_ref() {
        let thumbnail_asset =
            match thumbnail_to_library_asset(&normalization, thumbnail, resolution.content_item.id)
            {
                Ok(asset) => asset,
                Err(failure) => {
                    return fail_finalization(inbox, ingest_request_id, &job_attempt, failure)
                        .await;
                }
            };
        if let Err(error) =
            library.record_thumbnail_asset(resolution.content_item.id, thumbnail_asset).await
        {
            return fail_finalization(
                inbox,
                ingest_request_id,
                &job_attempt,
                map_library_error(error),
            )
            .await;
        }
    }
    inbox
        .complete_ingest_finalization(
            ingest_request_id,
            &job_attempt,
            IngestFinalization {
                content_item_id: resolution.content_item.id,
                canonical_asset_id: canonical_asset.id,
            },
        )
        .await
        .map_err(map_inbox_error)?;
    Ok(())
}

fn normalization_to_library_asset(
    normalization: &AssetNormalization,
) -> Result<NewMediaAssetDraft, HandlerFailure> {
    Ok(NewMediaAssetDraft {
        role: AssetRole::Canonical,
        media_kind: media_kind_for_normalization(normalization.media_kind)?,
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
        storage_state: StorageState::Local,
    })
}

fn content_kind_for_normalization(
    normalization: &AssetNormalization,
) -> Result<ContentKind, HandlerFailure> {
    match normalization.media_kind {
        SourceMediaKind::Video => Ok(ContentKind::Video),
        SourceMediaKind::Image => Ok(ContentKind::Image),
        SourceMediaKind::Audio => Ok(ContentKind::Audio),
        SourceMediaKind::Animation => Ok(ContentKind::Animation),
        SourceMediaKind::Unknown => Err(HandlerFailure::permanent(
            "invalid_normalization",
            "normalized media kind is unknown",
        )),
    }
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

fn thumbnail_to_library_asset(
    normalization: &AssetNormalization,
    thumbnail: &AssetThumbnailNormalization,
    content_item_id: Uuid,
) -> Result<sooqa_library::NewMediaAsset, HandlerFailure> {
    if normalization.media_kind != SourceMediaKind::Image {
        return Err(HandlerFailure::permanent(
            "invalid_normalization",
            "only image normalization may contain a thumbnail",
        ));
    }
    Ok(sooqa_library::NewMediaAsset {
        content_item_id,
        role: AssetRole::Thumbnail,
        media_kind: MediaKind::Image,
        mime_type: thumbnail.mime_type.clone(),
        container: None,
        video_codec: None,
        audio_codec: None,
        width: to_database_dimension(thumbnail.width, "thumbnail width")?,
        height: to_database_dimension(thumbnail.height, "thumbnail height")?,
        duration_ms: None,
        bit_rate: None,
        file_size_bytes: Some(thumbnail.file_size_bytes),
        sha256: Some(decode_sha256(&thumbnail.sha256)?),
        local_work_path: Some(thumbnail.local_work_path.clone()),
        storage_state: StorageState::Local,
    })
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

fn source_record_for_request(request: &sooqa_inbox::IngestRequest) -> NewSourceRecordDraft {
    let (source_type, normalized_url, platform, platform_content_id) = match request.kind {
        IngestKind::Url => (SourceType::DirectUrl, Some(request.source_url.clone()), None, None),
        IngestKind::TelegramMessage => (
            SourceType::Telegram,
            None,
            Some("telegram".to_owned()),
            Some(request.source_url.clone()),
        ),
        IngestKind::Upload => (
            SourceType::Upload,
            None,
            Some("sooqa_ingest".to_owned()),
            Some(request.id.to_string()),
        ),
    };
    NewSourceRecordDraft {
        ingest_request_id: Some(request.id),
        source_type,
        original_url: Some(request.source_url.clone()),
        normalized_url,
        platform,
        platform_content_id,
        author_name: None,
        source_title: request.page_title.clone(),
        source_description: request.supplied_caption.clone(),
        source_published_at: None,
        metadata_json: source_provenance_for_request(request),
    }
}

#[derive(Debug, serde::Serialize)]
struct SourceProvenance {
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
}

fn source_provenance_for_request(request: &sooqa_inbox::IngestRequest) -> serde_json::Value {
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
    let provenance = SourceProvenance {
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
    provider: &StorageUploadProvider<A, S>,
    job: Job,
) -> Result<(), HandlerFailure>
where
    A: TelegramStorageApi,
    S: StorageUploadStore,
{
    let (asset_id, generation) = match &job.command {
        JobCommand::UploadStorageAsset(payload) => (payload.asset_id, payload.generation),
        _ => {
            return Err(HandlerFailure::permanent(
                "invalid_payload",
                "upload_storage_asset handler received a different job command",
            ));
        }
    };

    provider
        .upload(StorageUploadInput { asset_id, job_id: job.id, generation })
        .await
        .map(|_| ())
        .map_err(|error| {
            let message = error.to_string();
            if let StorageUploadError::InProgress { retry_at: Some(retry_at) } = &error {
                return HandlerFailure::defer("storage_upload_in_progress", message, *retry_at);
            }
            if matches!(error, StorageUploadError::InProgress { retry_at: None }) {
                return HandlerFailure::permanent("storage_upload_unknown", message);
            }
            if error.is_retryable() && job.attempt_count < job.max_attempts {
                HandlerFailure::retryable("storage_upload", message)
            } else {
                HandlerFailure::permanent("storage_upload", message)
            }
        })
}

async fn inspect_source(
    inbox: &InboxRepository,
    downloader: &dyn SourceDownloader,
    job: Job,
) -> Result<(), HandlerFailure> {
    let ingest_request_id = match &job.command {
        JobCommand::InspectSource(payload) => payload.ingest_request_id,
        _ => {
            return Err(HandlerFailure::permanent(
                "invalid_payload",
                "inspect_source handler received a different job command",
            ));
        }
    };

    let request = match inbox.begin_source_inspection(ingest_request_id).await {
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
                .fail_source_inspection(ingest_request_id, status, &class, &message)
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
        .complete_source_inspection(ingest_request_id, inspection)
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
        JobCommand::DownloadSource(payload) => {
            (payload.ingest_request_id, payload.inspection.clone())
        }
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
        InboxRepositoryError::ResourceMissing(_)
        | InboxRepositoryError::MissingSourceUrl(_)
        | InboxRepositoryError::InvalidFailureStatus(_)
        | InboxRepositoryError::InvalidStateTransition(_)
        | InboxRepositoryError::UnknownIngestKind(_)
        | InboxRepositoryError::UnknownIngestStatus(_)
        | InboxRepositoryError::UnknownSubmittedVia(_) => {
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

            let Some(handler) = self.registry.handler(job.job_type()) else {
                self.repository
                    .fail(
                        job.id,
                        &self.worker_id,
                        "handler_not_registered",
                        "no handler is registered for this job type",
                    )
                    .await?;
                self.metrics.failed.fetch_add(1, Ordering::Relaxed);
                warn!(worker_id = %self.worker_id, job_id = %job.id, job_type = %job.job_type(), "job failed because no handler is registered");
                continue;
            };

            let outcome = self.execute_handler(&job, handler, &mut shutdown).await?;
            let stop_after_shutdown = match outcome {
                HandlerRunOutcome::Completed(result) => {
                    self.finish_job(&job, result).await?;
                    false
                }
                HandlerRunOutcome::Shutdown => {
                    self.repository
                        .retry(
                            job.id,
                            &self.worker_id,
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
        handler: HandlerFn,
        shutdown: &mut std::pin::Pin<&mut F>,
    ) -> Result<HandlerRunOutcome, WorkerError>
    where
        F: Future<Output = ()> + Send,
    {
        let (stop_heartbeat, heartbeat_signal) = oneshot::channel();
        let heartbeat_repository = self.repository.clone();
        let heartbeat_worker_id = self.worker_id.clone();
        let heartbeat_job_id = job.id;
        let heartbeat_lease_duration = self.lease_duration;
        let mut heartbeat_task = tokio::spawn(async move {
            heartbeat_loop(
                heartbeat_repository,
                heartbeat_job_id,
                heartbeat_worker_id,
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
        result: Result<(), HandlerFailure>,
    ) -> Result<(), WorkerError> {
        match result {
            Ok(()) => {
                self.repository.complete(job.id, &self.worker_id).await?;
                self.metrics.succeeded.fetch_add(1, Ordering::Relaxed);
                info!(worker_id = %self.worker_id, job_id = %job.id, "job completed");
            }
            Err(failure) if failure.defer_until.is_some() => {
                self.repository
                    .defer(
                        job.id,
                        &self.worker_id,
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
                    .retry(
                        job.id,
                        &self.worker_id,
                        OffsetDateTime::now_utc() + TimeDuration::seconds(1),
                        &failure.class,
                        &failure.message,
                    )
                    .await?;
                if updated.status == JobStatus::RetryWait {
                    self.metrics.retried.fetch_add(1, Ordering::Relaxed);
                    info!(worker_id = %self.worker_id, job_id = %job.id, "job scheduled for retry");
                } else {
                    self.metrics.failed.fetch_add(1, Ordering::Relaxed);
                    warn!(worker_id = %self.worker_id, job_id = %job.id, "job exhausted its retry attempts");
                }
            }
            Err(failure) => {
                self.repository
                    .fail(job.id, &self.worker_id, &failure.class, &failure.message)
                    .await?;
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
    job_id: uuid::Uuid,
    worker_id: String,
    lease_duration: Duration,
    mut stop: oneshot::Receiver<()>,
) -> Result<(), JobRepositoryError> {
    let interval = heartbeat_interval(lease_duration);
    loop {
        tokio::select! {
            _ = &mut stop => return Ok(()),
            _ = sleep(interval) => {
                repository.heartbeat(job_id, &worker_id, lease_duration).await?;
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
    use super::*;

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
}
