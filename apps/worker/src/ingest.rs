//! Source ingest, inspection, download, and probe jobs.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use sooqa_inbox::{IngestKind, IngestStatus, SourceDownload};
use sooqa_jobs::{Job, JobCommand};
use sooqa_media::{
    ArtifactPublicationError, DownloadError, DownloadLimits, DownloadedSource, FfprobeAdapter,
    MediaWorkspace, SourceDownloader, SourceInput, WorkspaceArea, publish_artifact,
};
use sooqa_persistence::{
    AssetProbeStart, InboxRepository, SourceDownloadStart, SourceInspectionStart,
};
use sooqa_telegram::TelegramApi;
use uuid::Uuid;

use crate::common::{
    HandlerFailure, HandlerFn, WorkspaceAdmission, load_ingest_for_admission, map_inbox_error,
    map_workspace_error, source_artifact_exists,
};

const DEFAULT_TELEGRAM_SOURCE_DOWNLOAD_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;

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
    download_source_handler_with_admission(
        inbox,
        work_root,
        downloader,
        limits,
        WorkspaceAdmission::disabled(),
    )
}

pub fn download_source_handler_with_admission(
    inbox: InboxRepository,
    work_root: impl Into<PathBuf>,
    downloader: Arc<dyn SourceDownloader>,
    limits: DownloadLimits,
    admission: WorkspaceAdmission,
) -> HandlerFn {
    let work_root = work_root.into();
    Arc::new(move |job| {
        let inbox = inbox.clone();
        let work_root = work_root.clone();
        let downloader = Arc::clone(&downloader);
        Box::pin(async move {
            download_source(&inbox, &work_root, downloader.as_ref(), &limits, admission, job).await
        })
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
    probe_asset_handler_with_telegram_source_and_admission(
        inbox,
        work_root,
        ffprobe,
        telegram_source,
        WorkspaceAdmission::disabled(),
        DEFAULT_TELEGRAM_SOURCE_DOWNLOAD_MAX_BYTES,
    )
}

pub fn probe_asset_handler_with_telegram_source_and_admission(
    inbox: InboxRepository,
    work_root: impl Into<std::path::PathBuf>,
    ffprobe: FfprobeAdapter,
    telegram_source: Option<Arc<dyn TelegramSourceDownloader>>,
    admission: WorkspaceAdmission,
    telegram_source_max_bytes: u64,
) -> HandlerFn {
    let work_root = work_root.into();
    Arc::new(move |job| {
        let inbox = inbox.clone();
        let work_root = work_root.clone();
        let ffprobe = ffprobe.clone();
        let telegram_source = telegram_source.clone();
        Box::pin(async move {
            probe_asset(
                &inbox,
                &work_root,
                &ffprobe,
                telegram_source.as_deref(),
                admission,
                telegram_source_max_bytes,
                job,
            )
            .await
        })
    })
}

async fn probe_asset(
    inbox: &InboxRepository,
    work_root: &std::path::Path,
    ffprobe: &FfprobeAdapter,
    telegram_source: Option<&dyn TelegramSourceDownloader>,
    admission: WorkspaceAdmission,
    telegram_source_max_bytes: u64,
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
    let job_attempt = job.lease().ok_or_else(|| {
        HandlerFailure::permanent(
            "invalid_job_state",
            "probe_asset handler requires a running job lease",
        )
    })?;

    // Telegram reconstruction is the only probe path that can create a large
    // artifact. Inspect the durable request and workspace before the stage
    // transition so a low-space refusal leaves the ingest untouched.
    let current_request = load_ingest_for_admission(inbox, ingest_request_id).await?;
    let mut preflight_workspace = None;
    let mut preflight_failure = None;
    if current_request.kind == IngestKind::TelegramMessage
        && probe_stage_may_run(current_request.status)
    {
        match MediaWorkspace::create(work_root, current_request.workspace_id).await {
            Ok(workspace) => {
                let result = workspace
                    .validate()
                    .and_then(|()| workspace.path(WorkspaceArea::Source, "telegram-input.bin"));
                match result {
                    Ok(input_path) => match source_artifact_exists(&input_path).await {
                        Ok(true) => preflight_workspace = Some((workspace, input_path)),
                        Ok(false) => {
                            admission.admit(workspace.root(), telegram_source_max_bytes)?;
                            preflight_workspace = Some((workspace, input_path));
                        }
                        Err(failure) => preflight_failure = Some(failure),
                    },
                    Err(error) => preflight_failure = Some(map_workspace_error(error)),
                }
            }
            Err(error) => preflight_failure = Some(map_workspace_error(error)),
        }
    }

    let request = match inbox.begin_asset_probe(ingest_request_id, &job_attempt).await {
        Ok(AssetProbeStart::Ready(request)) => request,
        Ok(AssetProbeStart::AlreadyAdvanced(_)) => return Ok(()),
        Err(error) => return Err(map_inbox_error(error)),
    };
    if let Some(failure) = preflight_failure {
        return fail_probe(inbox, ingest_request_id, &job_attempt, failure).await;
    }
    let (workspace_id, input_name) = if request.kind == IngestKind::Url {
        (request.workspace_id, "source.bin")
    } else {
        (request.workspace_id, "telegram-input.bin")
    };
    let (workspace, input_path) = match preflight_workspace {
        Some(value) => value,
        None => {
            let workspace = match MediaWorkspace::create(work_root, workspace_id).await {
                Ok(workspace) => workspace,
                Err(error) => {
                    return fail_probe(
                        inbox,
                        ingest_request_id,
                        &job_attempt,
                        map_workspace_error(error),
                    )
                    .await;
                }
            };
            if let Err(error) = workspace.validate() {
                return fail_probe(
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
                    return fail_probe(
                        inbox,
                        ingest_request_id,
                        &job_attempt,
                        map_workspace_error(error),
                    )
                    .await;
                }
            };
            (workspace, input_path)
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

async fn ensure_telegram_source(
    workspace: &MediaWorkspace,
    request: &sooqa_inbox::Ingest,
    telegram_source: Option<&dyn TelegramSourceDownloader>,
) -> Result<(), HandlerFailure> {
    let input_data = request
        .input_data()
        .map_err(|error| HandlerFailure::permanent("invalid_ingest_state", error.to_string()))?;
    let file_id = input_data
        .source
        .telegram_file_id
        .as_deref()
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
    job_attempt: &sooqa_jobs::JobLease,
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

    let job_attempt = job.lease().ok_or_else(|| {
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
    admission: WorkspaceAdmission,
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
    let job_attempt = job.lease().ok_or_else(|| {
        HandlerFailure::permanent(
            "invalid_job_state",
            "download_source handler requires a running job lease",
        )
    })?;

    // A duplicate or stale download job can be claimed after the ingest has
    // already advanced. Read the durable stage before admission so such a job
    // can be fenced by begin_source_download without being held forever by a
    // low-space deferral. yt-dlp may use two recovery attempts and a
    // progressive fallback; its aggregate attempt directory is bounded to
    // three source budgets, so URL admission reserves that worst case even
    // when direct HTTP was selected for this particular inspection.
    let current_request = load_ingest_for_admission(inbox, ingest_request_id).await?;
    if download_stage_may_run(&current_request) {
        let required_bytes = limits.max_bytes.saturating_mul(3);
        admission.admit(work_root, required_bytes)?;
    }

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
    let selected_format = downloaded.selected_format.clone();
    let downloaded = match publish_artifact(attempt.path(), &source_path).await {
        Ok(()) => {
            let mut published =
                match read_source_artifact(&workspace, &source_path, downloaded.mime_type.clone())
                    .await
                {
                    Ok(published) => published,
                    Err(failure) => {
                        return fail_download(inbox, ingest_request_id, &job_attempt, failure)
                            .await;
                    }
                };
            published.selected_format = selected_format.clone();
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
                Ok(mut published) => {
                    published.selected_format = selected_format;
                    published
                }
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
                selected_format: downloaded.selected_format,
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
    Ok(DownloadedSource {
        path: source_path.to_owned(),
        bytes: metadata.len(),
        mime_type,
        selected_format: None,
    })
}

async fn fail_download(
    inbox: &InboxRepository,
    ingest_request_id: uuid::Uuid,
    job_attempt: &sooqa_jobs::JobLease,
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
