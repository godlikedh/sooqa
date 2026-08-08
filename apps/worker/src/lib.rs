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

use sooqa_inbox::{IngestKind, IngestStatus, SourceDownload};
use sooqa_jobs::{Job, JobCommand, JobStatus, JobType};
use sooqa_library::StorageUploadStore;
use sooqa_media::{
    ArtifactPublicationError, DownloadError, DownloadLimits, DownloadedSource, FfprobeAdapter,
    MediaWorkspace, SourceDownloader, SourceInput, WorkspaceArea, WorkspaceError, publish_artifact,
};
use sooqa_persistence::{
    AssetProbeStart, InboxRepository, InboxRepositoryError, JobRepository, JobRepositoryError,
    SourceDownloadStart, SourceInspectionStart,
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

    let request = match inbox.begin_asset_probe(ingest_request_id).await {
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
            return fail_probe(inbox, ingest_request_id, map_workspace_error(error)).await;
        }
    };
    if let Err(error) = workspace.validate() {
        return fail_probe(inbox, ingest_request_id, map_workspace_error(error)).await;
    }
    let input_path = match workspace.path(WorkspaceArea::Source, input_name) {
        Ok(path) => path,
        Err(error) => {
            return fail_probe(inbox, ingest_request_id, map_workspace_error(error)).await;
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
    failure: HandlerFailure,
) -> Result<(), HandlerFailure> {
    let status = if failure.retryable {
        IngestStatus::FailedRetryable
    } else {
        IngestStatus::FailedTerminal
    };
    inbox
        .fail_asset_probe(ingest_request_id, status, &failure.class, &failure.message)
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
            SourceDownload { bytes: downloaded.bytes, mime_type: downloaded.mime_type },
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
}
