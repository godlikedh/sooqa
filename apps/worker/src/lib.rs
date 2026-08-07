//! Bounded durable-job worker loop for sooqa.

use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use sooqa_inbox::IngestStatus;
use sooqa_jobs::{Job, JobCommand, JobStatus, JobType};
use sooqa_media::{SourceDownloader, SourceInput};
use sooqa_persistence::{
    InboxRepository, InboxRepositoryError, JobRepository, JobRepositoryError, SourceInspectionStart,
};
use thiserror::Error;
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::time::sleep;
use tracing::{debug, info, warn};

pub type HandlerFuture = Pin<Box<dyn Future<Output = Result<(), HandlerFailure>> + Send + 'static>>;
pub type HandlerFn = Arc<dyn Fn(Job) -> HandlerFuture + Send + Sync>;

#[derive(Debug, Clone)]
pub struct HandlerFailure {
    pub retryable: bool,
    pub class: String,
    pub message: String,
}

impl HandlerFailure {
    pub fn retryable(class: impl Into<String>, message: impl Into<String>) -> Self {
        Self { retryable: true, class: class.into(), message: message.into() }
    }

    pub fn permanent(class: impl Into<String>, message: impl Into<String>) -> Self {
        Self { retryable: false, class: class.into(), message: message.into() }
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

fn map_inbox_error(error: InboxRepositoryError) -> HandlerFailure {
    let message = error.to_string();
    match error {
        InboxRepositoryError::ResourceMissing(_)
        | InboxRepositoryError::MissingSourceUrl(_)
        | InboxRepositoryError::InvalidSourceInspectionFailureStatus(_)
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
        tokio::pin!(shutdown);
        info!(worker_id = %self.worker_id, "worker loop started");

        loop {
            self.metrics.polls.fetch_add(1, Ordering::Relaxed);
            debug!(worker_id = %self.worker_id, "polling for a job");
            let claimed = tokio::select! {
                _ = &mut shutdown => break,
                result = self.repository.claim_next(&self.worker_id, self.lease_duration) => result?,
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

            let stop_after_shutdown = tokio::select! {
                _ = &mut shutdown => {
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
                result = handler(job.clone()) => {
                    self.finish_job(&job, result).await?;
                    false
                }
            };

            if stop_after_shutdown {
                break;
            }
        }

        info!(worker_id = %self.worker_id, "worker loop stopped");
        Ok(())
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
