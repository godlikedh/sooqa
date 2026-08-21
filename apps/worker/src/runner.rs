//! Durable queue runner and lease lifecycle.

use std::{future::Future, time::Duration};

use thiserror::Error;
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::{
    sync::{oneshot, watch},
    task::JoinError,
    time::sleep,
};
use tracing::{debug, info, warn};

use sooqa_jobs::{Job, JobLease, JobStatus, JobType};
use sooqa_persistence::{JobRepository, JobRepositoryError, JobSettlement};

use crate::common::{HandlerCancellation, HandlerEntry, HandlerFailure, HandlerRegistry};

#[derive(Debug, Default)]
struct WorkerLogCounters {
    polls: u64,
    claimed: u64,
    succeeded: u64,
    retried: u64,
    failed: u64,
    shutdown_requeued: u64,
}

pub struct Worker {
    repository: JobRepository,
    registry: HandlerRegistry,
    worker_id: String,
    poll_interval: Duration,
    lease_duration: Duration,
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
        })
    }

    pub fn worker_id(&self) -> &str {
        &self.worker_id
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
        let mut counters = WorkerLogCounters::default();
        info!(worker_id = %self.worker_id, "worker loop started");

        let result: Result<(), WorkerError> = async {
            loop {
                counters.polls += 1;
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

                counters.claimed += 1;
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
                    counters.failed += 1;
                    warn!(worker_id = %self.worker_id, job_id = %job.id, job_type = %job.job_type(), "job failed because no handler is registered");
                    continue;
                };

                let outcome = self.execute_handler(&job, &lease, handler, &mut shutdown).await?;
                let stop_after_shutdown = match outcome {
                    HandlerRunOutcome::Completed(result) => {
                        self.finish_job(&job, &lease, result, &mut counters).await?;
                        false
                    }
                    HandlerRunOutcome::ShutdownCompleted(result) => {
                        self.finish_job(&job, &lease, result, &mut counters).await?;
                        true
                    }
                    HandlerRunOutcome::HeartbeatFailed { result, error } => {
                        self.finish_job(&job, &lease, result, &mut counters).await?;
                        return Err(error);
                    }
                    HandlerRunOutcome::Shutdown => {
                        self.repository
                            .settle_lease(
                                &job,
                                &lease,
                                JobSettlement::retry(
                                    OffsetDateTime::now_utc(),
                                    "worker_shutdown",
                                    "worker stopped while the job was active",
                                ),
                            )
                            .await?;
                        counters.shutdown_requeued += 1;
                        true
                    }
                };

                if stop_after_shutdown {
                    break;
                }
            }
            Ok(())
        }
        .await;

        info!(
            worker_id = %self.worker_id,
            polls = counters.polls,
            claimed = counters.claimed,
            succeeded = counters.succeeded,
            retried = counters.retried,
            failed = counters.failed,
            shutdown_requeued = counters.shutdown_requeued,
            "worker loop stopped"
        );
        result
    }

    async fn execute_handler<F>(
        &self,
        job: &Job,
        lease: &JobLease,
        handler: HandlerEntry,
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
        let cancellation = HandlerCancellation::new();
        let cancellable = handler.cancellable_handler.is_some();
        let handler_future = if let Some(handler) = handler.cancellable_handler {
            handler(job.clone(), cancellation.clone())
        } else {
            handler.handler.expect("handler entries always contain a handler")(job.clone())
        };
        tokio::pin!(handler_future);

        tokio::select! {
            _ = shutdown.as_mut() => {
                if cancellable {
                    cancellation.cancel();
                    let _ = stop_heartbeat.send(());
                    let result = (&mut handler_future).await;
                    await_heartbeat(&mut heartbeat_task).await?;
                    return Ok(HandlerRunOutcome::ShutdownCompleted(result));
                }
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
                let heartbeat_error = match result {
                    Ok(Ok(())) => WorkerError::HeartbeatTask(
                        "heartbeat loop stopped while the handler was active".to_owned(),
                    ),
                    Ok(Err(error)) => WorkerError::Repository(error),
                    Err(error) => map_heartbeat_join_error(error),
                };
                if cancellable {
                    cancellation.cancel();
                    let result = (&mut handler_future).await;
                    let result = match result {
                        Ok(()) => Err(HandlerFailure::permanent(
                            "heartbeat_lost",
                            "worker heartbeat stopped while the upload was active",
                        )),
                        Err(failure) => Err(failure),
                    };
                    Ok(HandlerRunOutcome::HeartbeatFailed {
                        result,
                        error: heartbeat_error,
                    })
                } else {
                    Err(heartbeat_error)
                }
            }
        }
    }

    async fn finish_job(
        &self,
        job: &Job,
        lease: &JobLease,
        result: Result<(), HandlerFailure>,
        counters: &mut WorkerLogCounters,
    ) -> Result<(), WorkerError> {
        match result {
            Ok(()) => {
                self.repository.complete_lease(lease).await?;
                counters.succeeded += 1;
                info!(worker_id = %self.worker_id, job_id = %job.id, "job completed");
            }
            Err(failure) if failure.requires_storage_reconciliation => {
                warn!(
                    worker_id = %self.worker_id,
                    job_id = %job.id,
                    "storage result remains unresolved; leaving the leased job for stale reconciliation"
                );
            }
            Err(failure) if failure.defer_until.is_some() => {
                let defer_until = failure.defer_until.expect("defer timestamp was checked");
                if failure.defer_without_consuming_attempt {
                    self.repository
                        .settle_lease(
                            job,
                            lease,
                            JobSettlement::defer_without_consuming_attempt(
                                defer_until,
                                &failure.class,
                                &failure.message,
                            ),
                        )
                        .await?;
                } else {
                    self.repository
                        .settle_lease(
                            job,
                            lease,
                            JobSettlement::defer(defer_until, &failure.class, &failure.message),
                        )
                        .await?;
                }
                info!(worker_id = %self.worker_id, job_id = %job.id, "job deferred until dependent lease expires");
            }
            Err(failure) if failure.retryable => {
                let run_at = OffsetDateTime::now_utc() + TimeDuration::seconds(1);
                let updated = if failure.retry_without_consuming_attempt {
                    self.repository
                        .settle_lease(
                            job,
                            lease,
                            JobSettlement::retry_without_consuming_attempt(
                                run_at,
                                &failure.class,
                                &failure.message,
                            ),
                        )
                        .await?
                } else {
                    self.repository
                        .settle_lease(
                            job,
                            lease,
                            JobSettlement::retry(run_at, &failure.class, &failure.message),
                        )
                        .await?
                };
                if updated.status == JobStatus::Queued {
                    counters.retried += 1;
                    info!(worker_id = %self.worker_id, job_id = %job.id, "job scheduled for retry");
                } else {
                    counters.failed += 1;
                    warn!(worker_id = %self.worker_id, job_id = %job.id, "job exhausted its retry attempts");
                }
            }
            Err(failure) => {
                self.repository
                    .settle_lease(job, lease, JobSettlement::fail(&failure.class, &failure.message))
                    .await?;
                counters.failed += 1;
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
    ShutdownCompleted(Result<(), HandlerFailure>),
    HeartbeatFailed { result: Result<(), HandlerFailure>, error: WorkerError },
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
    use sooqa_persistence::InboxRepositoryError;

    use crate::common::{HandlerFuture, map_inbox_error, media_processing_components};

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
    fn corrupt_durable_envelope_is_settled_permanently() {
        let failure = map_inbox_error(InboxRepositoryError::InputEnvelope(
            sooqa_inbox::IngestDataError::UnsupportedVersion(99),
        ));
        assert!(!failure.retryable);
        assert_eq!(failure.class, "invalid_ingest_state");
        assert!(failure.message.contains("version 99"));
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
}
