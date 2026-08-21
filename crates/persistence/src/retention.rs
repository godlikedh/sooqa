//! Conservative retention for terminal technical jobs.
//!
//! The queue owns the deletion, but each durable aggregate owns the decision
//! that replaying a terminal job is harmless.  Candidate scans are ordered by
//! `(completed_at, id)` and continue past ineligible rows until the bounded
//! deletion batch is full.  This is deliberate: a permanent prefix of
//! unresolved storage/publication rows must not starve eligible work behind it.

use sooqa_jobs::{Job, JobCommand, JobStatus};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use thiserror::Error;
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

use crate::{jobs::JobRow, settlement};

/// Retention horizons are intentionally supplied by the worker rather than
/// stored in the database.  A batch is the maximum number of rows deleted by
/// one maintenance invocation; candidate inspection may continue past rows
/// that are intentionally retained so they cannot block eligible rows.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct JobRetentionPolicy {
    pub succeeded_after: Duration,
    pub cancelled_after: Duration,
    pub failed_after: Duration,
    pub batch_size: usize,
    pub scan_size: usize,
}

impl JobRetentionPolicy {
    const MAX_HORIZON: Duration = Duration::days(3650);

    pub const fn new(
        succeeded_after: Duration,
        cancelled_after: Duration,
        failed_after: Duration,
        batch_size: usize,
        scan_size: usize,
    ) -> Self {
        Self { succeeded_after, cancelled_after, failed_after, batch_size, scan_size }
    }

    pub fn validate(self) -> Result<(), JobRetentionError> {
        if self.succeeded_after <= Duration::ZERO
            || self.succeeded_after > Self::MAX_HORIZON
            || self.cancelled_after <= Duration::ZERO
            || self.cancelled_after > Self::MAX_HORIZON
            || self.failed_after <= Duration::ZERO
            || self.failed_after > Self::MAX_HORIZON
        {
            return Err(JobRetentionError::InvalidHorizon);
        }
        if self.batch_size == 0 || self.batch_size > 10_000 {
            return Err(JobRetentionError::InvalidBatchSize);
        }
        if self.scan_size < self.batch_size || self.scan_size > 100_000 {
            return Err(JobRetentionError::InvalidScanSize);
        }
        Ok(())
    }

    fn cutoff(self, status: JobStatus, now: OffsetDateTime) -> Option<OffsetDateTime> {
        let horizon = match status {
            JobStatus::Succeeded => self.succeeded_after,
            JobStatus::Cancelled => self.cancelled_after,
            JobStatus::Failed => self.failed_after,
            JobStatus::Queued | JobStatus::Running => return None,
        };
        Some(now - horizon)
    }
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct JobRetentionStats {
    pub candidates: u64,
    pub eligible: u64,
    pub pruned: u64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct JobRetentionCursor {
    pub completed_at: OffsetDateTime,
    pub id: Uuid,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct JobRetentionRun {
    pub stats: JobRetentionStats,
    /// `None` means the scan reached the end and the next run should start at
    /// the oldest terminal candidate again.
    pub next_cursor: Option<JobRetentionCursor>,
}

#[derive(Debug, Clone, Copy, FromRow)]
struct CandidateCursor {
    completed_at: OffsetDateTime,
    id: Uuid,
}

impl super::jobs::JobRepository {
    /// Prune one bounded deletion batch of terminal technical jobs.
    ///
    /// Each candidate gets its own short transaction.  The owning module
    /// locks its aggregate first, then this method locks the queue row.  That
    /// lock order matches re-enqueue paths and prevents a prune/re-enqueue
    /// deadlock while still fencing a concurrent state or generation change.
    pub async fn prune_terminal_jobs(
        &self,
        policy: JobRetentionPolicy,
    ) -> Result<JobRetentionRun, JobRetentionError> {
        self.prune_terminal_jobs_from(policy, None).await
    }

    /// Continue a bounded scan from the previous maintenance cursor.  The
    /// cursor is process-local on purpose: it is only a fairness aid, not a
    /// source of truth, and restarting a worker simply begins a new scan.
    pub async fn prune_terminal_jobs_from(
        &self,
        policy: JobRetentionPolicy,
        cursor: Option<JobRetentionCursor>,
    ) -> Result<JobRetentionRun, JobRetentionError> {
        policy.validate()?;

        let mut stats = JobRetentionStats::default();
        let mut cursor = cursor
            .map(|cursor| CandidateCursor { completed_at: cursor.completed_at, id: cursor.id });
        loop {
            let candidates = load_candidates(&self.pool, policy, cursor).await?;
            if candidates.is_empty() {
                return Ok(JobRetentionRun { stats, next_cursor: None });
            };

            for candidate in candidates {
                let candidate_cursor = CandidateCursor {
                    completed_at: candidate
                        .completed_at
                        .expect("retention query requires completed_at"),
                    id: candidate.id,
                };
                cursor = Some(candidate_cursor);
                stats.candidates += 1;
                let Some(job) = candidate.into_job().ok() else {
                    // A malformed terminal row is not safe to replay or
                    // classify.  Leave it for repair/diagnostics.
                    continue;
                };
                let Some(cutoff) = policy.cutoff(job.status, OffsetDateTime::now_utc()) else {
                    continue;
                };
                if job.completed_at.is_none_or(|completed_at| completed_at > cutoff) {
                    continue;
                }

                let mut transaction = self.pool.begin().await?;
                if !owner_allows_prune(&mut transaction, &job).await? {
                    transaction.rollback().await?;
                    continue;
                }
                stats.eligible += 1;

                let Some(locked) = lock_terminal_candidate(&mut transaction, &job, cutoff).await?
                else {
                    transaction.rollback().await?;
                    continue;
                };
                if settlement::validate_locked_command(&locked, &job.command).is_err() {
                    transaction.rollback().await?;
                    continue;
                }

                let deleted = sqlx::query(
                    "DELETE FROM queue.jobs WHERE id = $1 AND state = $2 AND completed_at <= $3",
                )
                .bind(job.id)
                .bind(job.status.as_str())
                .bind(cutoff)
                .execute(&mut *transaction)
                .await?;
                transaction.commit().await?;
                stats.pruned += u64::from(deleted.rows_affected() == 1);
                if stats.pruned >= policy.batch_size as u64 {
                    return Ok(JobRetentionRun {
                        stats,
                        next_cursor: cursor.map(|cursor| JobRetentionCursor {
                            completed_at: cursor.completed_at,
                            id: cursor.id,
                        }),
                    });
                }
            }

            if stats.candidates >= policy.scan_size as u64 {
                return Ok(JobRetentionRun {
                    stats,
                    next_cursor: cursor.map(|cursor| JobRetentionCursor {
                        completed_at: cursor.completed_at,
                        id: cursor.id,
                    }),
                });
            }
        }
    }
}

async fn load_candidates(
    pool: &PgPool,
    policy: JobRetentionPolicy,
    cursor: Option<CandidateCursor>,
) -> Result<Vec<JobRow>, sqlx::Error> {
    let now = OffsetDateTime::now_utc();
    let succeeded_cutoff = now - policy.succeeded_after;
    let cancelled_cutoff = now - policy.cancelled_after;
    let failed_cutoff = now - policy.failed_after;
    if let Some(cursor) = cursor {
        sqlx::query_as::<_, JobRow>(
            r#"
            SELECT id, kind, payload, state, priority, run_at, attempt_count,
                   max_attempts, lease_token, lease_owner, lease_expires_at,
                   last_heartbeat_at, error_class, error_message, dedupe_key,
                   created_at, updated_at, completed_at
            FROM queue.jobs
            WHERE state IN ('succeeded', 'failed', 'cancelled')
              AND completed_at IS NOT NULL
              AND (completed_at, id) > ($1, $2)
              AND ((state = 'succeeded' AND completed_at <= $3)
                   OR (state = 'cancelled' AND completed_at <= $4)
                   OR (state = 'failed' AND completed_at <= $5))
            ORDER BY completed_at ASC, id ASC
            LIMIT $6
            "#,
        )
        .bind(cursor.completed_at)
        .bind(cursor.id)
        .bind(succeeded_cutoff)
        .bind(cancelled_cutoff)
        .bind(failed_cutoff)
        .bind(policy.scan_size as i64)
        .fetch_all(pool)
        .await
    } else {
        sqlx::query_as::<_, JobRow>(
            r#"
            SELECT id, kind, payload, state, priority, run_at, attempt_count,
                   max_attempts, lease_token, lease_owner, lease_expires_at,
                   last_heartbeat_at, error_class, error_message, dedupe_key,
                   created_at, updated_at, completed_at
            FROM queue.jobs
            WHERE state IN ('succeeded', 'failed', 'cancelled')
              AND completed_at IS NOT NULL
              AND ((state = 'succeeded' AND completed_at <= $1)
                   OR (state = 'cancelled' AND completed_at <= $2)
                   OR (state = 'failed' AND completed_at <= $3))
            ORDER BY completed_at ASC, id ASC
            LIMIT $4
            "#,
        )
        .bind(succeeded_cutoff)
        .bind(cancelled_cutoff)
        .bind(failed_cutoff)
        .bind(policy.scan_size as i64)
        .fetch_all(pool)
        .await
    }
}

async fn owner_allows_prune(
    transaction: &mut Transaction<'_, Postgres>,
    job: &Job,
) -> Result<bool, sqlx::Error> {
    match &job.command {
        JobCommand::InspectSource(_)
        | JobCommand::DownloadSource(_)
        | JobCommand::ProbeAsset(_)
        | JobCommand::NormalizeAsset(_)
        | JobCommand::ComputeFingerprint(_)
        | JobCommand::FinalizeIngest(_) => {
            crate::inbox::terminal_job_retention_eligible(transaction, job).await
        }
        JobCommand::MaterializePublication(_) | JobCommand::PublishPost(_) => {
            crate::publisher::terminal_job_retention_eligible(transaction, job).await
        }
        JobCommand::UploadStorageAsset(_) | JobCommand::SyncStorageCaption(_) => {
            crate::library::terminal_job_retention_eligible(transaction, job).await
        }
        JobCommand::CleanupWorkspace(_) => {
            crate::cleanup::terminal_job_retention_eligible(transaction, job).await
        }
    }
}

async fn lock_terminal_candidate(
    transaction: &mut Transaction<'_, Postgres>,
    job: &Job,
    cutoff: OffsetDateTime,
) -> Result<Option<JobRow>, sqlx::Error> {
    let row = sqlx::query_as::<_, JobRow>(
        r#"
        SELECT id, kind, payload, state, priority, run_at, attempt_count,
               max_attempts, lease_token, lease_owner, lease_expires_at,
               last_heartbeat_at, error_class, error_message, dedupe_key,
               created_at, updated_at, completed_at
        FROM queue.jobs
        WHERE id = $1 AND state = $2 AND completed_at <= $3
        FOR UPDATE SKIP LOCKED
        "#,
    )
    .bind(job.id)
    .bind(job.status.as_str())
    .bind(cutoff)
    .fetch_optional(&mut **transaction)
    .await?;
    Ok(row)
}

#[derive(Debug, Error)]
pub enum JobRetentionError {
    #[error("job retention batch size must be greater than zero")]
    InvalidBatchSize,
    #[error("job retention horizon must be greater than zero and no more than ten years")]
    InvalidHorizon,
    #[error("job retention scan size must be at least the deletion batch and within bounds")]
    InvalidScanSize,
    #[error("database operation failed: {0}")]
    Database(#[from] sqlx::Error),
}
