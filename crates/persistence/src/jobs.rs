use std::time::Duration;

use sooqa_jobs::{
    Job, JobCommand, JobCounts, JobLease, JobPayloadError, JobStatus, JobType, NewJob,
};
use sqlx::{FromRow, PgPool};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Clone)]
pub struct JobRepository {
    pub(crate) pool: PgPool,
}

/// A queue settlement requested by a family owner after the handler has
/// finished. The family settlement boundary may update its domain row and the
/// queue row in one transaction; the queue-only fallback uses the same low
/// level transition helpers.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum JobSettlement {
    Retry {
        run_at: OffsetDateTime,
        error_class: String,
        error_message: String,
        non_consuming: bool,
    },
    Defer {
        run_at: OffsetDateTime,
        error_class: String,
        error_message: String,
        non_consuming: bool,
    },
    Fail {
        error_class: String,
        error_message: String,
    },
}

impl JobSettlement {
    pub fn retry(
        run_at: OffsetDateTime,
        error_class: impl Into<String>,
        error_message: impl Into<String>,
    ) -> Self {
        Self::Retry {
            run_at,
            error_class: error_class.into(),
            error_message: error_message.into(),
            non_consuming: false,
        }
    }

    pub fn retry_without_consuming_attempt(
        run_at: OffsetDateTime,
        error_class: impl Into<String>,
        error_message: impl Into<String>,
    ) -> Self {
        Self::Retry {
            run_at,
            error_class: error_class.into(),
            error_message: error_message.into(),
            non_consuming: true,
        }
    }

    pub fn defer(
        run_at: OffsetDateTime,
        error_class: impl Into<String>,
        error_message: impl Into<String>,
    ) -> Self {
        Self::Defer {
            run_at,
            error_class: error_class.into(),
            error_message: error_message.into(),
            non_consuming: false,
        }
    }

    pub fn defer_without_consuming_attempt(
        run_at: OffsetDateTime,
        error_class: impl Into<String>,
        error_message: impl Into<String>,
    ) -> Self {
        Self::Defer {
            run_at,
            error_class: error_class.into(),
            error_message: error_message.into(),
            non_consuming: true,
        }
    }

    pub fn fail(error_class: impl Into<String>, error_message: impl Into<String>) -> Self {
        Self::Fail { error_class: error_class.into(), error_message: error_message.into() }
    }

    pub(crate) fn allows_expired_lease(&self) -> bool {
        matches!(
            self,
            Self::Retry { non_consuming: true, .. } | Self::Defer { non_consuming: true, .. }
        )
    }

    pub(crate) fn error_class(&self) -> Option<&str> {
        match self {
            Self::Retry { error_class, .. }
            | Self::Defer { error_class, .. }
            | Self::Fail { error_class, .. } => Some(error_class),
        }
    }
}

impl JobRepository {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn enqueue(&self, new_job: NewJob) -> Result<Job, JobRepositoryError> {
        if new_job.max_attempts_value() <= 0 {
            return Err(JobRepositoryError::InvalidMaxAttempts);
        }
        let row = sqlx::query_as::<_, JobRow>(
            r#"
            INSERT INTO queue.jobs (kind, payload, state, priority, run_at, max_attempts, dedupe_key)
            VALUES ($1, $2, 'queued', $3, COALESCE($4, now()), $5, $6)
            RETURNING id, kind, payload, state, priority, run_at, attempt_count,
                      max_attempts, lease_token, lease_owner, lease_expires_at,
                      last_heartbeat_at, error_class, error_message, dedupe_key,
                      created_at, updated_at, completed_at
            "#,
        )
        .bind(new_job.job_type().as_str())
        .bind(new_job.payload_json())
        .bind(new_job.priority())
        .bind(new_job.run_at_value())
        .bind(new_job.max_attempts_value())
        .bind(new_job.dedupe_key_value())
        .fetch_one(&self.pool)
        .await?;
        row.into_job()
    }

    pub async fn claim_next(
        &self,
        worker_id: &str,
        lease_duration: Duration,
        capabilities: &[JobType],
    ) -> Result<Option<Job>, JobRepositoryError> {
        let lease_seconds = lease_seconds(lease_duration)?;
        let capabilities = capabilities.iter().map(|kind| kind.as_str()).collect::<Vec<_>>();
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query_as::<_, JobRow>(
            r#"
            WITH candidate AS (
                SELECT id
                FROM queue.jobs
                WHERE state = 'queued'
                  AND run_at <= now()
                  AND attempt_count < max_attempts
                  AND kind = ANY($3::text[])
                ORDER BY priority DESC, run_at ASC, created_at ASC, id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE queue.jobs
            SET state = 'running',
                lease_token = gen_random_uuid(),
                lease_owner = $1,
                lease_expires_at = now() + ($2::double precision * interval '1 second'),
                last_heartbeat_at = now(),
                attempt_count = attempt_count + 1,
                updated_at = now()
            WHERE id = (SELECT id FROM candidate)
            RETURNING id, kind, payload, state, priority, run_at, attempt_count,
                      max_attempts, lease_token, lease_owner, lease_expires_at,
                      last_heartbeat_at, error_class, error_message, dedupe_key,
                      created_at, updated_at, completed_at
            "#,
        )
        .bind(worker_id)
        .bind(lease_seconds)
        .bind(&capabilities)
        .fetch_optional(&mut *transaction)
        .await?;
        transaction.commit().await?;
        row.map(JobRow::into_job).transpose()
    }

    pub async fn heartbeat_lease(
        &self,
        lease: &JobLease,
        lease_duration: Duration,
    ) -> Result<Job, JobRepositoryError> {
        let lease_seconds = lease_seconds(lease_duration)?;
        let row = sqlx::query_as::<_, JobRow>(
            r#"
            UPDATE queue.jobs
            SET lease_expires_at = now() + ($3::double precision * interval '1 second'),
                last_heartbeat_at = now(), updated_at = now()
            WHERE id = $1 AND state = 'running' AND lease_owner = $2 AND lease_token = $4
              AND lease_expires_at > now()
            RETURNING id, kind, payload, state, priority, run_at, attempt_count,
                      max_attempts, lease_token, lease_owner, lease_expires_at,
                      last_heartbeat_at, error_class, error_message, dedupe_key,
                      created_at, updated_at, completed_at
            "#,
        )
        .bind(lease.job_id)
        .bind(&lease.lease_owner)
        .bind(lease_seconds)
        .bind(lease.lease_token)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(JobRepositoryError::LeaseLost)?;
        row.into_job()
    }

    /// Complete is intentionally queue-only. Domain handlers that need a
    /// domain-plus-queue commit complete their domain transition first and
    /// make this operation idempotent through the already-succeeded check.
    pub async fn complete_lease(&self, lease: &JobLease) -> Result<Job, JobRepositoryError> {
        let row = sqlx::query_as::<_, JobRow>(
            r#"
            UPDATE queue.jobs
            SET state = 'succeeded', lease_token = NULL, lease_owner = NULL,
                lease_expires_at = NULL, last_heartbeat_at = NULL,
                completed_at = now(), updated_at = now()
            WHERE id = $1 AND state = 'running' AND lease_owner = $2 AND lease_token = $3
              AND lease_expires_at > now()
            RETURNING id, kind, payload, state, priority, run_at, attempt_count,
                      max_attempts, lease_token, lease_owner, lease_expires_at,
                      last_heartbeat_at, error_class, error_message, dedupe_key,
                      created_at, updated_at, completed_at
            "#,
        )
        .bind(lease.job_id)
        .bind(&lease.lease_owner)
        .bind(lease.lease_token)
        .fetch_optional(&self.pool)
        .await?;
        if let Some(row) = row {
            return row.into_job();
        }

        let already_succeeded = sqlx::query_as::<_, JobRow>(
            r#"
            SELECT id, kind, payload, state, priority, run_at, attempt_count,
                   max_attempts, lease_token, lease_owner, lease_expires_at,
                   last_heartbeat_at, error_class, error_message, dedupe_key,
                   created_at, updated_at, completed_at
            FROM queue.jobs
            WHERE id = $1 AND state = 'succeeded' AND attempt_count = $2
            "#,
        )
        .bind(lease.job_id)
        .bind(lease.attempt_number)
        .fetch_optional(&self.pool)
        .await?;
        already_succeeded.map(JobRow::into_job).transpose()?.ok_or(JobRepositoryError::LeaseLost)
    }

    /// Dispatch a typed settlement to the owning family module. `job.command`
    /// is the command decoded by the claim query; the dispatcher never parses
    /// raw payload fields.
    pub async fn settle_lease(
        &self,
        job: &Job,
        lease: &JobLease,
        settlement: JobSettlement,
    ) -> Result<Job, JobRepositoryError> {
        if !lease_matches_job(job, lease) {
            return Err(JobRepositoryError::LeaseLost);
        }
        crate::settlement::settle(&self.pool, Some(&job.command), lease, settlement).await
    }

    // These lease-only methods remain as a small compatibility surface for
    // repository callers and old integration fixtures. They enter the same
    // typed settlement dispatcher, which decodes a command once at its
    // boundary when the caller did not retain the claimed Job.
    pub async fn retry_lease(
        &self,
        lease: &JobLease,
        run_at: OffsetDateTime,
        error_class: &str,
        error_message: &str,
    ) -> Result<Job, JobRepositoryError> {
        self.settle_lease_without_job(
            lease,
            JobSettlement::retry(run_at, error_class, error_message),
        )
        .await
    }

    pub async fn retry_lease_without_consuming_attempt(
        &self,
        lease: &JobLease,
        run_at: OffsetDateTime,
        error_class: &str,
        error_message: &str,
    ) -> Result<Job, JobRepositoryError> {
        self.settle_lease_without_job(
            lease,
            JobSettlement::retry_without_consuming_attempt(run_at, error_class, error_message),
        )
        .await
    }

    pub async fn defer_lease(
        &self,
        lease: &JobLease,
        run_at: OffsetDateTime,
        error_class: &str,
        error_message: &str,
    ) -> Result<Job, JobRepositoryError> {
        self.settle_lease_without_job(
            lease,
            JobSettlement::defer(run_at, error_class, error_message),
        )
        .await
    }

    pub async fn defer_lease_without_consuming_attempt(
        &self,
        lease: &JobLease,
        run_at: OffsetDateTime,
        error_class: &str,
        error_message: &str,
    ) -> Result<Job, JobRepositoryError> {
        self.settle_lease_without_job(
            lease,
            JobSettlement::defer_without_consuming_attempt(run_at, error_class, error_message),
        )
        .await
    }

    pub async fn fail_lease(
        &self,
        lease: &JobLease,
        error_class: &str,
        error_message: &str,
    ) -> Result<Job, JobRepositoryError> {
        self.settle_lease_without_job(lease, JobSettlement::fail(error_class, error_message)).await
    }

    async fn settle_lease_without_job(
        &self,
        lease: &JobLease,
        settlement: JobSettlement,
    ) -> Result<Job, JobRepositoryError> {
        crate::settlement::settle(&self.pool, None, lease, settlement).await
    }

    /// Recovery remains a compatibility entry point for worker startup; the
    /// typed dispatcher routes each family to its owning repository policy.
    pub async fn recover_stale_leases(&self) -> Result<u64, JobRepositoryError> {
        crate::settlement::recover_stale(&self.pool).await
    }

    pub async fn live_job_ids(&self) -> Result<Vec<Uuid>, JobRepositoryError> {
        Ok(sqlx::query_scalar("SELECT id FROM queue.jobs WHERE state = 'running'")
            .fetch_all(&self.pool)
            .await?)
    }

    pub async fn protected_workspace_ids(&self) -> Result<Vec<Uuid>, JobRepositoryError> {
        crate::cleanup::protected_workspace_ids(&self.pool).await.map_err(Into::into)
    }

    pub async fn count_technical_jobs(&self) -> Result<JobCounts, JobRepositoryError> {
        let rows = sqlx::query_as::<_, JobCountRow>(
            r#"
            SELECT state, count(*) AS count
            FROM queue.jobs
            WHERE kind NOT IN ('publish_post', 'materialize_publication')
              AND state IN ('queued', 'running')
            GROUP BY state
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        let mut counts = JobCounts::default();
        for row in rows {
            match row.state.as_str() {
                "queued" => {
                    counts.queued =
                        u64::try_from(row.count).map_err(|_| JobRepositoryError::InvalidCount)?
                }
                "running" => {
                    counts.running =
                        u64::try_from(row.count).map_err(|_| JobRepositoryError::InvalidCount)?
                }
                _ => {}
            }
        }
        Ok(counts)
    }
}

fn lease_seconds(duration: Duration) -> Result<f64, JobRepositoryError> {
    if duration.is_zero() {
        return Err(JobRepositoryError::InvalidLeaseDuration);
    }
    Ok(duration.as_secs_f64())
}

fn lease_matches_job(job: &Job, lease: &JobLease) -> bool {
    job.status == JobStatus::Running
        && job.id == lease.job_id
        && job.attempt_count == lease.attempt_number
        && job.lease_owner.as_deref() == Some(lease.lease_owner.as_str())
        && job.lease_token == Some(lease.lease_token)
}

#[derive(Debug, Clone, FromRow)]
pub(crate) struct JobRow {
    pub(crate) id: Uuid,
    pub(crate) kind: String,
    pub(crate) payload: serde_json::Value,
    pub(crate) state: String,
    pub(crate) priority: i32,
    pub(crate) run_at: OffsetDateTime,
    pub(crate) attempt_count: i32,
    pub(crate) max_attempts: i32,
    pub(crate) lease_token: Option<Uuid>,
    pub(crate) lease_owner: Option<String>,
    pub(crate) lease_expires_at: Option<OffsetDateTime>,
    pub(crate) last_heartbeat_at: Option<OffsetDateTime>,
    pub(crate) error_class: Option<String>,
    pub(crate) error_message: Option<String>,
    pub(crate) dedupe_key: Option<String>,
    pub(crate) created_at: OffsetDateTime,
    pub(crate) updated_at: OffsetDateTime,
    pub(crate) completed_at: Option<OffsetDateTime>,
}

#[derive(Debug, FromRow)]
struct JobCountRow {
    state: String,
    count: i64,
}

impl JobRow {
    pub(crate) fn into_job(self) -> Result<Job, JobRepositoryError> {
        let kind =
            JobType::try_from(self.kind.as_str()).map_err(JobRepositoryError::UnknownJobType)?;
        let command = JobCommand::from_payload(kind, self.payload)
            .map_err(JobRepositoryError::InvalidPayload)?;
        Ok(Job {
            id: self.id,
            command,
            status: JobStatus::try_from(self.state.as_str())
                .map_err(JobRepositoryError::UnknownJobStatus)?,
            priority: self.priority,
            run_at: self.run_at,
            attempt_count: self.attempt_count,
            max_attempts: self.max_attempts,
            lease_token: self.lease_token,
            lease_owner: self.lease_owner,
            lease_expires_at: self.lease_expires_at,
            last_heartbeat_at: self.last_heartbeat_at,
            last_error_class: self.error_class,
            last_error_message: self.error_message,
            dedupe_key: self.dedupe_key,
            created_at: self.created_at,
            updated_at: self.updated_at,
            completed_at: self.completed_at,
        })
    }
}

#[derive(Debug, Error)]
pub enum JobRepositoryError {
    #[error("database count was negative")]
    InvalidCount,
    #[error("job max_attempts must be greater than zero")]
    InvalidMaxAttempts,
    #[error("job lease duration must be greater than zero")]
    InvalidLeaseDuration,
    #[error("job lease was lost")]
    LeaseLost,
    #[error("unknown job type in database: {0}")]
    UnknownJobType(String),
    #[error("unknown job state in database: {0}")]
    UnknownJobStatus(String),
    #[error("invalid job payload: {0}")]
    InvalidPayload(#[from] JobPayloadError),
    #[error("database operation failed: {0}")]
    Database(#[from] sqlx::Error),
}
