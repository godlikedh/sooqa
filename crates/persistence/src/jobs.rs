use std::time::Duration;

use sooqa_jobs::{Job, JobCommand, JobLease, JobPayloadError, JobStatus, JobType, NewJob};
use sqlx::{FromRow, PgPool};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Clone)]
pub struct JobRepository {
    pool: PgPool,
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
                  AND kind = ANY($3::text[])
                ORDER BY priority DESC, run_at ASC, created_at ASC
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
            RETURNING id, kind, payload, state, priority, run_at, attempt_count,
                      max_attempts, lease_token, lease_owner, lease_expires_at,
                      last_heartbeat_at, error_class, error_message, dedupe_key,
                      created_at, updated_at, completed_at
            "#,
        )
        .bind(lease.job_id)
        .bind(&lease.worker_id)
        .bind(lease_seconds)
        .bind(lease.lease_token)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(JobRepositoryError::LeaseLost)?;
        row.into_job()
    }

    pub async fn complete_lease(&self, lease: &JobLease) -> Result<Job, JobRepositoryError> {
        let row = sqlx::query_as::<_, JobRow>(
            r#"
            UPDATE queue.jobs
            SET state = 'succeeded', lease_token = NULL, lease_owner = NULL,
                lease_expires_at = NULL, last_heartbeat_at = NULL,
                completed_at = now(), updated_at = now()
            WHERE id = $1 AND state = 'running' AND lease_owner = $2 AND lease_token = $3
            RETURNING id, kind, payload, state, priority, run_at, attempt_count,
                      max_attempts, lease_token, lease_owner, lease_expires_at,
                      last_heartbeat_at, error_class, error_message, dedupe_key,
                      created_at, updated_at, completed_at
            "#,
        )
        .bind(lease.job_id)
        .bind(&lease.worker_id)
        .bind(lease.lease_token)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(JobRepositoryError::LeaseLost)?;
        row.into_job()
    }

    pub async fn retry_lease(
        &self,
        lease: &JobLease,
        run_at: OffsetDateTime,
        error_class: &str,
        error_message: &str,
    ) -> Result<Job, JobRepositoryError> {
        let row = sqlx::query_as::<_, JobRow>(
            r#"
            UPDATE queue.jobs
            SET state = CASE WHEN attempt_count >= max_attempts THEN 'failed' ELSE 'queued' END,
                run_at = $4, lease_token = NULL, lease_owner = NULL,
                lease_expires_at = NULL, last_heartbeat_at = NULL,
                error_class = $5, error_message = $6,
                completed_at = CASE WHEN attempt_count >= max_attempts THEN now() ELSE NULL END,
                updated_at = now()
            WHERE id = $1 AND state = 'running' AND lease_owner = $2 AND lease_token = $3
            RETURNING id, kind, payload, state, priority, run_at, attempt_count,
                      max_attempts, lease_token, lease_owner, lease_expires_at,
                      last_heartbeat_at, error_class, error_message, dedupe_key,
                      created_at, updated_at, completed_at
            "#,
        )
        .bind(lease.job_id)
        .bind(&lease.worker_id)
        .bind(lease.lease_token)
        .bind(run_at)
        .bind(error_class)
        .bind(error_message)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(JobRepositoryError::LeaseLost)?;
        row.into_job()
    }

    pub async fn defer_lease(
        &self,
        lease: &JobLease,
        run_at: OffsetDateTime,
        error_class: &str,
        error_message: &str,
    ) -> Result<Job, JobRepositoryError> {
        let row = sqlx::query_as::<_, JobRow>(
            r#"
            UPDATE queue.jobs
            SET state = 'queued', run_at = $4, lease_token = NULL, lease_owner = NULL,
                lease_expires_at = NULL, last_heartbeat_at = NULL,
                error_class = $5, error_message = $6, updated_at = now()
            WHERE id = $1 AND state = 'running' AND lease_owner = $2 AND lease_token = $3
            RETURNING id, kind, payload, state, priority, run_at, attempt_count,
                      max_attempts, lease_token, lease_owner, lease_expires_at,
                      last_heartbeat_at, error_class, error_message, dedupe_key,
                      created_at, updated_at, completed_at
            "#,
        )
        .bind(lease.job_id)
        .bind(&lease.worker_id)
        .bind(lease.lease_token)
        .bind(run_at)
        .bind(error_class)
        .bind(error_message)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(JobRepositoryError::LeaseLost)?;
        row.into_job()
    }

    pub async fn fail_lease(
        &self,
        lease: &JobLease,
        error_class: &str,
        error_message: &str,
    ) -> Result<Job, JobRepositoryError> {
        let row = sqlx::query_as::<_, JobRow>(
            r#"
            UPDATE queue.jobs
            SET state = 'failed', lease_token = NULL, lease_owner = NULL,
                lease_expires_at = NULL, last_heartbeat_at = NULL,
                error_class = $4, error_message = $5, completed_at = now(), updated_at = now()
            WHERE id = $1 AND state = 'running' AND lease_owner = $2 AND lease_token = $3
            RETURNING id, kind, payload, state, priority, run_at, attempt_count,
                      max_attempts, lease_token, lease_owner, lease_expires_at,
                      last_heartbeat_at, error_class, error_message, dedupe_key,
                      created_at, updated_at, completed_at
            "#,
        )
        .bind(lease.job_id)
        .bind(&lease.worker_id)
        .bind(lease.lease_token)
        .bind(error_class)
        .bind(error_message)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(JobRepositoryError::LeaseLost)?;
        row.into_job()
    }

    pub async fn recover_stale_leases(&self) -> Result<u64, JobRepositoryError> {
        let result = sqlx::query(
            r#"
            UPDATE queue.jobs
            SET state = CASE WHEN attempt_count >= max_attempts THEN 'failed' ELSE 'queued' END,
                run_at = now(), lease_token = NULL, lease_owner = NULL,
                lease_expires_at = NULL, last_heartbeat_at = NULL,
                error_class = COALESCE(error_class, 'lease_expired'),
                error_message = COALESCE(error_message, 'job lease expired'),
                completed_at = CASE WHEN attempt_count >= max_attempts THEN now() ELSE NULL END,
                updated_at = now()
            WHERE state = 'running' AND lease_expires_at < now()
            "#,
        )
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    pub async fn live_job_ids(&self) -> Result<Vec<Uuid>, JobRepositoryError> {
        Ok(sqlx::query_scalar("SELECT id FROM queue.jobs WHERE state = 'running'")
            .fetch_all(&self.pool)
            .await?)
    }

    // These worker-era wrappers are deliberately owner-checked. New code uses
    // the lease-token methods above; keeping the narrow wrappers makes the
    // migration of handlers mechanical without weakening the database fence.
    pub async fn heartbeat(
        &self,
        job_id: Uuid,
        worker_id: &str,
        duration: Duration,
    ) -> Result<Job, JobRepositoryError> {
        let lease = self.current_lease(job_id, worker_id).await?;
        self.heartbeat_lease(&lease, duration).await
    }

    pub async fn complete(&self, job_id: Uuid, worker_id: &str) -> Result<Job, JobRepositoryError> {
        let lease = self.current_lease(job_id, worker_id).await?;
        self.complete_lease(&lease).await
    }

    pub async fn retry(
        &self,
        job_id: Uuid,
        worker_id: &str,
        run_at: OffsetDateTime,
        class: &str,
        message: &str,
    ) -> Result<Job, JobRepositoryError> {
        let lease = self.current_lease(job_id, worker_id).await?;
        self.retry_lease(&lease, run_at, class, message).await
    }

    pub async fn defer(
        &self,
        job_id: Uuid,
        worker_id: &str,
        run_at: OffsetDateTime,
        class: &str,
        message: &str,
    ) -> Result<Job, JobRepositoryError> {
        let lease = self.current_lease(job_id, worker_id).await?;
        self.defer_lease(&lease, run_at, class, message).await
    }

    pub async fn fail(
        &self,
        job_id: Uuid,
        worker_id: &str,
        class: &str,
        message: &str,
    ) -> Result<Job, JobRepositoryError> {
        let lease = self.current_lease(job_id, worker_id).await?;
        self.fail_lease(&lease, class, message).await
    }

    async fn current_lease(
        &self,
        job_id: Uuid,
        worker_id: &str,
    ) -> Result<JobLease, JobRepositoryError> {
        let row = sqlx::query_as::<_, LeaseRow>(
            "SELECT id, attempt_count, lease_owner, lease_token FROM queue.jobs WHERE id = $1 AND state = 'running' AND lease_owner = $2",
        )
        .bind(job_id)
        .bind(worker_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(JobRepositoryError::LeaseLost)?;
        Ok(JobLease {
            job_id: row.id,
            attempt_number: row.attempt_count,
            worker_id: row.lease_owner.clone(),
            lease_owner: row.lease_owner,
            lease_token: row.lease_token.ok_or(JobRepositoryError::LeaseLost)?,
        })
    }
}

fn lease_seconds(duration: Duration) -> Result<f64, JobRepositoryError> {
    if duration.is_zero() {
        return Err(JobRepositoryError::InvalidLeaseDuration);
    }
    Ok(duration.as_secs_f64())
}

#[derive(Debug, FromRow)]
struct JobRow {
    id: Uuid,
    kind: String,
    payload: serde_json::Value,
    state: String,
    priority: i32,
    run_at: OffsetDateTime,
    attempt_count: i32,
    max_attempts: i32,
    lease_token: Option<Uuid>,
    lease_owner: Option<String>,
    lease_expires_at: Option<OffsetDateTime>,
    last_heartbeat_at: Option<OffsetDateTime>,
    error_class: Option<String>,
    error_message: Option<String>,
    dedupe_key: Option<String>,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
    completed_at: Option<OffsetDateTime>,
}

impl JobRow {
    fn into_job(self) -> Result<Job, JobRepositoryError> {
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

#[derive(Debug, FromRow)]
struct LeaseRow {
    id: Uuid,
    attempt_count: i32,
    lease_owner: String,
    lease_token: Option<Uuid>,
}

#[derive(Debug, Error)]
pub enum JobRepositoryError {
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
