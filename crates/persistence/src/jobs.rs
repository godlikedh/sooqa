use std::time::Duration;

use sooqa_jobs::{Job, JobCommand, JobPayloadError, JobStatus, JobType, NewJob};
use sqlx::{FromRow, PgPool, postgres::PgQueryResult};
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
            INSERT INTO jobs (
                job_type, payload_json, priority, available_at, max_attempts, idempotency_key
            )
            VALUES ($1, $2, $3, COALESCE($4, now()), $5, $6)
            RETURNING
                id, job_type, payload_json, status, priority, available_at,
                attempt_count, max_attempts, lease_owner, lease_expires_at,
                last_heartbeat_at, last_error_class, last_error_message,
                idempotency_key, created_at, updated_at, completed_at
            "#,
        )
        .bind(new_job.job_type().as_str())
        .bind(new_job.payload_json())
        .bind(new_job.priority())
        .bind(new_job.available_at_value())
        .bind(new_job.max_attempts_value())
        .bind(new_job.idempotency_key_value())
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
        let capabilities =
            capabilities.iter().map(|job_type| job_type.as_str().to_owned()).collect::<Vec<_>>();
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query_as::<_, JobRow>(
            r#"
            WITH candidate AS (
                SELECT id
                FROM jobs
                WHERE status IN ('queued', 'retry_wait')
                  AND available_at <= now()
                  AND job_type = ANY($3::text[])
                ORDER BY priority DESC, available_at ASC, created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE jobs
            SET status = 'running',
                lease_owner = $1,
                lease_expires_at = now() + ($2::double precision * interval '1 second'),
                last_heartbeat_at = now(),
                attempt_count = attempt_count + 1,
                updated_at = now()
            WHERE id = (SELECT id FROM candidate)
            RETURNING
                id, job_type, payload_json, status, priority, available_at,
                attempt_count, max_attempts, lease_owner, lease_expires_at,
                last_heartbeat_at, last_error_class, last_error_message,
                idempotency_key, created_at, updated_at, completed_at
            "#,
        )
        .bind(worker_id)
        .bind(lease_seconds)
        .bind(&capabilities)
        .fetch_optional(&mut *transaction)
        .await?;

        let Some(row) = row else {
            transaction.commit().await?;
            return Ok(None);
        };

        let job = row.into_job()?;
        sqlx::query(
            "INSERT INTO job_attempts (job_id, attempt_number, status) VALUES ($1, $2, 'running')",
        )
        .bind(job.id)
        .bind(job.attempt_count)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;

        Ok(Some(job))
    }

    pub async fn heartbeat(
        &self,
        job_id: Uuid,
        worker_id: &str,
        lease_duration: Duration,
    ) -> Result<Job, JobRepositoryError> {
        let lease_seconds = lease_seconds(lease_duration)?;
        let row = sqlx::query_as::<_, JobRow>(
            r#"
            UPDATE jobs
            SET lease_expires_at = now() + ($3::double precision * interval '1 second'),
                last_heartbeat_at = now(),
                updated_at = now()
            WHERE id = $1 AND status = 'running' AND lease_owner = $2
            RETURNING
                id, job_type, payload_json, status, priority, available_at,
                attempt_count, max_attempts, lease_owner, lease_expires_at,
                last_heartbeat_at, last_error_class, last_error_message,
                idempotency_key, created_at, updated_at, completed_at
            "#,
        )
        .bind(job_id)
        .bind(worker_id)
        .bind(lease_seconds)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(JobRepositoryError::LeaseLost)?;

        row.into_job()
    }

    pub async fn complete(&self, job_id: Uuid, worker_id: &str) -> Result<Job, JobRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query_as::<_, JobRow>(
            r#"
            UPDATE jobs
            SET status = 'succeeded',
                lease_owner = NULL,
                lease_expires_at = NULL,
                last_heartbeat_at = NULL,
                completed_at = now(),
                updated_at = now()
            WHERE id = $1 AND status = 'running' AND lease_owner = $2
            RETURNING
                id, job_type, payload_json, status, priority, available_at,
                attempt_count, max_attempts, lease_owner, lease_expires_at,
                last_heartbeat_at, last_error_class, last_error_message,
                idempotency_key, created_at, updated_at, completed_at
            "#,
        )
        .bind(job_id)
        .bind(worker_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(JobRepositoryError::LeaseLost)?;
        let job = row.into_job()?;

        finish_attempt(&mut transaction, &job, "succeeded", None, None).await?;
        transaction.commit().await?;
        Ok(job)
    }

    pub async fn retry(
        &self,
        job_id: Uuid,
        worker_id: &str,
        available_at: OffsetDateTime,
        error_class: &str,
        error_message: &str,
    ) -> Result<Job, JobRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query_as::<_, JobRow>(
            r#"
            UPDATE jobs
            SET status = CASE WHEN attempt_count >= max_attempts THEN 'failed' ELSE 'retry_wait' END,
                available_at = $3,
                lease_owner = NULL,
                lease_expires_at = NULL,
                last_heartbeat_at = NULL,
                last_error_class = $4,
                last_error_message = $5,
                completed_at = CASE WHEN attempt_count >= max_attempts THEN now() ELSE NULL END,
                updated_at = now()
            WHERE id = $1 AND status = 'running' AND lease_owner = $2
            RETURNING
                id, job_type, payload_json, status, priority, available_at,
                attempt_count, max_attempts, lease_owner, lease_expires_at,
                last_heartbeat_at, last_error_class, last_error_message,
                idempotency_key, created_at, updated_at, completed_at
            "#,
        )
        .bind(job_id)
        .bind(worker_id)
        .bind(available_at)
        .bind(error_class)
        .bind(error_message)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(JobRepositoryError::LeaseLost)?;
        let job = row.into_job()?;
        let attempt_status = if job.status == JobStatus::Failed { "failed" } else { "retry_wait" };

        finish_attempt(
            &mut transaction,
            &job,
            attempt_status,
            Some(error_class),
            Some(error_message),
        )
        .await?;
        transaction.commit().await?;
        Ok(job)
    }

    /// Return a claimed job to the queue without counting the claim as an
    /// attempt. This is used when a durable sub-resource is owned by another
    /// live worker and the job should wait for that lease rather than burn its
    /// retry budget.
    pub async fn defer(
        &self,
        job_id: Uuid,
        worker_id: &str,
        available_at: OffsetDateTime,
        error_class: &str,
        error_message: &str,
    ) -> Result<Job, JobRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let attempt_number: i32 = sqlx::query_scalar(
            "SELECT attempt_count FROM jobs WHERE id = $1 AND status = 'running' AND lease_owner = $2 FOR UPDATE",
        )
        .bind(job_id)
        .bind(worker_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(JobRepositoryError::LeaseLost)?;
        if attempt_number <= 0 {
            return Err(JobRepositoryError::LeaseLost);
        }
        let row = sqlx::query_as::<_, JobRow>(
            r#"
            UPDATE jobs
            SET status = 'retry_wait',
                available_at = $3,
                attempt_count = attempt_count - 1,
                lease_owner = NULL,
                lease_expires_at = NULL,
                last_heartbeat_at = NULL,
                last_error_class = $4,
                last_error_message = $5,
                completed_at = NULL,
                updated_at = now()
            WHERE id = $1 AND status = 'running' AND lease_owner = $2
            RETURNING
                id, job_type, payload_json, status, priority, available_at,
                attempt_count, max_attempts, lease_owner, lease_expires_at,
                last_heartbeat_at, last_error_class, last_error_message,
                idempotency_key, created_at, updated_at, completed_at
            "#,
        )
        .bind(job_id)
        .bind(worker_id)
        .bind(available_at)
        .bind(error_class)
        .bind(error_message)
        .fetch_one(&mut *transaction)
        .await?;
        sqlx::query("DELETE FROM job_attempts WHERE job_id = $1 AND attempt_number = $2")
            .bind(job_id)
            .bind(attempt_number)
            .execute(&mut *transaction)
            .await?;
        let job = row.into_job()?;
        transaction.commit().await?;
        Ok(job)
    }

    pub async fn fail(
        &self,
        job_id: Uuid,
        worker_id: &str,
        error_class: &str,
        error_message: &str,
    ) -> Result<Job, JobRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query_as::<_, JobRow>(
            r#"
            UPDATE jobs
            SET status = 'failed',
                lease_owner = NULL,
                lease_expires_at = NULL,
                last_heartbeat_at = NULL,
                last_error_class = $3,
                last_error_message = $4,
                completed_at = now(),
                updated_at = now()
            WHERE id = $1 AND status = 'running' AND lease_owner = $2
            RETURNING
                id, job_type, payload_json, status, priority, available_at,
                attempt_count, max_attempts, lease_owner, lease_expires_at,
                last_heartbeat_at, last_error_class, last_error_message,
                idempotency_key, created_at, updated_at, completed_at
            "#,
        )
        .bind(job_id)
        .bind(worker_id)
        .bind(error_class)
        .bind(error_message)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(JobRepositoryError::LeaseLost)?;
        let job = row.into_job()?;

        finish_attempt(&mut transaction, &job, "failed", Some(error_class), Some(error_message))
            .await?;
        transaction.commit().await?;
        Ok(job)
    }

    pub async fn recover_stale_leases(&self) -> Result<u64, JobRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let stale_jobs = sqlx::query_as::<_, StaleJobRow>(
            r#"
            SELECT id, attempt_count, max_attempts
            FROM jobs
            WHERE status = 'running'
              AND lease_expires_at IS NOT NULL
              AND lease_expires_at <= now()
            FOR UPDATE SKIP LOCKED
            "#,
        )
        .fetch_all(&mut *transaction)
        .await?;

        for stale_job in &stale_jobs {
            let status = if stale_job.attempt_count >= stale_job.max_attempts {
                "failed"
            } else {
                "retry_wait"
            };
            sqlx::query(
                r#"
                UPDATE jobs
                SET status = $2,
                    available_at = now(),
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    last_heartbeat_at = NULL,
                    last_error_class = 'lease_expired',
                    last_error_message = 'job lease expired',
                    completed_at = CASE WHEN $2 = 'failed' THEN now() ELSE NULL END,
                    updated_at = now()
                WHERE id = $1
                "#,
            )
            .bind(stale_job.id)
            .bind(status)
            .execute(&mut *transaction)
            .await?;
            sqlx::query(
                r#"
                UPDATE job_attempts
                SET status = $2,
                    finished_at = now(),
                    error_class = 'lease_expired',
                    error_message = 'job lease expired'
                WHERE job_id = $1 AND attempt_number = $3
                "#,
            )
            .bind(stale_job.id)
            .bind(status)
            .bind(stale_job.attempt_count)
            .execute(&mut *transaction)
            .await?;
        }

        transaction.commit().await?;
        Ok(stale_jobs.len() as u64)
    }

    pub async fn live_job_ids(&self) -> Result<Vec<Uuid>, JobRepositoryError> {
        Ok(sqlx::query_scalar(
            "SELECT id FROM jobs WHERE status = 'running' AND lease_expires_at > now()",
        )
        .fetch_all(&self.pool)
        .await?)
    }
}

async fn finish_attempt(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    job: &Job,
    status: &str,
    error_class: Option<&str>,
    error_message: Option<&str>,
) -> Result<PgQueryResult, sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE job_attempts
        SET status = $2,
            finished_at = now(),
            error_class = $3,
            error_message = $4
        WHERE job_id = $1 AND attempt_number = $5
        "#,
    )
    .bind(job.id)
    .bind(status)
    .bind(error_class)
    .bind(error_message)
    .bind(job.attempt_count)
    .execute(&mut **transaction)
    .await
}

fn lease_seconds(duration: Duration) -> Result<i64, JobRepositoryError> {
    let seconds = duration.as_secs();
    if seconds == 0 || seconds > i64::MAX as u64 {
        return Err(JobRepositoryError::InvalidLeaseDuration);
    }
    Ok(seconds as i64)
}

#[derive(Debug, FromRow)]
struct JobRow {
    id: Uuid,
    job_type: String,
    payload_json: serde_json::Value,
    status: String,
    priority: i32,
    available_at: OffsetDateTime,
    attempt_count: i32,
    max_attempts: i32,
    lease_owner: Option<String>,
    lease_expires_at: Option<OffsetDateTime>,
    last_heartbeat_at: Option<OffsetDateTime>,
    last_error_class: Option<String>,
    last_error_message: Option<String>,
    idempotency_key: Option<String>,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
    completed_at: Option<OffsetDateTime>,
}

impl JobRow {
    fn into_job(self) -> Result<Job, JobRepositoryError> {
        let job_type = JobType::try_from(self.job_type.as_str())
            .map_err(JobRepositoryError::UnknownJobType)?;
        let command = JobCommand::from_payload(job_type, self.payload_json)
            .map_err(JobRepositoryError::InvalidPayload)?;

        Ok(Job {
            id: self.id,
            command,
            status: JobStatus::try_from(self.status.as_str())
                .map_err(JobRepositoryError::UnknownJobStatus)?,
            priority: self.priority,
            available_at: self.available_at,
            attempt_count: self.attempt_count,
            max_attempts: self.max_attempts,
            lease_owner: self.lease_owner,
            lease_expires_at: self.lease_expires_at,
            last_heartbeat_at: self.last_heartbeat_at,
            last_error_class: self.last_error_class,
            last_error_message: self.last_error_message,
            idempotency_key: self.idempotency_key,
            created_at: self.created_at,
            updated_at: self.updated_at,
            completed_at: self.completed_at,
        })
    }
}

#[derive(Debug, FromRow)]
struct StaleJobRow {
    id: Uuid,
    attempt_count: i32,
    max_attempts: i32,
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
    #[error("unknown job status in database: {0}")]
    UnknownJobStatus(String),
    #[error("invalid job payload: {0}")]
    InvalidPayload(#[from] JobPayloadError),
    #[error("database operation failed: {0}")]
    Database(#[from] sqlx::Error),
}
