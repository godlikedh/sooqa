use crate::{WORKSPACE_CLEANUP_RETENTION, cleanup::enqueue_workspace_cleanup};
use std::time::Duration;

use sooqa_jobs::{
    Job, JobCommand, JobCounts, JobLease, JobPayloadError, JobStatus, JobType, NewJob,
};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
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
                  AND attempt_count < max_attempts
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

    pub async fn retry_lease(
        &self,
        lease: &JobLease,
        run_at: OffsetDateTime,
        error_class: &str,
        error_message: &str,
    ) -> Result<Job, JobRepositoryError> {
        if self.is_publication_lease(lease).await? {
            return self
                .settle_publication_lease(lease, run_at, error_class, error_message, false)
                .await;
        }
        let mut transaction = self.pool.begin().await?;
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
        .bind(run_at)
        .bind(error_class)
        .bind(error_message)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(JobRepositoryError::LeaseLost)?;
        if row.kind == JobType::SyncStorageCaption.as_str() {
            let media_id = row
                .payload
                .get("media_id")
                .and_then(serde_json::Value::as_str)
                .and_then(|value| Uuid::parse_str(value).ok());
            let generation = row.payload.get("generation").and_then(serde_json::Value::as_i64);
            if let (Some(media_id), Some(generation)) = (media_id, generation) {
                if row.state == "queued" {
                    sqlx::query(
                        "UPDATE media SET caption_sync_state = 'pending', caption_sync_error = NULL, caption_sync_claim_token = NULL, updated_at = now() WHERE id = $1 AND caption_sync_generation = $2 AND caption_sync_state = 'syncing'",
                    )
                    .bind(media_id)
                    .bind(generation)
                    .execute(&mut *transaction)
                    .await?;
                } else if row.state == "failed" {
                    sqlx::query(
                        "UPDATE media SET caption_sync_state = 'failed', caption_sync_error = left($3, 512), caption_sync_claim_token = NULL, updated_at = now() WHERE id = $1 AND caption_sync_generation = $2 AND caption_sync_state = 'syncing'",
                    )
                    .bind(media_id)
                    .bind(generation)
                    .bind(error_message)
                    .execute(&mut *transaction)
                    .await?;
                }
            }
        }
        transaction.commit().await?;
        row.into_job()
    }

    pub async fn defer_lease(
        &self,
        lease: &JobLease,
        run_at: OffsetDateTime,
        error_class: &str,
        error_message: &str,
    ) -> Result<Job, JobRepositoryError> {
        if self.is_publication_lease(lease).await? {
            return self
                .settle_publication_lease(lease, run_at, error_class, error_message, false)
                .await;
        }
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
        if self.is_publication_lease(lease).await? {
            return self
                .settle_publication_lease(
                    lease,
                    OffsetDateTime::now_utc(),
                    error_class,
                    error_message,
                    true,
                )
                .await;
        }
        let row = sqlx::query_as::<_, JobRow>(
            r#"
            UPDATE queue.jobs
            SET state = 'failed', lease_token = NULL, lease_owner = NULL,
                lease_expires_at = NULL, last_heartbeat_at = NULL,
                error_class = $4, error_message = $5, completed_at = now(), updated_at = now()
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
        .bind(error_class)
        .bind(error_message)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(JobRepositoryError::LeaseLost)?;
        row.into_job()
    }

    pub async fn recover_stale_leases(&self) -> Result<u64, JobRepositoryError> {
        let publication_recovered = self.recover_stale_publication_leases().await?;
        let mut transaction = self.pool.begin().await?;
        let recovered = sqlx::query_as::<_, RecoveredJob>(
            r#"
            UPDATE queue.jobs
            SET state = CASE WHEN attempt_count >= max_attempts THEN 'failed' ELSE 'queued' END,
                run_at = now(), lease_token = NULL, lease_owner = NULL,
                lease_expires_at = NULL, last_heartbeat_at = NULL,
                error_class = COALESCE(error_class, 'lease_expired'),
                error_message = COALESCE(error_message, 'job lease expired'),
                completed_at = CASE WHEN attempt_count >= max_attempts THEN now() ELSE NULL END,
                updated_at = now()
            WHERE state = 'running' AND kind <> 'publish_post' AND lease_expires_at <= now()
            RETURNING kind, payload, state, attempt_count, max_attempts
            "#,
        )
        .fetch_all(&mut *transaction)
        .await?;
        for job in &recovered {
            if job.kind == JobType::SyncStorageCaption.as_str() && job.state == "queued" {
                let media_id = job
                    .payload
                    .get("media_id")
                    .and_then(serde_json::Value::as_str)
                    .and_then(|value| Uuid::parse_str(value).ok());
                let generation = job.payload.get("generation").and_then(serde_json::Value::as_i64);
                if let (Some(media_id), Some(generation)) = (media_id, generation) {
                    sqlx::query(
                        "UPDATE media SET caption_sync_state = 'pending', caption_sync_error = NULL, caption_sync_claim_token = NULL, updated_at = now() WHERE id = $1 AND caption_sync_generation = $2 AND caption_sync_state = 'syncing'",
                    )
                    .bind(media_id)
                    .bind(generation)
                    .execute(&mut *transaction)
                    .await?;
                }
            }
            if job.state == "failed" && job.attempt_count >= job.max_attempts {
                reconcile_exhausted_job(&mut transaction, job).await?;
            }
        }
        transaction.commit().await?;
        Ok(publication_recovered + recovered.len() as u64)
    }

    async fn is_publication_lease(&self, lease: &JobLease) -> Result<bool, JobRepositoryError> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT kind FROM queue.jobs WHERE id = $1 AND state = 'running' AND attempt_count = $2 AND lease_owner = $3 AND lease_token = $4 AND lease_expires_at > clock_timestamp()",
        )
        .bind(lease.job_id)
        .bind(lease.attempt_number)
        .bind(&lease.lease_owner)
        .bind(lease.lease_token)
        .fetch_optional(&self.pool)
        .await?
        .is_some_and(|kind| kind == JobType::PublishPost.as_str()))
    }

    async fn settle_publication_lease(
        &self,
        lease: &JobLease,
        run_at: OffsetDateTime,
        error_class: &str,
        error_message: &str,
        force_terminal: bool,
    ) -> Result<Job, JobRepositoryError> {
        let payload = self.publication_payload(lease).await?;
        let post_id = payload_uuid(&payload, "post_id");
        let mut transaction = self.pool.begin().await?;
        let post = match post_id {
            Some(post_id) => lock_publication_post(&mut transaction, post_id).await?,
            None => None,
        };
        let job = lock_running_job(&mut transaction, lease).await?;
        let terminal = force_terminal || job.attempt_count >= job.max_attempts;
        if terminal && let (Some(post), Some(post_id)) = (&post, post_id) {
            settle_terminal_publication_post(
                &mut transaction,
                post,
                post_id,
                &job.payload,
                error_class,
                error_message,
            )
            .await?;
        }
        let state = if terminal { "failed" } else { "queued" };
        let row = update_locked_job(
            &mut transaction,
            job.id,
            state,
            if terminal { job.run_at } else { run_at },
            error_class,
            error_message,
            terminal,
        )
        .await?;
        transaction.commit().await?;
        row.into_job()
    }

    async fn publication_payload(
        &self,
        lease: &JobLease,
    ) -> Result<serde_json::Value, JobRepositoryError> {
        sqlx::query_scalar::<_, serde_json::Value>(
            "SELECT payload FROM queue.jobs WHERE id = $1 AND kind = 'publish_post' AND state = 'running' AND attempt_count = $2 AND lease_owner = $3 AND lease_token = $4 AND lease_expires_at > clock_timestamp()",
        )
        .bind(lease.job_id)
        .bind(lease.attempt_number)
        .bind(&lease.lease_owner)
        .bind(lease.lease_token)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(JobRepositoryError::LeaseLost)
    }

    async fn recover_stale_publication_leases(&self) -> Result<u64, JobRepositoryError> {
        let candidates = sqlx::query_as::<_, StalePublicationJob>(
            "SELECT id, payload FROM queue.jobs WHERE kind = 'publish_post' AND state = 'running' AND lease_expires_at <= clock_timestamp()",
        )
        .fetch_all(&self.pool)
        .await?;
        let mut recovered = 0;
        for candidate in candidates {
            if self.recover_stale_publication_lease(candidate).await? {
                recovered += 1;
            }
        }
        Ok(recovered)
    }

    async fn recover_stale_publication_lease(
        &self,
        candidate: StalePublicationJob,
    ) -> Result<bool, JobRepositoryError> {
        let post_id = payload_uuid(&candidate.payload, "post_id");
        let mut transaction = self.pool.begin().await?;
        let post = match post_id {
            Some(post_id) => lock_publication_post(&mut transaction, post_id).await?,
            None => None,
        };
        let Some(job) = lock_expired_publication_job(&mut transaction, candidate.id).await? else {
            transaction.commit().await?;
            return Ok(false);
        };
        let terminal = job.attempt_count >= job.max_attempts;
        if terminal
            && let (Some(post), Some(post_id)) = (&post, post_id)
            && payload_uuid(&job.payload, "post_id") == Some(post_id)
        {
            let error_class = job.error_class.as_deref().unwrap_or("lease_expired");
            let error_message = job.error_message.as_deref().unwrap_or("job lease expired");
            let (post_error_class, post_error_message) = if post.state == "sending" {
                ("publication_interrupted", "publication job lease expired after the final attempt")
            } else {
                (error_class, error_message)
            };
            settle_terminal_publication_post(
                &mut transaction,
                post,
                post_id,
                &job.payload,
                post_error_class,
                post_error_message,
            )
            .await?;
        }
        update_locked_job(
            &mut transaction,
            job.id,
            if terminal { "failed" } else { "queued" },
            OffsetDateTime::now_utc(),
            job.error_class.as_deref().unwrap_or("lease_expired"),
            job.error_message.as_deref().unwrap_or("job lease expired"),
            terminal,
        )
        .await?;
        transaction.commit().await?;
        Ok(true)
    }

    pub async fn live_job_ids(&self) -> Result<Vec<Uuid>, JobRepositoryError> {
        Ok(sqlx::query_scalar("SELECT id FROM queue.jobs WHERE state = 'running'")
            .fetch_all(&self.pool)
            .await?)
    }

    pub async fn protected_workspace_ids(&self) -> Result<Vec<Uuid>, JobRepositoryError> {
        Ok(sqlx::query_scalar(
            r#"
            -- Reconciliation runs from a snapshot and deletes after this
            -- query commits. Protect every workspace that is still current
            -- for an ingest, not only active pipeline states, so a storage
            -- reset cannot reopen bytes between the snapshot and deletion.
            SELECT DISTINCT workspace_id
            FROM ingests
            "#,
        )
        .fetch_all(&self.pool)
        .await?)
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

#[derive(Debug, FromRow)]
struct JobCountRow {
    state: String,
    count: i64,
}

#[derive(Debug, FromRow)]
struct RecoveredJob {
    kind: String,
    payload: serde_json::Value,
    state: String,
    attempt_count: i32,
    max_attempts: i32,
}

#[derive(Debug, FromRow)]
struct StalePublicationJob {
    id: Uuid,
    payload: serde_json::Value,
}

#[derive(Debug, FromRow)]
struct PublicationPostRow {
    state: String,
    revision: i64,
}

async fn lock_publication_post(
    transaction: &mut Transaction<'_, Postgres>,
    post_id: Uuid,
) -> Result<Option<PublicationPostRow>, sqlx::Error> {
    let channel_id = sqlx::query_scalar::<_, Uuid>("SELECT channel_id FROM posts WHERE id = $1")
        .bind(post_id)
        .fetch_optional(&mut **transaction)
        .await?;
    let Some(channel_id) = channel_id else {
        return Ok(None);
    };
    sqlx::query("SELECT id FROM channels WHERE id = $1 FOR UPDATE")
        .bind(channel_id)
        .fetch_optional(&mut **transaction)
        .await?;
    sqlx::query_as::<_, PublicationPostRow>(
        "SELECT state, revision FROM posts WHERE id = $1 FOR UPDATE",
    )
    .bind(post_id)
    .fetch_optional(&mut **transaction)
    .await
}

async fn lock_running_job(
    transaction: &mut Transaction<'_, Postgres>,
    lease: &JobLease,
) -> Result<JobRow, JobRepositoryError> {
    sqlx::query_as::<_, JobRow>(
        r#"
        SELECT id, kind, payload, state, priority, run_at, attempt_count,
               max_attempts, lease_token, lease_owner, lease_expires_at,
               last_heartbeat_at, error_class, error_message, dedupe_key,
               created_at, updated_at, completed_at
        FROM queue.jobs
        WHERE id = $1 AND kind = 'publish_post' AND state = 'running'
          AND attempt_count = $2 AND lease_owner = $3 AND lease_token = $4
          AND lease_expires_at > clock_timestamp()
        FOR UPDATE
        "#,
    )
    .bind(lease.job_id)
    .bind(lease.attempt_number)
    .bind(&lease.lease_owner)
    .bind(lease.lease_token)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(JobRepositoryError::LeaseLost)
}

async fn lock_expired_publication_job(
    transaction: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
) -> Result<Option<JobRow>, JobRepositoryError> {
    Ok(sqlx::query_as::<_, JobRow>(
        r#"
        SELECT id, kind, payload, state, priority, run_at, attempt_count,
               max_attempts, lease_token, lease_owner, lease_expires_at,
               last_heartbeat_at, error_class, error_message, dedupe_key,
               created_at, updated_at, completed_at
        FROM queue.jobs
        WHERE id = $1 AND kind = 'publish_post' AND state = 'running'
          AND lease_expires_at <= clock_timestamp()
        FOR UPDATE
        "#,
    )
    .bind(job_id)
    .fetch_optional(&mut **transaction)
    .await?)
}

async fn update_locked_job(
    transaction: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    state: &str,
    run_at: OffsetDateTime,
    error_class: &str,
    error_message: &str,
    terminal: bool,
) -> Result<JobRow, JobRepositoryError> {
    Ok(sqlx::query_as::<_, JobRow>(
        r#"
        UPDATE queue.jobs
        SET state = $2, run_at = $3, lease_token = NULL, lease_owner = NULL,
            lease_expires_at = NULL, last_heartbeat_at = NULL,
            error_class = $4, error_message = $5,
            completed_at = CASE WHEN $6 THEN now() ELSE NULL END,
            updated_at = now()
        WHERE id = $1 AND state = 'running'
        RETURNING id, kind, payload, state, priority, run_at, attempt_count,
                  max_attempts, lease_token, lease_owner, lease_expires_at,
                  last_heartbeat_at, error_class, error_message, dedupe_key,
                  created_at, updated_at, completed_at
        "#,
    )
    .bind(job_id)
    .bind(state)
    .bind(run_at)
    .bind(error_class)
    .bind(error_message)
    .bind(terminal)
    .fetch_one(&mut **transaction)
    .await?)
}

async fn settle_terminal_publication_post(
    transaction: &mut Transaction<'_, Postgres>,
    post: &PublicationPostRow,
    post_id: Uuid,
    payload: &serde_json::Value,
    error_class: &str,
    error_message: &str,
) -> Result<(), sqlx::Error> {
    match post.state.as_str() {
        "queued" if payload_i64(payload, "expected_revision") == Some(post.revision) => {
            sqlx::query(
                "UPDATE posts SET state = 'failed', send_token = NULL, send_started_at = NULL, error_class = $2, error_message = $3, revision = revision + 1, updated_at = now() WHERE id = $1 AND state = 'queued' AND revision = $4",
            )
            .bind(post_id)
            .bind(error_class)
            .bind(error_message)
            .bind(post.revision)
            .execute(&mut **transaction)
            .await?;
        }
        "sending" => {
            sqlx::query(
                "UPDATE posts SET state = 'unknown', send_token = NULL, send_started_at = NULL, error_class = $2, error_message = $3, revision = revision + 1, updated_at = now() WHERE id = $1 AND state = 'sending'",
            )
            .bind(post_id)
            .bind(error_class)
            .bind(error_message)
            .execute(&mut **transaction)
            .await?;
        }
        _ => {}
    }
    Ok(())
}

fn payload_uuid(payload: &serde_json::Value, key: &str) -> Option<Uuid> {
    payload
        .get(key)
        .and_then(serde_json::Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok())
}

fn payload_i64(payload: &serde_json::Value, key: &str) -> Option<i64> {
    payload.get(key).and_then(serde_json::Value::as_i64)
}

async fn reconcile_exhausted_job(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    job: &RecoveredJob,
) -> Result<(), sqlx::Error> {
    let job_type = match JobType::try_from(job.kind.as_str()) {
        Ok(job_type) => job_type,
        Err(_) => return Ok(()),
    };
    let ingest_id = job
        .payload
        .get("ingest_id")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok());
    if matches!(
        job_type,
        JobType::InspectSource
            | JobType::DownloadSource
            | JobType::ProbeAsset
            | JobType::NormalizeAsset
            | JobType::ComputeFingerprint
            | JobType::FinalizeIngest
    ) {
        if let Some(ingest_id) = ingest_id {
            sqlx::query(
                "UPDATE ingests SET state = 'failed_terminal', error_code = 'job_lease_expired', error_message = 'job lease expired after the final attempt', completed_at = now(), updated_at = now() WHERE id = $1 AND state NOT IN ('completed', 'failed_terminal', 'cancelled')",
            )
            .bind(ingest_id)
            .execute(&mut **transaction)
            .await?;
            if let Some(workspace_id) =
                sqlx::query_scalar::<_, Uuid>("SELECT workspace_id FROM ingests WHERE id = $1")
                    .bind(ingest_id)
                    .fetch_optional(&mut **transaction)
                    .await?
            {
                enqueue_workspace_cleanup(
                    transaction,
                    ingest_id,
                    workspace_id,
                    OffsetDateTime::now_utc() + WORKSPACE_CLEANUP_RETENTION,
                )
                .await?;
            }
        }
        return Ok(());
    }
    if job_type == JobType::SyncStorageCaption {
        let Some(media_id) = job
            .payload
            .get("media_id")
            .and_then(serde_json::Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok())
        else {
            return Ok(());
        };
        let generation = job.payload.get("generation").and_then(serde_json::Value::as_i64);
        sqlx::query(
        "UPDATE media SET caption_sync_state = 'failed', caption_sync_error = 'caption sync job lease expired after the final attempt', caption_sync_claim_token = NULL, updated_at = now() WHERE id = $1 AND caption_sync_state = 'syncing' AND ($2::bigint IS NULL OR caption_sync_generation = $2)",
        )
        .bind(media_id)
        .bind(generation)
        .execute(&mut **transaction)
        .await?;
        return Ok(());
    }
    if job_type != JobType::UploadStorageAsset {
        return Ok(());
    }
    let Some(media_id) = job
        .payload
        .get("media_id")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok())
    else {
        return Ok(());
    };
    let storage_state =
        sqlx::query_scalar::<_, String>("SELECT storage_state FROM media WHERE id = $1 FOR UPDATE")
            .bind(media_id)
            .fetch_optional(&mut **transaction)
            .await?;
    match storage_state.as_deref() {
        Some("ready") => {
            sqlx::query(
                "UPDATE ingests SET state = 'completed', error_code = NULL, error_message = NULL, completed_at = now(), updated_at = now() WHERE media_id = $1 AND state <> 'cancelled' AND (state = 'storing' OR (state = 'failed_retryable' AND error_code IN ('storage_upload', 'storage_unknown')) OR (state = 'failed_terminal' AND error_code IN ('storage_upload', 'storage_unknown')))",
            )
            .bind(media_id)
            .execute(&mut **transaction)
            .await?;
        }
        Some(_) => {
            sqlx::query(
                "UPDATE media SET storage_state = 'storage_unknown', storage_token = NULL, storage_started_at = NULL, updated_at = now() WHERE id = $1 AND storage_state <> 'ready'",
            )
            .bind(media_id)
            .execute(&mut **transaction)
            .await?;
            sqlx::query(
                "UPDATE ingests SET state = 'failed_terminal', error_code = 'storage_unknown', error_message = 'storage job lease expired; external storage result requires reconciliation', completed_at = now(), updated_at = now() WHERE media_id = $1 AND state <> 'cancelled' AND (state NOT IN ('failed_terminal') OR error_code IN ('storage_upload', 'storage_unknown'))",
            )
            .bind(media_id)
            .execute(&mut **transaction)
            .await?;
        }
        None => {}
    }
    Ok(())
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
