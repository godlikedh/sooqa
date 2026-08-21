//! Typed settlement and stale-lease recovery for durable job families.
//!
//! Queue claiming and fencing stay in `jobs.rs`. This module is the narrow
//! dispatch boundary used after a worker has decoded a `JobCommand`; each
//! family module owns the domain row policy that must agree with a queue
//! transition.

use crate::jobs::{JobRepositoryError, JobRow, JobSettlement};
use sooqa_jobs::{Job, JobCommand, JobLease};
use sqlx::{PgPool, Postgres, Transaction};
use time::OffsetDateTime;
use uuid::Uuid;

pub(crate) async fn settle(
    pool: &PgPool,
    command: Option<&JobCommand>,
    lease: &JobLease,
    settlement: JobSettlement,
) -> Result<Job, JobRepositoryError> {
    let allow_expired = settlement.allows_expired_lease();
    let command = match command {
        Some(command) if allow_expired => match load_running_job(pool, lease, true).await {
            Ok(_) => command.clone(),
            Err(JobRepositoryError::LeaseLost) => {
                return load_already_requeued(pool, lease, &settlement, Some(command)).await;
            }
            Err(error) => return Err(error),
        },
        Some(command) => command.clone(),
        None => match load_running_job(pool, lease, allow_expired).await {
            Ok(job) => job.command,
            Err(JobRepositoryError::LeaseLost) if allow_expired => {
                return load_already_requeued(pool, lease, &settlement, None).await;
            }
            Err(error) => return Err(error),
        },
    };
    match &command {
        JobCommand::PublishPost(_) => {
            crate::publisher::settle_publish_job(pool, lease, &command, settlement).await
        }
        JobCommand::UploadStorageAsset(_) => {
            crate::library::settle_storage_job(pool, lease, &command, settlement).await
        }
        JobCommand::SyncStorageCaption(_) => {
            crate::library::settle_caption_job(pool, lease, &command, settlement).await
        }
        JobCommand::InspectSource(_) => {
            crate::inbox::settle_job(pool, lease, &command, settlement).await
        }
        JobCommand::DownloadSource(_) => {
            crate::inbox::settle_job(pool, lease, &command, settlement).await
        }
        JobCommand::ProbeAsset(_)
        | JobCommand::NormalizeAsset(_)
        | JobCommand::ComputeFingerprint(_)
        | JobCommand::FinalizeIngest(_) => {
            crate::inbox::settle_job(pool, lease, &command, settlement).await
        }
        JobCommand::MaterializePublication(_) => {
            crate::publisher::settle_materialize_job(pool, lease, &command, settlement).await
        }
        JobCommand::CleanupWorkspace(_) => {
            crate::cleanup::settle_job(pool, lease, &command, settlement).await
        }
    }
}

pub(crate) async fn recover_stale(pool: &PgPool) -> Result<u64, JobRepositoryError> {
    let candidates = sqlx::query_as::<_, JobRow>(
        r#"
        SELECT id, kind, payload, state, priority, run_at, attempt_count,
               max_attempts, lease_token, lease_owner, lease_expires_at,
               last_heartbeat_at, error_class, error_message, dedupe_key,
               created_at, updated_at, completed_at
        FROM queue.jobs
        WHERE state = 'running' AND lease_expires_at <= clock_timestamp()
        ORDER BY updated_at, id
        "#,
    )
    .fetch_all(pool)
    .await?;
    let mut recovered = 0;
    for candidate in candidates {
        if recover_one(pool, candidate).await? {
            recovered += 1;
        }
    }
    Ok(recovered)
}

async fn recover_one(pool: &PgPool, candidate: JobRow) -> Result<bool, JobRepositoryError> {
    let job_id = candidate.id;
    let command = match candidate.into_job() {
        Ok(job) => job.command,
        Err(_) => return recover_queue_only_untyped(pool, job_id).await,
    };
    match &command {
        JobCommand::PublishPost(_) => {
            crate::publisher::recover_publish_job(pool, job_id, &command).await
        }
        JobCommand::UploadStorageAsset(_) => {
            crate::library::recover_storage_job(pool, job_id, &command).await
        }
        JobCommand::SyncStorageCaption(_) => {
            crate::library::recover_caption_job(pool, job_id, &command).await
        }
        JobCommand::InspectSource(_) => crate::inbox::recover_job(pool, job_id, &command).await,
        JobCommand::DownloadSource(_) => crate::inbox::recover_job(pool, job_id, &command).await,
        JobCommand::ProbeAsset(_)
        | JobCommand::NormalizeAsset(_)
        | JobCommand::ComputeFingerprint(_)
        | JobCommand::FinalizeIngest(_) => crate::inbox::recover_job(pool, job_id, &command).await,
        JobCommand::MaterializePublication(_) => {
            crate::publisher::recover_materialize_job(pool, job_id, &command).await
        }
        JobCommand::CleanupWorkspace(_) => {
            crate::cleanup::recover_job(pool, job_id, &command).await
        }
    }
}

async fn load_running_job(
    pool: &PgPool,
    lease: &JobLease,
    allow_expired: bool,
) -> Result<Job, JobRepositoryError> {
    sqlx::query_as::<_, JobRow>(
        r#"
        SELECT id, kind, payload, state, priority, run_at, attempt_count,
               max_attempts, lease_token, lease_owner, lease_expires_at,
               last_heartbeat_at, error_class, error_message, dedupe_key,
               created_at, updated_at, completed_at
        FROM queue.jobs
        WHERE id = $1 AND state = 'running' AND attempt_count = $2
          AND lease_owner = $3 AND lease_token = $4
          AND ($5 OR lease_expires_at > clock_timestamp())
        "#,
    )
    .bind(lease.job_id)
    .bind(lease.attempt_number)
    .bind(&lease.lease_owner)
    .bind(lease.lease_token)
    .bind(allow_expired)
    .fetch_optional(pool)
    .await?
    .ok_or(JobRepositoryError::LeaseLost)
    .and_then(JobRow::into_job)
}

async fn load_already_requeued(
    pool: &PgPool,
    lease: &JobLease,
    settlement: &JobSettlement,
    expected: Option<&JobCommand>,
) -> Result<Job, JobRepositoryError> {
    let error_class = settlement.error_class().ok_or(JobRepositoryError::LeaseLost)?;
    sqlx::query_as::<_, JobRow>(
        r#"
        SELECT id, kind, payload, state, priority, run_at, attempt_count,
               max_attempts, lease_token, lease_owner, lease_expires_at,
               last_heartbeat_at, error_class, error_message, dedupe_key,
               created_at, updated_at, completed_at
        FROM queue.jobs
        WHERE id = $1 AND state = 'queued' AND error_class = $2
        "#,
    )
    .bind(lease.job_id)
    .bind(error_class)
    .fetch_optional(pool)
    .await?
    .ok_or(JobRepositoryError::LeaseLost)
    .and_then(JobRow::into_job)
    .and_then(|job| {
        if expected.is_none_or(|expected| commands_have_same_owner(expected, &job.command)) {
            Ok(job)
        } else {
            Err(JobRepositoryError::LeaseLost)
        }
    })
}

/// Queue-only family owners use this after locking their domain aggregate in
/// the same transaction. The queue row is still validated here, immediately
/// before any mutation, so a forged command cannot settle another aggregate.
pub(crate) async fn settle_queue_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    lease: &JobLease,
    expected: &JobCommand,
    settlement: JobSettlement,
) -> Result<JobRow, JobRepositoryError> {
    let job = lock_running_job(&mut *transaction, lease, settlement.allows_expired_lease()).await?;
    let _ = validate_locked_command(&job, expected)?;
    let (state, run_at, error_class, error_message, terminal, non_consuming) =
        queue_parameters(&job, settlement);
    let row = update_locked_job(
        &mut *transaction,
        job.id,
        state,
        run_at,
        &error_class,
        &error_message,
        terminal,
        non_consuming,
    )
    .await?;
    Ok(row)
}

pub(crate) fn validate_locked_command(
    row: &JobRow,
    expected: &JobCommand,
) -> Result<JobCommand, JobRepositoryError> {
    let actual = row.clone().into_job()?.command;
    if commands_have_same_owner(expected, &actual) {
        Ok(actual)
    } else {
        Err(JobRepositoryError::LeaseLost)
    }
}

fn commands_have_same_owner(expected: &JobCommand, actual: &JobCommand) -> bool {
    match (expected, actual) {
        (JobCommand::InspectSource(expected), JobCommand::InspectSource(actual)) => {
            expected.ingest_id == actual.ingest_id
        }
        (JobCommand::DownloadSource(expected), JobCommand::DownloadSource(actual)) => {
            expected.ingest_id == actual.ingest_id
        }
        (JobCommand::ProbeAsset(expected), JobCommand::ProbeAsset(actual))
        | (JobCommand::NormalizeAsset(expected), JobCommand::NormalizeAsset(actual))
        | (JobCommand::ComputeFingerprint(expected), JobCommand::ComputeFingerprint(actual))
        | (JobCommand::FinalizeIngest(expected), JobCommand::FinalizeIngest(actual)) => {
            expected.ingest_id == actual.ingest_id
        }
        (
            JobCommand::MaterializePublication(expected),
            JobCommand::MaterializePublication(actual),
        ) => expected.ingest_id == actual.ingest_id,
        (JobCommand::UploadStorageAsset(expected), JobCommand::UploadStorageAsset(actual)) => {
            expected.media_id == actual.media_id && expected.generation == actual.generation
        }
        (JobCommand::SyncStorageCaption(expected), JobCommand::SyncStorageCaption(actual)) => {
            expected.media_id == actual.media_id && expected.generation == actual.generation
        }
        (JobCommand::PublishPost(expected), JobCommand::PublishPost(actual)) => {
            expected.post_id == actual.post_id
        }
        (JobCommand::CleanupWorkspace(expected), JobCommand::CleanupWorkspace(actual)) => {
            expected.ingest_id == actual.ingest_id && expected.workspace_id == actual.workspace_id
        }
        _ => false,
    }
}

pub(crate) fn queue_parameters(
    job: &JobRow,
    settlement: JobSettlement,
) -> (&'static str, OffsetDateTime, String, String, bool, bool) {
    match settlement {
        JobSettlement::Retry { run_at, error_class, error_message, non_consuming }
        | JobSettlement::Defer { run_at, error_class, error_message, non_consuming } => {
            let terminal = !non_consuming && job.attempt_count >= job.max_attempts;
            (
                if terminal { "failed" } else { "queued" },
                if terminal { job.run_at } else { run_at },
                error_class,
                error_message,
                terminal,
                non_consuming,
            )
        }
        JobSettlement::Fail { error_class, error_message } => {
            ("failed", job.run_at, error_class, error_message, true, false)
        }
    }
}

pub(crate) async fn lock_running_job(
    transaction: &mut Transaction<'_, Postgres>,
    lease: &JobLease,
    allow_expired: bool,
) -> Result<JobRow, JobRepositoryError> {
    sqlx::query_as::<_, JobRow>(
        r#"
        SELECT id, kind, payload, state, priority, run_at, attempt_count,
               max_attempts, lease_token, lease_owner, lease_expires_at,
               last_heartbeat_at, error_class, error_message, dedupe_key,
               created_at, updated_at, completed_at
        FROM queue.jobs
        WHERE id = $1 AND state = 'running' AND attempt_count = $2
          AND lease_owner = $3 AND lease_token = $4
          AND ($5 OR lease_expires_at > clock_timestamp())
        FOR UPDATE
        "#,
    )
    .bind(lease.job_id)
    .bind(lease.attempt_number)
    .bind(&lease.lease_owner)
    .bind(lease.lease_token)
    .bind(allow_expired)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(JobRepositoryError::LeaseLost)
}

// This helper mirrors the queue.jobs columns changed by every family
// settlement. Keeping them explicit makes terminal and non-consuming retry
// semantics visible at each call site.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn update_locked_job(
    transaction: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    state: &str,
    run_at: OffsetDateTime,
    error_class: &str,
    error_message: &str,
    terminal: bool,
    non_consuming: bool,
) -> Result<JobRow, JobRepositoryError> {
    Ok(sqlx::query_as::<_, JobRow>(
        r#"
        UPDATE queue.jobs
        SET state = $2,
            attempt_count = CASE WHEN $7 THEN GREATEST(attempt_count - 1, 0) ELSE attempt_count END,
            run_at = $3, lease_token = NULL, lease_owner = NULL,
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
    .bind(non_consuming)
    .fetch_one(&mut **transaction)
    .await?)
}

/// Recover a queue-only family while its domain row is already locked by the
/// caller. Returning `None` means the stale lease was claimed by another
/// worker before this transaction acquired the queue lock.
pub(crate) async fn recover_queue_only_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    expected: &JobCommand,
) -> Result<Option<JobRow>, JobRepositoryError> {
    let Some(job) = lock_expired_job(transaction, job_id).await? else {
        return Ok(None);
    };
    let _ = validate_locked_command(&job, expected)?;
    let terminal = job.attempt_count >= job.max_attempts;
    let row = update_locked_job(
        transaction,
        job.id,
        if terminal { "failed" } else { "queued" },
        OffsetDateTime::now_utc(),
        job.error_class.as_deref().unwrap_or("lease_expired"),
        job.error_message.as_deref().unwrap_or("job lease expired"),
        terminal,
        false,
    )
    .await?;
    Ok(Some(row))
}

pub(crate) async fn recover_queue_only_untyped(
    pool: &PgPool,
    job_id: Uuid,
) -> Result<bool, JobRepositoryError> {
    let mut transaction = pool.begin().await?;
    let Some(job) = lock_expired_job(&mut transaction, job_id).await? else {
        transaction.commit().await?;
        return Ok(false);
    };
    let terminal = job.attempt_count >= job.max_attempts;
    update_locked_job(
        &mut transaction,
        job.id,
        if terminal { "failed" } else { "queued" },
        OffsetDateTime::now_utc(),
        job.error_class.as_deref().unwrap_or("lease_expired"),
        job.error_message.as_deref().unwrap_or("job lease expired"),
        terminal,
        false,
    )
    .await?;
    transaction.commit().await?;
    Ok(true)
}

pub(crate) async fn lock_expired_job(
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
        WHERE id = $1 AND state = 'running' AND lease_expires_at <= clock_timestamp()
        FOR UPDATE
        "#,
    )
    .bind(job_id)
    .fetch_optional(&mut **transaction)
    .await?)
}
