use sooqa_jobs::{Job, JobCommand, JobLease, JobStatus, NewJob};
use sqlx::{PgPool, Postgres, Transaction};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

use crate::{jobs::JobSettlement, settlement};

/// Completed and terminal workspaces remain available for operator recovery
/// for one day. The cleanup job is still queued immediately; this delay is the
/// fallback retention window and the delay used for duplicate/terminal work.
pub const WORKSPACE_CLEANUP_RETENTION: Duration = Duration::days(1);

/// Serializes database decisions that affect a media workspace. The advisory
/// key is a hash rather than a filesystem path, so it remains valid across
/// workers and cannot be bypassed by a second process. Hash collisions only
/// add serialization; they cannot permit an unsafe concurrent mutation.
pub(crate) async fn lock_workspace_fence(
    transaction: &mut Transaction<'_, Postgres>,
    resource_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(resource_id.to_string())
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

pub(crate) async fn enqueue_workspace_cleanup(
    transaction: &mut Transaction<'_, Postgres>,
    ingest_id: Uuid,
    workspace_id: Uuid,
    run_at: OffsetDateTime,
) -> Result<(), sqlx::Error> {
    let job = NewJob::cleanup_workspace(ingest_id, workspace_id)
        .run_at(run_at)
        .dedupe_key(format!("ingest:{ingest_id}:cleanup_workspace:v1:{workspace_id}"));
    sqlx::query(
        "INSERT INTO queue.jobs (kind, payload, state, run_at, max_attempts, dedupe_key) VALUES ($1, $2, 'queued', COALESCE($3, now()), $4, $5) ON CONFLICT (dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING",
    )
    .bind(job.job_type().as_str())
    .bind(job.payload_json())
    .bind(job.run_at_value())
    .bind(job.max_attempts_value())
    .bind(job.dedupe_key_value())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

pub(crate) async fn enqueue_workspace_cleanup_for_media(
    transaction: &mut Transaction<'_, Postgres>,
    media_id: Uuid,
    run_at: OffsetDateTime,
) -> Result<(), sqlx::Error> {
    let workspaces = sqlx::query_as::<_, (Uuid, Uuid)>(
        "SELECT id, workspace_id FROM ingests WHERE media_id = $1 AND state <> 'cancelled'",
    )
    .bind(media_id)
    .fetch_all(&mut **transaction)
    .await?;
    for (ingest_id, workspace_id) in workspaces {
        enqueue_workspace_cleanup(transaction, ingest_id, workspace_id, run_at).await?;
    }
    Ok(())
}

pub(crate) async fn protected_workspace_ids(pool: &PgPool) -> Result<Vec<Uuid>, sqlx::Error> {
    // Reconciliation runs from a snapshot and deletes after this query
    // commits. Protect every workspace that is still current for an ingest,
    // not only active pipeline states, so a storage reset cannot reopen bytes
    // between the snapshot and deletion.
    sqlx::query_scalar("SELECT DISTINCT workspace_id FROM ingests").fetch_all(pool).await
}

/// A cleanup replay is safe only for the generation it names.  A force-save
/// changes the ingest workspace ID, so an old job cannot touch the current
/// workspace.  For the current generation, the filesystem hand-off is known
/// complete only after the media path has been cleared and storage is in a
/// resolved state.
pub(crate) async fn terminal_job_retention_eligible(
    transaction: &mut Transaction<'_, Postgres>,
    job: &Job,
) -> Result<bool, sqlx::Error> {
    let JobCommand::CleanupWorkspace(payload) = &job.command else {
        return Ok(false);
    };
    let Some((current_workspace_id, state, media_id)) =
        sqlx::query_as::<_, (Uuid, String, Option<Uuid>)>(
            "SELECT workspace_id, state, media_id FROM ingests WHERE id = $1 FOR UPDATE",
        )
        .bind(payload.ingest_id)
        .fetch_optional(&mut **transaction)
        .await?
    else {
        return Ok(false);
    };
    if current_workspace_id != payload.workspace_id {
        // The payload names an orphaned generation.  It cannot refer to the
        // current workspace after force-save changed the generation fence;
        // replaying its filesystem removal cannot corrupt current state.
        return Ok(true);
    }
    // Clearing local_work_path is the hand-off before the filesystem call,
    // so a failed/cancelled current-generation cleanup is not proof that the
    // directory was removed.  Preserve the only durable retry signal.
    if job.status != JobStatus::Succeeded {
        return Ok(false);
    }
    if !matches!(
        state.as_str(),
        "completed" | "duplicate_pending" | "failed_terminal" | "cancelled" | "storing"
    ) {
        return Ok(false);
    }
    let Some(media_id) = media_id else {
        return Ok(true);
    };
    let Some((storage_state, local_work_path)) = sqlx::query_as::<_, (String, Option<String>)>(
        "SELECT storage_state, local_work_path FROM media WHERE id = $1 FOR UPDATE",
    )
    .bind(media_id)
    .fetch_optional(&mut **transaction)
    .await?
    else {
        return Ok(false);
    };
    Ok(matches!(storage_state.as_str(), "ready" | "missing") && local_work_path.is_none())
}

/// Cleanup has no additional durable domain transition after its handler has
/// released the workspace. Keep its queue settlement ownership here so the
/// central dispatcher remains a family router rather than a policy owner.
pub(crate) async fn settle_job(
    pool: &PgPool,
    lease: &JobLease,
    expected: &JobCommand,
    settlement: JobSettlement,
) -> Result<Job, crate::JobRepositoryError> {
    if !matches!(expected, JobCommand::CleanupWorkspace(_)) {
        return Err(crate::JobRepositoryError::LeaseLost);
    }
    let mut transaction = pool.begin().await?;
    let row =
        settlement::settle_queue_in_transaction(&mut transaction, lease, expected, settlement)
            .await?;
    transaction.commit().await?;
    row.into_job()
}

pub(crate) async fn recover_job(
    pool: &PgPool,
    job_id: Uuid,
    expected: &JobCommand,
) -> Result<bool, crate::JobRepositoryError> {
    if !matches!(expected, JobCommand::CleanupWorkspace(_)) {
        return Err(crate::JobRepositoryError::LeaseLost);
    }
    let mut transaction = pool.begin().await?;
    let recovered =
        settlement::recover_queue_only_in_transaction(&mut transaction, job_id, expected).await?;
    transaction.commit().await?;
    Ok(recovered.is_some())
}
