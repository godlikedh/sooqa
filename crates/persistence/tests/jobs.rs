use std::time::Duration;

use serde_json::json;
use sooqa_inbox::{
    IngestSubmission, IngestSubmissionInput, SourceInspection, SourceMediaKind, SubmittedVia,
};
use sooqa_jobs::{InspectSourcePayload, JobCommand, JobStatus, JobType, NewJob};
use sooqa_persistence::{
    Database, JobRepositoryError, JobRetentionError, JobRetentionPolicy, JobSettlement,
};
use sooqa_publisher::{PostSchedule, PostUpdate};
use time::{Duration as TimeDuration, OffsetDateTime};
use uuid::Uuid;

#[test]
fn retention_policy_rejects_unbounded_horizons() {
    let policy = JobRetentionPolicy::new(
        TimeDuration::days(3651),
        TimeDuration::days(1),
        TimeDuration::days(1),
        1,
        1,
    );
    assert!(matches!(policy.validate(), Err(JobRetentionError::InvalidHorizon)));
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn claim_retry_and_fencing_use_the_queue_jobs_row(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let repository = database.jobs();
    let ingest_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    let job = repository
        .enqueue(
            NewJob::cleanup_workspace(ingest_id, workspace_id)
                .dedupe_key(format!("test:{}", Uuid::new_v4())),
        )
        .await
        .expect("job should enqueue");
    assert_eq!(job.job_type(), JobType::CleanupWorkspace);
    let claimed = repository
        .claim_next("test-worker", Duration::from_secs(30), &[JobType::CleanupWorkspace])
        .await
        .expect("job should claim")
        .expect("a queued job should be available");
    let lease = claimed.lease().expect("claim should carry a fence");
    let retried = repository
        .retry_lease(&lease, OffsetDateTime::now_utc(), "test", "retry")
        .await
        .expect("retry should update the same row");
    assert_eq!(retried.status, JobStatus::Queued);
    let stale = repository.complete_lease(&lease).await;
    assert!(stale.is_err(), "the old fence must not complete a retried job");
    let retried_claim = repository
        .claim_next("cleanup-worker", Duration::from_secs(30), &[JobType::CleanupWorkspace])
        .await
        .expect("retried job should be claimable")
        .expect("retried job should be available");
    repository
        .complete_lease(&retried_claim.lease().expect("retried job should have a lease"))
        .await
        .expect("retried job should complete with its current lease");
    let _bounded = repository
        .enqueue(
            NewJob::cleanup_workspace(Uuid::new_v4(), Uuid::new_v4())
                .max_attempts(1)
                .dedupe_key(format!("bounded-test:{}", Uuid::new_v4())),
        )
        .await
        .expect("bounded job should enqueue");
    let bounded_claim = repository
        .claim_next("bounded-worker", Duration::from_secs(30), &[JobType::CleanupWorkspace])
        .await
        .expect("bounded job should claim")
        .expect("bounded job should be available");
    let exhausted = repository
        .retry_lease(
            &bounded_claim.lease().expect("bounded claim should have a lease"),
            OffsetDateTime::now_utc(),
            "test_exhausted",
            "simulated final retry",
        )
        .await
        .expect("last retry should persist a terminal state");
    assert_eq!(exhausted.status, JobStatus::Failed);
    assert!(
        repository
            .claim_next("bounded-worker-2", Duration::from_secs(30), &[JobType::CleanupWorkspace])
            .await
            .expect("exhausted job query should succeed")
            .is_none()
    );
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn settlement_rejects_a_job_with_another_jobs_lease(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let repository = database.jobs();
    for suffix in ["a", "b"] {
        repository
            .enqueue(
                NewJob::cleanup_workspace(Uuid::new_v4(), Uuid::new_v4())
                    .dedupe_key(format!("mismatched-settlement-{suffix}-{}", Uuid::new_v4())),
            )
            .await
            .expect("job should enqueue");
    }
    let first = repository
        .claim_next("mismatch-worker-a", Duration::from_secs(30), &[JobType::CleanupWorkspace])
        .await
        .expect("first job should claim")
        .expect("first job should be available");
    let second = repository
        .claim_next("mismatch-worker-b", Duration::from_secs(30), &[JobType::CleanupWorkspace])
        .await
        .expect("second job should claim")
        .expect("second job should be available");
    let error = repository
        .settle_lease(
            &first,
            &second.lease().expect("second job should carry a lease"),
            JobSettlement::retry(OffsetDateTime::now_utc(), "mismatch", "wrong lease"),
        )
        .await
        .expect_err("a lease from another job must be rejected before dispatch");
    assert!(matches!(error, JobRepositoryError::LeaseLost));
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM queue.jobs WHERE id = $1")
            .bind(first.id)
            .fetch_one(database.pool())
            .await
            .expect("first job state should be readable"),
        "running"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM queue.jobs WHERE id = $1")
            .bind(second.id)
            .fetch_one(database.pool())
            .await
            .expect("second job state should be readable"),
        "running"
    );
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn settlement_rejects_a_forged_command_before_domain_or_queue_mutation(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let repository = database.jobs();
    let first_ingest = database
        .inbox()
        .create_ingest(
            IngestSubmission::try_new(IngestSubmissionInput::new(
                format!("https://example.test/forged-a-{}", Uuid::new_v4()),
                SubmittedVia::Api,
            ))
            .unwrap(),
        )
        .await
        .unwrap()
        .ingest
        .id;
    let second_ingest = database
        .inbox()
        .create_ingest(
            IngestSubmission::try_new(IngestSubmissionInput::new(
                format!("https://example.test/forged-b-{}", Uuid::new_v4()),
                SubmittedVia::Api,
            ))
            .unwrap(),
        )
        .await
        .unwrap()
        .ingest
        .id;
    let first = repository
        .claim_next("forged-worker-a", Duration::from_secs(30), &[JobType::InspectSource])
        .await
        .unwrap()
        .expect("first inspect job should claim");
    let second = repository
        .claim_next("forged-worker-b", Duration::from_secs(30), &[JobType::InspectSource])
        .await
        .unwrap()
        .expect("second inspect job should claim");
    assert_eq!(
        first.command,
        JobCommand::InspectSource(InspectSourcePayload { ingest_id: first_ingest })
    );
    assert_eq!(
        second.command,
        JobCommand::InspectSource(InspectSourcePayload { ingest_id: second_ingest })
    );

    let mut forged = first.clone();
    forged.command = JobCommand::InspectSource(InspectSourcePayload { ingest_id: second_ingest });
    let error = repository
        .settle_lease(
            &forged,
            &first.lease().expect("first job should carry a lease"),
            JobSettlement::fail("forged_command", "must not mutate another ingest"),
        )
        .await
        .expect_err("a same-lease command for another ingest must be fenced");
    assert!(matches!(error, JobRepositoryError::LeaseLost));

    for ingest_id in [first_ingest, second_ingest] {
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT state FROM ingests WHERE id = $1")
                .bind(ingest_id)
                .fetch_one(database.pool())
                .await
                .unwrap(),
            "queued"
        );
    }
    let first_queue = sqlx::query_as::<_, (String, i32, Option<String>)>(
        "SELECT state, attempt_count, error_class FROM queue.jobs WHERE id = $1",
    )
    .bind(first.id)
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(first_queue, ("running".to_owned(), 1, None));
    let second_queue =
        sqlx::query_scalar::<_, String>("SELECT state FROM queue.jobs WHERE id = $1")
            .bind(second.id)
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert_eq!(second_queue, "running");
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn malformed_storage_payload_does_not_abort_stale_recovery(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let malformed_id: Uuid = sqlx::query_scalar(
        "INSERT INTO queue.jobs (kind, payload, state, attempt_count, lease_token, lease_owner, lease_expires_at, last_heartbeat_at, dedupe_key) VALUES ('upload_storage_asset', $1, 'running', 1, $2, 'malformed-worker', now() - interval '1 second', now() - interval '1 second', $3) RETURNING id",
    )
    .bind(json!({ "media_id": "not-a-uuid" }))
    .bind(Uuid::new_v4())
    .bind(format!("malformed-storage:{}", Uuid::new_v4()))
    .fetch_one(database.pool())
    .await
    .expect("malformed storage job should insert");
    let valid = database
        .jobs()
        .enqueue(
            NewJob::cleanup_workspace(Uuid::new_v4(), Uuid::new_v4())
                .dedupe_key(format!("valid-stale:{}", Uuid::new_v4())),
        )
        .await
        .expect("valid job should enqueue");
    let valid_claim = database
        .jobs()
        .claim_next("valid-worker", Duration::from_secs(30), &[JobType::CleanupWorkspace])
        .await
        .unwrap()
        .expect("valid job should claim");
    assert_eq!(valid.id, valid_claim.id);
    sqlx::query(
        "UPDATE queue.jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1",
    )
    .bind(valid.id)
    .execute(database.pool())
    .await
    .unwrap();

    assert_eq!(database.jobs().recover_stale_leases().await.unwrap(), 2);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM queue.jobs WHERE id = $1")
            .bind(malformed_id)
            .fetch_one(database.pool())
            .await
            .unwrap(),
        "queued"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM queue.jobs WHERE id = $1")
            .bind(valid.id)
            .fetch_one(database.pool())
            .await
            .unwrap(),
        "queued"
    );
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn deferral_consumes_the_final_attempt(pool: sqlx::PgPool) {
    let repository = Database::from_pool(pool).jobs();
    repository
        .enqueue(
            NewJob::cleanup_workspace(Uuid::new_v4(), Uuid::new_v4())
                .max_attempts(1)
                .dedupe_key(format!("defer-test:{}", Uuid::new_v4())),
        )
        .await
        .expect("bounded job should enqueue");
    let claimed = repository
        .claim_next("defer-worker", Duration::from_secs(30), &[JobType::CleanupWorkspace])
        .await
        .expect("job should claim")
        .expect("queued job should be available");
    assert_eq!(claimed.attempt_count, 1);

    let deferred = repository
        .defer_lease(
            &claimed.lease().expect("claim should carry a lease"),
            OffsetDateTime::now_utc(),
            "work_disk_low",
            "synthetic reserve is full",
        )
        .await
        .expect("resource deferral should remain durable");
    assert_eq!(deferred.status, JobStatus::Failed);
    assert_eq!(deferred.attempt_count, 1);
    assert!(deferred.completed_at.is_some());

    assert!(
        repository
            .claim_next("defer-worker-2", Duration::from_secs(30), &[JobType::CleanupWorkspace])
            .await
            .expect("deferred job query should succeed")
            .is_none()
    );
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn scheduled_deferral_without_consuming_attempt_is_reclaimable(pool: sqlx::PgPool) {
    let repository = Database::from_pool(pool).jobs();
    let job = repository
        .enqueue(
            NewJob::cleanup_workspace(Uuid::new_v4(), Uuid::new_v4())
                .max_attempts(1)
                .dedupe_key(format!("non-consuming-defer-test:{}", Uuid::new_v4())),
        )
        .await
        .expect("bounded job should enqueue");
    let claimed = repository
        .claim_next("defer-worker", Duration::from_secs(30), &[JobType::CleanupWorkspace])
        .await
        .expect("job should claim")
        .expect("queued job should be available");
    assert_eq!(claimed.attempt_count, 1);

    let deferred = repository
        .defer_lease_without_consuming_attempt(
            &claimed.lease().expect("claim should carry a lease"),
            OffsetDateTime::now_utc(),
            "work_disk_low",
            "synthetic reserve is full",
        )
        .await
        .expect("resource deferral should remain durable");
    assert_eq!(deferred.status, JobStatus::Queued);
    assert_eq!(deferred.attempt_count, 0);
    assert!(deferred.completed_at.is_none());

    let reclaimed = repository
        .claim_next("defer-worker-2", Duration::from_secs(30), &[JobType::CleanupWorkspace])
        .await
        .expect("deferred job query should succeed")
        .expect("a max-attempts=1 deferred job should be claimable again");
    assert_eq!(reclaimed.attempt_count, 1);
    assert_eq!(reclaimed.id, job.id);
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn expired_final_attempt_reconciles_ingest_and_fences_all_mutations(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let ingest = database
        .inbox()
        .create_ingest(
            IngestSubmission::try_new(IngestSubmissionInput::new(
                format!("https://example.test/expired-{}", uuid::Uuid::new_v4()),
                SubmittedVia::Api,
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    let claimed = database
        .jobs()
        .claim_next("expired-worker", Duration::from_secs(30), &[JobType::InspectSource])
        .await
        .unwrap()
        .expect("inspect job should be claimable");
    let lease = claimed.lease().expect("claimed job should have a lease");
    sqlx::query(
        "UPDATE queue.jobs SET max_attempts = 1, lease_expires_at = now() - interval '1 second' WHERE id = $1",
    )
    .bind(claimed.id)
    .execute(database.pool())
    .await
    .unwrap();

    assert!(database.jobs().heartbeat_lease(&lease, Duration::from_secs(30)).await.is_err());
    assert!(database.jobs().complete_lease(&lease).await.is_err());
    assert!(
        database
            .jobs()
            .retry_lease(&lease, OffsetDateTime::now_utc(), "stale", "stale")
            .await
            .is_err()
    );
    assert!(
        database
            .jobs()
            .defer_lease(&lease, OffsetDateTime::now_utc(), "stale", "stale")
            .await
            .is_err()
    );
    assert!(database.jobs().fail_lease(&lease, "stale", "stale").await.is_err());

    assert_eq!(database.jobs().recover_stale_leases().await.unwrap(), 1);
    let job_state = sqlx::query_scalar::<_, String>("SELECT state FROM queue.jobs WHERE id = $1")
        .bind(claimed.id)
        .fetch_one(database.pool())
        .await
        .unwrap();
    assert_eq!(job_state, "failed");
    let request = database.inbox().find(ingest.ingest.id).await.unwrap().unwrap();
    assert_eq!(request.status.as_str(), "failed_terminal");
    assert_eq!(request.error_code.as_deref(), Some("job_lease_expired"));
}

async fn create_cancelled_ingest(database: &Database, suffix: &str) -> (Uuid, Uuid) {
    let ingest = database
        .inbox()
        .create_ingest(
            IngestSubmission::try_new(IngestSubmissionInput::new(
                format!("https://example.test/retention/{suffix}/{}", Uuid::new_v4()),
                SubmittedVia::Api,
            ))
            .unwrap(),
        )
        .await
        .unwrap()
        .ingest;
    sqlx::query("DELETE FROM queue.jobs WHERE payload->>'ingest_id' = $1")
        .bind(ingest.id.to_string())
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query(
        "UPDATE ingests SET state = 'cancelled', completed_at = now(), updated_at = now() WHERE id = $1",
    )
    .bind(ingest.id)
    .execute(database.pool())
    .await
    .unwrap();
    (ingest.id, ingest.workspace_id)
}

async fn age_terminal_job(pool: &sqlx::PgPool, id: Uuid, state: &str) {
    sqlx::query(
        "UPDATE queue.jobs SET state = $2, error_class = 'retention_test', error_message = 'synthetic terminal row', completed_at = now() - interval '2 days', updated_at = now() - interval '2 days' WHERE id = $1",
    )
    .bind(id)
    .bind(state)
    .execute(pool)
    .await
    .unwrap();
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn terminal_job_retention_covers_all_families_and_keeps_unknown_work(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let repository = database.jobs();
    let mut prunable = Vec::new();
    let inspection = SourceInspection {
        adapter: "retention-test".to_owned(),
        source_url: "https://example.test/media".to_owned(),
        resolved_url: None,
        media_kind: SourceMediaKind::Video,
        mime_type: Some("video/mp4".to_owned()),
        content_length_bytes: Some(1),
        title: None,
        metadata: json!({}),
    };

    let ingest_specs = [
        ("inspect", NewJob::inspect_source as fn(Uuid) -> NewJob),
        ("probe", NewJob::probe_asset as fn(Uuid) -> NewJob),
        ("normalize", NewJob::normalize_asset as fn(Uuid) -> NewJob),
        ("fingerprint", NewJob::compute_fingerprint as fn(Uuid) -> NewJob),
        ("finalize", NewJob::finalize_ingest as fn(Uuid) -> NewJob),
    ];
    for (name, make_job) in ingest_specs {
        let ingest_id = create_cancelled_ingest(&database, name).await.0;
        let job = repository
            .enqueue(make_job(ingest_id).dedupe_key(format!("retention:{name}:{}", Uuid::new_v4())))
            .await
            .unwrap();
        let state = if name == "inspect" { "cancelled" } else { "succeeded" };
        age_terminal_job(database.pool(), job.id, state).await;
        prunable.push(job.id);
    }
    let ingest_id = create_cancelled_ingest(&database, "download").await.0;
    let job = repository
        .enqueue(
            NewJob::download_source(ingest_id, inspection)
                .dedupe_key(format!("retention:download:{}", Uuid::new_v4())),
        )
        .await
        .unwrap();
    age_terminal_job(database.pool(), job.id, "succeeded").await;
    prunable.push(job.id);

    let (materialize_ingest, _) = create_cancelled_ingest(&database, "materialize").await;
    let media_id: Uuid = sqlx::query_scalar(
        "INSERT INTO media (kind, storage_state, canonical_sha256, telegram_storage_chat_id, telegram_storage_message_id, telegram_file_id) VALUES ('video', 'ready', $1, -100123400001, 1, 'retention-file') RETURNING id",
    )
    .bind([Uuid::new_v4().as_bytes().to_vec(), Uuid::new_v4().as_bytes().to_vec()].concat())
    .fetch_one(database.pool())
    .await
    .unwrap();
    let channel_id: Uuid = sqlx::query_scalar(
        "INSERT INTO channels (telegram_chat_id, name) VALUES (-100123400002, 'retention') RETURNING id",
    )
    .fetch_one(database.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE ingests SET state = 'completed', completed_at = now(), media_id = $2, requested_action = 'queue', requested_channel_id = $3 WHERE id = $1",
    )
    .bind(materialize_ingest)
    .bind(media_id)
    .bind(channel_id)
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query("INSERT INTO posts (origin_ingest_id, media_id, channel_id, state) VALUES ($1, $2, $3, 'queued')")
        .bind(materialize_ingest)
        .bind(media_id)
        .bind(channel_id)
        .execute(database.pool())
        .await
        .unwrap();
    let materialize = repository
        .enqueue(
            NewJob::materialize_publication(materialize_ingest)
                .dedupe_key(format!("retention:materialize:{}", Uuid::new_v4())),
        )
        .await
        .unwrap();
    age_terminal_job(database.pool(), materialize.id, "failed").await;
    prunable.push(materialize.id);

    let (actionable_materialize_ingest, _) =
        create_cancelled_ingest(&database, "actionable-materialize").await;
    sqlx::query(
        "UPDATE ingests SET state = 'completed', completed_at = now(), media_id = $2, requested_action = 'queue', requested_channel_id = $3 WHERE id = $1",
    )
    .bind(actionable_materialize_ingest)
    .bind(media_id)
    .bind(channel_id)
    .execute(database.pool())
    .await
    .unwrap();
    let actionable_materialize = repository
        .enqueue(
            NewJob::materialize_publication(actionable_materialize_ingest)
                .dedupe_key(format!("retention:actionable-materialize:{}", Uuid::new_v4())),
        )
        .await
        .unwrap();
    age_terminal_job(database.pool(), actionable_materialize.id, "failed").await;

    let publish_post: Uuid = sqlx::query_scalar(
        "INSERT INTO posts (media_id, channel_id, state) VALUES ($1, $2, 'failed') RETURNING id",
    )
    .bind(media_id)
    .bind(channel_id)
    .fetch_one(database.pool())
    .await
    .unwrap();
    let publish = repository
        .enqueue(
            NewJob::publish_post(publish_post, 0)
                .dedupe_key(format!("retention:publish:{}", Uuid::new_v4())),
        )
        .await
        .unwrap();
    age_terminal_job(database.pool(), publish.id, "failed").await;
    prunable.push(publish.id);

    let caption = repository
        .enqueue(
            NewJob::sync_storage_caption(media_id, 0)
                .dedupe_key(format!("retention:caption:{}", Uuid::new_v4())),
        )
        .await
        .unwrap();
    sqlx::query("UPDATE media SET caption_sync_state = 'synced' WHERE id = $1")
        .bind(media_id)
        .execute(database.pool())
        .await
        .unwrap();
    age_terminal_job(database.pool(), caption.id, "succeeded").await;
    prunable.push(caption.id);

    let pending_caption_media: Uuid = sqlx::query_scalar(
        "INSERT INTO media (kind, storage_state, caption_sync_state, telegram_storage_chat_id, telegram_storage_message_id, telegram_file_id) VALUES ('video', 'ready', 'pending', -100123400003, 2, 'pending-caption-file') RETURNING id",
    )
    .fetch_one(database.pool())
    .await
    .unwrap();
    let pending_caption = repository
        .enqueue(
            NewJob::sync_storage_caption(pending_caption_media, 0)
                .dedupe_key(format!("retention:pending-caption:{}", Uuid::new_v4())),
        )
        .await
        .unwrap();
    age_terminal_job(database.pool(), pending_caption.id, "failed").await;

    let upload = repository
        .enqueue(
            NewJob::upload_storage_asset_generation(media_id, 0)
                .dedupe_key(format!("retention:upload:{}", Uuid::new_v4())),
        )
        .await
        .unwrap();
    age_terminal_job(database.pool(), upload.id, "succeeded").await;
    prunable.push(upload.id);

    let (cleanup_ingest, cleanup_workspace) = create_cancelled_ingest(&database, "cleanup").await;
    let cleanup = repository
        .enqueue(
            NewJob::cleanup_workspace(cleanup_ingest, cleanup_workspace)
                .dedupe_key(format!("retention:cleanup:{}", Uuid::new_v4())),
        )
        .await
        .unwrap();
    age_terminal_job(database.pool(), cleanup.id, "succeeded").await;
    prunable.push(cleanup.id);

    let failed_current_cleanup = repository
        .enqueue(
            NewJob::cleanup_workspace(cleanup_ingest, cleanup_workspace)
                .dedupe_key(format!("retention:failed-cleanup:{}", Uuid::new_v4())),
        )
        .await
        .unwrap();
    age_terminal_job(database.pool(), failed_current_cleanup.id, "failed").await;

    let old_generation_cleanup = repository
        .enqueue(
            NewJob::cleanup_workspace(cleanup_ingest, Uuid::new_v4())
                .dedupe_key(format!("retention:old-cleanup:{}", Uuid::new_v4())),
        )
        .await
        .unwrap();
    age_terminal_job(database.pool(), old_generation_cleanup.id, "failed").await;
    prunable.push(old_generation_cleanup.id);

    let unknown_media: Uuid = sqlx::query_scalar(
        "INSERT INTO media (kind, storage_state, storage_generation) VALUES ('video', 'storage_unknown', 0) RETURNING id",
    )
    .fetch_one(database.pool())
    .await
    .unwrap();
    let unknown_upload = repository
        .enqueue(
            NewJob::upload_storage_asset_generation(unknown_media, 0)
                .dedupe_key(format!("retention:unknown-upload:{}", Uuid::new_v4())),
        )
        .await
        .unwrap();
    age_terminal_job(database.pool(), unknown_upload.id, "failed").await;

    let missing_media: Uuid = sqlx::query_scalar(
        "INSERT INTO media (kind, storage_state) VALUES ('video', 'missing') RETURNING id",
    )
    .fetch_one(database.pool())
    .await
    .unwrap();
    let missing_upload = repository
        .enqueue(
            NewJob::upload_storage_asset_generation(missing_media, 0)
                .dedupe_key(format!("retention:missing-upload:{}", Uuid::new_v4())),
        )
        .await
        .unwrap();
    age_terminal_job(database.pool(), missing_upload.id, "cancelled").await;
    prunable.push(missing_upload.id);

    let unknown_post: Uuid = sqlx::query_scalar(
        "INSERT INTO posts (media_id, channel_id, state) VALUES ($1, $2, 'unknown') RETURNING id",
    )
    .bind(media_id)
    .bind(channel_id)
    .fetch_one(database.pool())
    .await
    .unwrap();
    let unknown_publish = repository
        .enqueue(
            NewJob::publish_post(unknown_post, 0)
                .dedupe_key(format!("retention:unknown-publish:{}", Uuid::new_v4())),
        )
        .await
        .unwrap();
    age_terminal_job(database.pool(), unknown_publish.id, "failed").await;

    let recent = repository
        .enqueue(
            NewJob::cleanup_workspace(cleanup_ingest, Uuid::new_v4())
                .dedupe_key(format!("retention:recent:{}", Uuid::new_v4())),
        )
        .await
        .unwrap();
    age_terminal_job(database.pool(), recent.id, "failed").await;
    sqlx::query("UPDATE queue.jobs SET completed_at = now(), updated_at = now() WHERE id = $1")
        .bind(recent.id)
        .execute(database.pool())
        .await
        .unwrap();

    let queued = repository
        .enqueue(
            NewJob::cleanup_workspace(cleanup_ingest, Uuid::new_v4())
                .run_at(OffsetDateTime::now_utc() + TimeDuration::days(1))
                .dedupe_key(format!("retention:queued:{}", Uuid::new_v4())),
        )
        .await
        .unwrap();
    let running_seed = repository
        .enqueue(
            NewJob::cleanup_workspace(cleanup_ingest, Uuid::new_v4())
                .dedupe_key(format!("retention:running:{}", Uuid::new_v4())),
        )
        .await
        .unwrap();
    let running = repository
        .claim_next("retention-running", Duration::from_secs(30), &[JobType::CleanupWorkspace])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(running.id, running_seed.id);

    let run = repository
        .prune_terminal_jobs(JobRetentionPolicy::new(
            TimeDuration::days(1),
            TimeDuration::days(1),
            TimeDuration::days(1),
            64,
            128,
        ))
        .await
        .unwrap();
    assert_eq!(run.stats.pruned, prunable.len() as u64);
    assert!(run.stats.candidates <= 128);
    for id in prunable {
        assert!(
            !sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (SELECT 1 FROM queue.jobs WHERE id = $1)",
            )
            .bind(id)
            .fetch_one(database.pool())
            .await
            .unwrap()
        );
    }
    for id in [
        unknown_upload.id,
        unknown_publish.id,
        recent.id,
        failed_current_cleanup.id,
        actionable_materialize.id,
        pending_caption.id,
        queued.id,
        running.id,
    ] {
        assert!(sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM queue.jobs WHERE id = $1)",
        )
        .bind(id)
        .fetch_one(database.pool())
        .await
        .unwrap());
    }

    database
        .publisher()
        .update_post(
            publish_post,
            PostUpdate {
                caption: Some(Some("recreated after pruning".to_owned())),
                parse_mode: None,
                disable_notification: None,
                expected_updated_at: None,
                expected_revision: 0,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM queue.jobs WHERE dedupe_key = $1",)
            .bind(format!("post:{publish_post}:publish:v1"))
            .fetch_one(database.pool())
            .await
            .unwrap(),
        "failed"
    );
    let requeued = database
        .publisher()
        .schedule_post(
            PostSchedule::try_new(publish_post, OffsetDateTime::now_utc(), "retention-requeue", 1)
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(requeued.state, sooqa_publisher::PostState::Queued);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM queue.jobs WHERE dedupe_key = $1",)
            .bind(format!("post:{publish_post}:publish:v1"))
            .fetch_one(database.pool())
            .await
            .unwrap(),
        "queued"
    );

    let rerun = repository
        .prune_terminal_jobs(JobRetentionPolicy::new(
            TimeDuration::days(1),
            TimeDuration::days(1),
            TimeDuration::days(1),
            64,
            128,
        ))
        .await
        .unwrap();
    assert_eq!(rerun.stats.pruned, 0);
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn terminal_retention_cursor_advances_past_unresolved_prefix(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let repository = database.jobs();
    let mut unresolved = Vec::new();
    for index in 0..3 {
        let media_id: Uuid = sqlx::query_scalar(
            "INSERT INTO media (kind, storage_state) VALUES ('video', 'storage_unknown') RETURNING id",
        )
        .fetch_one(database.pool())
        .await
        .unwrap();
        let job = repository
            .enqueue(
                NewJob::upload_storage_asset_generation(media_id, 0)
                    .dedupe_key(format!("retention-prefix:{index}:{}", Uuid::new_v4())),
            )
            .await
            .unwrap();
        age_terminal_job(database.pool(), job.id, "failed").await;
        unresolved.push(job.id);
    }
    let ready_media: Uuid = sqlx::query_scalar(
        "INSERT INTO media (kind, storage_state, telegram_storage_chat_id, telegram_storage_message_id, telegram_file_id) VALUES ('video', 'ready', -100123499999, 1, 'ready-file') RETURNING id",
    )
    .fetch_one(database.pool())
    .await
    .unwrap();
    let eligible = repository
        .enqueue(
            NewJob::upload_storage_asset_generation(ready_media, 0)
                .dedupe_key(format!("retention-prefix-eligible:{}", Uuid::new_v4())),
        )
        .await
        .unwrap();
    age_terminal_job(database.pool(), eligible.id, "failed").await;
    sqlx::query(
        "UPDATE queue.jobs SET completed_at = now() - interval '3 days' WHERE id = ANY($1)",
    )
    .bind(&unresolved)
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE queue.jobs SET completed_at = now() - interval '2 days' WHERE id = $1")
        .bind(eligible.id)
        .execute(database.pool())
        .await
        .unwrap();

    let policy = JobRetentionPolicy::new(
        TimeDuration::hours(1),
        TimeDuration::hours(1),
        TimeDuration::hours(1),
        1,
        2,
    );
    let first = repository.prune_terminal_jobs(policy).await.unwrap();
    assert_eq!(first.stats.pruned, 0);
    assert!(first.next_cursor.is_some());
    assert!(first.stats.candidates <= 2);
    let second = repository.prune_terminal_jobs_from(policy, first.next_cursor).await.unwrap();
    assert_eq!(second.stats.pruned, 1);
    assert!(second.next_cursor.is_some());
    assert!(
        !sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM queue.jobs WHERE id = $1)",)
            .bind(eligible.id)
            .fetch_one(database.pool())
            .await
            .unwrap()
    );
    for id in unresolved {
        assert!(sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM queue.jobs WHERE id = $1)",
        )
        .bind(id)
        .fetch_one(database.pool())
        .await
        .unwrap());
    }
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn terminal_retention_concurrent_pruners_are_idempotent(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let media_id: Uuid = sqlx::query_scalar(
        "INSERT INTO media (kind, storage_state, telegram_storage_chat_id, telegram_storage_message_id, telegram_file_id) VALUES ('video', 'ready', -100123498888, 1, 'concurrent-file') RETURNING id",
    )
    .fetch_one(database.pool())
    .await
    .unwrap();
    let repository = database.jobs();
    let job = repository
        .enqueue(
            NewJob::upload_storage_asset_generation(media_id, 0)
                .dedupe_key(format!("retention-concurrent:{}", Uuid::new_v4())),
        )
        .await
        .unwrap();
    age_terminal_job(database.pool(), job.id, "succeeded").await;
    let policy = JobRetentionPolicy::new(
        TimeDuration::hours(1),
        TimeDuration::hours(1),
        TimeDuration::hours(1),
        1,
        8,
    );
    let (left, right) = tokio::join!(
        repository.prune_terminal_jobs(policy),
        repository.prune_terminal_jobs(policy)
    );
    assert!(left.is_ok());
    assert!(right.is_ok());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM queue.jobs WHERE id = $1")
            .bind(job.id)
            .fetch_one(database.pool())
            .await
            .unwrap(),
        0
    );
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn terminal_retention_prune_and_claim_do_not_affect_each_other(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let repository = database.jobs();
    let media_id: Uuid = sqlx::query_scalar(
        "INSERT INTO media (kind, storage_state, telegram_storage_chat_id, telegram_storage_message_id, telegram_file_id) VALUES ('video', 'ready', -100123498887, 1, 'prune-claim-file') RETURNING id",
    )
    .fetch_one(database.pool())
    .await
    .unwrap();
    let terminal = repository
        .enqueue(
            NewJob::upload_storage_asset_generation(media_id, 0)
                .dedupe_key(format!("retention-prune-claim-terminal:{}", Uuid::new_v4())),
        )
        .await
        .unwrap();
    age_terminal_job(database.pool(), terminal.id, "succeeded").await;
    let queued = repository
        .enqueue(
            NewJob::upload_storage_asset_generation(media_id, 0)
                .dedupe_key(format!("retention-prune-claim-queued:{}", Uuid::new_v4())),
        )
        .await
        .unwrap();

    let policy = JobRetentionPolicy::new(
        TimeDuration::hours(1),
        TimeDuration::hours(1),
        TimeDuration::hours(1),
        1,
        8,
    );
    let (pruned, claimed) = tokio::join!(
        repository.prune_terminal_jobs(policy),
        repository.claim_next(
            "retention-prune-claim",
            Duration::from_secs(30),
            &[JobType::UploadStorageAsset]
        )
    );
    assert_eq!(pruned.unwrap().stats.pruned, 1);
    assert_eq!(claimed.unwrap().unwrap().id, queued.id);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM queue.jobs WHERE id = $1")
            .bind(terminal.id)
            .fetch_one(database.pool())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM queue.jobs WHERE id = $1")
            .bind(queued.id)
            .fetch_one(database.pool())
            .await
            .unwrap(),
        "running"
    );
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn terminal_retention_stops_at_batch_and_drains_on_next_cursor(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let repository = database.jobs();
    let mut terminal_ids = Vec::new();
    for index in 0..3 {
        let media_id: Uuid = sqlx::query_scalar(
            "INSERT INTO media (kind, storage_state, telegram_storage_chat_id, telegram_storage_message_id, telegram_file_id) VALUES ('video', 'ready', -100123498700, $1, $2) RETURNING id",
        )
        .bind(i64::from(index + 1))
        .bind(format!("batch-file-{index}"))
        .fetch_one(database.pool())
        .await
        .unwrap();
        let job = repository
            .enqueue(
                NewJob::upload_storage_asset_generation(media_id, 0)
                    .dedupe_key(format!("retention-batch-{index}:{}", Uuid::new_v4())),
            )
            .await
            .unwrap();
        age_terminal_job(database.pool(), job.id, "succeeded").await;
        terminal_ids.push(job.id);
    }
    let policy = JobRetentionPolicy::new(
        TimeDuration::hours(1),
        TimeDuration::hours(1),
        TimeDuration::hours(1),
        2,
        8,
    );
    let first = repository.prune_terminal_jobs(policy).await.unwrap();
    assert_eq!(first.stats.pruned, 2);
    assert!(first.next_cursor.is_some());
    assert!(first.stats.candidates <= 8);
    let second = repository.prune_terminal_jobs_from(policy, first.next_cursor).await.unwrap();
    assert_eq!(second.stats.pruned, 1);
    assert!(second.next_cursor.is_none());
    let rerun = repository.prune_terminal_jobs(policy).await.unwrap();
    assert_eq!(rerun.stats.pruned, 0);
    for id in terminal_ids {
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM queue.jobs WHERE id = $1")
                .bind(id)
                .fetch_one(database.pool())
                .await
                .unwrap(),
            0
        );
    }
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn claim_query_uses_live_partial_index(pool: sqlx::PgPool) {
    let mut transaction = pool.begin().await.unwrap();
    sqlx::query("SET LOCAL enable_seqscan = off").execute(&mut *transaction).await.unwrap();
    let plan = sqlx::query_as::<_, (String,)>(
        r#"
        EXPLAIN (COSTS OFF)
        WITH candidate AS (
            SELECT id
            FROM queue.jobs
            WHERE state = 'queued'
              AND run_at <= now()
              AND attempt_count < max_attempts
              AND kind = ANY(ARRAY['upload_storage_asset']::text[])
            ORDER BY priority DESC, run_at ASC, created_at ASC, id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        SELECT id FROM candidate
        "#,
    )
    .fetch_all(&mut *transaction)
    .await
    .unwrap()
    .into_iter()
    .map(|(line,)| line)
    .collect::<Vec<_>>()
    .join("\n");
    assert!(plan.contains("queue_jobs_queued_claim_idx"), "{plan}");

    let broad_claim_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_indexes WHERE schemaname = 'queue' AND indexname = 'queue_jobs_claim_idx'",
    )
    .fetch_one(&mut *transaction)
    .await
    .unwrap();
    assert_eq!(broad_claim_count, 0);
    let indexes = sqlx::query_as::<_, (String, String)>(
        "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'queue' AND indexname IN ('queue_jobs_queued_claim_idx', 'queue_jobs_running_expiry_idx', 'queue_jobs_terminal_retention_idx') ORDER BY indexname",
    )
    .fetch_all(&mut *transaction)
    .await
    .unwrap();
    assert_eq!(indexes.len(), 3);
    for (name, expected_predicate) in [
        ("queue_jobs_queued_claim_idx", "state = 'queued'"),
        ("queue_jobs_running_expiry_idx", "state = 'running'"),
        ("queue_jobs_terminal_retention_idx", "state = ANY"),
    ] {
        let definition = indexes
            .iter()
            .find(|(index_name, _)| index_name == name)
            .map(|(_, definition)| definition)
            .unwrap_or_else(|| panic!("missing {name}"));
        assert!(definition.contains(expected_predicate), "{name}: {definition}");
    }
    transaction.rollback().await.unwrap();
}
