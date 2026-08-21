use std::time::Duration;

use serde_json::json;
use sooqa_inbox::{IngestSubmission, IngestSubmissionInput, SubmittedVia};
use sooqa_jobs::{InspectSourcePayload, JobCommand, JobStatus, JobType, NewJob};
use sooqa_persistence::{Database, JobRepositoryError, JobSettlement};
use time::OffsetDateTime;
use uuid::Uuid;

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
