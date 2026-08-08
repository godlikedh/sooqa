use std::env;

use sooqa_inbox::{IngestSubmission, SubmittedVia, TelegramSubmissionInput};
use sooqa_jobs::{JobStatus, JobType, NewJob};
use sooqa_persistence::Database;
use time::OffsetDateTime;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance"]
async fn job_repository_claims_concurrently_and_recovers_leases() {
    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must point to the integration database");
    let database =
        Database::connect(&database_url, 10).await.expect("database should be reachable");
    database.migrate().await.expect("migrations should succeed");
    sqlx::query("DELETE FROM jobs WHERE idempotency_key LIKE 'b2-%'")
        .execute(database.pool())
        .await
        .expect("old B2 test jobs should clean up");
    let jobs = database.jobs();

    let first_key = format!("b2-first-{}", Uuid::new_v4());
    let first = jobs
        .enqueue(NewJob::inspect_source(Uuid::new_v4()).idempotency_key(first_key))
        .await
        .expect("job should enqueue");
    assert_eq!(first.status, JobStatus::Queued);

    let jobs_a = jobs.clone();
    let jobs_b = jobs.clone();
    let (left, right) = tokio::join!(
        jobs_a.claim_next(
            "worker-a",
            std::time::Duration::from_secs(30),
            &[JobType::InspectSource]
        ),
        jobs_b.claim_next(
            "worker-b",
            std::time::Duration::from_secs(30),
            &[JobType::InspectSource]
        ),
    );
    let left = left.expect("first claim should succeed");
    let right = right.expect("second claim should succeed");
    let claimed = match (left, right) {
        (Some(job), None) | (None, Some(job)) => job,
        claims => panic!("expected exactly one claim, got {claims:?}"),
    };
    assert_eq!(claimed.id, first.id);
    assert_eq!(claimed.status, JobStatus::Running);
    assert_eq!(claimed.attempt_count, 1);
    let claiming_worker =
        claimed.lease_owner.clone().expect("claimed job should have a lease owner");

    let heartbeat = jobs
        .heartbeat(claimed.id, &claiming_worker, std::time::Duration::from_secs(60))
        .await
        .expect("heartbeat should renew the lease");
    assert!(heartbeat.lease_expires_at > claimed.lease_expires_at);

    let retried = jobs
        .retry(
            claimed.id,
            &claiming_worker,
            // Keep the immediate retry safely behind the database clock; the
            // caller and PostgreSQL can differ by a few milliseconds.
            OffsetDateTime::now_utc() - time::Duration::seconds(1),
            "temporary_network",
            "upstream unavailable",
        )
        .await;
    let retried = retried.expect("the claiming worker should retry the job");
    assert_eq!(retried.status, JobStatus::RetryWait);

    let claimed_again = jobs
        .claim_next("worker-c", std::time::Duration::from_secs(30), &[JobType::InspectSource])
        .await
        .expect("retry claim should succeed")
        .expect("retried job should be claimable");
    assert_eq!(claimed_again.id, first.id);
    assert_eq!(claimed_again.attempt_count, 2);

    let completed = jobs.complete(claimed_again.id, "worker-c").await.expect("job should complete");
    assert_eq!(completed.status, JobStatus::Succeeded);

    let unsupported = jobs
        .enqueue(
            NewJob::inspect_source(Uuid::new_v4())
                .with_priority(1_000)
                .idempotency_key(format!("b2-capability-inspect-{}", Uuid::new_v4())),
        )
        .await
        .expect("unsupported job should enqueue");
    let supported = jobs
        .enqueue(
            NewJob::cleanup_workspace()
                .with_priority(900)
                .idempotency_key(format!("b2-capability-cleanup-{}", Uuid::new_v4())),
        )
        .await
        .expect("supported job should enqueue");
    let claimed_supported = jobs
        .claim_next(
            "worker-capability",
            std::time::Duration::from_secs(30),
            &[JobType::CleanupWorkspace],
        )
        .await
        .expect("capability claim should succeed")
        .expect("supported job should be claimable");
    assert_eq!(claimed_supported.id, supported.id);
    jobs.complete(supported.id, "worker-capability").await.expect("supported job should complete");
    let claimed_unsupported = jobs
        .claim_next(
            "worker-capability-inspect",
            std::time::Duration::from_secs(30),
            &[JobType::InspectSource],
        )
        .await
        .expect("unsupported job should become claimable")
        .expect("unsupported job should remain queued");
    assert_eq!(claimed_unsupported.id, unsupported.id);
    jobs.complete(unsupported.id, "worker-capability-inspect")
        .await
        .expect("unsupported job should complete when its capability is enabled");

    let second_key = format!("b2-stale-{}", Uuid::new_v4());
    let second = jobs
        .enqueue(
            NewJob::cleanup_workspace()
                .with_priority(100)
                .max_attempts(2)
                .idempotency_key(second_key),
        )
        .await
        .expect("stale job should enqueue");
    let stale_claim = jobs
        .claim_next(
            "worker-stale",
            std::time::Duration::from_secs(30),
            &[JobType::CleanupWorkspace],
        )
        .await
        .expect("stale job should claim");
    assert_eq!(stale_claim.expect("stale job should be present").id, second.id);

    sqlx::query("UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1")
        .bind(second.id)
        .execute(database.pool())
        .await
        .expect("test should expire the lease");

    assert!(jobs.recover_stale_leases().await.expect("stale jobs should recover") >= 1);
    let recovered = jobs
        .claim_next(
            "worker-recovered",
            std::time::Duration::from_secs(30),
            &[JobType::CleanupWorkspace],
        )
        .await
        .expect("recovered job should claim")
        .expect("recovered job should be available");
    assert_eq!(recovered.id, second.id);
    assert_eq!(recovered.attempt_count, 2);
    jobs.complete(recovered.id, "worker-recovered").await.expect("recovered job should complete");

    let third_key = format!("b2-failed-{}", Uuid::new_v4());
    let third = jobs
        .enqueue(NewJob::publish_post(Uuid::new_v4()).with_priority(200).idempotency_key(third_key))
        .await
        .expect("failed job should enqueue");
    let failed_claim = jobs
        .claim_next("worker-fail", std::time::Duration::from_secs(30), &[JobType::PublishPost])
        .await
        .expect("failed job should claim")
        .expect("failed job should be available");
    assert_eq!(failed_claim.id, third.id);
    let failed = jobs
        .fail(third.id, "worker-fail", "invalid_payload", "payload is invalid")
        .await
        .expect("job should fail");
    assert_eq!(failed.status, JobStatus::Failed);
    assert_eq!(failed.last_error_class.as_deref(), Some("invalid_payload"));

    sqlx::query("DELETE FROM jobs WHERE id IN ($1, $2, $3)")
        .bind(first.id)
        .bind(second.id)
        .bind(third.id)
        .execute(database.pool())
        .await
        .expect("test jobs should clean up");
}

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance"]
async fn exhausted_fingerprint_lease_marks_ingest_terminal() {
    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must point to the integration database");
    let database =
        Database::connect(&database_url, 10).await.expect("database should be reachable");
    database.migrate().await.expect("migrations should succeed");
    let key = format!("b2-fingerprint-exhausted-{}", Uuid::new_v4());
    let request = database
        .inbox()
        .create_ingest(
            IngestSubmission::try_new_telegram(TelegramSubmissionInput {
                source_reference: "telegram://42/1999".to_owned(),
                submitted_via: SubmittedVia::TelegramBot,
                submitted_by_admin_id: None,
                original_input: serde_json::json!({"media_kind": "video"}),
                supplied_caption: None,
                idempotency_key: Some(key.clone()),
            })
            .expect("Telegram submission should be valid"),
        )
        .await
        .expect("ingest should be created");
    sqlx::query("DELETE FROM jobs WHERE payload_json->>'ingest_request_id' = $1")
        .bind(request.request.id.to_string())
        .execute(database.pool())
        .await
        .expect("initial ingest job should be removed");
    sqlx::query(
        "UPDATE ingest_requests SET status = 'fingerprinting', original_input = $2, completed_at = NULL WHERE id = $1",
    )
    .bind(request.request.id)
    .bind(serde_json::json!({"normalization": {"media_kind": "video"}}))
    .execute(database.pool())
    .await
    .expect("fingerprinting fixture should be prepared");

    let job = database
        .jobs()
        .enqueue(
            NewJob::compute_fingerprint(request.request.id)
                .max_attempts(1)
                .idempotency_key(format!("{key}-job")),
        )
        .await
        .expect("fingerprint job should be enqueued");
    database
        .jobs()
        .claim_next(
            "worker-fingerprint-exhausted",
            std::time::Duration::from_secs(30),
            &[JobType::ComputeFingerprint],
        )
        .await
        .expect("fingerprint job should be claimable")
        .expect("fingerprint job should be present");
    sqlx::query("UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1")
        .bind(job.id)
        .execute(database.pool())
        .await
        .expect("fingerprint lease should expire");

    database
        .jobs()
        .recover_stale_leases()
        .await
        .expect("exhausted fingerprint lease should recover");
    let job_status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = $1")
        .bind(job.id)
        .fetch_one(database.pool())
        .await
        .expect("failed fingerprint job should remain queryable");
    assert_eq!(job_status, "failed");
    let (request_status, error_code): (String, Option<String>) =
        sqlx::query_as("SELECT status, error_code FROM ingest_requests WHERE id = $1")
            .bind(request.request.id)
            .fetch_one(database.pool())
            .await
            .expect("exhausted fingerprint request should remain queryable");
    assert_eq!(request_status, "failed_terminal");
    assert_eq!(error_code.as_deref(), Some("fingerprint_job_exhausted"));

    sqlx::query("DELETE FROM jobs WHERE id = $1")
        .bind(job.id)
        .execute(database.pool())
        .await
        .expect("fingerprint job should clean up");
    sqlx::query(
        "DELETE FROM idempotency_records WHERE scope = 'ingest:create' AND idempotency_key = $1",
    )
    .bind(&key)
    .execute(database.pool())
    .await
    .expect("ingest idempotency record should clean up");
    sqlx::query("DELETE FROM ingest_requests WHERE id = $1")
        .bind(request.request.id)
        .execute(database.pool())
        .await
        .expect("ingest request should clean up");
}
