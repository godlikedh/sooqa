use std::{env, time::Duration};

use sooqa_inbox::{IngestSubmission, IngestSubmissionInput, SubmittedVia};
use sooqa_jobs::{JobStatus, JobType, NewJob};
use sooqa_persistence::Database;
use time::OffsetDateTime;
use uuid::Uuid;

async fn database() -> Database {
    let url = env::var("DATABASE_URL").expect("DATABASE_URL must point to PostgreSQL");
    let database = Database::connect(&url, 10).await.expect("database should connect");
    database.migrate().await.expect("migration should apply");
    database
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn claim_retry_and_fencing_use_the_queue_jobs_row() {
    let database = database().await;
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
    let bounded = repository
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
    sqlx::query("DELETE FROM queue.jobs WHERE id = $1")
        .bind(job.id)
        .execute(database.pool())
        .await
        .expect("fixture should clean");
    sqlx::query("DELETE FROM queue.jobs WHERE id = $1")
        .bind(bounded.id)
        .execute(database.pool())
        .await
        .expect("bounded fixture should clean");
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn expired_final_attempt_reconciles_ingest_and_fences_all_mutations() {
    let database = database().await;
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

    sqlx::query("DELETE FROM queue.jobs WHERE payload->>'ingest_id' = $1")
        .bind(ingest.ingest.id.to_string())
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM ingests WHERE id = $1")
        .bind(ingest.ingest.id)
        .execute(database.pool())
        .await
        .unwrap();
}
