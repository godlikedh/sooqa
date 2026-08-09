use std::{env, time::Duration};

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
    let job = repository
        .enqueue(NewJob::cleanup_workspace().dedupe_key(format!("test:{}", Uuid::new_v4())))
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
            NewJob::cleanup_workspace()
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
