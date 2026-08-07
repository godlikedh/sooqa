use std::env;

use sooqa_jobs::{JobStatus, NewJob};
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
        jobs_a.claim_next("worker-a", std::time::Duration::from_secs(30)),
        jobs_b.claim_next("worker-b", std::time::Duration::from_secs(30)),
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
            OffsetDateTime::now_utc(),
            "temporary_network",
            "upstream unavailable",
        )
        .await;
    let retried = retried.expect("the claiming worker should retry the job");
    assert_eq!(retried.status, JobStatus::RetryWait);

    let claimed_again = jobs
        .claim_next("worker-c", std::time::Duration::from_secs(30))
        .await
        .expect("retry claim should succeed")
        .expect("retried job should be claimable");
    assert_eq!(claimed_again.id, first.id);
    assert_eq!(claimed_again.attempt_count, 2);

    let completed = jobs.complete(claimed_again.id, "worker-c").await.expect("job should complete");
    assert_eq!(completed.status, JobStatus::Succeeded);

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
        .claim_next("worker-stale", std::time::Duration::from_secs(30))
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
        .claim_next("worker-recovered", std::time::Duration::from_secs(30))
        .await
        .expect("recovered job should claim")
        .expect("recovered job should be available");
    assert_eq!(recovered.id, second.id);
    assert_eq!(recovered.attempt_count, 2);
    jobs.complete(recovered.id, "worker-recovered").await.expect("recovered job should complete");

    let third_key = format!("b2-failed-{}", Uuid::new_v4());
    let third = jobs
        .enqueue(NewJob::publish_post("example").with_priority(200).idempotency_key(third_key))
        .await
        .expect("failed job should enqueue");
    let failed_claim = jobs
        .claim_next("worker-fail", std::time::Duration::from_secs(30))
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
