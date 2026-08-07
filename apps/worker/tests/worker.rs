use std::{env, sync::Arc, time::Duration};

use sooqa_jobs::{Job, JobType, NewJob};
use sooqa_persistence::Database;
use sooqa_worker::{HandlerFuture, HandlerRegistry, Worker};
use tokio::{sync::oneshot, time::timeout};
use uuid::Uuid;

fn test_handler(_job: Job) -> HandlerFuture {
    Box::pin(async { Ok(()) })
}

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance"]
async fn worker_processes_test_job_and_stops_gracefully() {
    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must point to the integration database");
    let database =
        Database::connect(&database_url, 10).await.expect("database should be reachable");
    database.migrate().await.expect("migrations should succeed");
    sqlx::query("DELETE FROM jobs WHERE idempotency_key LIKE 'b3-worker-%'")
        .execute(database.pool())
        .await
        .expect("old B3 test jobs should clean up");
    let jobs = database.jobs();
    let job = jobs
        .enqueue(
            NewJob::new(JobType::CleanupWorkspace, serde_json::json!({}))
                .with_priority(1_000)
                .idempotency_key(format!("b3-worker-{}", Uuid::new_v4())),
        )
        .await
        .expect("test job should enqueue");

    let mut registry = HandlerRegistry::new();
    registry.register(JobType::CleanupWorkspace, test_handler);
    let worker = Arc::new(
        Worker::new(
            jobs.clone(),
            registry,
            "worker-b3-test",
            Duration::from_millis(10),
            Duration::from_secs(30),
        )
        .expect("worker timing should be valid"),
    );
    let metrics = worker.metrics();
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let worker_task = Arc::clone(&worker);
    let task = tokio::spawn(async move {
        worker_task
            .run(async move {
                let _ = shutdown_receiver.await;
            })
            .await
    });

    timeout(Duration::from_secs(3), async {
        loop {
            let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = $1")
                .bind(job.id)
                .fetch_one(database.pool())
                .await
                .expect("job status should be queryable");
            if status == "succeeded" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("worker should complete the test job");

    shutdown_sender.send(()).expect("worker shutdown receiver should be alive");
    timeout(Duration::from_secs(1), task)
        .await
        .expect("worker should stop promptly")
        .expect("worker task should not panic")
        .expect("worker should stop without a repository error");

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.claimed, 1);
    assert_eq!(snapshot.succeeded, 1);
    assert_eq!(snapshot.failed, 0);

    sqlx::query("DELETE FROM jobs WHERE id = $1")
        .bind(job.id)
        .execute(database.pool())
        .await
        .expect("test job should clean up");
}
