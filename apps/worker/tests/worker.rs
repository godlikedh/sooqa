use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use sooqa_jobs::{Job, JobStatus, JobType, NewJob};
use sooqa_persistence::Database;
use sooqa_worker::{HandlerRegistry, StoragePreflight, Worker, spawn_storage_preflight};
use thiserror::Error;
use time::OffsetDateTime;
use tokio::{
    sync::{Notify, oneshot},
    time::timeout,
};
use uuid::Uuid;

#[test]
fn registry_is_typed_by_job_kind() {
    let mut registry = HandlerRegistry::new();
    registry.register(JobType::CleanupWorkspace, |_job| Box::pin(async { Ok(()) }));
    assert!(registry.contains(JobType::CleanupWorkspace));
    assert_eq!(registry.job_types(), vec![JobType::CleanupWorkspace]);
}

#[test]
fn job_envelopes_have_typed_payloads_and_fenced_leases() {
    let new_job = NewJob::publish_post(Uuid::new_v4(), 0);
    let now = OffsetDateTime::now_utc();
    let job = Job {
        id: Uuid::new_v4(),
        command: new_job.command().clone(),
        status: JobStatus::Running,
        priority: 0,
        run_at: now,
        attempt_count: 1,
        max_attempts: 3,
        lease_token: Some(Uuid::new_v4()),
        lease_owner: Some("worker".to_owned()),
        lease_expires_at: Some(now),
        last_heartbeat_at: Some(now),
        last_error_class: None,
        last_error_message: None,
        dedupe_key: None,
        created_at: now,
        updated_at: now,
        completed_at: None,
    };
    assert_eq!(job.job_type(), JobType::PublishPost);
    assert_eq!(job.lease().unwrap().worker_id, "worker");
}

#[derive(Debug, Error)]
#[error("synthetic Telegram storage outage")]
struct StorageOutage;

#[derive(Clone)]
struct BlockingStorageProbe {
    started: Arc<Notify>,
    release: Arc<Notify>,
}

#[async_trait]
impl StoragePreflight for BlockingStorageProbe {
    type Error = StorageOutage;

    async fn verify_storage_chat(&self) -> Result<(), Self::Error> {
        self.started.notify_one();
        self.release.notified().await;
        Err(StorageOutage)
    }

    fn is_terminal_configuration(_error: &Self::Error) -> bool {
        false
    }
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn worker_claims_non_storage_job_while_storage_preflight_fails(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    database
        .jobs()
        .enqueue(NewJob::cleanup_workspace(Uuid::new_v4(), Uuid::new_v4()))
        .await
        .expect("non-storage job should be queued");

    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let preflight_task = spawn_storage_preflight(
        BlockingStorageProbe { started: Arc::clone(&started), release: Arc::clone(&release) },
        -100123,
    );
    timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("storage preflight should run independently");

    let handled = Arc::new(Notify::new());
    let mut registry = HandlerRegistry::new();
    let handled_by_job = Arc::clone(&handled);
    registry.register(JobType::CleanupWorkspace, move |_job| {
        let handled = Arc::clone(&handled_by_job);
        Box::pin(async move {
            handled.notify_one();
            Ok(())
        })
    });
    let worker = Worker::new(
        database.jobs(),
        registry,
        "telegram-outage-test-worker",
        Duration::from_millis(5),
        Duration::from_secs(5),
    )
    .expect("worker timing should be valid");
    let (stop, stop_signal) = oneshot::channel();
    let worker_task = tokio::spawn(async move {
        worker
            .run(async move {
                let _ = stop_signal.await;
            })
            .await
    });

    timeout(Duration::from_secs(2), handled.notified())
        .await
        .expect("non-storage job should run while Telegram is unavailable");
    release.notify_one();
    preflight_task.await.expect("preflight task should not panic");
    stop.send(()).expect("worker should still be running");
    worker_task.await.expect("worker task should not panic").expect("worker should stop cleanly");

    let state: String = sqlx::query_scalar("SELECT state FROM queue.jobs LIMIT 1")
        .fetch_one(database.pool())
        .await
        .expect("queued job state should be readable");
    assert_eq!(state, "succeeded");
}
