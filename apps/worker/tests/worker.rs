use std::{
    env,
    path::PathBuf,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use sooqa_inbox::{IngestSubmission, SubmittedVia, TelegramSubmissionInput};
use sooqa_jobs::{Job, JobType, NewJob};
use sooqa_media::{
    CommandError, ExternalCommand, ExternalCommandOutput, ExternalCommandRunner, FfprobeAdapter,
    MediaWorkspace, WorkspaceArea,
};
use sooqa_persistence::Database;
use sooqa_worker::{HandlerFuture, HandlerRegistry, Worker, probe_asset_handler};
use tokio::{
    sync::{Mutex, Notify, oneshot},
    time::timeout,
};
use uuid::Uuid;

fn test_handler(_job: Job) -> HandlerFuture {
    Box::pin(async { Ok(()) })
}

static WORKER_INTEGRATION_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

async fn integration_test_lock() -> tokio::sync::MutexGuard<'static, ()> {
    WORKER_INTEGRATION_LOCK.get_or_init(|| Mutex::new(())).lock().await
}

async fn clean_probe_fixtures(database: &Database) {
    sqlx::query(
        r#"
        DELETE FROM jobs
        WHERE job_type = 'probe_asset'
          AND payload_json->>'ingest_request_id' IN (
              SELECT id::text
              FROM ingest_requests
              WHERE source_url IN ('telegram://42/99', 'telegram://42/100')
          )
        "#,
    )
    .execute(database.pool())
    .await
    .expect("old probe fixtures should clean up");
}

#[derive(Clone, Copy)]
struct FakeProbeRunner;

#[async_trait]
impl ExternalCommandRunner for FakeProbeRunner {
    async fn run(&self, _command: ExternalCommand) -> Result<ExternalCommandOutput, CommandError> {
        Ok(ExternalCommandOutput {
            success: true,
            exit_code: Some(0),
            stdout: br#"{"format":{"format_name":"webm","duration":"1.0","size":"10"},"streams":[{"index":0,"codec_type":"video","codec_name":"vp9","width":16,"height":16}]}"#.to_vec(),
            stderr: Vec::new(),
            stdout_truncated: false,
            stderr_truncated: false,
        })
    }
}

#[derive(Clone)]
struct RetryProbeRunner {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl ExternalCommandRunner for RetryProbeRunner {
    async fn run(&self, _command: ExternalCommand) -> Result<ExternalCommandOutput, CommandError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(CommandError::TimedOut {
                program: PathBuf::from("ffprobe"),
                timeout: Duration::from_secs(1),
            });
        }
        FakeProbeRunner.run(ExternalCommand::new("ffprobe")).await
    }
}

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance"]
async fn worker_processes_test_job_and_stops_gracefully() {
    let _test_guard = integration_test_lock().await;
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
            NewJob::cleanup_workspace()
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

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance"]
async fn worker_heartbeats_long_jobs_and_keeps_them_owned() {
    let _test_guard = integration_test_lock().await;
    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must point to the integration database");
    let database =
        Database::connect(&database_url, 10).await.expect("database should be reachable");
    database.migrate().await.expect("migrations should succeed");
    let key = format!("b3-heartbeat-{}", Uuid::new_v4());
    sqlx::query("DELETE FROM jobs WHERE idempotency_key LIKE 'b3-heartbeat-%'")
        .execute(database.pool())
        .await
        .expect("old heartbeat job should clean up");
    let job = database
        .jobs()
        .enqueue(NewJob::publish_post("heartbeat").idempotency_key(key))
        .await
        .expect("heartbeat job should enqueue");

    let release = Arc::new(Notify::new());
    let mut first_registry = HandlerRegistry::new();
    let first_release = Arc::clone(&release);
    first_registry.register(JobType::PublishPost, move |_job| {
        let release = Arc::clone(&first_release);
        Box::pin(async move {
            release.notified().await;
            Ok(())
        })
    });
    let first_worker = Arc::new(
        Worker::new(
            database.jobs(),
            first_registry,
            "worker-b3-heartbeat-first",
            Duration::from_millis(10),
            Duration::from_secs(2),
        )
        .expect("worker timing should be valid"),
    );
    let (first_shutdown_sender, first_shutdown_receiver) = oneshot::channel();
    let first_task = tokio::spawn({
        let worker = Arc::clone(&first_worker);
        async move {
            worker
                .run(async move {
                    let _ = first_shutdown_receiver.await;
                })
                .await
        }
    });

    let claim_wait = timeout(Duration::from_secs(3), async {
        loop {
            let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = $1")
                .bind(job.id)
                .fetch_one(database.pool())
                .await
                .expect("job status should be queryable");
            if status == "running" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    claim_wait.expect("first worker should claim the job");
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    let (owner, lease_is_alive): (String, bool) =
        sqlx::query_as("SELECT lease_owner, lease_expires_at > now() FROM jobs WHERE id = $1")
            .bind(job.id)
            .fetch_one(database.pool())
            .await
            .expect("lease should be queryable");
    assert_eq!(owner, "worker-b3-heartbeat-first");
    assert!(lease_is_alive, "heartbeat should renew a long-running job lease");

    let mut second_registry = HandlerRegistry::new();
    second_registry.register(JobType::PublishPost, test_handler);
    let second_worker = Arc::new(
        Worker::new(
            database.jobs(),
            second_registry,
            "worker-b3-heartbeat-second",
            Duration::from_millis(10),
            Duration::from_secs(2),
        )
        .expect("worker timing should be valid"),
    );
    let (second_shutdown_sender, second_shutdown_receiver) = oneshot::channel();
    let second_task = tokio::spawn({
        let worker = Arc::clone(&second_worker);
        async move {
            worker
                .run(async move {
                    let _ = second_shutdown_receiver.await;
                })
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    let owner_after_second_worker: String =
        sqlx::query_scalar("SELECT lease_owner FROM jobs WHERE id = $1")
            .bind(job.id)
            .fetch_one(database.pool())
            .await
            .expect("lease owner should be queryable");
    assert_eq!(owner_after_second_worker, "worker-b3-heartbeat-first");

    release.notify_one();
    timeout(Duration::from_secs(2), async {
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
    .expect("first worker should complete the job");
    first_shutdown_sender.send(()).expect("first worker should be running");
    second_shutdown_sender.send(()).expect("second worker should be running");
    timeout(Duration::from_secs(2), first_task)
        .await
        .expect("first worker should stop")
        .expect("first worker should not panic")
        .expect("first worker should stop without an error");
    timeout(Duration::from_secs(2), second_task)
        .await
        .expect("second worker should stop")
        .expect("second worker should not panic")
        .expect("second worker should stop without an error");
    sqlx::query("DELETE FROM jobs WHERE idempotency_key LIKE 'b3-heartbeat-%'")
        .execute(database.pool())
        .await
        .expect("heartbeat job should clean up");
}

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance"]
async fn probe_handler_consumes_telegram_media_from_the_shared_workspace() {
    let _test_guard = integration_test_lock().await;
    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must point to the integration database");
    let database =
        Database::connect(&database_url, 10).await.expect("database should be reachable");
    database.migrate().await.expect("migrations should succeed");
    clean_probe_fixtures(&database).await;

    let key_prefix = format!("h4-worker-{}-", Uuid::new_v4());
    let work_root = std::env::temp_dir().join(format!("sooqa-h4-worker-{}", Uuid::new_v4()));
    let workspace_id = Uuid::new_v4();
    let workspace = MediaWorkspace::create(&work_root, workspace_id)
        .await
        .expect("workspace should be created");
    let input_path = workspace
        .path(WorkspaceArea::Source, "telegram-input.bin")
        .expect("workspace source path should be valid");
    tokio::fs::write(&input_path, b"test media").await.expect("test media should be written");

    let submission = IngestSubmission::try_new_telegram(TelegramSubmissionInput {
        source_reference: "telegram://42/99".to_owned(),
        submitted_via: SubmittedVia::TelegramBot,
        submitted_by_admin_id: None,
        original_input: serde_json::json!({
            "telegram_workspace_id": workspace_id,
            "local_work_path": input_path,
            "media_kind": "video",
        }),
        supplied_caption: Some("caption".to_owned()),
        idempotency_key: Some(format!("{}request", key_prefix)),
    })
    .expect("Telegram submission should be valid");
    let created = database
        .inbox()
        .create_ingest(submission)
        .await
        .expect("Telegram ingest should be created");
    sqlx::query("UPDATE jobs SET priority = 100000 WHERE idempotency_key = $1")
        .bind(format!("ingest:{}:probe_asset:v1", created.request.id))
        .execute(database.pool())
        .await
        .expect("probe job should be prioritized");
    let job = database
        .jobs()
        .claim_next("worker-h4-probe", Duration::from_secs(30), &[JobType::ProbeAsset])
        .await
        .expect("probe job should be claimable")
        .expect("probe job should exist");

    let ffprobe = FfprobeAdapter::with_runner(
        "ffprobe",
        Duration::from_secs(1),
        4096,
        Arc::new(FakeProbeRunner),
    );
    let handler = probe_asset_handler(database.inbox(), work_root.clone(), ffprobe);
    handler(job.clone()).await.expect("probe job should succeed");
    database.jobs().complete(job.id, "worker-h4-probe").await.expect("probe job should complete");

    let (status, original_input): (String, serde_json::Value) =
        sqlx::query_as("SELECT status, original_input FROM ingest_requests WHERE id = $1")
            .bind(created.request.id)
            .fetch_one(database.pool())
            .await
            .expect("Telegram ingest should remain queryable");
    assert_eq!(status, "probing");
    assert_eq!(original_input["probe"]["container_format"], "webm");

    sqlx::query("DELETE FROM jobs WHERE payload_json->>'ingest_request_id' = $1 OR id = $2")
        .bind(created.request.id.to_string())
        .bind(job.id)
        .execute(database.pool())
        .await
        .expect("probe job should be cleaned up");
    sqlx::query(
        "DELETE FROM idempotency_records WHERE scope = 'ingest:create' AND idempotency_key LIKE $1",
    )
    .bind(format!("{}%", key_prefix))
    .execute(database.pool())
    .await
    .expect("ingest idempotency record should be cleaned up");
    sqlx::query("DELETE FROM ingest_requests WHERE id = $1")
        .bind(created.request.id)
        .execute(database.pool())
        .await
        .expect("Telegram ingest should be cleaned up");
    workspace.cleanup().await.expect("workspace should be cleaned");
    tokio::fs::remove_dir_all(PathBuf::from(&work_root)).await.ok();
}

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance"]
async fn probe_handler_retries_after_a_retryable_probe_failure() {
    let _test_guard = integration_test_lock().await;
    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must point to the integration database");
    let database =
        Database::connect(&database_url, 10).await.expect("database should be reachable");
    database.migrate().await.expect("migrations should succeed");
    clean_probe_fixtures(&database).await;

    let key_prefix = format!("h4-retry-{}-", Uuid::new_v4());
    let work_root = std::env::temp_dir().join(format!("sooqa-h4-retry-{}", Uuid::new_v4()));
    let workspace_id = Uuid::new_v4();
    let workspace = MediaWorkspace::create(&work_root, workspace_id)
        .await
        .expect("workspace should be created");
    let input_path = workspace
        .path(WorkspaceArea::Source, "telegram-input.bin")
        .expect("workspace source path should be valid");
    tokio::fs::write(&input_path, b"test media").await.expect("test media should be written");

    let submission = IngestSubmission::try_new_telegram(TelegramSubmissionInput {
        source_reference: "telegram://42/100".to_owned(),
        submitted_via: SubmittedVia::TelegramBot,
        submitted_by_admin_id: None,
        original_input: serde_json::json!({
            "telegram_workspace_id": workspace_id,
            "local_work_path": input_path,
            "media_kind": "video",
        }),
        supplied_caption: None,
        idempotency_key: Some(format!("{}request", key_prefix)),
    })
    .expect("Telegram submission should be valid");
    let created = database
        .inbox()
        .create_ingest(submission)
        .await
        .expect("Telegram ingest should be created");
    let job = database
        .jobs()
        .claim_next("worker-h4-retry", Duration::from_secs(30), &[JobType::ProbeAsset])
        .await
        .expect("probe job should be claimable")
        .expect("probe job should exist");

    let calls = Arc::new(AtomicUsize::new(0));
    let ffprobe = FfprobeAdapter::with_runner(
        "ffprobe",
        Duration::from_secs(1),
        4096,
        Arc::new(RetryProbeRunner { calls: Arc::clone(&calls) }),
    );
    let handler = probe_asset_handler(database.inbox(), work_root.clone(), ffprobe);
    let first_error = handler(job.clone()).await.expect_err("first probe should be retryable");
    assert!(first_error.retryable);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM ingest_requests WHERE id = $1")
            .bind(created.request.id)
            .fetch_one(database.pool())
            .await
            .expect("ingest status should be queryable"),
        "failed_retryable"
    );

    database
        .jobs()
        .retry(
            job.id,
            "worker-h4-retry",
            time::OffsetDateTime::now_utc() - time::Duration::seconds(1),
            &first_error.class,
            &first_error.message,
        )
        .await
        .expect("job should enter retry wait");
    let retried_job = database
        .jobs()
        .claim_next("worker-h4-retry", Duration::from_secs(30), &[JobType::ProbeAsset])
        .await
        .expect("retry job should be claimable")
        .expect("retry job should exist");
    handler(retried_job.clone()).await.expect("second probe should succeed");
    database
        .jobs()
        .complete(retried_job.id, "worker-h4-retry")
        .await
        .expect("retried probe job should complete");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM ingest_requests WHERE id = $1")
            .bind(created.request.id)
            .fetch_one(database.pool())
            .await
            .expect("ingest status should be queryable"),
        "probing"
    );

    sqlx::query("DELETE FROM jobs WHERE payload_json->>'ingest_request_id' = $1")
        .bind(created.request.id.to_string())
        .execute(database.pool())
        .await
        .expect("probe jobs should be cleaned up");
    sqlx::query(
        "DELETE FROM idempotency_records WHERE scope = 'ingest:create' AND idempotency_key LIKE $1",
    )
    .bind(format!("{}%", key_prefix))
    .execute(database.pool())
    .await
    .expect("ingest idempotency record should be cleaned up");
    sqlx::query("DELETE FROM ingest_requests WHERE id = $1")
        .bind(created.request.id)
        .execute(database.pool())
        .await
        .expect("Telegram ingest should be cleaned up");
    workspace.cleanup().await.expect("workspace should be cleaned");
    tokio::fs::remove_dir_all(work_root).await.ok();
}
