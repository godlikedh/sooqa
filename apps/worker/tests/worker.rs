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
    CanonicalVideoProfile, CommandError, ExternalCommand, ExternalCommandOutput,
    ExternalCommandRunner, FfmpegExecutor, FfprobeAdapter, MediaWorkspace, NormalizationPlanner,
    WorkspaceArea,
};
use sooqa_persistence::Database;
use sooqa_worker::{
    HandlerFuture, HandlerRegistry, Worker, finalize_ingest_handler, normalize_asset_handler,
    probe_asset_handler,
};
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
              WHERE source_url IN ('telegram://42/99', 'telegram://42/100', 'telegram://42/101')
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

#[derive(Clone, Copy)]
struct FakeNormalizeRunner;

#[async_trait]
impl ExternalCommandRunner for FakeNormalizeRunner {
    async fn run(&self, command: ExternalCommand) -> Result<ExternalCommandOutput, CommandError> {
        if command.args().iter().any(|argument| argument == "-progress") {
            let output = PathBuf::from(
                command.args().last().expect("ffmpeg output argument should be present"),
            );
            tokio::fs::write(output, b"normalized-media")
                .await
                .expect("fake ffmpeg should write output");
            return Ok(ExternalCommandOutput {
                success: true,
                exit_code: Some(0),
                stdout: b"frame=1\nout_time_ms=1000\nprogress=end\n".to_vec(),
                stderr: Vec::new(),
                stdout_truncated: false,
                stderr_truncated: false,
            });
        }

        Ok(ExternalCommandOutput {
            success: true,
            exit_code: Some(0),
            stdout: br#"{"format":{"format_name":"mov,mp4,m4a,3gp,3g2,mj2","duration":"1.0","size":"16","bit_rate":"1000"},"streams":[{"index":0,"codec_type":"video","codec_name":"h264","pix_fmt":"yuv420p","width":16,"height":16,"avg_frame_rate":"25/1"},{"index":1,"codec_type":"audio","codec_name":"aac","sample_rate":"48000","channels":2}]}"#.to_vec(),
            stderr: Vec::new(),
            stdout_truncated: false,
            stderr_truncated: false,
        })
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
    sqlx::query("UPDATE jobs SET priority = 300000 WHERE idempotency_key = $1")
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
    assert_eq!(status, "normalizing");
    assert_eq!(original_input["probe"]["container_format"], "webm");
    let (normalize_job_type, normalize_payload, normalize_key): (
        String,
        serde_json::Value,
        String,
    ) = sqlx::query_as(
        "SELECT job_type, payload_json, idempotency_key FROM jobs WHERE idempotency_key = $1",
    )
    .bind(format!("ingest:{}:normalize_asset:v1", created.request.id))
    .fetch_one(database.pool())
    .await
    .expect("normalize job should be durable");
    assert_eq!(normalize_job_type, "normalize_asset");
    assert_eq!(normalize_payload["ingest_request_id"], created.request.id.to_string());
    assert_eq!(normalize_key, format!("ingest:{}:normalize_asset:v1", created.request.id));

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
    sqlx::query("UPDATE jobs SET priority = 200000 WHERE idempotency_key = $1")
        .bind(format!("ingest:{}:probe_asset:v1", created.request.id))
        .execute(database.pool())
        .await
        .expect("retry probe job should be prioritized");
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
        "normalizing"
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

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance"]
async fn normalize_handler_executes_ffmpeg_and_enqueues_finalize() {
    let _test_guard = integration_test_lock().await;
    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must point to the integration database");
    let database =
        Database::connect(&database_url, 10).await.expect("database should be reachable");
    database.migrate().await.expect("migrations should succeed");

    let key_prefix = format!("h5-normalize-{}-", Uuid::new_v4());
    let work_root = std::env::temp_dir().join(format!("sooqa-h5-worker-{}", Uuid::new_v4()));
    let workspace_id = Uuid::new_v4();
    let workspace = MediaWorkspace::create(&work_root, workspace_id)
        .await
        .expect("workspace should be created");
    let input_path = workspace
        .path(WorkspaceArea::Source, "telegram-input.bin")
        .expect("workspace source path should be valid");
    tokio::fs::write(&input_path, b"test media").await.expect("test media should be written");

    let submission = IngestSubmission::try_new_telegram(TelegramSubmissionInput {
        source_reference: "telegram://42/101".to_owned(),
        submitted_via: SubmittedVia::TelegramBot,
        submitted_by_admin_id: None,
        original_input: serde_json::json!({
            "telegram_workspace_id": workspace_id,
            "telegram_update_id": 101,
            "telegram_chat_id": 42,
            "telegram_message_id": 101,
            "telegram_file_unique_id": "worker-test-file",
            "file_size": 10,
            "mime_type": "video/webm",
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
    sqlx::query("UPDATE jobs SET priority = 400000 WHERE idempotency_key = $1")
        .bind(format!("ingest:{}:probe_asset:v1", created.request.id))
        .execute(database.pool())
        .await
        .expect("normalization probe job should be prioritized");

    let probe_job = database
        .jobs()
        .claim_next("worker-h5-probe", Duration::from_secs(30), &[JobType::ProbeAsset])
        .await
        .expect("probe job should be claimable")
        .expect("probe job should exist");
    let ffprobe = FfprobeAdapter::with_runner(
        "ffprobe",
        Duration::from_secs(1),
        4096,
        Arc::new(FakeProbeRunner),
    );
    let probe_handler = probe_asset_handler(database.inbox(), work_root.clone(), ffprobe);
    probe_handler(probe_job.clone()).await.expect("probe job should succeed");
    database
        .jobs()
        .complete(probe_job.id, "worker-h5-probe")
        .await
        .expect("probe job should complete");
    sqlx::query("UPDATE jobs SET priority = 400000 WHERE idempotency_key = $1")
        .bind(format!("ingest:{}:normalize_asset:v1", created.request.id))
        .execute(database.pool())
        .await
        .expect("normalize job should be prioritized");

    let normalize_job = database
        .jobs()
        .claim_next("worker-h5-normalize", Duration::from_secs(30), &[JobType::NormalizeAsset])
        .await
        .expect("normalize job should be claimable")
        .expect("normalize job should exist");
    let runner: Arc<dyn ExternalCommandRunner> = Arc::new(FakeNormalizeRunner);
    let ffprobe =
        FfprobeAdapter::with_runner("ffprobe", Duration::from_secs(1), 4096, Arc::clone(&runner));
    let executor = FfmpegExecutor::with_runner(runner, ffprobe, Duration::from_secs(1), 4096);
    let planner = NormalizationPlanner::new("ffmpeg", CanonicalVideoProfile::default())
        .expect("canonical profile should be valid");
    let normalize_handler =
        normalize_asset_handler(database.inbox(), work_root.clone(), planner, executor);
    normalize_handler(normalize_job.clone()).await.expect("normalize job should succeed");
    database
        .jobs()
        .complete(normalize_job.id, "worker-h5-normalize")
        .await
        .expect("normalize job should complete");
    sqlx::query("UPDATE jobs SET priority = 400000 WHERE idempotency_key = $1")
        .bind(format!("ingest:{}:finalize_ingest:v1", created.request.id))
        .execute(database.pool())
        .await
        .expect("finalize job should be prioritized");

    let finalize_job = database
        .jobs()
        .claim_next("worker-h5-finalize", Duration::from_secs(30), &[JobType::FinalizeIngest])
        .await
        .expect("finalize job should be claimable")
        .expect("finalize job should exist");
    let finalize_handler = finalize_ingest_handler(database.inbox(), database.library());
    finalize_handler(finalize_job.clone()).await.expect("finalize job should succeed");
    finalize_handler(finalize_job.clone()).await.expect("finalization replay should be idempotent");
    database
        .jobs()
        .complete(finalize_job.id, "worker-h5-finalize")
        .await
        .expect("finalize job should complete");

    let (status, original_input): (String, serde_json::Value) =
        sqlx::query_as("SELECT status, original_input FROM ingest_requests WHERE id = $1")
            .bind(created.request.id)
            .fetch_one(database.pool())
            .await
            .expect("Telegram ingest should remain queryable");
    assert_eq!(status, "completed");
    assert_eq!(original_input["normalization"]["media_kind"], "video");
    assert_eq!(original_input["normalization"]["file_size_bytes"], 16);
    assert!(!original_input["normalization"]["sha256"].as_str().unwrap_or_default().is_empty());
    assert!(
        original_input["normalization"]["local_work_path"]
            .as_str()
            .is_some_and(|path| path.ends_with("normalized/canonical.mp4"))
    );
    let content_item_id = original_input["finalization"]["content_item_id"]
        .as_str()
        .expect("content item ID should be stored");
    let canonical_asset_id = original_input["finalization"]["canonical_asset_id"]
        .as_str()
        .expect("canonical asset ID should be stored");
    let (content_kind, stored_asset_id, asset_kind, asset_sha256): (String, Uuid, String, Vec<u8>) =
        sqlx::query_as(
            "SELECT ci.kind, ma.id, ma.media_kind, ma.sha256 FROM content_items ci JOIN media_assets ma ON ma.id = ci.canonical_asset_id WHERE ci.id = $1",
        )
        .bind(content_item_id.parse::<Uuid>().expect("content item ID should be a UUID"))
        .fetch_one(database.pool())
        .await
        .expect("canonical library row should be queryable");
    assert_eq!(content_kind, "video");
    assert_eq!(
        stored_asset_id,
        canonical_asset_id.parse::<Uuid>().expect("asset ID should be a UUID")
    );
    assert_eq!(asset_kind, "video");
    assert_eq!(asset_sha256.len(), 32);

    let (source_type, platform, platform_content_id, source_metadata): (
        String,
        Option<String>,
        Option<String>,
        serde_json::Value,
    ) = sqlx::query_as(
        "SELECT source_type, platform, platform_content_id, metadata_json FROM source_records WHERE content_item_id = $1",
    )
    .bind(content_item_id.parse::<Uuid>().expect("content item ID should be a UUID"))
    .fetch_one(database.pool())
    .await
    .expect("source record should be queryable");
    assert_eq!(source_type, "telegram");
    assert_eq!(platform.as_deref(), Some("telegram"));
    assert_eq!(platform_content_id.as_deref(), Some("telegram://42/101"));
    assert_eq!(source_metadata["media_kind"], "video");
    assert_eq!(source_metadata["telegram_file_unique_id"], "worker-test-file");
    assert!(source_metadata.get("local_work_path").is_none());
    assert!(source_metadata.get("telegram_workspace_id").is_none());
    assert!(source_metadata.get("normalization").is_none());

    let canonical_asset_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM media_assets WHERE content_item_id = $1 AND role = 'canonical'",
    )
    .bind(content_item_id.parse::<Uuid>().expect("content item ID should be a UUID"))
    .fetch_one(database.pool())
    .await
    .expect("canonical asset count should be queryable");
    assert_eq!(canonical_asset_count, 1);

    let (finalize_job_type, finalize_payload): (String, serde_json::Value) =
        sqlx::query_as("SELECT job_type, payload_json FROM jobs WHERE idempotency_key = $1")
            .bind(format!("ingest:{}:finalize_ingest:v1", created.request.id))
            .fetch_one(database.pool())
            .await
            .expect("finalize job should be durable");
    assert_eq!(finalize_job_type, "finalize_ingest");
    assert_eq!(finalize_payload["ingest_request_id"], created.request.id.to_string());

    let (storage_job_type, storage_payload): (String, serde_json::Value) =
        sqlx::query_as("SELECT job_type, payload_json FROM jobs WHERE idempotency_key = $1")
            .bind(format!("asset:{canonical_asset_id}:upload_storage:v1:0"))
            .fetch_one(database.pool())
            .await
            .expect("storage upload job should be durable");
    assert_eq!(storage_job_type, "upload_storage_asset");
    assert_eq!(storage_payload["asset_id"], canonical_asset_id);

    let storage_job_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE job_type = 'upload_storage_asset' AND payload_json->>'asset_id' = $1",
    )
    .bind(canonical_asset_id)
    .fetch_one(database.pool())
    .await
    .expect("storage upload job count should be queryable");
    assert_eq!(storage_job_count, 1);

    let normalized_path = original_input["normalization"]["local_work_path"]
        .as_str()
        .expect("normalized path should be stored");
    assert_eq!(
        tokio::fs::read(normalized_path).await.expect("normalized output should exist"),
        b"normalized-media"
    );

    sqlx::query("DELETE FROM jobs WHERE payload_json->>'ingest_request_id' = $1 OR payload_json->>'asset_id' = $2")
        .bind(created.request.id.to_string())
        .bind(canonical_asset_id)
        .execute(database.pool())
        .await
        .expect("ingest jobs should be cleaned up");
    sqlx::query("DELETE FROM idempotency_records WHERE storage_asset_id = $1")
        .bind(canonical_asset_id.parse::<Uuid>().expect("asset ID should be a UUID"))
        .execute(database.pool())
        .await
        .expect("storage idempotency records should be cleaned up");
    sqlx::query("DELETE FROM storage_objects WHERE asset_id = $1")
        .bind(canonical_asset_id.parse::<Uuid>().expect("asset ID should be a UUID"))
        .execute(database.pool())
        .await
        .expect("storage objects should be cleaned up");
    sqlx::query("DELETE FROM source_records WHERE content_item_id = $1")
        .bind(content_item_id.parse::<Uuid>().expect("content item ID should be a UUID"))
        .execute(database.pool())
        .await
        .expect("source record should be cleaned up");
    sqlx::query("UPDATE content_items SET canonical_asset_id = NULL WHERE id = $1")
        .bind(content_item_id.parse::<Uuid>().expect("content item ID should be a UUID"))
        .execute(database.pool())
        .await
        .expect("canonical asset pointer should be cleared");
    sqlx::query("DELETE FROM media_assets WHERE id = $1")
        .bind(canonical_asset_id.parse::<Uuid>().expect("asset ID should be a UUID"))
        .execute(database.pool())
        .await
        .expect("canonical asset should be cleaned up");
    sqlx::query("DELETE FROM content_items WHERE id = $1")
        .bind(content_item_id.parse::<Uuid>().expect("content item ID should be a UUID"))
        .execute(database.pool())
        .await
        .expect("content item should be cleaned up");
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
