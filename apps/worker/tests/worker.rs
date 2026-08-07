use std::{env, path::PathBuf, sync::Arc, time::Duration};

use async_trait::async_trait;
use sooqa_inbox::{IngestSubmission, SubmittedVia, TelegramSubmissionInput};
use sooqa_jobs::{Job, JobType, NewJob};
use sooqa_media::{
    CommandError, ExternalCommand, ExternalCommandOutput, ExternalCommandRunner, FfprobeAdapter,
    MediaWorkspace, WorkspaceArea,
};
use sooqa_persistence::Database;
use sooqa_worker::{HandlerFuture, HandlerRegistry, Worker, probe_asset_handler};
use tokio::{sync::oneshot, time::timeout};
use uuid::Uuid;

fn test_handler(_job: Job) -> HandlerFuture {
    Box::pin(async { Ok(()) })
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
async fn probe_handler_consumes_telegram_media_from_the_shared_workspace() {
    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must point to the integration database");
    let database =
        Database::connect(&database_url, 10).await.expect("database should be reachable");
    database.migrate().await.expect("migrations should succeed");

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
        .claim_next("worker-h4-probe", Duration::from_secs(30))
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
