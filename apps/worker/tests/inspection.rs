use std::{
    env,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use sooqa_inbox::{
    IngestStatus, IngestSubmission, IngestSubmissionInput, SourceInspection, SourceMediaKind,
    SubmittedVia,
};
use sooqa_jobs::{JobCommand, JobType};
use sooqa_media::{DownloadError, DownloadLimits, DownloadedSource, SourceDownloader, SourceInput};
use sooqa_persistence::Database;
use sooqa_test_support::FakeSourceDownloader;
use sooqa_worker::{download_source_handler, inspect_source_handler};
use tokio::sync::{Barrier, Mutex, Notify};
use uuid::Uuid;

fn submission(url: &str, key: &str) -> IngestSubmission {
    let mut input = IngestSubmissionInput::new(url, SubmittedVia::Api);
    input.idempotency_key = Some(key.to_owned());
    IngestSubmission::try_new(input).expect("submission should be valid")
}

async fn clean_up(database: &Database, key_prefix: &str) {
    sqlx::query(
        r#"
        DELETE FROM jobs
        WHERE payload_json->>'ingest_request_id' IN (
            SELECT id::text
            FROM ingest_requests
            WHERE idempotency_key LIKE $1
        )
        "#,
    )
    .bind(format!("{key_prefix}%"))
    .execute(database.pool())
    .await
    .expect("test jobs should clean up");
    sqlx::query(
        "DELETE FROM idempotency_records WHERE scope = 'ingest:create' AND idempotency_key LIKE $1",
    )
    .bind(format!("{key_prefix}%"))
    .execute(database.pool())
    .await
    .expect("test idempotency records should clean up");
    sqlx::query("DELETE FROM ingest_requests WHERE idempotency_key LIKE $1")
        .bind(format!("{key_prefix}%"))
        .execute(database.pool())
        .await
        .expect("test ingest requests should clean up");
}

#[derive(Clone)]
struct FakeSourcePipeline {
    inspection: SourceInspection,
    inspect_calls: Arc<AtomicUsize>,
    download_calls: Arc<AtomicUsize>,
    download_barrier: Option<Arc<Barrier>>,
    download_started: Option<Arc<Notify>>,
    fail_first_download: bool,
    download_contents: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl FakeSourcePipeline {
    fn new(inspection: SourceInspection) -> Self {
        Self {
            inspection,
            inspect_calls: Arc::new(AtomicUsize::new(0)),
            download_calls: Arc::new(AtomicUsize::new(0)),
            download_barrier: None,
            download_started: None,
            fail_first_download: false,
            download_contents: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn with_download_barrier(mut self, barrier: Arc<Barrier>) -> Self {
        self.download_barrier = Some(barrier);
        self
    }

    fn with_download_started(mut self, started: Arc<Notify>) -> Self {
        self.download_started = Some(started);
        self
    }

    fn fail_first_download(mut self) -> Self {
        self.fail_first_download = true;
        self
    }

    fn with_download_contents(mut self, contents: Vec<Vec<u8>>) -> Self {
        self.download_contents = Arc::new(Mutex::new(contents));
        self
    }
}

#[async_trait]
impl SourceDownloader for FakeSourcePipeline {
    async fn inspect(&self, _source: &SourceInput) -> Result<SourceInspection, DownloadError> {
        self.inspect_calls.fetch_add(1, Ordering::Relaxed);
        Ok(self.inspection.clone())
    }

    async fn download(
        &self,
        inspection: &SourceInspection,
        destination: &Path,
        _limits: &DownloadLimits,
    ) -> Result<DownloadedSource, DownloadError> {
        let call_number = self.download_calls.fetch_add(1, Ordering::Relaxed) + 1;
        if let Some(started) = &self.download_started {
            started.notify_one();
        }
        if let Some(barrier) = &self.download_barrier {
            barrier.wait().await;
        }
        if self.fail_first_download && call_number == 1 {
            return Err(DownloadError::terminal("fake_download", "first attempt failed"));
        }
        let bytes =
            self.download_contents.lock().await.pop().unwrap_or_else(|| b"fake-source".to_vec());
        tokio::fs::write(destination, &bytes).await.map_err(|error| {
            DownloadError::terminal(
                "fake_download",
                format!("could not write fake source: {error}"),
            )
        })?;
        Ok(DownloadedSource {
            path: destination.to_owned(),
            bytes: bytes.len() as u64,
            mime_type: inspection.mime_type.clone(),
        })
    }
}

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance"]
async fn inspect_source_uses_fake_adapter_and_advances_durably() {
    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must point to the integration database");
    let database =
        Database::connect(&database_url, 10).await.expect("database should be reachable");
    database.migrate().await.expect("migrations should succeed");

    let key_prefix = format!("c3-inspection-{}-", Uuid::new_v4());
    clean_up(&database, &key_prefix).await;
    let key = format!("{key_prefix}request");
    let created = database
        .inbox()
        .create_ingest(submission("https://example.com/video", &key))
        .await
        .expect("ingest should be created");

    sqlx::query("UPDATE jobs SET priority = 100000 WHERE idempotency_key = $1")
        .bind(format!("ingest:{}:inspect_source:v1", created.request.id))
        .execute(database.pool())
        .await
        .expect("test inspect job should be prioritized");
    assert_eq!(created.request.status, IngestStatus::Queued);

    let job = database
        .jobs()
        .claim_next("worker-c3-test", Duration::from_secs(30), &[JobType::InspectSource])
        .await
        .expect("inspect job should be claimable")
        .expect("inspect job should exist");
    assert_eq!(job.job_type().as_str(), "inspect_source");

    let fake = FakeSourceDownloader::successful(SourceInspection {
        adapter: "fake".to_owned(),
        source_url: created.request.source_url.clone(),
        resolved_url: Some("https://cdn.example.com/video.mp4".to_owned()),
        media_kind: SourceMediaKind::Video,
        mime_type: Some("video/mp4".to_owned()),
        content_length_bytes: Some(1024),
        title: Some("Fake video".to_owned()),
        metadata: serde_json::json!({"duration_seconds": 2}),
    });
    let handler = inspect_source_handler(database.inbox(), Arc::new(fake.clone()));

    handler(job.clone()).await.expect("source inspection should succeed");
    handler(job.clone()).await.expect("replayed source inspection should be idempotent");
    assert_eq!(fake.calls(), 1);

    database.jobs().complete(job.id, "worker-c3-test").await.expect("inspect job should complete");

    let status: String = sqlx::query_scalar("SELECT status FROM ingest_requests WHERE id = $1")
        .bind(created.request.id)
        .fetch_one(database.pool())
        .await
        .expect("ingest status should be queryable");
    assert_eq!(status, "downloading");

    let (job_type, payload): (String, serde_json::Value) =
        sqlx::query_as("SELECT job_type, payload_json FROM jobs WHERE idempotency_key = $1")
            .bind(format!("ingest:{}:download_source:v1", created.request.id))
            .fetch_one(database.pool())
            .await
            .expect("download job should be durable");
    assert_eq!(job_type, "download_source");
    assert_eq!(payload["inspection"]["adapter"], "fake");
    assert_eq!(payload["inspection"]["mime_type"], "video/mp4");

    clean_up(&database, &key_prefix).await;
}

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance"]
async fn download_source_uses_the_shared_workspace_and_enqueues_probe() {
    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must point to the integration database");
    let database =
        Database::connect(&database_url, 10).await.expect("database should be reachable");
    database.migrate().await.expect("migrations should succeed");

    let key_prefix = format!("c3-download-{}-", Uuid::new_v4());
    clean_up(&database, &key_prefix).await;
    let key = format!("{key_prefix}request");
    let created = database
        .inbox()
        .create_ingest(submission("https://example.com/video", &key))
        .await
        .expect("ingest should be created");

    sqlx::query("UPDATE jobs SET priority = 100000 WHERE idempotency_key = $1")
        .bind(format!("ingest:{}:inspect_source:v1", created.request.id))
        .execute(database.pool())
        .await
        .expect("test inspect job should be prioritized");

    let inspection = SourceInspection {
        adapter: "fake".to_owned(),
        source_url: created.request.source_url.clone(),
        resolved_url: Some("https://cdn.example.com/video.mp4".to_owned()),
        media_kind: SourceMediaKind::Video,
        mime_type: Some("video/mp4".to_owned()),
        content_length_bytes: Some(11),
        title: Some("Fake video".to_owned()),
        metadata: serde_json::json!({"duration_seconds": 2}),
    };
    let fake = FakeSourcePipeline::new(inspection)
        .with_download_barrier(Arc::new(Barrier::new(2)))
        .with_download_contents(vec![b"first".to_vec(), b"second".to_vec()]);
    let inspect_job = database
        .jobs()
        .claim_next("worker-c3-download-test", Duration::from_secs(30), &[JobType::InspectSource])
        .await
        .expect("inspect job should be claimable")
        .expect("inspect job should exist");
    inspect_source_handler(database.inbox(), Arc::new(fake.clone()))(inspect_job.clone())
        .await
        .expect("source inspection should succeed");
    database
        .jobs()
        .complete(inspect_job.id, "worker-c3-download-test")
        .await
        .expect("inspect job should complete");

    sqlx::query("UPDATE jobs SET priority = 100000 WHERE idempotency_key = $1")
        .bind(format!("ingest:{}:download_source:v1", created.request.id))
        .execute(database.pool())
        .await
        .expect("test download job should be prioritized");

    let download_job = database
        .jobs()
        .claim_next("worker-c3-download-test", Duration::from_secs(30), &[JobType::DownloadSource])
        .await
        .expect("download job should be claimable")
        .expect("download job should exist");
    match &download_job.command {
        JobCommand::DownloadSource(payload) => {
            assert_eq!(payload.ingest_request_id, created.request.id);
        }
        command => panic!("expected a download_source command, got {command:?}"),
    }
    let work_root = std::env::temp_dir().join(format!("sooqa-c3-download-{}", Uuid::new_v4()));
    let handler = download_source_handler(
        database.inbox(),
        work_root.clone(),
        Arc::new(fake.clone()),
        DownloadLimits::default(),
    );
    let (first, second) =
        tokio::join!(handler(download_job.clone()), handler(download_job.clone()));
    first.expect("first concurrent source download should succeed");
    second.expect("second concurrent source download should converge");
    database
        .jobs()
        .complete(download_job.id, "worker-c3-download-test")
        .await
        .expect("download job should complete");

    assert_eq!(fake.inspect_calls.load(Ordering::Relaxed), 1);
    assert_eq!(fake.download_calls.load(Ordering::Relaxed), 2);
    let status: String = sqlx::query_scalar("SELECT status FROM ingest_requests WHERE id = $1")
        .bind(created.request.id)
        .fetch_one(database.pool())
        .await
        .expect("ingest status should be queryable");
    assert_eq!(status, "downloading");
    let original_input: serde_json::Value =
        sqlx::query_scalar("SELECT original_input FROM ingest_requests WHERE id = $1")
            .bind(created.request.id)
            .fetch_one(database.pool())
            .await
            .expect("original input should be queryable");
    let recorded_bytes =
        original_input["download"]["bytes"].as_u64().expect("download bytes should be recorded");
    assert!(matches!(recorded_bytes, 5 | 6));
    assert_eq!(original_input["download"]["mime_type"], "video/mp4");
    assert_eq!(original_input["download"]["media_kind"], "video");

    let probe_job_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM jobs WHERE idempotency_key = $1")
            .bind(format!("ingest:{}:probe_asset:v1", created.request.id))
            .fetch_one(database.pool())
            .await
            .expect("probe job count should be queryable");
    assert_eq!(probe_job_count, 1);

    let workspace = sooqa_media::MediaWorkspace::create(&work_root, created.request.id)
        .await
        .expect("source workspace should exist");
    let source_path = workspace
        .path(sooqa_media::WorkspaceArea::Source, "source.bin")
        .expect("source path should be safe");
    let source = tokio::fs::read(source_path).await.expect("source should exist");
    assert!(source == b"first" || source == b"second");
    assert_eq!(recorded_bytes, source.len() as u64);

    clean_up(&database, &key_prefix).await;
    tokio::fs::remove_dir_all(work_root).await.expect("test work root should be removed");
}

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance"]
async fn stale_download_failure_cannot_poison_a_newer_attempt() {
    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must point to the integration database");
    let database =
        Database::connect(&database_url, 10).await.expect("database should be reachable");
    database.migrate().await.expect("migrations should succeed");

    let key_prefix = format!("c3-download-fence-{}-", Uuid::new_v4());
    clean_up(&database, &key_prefix).await;
    let key = format!("{key_prefix}request");
    let created = database
        .inbox()
        .create_ingest(submission("https://example.com/video", &key))
        .await
        .expect("ingest should be created");

    sqlx::query("UPDATE jobs SET priority = 100000 WHERE idempotency_key = $1")
        .bind(format!("ingest:{}:inspect_source:v1", created.request.id))
        .execute(database.pool())
        .await
        .expect("test inspect job should be prioritized");
    let inspection = SourceInspection {
        adapter: "fake".to_owned(),
        source_url: created.request.source_url.clone(),
        resolved_url: Some("https://cdn.example.com/video.mp4".to_owned()),
        media_kind: SourceMediaKind::Video,
        mime_type: Some("video/mp4".to_owned()),
        content_length_bytes: Some(11),
        title: Some("Fake video".to_owned()),
        metadata: serde_json::json!({"duration_seconds": 2}),
    };
    let download_started = Arc::new(Notify::new());
    let fake = FakeSourcePipeline::new(inspection)
        .with_download_barrier(Arc::new(Barrier::new(2)))
        .with_download_started(Arc::clone(&download_started))
        .fail_first_download();
    let inspect_job = database
        .jobs()
        .claim_next("worker-c3-fence-test", Duration::from_secs(30), &[JobType::InspectSource])
        .await
        .expect("inspect job should be claimable")
        .expect("inspect job should exist");
    inspect_source_handler(database.inbox(), Arc::new(fake.clone()))(inspect_job.clone())
        .await
        .expect("source inspection should succeed");
    database
        .jobs()
        .complete(inspect_job.id, "worker-c3-fence-test")
        .await
        .expect("inspect job should complete");

    sqlx::query("UPDATE jobs SET priority = 100000 WHERE idempotency_key = $1")
        .bind(format!("ingest:{}:download_source:v1", created.request.id))
        .execute(database.pool())
        .await
        .expect("test download job should be prioritized");
    let first_job = database
        .jobs()
        .claim_next("worker-c3-stale-download", Duration::from_secs(1), &[JobType::DownloadSource])
        .await
        .expect("first download attempt should be claimable")
        .expect("first download attempt should exist");
    assert_eq!(first_job.attempt_count, 1);
    let work_root =
        std::env::temp_dir().join(format!("sooqa-c3-download-fence-{}", Uuid::new_v4()));
    let handler = download_source_handler(
        database.inbox(),
        work_root.clone(),
        Arc::new(fake.clone()),
        DownloadLimits::default(),
    );
    let first_task = tokio::spawn(handler(first_job.clone()));
    download_started.notified().await;

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    database.jobs().recover_stale_leases().await.expect("stale first attempt should be recovered");
    let second_job = database
        .jobs()
        .claim_next("worker-c3-fresh-download", Duration::from_secs(30), &[JobType::DownloadSource])
        .await
        .expect("second download attempt should be claimable")
        .expect("second download attempt should exist");
    assert_eq!(second_job.id, first_job.id);
    assert_eq!(second_job.attempt_count, 2);

    let second_task = tokio::spawn(handler(second_job.clone()));
    let first_result = first_task.await.expect("first download task should join");
    assert!(first_result.is_err(), "first download attempt should fail");
    second_task
        .await
        .expect("second download task should join")
        .expect("second download attempt should succeed");
    database
        .jobs()
        .complete(second_job.id, "worker-c3-fresh-download")
        .await
        .expect("second download job should complete");

    let status: String = sqlx::query_scalar("SELECT status FROM ingest_requests WHERE id = $1")
        .bind(created.request.id)
        .fetch_one(database.pool())
        .await
        .expect("ingest status should be queryable");
    assert_eq!(status, "downloading");
    let original_input: serde_json::Value =
        sqlx::query_scalar("SELECT original_input FROM ingest_requests WHERE id = $1")
            .bind(created.request.id)
            .fetch_one(database.pool())
            .await
            .expect("original input should be queryable");
    assert_eq!(original_input["download"]["bytes"], 11);
    let probe_job_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM jobs WHERE idempotency_key = $1")
            .bind(format!("ingest:{}:probe_asset:v1", created.request.id))
            .fetch_one(database.pool())
            .await
            .expect("probe job count should be queryable");
    assert_eq!(probe_job_count, 1);

    clean_up(&database, &key_prefix).await;
    tokio::fs::remove_dir_all(work_root).await.expect("test work root should be removed");
}
