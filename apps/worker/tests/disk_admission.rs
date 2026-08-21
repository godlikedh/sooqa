use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use sooqa_inbox::{
    IngestStatus, IngestSubmission, IngestSubmissionInput, SourceDownload, SourceInspection,
    SourceMediaKind, SubmittedVia,
};
use sooqa_jobs::{JobType, NewJob};
use sooqa_media::{DownloadError, DownloadLimits, DownloadedSource, SourceDownloader, SourceInput};
use sooqa_persistence::Database;
use sooqa_worker::{
    HandlerRegistry, Worker, WorkspaceAdmission, download_source_handler_with_admission,
};
use time::OffsetDateTime;
use tokio::{fs, time::sleep};
use uuid::Uuid;

#[derive(Clone, Default)]
struct CountingDownloader {
    download_calls: Arc<AtomicUsize>,
}

impl CountingDownloader {
    fn calls(&self) -> usize {
        self.download_calls.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl SourceDownloader for CountingDownloader {
    async fn inspect(&self, _source: &SourceInput) -> Result<SourceInspection, DownloadError> {
        unreachable!("the test persists synthetic inspection metadata directly")
    }

    async fn download(
        &self,
        _inspection: &SourceInspection,
        destination: &Path,
        _limits: &DownloadLimits,
    ) -> Result<DownloadedSource, DownloadError> {
        self.download_calls.fetch_add(1, Ordering::Relaxed);
        let bytes = b"synthetic-download";
        fs::write(destination, bytes)
            .await
            .map_err(|error| DownloadError::terminal("synthetic_download", error.to_string()))?;
        Ok(DownloadedSource {
            path: destination.to_owned(),
            bytes: bytes.len() as u64,
            mime_type: Some("video/mp4".to_owned()),
            selected_format: None,
        })
    }
}

async fn wait_for_job(
    pool: &sqlx::PgPool,
    job_id: Uuid,
    expected_state: &'static str,
    expected_attempt_count: i32,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let row = sqlx::query_as::<_, (String, i32)>(
            "SELECT state, attempt_count FROM queue.jobs WHERE id = $1",
        )
        .bind(job_id)
        .fetch_one(pool)
        .await
        .expect("job state should be readable");
        if row.0 == expected_state && row.1 == expected_attempt_count {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "job {job_id} did not reach {expected_state}/{expected_attempt_count}; got {row:?}"
        );
        sleep(Duration::from_millis(5)).await;
    }
}

async fn run_download_worker_until(
    database: &Database,
    handler: sooqa_worker::HandlerFn,
    job_id: Uuid,
    expected_state: &'static str,
    expected_attempt_count: i32,
    worker_id: &'static str,
) {
    let mut registry = HandlerRegistry::new();
    registry.register(JobType::DownloadSource, move |job| handler(job));
    let worker = Worker::new(
        database.jobs(),
        registry,
        worker_id,
        Duration::from_millis(5),
        Duration::from_secs(5),
    )
    .expect("worker timing should be valid");
    let pool = database.pool().clone();
    tokio::time::timeout(
        Duration::from_secs(8),
        worker.run(async move {
            wait_for_job(&pool, job_id, expected_state, expected_attempt_count).await;
        }),
    )
    .await
    .expect("worker should reach the expected durable state")
    .expect("worker should stop cleanly");
}

async fn prepare_download_job(
    database: &Database,
    key_prefix: &str,
) -> (sooqa_inbox::Ingest, SourceInspection, Uuid) {
    let ingest = database
        .inbox()
        .create_ingest(
            IngestSubmission::try_new(IngestSubmissionInput::new(
                format!("https://example.test/{key_prefix}-{}", Uuid::new_v4()),
                SubmittedVia::Api,
            ))
            .expect("synthetic URL should validate"),
        )
        .await
        .expect("synthetic ingest should be durable")
        .ingest;
    let inspection = SourceInspection {
        adapter: "synthetic".to_owned(),
        source_url: ingest.source_url.clone(),
        resolved_url: None,
        media_kind: SourceMediaKind::Video,
        mime_type: Some("video/mp4".to_owned()),
        content_length_bytes: Some(18),
        title: Some("synthetic".to_owned()),
        metadata: serde_json::json!({}),
    };
    let inspect_job = database
        .jobs()
        .claim_next("disk-admission-setup", Duration::from_secs(30), &[JobType::InspectSource])
        .await
        .expect("inspect job should claim")
        .expect("inspect job should exist");
    let inspect_lease = inspect_job.lease().expect("inspect claim should be fenced");
    database
        .inbox()
        .begin_source_inspection(ingest.id, &inspect_lease)
        .await
        .expect("inspect stage should begin");
    database
        .inbox()
        .complete_source_inspection(ingest.id, &inspect_lease, inspection.clone())
        .await
        .expect("synthetic inspection should enqueue download");
    let download_job_id: Uuid = sqlx::query_scalar(
        "SELECT id FROM queue.jobs WHERE kind = 'download_source' AND payload->>'ingest_id' = $1",
    )
    .bind(ingest.id.to_string())
    .fetch_one(database.pool())
    .await
    .expect("download job should be durable");
    (ingest, inspection, download_job_id)
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires an ephemeral PostgreSQL test database"]
async fn download_admission_refusal_is_durable_and_recovers_without_stage_mutation(
    pool: sqlx::PgPool,
) {
    let database = Database::from_pool(pool);
    let (ingest, _inspection, download_job_id) =
        prepare_download_job(&database, "disk-admission").await;
    sqlx::query("UPDATE queue.jobs SET max_attempts = 1 WHERE id = $1")
        .bind(download_job_id)
        .execute(database.pool())
        .await
        .expect("synthetic download job should have one attempt");

    let work_root = std::env::temp_dir().join(format!("sooqa-disk-admission-{}", Uuid::new_v4()));
    fs::create_dir_all(&work_root).await.expect("synthetic work root should be writable");
    let downloader = CountingDownloader::default();
    let limits = DownloadLimits { max_bytes: 1, max_redirects: 0, timeout: Duration::from_secs(1) };
    let refused_handler = download_source_handler_with_admission(
        database.inbox(),
        &work_root,
        Arc::new(downloader.clone()),
        limits,
        WorkspaceAdmission::new(u64::MAX),
    );
    run_download_worker_until(
        &database,
        refused_handler,
        download_job_id,
        "queued",
        0,
        "disk-admission-refusal",
    )
    .await;

    let refused_request = database
        .inbox()
        .find(ingest.id)
        .await
        .expect("ingest should remain readable")
        .expect("ingest should remain present");
    assert_eq!(refused_request.status, IngestStatus::Downloading);
    assert!(refused_request.original_input.get("download").is_none());
    assert_eq!(downloader.calls(), 0, "disk refusal must happen before the downloader");
    assert!(!work_root.join("jobs").join(ingest.workspace_id.to_string()).exists());
    let refusal_error: Option<String> =
        sqlx::query_scalar("SELECT error_class FROM queue.jobs WHERE id = $1")
            .bind(download_job_id)
            .fetch_one(database.pool())
            .await
            .expect("deferral error should be persisted");
    assert_eq!(refusal_error.as_deref(), Some("work_disk_low"));

    sqlx::query("UPDATE queue.jobs SET run_at = $2 WHERE id = $1")
        .bind(download_job_id)
        .bind(OffsetDateTime::now_utc())
        .execute(database.pool())
        .await
        .expect("capacity recovery should make the deferred job eligible");
    let admitted_handler = download_source_handler_with_admission(
        database.inbox(),
        &work_root,
        Arc::new(downloader.clone()),
        limits,
        WorkspaceAdmission::new(0),
    );
    run_download_worker_until(
        &database,
        admitted_handler,
        download_job_id,
        "succeeded",
        1,
        "disk-admission-recovery",
    )
    .await;

    let recovered_request = database
        .inbox()
        .find(ingest.id)
        .await
        .expect("recovered ingest should be readable")
        .expect("recovered ingest should remain present");
    assert_eq!(recovered_request.status, IngestStatus::Downloading);
    assert!(recovered_request.original_input.get("download").is_some());
    assert_eq!(downloader.calls(), 1, "admitted retry should reach the downloader once");

    fs::remove_dir_all(&work_root).await.expect("synthetic work root should be removable");
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires an ephemeral PostgreSQL test database"]
async fn already_advanced_download_skips_admission_and_large_work(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let (ingest, inspection, initial_job_id) =
        prepare_download_job(&database, "already-advanced").await;
    let initial_job = database
        .jobs()
        .claim_next("already-advanced-setup", Duration::from_secs(30), &[JobType::DownloadSource])
        .await
        .expect("initial download job should claim")
        .expect("initial download job should exist");
    assert_eq!(initial_job.id, initial_job_id);
    let initial_lease = initial_job.lease().expect("initial claim should be fenced");
    database
        .inbox()
        .begin_source_download(ingest.id, &initial_lease)
        .await
        .expect("initial download stage should begin");
    database
        .inbox()
        .complete_source_download(
            ingest.id,
            &initial_lease,
            SourceDownload {
                bytes: 18,
                mime_type: Some("video/mp4".to_owned()),
                media_kind: SourceMediaKind::Video,
                selected_format: None,
            },
        )
        .await
        .expect("synthetic download should advance the durable input");

    let duplicate = database
        .jobs()
        .enqueue(
            NewJob::download_source(ingest.id, inspection)
                .dedupe_key(format!("already-advanced:{}", Uuid::new_v4())),
        )
        .await
        .expect("duplicate download job should be durable");
    let work_root = std::env::temp_dir().join(format!("sooqa-already-advanced-{}", Uuid::new_v4()));
    fs::create_dir_all(&work_root).await.expect("synthetic work root should be writable");
    let downloader = CountingDownloader::default();
    let handler = download_source_handler_with_admission(
        database.inbox(),
        &work_root,
        Arc::new(downloader.clone()),
        DownloadLimits { max_bytes: 1, max_redirects: 0, timeout: Duration::from_secs(1) },
        WorkspaceAdmission::new(u64::MAX),
    );
    run_download_worker_until(
        &database,
        handler,
        duplicate.id,
        "succeeded",
        1,
        "already-advanced-worker",
    )
    .await;

    let request = database
        .inbox()
        .find(ingest.id)
        .await
        .expect("advanced ingest should remain readable")
        .expect("advanced ingest should remain present");
    assert_eq!(request.status, IngestStatus::Downloading);
    assert!(request.original_input.get("download").is_some());
    assert_eq!(downloader.calls(), 0, "already-advanced jobs must not start large work");
    assert!(!work_root.join("jobs").join(ingest.workspace_id.to_string()).exists());

    fs::remove_dir_all(&work_root).await.expect("synthetic work root should be removable");
}
