use std::{
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use sooqa_inbox::{
    IngestStatus, IngestSubmission, IngestSubmissionInput, SourceInspection, SourceMediaKind,
    SubmittedVia,
};
use sooqa_jobs::{Job, JobType};
use sooqa_media::{
    DownloadError, DownloadLimits, DownloadedSource, MediaWorkspace, SourceDownloader, SourceInput,
    WorkspaceArea,
};
use sooqa_persistence::{Database, JobSettlement};
use sooqa_worker::{download_source_handler, inspect_source_handler};
use tokio::fs;
use uuid::Uuid;

#[derive(Clone)]
struct FakeSourceDownloader {
    outcome: Arc<SourceInspection>,
    calls: Arc<AtomicUsize>,
    download_calls: Arc<AtomicUsize>,
    downloaded_inspections: Arc<Mutex<Vec<SourceInspection>>>,
}

impl FakeSourceDownloader {
    fn successful(inspection: SourceInspection) -> Self {
        Self {
            outcome: Arc::new(inspection),
            calls: Arc::new(AtomicUsize::new(0)),
            download_calls: Arc::new(AtomicUsize::new(0)),
            downloaded_inspections: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }

    fn download_calls(&self) -> usize {
        self.download_calls.load(Ordering::Relaxed)
    }

    fn downloaded_inspections(&self) -> Vec<SourceInspection> {
        self.downloaded_inspections.lock().unwrap().clone()
    }
}

#[async_trait]
impl SourceDownloader for FakeSourceDownloader {
    async fn inspect(&self, _source: &SourceInput) -> Result<SourceInspection, DownloadError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(self.outcome.as_ref().clone())
    }

    async fn download(
        &self,
        inspection: &SourceInspection,
        destination: &Path,
        _limits: &DownloadLimits,
    ) -> Result<DownloadedSource, DownloadError> {
        self.download_calls.fetch_add(1, Ordering::Relaxed);
        self.downloaded_inspections.lock().unwrap().push(inspection.clone());
        let bytes = b"synthetic media bytes";
        fs::write(destination, bytes).await.map_err(|error| {
            DownloadError::terminal("test_download", format!("could not write fixture: {error}"))
        })?;
        Ok(DownloadedSource {
            path: destination.to_owned(),
            bytes: bytes.len() as u64,
            mime_type: inspection.mime_type.clone(),
            selected_format: None,
        })
    }
}

#[tokio::test]
async fn source_inspection_adapter_remains_independent_of_persistence() {
    let inspection = SourceInspection {
        adapter: "test".to_owned(),
        source_url: "https://example.test".to_owned(),
        resolved_url: None,
        media_kind: SourceMediaKind::Video,
        mime_type: None,
        content_length_bytes: None,
        title: None,
        metadata: serde_json::json!({}),
    };
    let downloader = FakeSourceDownloader::successful(inspection.clone());
    let result = downloader
        .inspect(&SourceInput {
            ingest_request_id: Uuid::new_v4(),
            source_url: "https://example.test".to_owned(),
            page_url: None,
        })
        .await
        .unwrap();
    assert_eq!(result, inspection);
    assert_eq!(downloader.calls(), 1);
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn download_uses_matching_durable_inspection_snapshot(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let source_url = format!("https://example.test/matching-{}", Uuid::new_v4());
    let inspection = source_inspection(&source_url);
    let fixture = prepare_download_job(&database, &source_url, inspection.clone()).await;
    let work_root = temp_work_root("matching");
    let job = claim_download_job(&database).await;
    assert_eq!(job.id, fixture.download_job_id);

    let handler = download_source_handler(
        database.inbox(),
        work_root.clone(),
        Arc::new(fixture.downloader.clone()),
        DownloadLimits::default(),
    );
    handler(job.clone()).await.unwrap();

    assert_eq!(fixture.downloader.download_calls(), 1);
    assert_eq!(fixture.downloader.downloaded_inspections(), vec![inspection.clone()]);
    let ingest = database.inbox().find(fixture.ingest_id).await.unwrap().unwrap();
    assert_eq!(ingest.status, IngestStatus::Downloading);
    let data = ingest.input_data().unwrap();
    assert_eq!(data.inspection.as_ref(), Some(&inspection));
    assert_eq!(data.download.as_ref().map(|download| download.bytes), Some(21));
    assert_eq!(count_jobs(&database, fixture.ingest_id, "probe_asset").await, 1);
    assert_eq!(job_state(&database, fixture.inspection_job_id).await, "succeeded");
    assert_eq!(job_state(&database, job.id).await, "succeeded");

    let workspace = MediaWorkspace::create(&work_root, ingest.workspace_id).await.unwrap();
    let source_path = workspace.path(WorkspaceArea::Source, "source.bin").unwrap();
    assert_eq!(fs::read(source_path).await.unwrap(), b"synthetic media bytes");
    fs::remove_dir_all(work_root).await.unwrap();
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn conflicting_download_snapshot_fails_before_workspace_or_adapter_download(
    pool: sqlx::PgPool,
) {
    let database = Database::from_pool(pool);
    let source_url = format!("https://example.test/conflict-{}", Uuid::new_v4());
    let fixture =
        prepare_download_job(&database, &source_url, source_inspection(&source_url)).await;
    let mut payload: serde_json::Value =
        sqlx::query_scalar("SELECT payload FROM queue.jobs WHERE id = $1")
            .bind(fixture.download_job_id)
            .fetch_one(database.pool())
            .await
            .unwrap();
    payload["inspection"]["mime_type"] = serde_json::Value::String("image/jpeg".to_owned());
    sqlx::query("UPDATE queue.jobs SET payload = $2 WHERE id = $1")
        .bind(fixture.download_job_id)
        .bind(payload)
        .execute(database.pool())
        .await
        .unwrap();
    let work_root = temp_work_root("conflicting");
    let job = claim_download_job(&database).await;
    assert_eq!(job.id, fixture.download_job_id);
    let handler = download_source_handler(
        database.inbox(),
        work_root.clone(),
        Arc::new(fixture.downloader.clone()),
        DownloadLimits::default(),
    );

    let failure = handler(job.clone()).await.expect_err("conflicting snapshot must fail");
    assert!(!failure.retryable);
    assert_eq!(failure.class, "invalid_ingest_state");
    assert_eq!(fixture.downloader.download_calls(), 0);
    assert!(!work_root.exists(), "snapshot rejection must not create a workspace");
    let ingest = database.inbox().find(fixture.ingest_id).await.unwrap().unwrap();
    assert_eq!(ingest.status, IngestStatus::FailedTerminal);
    assert_eq!(ingest.error_code.as_deref(), Some("invalid_ingest_state"));
    database
        .jobs()
        .settle_lease(
            &job,
            &job.lease().unwrap(),
            JobSettlement::fail(&failure.class, &failure.message),
        )
        .await
        .unwrap();
    assert_eq!(job_state(&database, job.id).await, "failed");
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn missing_durable_inspection_fails_before_workspace_or_adapter_download(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let source_url = format!("https://example.test/missing-{}", Uuid::new_v4());
    let fixture =
        prepare_download_job(&database, &source_url, source_inspection(&source_url)).await;
    sqlx::query("UPDATE ingests SET input_json = input_json - 'inspection' WHERE id = $1")
        .bind(fixture.ingest_id)
        .execute(database.pool())
        .await
        .unwrap();
    let work_root = temp_work_root("missing");
    let job = claim_download_job(&database).await;
    let handler = download_source_handler(
        database.inbox(),
        work_root.clone(),
        Arc::new(fixture.downloader.clone()),
        DownloadLimits::default(),
    );

    let failure = handler(job.clone()).await.expect_err("missing snapshot must fail");
    assert!(!failure.retryable);
    assert_eq!(failure.class, "invalid_ingest_state");
    assert_eq!(fixture.downloader.download_calls(), 0);
    assert!(!work_root.exists(), "snapshot rejection must not create a workspace");
    let ingest = database.inbox().find(fixture.ingest_id).await.unwrap().unwrap();
    assert_eq!(ingest.status, IngestStatus::FailedTerminal);
    assert_eq!(ingest.error_code.as_deref(), Some("invalid_ingest_state"));
    database
        .jobs()
        .settle_lease(
            &job,
            &job.lease().unwrap(),
            JobSettlement::fail(&failure.class, &failure.message),
        )
        .await
        .unwrap();
    assert_eq!(job_state(&database, job.id).await, "failed");
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn already_advanced_download_job_is_harmless(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let source_url = format!("https://example.test/advanced-{}", Uuid::new_v4());
    let fixture =
        prepare_download_job(&database, &source_url, source_inspection(&source_url)).await;
    sqlx::query("UPDATE ingests SET state = 'completed' WHERE id = $1")
        .bind(fixture.ingest_id)
        .execute(database.pool())
        .await
        .unwrap();
    let work_root = temp_work_root("advanced");
    let job = claim_download_job(&database).await;
    let handler = download_source_handler(
        database.inbox(),
        work_root.clone(),
        Arc::new(fixture.downloader.clone()),
        DownloadLimits::default(),
    );

    handler(job.clone()).await.unwrap();
    assert_eq!(fixture.downloader.download_calls(), 0);
    assert!(!work_root.exists(), "already-advanced jobs must not create a workspace");
    database.jobs().complete_lease(&job.lease().unwrap()).await.unwrap();
    assert_eq!(job_state(&database, job.id).await, "succeeded");
    assert_eq!(
        database.inbox().find(fixture.ingest_id).await.unwrap().unwrap().status,
        IngestStatus::Completed
    );
}

struct PreparedDownloadJob {
    ingest_id: Uuid,
    inspection_job_id: Uuid,
    download_job_id: Uuid,
    downloader: FakeSourceDownloader,
}

async fn prepare_download_job(
    database: &Database,
    source_url: &str,
    inspection: SourceInspection,
) -> PreparedDownloadJob {
    let ingest = database
        .inbox()
        .create_ingest(
            IngestSubmission::try_new(IngestSubmissionInput::new(
                source_url.to_owned(),
                SubmittedVia::Api,
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    let downloader = FakeSourceDownloader::successful(inspection);
    let inspect_handler = inspect_source_handler(database.inbox(), Arc::new(downloader.clone()));
    let inspect_job = database
        .jobs()
        .claim_next("inspection-test", Duration::from_secs(30), &[JobType::InspectSource])
        .await
        .unwrap()
        .expect("new ingest should have a source inspection job");
    inspect_handler(inspect_job.clone()).await.unwrap();
    let download_job_id = sqlx::query_scalar(
        "SELECT id FROM queue.jobs WHERE kind = 'download_source' AND payload->>'ingest_id' = $1",
    )
    .bind(ingest.ingest.id.to_string())
    .fetch_one(database.pool())
    .await
    .unwrap();

    PreparedDownloadJob {
        ingest_id: ingest.ingest.id,
        inspection_job_id: inspect_job.id,
        download_job_id,
        downloader,
    }
}

async fn claim_download_job(database: &Database) -> Job {
    database
        .jobs()
        .claim_next("download-test", Duration::from_secs(30), &[JobType::DownloadSource])
        .await
        .unwrap()
        .expect("source inspection should enqueue a download job")
}

fn source_inspection(source_url: &str) -> SourceInspection {
    SourceInspection {
        adapter: "direct_http".to_owned(),
        source_url: source_url.to_owned(),
        resolved_url: Some("https://cdn.example.test/image.png".to_owned()),
        media_kind: SourceMediaKind::Image,
        mime_type: Some("image/png".to_owned()),
        content_length_bytes: Some(21),
        title: Some("inspection fixture".to_owned()),
        metadata: serde_json::json!({"fixture": true}),
    }
}

fn temp_work_root(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!("sooqa-worker-inspection-{label}-{}", Uuid::new_v4()))
}

async fn count_jobs(database: &Database, ingest_id: Uuid, kind: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM queue.jobs WHERE kind = $2 AND payload->>'ingest_id' = $1",
    )
    .bind(ingest_id.to_string())
    .bind(kind)
    .fetch_one(database.pool())
    .await
    .unwrap()
}

async fn job_state(database: &Database, job_id: Uuid) -> String {
    sqlx::query_scalar("SELECT state FROM queue.jobs WHERE id = $1")
        .bind(job_id)
        .fetch_one(database.pool())
        .await
        .unwrap()
}
