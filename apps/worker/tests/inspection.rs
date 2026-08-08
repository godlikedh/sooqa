use std::{env, sync::Arc, time::Duration};

use sooqa_inbox::{
    IngestStatus, IngestSubmission, IngestSubmissionInput, SourceInspection, SourceMediaKind,
    SubmittedVia,
};
use sooqa_jobs::JobType;
use sooqa_persistence::Database;
use sooqa_test_support::FakeSourceDownloader;
use sooqa_worker::inspect_source_handler;
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
    assert_eq!(created.request.status, IngestStatus::Queued);

    sqlx::query("UPDATE jobs SET priority = 100000 WHERE idempotency_key = $1")
        .bind(format!("ingest:{}:inspect_source:v1", created.request.id))
        .execute(database.pool())
        .await
        .expect("test inspect job should be prioritized");

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
