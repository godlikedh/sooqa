use std::{env, time::Duration};

use sooqa_inbox::{
    IngestStatus, IngestSubmission, IngestSubmissionInput, SubmittedVia, TelegramSubmissionInput,
};
use sooqa_jobs::JobType;
use sooqa_persistence::{Database, InboxRepositoryError};
use uuid::Uuid;

fn submission(url: &str, key: &str) -> IngestSubmission {
    let mut input = IngestSubmissionInput::new(url, SubmittedVia::Api);
    input.idempotency_key = Some(key.to_owned());
    IngestSubmission::try_new(input).expect("submission should be valid")
}

fn telegram_submission(key: &str) -> IngestSubmission {
    IngestSubmission::try_new_telegram(TelegramSubmissionInput {
        source_reference: "telegram://42/99".to_owned(),
        submitted_via: SubmittedVia::TelegramBot,
        submitted_by_admin_id: None,
        original_input: serde_json::json!({
            "telegram_chat_id": 42,
            "telegram_message_id": 99,
            "telegram_file_unique_id": "unique-file",
            "media_kind": "video",
            "local_work_path": "/tmp/sooqa-telegram-11.bin",
        }),
        supplied_caption: Some("caption".to_owned()),
        idempotency_key: Some(key.to_owned()),
    })
    .expect("Telegram submission should be valid")
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
async fn creates_ingest_and_inspect_job_atomically_with_idempotency() {
    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must point to the integration database");
    let database =
        Database::connect(&database_url, 10).await.expect("database should be reachable");
    database.migrate().await.expect("migrations should succeed");

    let key_prefix = format!("c1-{}-", Uuid::new_v4());
    clean_up(&database, &key_prefix).await;
    let key = format!("{key_prefix}request");
    let inbox = database.inbox();

    let first = inbox
        .create_ingest(submission(
            "HTTPS://Example.COM:443/video?id=123&utm_source=test#fragment",
            &key,
        ))
        .await
        .expect("ingest should be created");
    assert!(first.created);
    assert_eq!(first.request.status, IngestStatus::Queued);
    assert_eq!(first.request.source_url, "https://example.com/video?id=123");

    let (job_type, payload, job_key): (String, serde_json::Value, String) = sqlx::query_as(
        "SELECT job_type, payload_json, idempotency_key FROM jobs WHERE payload_json->>'ingest_request_id' = $1",
    )
    .bind(first.request.id.to_string())
    .fetch_one(database.pool())
    .await
    .expect("inspect job should exist");
    assert_eq!(job_type, "inspect_source");
    assert_eq!(payload["ingest_request_id"], first.request.id.to_string());
    assert_eq!(job_key, format!("ingest:{}:inspect_source:v1", first.request.id));

    let repeated = inbox
        .create_ingest(submission(
            "HTTPS://Example.COM:443/video?id=123&utm_source=test#fragment",
            &key,
        ))
        .await
        .expect("repeated idempotent request should succeed");
    assert!(!repeated.created);
    assert_eq!(repeated.request.id, first.request.id);

    let request_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM ingest_requests WHERE idempotency_key = $1")
            .bind(&key)
            .fetch_one(database.pool())
            .await
            .expect("request count should load");
    assert_eq!(request_count, 1);

    let job_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE payload_json->>'ingest_request_id' = $1",
    )
    .bind(first.request.id.to_string())
    .fetch_one(database.pool())
    .await
    .expect("job count should load");
    assert_eq!(job_count, 1);

    let conflict =
        inbox.create_ingest(submission("https://example.com/a-different-video", &key)).await;
    assert!(matches!(
        conflict,
        Err(InboxRepositoryError::IdempotencyConflict { key: conflict_key })
            if conflict_key == key
    ));

    clean_up(&database, &key_prefix).await;
}

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance"]
async fn creates_telegram_ingest_and_probe_job_atomically() {
    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must point to the integration database");
    let database =
        Database::connect(&database_url, 10).await.expect("database should be reachable");
    database.migrate().await.expect("migrations should succeed");

    let key_prefix = format!("h4-{}-", Uuid::new_v4());
    clean_up(&database, &key_prefix).await;
    let key = format!("{}telegram", key_prefix);
    let first = database
        .inbox()
        .create_ingest(telegram_submission(&key))
        .await
        .expect("Telegram ingest should be created");

    assert!(first.created);
    assert_eq!(first.request.kind.as_str(), "telegram_message");
    assert_eq!(first.request.status, IngestStatus::Queued);
    assert_eq!(first.request.source_url, "telegram://42/99");
    assert_eq!(first.request.supplied_caption.as_deref(), Some("caption"));
    assert_eq!(first.request.original_input["telegram_file_unique_id"], "unique-file");

    let (job_type, payload, job_key): (String, serde_json::Value, String) = sqlx::query_as(
        "SELECT job_type, payload_json, idempotency_key FROM jobs WHERE payload_json->>'ingest_request_id' = $1",
    )
    .bind(first.request.id.to_string())
    .fetch_one(database.pool())
    .await
    .expect("probe job should exist");
    assert_eq!(job_type, "probe_asset");
    assert_eq!(payload["ingest_request_id"], first.request.id.to_string());
    assert_eq!(job_key, format!("ingest:{}:probe_asset:v1", first.request.id));

    let repeated = database
        .inbox()
        .create_ingest(telegram_submission(&key))
        .await
        .expect("repeated Telegram ingest should be idempotent");
    assert!(!repeated.created);
    assert_eq!(repeated.request.id, first.request.id);

    clean_up(&database, &key_prefix).await;
}

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance"]
async fn stale_probe_failure_cannot_poison_a_newer_normalize_handoff() {
    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must point to the integration database");
    let database =
        Database::connect(&database_url, 10).await.expect("database should be reachable");
    database.migrate().await.expect("migrations should succeed");

    let key_prefix = format!("h5-probe-fence-{}-", Uuid::new_v4());
    clean_up(&database, &key_prefix).await;
    let key = format!("{key_prefix}telegram");
    let created = database
        .inbox()
        .create_ingest(telegram_submission(&key))
        .await
        .expect("Telegram ingest should be created");
    sqlx::query("UPDATE jobs SET priority = 100000 WHERE idempotency_key = $1")
        .bind(format!("ingest:{}:probe_asset:v1", created.request.id))
        .execute(database.pool())
        .await
        .expect("probe job should be prioritized");

    let first_job = database
        .jobs()
        .claim_next("worker-h5-stale-probe", Duration::from_secs(1), &[JobType::ProbeAsset])
        .await
        .expect("first probe attempt should be claimable")
        .expect("first probe attempt should exist");
    assert_eq!(first_job.attempt_count, 1);
    let first_attempt = first_job.attempt().expect("claimed job should have an attempt");
    assert!(matches!(
        database
            .inbox()
            .begin_asset_probe(created.request.id, &first_attempt)
            .await
            .expect("first probe should begin"),
        sooqa_persistence::AssetProbeStart::Ready(_)
    ));

    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    database.jobs().recover_stale_leases().await.expect("stale probe attempt should be recovered");
    let second_job = database
        .jobs()
        .claim_next("worker-h5-fresh-probe", Duration::from_secs(30), &[JobType::ProbeAsset])
        .await
        .expect("second probe attempt should be claimable")
        .expect("second probe attempt should exist");
    assert_eq!(second_job.id, first_job.id);
    assert_eq!(second_job.attempt_count, 2);
    let second_attempt = second_job.attempt().expect("reclaimed job should have an attempt");
    assert!(matches!(
        database
            .inbox()
            .begin_asset_probe(created.request.id, &second_attempt)
            .await
            .expect("second probe should begin"),
        sooqa_persistence::AssetProbeStart::Ready(_)
    ));
    database
        .inbox()
        .complete_asset_probe(
            created.request.id,
            &second_attempt,
            serde_json::json!({"container_format": "webm", "size_bytes": 10}),
        )
        .await
        .expect("second probe should enqueue normalization");
    database
        .inbox()
        .fail_asset_probe(
            created.request.id,
            &first_attempt,
            IngestStatus::FailedTerminal,
            "stale_probe",
            "stale attempt must not change the request",
        )
        .await
        .expect("stale failure should be ignored");

    let status: String = sqlx::query_scalar("SELECT status FROM ingest_requests WHERE id = $1")
        .bind(created.request.id)
        .fetch_one(database.pool())
        .await
        .expect("ingest status should be queryable");
    assert_eq!(status, "normalizing");
    let normalize_job_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM jobs WHERE idempotency_key = $1")
            .bind(format!("ingest:{}:normalize_asset:v1", created.request.id))
            .fetch_one(database.pool())
            .await
            .expect("normalize job count should be queryable");
    assert_eq!(normalize_job_count, 1);

    clean_up(&database, &key_prefix).await;
}
