use std::env;

use serde_json::json;
use sooqa_inbox::{
    IngestStatus, IngestSubmission, IngestSubmissionInput, SourceInspection, SourceMediaKind,
    SubmittedVia,
};
use sooqa_jobs::{JobStatus, JobType};
use sooqa_library::VideoIdentityOutcome;
use sooqa_persistence::{Database, InboxRepositoryError, SourceInspectionStart};
use uuid::Uuid;

async fn database() -> Database {
    let url = env::var("DATABASE_URL").expect("DATABASE_URL must point to PostgreSQL");
    let database = Database::connect(&url, 10).await.expect("database should connect");
    database.migrate().await.expect("migration should apply");
    database
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn input_key_replays_identical_ingests_and_rejects_conflicts() {
    let database = database().await;
    let key = format!("test-ingest-{}", Uuid::new_v4());
    let mut input = IngestSubmissionInput::new("https://example.test/video", SubmittedVia::Api);
    input.idempotency_key = Some(key.clone());
    let first = database
        .inbox()
        .create_ingest(IngestSubmission::try_new(input.clone()).unwrap())
        .await
        .unwrap();
    let replay =
        database.inbox().create_ingest(IngestSubmission::try_new(input).unwrap()).await.unwrap();
    assert_eq!(first.ingest.id, replay.ingest.id);
    assert!(!replay.created);

    let mut conflicting =
        IngestSubmissionInput::new("https://example.test/other", SubmittedVia::Api);
    conflicting.idempotency_key = Some(key);
    let error = database
        .inbox()
        .create_ingest(IngestSubmission::try_new(conflicting).unwrap())
        .await
        .unwrap_err();
    assert!(matches!(error, InboxRepositoryError::IdempotencyConflict { .. }));
    let claimed = database
        .jobs()
        .claim_next(
            "stale-inspection-worker",
            std::time::Duration::from_secs(30),
            &[JobType::InspectSource],
        )
        .await
        .unwrap()
        .expect("inspect job should be claimable");
    let stale_attempt = claimed.lease().expect("claimed job should have a lease");
    database
        .jobs()
        .retry_lease(
            &stale_attempt,
            time::OffsetDateTime::now_utc(),
            "test_retry",
            "simulated retry",
        )
        .await
        .unwrap();
    assert!(matches!(
        database.inbox().begin_source_inspection(first.ingest.id, &stale_attempt).await.unwrap(),
        SourceInspectionStart::AlreadyAdvanced(_)
    ));
    sqlx::query("DELETE FROM queue.jobs WHERE payload->>'ingest_id' = $1")
        .bind(first.ingest.id.to_string())
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM ingests WHERE id = $1")
        .bind(first.ingest.id)
        .execute(database.pool())
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn source_inspection_completion_commits_job_success_with_transition() {
    let database = database().await;
    let ingest = database
        .inbox()
        .create_ingest(
            IngestSubmission::try_new(IngestSubmissionInput::new(
                format!("https://example.test/inspect-{}", Uuid::new_v4()),
                SubmittedVia::Api,
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    sqlx::query(
        "UPDATE queue.jobs SET max_attempts = 1, priority = 2147483647 WHERE kind = 'inspect_source' AND payload->>'ingest_id' = $1",
    )
    .bind(ingest.ingest.id.to_string())
    .execute(database.pool())
    .await
    .unwrap();

    let claimed = database
        .jobs()
        .claim_next(
            "inspect-success-worker",
            std::time::Duration::from_secs(30),
            &[JobType::InspectSource],
        )
        .await
        .unwrap()
        .expect("inspect job should be claimable");
    let lease = claimed.lease().expect("claimed job should have a lease");
    let completed = database
        .inbox()
        .complete_source_inspection(
            ingest.ingest.id,
            &lease,
            SourceInspection {
                adapter: "test".to_owned(),
                source_url: "https://example.test/inspected.webm".to_owned(),
                resolved_url: None,
                media_kind: SourceMediaKind::Video,
                mime_type: Some("video/webm".to_owned()),
                content_length_bytes: Some(1),
                title: None,
                metadata: json!({}),
            },
        )
        .await
        .unwrap();
    assert_eq!(completed.status, IngestStatus::Downloading);

    let job_state = sqlx::query_scalar::<_, String>("SELECT state FROM queue.jobs WHERE id = $1")
        .bind(claimed.id)
        .fetch_one(database.pool())
        .await
        .unwrap();
    assert_eq!(job_state, JobStatus::Succeeded.as_str());
    let successor_count = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM queue.jobs WHERE kind = 'download_source' AND state = 'queued' AND payload->>'ingest_id' = $1",
    )
    .bind(ingest.ingest.id.to_string())
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(successor_count, 1);

    let acknowledged = database.jobs().complete_lease(&lease).await.unwrap();
    assert_eq!(acknowledged.status, JobStatus::Succeeded);

    database.jobs().recover_stale_leases().await.unwrap();
    let request = database.inbox().find(ingest.ingest.id).await.unwrap().unwrap();
    assert_eq!(request.status, IngestStatus::Downloading);
    let successor_count = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM queue.jobs WHERE kind = 'download_source' AND state = 'queued' AND payload->>'ingest_id' = $1",
    )
    .bind(ingest.ingest.id.to_string())
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(successor_count, 1);

    sqlx::query("DELETE FROM queue.jobs WHERE payload->>'ingest_id' = $1")
        .bind(ingest.ingest.id.to_string())
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM ingests WHERE id = $1")
        .bind(ingest.ingest.id)
        .execute(database.pool())
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn duplicate_pending_force_save_is_durable_and_idempotent() {
    let database = database().await;
    let ingest = database
        .inbox()
        .create_ingest(
            IngestSubmission::try_new(IngestSubmissionInput::new(
                format!("https://example.test/duplicate-{}", Uuid::new_v4()),
                SubmittedVia::Api,
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    sqlx::query(
        "UPDATE ingests SET state = 'duplicate_pending', duplicate_evidence = $2 WHERE id = $1",
    )
    .bind(ingest.ingest.id)
    .bind(json!({"algorithm_version": "video_sequence_v1", "matches": []}))
    .execute(database.pool())
    .await
    .unwrap();

    let resumed = database.inbox().force_save(ingest.ingest.id).await.unwrap();
    assert!(resumed.resumed);
    assert_eq!(resumed.ingest.status, IngestStatus::Normalizing);
    assert!(resumed.ingest.force_save);
    assert!(resumed.ingest.duplicate_evidence.is_none());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM queue.jobs WHERE kind = 'normalize_asset' AND payload->>'ingest_id' = $1",
        )
        .bind(ingest.ingest.id.to_string())
        .fetch_one(database.pool())
        .await
        .unwrap(),
        1
    );

    let replay = database.inbox().force_save(ingest.ingest.id).await.unwrap();
    assert!(!replay.resumed);
    assert_eq!(replay.ingest.id, resumed.ingest.id);
    assert_eq!(replay.ingest.status, IngestStatus::Normalizing);

    sqlx::query("DELETE FROM queue.jobs WHERE payload->>'ingest_id' = $1")
        .bind(ingest.ingest.id.to_string())
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM ingests WHERE id = $1")
        .bind(ingest.ingest.id)
        .execute(database.pool())
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn video_identity_completion_persists_duplicate_pending_and_fences_job() {
    let database = database().await;
    let ingest = database
        .inbox()
        .create_ingest(
            IngestSubmission::try_new(IngestSubmissionInput::new(
                format!("https://example.test/pending-{}", Uuid::new_v4()),
                SubmittedVia::Api,
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    sqlx::query("UPDATE ingests SET state = 'fingerprinting' WHERE id = $1")
        .bind(ingest.ingest.id)
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM queue.jobs WHERE payload->>'ingest_id' = $1")
        .bind(ingest.ingest.id.to_string())
        .execute(database.pool())
        .await
        .unwrap();
    let job = database
        .jobs()
        .enqueue(
            sooqa_jobs::NewJob::compute_fingerprint(ingest.ingest.id)
                .with_priority(i32::MAX)
                .dedupe_key(format!("test:fingerprint:{}", ingest.ingest.id)),
        )
        .await
        .unwrap();
    let claimed = database
        .jobs()
        .claim_next(
            "duplicate-pending-worker",
            std::time::Duration::from_secs(30),
            &[JobType::ComputeFingerprint],
        )
        .await
        .unwrap()
        .expect("fingerprint job should be claimable");
    assert_eq!(claimed.id, job.id);
    let attempt = claimed.lease().unwrap();
    let completed = database
        .inbox()
        .complete_video_identity(
            ingest.ingest.id,
            &attempt,
            VideoIdentityOutcome::DuplicatePending {
                evidence: sooqa_library::VideoDuplicateEvidence {
                    algorithm_version: "video_sequence_v1".to_owned(),
                    matches: Vec::new(),
                },
            },
        )
        .await
        .unwrap();
    assert_eq!(completed.status, IngestStatus::DuplicatePending);
    assert!(completed.duplicate_evidence.is_some());
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM queue.jobs WHERE id = $1")
            .bind(claimed.id)
            .fetch_one(database.pool())
            .await
            .unwrap(),
        JobStatus::Succeeded.as_str()
    );

    sqlx::query("DELETE FROM queue.jobs WHERE id = $1")
        .bind(claimed.id)
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM ingests WHERE id = $1")
        .bind(ingest.ingest.id)
        .execute(database.pool())
        .await
        .unwrap();
}
