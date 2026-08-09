use std::env;

use serde_json::json;
use sooqa_inbox::{
    IngestFinalization, IngestStatus, IngestSubmission, IngestSubmissionInput, SourceInspection,
    SourceMediaKind, SubmittedVia,
};
use sooqa_jobs::{JobStatus, JobType, NewJob};
use sooqa_library::{
    MediaIngest, MediaKind, MediaMetadata, MediaSourceInput, NewMedia, SourceKind,
};
use sooqa_persistence::{
    Database, InboxRepositoryError, IngestFinalizationStart, IngestFingerprintStart,
    SourceInspectionStart,
};
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

fn video_ingest(sha256: Vec<u8>, source: &str) -> MediaIngest {
    MediaIngest {
        media: NewMedia {
            kind: MediaKind::Video,
            title: Some("ordering test".to_owned()),
            description: None,
            notes: None,
        },
        metadata: MediaMetadata {
            kind: MediaKind::Video,
            mime_type: Some("video/mp4".to_owned()),
            container: Some("mp4".to_owned()),
            video_codec: Some("h264".to_owned()),
            audio_codec: Some("aac".to_owned()),
            width: Some(1),
            height: Some(1),
            duration_ms: Some(1),
            bit_rate: Some(1),
            file_size_bytes: Some(1),
            sha256: Some(sha256),
            local_work_path: Some("/tmp/sooqa-ordering-test.mp4".to_owned()),
        },
        source: MediaSourceInput {
            ingest_id: None,
            kind: SourceKind::DirectUrl,
            original_url: Some(source.to_owned()),
            normalized_url: Some(source.to_owned()),
            platform: None,
            platform_content_id: None,
            author_name: None,
            title: None,
            description: None,
            published_at: None,
            metadata: json!({}),
        },
        tags: Vec::new(),
    }
}

async fn prepare_video_ingest(database: &Database, suffix: &str) -> (Uuid, Uuid) {
    let source = format!("https://example.test/video-{suffix}");
    let media = database
        .library()
        .resolve_media(video_ingest(vec![suffix.as_bytes()[0]; 32], &source))
        .await
        .unwrap();
    let request = database
        .inbox()
        .create_ingest(
            IngestSubmission::try_new(IngestSubmissionInput::new(&source, SubmittedVia::Api))
                .unwrap(),
        )
        .await
        .unwrap();
    sqlx::query(
        "UPDATE ingests SET media_id = $2, state = 'storing', input_json = $3, completed_at = NULL, error_code = NULL, error_message = NULL WHERE id = $1",
    )
    .bind(request.ingest.id)
    .bind(media.media.id)
    .bind(json!({ "normalization": { "media_kind": "video" } }))
    .execute(database.pool())
    .await
    .unwrap();
    (media.media.id, request.ingest.id)
}

async fn advance_video_to_similarity(database: &Database, media_id: Uuid, ingest_id: Uuid) {
    database
        .jobs()
        .enqueue(
            NewJob::finalize_ingest(ingest_id).dedupe_key(format!("test:{ingest_id}:finalize")),
        )
        .await
        .unwrap();
    let finalization_job = database
        .jobs()
        .claim_next(
            "ordering-finalizer",
            std::time::Duration::from_secs(30),
            &[JobType::FinalizeIngest],
        )
        .await
        .unwrap()
        .unwrap();
    let finalization_attempt = finalization_job.lease().unwrap();
    assert!(matches!(
        database.inbox().begin_ingest_finalization(ingest_id, &finalization_attempt).await.unwrap(),
        IngestFinalizationStart::Ready(_)
    ));
    let finalized = database
        .inbox()
        .complete_ingest_finalization(
            ingest_id,
            &finalization_attempt,
            IngestFinalization { media_id },
        )
        .await
        .unwrap();
    assert_eq!(finalized.status, IngestStatus::Fingerprinting);
    database.jobs().complete_lease(&finalization_attempt).await.unwrap();

    let fingerprint_job = database
        .jobs()
        .claim_next(
            "ordering-fingerprinter",
            std::time::Duration::from_secs(30),
            &[JobType::ComputeFingerprint],
        )
        .await
        .unwrap()
        .unwrap();
    let fingerprint_attempt = fingerprint_job.lease().unwrap();
    assert!(matches!(
        database
            .inbox()
            .begin_ingest_fingerprinting(ingest_id, &fingerprint_attempt)
            .await
            .unwrap(),
        IngestFingerprintStart::Ready(_)
    ));
    let fingerprinted = database
        .inbox()
        .complete_ingest_fingerprint(
            ingest_id,
            &fingerprint_attempt,
            Some(json!({ "algorithm": "test" })),
        )
        .await
        .unwrap();
    assert_eq!(fingerprinted.status, IngestStatus::SimilarityCheck);
    database.jobs().complete_lease(&fingerprint_attempt).await.unwrap();
}

async fn complete_similarity(database: &Database, ingest_id: Uuid) {
    let similarity_job = database
        .jobs()
        .claim_next(
            "ordering-similarity",
            std::time::Duration::from_secs(30),
            &[JobType::CheckSimilarity],
        )
        .await
        .unwrap()
        .unwrap();
    let similarity_attempt = similarity_job.lease().unwrap();
    assert!(matches!(
        database.inbox().begin_ingest_similarity(ingest_id, &similarity_attempt).await.unwrap(),
        sooqa_persistence::IngestSimilarityStart::Ready(_)
    ));
    let completed =
        database.inbox().complete_ingest_similarity(ingest_id, &similarity_attempt).await.unwrap();
    assert_eq!(completed.status, IngestStatus::Storing);
    database.jobs().complete_lease(&similarity_attempt).await.unwrap();
}

async fn storage_job_count(database: &Database, media_id: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM queue.jobs WHERE kind = 'upload_storage_asset' AND payload->>'media_id' = $1",
    )
    .bind(media_id.to_string())
    .fetch_one(database.pool())
    .await
    .unwrap()
}

async fn cleanup_video_ingest(database: &Database, media_id: Uuid, ingest_id: Uuid) {
    sqlx::query(
        "DELETE FROM queue.jobs WHERE payload->>'ingest_id' = $1 OR payload->>'media_id' = $2",
    )
    .bind(ingest_id.to_string())
    .bind(media_id.to_string())
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query("DELETE FROM ingests WHERE id = $1")
        .bind(ingest_id)
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM media WHERE id = $1")
        .bind(media_id)
        .execute(database.pool())
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn video_storage_waits_for_similarity_and_reconciles_success_or_failure() {
    let database = database().await;
    let (success_media, success_ingest) = prepare_video_ingest(&database, "success").await;
    let (failure_media, failure_ingest) = prepare_video_ingest(&database, "failure").await;
    advance_video_to_similarity(&database, success_media, success_ingest).await;
    advance_video_to_similarity(&database, failure_media, failure_ingest).await;

    assert_eq!(storage_job_count(&database, success_media).await, 0);
    assert_eq!(storage_job_count(&database, failure_media).await, 0);
    assert_eq!(database.inbox().complete_storage_for_media(success_media).await.unwrap(), 0);
    assert_eq!(
        database
            .inbox()
            .fail_storage_for_media(
                failure_media,
                IngestStatus::FailedRetryable,
                "storage_upload",
                "must wait for similarity",
            )
            .await
            .unwrap(),
        0
    );

    complete_similarity(&database, success_ingest).await;
    assert_eq!(storage_job_count(&database, success_media).await, 1);
    sqlx::query(
        "UPDATE media SET storage_state = 'ready', telegram_storage_chat_id = -100123, telegram_storage_message_id = 9001, telegram_file_id = 'success-file', stored_at = now() WHERE id = $1",
    )
    .bind(success_media)
    .execute(database.pool())
    .await
    .unwrap();
    assert_eq!(database.inbox().complete_storage_for_media(success_media).await.unwrap(), 1);
    assert_eq!(
        database.inbox().find(success_ingest).await.unwrap().unwrap().status,
        IngestStatus::Completed
    );

    complete_similarity(&database, failure_ingest).await;
    assert_eq!(storage_job_count(&database, failure_media).await, 1);
    assert_eq!(
        database
            .inbox()
            .fail_storage_for_media(
                failure_media,
                IngestStatus::FailedTerminal,
                "storage_upload",
                "storage failed after similarity",
            )
            .await
            .unwrap(),
        1
    );
    let failed = database.inbox().find(failure_ingest).await.unwrap().unwrap();
    assert_eq!(failed.status, IngestStatus::FailedTerminal);
    assert_eq!(failed.error_code.as_deref(), Some("storage_upload"));

    cleanup_video_ingest(&database, success_media, success_ingest).await;
    cleanup_video_ingest(&database, failure_media, failure_ingest).await;
}
