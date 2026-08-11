use std::{env, sync::OnceLock};

use serde_json::json;
use sooqa_inbox::{
    IngestStatus, IngestSubmission, IngestSubmissionInput, SourceInspection, SourceMediaKind,
    SubmittedVia,
};
use sooqa_jobs::{JobStatus, JobType};
use sooqa_library::{
    MediaIngest, MediaKind, MediaMetadata, MediaSourceInput, NewMedia, SourceKind,
};
use sooqa_media::{SequenceAlignmentConfig, VideoSequenceFingerprint, VideoSequenceSample};
use sooqa_persistence::{Database, InboxRepositoryError, SourceInspectionStart};
use uuid::Uuid;

async fn database() -> Database {
    let url = env::var("DATABASE_URL").expect("DATABASE_URL must point to PostgreSQL");
    let database = Database::connect(&url, 10).await.expect("database should connect");
    database.migrate().await.expect("migration should apply");
    database
}

// These tests share the CI database; a test claiming "any inspect_source" job
// must not steal the job another test just created.
static TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

fn test_lock() -> &'static tokio::sync::Mutex<()> {
    TEST_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn input_key_replays_identical_ingests_and_rejects_conflicts() {
    let _guard = test_lock().lock().await;
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
    let _guard = test_lock().lock().await;
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
    let _guard = test_lock().lock().await;
    let database = database().await;
    let mut submission_input = IngestSubmissionInput::new(
        format!("https://example.test/duplicate-{}", Uuid::new_v4()),
        SubmittedVia::Companion,
    );
    submission_input.supplied_description = Some("keep this internal note".to_owned());
    submission_input.supplied_tags = vec!["cats".to_owned(), "reaction".to_owned()];
    let ingest = database
        .inbox()
        .create_ingest(IngestSubmission::try_new(submission_input).unwrap())
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
    sqlx::query(
        "UPDATE queue.jobs SET state = 'succeeded', completed_at = now() WHERE kind = 'inspect_source' AND payload->>'ingest_id' = $1",
    )
        .bind(ingest.ingest.id.to_string())
        .execute(database.pool())
        .await
        .unwrap();

    let inbox = database.inbox();
    let (left, right) =
        tokio::join!(inbox.force_save(ingest.ingest.id), inbox.force_save(ingest.ingest.id),);
    let left = left.unwrap();
    let right = right.unwrap();
    assert_ne!(left.resumed, right.resumed);
    let resumed = if left.resumed { left } else { right };
    assert_eq!(resumed.ingest.status, IngestStatus::Queued);
    assert!(resumed.ingest.force_save);
    assert!(resumed.ingest.duplicate_evidence.is_none());
    assert_eq!(resumed.ingest.supplied_description.as_deref(), Some("keep this internal note"));
    assert_eq!(resumed.ingest.supplied_tags, ["cats", "reaction"]);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM queue.jobs WHERE kind = 'inspect_source' AND payload->>'ingest_id' = $1",
        )
        .bind(ingest.ingest.id.to_string())
        .fetch_one(database.pool())
        .await
        .unwrap(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM queue.jobs WHERE kind = 'inspect_source' AND state = 'queued' AND payload->>'ingest_id' = $1",
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
    assert_eq!(replay.ingest.status, IngestStatus::Queued);

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
async fn stale_video_identity_finalizer_cannot_mutate_after_lease_recovery() {
    let _guard = test_lock().lock().await;
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
    let stale_attempt = claimed.lease().unwrap();
    // Model extraction having completed before the worker discovers its lease expired.
    let media_ingest = test_video_ingest(ingest.ingest.id, vec![91_u8; 32]);
    let fingerprint = test_video_sequence(0x1234_5678_9abc_def0);
    sqlx::query(
        "UPDATE queue.jobs SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE id = $1",
    )
    .bind(claimed.id)
    .execute(database.pool())
    .await
    .unwrap();
    database.jobs().recover_stale_leases().await.unwrap();

    let stale_result = database
        .inbox()
        .finalize_video_identity(
            ingest.ingest.id,
            &stale_attempt,
            media_ingest.clone(),
            Some(&fingerprint),
            SequenceAlignmentConfig::default(),
        )
        .await
        .unwrap();
    assert_eq!(stale_result.status, IngestStatus::Fingerprinting);
    assert!(stale_result.media_id.is_none());
    assert!(stale_result.duplicate_evidence.is_none());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM media WHERE canonical_sha256 = $1")
            .bind(vec![91_u8; 32])
            .fetch_one(database.pool())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM queue.jobs WHERE kind = 'upload_storage_asset' AND payload->>'media_id' IN (SELECT id::text FROM media WHERE canonical_sha256 = $1)",
        )
        .bind(vec![91_u8; 32])
        .fetch_one(database.pool())
        .await
        .unwrap(),
        0
    );

    let winner = database
        .jobs()
        .claim_next(
            "winner-worker",
            std::time::Duration::from_secs(30),
            &[JobType::ComputeFingerprint],
        )
        .await
        .unwrap()
        .expect("recovered fingerprint job should be claimable");
    let winner_attempt = winner.lease().unwrap();
    let completed = database
        .inbox()
        .finalize_video_identity(
            ingest.ingest.id,
            &winner_attempt,
            media_ingest,
            Some(&fingerprint),
            SequenceAlignmentConfig::default(),
        )
        .await
        .unwrap();
    assert_eq!(completed.status, IngestStatus::Storing);
    let media_id = completed.media_id.expect("winner should reserve media");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM media WHERE canonical_sha256 = $1")
            .bind(vec![91_u8; 32])
            .fetch_one(database.pool())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM queue.jobs WHERE kind = 'upload_storage_asset' AND payload->>'media_id' = $1",
        )
        .bind(media_id.to_string())
        .fetch_one(database.pool())
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM queue.jobs WHERE id = $1")
            .bind(winner.id)
            .fetch_one(database.pool())
            .await
            .unwrap(),
        JobStatus::Succeeded.as_str()
    );

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
    sqlx::query("DELETE FROM queue.jobs WHERE payload->>'media_id' = $1")
        .bind(media_id.to_string())
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM media WHERE id = $1")
        .bind(media_id)
        .execute(database.pool())
        .await
        .unwrap();
}

fn test_video_ingest(ingest_id: Uuid, sha256: Vec<u8>) -> MediaIngest {
    MediaIngest {
        media: NewMedia {
            kind: MediaKind::Video,
            title: Some("stale-finalizer-test".to_owned()),
            description: None,
            notes: None,
        },
        metadata: MediaMetadata {
            kind: MediaKind::Video,
            mime_type: Some("video/mp4".to_owned()),
            container: Some("mp4".to_owned()),
            video_codec: Some("h264".to_owned()),
            audio_codec: None,
            width: Some(320),
            height: Some(240),
            duration_ms: Some(4_000),
            bit_rate: Some(100_000),
            file_size_bytes: Some(1_024),
            sha256: Some(sha256),
            local_work_path: Some(format!("/tmp/{ingest_id}.mp4")),
        },
        source: MediaSourceInput {
            ingest_id: Some(ingest_id),
            kind: SourceKind::DirectUrl,
            original_url: Some("https://example.test/stale-finalizer.mp4".to_owned()),
            normalized_url: Some("https://example.test/stale-finalizer.mp4".to_owned()),
            platform: None,
            platform_content_id: None,
            author_name: None,
            title: None,
            description: None,
            published_at: None,
            metadata: json!({"test": "stale-finalizer"}),
        },
        tags: Vec::new(),
    }
}

fn test_video_sequence(seed: u64) -> VideoSequenceFingerprint {
    VideoSequenceFingerprint::new(
        4_000,
        500,
        (0..8)
            .map(|index| VideoSequenceSample {
                phash: seed.wrapping_add(index),
                dhash: seed.rotate_left(index as u32),
                mean_luma: 80 + index as u8,
                mean_chroma_u: -10,
                mean_chroma_v: 12,
                information_bps: 3_000,
                transition_bps: if index != 0 { 1_000 } else { 0 },
            })
            .collect(),
    )
    .unwrap()
}
