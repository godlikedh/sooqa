use serde::Serialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use sooqa_inbox::{
    IngestKind, IngestStatus, IngestSubmission, IngestSubmissionInput, RequestedAction,
    SourceInspection, SourceMediaKind, SubmittedVia,
};
use sooqa_jobs::{JobCommand, JobStatus, JobType};
use sooqa_library::{
    MediaIngest, MediaKind, MediaMetadata, MediaSourceInput, NewMedia, SourceKind,
};
use sooqa_media::{SequenceAlignmentConfig, VideoSequenceFingerprint, VideoSequenceSample};
use sooqa_persistence::{Database, InboxRepositoryError, SourceInspectionStart};
use sooqa_publisher::NewChannel;
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Serialize)]
struct LegacyIngestSubmission<'a> {
    kind: IngestKind,
    submitted_via: SubmittedVia,
    submitted_by_admin_id: Option<Uuid>,
    original_url: &'a str,
    normalized_url: &'a str,
    original_input: &'a serde_json::Value,
    page_url: &'a Option<String>,
    page_title: &'a Option<String>,
    supplied_caption: &'a Option<String>,
    supplied_description: &'a Option<String>,
    supplied_tags: &'a Vec<String>,
    requested_action: RequestedAction,
    requested_publish_at: &'a Option<OffsetDateTime>,
    requested_post_caption: &'a Option<String>,
    idempotency_key: &'a Option<String>,
}

fn legacy_request_hash(submission: &IngestSubmission) -> Vec<u8> {
    let serialized = serde_json::to_vec(&LegacyIngestSubmission {
        kind: submission.kind,
        submitted_via: submission.submitted_via,
        submitted_by_admin_id: None,
        original_url: &submission.original_url,
        normalized_url: &submission.normalized_url,
        original_input: &submission.original_input,
        page_url: &submission.page_url,
        page_title: &submission.page_title,
        supplied_caption: &submission.supplied_caption,
        supplied_description: &submission.supplied_description,
        supplied_tags: &submission.supplied_tags,
        requested_action: submission.requested_action,
        requested_publish_at: &submission.requested_publish_at,
        requested_post_caption: &submission.requested_post_caption,
        idempotency_key: &submission.idempotency_key,
    })
    .unwrap();
    Sha256::digest(serialized).to_vec()
}

async fn enabled_channel(database: &Database, chat_id: i64) -> Uuid {
    database
        .publisher()
        .create_channel(
            NewChannel::try_new(format!("ingest-test-{}", Uuid::new_v4()), chat_id).unwrap(),
        )
        .await
        .unwrap()
        .id
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn input_key_replays_identical_ingests_and_rejects_conflicts(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
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
    conflicting.idempotency_key = Some(key.clone());
    let error = database
        .inbox()
        .create_ingest(IngestSubmission::try_new(conflicting).unwrap())
        .await
        .unwrap_err();
    assert!(matches!(error, InboxRepositoryError::IdempotencyConflict { .. }));

    let mut changed_intent =
        IngestSubmissionInput::new("https://example.test/video", SubmittedVia::Api);
    changed_intent.idempotency_key = Some(key);
    changed_intent.requested_action = RequestedAction::PostNow;
    let error = database
        .inbox()
        .create_ingest(IngestSubmission::try_new(changed_intent).unwrap())
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
    assert!(matches!(
        &claimed.command,
        JobCommand::InspectSource(payload) if payload.ingest_id == first.ingest.id
    ));
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
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn legacy_request_hash_replays_after_admin_identity_removal(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let key = format!("legacy-hash-replay-{}", Uuid::new_v4());
    let mut input =
        IngestSubmissionInput::new("https://example.test/legacy-hash-replay", SubmittedVia::Api);
    input.idempotency_key = Some(key);
    let submission = IngestSubmission::try_new(input.clone()).unwrap();
    let created = database.inbox().create_ingest(submission.clone()).await.unwrap();

    // Simulate the hash persisted by the previous release, whose serialized
    // submission included submitted_by_admin_id: null.
    sqlx::query("UPDATE ingests SET request_hash = $2 WHERE id = $1")
        .bind(created.ingest.id)
        .bind(legacy_request_hash(&submission))
        .execute(database.pool())
        .await
        .unwrap();

    let replay =
        database.inbox().create_ingest(IngestSubmission::try_new(input).unwrap()).await.unwrap();
    assert_eq!(replay.ingest.id, created.ingest.id);
    assert!(!replay.created);
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn exact_queue_replay_remains_idempotent_after_requested_time_passes(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    enabled_channel(&database, -1000000000301).await;
    let clock = OffsetDateTime::now_utc();
    let requested_publish_at = clock + time::Duration::seconds(1);
    let key = format!("exact-queue-replay-{}", Uuid::new_v4());
    let mut input =
        IngestSubmissionInput::new("https://example.test/exact-queue", SubmittedVia::Api);
    input.idempotency_key = Some(key);
    input.requested_action = RequestedAction::Queue;
    input.requested_publish_at = Some(requested_publish_at);
    let first = database
        .inbox()
        .create_ingest_at(IngestSubmission::try_new(input.clone()).unwrap(), clock)
        .await
        .unwrap();

    let replay = database
        .inbox()
        .create_ingest_at(
            IngestSubmission::try_new_for_idempotency_lookup(input).unwrap(),
            clock + time::Duration::seconds(2),
        )
        .await
        .unwrap();
    assert_eq!(replay.ingest.id, first.ingest.id);
    assert!(!replay.created);
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn publication_intent_requires_and_snapshots_one_enabled_channel(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let mut no_channel = IngestSubmissionInput::new(
        format!("https://example.test/no-channel-{}", Uuid::new_v4()),
        SubmittedVia::Api,
    );
    no_channel.requested_action = RequestedAction::Queue;
    no_channel.idempotency_key = Some(format!("no-channel-{}", Uuid::new_v4()));
    let no_channel_key = no_channel.idempotency_key.clone().unwrap();
    let error = database
        .inbox()
        .create_ingest(IngestSubmission::try_new(no_channel).unwrap())
        .await
        .unwrap_err();
    assert!(matches!(error, InboxRepositoryError::RequestedChannelNotConfigured));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM ingests WHERE input_key = $1")
            .bind(no_channel_key)
            .fetch_one(database.pool())
            .await
            .unwrap(),
        0
    );

    let channel_id = enabled_channel(&database, -1000000000303).await;
    let mut queued = IngestSubmissionInput::new(
        format!("https://example.test/one-channel-{}", Uuid::new_v4()),
        SubmittedVia::Api,
    );
    queued.requested_action = RequestedAction::Queue;
    queued.idempotency_key = Some(format!("one-channel-{}", Uuid::new_v4()));
    let queued_submission = IngestSubmission::try_new(queued.clone()).unwrap();
    let created = database.inbox().create_ingest(queued_submission).await.unwrap();
    assert_eq!(created.ingest.requested_channel_id, Some(channel_id));

    sqlx::query("UPDATE channels SET is_enabled = false WHERE id = $1")
        .bind(channel_id)
        .execute(database.pool())
        .await
        .unwrap();
    let replay = database
        .inbox()
        .create_ingest(IngestSubmission::try_new_for_idempotency_lookup(queued).unwrap())
        .await
        .unwrap();
    assert!(!replay.created);
    assert_eq!(replay.ingest.id, created.ingest.id);
    assert_eq!(replay.ingest.requested_channel_id, Some(channel_id));

    let _first = enabled_channel(&database, -1000000000304).await;
    let _second = enabled_channel(&database, -1000000000305).await;
    let mut ambiguous = IngestSubmissionInput::new(
        format!("https://example.test/multiple-channels-{}", Uuid::new_v4()),
        SubmittedVia::Api,
    );
    ambiguous.requested_action = RequestedAction::PostNow;
    ambiguous.idempotency_key = Some(format!("multiple-channels-{}", Uuid::new_v4()));
    let ambiguous_key = ambiguous.idempotency_key.clone().unwrap();
    let error = database
        .inbox()
        .create_ingest(IngestSubmission::try_new(ambiguous).unwrap())
        .await
        .unwrap_err();
    assert!(matches!(error, InboxRepositoryError::RequestedChannelAmbiguous));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM ingests WHERE input_key = $1")
            .bind(ambiguous_key)
            .fetch_one(database.pool())
            .await
            .unwrap(),
        0
    );

    let save = database
        .inbox()
        .create_ingest(
            IngestSubmission::try_new(IngestSubmissionInput::new(
                format!("https://example.test/save-{}", Uuid::new_v4()),
                SubmittedVia::Api,
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(save.ingest.requested_channel_id, None);
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn source_inspection_completion_commits_job_success_with_transition(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
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
    assert!(matches!(
        &claimed.command,
        JobCommand::InspectSource(payload) if payload.ingest_id == ingest.ingest.id
    ));
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
                metadata: json!({
                    "two_ch_mirror": {
                        "submitted_host": "2ch.life",
                        "selected_host": "2ch.org",
                        "selected_url": "https://2ch.org/b/src/inspected.webm"
                    }
                }),
            },
        )
        .await
        .unwrap();
    assert_eq!(completed.status, IngestStatus::Downloading);
    let stored_inspection = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT input_json->'inspection' FROM ingests WHERE id = $1",
    )
    .bind(ingest.ingest.id)
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(stored_inspection["metadata"]["two_ch_mirror"]["selected_host"], "2ch.org");

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
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn duplicate_pending_force_save_is_durable_and_idempotent(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let channel_id = enabled_channel(&database, -1000000000302).await;
    let mut submission_input = IngestSubmissionInput::new(
        format!("https://example.test/duplicate-{}", Uuid::new_v4()),
        SubmittedVia::Companion,
    );
    submission_input.supplied_description = Some("keep this internal note".to_owned());
    submission_input.supplied_tags = vec!["cats".to_owned(), "reaction".to_owned()];
    submission_input.requested_action = RequestedAction::Queue;
    submission_input.requested_publish_at =
        Some(time::OffsetDateTime::now_utc() + time::Duration::minutes(5));
    submission_input.requested_post_caption = Some("public queue text".to_owned());
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
    sqlx::query("UPDATE ingests SET input_json = input_json || $2 WHERE id = $1")
        .bind(ingest.ingest.id)
        .bind(json!({
            "inspection": {
                "metadata": {
                    "two_ch_mirror": {
                        "selected_host": "2ch.org"
                    }
                }
            }
        }))
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
    assert!(resumed.ingest.original_input.get("inspection").is_none());
    assert_eq!(resumed.ingest.supplied_description.as_deref(), Some("keep this internal note"));
    assert_eq!(resumed.ingest.supplied_tags, ["cats", "reaction"]);
    assert_eq!(resumed.ingest.requested_action, RequestedAction::Queue);
    assert!(resumed.ingest.requested_publish_at.is_some());
    assert_eq!(resumed.ingest.requested_post_caption.as_deref(), Some("public queue text"));
    assert_eq!(resumed.ingest.requested_channel_id, Some(channel_id));
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
    assert_eq!(replay.ingest.requested_channel_id, Some(channel_id));
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn duplicate_acceptance_reuses_evidenced_media_and_merges_metadata(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let channel_id = enabled_channel(&database, -1000000000306).await;
    let media_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO media (id, kind, storage_state, tags, description, telegram_storage_chat_id, telegram_storage_message_id, telegram_file_id) VALUES ($1, 'video', 'ready', $2, $3, $4, $5, $6)",
    )
    .bind(media_id)
    .bind(vec!["existing".to_owned()])
    .bind("old description")
    .bind(-100123_i64)
    .bind(88_i64)
    .bind("ready-file")
    .execute(database.pool())
    .await
    .unwrap();

    let mut input = IngestSubmissionInput::new(
        format!("https://example.test/accept-{}", Uuid::new_v4()),
        SubmittedVia::Companion,
    );
    input.supplied_description = Some("new description".to_owned());
    input.supplied_tags = vec!["incoming".to_owned(), "existing".to_owned()];
    input.requested_action = RequestedAction::Queue;
    input.requested_publish_at = Some(time::OffsetDateTime::now_utc() + time::Duration::minutes(5));
    input.requested_post_caption = Some("queued public text".to_owned());
    let ingest =
        database.inbox().create_ingest(IngestSubmission::try_new(input).unwrap()).await.unwrap();
    sqlx::query(
        "UPDATE ingests SET state = 'duplicate_pending', duplicate_evidence = $2 WHERE id = $1",
    )
    .bind(ingest.ingest.id)
    .bind(duplicate_evidence(media_id))
    .execute(database.pool())
    .await
    .unwrap();

    let pending = database.inbox().list_duplicate_pending(3).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].ingest.id, ingest.ingest.id);
    assert_eq!(pending[0].candidates.len(), 1);
    assert_eq!(pending[0].candidates[0].media_id, media_id);
    assert_eq!(pending[0].candidates[0].score_bps, 9500);
    assert_eq!(pending[0].candidates[0].storage_state, "ready");
    assert_eq!(pending[0].candidates[0].storage_chat_id, Some(-100123));
    assert_eq!(pending[0].candidates[0].storage_message_id, Some(88));

    let invalid = database.inbox().accept_duplicate(ingest.ingest.id, Uuid::now_v7()).await;
    assert!(matches!(invalid, Err(InboxRepositoryError::DuplicateCandidateNotEvidenced(_))));
    assert_eq!(
        database.inbox().find(ingest.ingest.id).await.unwrap().unwrap().status,
        IngestStatus::DuplicatePending
    );

    let accepted = database.inbox().accept_duplicate(ingest.ingest.id, media_id).await.unwrap();
    assert!(!accepted.replayed);
    assert_eq!(accepted.ingest.status, IngestStatus::Completed);
    assert_eq!(accepted.ingest.media_id, Some(media_id));
    assert!(accepted.ingest.duplicate_evidence.is_none());
    assert_eq!(accepted.ingest.requested_action, RequestedAction::Queue);
    assert!(accepted.ingest.requested_publish_at.is_some());
    assert_eq!(accepted.ingest.requested_post_caption.as_deref(), Some("queued public text"));
    assert_eq!(accepted.ingest.requested_channel_id, Some(channel_id));
    let input_json =
        sqlx::query_scalar::<_, serde_json::Value>("SELECT input_json FROM ingests WHERE id = $1")
            .bind(ingest.ingest.id)
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert_eq!(input_json["_sooqa_duplicate_decision_v1"]["kind"], "accepted");
    assert_eq!(input_json["_sooqa_duplicate_decision_v1"]["media_id"], media_id.to_string());

    let replay = database.inbox().accept_duplicate(ingest.ingest.id, media_id).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.ingest.media_id, Some(media_id));
    assert_eq!(replay.ingest.requested_action, RequestedAction::Queue);
    assert!(replay.ingest.requested_publish_at.is_some());
    assert_eq!(replay.ingest.requested_post_caption.as_deref(), Some("queued public text"));
    assert_eq!(replay.ingest.requested_channel_id, Some(channel_id));
    assert!(matches!(
        database.inbox().accept_duplicate(ingest.ingest.id, Uuid::now_v7()).await,
        Err(InboxRepositoryError::DuplicateDecisionNotAllowed(IngestStatus::Completed))
    ));

    sqlx::query("UPDATE ingests SET state = 'storing', input_json = '{}'::jsonb WHERE id = $1")
        .bind(ingest.ingest.id)
        .execute(database.pool())
        .await
        .unwrap();
    assert!(matches!(
        database.inbox().accept_duplicate(ingest.ingest.id, media_id).await,
        Err(InboxRepositoryError::DuplicateDecisionNotAllowed(IngestStatus::Storing))
    ));

    let (description, tags) = sqlx::query_as::<_, (Option<String>, Vec<String>)>(
        "SELECT description, tags FROM media WHERE id = $1",
    )
    .bind(media_id)
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(description.as_deref(), Some("new description"));
    assert_eq!(tags, ["existing", "incoming"]);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM queue.jobs WHERE kind = 'upload_storage_asset' AND payload->>'media_id' = $1",
        )
        .bind(media_id.to_string())
        .fetch_one(database.pool())
        .await
        .unwrap(),
        0
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
    sqlx::query("DELETE FROM media WHERE id = $1")
        .bind(media_id)
        .execute(database.pool())
        .await
        .unwrap();
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn duplicate_acceptance_joins_pending_storage_without_replacing_upload(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let media_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO media (id, kind, storage_state) VALUES ($1, 'video', 'pending_storage')",
    )
    .bind(media_id)
    .execute(database.pool())
    .await
    .unwrap();
    database
        .jobs()
        .enqueue(
            sooqa_jobs::NewJob::upload_storage_asset_generation(media_id, 0)
                .dedupe_key(format!("test:duplicate-upload:{media_id}")),
        )
        .await
        .unwrap();

    let ingest = database
        .inbox()
        .create_ingest(
            IngestSubmission::try_new(IngestSubmissionInput::new(
                format!("https://example.test/pending-accept-{}", Uuid::new_v4()),
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
    .bind(duplicate_evidence(media_id))
    .execute(database.pool())
    .await
    .unwrap();

    let before = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM queue.jobs WHERE kind = 'upload_storage_asset' AND payload->>'media_id' = $1",
    )
    .bind(media_id.to_string())
    .fetch_one(database.pool())
    .await
    .unwrap();
    let accepted = database.inbox().accept_duplicate(ingest.ingest.id, media_id).await.unwrap();
    assert_eq!(accepted.ingest.status, IngestStatus::Storing);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM queue.jobs WHERE kind = 'upload_storage_asset' AND payload->>'media_id' = $1",
        )
        .bind(media_id.to_string())
        .fetch_one(database.pool())
        .await
        .unwrap(),
        before
    );

    sqlx::query(
        "UPDATE media SET storage_state = 'ready', telegram_storage_chat_id = -100123, telegram_storage_message_id = 99, telegram_file_id = 'pending-now-ready' WHERE id = $1",
    )
    .bind(media_id)
    .execute(database.pool())
    .await
    .unwrap();
    assert_eq!(database.inbox().complete_storage_for_media(media_id).await.unwrap(), 1);
    assert_eq!(
        database.inbox().find(ingest.ingest.id).await.unwrap().unwrap().status,
        IngestStatus::Completed
    );

    sqlx::query(
        "DELETE FROM queue.jobs WHERE payload->>'ingest_id' = $1 OR payload->>'media_id' = $2",
    )
    .bind(ingest.ingest.id.to_string())
    .bind(media_id.to_string())
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query("DELETE FROM ingests WHERE id = $1")
        .bind(ingest.ingest.id)
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM media WHERE id = $1")
        .bind(media_id)
        .execute(database.pool())
        .await
        .unwrap();
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn accepted_pending_storage_decision_replays_after_storage_failures(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let media_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO media (id, kind, storage_state) VALUES ($1, 'video', 'pending_storage')",
    )
    .bind(media_id)
    .execute(database.pool())
    .await
    .unwrap();
    let ingest = database
        .inbox()
        .create_ingest(
            IngestSubmission::try_new(IngestSubmissionInput::new(
                format!("https://example.test/failure-replay-{}", Uuid::new_v4()),
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
    .bind(duplicate_evidence(media_id))
    .execute(database.pool())
    .await
    .unwrap();

    let accepted = database.inbox().accept_duplicate(ingest.ingest.id, media_id).await.unwrap();
    assert_eq!(accepted.ingest.status, IngestStatus::Storing);
    assert_eq!(
        database
            .inbox()
            .fail_storage_for_media(
                media_id,
                IngestStatus::FailedRetryable,
                "storage_upload",
                "temporary storage failure",
            )
            .await
            .unwrap(),
        1
    );
    let retryable_replay =
        database.inbox().accept_duplicate(ingest.ingest.id, media_id).await.unwrap();
    assert!(retryable_replay.replayed);
    assert_eq!(retryable_replay.ingest.status, IngestStatus::FailedRetryable);
    assert!(matches!(
        database.inbox().accept_duplicate(ingest.ingest.id, Uuid::now_v7()).await,
        Err(InboxRepositoryError::DuplicateDecisionNotAllowed(IngestStatus::FailedRetryable))
    ));

    assert_eq!(
        database
            .inbox()
            .fail_storage_for_media(
                media_id,
                IngestStatus::FailedTerminal,
                "storage_upload",
                "permanent storage failure",
            )
            .await
            .unwrap(),
        1
    );
    let terminal_replay =
        database.inbox().accept_duplicate(ingest.ingest.id, media_id).await.unwrap();
    assert!(terminal_replay.replayed);
    assert_eq!(terminal_replay.ingest.status, IngestStatus::FailedTerminal);
    assert!(matches!(
        database.inbox().accept_duplicate(ingest.ingest.id, Uuid::now_v7()).await,
        Err(InboxRepositoryError::DuplicateDecisionNotAllowed(IngestStatus::FailedTerminal))
    ));

    sqlx::query(
        "DELETE FROM queue.jobs WHERE payload->>'ingest_id' = $1 OR payload->>'media_id' = $2",
    )
    .bind(ingest.ingest.id.to_string())
    .bind(media_id.to_string())
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query("DELETE FROM ingests WHERE id = $1")
        .bind(ingest.ingest.id)
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM media WHERE id = $1")
        .bind(media_id)
        .execute(database.pool())
        .await
        .unwrap();
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn duplicate_accept_and_force_save_have_one_row_locked_winner(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let media_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO media (id, kind, storage_state, telegram_storage_chat_id, telegram_storage_message_id, telegram_file_id) VALUES ($1, 'video', 'ready', $2, $3, $4)",
    )
    .bind(media_id)
    .bind(-100123_i64)
    .bind(101_i64)
    .bind("race-ready-file")
    .execute(database.pool())
    .await
    .unwrap();
    let ingest = database
        .inbox()
        .create_ingest(
            IngestSubmission::try_new(IngestSubmissionInput::new(
                format!("https://example.test/race-{}", Uuid::new_v4()),
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
    .bind(duplicate_evidence(media_id))
    .execute(database.pool())
    .await
    .unwrap();

    let inbox = database.inbox();
    let (accepted, forced) = tokio::join!(
        inbox.accept_duplicate(ingest.ingest.id, media_id),
        inbox.force_save(ingest.ingest.id),
    );
    assert!(accepted.is_ok() ^ forced.is_ok());
    if accepted.is_ok() {
        assert!(matches!(
            forced,
            Err(InboxRepositoryError::ForceSaveNotAllowed(IngestStatus::Completed))
        ));
    } else {
        assert!(matches!(
            accepted,
            Err(InboxRepositoryError::DuplicateDecisionNotAllowed(IngestStatus::Queued))
        ));
    }

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
    sqlx::query("DELETE FROM media WHERE id = $1")
        .bind(media_id)
        .execute(database.pool())
        .await
        .unwrap();
}

fn duplicate_evidence(media_id: Uuid) -> serde_json::Value {
    json!({
        "algorithm_version": "video_sequence_v1",
        "matches": [{
            "media_id": media_id,
            "fingerprint_version": "video_sequence_v1",
            "classification": "strong_duplicate",
            "aligned_offset_ms": 0,
            "informative_matched_samples": 8,
            "incoming_coverage_bps": 9000,
            "candidate_coverage_bps": 9000,
            "median_distance_bps": 100,
            "high_percentile_distance_bps": 200,
            "longest_temporally_consistent_run": 8,
            "unmatched_incoming_prefix": 0,
            "unmatched_incoming_suffix": 0,
            "unmatched_candidate_prefix": 0,
            "unmatched_candidate_suffix": 0,
            "gap_count": 0,
            "score_bps": 9500,
            "shared_token_count": 12,
            "token_overlap_bps": 8000
        }]
    })
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn stale_video_identity_finalizer_cannot_mutate_after_lease_recovery(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
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
            preview: None,
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
