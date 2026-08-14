use serde_json::json;
use sooqa_inbox::{IngestSubmission, IngestSubmissionInput, RequestedAction, SubmittedVia};
use sooqa_jobs::JobType;
use sooqa_library::{
    MediaIngest, MediaKind, MediaMetadata, MediaSourceInput, NewMedia, SourceKind,
    StorageUploadAttachment, StorageUploadReservation, StorageUploadReservationRequest,
    StorageUploadStore,
};
use sooqa_persistence::{Database, PublisherRepositoryError};
use sooqa_publisher::{
    NewChannel, NewPost, PostExactSchedule, PostState, PublicationAction, PublicationDecision,
    PublicationIntent,
};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

async fn stored_media(database: &Database) -> Uuid {
    let media = database
        .library()
        .resolve_media(MediaIngest {
            media: NewMedia::new(MediaKind::Video),
            metadata: MediaMetadata {
                kind: MediaKind::Video,
                mime_type: Some("video/mp4".to_owned()),
                container: Some("mp4".to_owned()),
                video_codec: None,
                audio_codec: None,
                width: Some(1),
                height: Some(1),
                duration_ms: Some(1),
                bit_rate: None,
                file_size_bytes: Some(1),
                sha256: Some(vec![Uuid::new_v4().as_bytes()[0]; 32]),
                local_work_path: None,
            },
            source: MediaSourceInput {
                ingest_id: None,
                kind: SourceKind::DirectUrl,
                original_url: Some(format!("https://example.test/{}", Uuid::new_v4())),
                normalized_url: None,
                platform: None,
                platform_content_id: None,
                author_name: None,
                title: None,
                description: None,
                published_at: None,
                metadata: json!({}),
            },
            tags: Vec::new(),
        })
        .await
        .unwrap();
    sqlx::query(
        "UPDATE media SET storage_state = 'ready', telegram_storage_chat_id = -100123456789, telegram_storage_message_id = 1, telegram_file_id = 'file' WHERE id = $1",
    )
    .bind(media.media.id)
    .execute(database.pool())
    .await
    .unwrap();
    media.media.id
}

async fn publication_channel(database: &Database, chat_id: i64) -> sooqa_publisher::Channel {
    let mut channel = NewChannel::try_new(format!("test-{chat_id}"), chat_id).unwrap();
    channel.window_start = time::Time::from_hms(0, 0, 0).unwrap();
    channel.window_end = time::Time::from_hms(23, 59, 0).unwrap();
    channel.interval_minutes = 30;
    database.publisher().create_channel(channel).await.unwrap()
}

async fn completed_ingest(
    database: &Database,
    media_id: Uuid,
    requested_action: RequestedAction,
    requested_publish_at: Option<OffsetDateTime>,
) -> Uuid {
    let mut input = IngestSubmissionInput::new(
        format!("https://example.test/ingest/{}", Uuid::new_v4()),
        SubmittedVia::Api,
    );
    input.requested_action = requested_action;
    input.requested_publish_at = requested_publish_at;
    input.requested_post_caption =
        (requested_action != RequestedAction::Save).then(|| "captured caption".to_owned());
    input.idempotency_key = Some(format!("ingest-{}", Uuid::new_v4()));
    let ingest = database
        .inbox()
        .create_ingest(IngestSubmission::try_new_for_idempotency_lookup(input).unwrap())
        .await
        .unwrap()
        .ingest;

    sqlx::query(
        "UPDATE ingests SET media_id = $2, state = 'storing', completed_at = NULL WHERE id = $1",
    )
    .bind(ingest.id)
    .bind(media_id)
    .execute(database.pool())
    .await
    .unwrap();
    assert_eq!(database.inbox().complete_storage_for_media(media_id).await.unwrap(), 1);
    ingest.id
}

async fn materialization_job_count(database: &Database, ingest_id: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM queue.jobs WHERE kind = $1 AND payload->>'ingest_id' = $2",
    )
    .bind(JobType::MaterializePublication.as_str())
    .bind(ingest_id.to_string())
    .fetch_one(database.pool())
    .await
    .unwrap()
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn storage_completion_enqueues_materialization_in_the_ready_transaction(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let _channel = publication_channel(&database, -100123456789).await;
    let media_id = stored_media(&database).await;
    sqlx::query(
        "UPDATE media SET storage_state = 'pending_storage', telegram_storage_chat_id = NULL, telegram_storage_message_id = NULL, telegram_file_id = NULL, storage_token = NULL WHERE id = $1",
    )
    .bind(media_id)
    .execute(database.pool())
    .await
    .unwrap();
    let mut input = IngestSubmissionInput::new(
        format!("https://example.test/storage/{}", Uuid::new_v4()),
        SubmittedVia::Api,
    );
    input.requested_action = RequestedAction::Queue;
    input.idempotency_key = Some(format!("storage-ingest-{}", Uuid::new_v4()));
    let ingest = database
        .inbox()
        .create_ingest(IngestSubmission::try_new_for_idempotency_lookup(input).unwrap())
        .await
        .unwrap()
        .ingest;
    sqlx::query(
        "UPDATE ingests SET media_id = $2, state = 'storing', completed_at = NULL WHERE id = $1",
    )
    .bind(ingest.id)
    .bind(media_id)
    .execute(database.pool())
    .await
    .unwrap();

    let owner_token = match database
        .library()
        .reserve_storage_upload(StorageUploadReservationRequest { media_id, generation: 0 })
        .await
        .unwrap()
    {
        StorageUploadReservation::Reserved { owner_token, .. } => owner_token,
        other => panic!("expected a fresh storage reservation, got {other:?}"),
    };
    database
        .library()
        .complete_storage_upload(
            media_id,
            owner_token,
            StorageUploadAttachment {
                storage_chat_id: -100123456789,
                storage_message_id: 77,
                telegram_file_id: Some("file-77".to_owned()),
                telegram_file_unique_id: Some("unique-77".to_owned()),
            },
        )
        .await
        .unwrap();

    assert_eq!(materialization_job_count(&database, ingest.id).await, 1);
    assert_eq!(
        database.inbox().find(ingest.id).await.unwrap().unwrap().status.as_str(),
        "completed"
    );
}

async fn publish_job_count(database: &Database, post_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM queue.jobs WHERE dedupe_key = $1")
        .bind(format!("post:{post_id}:publish:v1"))
        .fetch_one(database.pool())
        .await
        .unwrap()
}

async fn published_post(
    database: &Database,
    media_id: Uuid,
    channel_id: Uuid,
    published_at: OffsetDateTime,
    key: &str,
) -> Uuid {
    let post = database
        .publisher()
        .create_post_idempotent(
            NewPost {
                media_id,
                channel_id,
                caption: None,
                parse_mode: None,
                disable_notification: false,
            },
            key.to_owned(),
            key.as_bytes(),
        )
        .await
        .unwrap()
        .post;
    sqlx::query(
        "UPDATE posts SET state = 'published', published_at = $2, telegram_message_id = 42, revision = 1 WHERE id = $1",
    )
    .bind(post.id)
    .bind(published_at)
    .execute(database.pool())
    .await
    .unwrap();
    post.id
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn direct_publication_intent_is_idempotent_and_creates_one_publish_job(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let _channel = publication_channel(&database, -100123456789).await;
    let media_id = stored_media(&database).await;
    let intent = PublicationIntent::try_new(
        PublicationAction::Queue,
        None,
        Some(" direct caption ".to_owned()),
    )
    .unwrap();

    let first = database
        .publisher()
        .create_publication_intent(media_id, intent.clone(), "direct-intent".to_owned())
        .await
        .unwrap();
    assert!(first.created);
    assert_eq!(first.post.state, PostState::Queued);
    assert_eq!(first.post.requested_action, PublicationAction::Queue);
    assert_eq!(first.post.caption.as_deref(), Some("direct caption"));
    assert!(first.post.cadence_slot_at.is_some());
    assert_eq!(publish_job_count(&database, first.post.id).await, 1);

    let replay = database
        .publisher()
        .create_publication_intent(media_id, intent, "direct-intent".to_owned())
        .await
        .unwrap();
    assert!(!replay.created);
    assert_eq!(replay.post.id, first.post.id);
    assert_eq!(publish_job_count(&database, first.post.id).await, 1);

    let concurrent_media = stored_media(&database).await;
    let repository_a = database.publisher();
    let repository_b = database.publisher();
    let intent = PublicationIntent::try_new(PublicationAction::PostNow, None, None).unwrap();
    let (left, right) = tokio::join!(
        repository_a.create_publication_intent(
            concurrent_media,
            intent.clone(),
            "concurrent-direct".to_owned(),
        ),
        repository_b.create_publication_intent(
            concurrent_media,
            intent,
            "concurrent-direct".to_owned(),
        ),
    );
    let left = left.unwrap();
    let right = right.unwrap();
    assert_ne!(left.created, right.created);
    assert_eq!(left.post.id, right.post.id);
    assert_eq!(publish_job_count(&database, left.post.id).await, 1);
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn exact_publication_allows_collisions_and_ignores_cadence_rules(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let _channel = publication_channel(&database, -100123456789).await;
    let first_media = stored_media(&database).await;
    let second_media = stored_media(&database).await;
    let requested_at = OffsetDateTime::now_utc() + Duration::hours(2);
    let repository = database.publisher();
    let first = repository
        .create_publication_intent(
            first_media,
            PublicationIntent::try_new(PublicationAction::Queue, Some(requested_at), None).unwrap(),
            format!("exact-{first_media}"),
        )
        .await
        .unwrap()
        .post;
    let second = repository
        .create_publication_intent(
            second_media,
            PublicationIntent::try_new(PublicationAction::Queue, Some(requested_at), None).unwrap(),
            format!("exact-{second_media}"),
        )
        .await
        .unwrap()
        .post;
    assert_eq!(first.state, PostState::Queued);
    assert_eq!(first.scheduled_at, requested_at);
    assert!(first.cadence_slot_at.is_none());
    assert_eq!(second.scheduled_at, requested_at);
    assert!(second.cadence_slot_at.is_none());
    assert_eq!(
        sqlx::query_scalar::<_, OffsetDateTime>(
            "SELECT run_at FROM queue.jobs WHERE dedupe_key = $1",
        )
        .bind(format!("post:{}:publish:v1", second.id))
        .fetch_one(database.pool())
        .await
        .unwrap(),
        requested_at
    );

    let past = database
        .publisher()
        .schedule_post_exact(
            PostExactSchedule::try_new(
                first.id,
                OffsetDateTime::now_utc() - Duration::minutes(1),
                "past-exact",
                first.revision,
            )
            .unwrap(),
        )
        .await;
    assert!(matches!(past, Err(PublisherRepositoryError::ExactScheduleInPast)));

    let moved_at = requested_at + Duration::hours(1);
    let moved = database
        .publisher()
        .schedule_post_exact(
            PostExactSchedule::try_new(first.id, moved_at, "manual-exact", first.revision).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(moved.scheduled_at, moved_at);
    assert!(moved.cadence_slot_at.is_none());
    let unchanged = database.publisher().find_post(second.id).await.unwrap().unwrap();
    assert_eq!(unchanged.scheduled_at, requested_at);
    assert!(unchanged.cadence_slot_at.is_none());
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn repeat_evaluation_uses_the_intended_send_instant_and_persists_evidence(
    pool: sqlx::PgPool,
) {
    let database = Database::from_pool(pool);
    let channel = publication_channel(&database, -100123456789).await;
    let conflict_media = stored_media(&database).await;
    let old_media = stored_media(&database).await;
    let queued_media = stored_media(&database).await;
    let now = OffsetDateTime::now_utc();
    published_post(
        &database,
        conflict_media,
        channel.id,
        now - Duration::days(5),
        "published-conflict",
    )
    .await;
    published_post(&database, old_media, channel.id, now - Duration::days(13), "published-old")
        .await;

    let intended_at = now + Duration::days(5);
    let conflict = database
        .publisher()
        .create_publication_intent(
            conflict_media,
            PublicationIntent::try_new(PublicationAction::Queue, Some(intended_at), None).unwrap(),
            "repeat-conflict".to_owned(),
        )
        .await
        .unwrap()
        .post;
    assert_eq!(conflict.state, PostState::Draft);
    let evidence = conflict.repeat_evidence.expect("repeat evidence should be persisted");
    assert_eq!(evidence.conflicts.len(), 1);
    assert_eq!(evidence.conflicts[0].state, PostState::Published);
    assert_eq!(
        evidence.conflicts[0].target_message_link.as_deref(),
        Some("https://t.me/c/123456789/42")
    );
    assert_eq!(publish_job_count(&database, conflict.id).await, 0);

    let allowed = database
        .publisher()
        .create_publication_intent(
            old_media,
            PublicationIntent::try_new(PublicationAction::Queue, Some(intended_at), None).unwrap(),
            "repeat-old-allowed".to_owned(),
        )
        .await
        .unwrap()
        .post;
    assert_eq!(allowed.state, PostState::Queued);
    assert!(allowed.repeat_evidence.is_none());

    let first_queued = database
        .publisher()
        .create_publication_intent(
            queued_media,
            PublicationIntent::try_new(PublicationAction::Queue, None, None).unwrap(),
            "queued-conflict-first".to_owned(),
        )
        .await
        .unwrap()
        .post;
    assert_eq!(first_queued.state, PostState::Queued);
    let queued_conflict = database
        .publisher()
        .create_publication_intent(
            queued_media,
            PublicationIntent::try_new(PublicationAction::Queue, None, None).unwrap(),
            "queued-conflict-second".to_owned(),
        )
        .await
        .unwrap()
        .post;
    assert_eq!(queued_conflict.state, PostState::Draft);
    assert_eq!(queued_conflict.repeat_evidence.unwrap().conflicts[0].post_id, first_queued.id);
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn publication_decisions_are_revision_fenced_and_idempotent(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let _channel = publication_channel(&database, -100123456789).await;
    let media = stored_media(&database).await;
    let seed = database
        .publisher()
        .create_publication_intent(
            media,
            PublicationIntent::try_new(
                PublicationAction::Queue,
                Some(OffsetDateTime::now_utc() + Duration::hours(3)),
                None,
            )
            .unwrap(),
            "decision-seed".to_owned(),
        )
        .await
        .unwrap()
        .post;
    let exact_draft = database
        .publisher()
        .create_publication_intent(
            media,
            PublicationIntent::try_new(
                PublicationAction::Queue,
                Some(OffsetDateTime::now_utc() + Duration::hours(4)),
                None,
            )
            .unwrap(),
            "decision-exact".to_owned(),
        )
        .await
        .unwrap()
        .post;
    assert_eq!(seed.state, PostState::Queued);
    assert_eq!(exact_draft.state, PostState::Draft);
    let expected_revision = exact_draft.revision;
    let approved = database
        .publisher()
        .decide_post(
            exact_draft.id,
            PublicationDecision::KeepExactTime,
            "decision-exact-key".to_owned(),
            expected_revision,
        )
        .await
        .unwrap();
    assert_eq!(approved.state, PostState::Queued);
    assert!(approved.cadence_slot_at.is_none());
    assert!(approved.repeat_evidence.is_none());
    let replay = database
        .publisher()
        .decide_post(
            exact_draft.id,
            PublicationDecision::KeepExactTime,
            "decision-exact-key".to_owned(),
            expected_revision,
        )
        .await
        .unwrap();
    assert_eq!(replay.revision, approved.revision);
    assert_eq!(publish_job_count(&database, approved.id).await, 1);
    assert!(matches!(
        database
            .publisher()
            .decide_post(
                exact_draft.id,
                PublicationDecision::Cancel,
                "decision-exact-key".to_owned(),
                expected_revision,
            )
            .await,
        Err(PublisherRepositoryError::RequestKeyConflict(_))
    ));
    assert!(matches!(
        database
            .publisher()
            .decide_post(
                exact_draft.id,
                PublicationDecision::Cancel,
                "stale-decision".to_owned(),
                expected_revision,
            )
            .await,
        Err(PublisherRepositoryError::PostDecisionNotAllowed { .. })
    ));

    let post_now_media = stored_media(&database).await;
    let post_now_seed = database
        .publisher()
        .create_publication_intent(
            post_now_media,
            PublicationIntent::try_new(PublicationAction::Queue, None, None).unwrap(),
            "post-now-seed".to_owned(),
        )
        .await
        .unwrap()
        .post;
    let post_now_draft = database
        .publisher()
        .create_publication_intent(
            post_now_media,
            PublicationIntent::try_new(PublicationAction::PostNow, None, None).unwrap(),
            "post-now-draft".to_owned(),
        )
        .await
        .unwrap()
        .post;
    assert_eq!(post_now_seed.state, PostState::Queued);
    assert_eq!(post_now_draft.state, PostState::Draft);
    let post_now = database
        .publisher()
        .decide_post(
            post_now_draft.id,
            PublicationDecision::PostNowAnyway,
            "post-now-decision".to_owned(),
            post_now_draft.revision,
        )
        .await
        .unwrap();
    assert_eq!(post_now.state, PostState::Queued);
    assert!(post_now.cadence_slot_at.is_none());
    assert!(post_now.scheduled_at <= OffsetDateTime::now_utc());

    let normal_draft = database
        .publisher()
        .create_publication_intent(
            post_now_media,
            PublicationIntent::try_new(PublicationAction::Queue, None, None).unwrap(),
            "normal-draft".to_owned(),
        )
        .await
        .unwrap()
        .post;
    assert_eq!(normal_draft.state, PostState::Draft);
    let normal = database
        .publisher()
        .decide_post(
            normal_draft.id,
            PublicationDecision::QueueAnyway,
            "normal-decision".to_owned(),
            normal_draft.revision,
        )
        .await
        .unwrap();
    assert_eq!(normal.state, PostState::Queued);
    assert!(normal.cadence_slot_at.is_some());

    let cancel_media = stored_media(&database).await;
    let _cancel_seed = database
        .publisher()
        .create_publication_intent(
            cancel_media,
            PublicationIntent::try_new(PublicationAction::Queue, None, None).unwrap(),
            "cancel-seed".to_owned(),
        )
        .await
        .unwrap();
    let cancel_draft = database
        .publisher()
        .create_publication_intent(
            cancel_media,
            PublicationIntent::try_new(PublicationAction::PostNow, None, None).unwrap(),
            "cancel-draft".to_owned(),
        )
        .await
        .unwrap()
        .post;
    let cancelled = database
        .publisher()
        .decide_post(
            cancel_draft.id,
            PublicationDecision::Cancel,
            "cancel-decision".to_owned(),
            cancel_draft.revision,
        )
        .await
        .unwrap();
    assert_eq!(cancelled.state, PostState::Cancelled);
    assert_eq!(cancelled.media_id, cancel_media);
    assert_eq!(publish_job_count(&database, cancelled.id).await, 0);
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn materialization_is_atomic_idempotent_and_skips_save_only(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let _channel = publication_channel(&database, -100123456789).await;
    let queue_media = stored_media(&database).await;
    let queue_ingest = completed_ingest(
        &database,
        queue_media,
        RequestedAction::Queue,
        Some(OffsetDateTime::now_utc() + Duration::hours(2)),
    )
    .await;
    assert_eq!(materialization_job_count(&database, queue_ingest).await, 1);

    let repository_a = database.publisher();
    let repository_b = database.publisher();
    let (left, right) = tokio::join!(
        repository_a.materialize_ingest(queue_ingest),
        repository_b.materialize_ingest(queue_ingest),
    );
    let left = left.unwrap();
    let right = right.unwrap();
    assert_ne!(left.created, right.created);
    let first = left.post.clone().unwrap();
    let second = right.post.clone().unwrap();
    assert_eq!(first.id, second.id);
    assert_eq!(first.origin_ingest_id, Some(queue_ingest));
    assert_eq!(first.state, PostState::Queued);
    assert_eq!(publish_job_count(&database, first.id).await, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM posts WHERE origin_ingest_id = $1")
            .bind(queue_ingest)
            .fetch_one(database.pool())
            .await
            .unwrap(),
        1
    );

    let save_media = stored_media(&database).await;
    let save_ingest = completed_ingest(&database, save_media, RequestedAction::Save, None).await;
    assert_eq!(materialization_job_count(&database, save_ingest).await, 0);
    let save = database.publisher().materialize_ingest(save_ingest).await.unwrap();
    assert!(!save.created);
    assert!(save.post.is_none());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM posts WHERE origin_ingest_id = $1")
            .bind(save_ingest)
            .fetch_one(database.pool())
            .await
            .unwrap(),
        0
    );
}
