use std::time::Duration as StdDuration;

use serde_json::json;
use sooqa_jobs::JobType;
use sooqa_library::{
    MediaIngest, MediaKind, MediaMetadata, MediaSourceInput, NewMedia, SourceKind,
};
use sooqa_persistence::{Database, PublishLease, PublisherRepositoryError};
use sooqa_publisher::{NewChannel, NewPost, PostSchedule, PostState, PostUpdate};
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
    sqlx::query("UPDATE media SET storage_state = 'ready', telegram_storage_chat_id = -100123, telegram_storage_message_id = 1, telegram_file_id = 'file' WHERE id = $1")
        .bind(media.media.id)
        .execute(database.pool())
        .await
        .unwrap();
    media.media.id
}

async fn claim_publish_job(database: &Database, post_id: Uuid) -> sooqa_jobs::Job {
    sqlx::query("UPDATE queue.jobs SET run_at = now() WHERE dedupe_key = $1")
        .bind(format!("post:{post_id}:publish:v1"))
        .execute(database.pool())
        .await
        .unwrap();
    database
        .jobs()
        .claim_next("publisher-test", StdDuration::from_secs(60), &[JobType::PublishPost])
        .await
        .unwrap()
        .expect("publish job should be claimable")
}

async fn publish_job_snapshot(
    database: &Database,
    post_id: Uuid,
) -> (String, OffsetDateTime, serde_json::Value) {
    sqlx::query_as::<_, (String, OffsetDateTime, serde_json::Value)>(
        "SELECT state, run_at, payload FROM queue.jobs WHERE dedupe_key = $1",
    )
    .bind(format!("post:{post_id}:publish:v1"))
    .fetch_one(database.pool())
    .await
    .unwrap()
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn post_schedule_uses_one_row_and_channel_cadence(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let channel = database
        .publisher()
        .create_channel(
            NewChannel::try_new(format!("test-{}", Uuid::new_v4()), -1000000000000).unwrap(),
        )
        .await
        .unwrap();
    let media_id = stored_media(&database).await;
    let created = database
        .publisher()
        .create_post_idempotent(
            NewPost {
                media_id,
                channel_id: channel.id,
                caption: Some("hello".to_owned()),
                parse_mode: None,
                disable_notification: false,
            },
            format!("post-{}", Uuid::new_v4()),
            b"post-hash",
        )
        .await
        .unwrap();
    let second = database
        .publisher()
        .create_post_idempotent(
            NewPost {
                media_id,
                channel_id: channel.id,
                caption: Some("second".to_owned()),
                parse_mode: None,
                disable_notification: false,
            },
            format!("post-{}", Uuid::new_v4()),
            b"second-post-hash",
        )
        .await
        .unwrap();
    let updated = database
        .publisher()
        .update_post(
            created.post.id,
            PostUpdate {
                caption: None,
                parse_mode: None,
                disable_notification: Some(true),
                expected_updated_at: None,
                expected_revision: 0,
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.state, PostState::Draft);
    assert!(updated.disable_notification);
    let repository = database.publisher();
    let requested_at = OffsetDateTime::now_utc();
    let (scheduled, second_scheduled) = tokio::join!(
        repository.schedule_post(
            PostSchedule::try_new(created.post.id, requested_at, "schedule-key", updated.revision)
                .unwrap()
        ),
        repository.schedule_post(
            PostSchedule::try_new(second.post.id, requested_at, "second-schedule-key", 0).unwrap()
        )
    );
    let scheduled = scheduled.unwrap();
    let second_scheduled = second_scheduled.unwrap();
    assert_eq!(scheduled.state, PostState::Queued);
    assert_eq!(second_scheduled.state, PostState::Queued);
    let first_slot = scheduled.cadence_slot_at.expect("first slot should be assigned");
    let second_slot = second_scheduled.cadence_slot_at.expect("second slot should be assigned");
    assert_ne!(first_slot, second_slot);
    assert!((second_slot - first_slot).whole_minutes().abs() >= 30);
    let replay = repository
        .schedule_post(
            PostSchedule::try_new(
                created.post.id,
                requested_at,
                "schedule-key",
                scheduled.revision,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(replay.cadence_slot_at, scheduled.cadence_slot_at);
    assert!(matches!(
        repository
            .schedule_post(
                PostSchedule::try_new(
                    created.post.id,
                    requested_at + Duration::minutes(1),
                    "schedule-key",
                    scheduled.revision,
                )
                .unwrap()
            )
            .await,
        Err(sooqa_persistence::PublisherRepositoryError::RequestKeyConflict(_))
    ));
    let rescheduled = repository
        .schedule_post(
            PostSchedule::try_new(
                created.post.id,
                requested_at + Duration::hours(4),
                "reschedule-key",
                scheduled.revision,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let queued_run_at: OffsetDateTime =
        sqlx::query_scalar("SELECT run_at FROM queue.jobs WHERE dedupe_key = $1")
            .bind(format!("post:{}:publish:v1", created.post.id))
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert_eq!(queued_run_at, rescheduled.scheduled_at);
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn post_mutations_preserve_revision_fences(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let mut new_channel =
        NewChannel::try_new(format!("test-{}", Uuid::new_v4()), -1000000000001).unwrap();
    new_channel.window_start = time::Time::from_hms(0, 0, 0).unwrap();
    new_channel.window_end = time::Time::from_hms(23, 59, 0).unwrap();
    new_channel.interval_minutes = 1;
    let channel = database.publisher().create_channel(new_channel).await.unwrap();
    let media = [
        stored_media(&database).await,
        stored_media(&database).await,
        stored_media(&database).await,
    ];
    let posts = [
        database
            .publisher()
            .create_post_idempotent(
                NewPost {
                    media_id: media[0],
                    channel_id: channel.id,
                    caption: Some("first".to_owned()),
                    parse_mode: None,
                    disable_notification: false,
                },
                format!("post-{}", Uuid::new_v4()),
                b"first",
            )
            .await
            .unwrap()
            .post,
        database
            .publisher()
            .create_post_idempotent(
                NewPost {
                    media_id: media[1],
                    channel_id: channel.id,
                    caption: Some("second".to_owned()),
                    parse_mode: None,
                    disable_notification: false,
                },
                format!("post-{}", Uuid::new_v4()),
                b"second",
            )
            .await
            .unwrap()
            .post,
        database
            .publisher()
            .create_post_idempotent(
                NewPost {
                    media_id: media[2],
                    channel_id: channel.id,
                    caption: Some("third".to_owned()),
                    parse_mode: None,
                    disable_notification: false,
                },
                format!("post-{}", Uuid::new_v4()),
                b"third",
            )
            .await
            .unwrap()
            .post,
    ];
    let requested_at = OffsetDateTime::now_utc();
    let mut queued = Vec::new();
    for (index, post) in posts.iter().enumerate() {
        queued.push(
            database
                .publisher()
                .enqueue_post(
                    PostSchedule::try_new(
                        post.id,
                        requested_at,
                        format!("schedule-{index}-{}", Uuid::new_v4()),
                        0,
                    )
                    .unwrap(),
                )
                .await
                .unwrap(),
        );
    }
    assert!(queued.iter().all(|post| post.revision == 1));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM queue.jobs WHERE kind = 'publish_post' AND dedupe_key LIKE 'post:%:publish:v1' AND state = 'queued'",
        )
        .fetch_one(database.pool())
        .await
        .unwrap(),
        3
    );

    let first_slot = queued[0].cadence_slot_at.unwrap();
    let second_slot = queued[1].cadence_slot_at.unwrap();
    let third_slot = queued[2].cadence_slot_at.unwrap();
    assert!(first_slot < second_slot && second_slot < third_slot);
    let sending_before = database.publisher().find_post(queued[0].id).await.unwrap().unwrap();
    let sending_job = claim_publish_job(&database, sending_before.id).await;
    let sending_attempt = sending_job.lease().unwrap();
    let sending = database
        .publisher()
        .claim_publish(sending_before.id, sending_before.revision, &sending_attempt)
        .await
        .unwrap();
    let target_before = database.publisher().find_post(queued[2].id).await.unwrap().unwrap();
    let sending_job_before = publish_job_snapshot(&database, sending.post.id).await;
    let sending_after = database.publisher().find_post(sending.post.id).await.unwrap().unwrap();
    assert_eq!(sending_after.state, PostState::Sending);
    assert_eq!(sending_after.revision, sending.post.revision);
    assert_eq!(publish_job_snapshot(&database, sending.post.id).await, sending_job_before);

    let edited = database
        .publisher()
        .update_post(
            queued[2].id,
            PostUpdate {
                caption: Some(Some("edited".to_owned())),
                parse_mode: None,
                disable_notification: None,
                expected_updated_at: None,
                expected_revision: target_before.revision,
            },
        )
        .await
        .unwrap();
    assert_eq!(edited.caption.as_deref(), Some("edited"));
    assert_eq!(edited.revision, target_before.revision + 1);
    let job_payload: serde_json::Value =
        sqlx::query_scalar("SELECT payload FROM queue.jobs WHERE dedupe_key = $1")
            .bind(format!("post:{}:publish:v1", queued[2].id))
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert_eq!(job_payload["expected_revision"], edited.revision);
    assert_eq!(publish_job_snapshot(&database, sending.post.id).await, sending_job_before);

    let now = database
        .publisher()
        .publish_now(queued[2].id, "publish-now-test".to_owned(), edited.revision)
        .await
        .unwrap();
    assert!(now.cadence_slot_at.is_none());
    assert!(now.scheduled_at <= OffsetDateTime::now_utc());
    let now_job = publish_job_snapshot(&database, now.id).await;
    assert_eq!(now_job.1, now.scheduled_at);
    assert_eq!(now_job.2["expected_revision"], now.revision);
    let replay = database
        .publisher()
        .publish_now(queued[2].id, "publish-now-test".to_owned(), now.revision)
        .await
        .unwrap();
    assert_eq!(replay.revision, now.revision);
    assert!(replay.cadence_slot_at.is_none());

    let cancelled =
        database.publisher().cancel_post(queued[1].id, queued[1].revision).await.unwrap();
    assert_eq!(cancelled.state, PostState::Cancelled);
    assert!(cancelled.cadence_slot_at.is_none());
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM queue.jobs WHERE dedupe_key = $1",)
            .bind(format!("post:{}:publish:v1", queued[1].id))
            .fetch_one(database.pool())
            .await
            .unwrap(),
        "cancelled"
    );

    let stale = database
        .publisher()
        .update_post(
            queued[2].id,
            PostUpdate {
                caption: Some(None),
                parse_mode: None,
                disable_notification: None,
                expected_updated_at: None,
                expected_revision: edited.revision,
            },
        )
        .await;
    assert!(matches!(stale, Err(PublisherRepositoryError::OptimisticConflict(_))));
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn queued_post_cannot_be_rescheduled_after_publish_job_is_claimed(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let channel = database
        .publisher()
        .create_channel(
            NewChannel::try_new(format!("test-{}", Uuid::new_v4()), -1000000000002).unwrap(),
        )
        .await
        .unwrap();
    let media_id = stored_media(&database).await;
    let post = database
        .publisher()
        .create_post_idempotent(
            NewPost {
                media_id,
                channel_id: channel.id,
                caption: None,
                parse_mode: None,
                disable_notification: false,
            },
            format!("post-{}", Uuid::new_v4()),
            b"claim",
        )
        .await
        .unwrap()
        .post;
    let queued = database
        .publisher()
        .enqueue_post(
            PostSchedule::try_new(post.id, OffsetDateTime::now_utc(), "claim-schedule", 0).unwrap(),
        )
        .await
        .unwrap();
    let job = claim_publish_job(&database, queued.id).await;
    let attempt = job.lease().unwrap();
    let claimed =
        database.publisher().claim_publish(queued.id, queued.revision, &attempt).await.unwrap();
    assert_eq!(claimed.post.state, PostState::Sending);
    let stale_lease = PublishLease {
        generation: claimed.post.send_generation,
        token: Uuid::new_v4(),
        attempt: attempt.clone(),
    };
    assert!(matches!(
        database.publisher().complete_publish(queued.id, &stale_lease, 44).await,
        Err(PublisherRepositoryError::PublishLeaseLost(_))
    ));
    assert_eq!(
        database.publisher().find_post(queued.id).await.unwrap().unwrap().state,
        PostState::Sending
    );
    assert!(matches!(
        database.publisher().cancel_post(queued.id, claimed.post.revision).await,
        Err(PublisherRepositoryError::PostCannotBeScheduled { state: PostState::Sending, .. })
    ));
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn concurrent_publish_claim_and_edit_finish_without_deadlock(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let channel = database
        .publisher()
        .create_channel(
            NewChannel::try_new(format!("test-{}", Uuid::new_v4()), -1000000000006).unwrap(),
        )
        .await
        .unwrap();
    let media_id = stored_media(&database).await;
    let created = database
        .publisher()
        .create_post_idempotent(
            NewPost {
                media_id,
                channel_id: channel.id,
                caption: Some("before race".to_owned()),
                parse_mode: None,
                disable_notification: false,
            },
            format!("post-{}", Uuid::new_v4()),
            b"claim-edit-race",
        )
        .await
        .unwrap()
        .post;
    let queued = database
        .publisher()
        .enqueue_post(
            PostSchedule::try_new(
                created.id,
                OffsetDateTime::now_utc(),
                "claim-edit-race-schedule",
                created.revision,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let job = claim_publish_job(&database, queued.id).await;
    let attempt = job.lease().unwrap();
    let claim_repository = database.publisher();
    let edit_repository = database.publisher();
    let (claim, edit) = tokio::time::timeout(StdDuration::from_secs(5), async move {
        tokio::join!(
            claim_repository.claim_publish(queued.id, queued.revision, &attempt),
            edit_repository.update_post(
                queued.id,
                PostUpdate {
                    caption: Some(Some("edited during race".to_owned())),
                    parse_mode: None,
                    disable_notification: None,
                    expected_updated_at: None,
                    expected_revision: queued.revision,
                },
            ),
        )
    })
    .await
    .expect("claim and edit must not deadlock");

    let claim = claim.expect("the exact running publication attempt should win the claim");
    assert!(matches!(
        edit,
        Err(PublisherRepositoryError::PostNotEditable { .. })
            | Err(PublisherRepositoryError::PublishJobRunning(_))
    ));
    assert_eq!(claim.post.state, PostState::Sending);
    assert_eq!(claim.post.revision, queued.revision + 1);
    let (job_state, payload): (String, serde_json::Value) =
        sqlx::query_as("SELECT state, payload FROM queue.jobs WHERE id = $1")
            .bind(job.id)
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert_eq!(job_state, "running");
    assert_eq!(payload["post_id"], queued.id.to_string());
    assert_eq!(payload["expected_revision"], queued.revision);
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn safe_publication_retry_requeues_post_and_updates_job_revision(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let mut new_channel =
        NewChannel::try_new(format!("test-{}", Uuid::new_v4()), -1000000000003).unwrap();
    new_channel.window_start = time::Time::from_hms(0, 0, 0).unwrap();
    new_channel.window_end = time::Time::from_hms(23, 59, 0).unwrap();
    new_channel.interval_minutes = 1;
    let channel = database.publisher().create_channel(new_channel).await.unwrap();
    let media_id = stored_media(&database).await;
    let post = database
        .publisher()
        .create_post_idempotent(
            NewPost {
                media_id,
                channel_id: channel.id,
                caption: Some("retry me".to_owned()),
                parse_mode: None,
                disable_notification: false,
            },
            format!("post-{}", Uuid::new_v4()),
            b"safe-retry",
        )
        .await
        .unwrap()
        .post;
    let queued = database
        .publisher()
        .enqueue_post(
            PostSchedule::try_new(post.id, OffsetDateTime::now_utc(), "retry-schedule", 0).unwrap(),
        )
        .await
        .unwrap();
    let due = database
        .publisher()
        .publish_now(queued.id, "retry-now".to_owned(), queued.revision)
        .await
        .unwrap();
    let job = database
        .jobs()
        .claim_next("publisher-test", StdDuration::from_secs(60), &[JobType::PublishPost])
        .await
        .unwrap()
        .expect("publish job should be claimed");
    let attempt = job.lease().unwrap();
    let claim = database.publisher().claim_publish(due.id, due.revision, &attempt).await.unwrap();
    let token = claim.post.send_token.unwrap();
    let lease =
        PublishLease { generation: claim.post.send_generation, token, attempt: attempt.clone() };
    let retried = database
        .publisher()
        .retry_publish(queued.id, &lease, "telegram_retryable", "flood control")
        .await
        .unwrap();
    assert!(!retried.terminal);
    assert_eq!(retried.post.state, PostState::Queued);
    assert_eq!(retried.post.revision, claim.post.revision + 1);
    assert!(retried.post.send_token.is_none());
    let payload: serde_json::Value =
        sqlx::query_scalar("SELECT payload FROM queue.jobs WHERE dedupe_key = $1")
            .bind(format!("post:{}:publish:v1", queued.id))
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert_eq!(payload["expected_revision"], retried.post.revision);
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn failed_post_edit_keeps_failed_job_until_explicit_requeue(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let channel = database
        .publisher()
        .create_channel(
            NewChannel::try_new(format!("test-{}", Uuid::new_v4()), -1000000000004).unwrap(),
        )
        .await
        .unwrap();
    let media_id = stored_media(&database).await;
    let post = database
        .publisher()
        .create_post_idempotent(
            NewPost {
                media_id,
                channel_id: channel.id,
                caption: Some("before failure".to_owned()),
                parse_mode: None,
                disable_notification: false,
            },
            format!("post-{}", Uuid::new_v4()),
            b"failed-edit",
        )
        .await
        .unwrap()
        .post;
    let queued = database
        .publisher()
        .schedule_post(
            PostSchedule::try_new(post.id, OffsetDateTime::now_utc(), "initial", 0).unwrap(),
        )
        .await
        .unwrap();
    sqlx::query(
        "UPDATE posts SET state = 'failed', error_class = 'telegram', error_message = 'failed' WHERE id = $1",
    )
    .bind(queued.id)
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE queue.jobs SET state = 'failed', error_class = 'telegram', error_message = 'failed' WHERE dedupe_key = $1",
    )
    .bind(format!("post:{}:publish:v1", queued.id))
    .execute(database.pool())
    .await
    .unwrap();

    let failed = database.publisher().find_post(queued.id).await.unwrap().unwrap();
    let edited = database
        .publisher()
        .update_post(
            failed.id,
            PostUpdate {
                caption: Some(Some("edited after failure".to_owned())),
                parse_mode: None,
                disable_notification: None,
                expected_updated_at: None,
                expected_revision: failed.revision,
            },
        )
        .await
        .unwrap();
    assert_eq!(edited.state, PostState::Failed);
    let (job_state, payload, error_class): (String, serde_json::Value, Option<String>) =
        sqlx::query_as("SELECT state, payload, error_class FROM queue.jobs WHERE dedupe_key = $1")
            .bind(format!("post:{}:publish:v1", edited.id))
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert_eq!(job_state, "failed");
    assert_eq!(payload["expected_revision"], edited.revision);
    assert_eq!(error_class.as_deref(), Some("telegram"));

    let requeued = database
        .publisher()
        .schedule_post(
            PostSchedule::try_new(edited.id, OffsetDateTime::now_utc(), "retry", edited.revision)
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(requeued.state, PostState::Queued);
    let (job_state, payload): (String, serde_json::Value) =
        sqlx::query_as("SELECT state, payload FROM queue.jobs WHERE dedupe_key = $1")
            .bind(format!("post:{}:publish:v1", edited.id))
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert_eq!(job_state, "queued");
    assert_eq!(payload["expected_revision"], requeued.revision);
    let job = claim_publish_job(&database, requeued.id).await;
    let attempt = job.lease().unwrap();
    let claimed =
        database.publisher().claim_publish(requeued.id, requeued.revision, &attempt).await.unwrap();
    assert_eq!(claimed.post.state, PostState::Sending);
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn failed_post_and_job_are_cancelled_together(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let channel = database
        .publisher()
        .create_channel(
            NewChannel::try_new(format!("test-{}", Uuid::new_v4()), -1000000000005).unwrap(),
        )
        .await
        .unwrap();
    let media_id = stored_media(&database).await;
    let post = database
        .publisher()
        .create_post_idempotent(
            NewPost {
                media_id,
                channel_id: channel.id,
                caption: Some("failed cancellation".to_owned()),
                parse_mode: None,
                disable_notification: false,
            },
            format!("post-{}", Uuid::new_v4()),
            b"failed-cancellation",
        )
        .await
        .unwrap()
        .post;
    let queued = database
        .publisher()
        .schedule_post(
            PostSchedule::try_new(post.id, OffsetDateTime::now_utc(), "initial", 0).unwrap(),
        )
        .await
        .unwrap();
    sqlx::query(
        "UPDATE posts SET state = 'failed', error_class = 'telegram', error_message = 'failed' WHERE id = $1",
    )
    .bind(queued.id)
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE queue.jobs SET state = 'failed', error_class = 'telegram', error_message = 'failed' WHERE dedupe_key = $1",
    )
    .bind(format!("post:{}:publish:v1", queued.id))
    .execute(database.pool())
    .await
    .unwrap();

    let failed = database.publisher().find_post(queued.id).await.unwrap().unwrap();
    assert_eq!(failed.state, PostState::Failed);
    let cancelled = database.publisher().cancel_post(failed.id, failed.revision).await.unwrap();
    assert_eq!(cancelled.state, PostState::Cancelled);
    assert!(cancelled.cadence_slot_at.is_none());
    assert_eq!(cancelled.revision, failed.revision + 1);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM queue.jobs WHERE dedupe_key = $1")
            .bind(format!("post:{}:publish:v1", failed.id))
            .fetch_one(database.pool())
            .await
            .unwrap(),
        "cancelled"
    );
}
