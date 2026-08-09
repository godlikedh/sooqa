use std::env;

use serde_json::json;
use sooqa_library::{
    MediaIngest, MediaKind, MediaMetadata, MediaSourceInput, NewMedia, SourceKind,
};
use sooqa_persistence::Database;
use sooqa_publisher::{NewChannel, NewPost, PostSchedule, PostState, PostUpdate};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

async fn database() -> Database {
    let url = env::var("DATABASE_URL").expect("DATABASE_URL must point to PostgreSQL");
    let database = Database::connect(&url, 10).await.expect("database should connect");
    database.migrate().await.expect("migration should apply");
    database
}

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

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn post_schedule_uses_one_row_and_channel_cadence() {
    let database = database().await;
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
            PostSchedule::try_new(created.post.id, requested_at, "schedule-key").unwrap()
        ),
        repository.schedule_post(
            PostSchedule::try_new(second.post.id, requested_at, "second-schedule-key").unwrap()
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
            PostSchedule::try_new(created.post.id, requested_at, "schedule-key").unwrap(),
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
                    "schedule-key"
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
    sqlx::query("DELETE FROM posts WHERE id IN ($1, $2)")
        .bind(created.post.id)
        .bind(second.post.id)
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM channels WHERE id = $1")
        .bind(channel.id)
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM media WHERE id = $1")
        .bind(media_id)
        .execute(database.pool())
        .await
        .unwrap();
}
