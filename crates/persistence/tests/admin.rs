use sooqa_inbox::{IngestSubmission, IngestSubmissionInput, SubmittedVia};
use sooqa_library::{
    MediaIngest, MediaKind, MediaLookup, MediaMetadata, MediaSourceInput, NewMedia, SourceKind,
};
use sooqa_persistence::{Database, PublisherRepositoryError};
use sooqa_publisher::{ChannelUpdate, NewChannel, NewPost, PostExactSchedule};
use time::OffsetDateTime;
use uuid::Uuid;

async fn ingest(database: &Database, suffix: &str) -> Uuid {
    database
        .inbox()
        .create_ingest(
            IngestSubmission::try_new(IngestSubmissionInput::new(
                format!("https://example.test/admin-{suffix}"),
                SubmittedVia::Api,
            ))
            .unwrap(),
        )
        .await
        .unwrap()
        .ingest
        .id
}

async fn stored_media(database: &Database, ingest_id: Option<Uuid>, source_url: &str) -> Uuid {
    let result = database
        .library()
        .resolve_media(MediaIngest {
            media: NewMedia::new(MediaKind::Video),
            metadata: MediaMetadata {
                kind: MediaKind::Video,
                mime_type: Some("video/webm".to_owned()),
                container: Some("webm".to_owned()),
                video_codec: None,
                audio_codec: None,
                width: Some(640),
                height: Some(360),
                duration_ms: Some(1_000),
                bit_rate: None,
                file_size_bytes: Some(100),
                sha256: Some(Uuid::new_v4().as_bytes().repeat(2)),
                local_work_path: None,
            },
            source: MediaSourceInput {
                ingest_id,
                kind: SourceKind::DirectUrl,
                original_url: Some(source_url.to_owned()),
                normalized_url: Some(source_url.to_owned()),
                platform: None,
                platform_content_id: None,
                author_name: None,
                title: None,
                description: None,
                published_at: None,
                metadata: serde_json::json!({"provenance": "kept"}),
            },
            tags: vec!["admin".to_owned()],
        })
        .await
        .unwrap();
    sqlx::query(
        "UPDATE media SET storage_state = 'ready', telegram_storage_chat_id = -1003971341583, telegram_storage_message_id = 57, telegram_file_id = 'admin-file' WHERE id = $1",
    )
    .bind(result.media.id)
    .execute(database.pool())
    .await
    .unwrap();
    if let Some(ingest_id) = ingest_id {
        sqlx::query("UPDATE ingests SET media_id = $2, state = 'completed', completed_at = now() WHERE id = $1")
            .bind(ingest_id)
            .bind(result.media.id)
            .execute(database.pool())
            .await
            .unwrap();
    }
    result.media.id
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn ingest_admin_cursor_is_bounded_and_stable_under_new_inserts(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    for suffix in ["one", "two", "three"] {
        ingest(&database, suffix).await;
    }
    let first = database.inbox().list_admin(2, None).await.unwrap();
    assert_eq!(first.items.len(), 2);
    let cursor = first.next_cursor.expect("first page should have a cursor");
    let newest = ingest(&database, "inserted-after-page").await;
    let second = database.inbox().list_admin(2, Some(cursor)).await.unwrap();
    assert_eq!(second.items.len(), 1);
    assert!(!second.items.iter().any(|item| item.id == newest));
    assert!(second.items.iter().all(|item| item.error_message.is_none()));
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn media_lookup_resolves_ids_mirrors_and_private_storage_links(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let ingest_id = ingest(&database, "lookup").await;
    let media_id =
        stored_media(&database, Some(ingest_id), "https://2ch.su/b/src/admin.webm").await;

    let mirror_page = database
        .library()
        .lookup_media(
            MediaLookup::SourceUrls(vec![
                "https://2ch.org/b/src/admin.webm".to_owned(),
                "https://2ch.su/b/src/admin.webm".to_owned(),
                "https://2ch.life/b/src/admin.webm".to_owned(),
            ]),
            50,
            None,
        )
        .await
        .unwrap();
    assert_eq!(mirror_page.items.len(), 1);
    assert_eq!(mirror_page.items[0].media.id, media_id);
    assert_eq!(mirror_page.items[0].storage_url.as_deref(), Some("https://t.me/c/3971341583/57"));

    let ingest_page = database
        .library()
        .lookup_media(MediaLookup::Identifier(ingest_id), 50, None)
        .await
        .unwrap();
    assert_eq!(ingest_page.items[0].media.id, media_id);
    let storage_page = database
        .library()
        .lookup_media(
            MediaLookup::StorageMessage { chat_id: -1003971341583, message_id: 57 },
            50,
            None,
        )
        .await
        .unwrap();
    assert_eq!(storage_page.items[0].media.id, media_id);
    assert_eq!(
        database
            .library()
            .find_media_details(media_id)
            .await
            .unwrap()
            .unwrap()
            .source
            .unwrap()
            .original_url,
        Some("https://2ch.su/b/src/admin.webm".to_owned())
    );
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn schedule_page_is_bounded_and_channel_updates_are_revision_fenced(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let mut channel = NewChannel::try_new("admin-channel", -1003971341590).unwrap();
    channel.window_start = time::Time::MIDNIGHT;
    channel.window_end = time::Time::from_hms(23, 59, 0).unwrap();
    let channel = database.publisher().create_channel(channel).await.unwrap();
    let media_id = stored_media(&database, None, "https://example.test/schedule.webm").await;
    sqlx::query("UPDATE media SET source_metadata = source_metadata || jsonb_build_object('caption_sync_state', 'failed', 'caption_sync_error', 'caption sync failed') WHERE id = $1")
        .bind(media_id)
        .execute(database.pool())
        .await
        .unwrap();
    let caption_failures = database.library().list_caption_sync_failures(50).await.unwrap();
    assert_eq!(caption_failures.len(), 1);
    assert_eq!(caption_failures[0].media_id, media_id);
    assert_eq!(caption_failures[0].error_message.as_deref(), Some("caption sync failed"));
    let post = database
        .publisher()
        .create_post_idempotent(
            NewPost {
                media_id,
                channel_id: channel.id,
                caption: Some("schedule card".to_owned()),
                parse_mode: None,
                disable_notification: false,
            },
            "admin-schedule-post".to_owned(),
            b"admin-schedule-post",
        )
        .await
        .unwrap()
        .post;
    let exact =
        (OffsetDateTime::now_utc() + time::Duration::hours(2)).replace_nanosecond(0).unwrap();
    database
        .publisher()
        .schedule_post_exact(PostExactSchedule::try_new(post.id, exact, "admin-exact", 0).unwrap())
        .await
        .unwrap();
    let page = database.publisher().list_posts(50, None, false).await.unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].post.requested_publish_at, Some(exact));
    assert_eq!(page.items[0].storage_url.as_deref(), Some("https://t.me/c/3971341583/57"));

    let updated = database
        .publisher()
        .update_channel(
            channel.id,
            ChannelUpdate {
                name: Some("renamed".to_owned()),
                expected_updated_at: Some(channel.updated_at),
                ..ChannelUpdate::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.name, "renamed");
    let stale = database
        .publisher()
        .update_channel(
            channel.id,
            ChannelUpdate {
                name: Some("stale".to_owned()),
                expected_updated_at: Some(channel.updated_at),
                ..ChannelUpdate::default()
            },
        )
        .await;
    assert!(matches!(stale, Err(PublisherRepositoryError::ChannelOptimisticConflict(_))));
    let invalid = database
        .publisher()
        .update_channel(
            channel.id,
            ChannelUpdate {
                time_zone: Some("Not/AZone".to_owned()),
                expected_updated_at: Some(updated.updated_at),
                ..ChannelUpdate::default()
            },
        )
        .await;
    assert!(matches!(invalid, Err(PublisherRepositoryError::ChannelValidation(_))));
}
