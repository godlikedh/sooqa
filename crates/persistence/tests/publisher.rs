use std::env;

use sooqa_library::{
    AssetRole, ContentKind, ExactDuplicateRequest, MediaKind, NewContentItem, NewMediaAssetDraft,
    NewSourceRecordDraft, SourceType, StorageState,
};
use sooqa_persistence::Database;
use sooqa_publisher::{NewPostDraft, NewPublicationSchedule, NewTargetChannel, PostDraftStatus};
use time::OffsetDateTime;
use uuid::Uuid;

async fn database() -> Database {
    let url = env::var("DATABASE_URL").expect("DATABASE_URL must point to PostgreSQL");
    let database = Database::connect(&url, 10).await.expect("database should connect");
    database.migrate().await.expect("migration should apply");
    database
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn one_post_row_carries_schedule_and_publication_state() {
    let database = database().await;
    let channel = database
        .publisher()
        .create_target_channel(
            NewTargetChannel::try_new(format!("test-{}", Uuid::new_v4()), -1000000000000).unwrap(),
        )
        .await
        .unwrap();
    let item = database
        .library()
        .resolve_exact_duplicate(ExactDuplicateRequest {
            content_item: NewContentItem::new(ContentKind::Video),
            asset: NewMediaAssetDraft {
                role: AssetRole::Canonical,
                media_kind: MediaKind::Video,
                mime_type: None,
                container: None,
                video_codec: None,
                audio_codec: None,
                width: None,
                height: None,
                duration_ms: None,
                bit_rate: None,
                file_size_bytes: Some(1),
                sha256: Some(vec![8; 32]),
                local_work_path: None,
                storage_state: StorageState::Local,
            },
            source: NewSourceRecordDraft {
                ingest_request_id: None,
                source_type: SourceType::DirectUrl,
                original_url: Some(format!("https://example.test/{}", Uuid::new_v4())),
                normalized_url: None,
                platform: None,
                platform_content_id: None,
                author_name: None,
                source_title: None,
                source_description: None,
                source_published_at: None,
                metadata_json: serde_json::json!({}),
            },
        })
        .await
        .unwrap();
    let draft = database
        .publisher()
        .create_post_draft(NewPostDraft {
            content_item_id: item.content_item.id,
            asset_id: item.canonical_asset.id,
            target_channel_id: channel.id,
            caption: Some("hello".to_owned()),
            parse_mode: None,
        })
        .await
        .unwrap();
    assert_eq!(draft.status, PostDraftStatus::Editing);
    let ready = database
        .publisher()
        .update_post_draft(
            draft.id,
            sooqa_publisher::PostDraftUpdate {
                caption: None,
                parse_mode: None,
                status: Some(PostDraftStatus::Ready),
                expected_updated_at: None,
            },
        )
        .await
        .unwrap();
    let schedule = database
        .publisher()
        .create_publication_schedule(
            NewPublicationSchedule::try_new(
                ready.id,
                OffsetDateTime::now_utc(),
                format!("schedule-{}", Uuid::new_v4()),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(schedule.post_draft_id, draft.id);
    let due = database
        .publisher()
        .list_due_publication_schedules(OffsetDateTime::now_utc(), 10)
        .await
        .unwrap();
    assert!(due.iter().any(|row| row.id == schedule.id));
    sqlx::query("DELETE FROM posts WHERE id = $1")
        .bind(draft.id)
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM channels WHERE id = $1")
        .bind(channel.id)
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM media WHERE id = $1")
        .bind(item.content_item.id)
        .execute(database.pool())
        .await
        .unwrap();
}
