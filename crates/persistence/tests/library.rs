use std::env;

use serde_json::json;
use sooqa_library::{
    AssetRole, ContentKind, MediaKind, NewContentItem, NewMediaAsset, NewSourceRecord,
    NewStorageObject, NewTag, SourceType, StorageState,
};
use sooqa_persistence::Database;
use uuid::Uuid;

async fn clean_up(database: &Database, content_id: Uuid, tag_id: Uuid, normalized_url: &str) {
    sqlx::query("DELETE FROM storage_objects WHERE asset_id IN (SELECT id FROM media_assets WHERE content_item_id = $1)")
        .bind(content_id)
        .execute(database.pool())
        .await
        .expect("storage objects should clean up");
    sqlx::query("DELETE FROM source_records WHERE normalized_url = $1")
        .bind(normalized_url)
        .execute(database.pool())
        .await
        .expect("source records should clean up");
    sqlx::query("DELETE FROM content_item_tags WHERE content_item_id = $1")
        .bind(content_id)
        .execute(database.pool())
        .await
        .expect("tag attachments should clean up");
    sqlx::query("DELETE FROM tags WHERE id = $1")
        .bind(tag_id)
        .execute(database.pool())
        .await
        .expect("tag should clean up");
    sqlx::query("DELETE FROM media_assets WHERE content_item_id = $1")
        .bind(content_id)
        .execute(database.pool())
        .await
        .expect("media assets should clean up");
    sqlx::query("DELETE FROM content_items WHERE id = $1")
        .bind(content_id)
        .execute(database.pool())
        .await
        .expect("content item should clean up");
}

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance"]
async fn library_repositories_round_trip_content_sources_tags_and_storage() {
    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must point to the integration database");
    let database =
        Database::connect(&database_url, 10).await.expect("database should be reachable");
    database.migrate().await.expect("migrations should succeed");

    let library = database.library();
    let content = library
        .create_content_item(NewContentItem {
            kind: ContentKind::Video,
            preferred_title: Some("Library test video".to_owned()),
            editorial_description: Some("A repository round-trip".to_owned()),
            notes: None,
        })
        .await
        .expect("content item should be created");
    let normalized_url = format!("https://library.test/source/{}", Uuid::new_v4());
    let asset = library
        .create_media_asset(NewMediaAsset {
            content_item_id: content.id,
            role: AssetRole::Original,
            media_kind: MediaKind::Video,
            mime_type: Some("video/webm".to_owned()),
            container: Some("webm".to_owned()),
            video_codec: Some("vp9".to_owned()),
            audio_codec: Some("opus".to_owned()),
            width: Some(1280),
            height: Some(720),
            duration_ms: Some(3500),
            bit_rate: Some(500_000),
            file_size_bytes: Some(42),
            sha256: Some(vec![7; 32]),
            local_work_path: Some("/var/lib/sooqa/work/jobs/test/source/original.webm".to_owned()),
            storage_state: StorageState::Local,
        })
        .await
        .expect("media asset should be created");
    let source = library
        .create_source_record(NewSourceRecord {
            content_item_id: content.id,
            ingest_request_id: None,
            source_type: SourceType::Webpage,
            original_url: Some(format!("{normalized_url}?utm_source=test")),
            normalized_url: Some(normalized_url.clone()),
            platform: Some("test".to_owned()),
            platform_content_id: Some(Uuid::new_v4().to_string()),
            author_name: Some("Test author".to_owned()),
            source_title: Some("Source title".to_owned()),
            source_description: None,
            source_published_at: None,
            metadata_json: json!({"adapter": "test"}),
        })
        .await
        .expect("source record should be created");
    let tag = library
        .upsert_tag(NewTag::try_new("  Rust  ").expect("tag should be valid"))
        .await
        .expect("tag should be created");
    library.attach_tag(content.id, tag.id).await.expect("tag should attach");
    let storage = library
        .create_storage_object(NewStorageObject {
            asset_id: asset.id,
            provider: "telegram".to_owned(),
            storage_chat_id: -100123,
            storage_message_id: 456,
            telegram_file_id: Some("file-id".to_owned()),
            telegram_file_unique_id: Some("unique-id".to_owned()),
            media_kind: MediaKind::Video,
        })
        .await
        .expect("storage object should be created");

    let loaded_content = library
        .find_content_item(content.id)
        .await
        .expect("content item should load")
        .expect("content item should exist");
    assert_eq!(loaded_content, content);
    assert_eq!(
        library
            .find_media_asset(asset.id)
            .await
            .expect("asset should load")
            .expect("asset should exist"),
        asset
    );
    assert_eq!(
        library.list_source_records(content.id).await.expect("sources should load"),
        vec![source]
    );
    assert_eq!(library.list_tags(content.id).await.expect("tags should load"), vec![tag.clone()]);
    assert_eq!(storage.asset_id, asset.id);
    assert_eq!(storage.provider, "telegram");

    clean_up(&database, content.id, tag.id, &normalized_url).await;
}
