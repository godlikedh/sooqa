use std::env;

use serde_json::json;
use sooqa_library::{
    AssetRole, ContentKind, ExactDuplicateRequest, MediaKind, NewContentItem, NewMediaAssetDraft,
    NewSourceRecordDraft, NewTag, SourceType, StorageState,
};
use sooqa_persistence::Database;

async fn database() -> Database {
    let url = env::var("DATABASE_URL").expect("DATABASE_URL must point to PostgreSQL");
    let database = Database::connect(&url, 10).await.expect("database should connect");
    database.migrate().await.expect("migration should apply");
    database
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn normalized_media_row_contains_source_and_tags_without_child_tables() {
    let database = database().await;
    let digest = vec![7_u8; 32];
    let resolution = database
        .library()
        .resolve_exact_duplicate(ExactDuplicateRequest {
            content_item: NewContentItem {
                kind: ContentKind::Video,
                preferred_title: Some("test".to_owned()),
                editorial_description: None,
                notes: None,
            },
            asset: NewMediaAssetDraft {
                role: AssetRole::Canonical,
                media_kind: MediaKind::Video,
                mime_type: Some("video/mp4".to_owned()),
                container: None,
                video_codec: None,
                audio_codec: None,
                width: None,
                height: None,
                duration_ms: None,
                bit_rate: None,
                file_size_bytes: Some(1),
                sha256: Some(digest),
                local_work_path: Some("/tmp/test.mp4".to_owned()),
                storage_state: StorageState::Local,
            },
            source: NewSourceRecordDraft {
                ingest_request_id: None,
                source_type: SourceType::DirectUrl,
                original_url: Some("https://example.test/video".to_owned()),
                normalized_url: Some("https://example.test/video".to_owned()),
                platform: None,
                platform_content_id: None,
                author_name: None,
                source_title: None,
                source_description: None,
                source_published_at: None,
                metadata_json: json!({"test": true}),
            },
        })
        .await
        .unwrap();
    let tag = database
        .library()
        .add_tag(resolution.content_item.id, NewTag::try_new("Rust").unwrap())
        .await
        .unwrap();
    assert_eq!(tag.normalized_name, "rust");
    let item =
        database.library().find_library_item(resolution.content_item.id).await.unwrap().unwrap();
    assert_eq!(item.tags.len(), 1);
    assert_eq!(item.sources.len(), 1);
    sqlx::query("DELETE FROM media WHERE id = $1")
        .bind(resolution.content_item.id)
        .execute(database.pool())
        .await
        .unwrap();
}
