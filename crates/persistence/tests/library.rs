use std::env;

use serde_json::json;
use sooqa_library::{
    MediaIngest, MediaKind, MediaMetadata, MediaSourceInput, NewMedia, NewTag, SourceKind,
};
use sooqa_persistence::Database;

async fn database() -> Database {
    let url = env::var("DATABASE_URL").expect("DATABASE_URL must point to PostgreSQL");
    let database = Database::connect(&url, 10).await.expect("database should connect");
    database.migrate().await.expect("migration should apply");
    database
}

fn ingest(sha256: Vec<u8>, source: &str) -> MediaIngest {
    MediaIngest {
        media: NewMedia {
            kind: MediaKind::Video,
            title: Some("test".to_owned()),
            description: None,
            notes: None,
        },
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
            sha256: Some(sha256),
            local_work_path: Some("/tmp/test.mp4".to_owned()),
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
            metadata: json!({"test": true}),
        },
        tags: vec!["rust".to_owned()],
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn media_aggregate_contains_source_and_tags_without_child_tables() {
    let database = database().await;
    let resolution = database
        .library()
        .resolve_media(ingest(vec![7_u8; 32], "https://example.test/video"))
        .await
        .unwrap();
    database
        .library()
        .add_tag(resolution.media.id, NewTag::try_new("Rust").unwrap())
        .await
        .unwrap();
    let details =
        database.library().find_media_details(resolution.media.id).await.unwrap().unwrap();
    assert_eq!(details.tags.len(), 1);
    assert!(details.source.is_some());
    assert_eq!(details.media.storage_state.as_str(), "pending_storage");
    sqlx::query("DELETE FROM media WHERE id = $1")
        .bind(resolution.media.id)
        .execute(database.pool())
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn concurrent_same_sha_resolves_to_one_media_row() {
    let database = database().await;
    let digest = vec![8_u8; 32];
    let left = ingest(digest.clone(), "https://example.test/left");
    let right = ingest(digest, "https://example.test/right");
    let repository = database.library();
    let (left, right) =
        tokio::join!(repository.resolve_media(left), repository.resolve_media(right));
    let left = left.unwrap();
    let right = right.unwrap();
    assert_eq!(left.media.id, right.media.id);
    assert!(left.media_created ^ right.media_created);
    sqlx::query("DELETE FROM media WHERE id = $1")
        .bind(left.media.id)
        .execute(database.pool())
        .await
        .unwrap();
}
