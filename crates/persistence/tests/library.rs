use std::env;

use serde_json::json;
use sooqa_inbox::{IngestSubmission, IngestSubmissionInput, SubmittedVia};
use sooqa_library::{
    MediaIngest, MediaKind, MediaMetadata, MediaSearchQuery, MediaSourceInput, MediaStatus,
    NewMedia, NewTag, SourceKind, StorageUploadAttachment, StorageUploadReservation,
    StorageUploadReservationRequest, StorageUploadStore,
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
    let page = database
        .library()
        .search_media(MediaSearchQuery {
            text: None,
            tags: vec!["rust".to_owned()],
            kind: Some(MediaKind::Video),
            status: Some(MediaStatus::Active),
            limit: 10,
            cursor: None,
        })
        .await
        .unwrap();
    assert_eq!(page.items.iter().filter(|item| item.media.id == resolution.media.id).count(), 1);
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

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn exact_sha_dedup_preserves_primary_source_metadata() {
    let database = database().await;
    let repository = database.library();
    let first = repository
        .resolve_media(ingest(vec![6_u8; 32], "https://example.test/primary"))
        .await
        .unwrap();
    let second = repository
        .resolve_media(ingest(vec![6_u8; 32], "https://example.test/duplicate"))
        .await
        .unwrap();
    assert!(first.media_created);
    assert!(!second.media_created);
    assert_eq!(first.media.id, second.media.id);
    let details = database.library().find_media_details(first.media.id).await.unwrap().unwrap();
    assert_eq!(
        details.source.unwrap().original_url.as_deref(),
        Some("https://example.test/primary")
    );
    sqlx::query("DELETE FROM media WHERE id = $1")
        .bind(first.media.id)
        .execute(database.pool())
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn storage_reconciliation_reopens_and_completes_linked_ingest() {
    let database = database().await;
    let media = database
        .library()
        .resolve_media(ingest(vec![9_u8; 32], "https://example.test/reconcile"))
        .await
        .unwrap();
    let ingest = database
        .inbox()
        .create_ingest(
            IngestSubmission::try_new(IngestSubmissionInput::new(
                "https://example.test/reconcile-ingest",
                SubmittedVia::Api,
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    sqlx::query(
        "UPDATE ingests SET media_id = $2, state = 'storing', completed_at = NULL WHERE id = $1",
    )
    .bind(ingest.ingest.id)
    .bind(media.media.id)
    .execute(database.pool())
    .await
    .unwrap();

    let initial_reservation = database
        .library()
        .reserve_storage_upload(StorageUploadReservationRequest {
            media_id: media.media.id,
            generation: 0,
        })
        .await
        .unwrap();
    let initial_owner_token = match initial_reservation {
        StorageUploadReservation::Reserved { owner_token, .. } => owner_token,
        other => panic!("expected a fresh storage reservation, got {other:?}"),
    };
    database
        .library()
        .complete_storage_upload(
            media.media.id,
            initial_owner_token,
            StorageUploadAttachment {
                storage_chat_id: -100123,
                storage_message_id: 40,
                telegram_file_id: Some("file-40".to_owned()),
                telegram_file_unique_id: Some("unique-40".to_owned()),
            },
        )
        .await
        .unwrap();
    assert_eq!(database.inbox().complete_storage_for_media(media.media.id).await.unwrap(), 1);
    let initially_ready =
        database.library().find_storage_receipt(media.media.id).await.unwrap().unwrap();
    assert_eq!(initially_ready.storage_message_id, 40);
    let initially_completed = database.inbox().find(ingest.ingest.id).await.unwrap().unwrap();
    assert_eq!(initially_completed.status.as_str(), "completed");

    database.library().mark_storage_upload_unknown(media.media.id, true).await.unwrap();
    let failed = database.inbox().find(ingest.ingest.id).await.unwrap().unwrap();
    assert_eq!(failed.status.as_str(), "failed_terminal");
    assert_eq!(failed.error_code.as_deref(), Some("storage_unknown"));
    assert!(matches!(
        database
            .library()
            .reserve_storage_upload(StorageUploadReservationRequest {
                media_id: media.media.id,
                generation: 0,
            })
            .await
            .unwrap(),
        StorageUploadReservation::ReconciliationRequired
    ));

    database
        .library()
        .attach_storage_upload(
            media.media.id,
            0,
            StorageUploadAttachment {
                storage_chat_id: -100123,
                storage_message_id: 41,
                telegram_file_id: Some("file-41".to_owned()),
                telegram_file_unique_id: Some("unique-41".to_owned()),
            },
        )
        .await
        .unwrap();
    let attached = database.inbox().find(ingest.ingest.id).await.unwrap().unwrap();
    assert_eq!(attached.status.as_str(), "completed");

    database.library().reset_storage_upload(media.media.id).await.unwrap();
    let reopened = database.inbox().find(ingest.ingest.id).await.unwrap().unwrap();
    assert_eq!(reopened.status.as_str(), "storing");
    let reservation = database
        .library()
        .reserve_storage_upload(StorageUploadReservationRequest {
            media_id: media.media.id,
            generation: 1,
        })
        .await
        .unwrap();
    let owner_token = match reservation {
        StorageUploadReservation::Reserved { owner_token, .. } => owner_token,
        other => panic!("expected a fresh storage reservation, got {other:?}"),
    };
    database
        .library()
        .complete_storage_upload(
            media.media.id,
            owner_token,
            StorageUploadAttachment {
                storage_chat_id: -100123,
                storage_message_id: 42,
                telegram_file_id: Some("file-42".to_owned()),
                telegram_file_unique_id: Some("unique-42".to_owned()),
            },
        )
        .await
        .unwrap();
    database.inbox().complete_storage_for_media(media.media.id).await.unwrap();
    let completed = database.inbox().find(ingest.ingest.id).await.unwrap().unwrap();
    assert_eq!(completed.status.as_str(), "completed");
    let receipt = database.library().find_storage_receipt(media.media.id).await.unwrap().unwrap();
    assert_eq!(receipt.storage_message_id, 42);

    sqlx::query(
        "DELETE FROM queue.jobs WHERE payload->>'ingest_id' = $1 OR payload->>'media_id' = $2",
    )
    .bind(ingest.ingest.id.to_string())
    .bind(media.media.id.to_string())
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query("DELETE FROM ingests WHERE id = $1")
        .bind(ingest.ingest.id)
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM media WHERE id = $1")
        .bind(media.media.id)
        .execute(database.pool())
        .await
        .unwrap();
}
