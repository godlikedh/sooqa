use std::env;

use serde_json::json;
use sooqa_library::{
    AssetRole, ContentKind, ExactDuplicateRequest, MediaKind, NewContentItem,
    NewDuplicateCandidate, NewMediaAsset, NewMediaAssetDraft, NewSourceRecord,
    NewSourceRecordDraft, NewStorageObject, NewTag, SourceType, StorageState,
    StorageUploadReservation, StorageUploadStore,
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

async fn clean_up_content(database: &Database, content_id: Uuid) {
    sqlx::query("DELETE FROM source_records WHERE content_item_id = $1")
        .bind(content_id)
        .execute(database.pool())
        .await
        .expect("source records should clean up");
    sqlx::query("UPDATE content_items SET canonical_asset_id = NULL WHERE id = $1")
        .bind(content_id)
        .execute(database.pool())
        .await
        .expect("canonical asset reference should clean up");
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

fn canonical_asset(content_item_id: Uuid, sha256: Vec<u8>) -> NewMediaAsset {
    NewMediaAsset {
        content_item_id,
        role: AssetRole::Canonical,
        media_kind: MediaKind::Video,
        mime_type: Some("video/mp4".to_owned()),
        container: Some("mp4".to_owned()),
        video_codec: Some("h264".to_owned()),
        audio_codec: Some("aac".to_owned()),
        width: Some(320),
        height: Some(240),
        duration_ms: Some(1_000),
        bit_rate: Some(100_000),
        file_size_bytes: Some(8),
        sha256: Some(sha256),
        local_work_path: Some(format!(
            "/var/lib/sooqa/work/jobs/test/{content_item_id}/normalized.mp4"
        )),
        storage_state: StorageState::Local,
    }
}

fn exact_duplicate_request(sha256: Vec<u8>, normalized_url: &str) -> ExactDuplicateRequest {
    ExactDuplicateRequest {
        content_item: NewContentItem {
            kind: ContentKind::Video,
            preferred_title: Some("Exact duplicate test".to_owned()),
            editorial_description: None,
            notes: None,
        },
        asset: NewMediaAssetDraft {
            role: AssetRole::Canonical,
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
            sha256: Some(sha256),
            local_work_path: Some("/var/lib/sooqa/work/jobs/test/canonical.webm".to_owned()),
            storage_state: StorageState::Local,
        },
        source: NewSourceRecordDraft {
            ingest_request_id: None,
            source_type: SourceType::DirectUrl,
            original_url: Some(normalized_url.to_owned()),
            normalized_url: Some(normalized_url.to_owned()),
            platform: None,
            platform_content_id: None,
            author_name: None,
            source_title: Some("Exact duplicate source".to_owned()),
            source_description: None,
            source_published_at: None,
            metadata_json: json!({"test": true}),
        },
    }
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

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance"]
async fn storage_upload_intent_is_idempotent_and_marks_asset_uploaded() {
    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must point to the integration database");
    let database =
        Database::connect(&database_url, 10).await.expect("database should be reachable");
    database.migrate().await.expect("migrations should succeed");

    let library = database.library();
    let content = library
        .create_content_item(NewContentItem::new(ContentKind::Video))
        .await
        .expect("content item should be created");
    let sha256 = vec![13; 32];
    let asset = library
        .create_media_asset(canonical_asset(content.id, sha256.clone()))
        .await
        .expect("asset should be created");
    let idempotency_key = format!("telegram:storage:{}:v1", asset.id);

    let reservation = library
        .reserve_storage_upload(asset.id, "telegram", &idempotency_key, &sha256)
        .await
        .expect("upload intent should be reserved");
    let StorageUploadReservation::Reserved { intent_id } = reservation else {
        panic!("first upload should reserve an intent");
    };
    assert_eq!(
        library
            .reserve_storage_upload(asset.id, "telegram", &idempotency_key, &sha256)
            .await
            .expect("duplicate reservation should load"),
        StorageUploadReservation::InProgress
    );
    sqlx::query(
        "UPDATE idempotency_records SET expires_at = now() - interval '1 minute' WHERE id = $1",
    )
    .bind(intent_id)
    .execute(database.pool())
    .await
    .expect("upload lease should be adjustable in the fixture");
    assert!(matches!(
        library
            .reserve_storage_upload(asset.id, "telegram", &idempotency_key, &sha256)
            .await
            .expect("expired upload lease should be reclaimable"),
        StorageUploadReservation::Reserved { intent_id: reclaimed } if reclaimed == intent_id
    ));
    library
        .mark_storage_upload_unknown(intent_id)
        .await
        .expect("upload intent should be markable as unknown");
    assert_eq!(
        library
            .reserve_storage_upload(asset.id, "telegram", &idempotency_key, &sha256)
            .await
            .expect("unknown upload should load"),
        StorageUploadReservation::InProgress
    );

    let stored = library
        .complete_storage_upload(
            intent_id,
            NewStorageObject {
                asset_id: asset.id,
                provider: "telegram".to_owned(),
                storage_chat_id: -100123,
                storage_message_id: 789,
                telegram_file_id: Some("file-id".to_owned()),
                telegram_file_unique_id: Some("unique-id".to_owned()),
                media_kind: MediaKind::Video,
            },
        )
        .await
        .expect("upload intent should complete");
    assert_eq!(stored.asset_id, asset.id);
    assert_eq!(
        library
            .find_media_asset(asset.id)
            .await
            .expect("asset should load")
            .expect("asset should exist")
            .storage_state,
        StorageState::Uploaded
    );
    assert_eq!(
        library
            .reserve_storage_upload(asset.id, "telegram", &idempotency_key, &sha256)
            .await
            .expect("completed reservation should replay"),
        StorageUploadReservation::Reused(stored.clone())
    );

    sqlx::query("DELETE FROM idempotency_records WHERE idempotency_key = $1")
        .bind(&idempotency_key)
        .execute(database.pool())
        .await
        .expect("upload intent should clean up");
    sqlx::query("DELETE FROM storage_objects WHERE asset_id = $1")
        .bind(asset.id)
        .execute(database.pool())
        .await
        .expect("storage object should clean up");
    clean_up_content(&database, content.id).await;
}

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance"]
async fn exact_duplicate_resolution_reuses_content_and_attaches_new_sources() {
    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must point to the integration database");
    let database =
        Database::connect(&database_url, 10).await.expect("database should be reachable");
    database.migrate().await.expect("migrations should succeed");
    let library = database.library();
    let sha256 = vec![8; 32];
    let first_url = format!("https://library.test/exact/{}", Uuid::new_v4());
    let second_url = format!("https://library.test/exact/{}", Uuid::new_v4());

    let created = library
        .resolve_exact_duplicate(exact_duplicate_request(sha256.clone(), &first_url))
        .await
        .expect("first exact duplicate resolution should succeed");
    assert!(created.content_created);
    assert!(created.source_created);
    assert_eq!(created.content_item.canonical_asset_id, Some(created.canonical_asset.id));
    assert_eq!(created.canonical_asset.role, AssetRole::Canonical);

    let replayed = library
        .resolve_exact_duplicate(exact_duplicate_request(sha256.clone(), &first_url))
        .await
        .expect("source replay should succeed");
    assert!(!replayed.content_created);
    assert!(!replayed.source_created);
    assert_eq!(replayed.content_item.id, created.content_item.id);
    assert_eq!(replayed.canonical_asset.id, created.canonical_asset.id);
    assert_eq!(replayed.source_record.id, created.source_record.id);

    let attached = library
        .resolve_exact_duplicate(exact_duplicate_request(sha256, &second_url))
        .await
        .expect("same-file duplicate should succeed");
    assert!(!attached.content_created);
    assert!(attached.source_created);
    assert_eq!(attached.content_item.id, created.content_item.id);
    assert_eq!(attached.canonical_asset.id, created.canonical_asset.id);
    assert_eq!(
        library
            .list_source_records(created.content_item.id)
            .await
            .expect("sources should load")
            .len(),
        2
    );

    clean_up_content(&database, created.content_item.id).await;
}

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance"]
async fn exact_duplicate_resolution_checks_platform_identity_before_hash() {
    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must point to the integration database");
    let database =
        Database::connect(&database_url, 10).await.expect("database should be reachable");
    database.migrate().await.expect("migrations should succeed");
    let library = database.library();
    let first_url = format!("https://library.test/platform/{}", Uuid::new_v4());
    let second_url = format!("https://library.test/platform/{}", Uuid::new_v4());
    let mut first_request = exact_duplicate_request(vec![10; 32], &first_url);
    first_request.source.platform = Some("youtube".to_owned());
    first_request.source.platform_content_id = Some("video-123".to_owned());

    let created = library
        .resolve_exact_duplicate(first_request)
        .await
        .expect("platform source should be created");

    let mut replay_request = exact_duplicate_request(vec![11; 32], &second_url);
    replay_request.source.platform = Some("youtube".to_owned());
    replay_request.source.platform_content_id = Some("video-123".to_owned());
    let replayed = library
        .resolve_exact_duplicate(replay_request)
        .await
        .expect("platform identity replay should succeed");

    assert!(!replayed.content_created);
    assert!(!replayed.source_created);
    assert_eq!(replayed.content_item.id, created.content_item.id);
    assert_eq!(replayed.canonical_asset.id, created.canonical_asset.id);
    assert_eq!(replayed.source_record.id, created.source_record.id);

    clean_up_content(&database, created.content_item.id).await;
}

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance"]
async fn exact_duplicate_resolution_converges_under_concurrency() {
    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must point to the integration database");
    let database =
        Database::connect(&database_url, 10).await.expect("database should be reachable");
    database.migrate().await.expect("migrations should succeed");
    let library = database.library();
    let sha256 = vec![9; 32];
    let left_url = format!("https://library.test/race/{}", Uuid::new_v4());
    let right_url = format!("https://library.test/race/{}", Uuid::new_v4());

    let (left, right) = tokio::join!(
        library.resolve_exact_duplicate(exact_duplicate_request(sha256.clone(), &left_url)),
        library.resolve_exact_duplicate(exact_duplicate_request(sha256, &right_url)),
    );
    let left = left.expect("left resolution should succeed");
    let right = right.expect("right resolution should succeed");

    assert_ne!(left.content_created, right.content_created);
    assert!(left.source_created);
    assert!(right.source_created);
    assert_eq!(left.content_item.id, right.content_item.id);
    assert_eq!(left.canonical_asset.id, right.canonical_asset.id);

    let canonical_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM media_assets WHERE content_item_id = $1 AND role = 'canonical'",
    )
    .bind(left.content_item.id)
    .fetch_one(database.pool())
    .await
    .expect("canonical asset count should load");
    assert_eq!(canonical_count, 1);

    let source_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM source_records WHERE content_item_id = $1")
            .bind(left.content_item.id)
            .fetch_one(database.pool())
            .await
            .expect("source count should load");
    assert_eq!(source_count, 2);

    clean_up_content(&database, left.content_item.id).await;
}

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance"]
async fn recording_canonical_asset_is_idempotent_and_hash_unique() {
    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must point to the integration database");
    let database =
        Database::connect(&database_url, 10).await.expect("database should be reachable");
    database.migrate().await.expect("migrations should succeed");
    let library = database.library();
    let first = library
        .create_content_item(NewContentItem::new(ContentKind::Video))
        .await
        .expect("first content item should be created");
    let second = library
        .create_content_item(NewContentItem::new(ContentKind::Video))
        .await
        .expect("second content item should be created");
    let sha256 = vec![12; 32];

    let recorded = library
        .record_canonical_asset(first.id, canonical_asset(first.id, sha256.clone()))
        .await
        .expect("canonical asset should be recorded");
    let replayed = library
        .record_canonical_asset(first.id, canonical_asset(first.id, sha256.clone()))
        .await
        .expect("same canonical asset should be idempotent");
    assert_eq!(replayed.id, recorded.id);
    assert_eq!(
        library
            .find_content_item(first.id)
            .await
            .expect("first content item should load")
            .expect("first content item should exist")
            .canonical_asset_id,
        Some(recorded.id)
    );

    let conflict = library
        .record_canonical_asset(second.id, canonical_asset(second.id, sha256))
        .await
        .expect_err("one canonical SHA-256 cannot belong to two content items");
    assert!(matches!(
        conflict,
        sooqa_persistence::LibraryRepositoryError::CanonicalAssetConflict {
            asset_id,
            content_item_id
        } if asset_id == recorded.id && content_item_id == first.id
    ));

    clean_up_content(&database, first.id).await;
    clean_up_content(&database, second.id).await;
}

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance"]
async fn duplicate_candidates_upsert_ordered_pairs_and_evidence() {
    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must point to the integration database");
    let database =
        Database::connect(&database_url, 10).await.expect("database should be reachable");
    database.migrate().await.expect("migrations should succeed");

    let library = database.library();
    let left = library
        .resolve_exact_duplicate(exact_duplicate_request(
            vec![31; 32],
            &format!("https://library.test/candidate-left/{}", Uuid::new_v4()),
        ))
        .await
        .expect("left content should be created");
    let right = library
        .resolve_exact_duplicate(exact_duplicate_request(
            vec![32; 32],
            &format!("https://library.test/candidate-right/{}", Uuid::new_v4()),
        ))
        .await
        .expect("right content should be created");

    let candidate = NewDuplicateCandidate::try_new(
        right.content_item.id,
        left.content_item.id,
        "frame_dhash_v1",
        9_250,
        json!({"duration_score": 0.98, "frame_distances": []}),
    )
    .expect("candidate should be valid");
    let recorded =
        library.upsert_duplicate_candidate(candidate).await.expect("candidate should be recorded");
    assert_eq!(recorded.score_basis_points, 9_250);
    assert!(recorded.left_content_item_id < recorded.right_content_item_id);
    assert_eq!(recorded.status.as_str(), "pending");

    let updated = library
        .upsert_duplicate_candidate(
            NewDuplicateCandidate::try_new(
                left.content_item.id,
                right.content_item.id,
                "frame_dhash_v1",
                8_750,
                json!({"duration_score": 0.9}),
            )
            .expect("updated candidate should be valid"),
        )
        .await
        .expect("candidate should update idempotently");
    assert_eq!(updated.id, recorded.id);
    assert_eq!(updated.score_basis_points, 8_750);

    let found = library
        .find_duplicate_candidate(left.content_item.id, right.content_item.id, "frame_dhash_v1")
        .await
        .expect("candidate lookup should succeed")
        .expect("candidate should be found");
    assert_eq!(found.id, recorded.id);
    assert_eq!(found.evidence_json, json!({"duration_score": 0.9}));

    clean_up_content(&database, left.content_item.id).await;
    clean_up_content(&database, right.content_item.id).await;
}
