use serde_json::json;
use sooqa_inbox::{IngestSubmission, IngestSubmissionInput, SubmittedVia};
use sooqa_library::{
    CaptionSyncCompletion, CaptionSyncState, MediaIngest, MediaKind, MediaMetadata,
    MediaSearchQuery, MediaSourceInput, MediaStatus, MediaUpdate, NewMedia, SourceKind,
    StorageUploadAttachment, StorageUploadReservation, StorageUploadReservationRequest,
    StorageUploadStore, VideoIdentityOutcome,
};
use sooqa_media::{SequenceAlignmentConfig, VideoSequenceFingerprint, VideoSequenceSample};
use sooqa_persistence::{Database, LibraryRepositoryError};
use uuid::Uuid;

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
            preview: None,
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

fn exact_ingest(kind: MediaKind, sha256: Vec<u8>, source: &str) -> MediaIngest {
    let mut ingest = ingest(sha256, source);
    ingest.media.kind = kind;
    ingest.metadata.kind = kind;
    ingest
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn media_aggregate_contains_source_and_tags_without_child_tables(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let resolution = database
        .library()
        .resolve_media(ingest(vec![7_u8; 32], "https://example.test/video"))
        .await
        .unwrap();
    database
        .library()
        .update_media(
            resolution.media.id,
            MediaUpdate { tags: Some(vec!["rust".to_owned()]), ..MediaUpdate::default() },
        )
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
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn caption_sync_is_generation_fenced_and_caption_metadata_is_bounded(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let media = database
        .library()
        .resolve_media(ingest(vec![71_u8; 32], "https://example.test/caption-sync"))
        .await
        .unwrap()
        .media;
    sqlx::query(
        "UPDATE media SET storage_state = 'ready', telegram_storage_chat_id = -100123, telegram_storage_message_id = 42, telegram_file_id = 'file', caption_sync_state = 'synced' WHERE id = $1",
    )
    .bind(media.id)
    .execute(database.pool())
    .await
    .unwrap();

    let updated = database
        .library()
        .update_media(
            media.id,
            MediaUpdate {
                description: Some(Some("first internal description".to_owned())),
                tags: Some(vec!["rust".to_owned(), "reviewed".to_owned()]),
                ..MediaUpdate::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.caption_sync_generation, 1);
    assert_eq!(updated.caption_sync_state, CaptionSyncState::Pending);
    let first_claim_token = Uuid::from_u128(101);
    let first_claim = database
        .library()
        .begin_caption_sync(media.id, 1, first_claim_token)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first_claim.metadata.description.as_deref(), Some("first internal description"));
    assert_eq!(first_claim.metadata.tags, vec!["rust".to_owned(), "reviewed".to_owned()]);

    let newer = database
        .library()
        .update_media(
            media.id,
            MediaUpdate {
                description: Some(Some("newer internal description".to_owned())),
                ..MediaUpdate::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(newer.caption_sync_generation, 2);
    assert_eq!(newer.caption_sync_state, CaptionSyncState::Pending);
    assert_eq!(
        database
            .library()
            .complete_caption_sync(media.id, 1, first_claim_token, true, false, None)
            .await
            .unwrap(),
        CaptionSyncCompletion::Stale
    );
    let fenced = database.library().find_media(media.id).await.unwrap().unwrap();
    assert_eq!(fenced.caption_sync_generation, 2);
    assert_eq!(fenced.caption_sync_state, CaptionSyncState::Pending);
    assert_eq!(fenced.description.as_deref(), Some("newer internal description"));

    let second_claim_token = Uuid::from_u128(102);
    let second_claim = database
        .library()
        .begin_caption_sync(media.id, 2, second_claim_token)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second_claim.metadata.description.as_deref(), Some("newer internal description"));
    sqlx::query(
        "UPDATE media SET caption_sync_state = 'pending', caption_sync_claim_token = NULL WHERE id = $1",
    )
    .bind(media.id)
    .execute(database.pool())
    .await
    .unwrap();
    let recovered_claim_token = Uuid::from_u128(103);
    database
        .library()
        .begin_caption_sync(media.id, 2, recovered_claim_token)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        database
            .library()
            .complete_caption_sync(media.id, 2, second_claim_token, true, false, None)
            .await
            .unwrap(),
        CaptionSyncCompletion::Stale
    );
    database
        .library()
        .complete_caption_sync(media.id, 2, recovered_claim_token, true, false, None)
        .await
        .unwrap();
    let synced = database.library().find_media(media.id).await.unwrap().unwrap();
    assert_eq!(synced.caption_sync_state, CaptionSyncState::Synced);
    assert_eq!(synced.caption_sync_error, None);
    let retried_synced = database.library().retry_caption_sync(media.id).await.unwrap();
    assert_eq!(retried_synced.caption_sync_generation, 2);
    assert_eq!(retried_synced.caption_sync_state, CaptionSyncState::Synced);
    let jobs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM queue.jobs WHERE kind = 'sync_storage_caption' AND payload->>'media_id' = $1",
    )
    .bind(media.id.to_string())
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(jobs, 3);
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn concurrent_same_sha_resolves_to_one_media_row(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
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
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn exact_sha_dedup_preserves_primary_source_metadata(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
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
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn exact_duplicate_unions_tags_and_replaces_explicit_description(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let repository = database.library();
    let mut first_ingest = ingest(vec![70_u8; 32], "https://example.test/metadata-first");
    first_ingest.media.description = Some("first internal note".to_owned());
    let first = repository.resolve_media(first_ingest).await.unwrap();

    let mut second_ingest = ingest(vec![70_u8; 32], "https://example.test/metadata-second");
    second_ingest.media.description = Some("replacement internal note".to_owned());
    second_ingest.tags = vec!["reaction".to_owned()];
    let second = repository.resolve_media(second_ingest).await.unwrap();
    assert!(!second.media_created);

    let (description, tags) = sqlx::query_as::<_, (Option<String>, Vec<String>)>(
        "SELECT description, tags FROM media WHERE id = $1",
    )
    .bind(first.media.id)
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(description.as_deref(), Some("replacement internal note"));
    assert_eq!(tags, ["rust", "reaction"]);
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn non_video_media_uses_exact_sha_without_fingerprint_data(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let repository = database.library();
    let mut media_ids = Vec::new();
    for (index, kind) in
        [MediaKind::Image, MediaKind::Animation, MediaKind::Audio].into_iter().enumerate()
    {
        let sha = vec![60_u8 + index as u8; 32];
        let first = repository
            .resolve_media(exact_ingest(
                kind,
                sha.clone(),
                &format!("https://example.test/{kind:?}"),
            ))
            .await
            .unwrap();
        let second = repository
            .resolve_media(exact_ingest(kind, sha, &format!("https://example.test/{kind:?}-again")))
            .await
            .unwrap();
        assert!(first.media_created);
        assert!(!second.media_created);
        assert_eq!(first.media.id, second.media.id);
        let fingerprint = sqlx::query_scalar::<_, Option<Vec<u8>>>(
            "SELECT fingerprint_data FROM media WHERE id = $1",
        )
        .bind(first.media.id)
        .fetch_one(database.pool())
        .await
        .unwrap();
        assert!(fingerprint.is_none());
        media_ids.push(first.media.id);
    }
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn video_fingerprint_shortlist_uses_tokens_state_and_version_bounds(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let repository = database.library();
    let incoming = repository
        .resolve_media(ingest(vec![21_u8; 32], "https://example.test/incoming"))
        .await
        .unwrap();
    let pending = repository
        .resolve_media(ingest(vec![22_u8; 32], "https://example.test/pending"))
        .await
        .unwrap();
    let ready = repository
        .resolve_media(ingest(vec![23_u8; 32], "https://example.test/ready"))
        .await
        .unwrap();
    let unknown = repository
        .resolve_media(ingest(vec![24_u8; 32], "https://example.test/unknown"))
        .await
        .unwrap();

    let fingerprint = test_sequence(0x1234_5678_9abc_def0);
    let tokens = fingerprint.search_tokens();
    repository.record_video_sequence_fingerprint(incoming.media.id, &fingerprint).await.unwrap();
    repository.record_video_sequence_fingerprint(pending.media.id, &fingerprint).await.unwrap();
    repository.record_video_sequence_fingerprint(ready.media.id, &fingerprint).await.unwrap();
    repository.record_video_sequence_fingerprint(unknown.media.id, &fingerprint).await.unwrap();
    sqlx::query(
        "UPDATE media SET storage_state = 'ready', telegram_storage_chat_id = -100123, telegram_storage_message_id = 501, telegram_file_id = 'ready-501' WHERE id = $1",
    )
    .bind(ready.media.id)
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE media SET storage_state = 'storage_unknown' WHERE id = $1")
        .bind(unknown.media.id)
        .execute(database.pool())
        .await
        .unwrap();

    let candidates = repository
        .list_video_fingerprint_candidates(incoming.media.id, "video_sequence_v1", &tokens)
        .await
        .unwrap();
    assert_eq!(candidates.len(), 2);
    assert_eq!(candidates[0].media_id, pending.media.id);
    assert_eq!(candidates[1].media_id, ready.media.id);
    assert!(candidates.iter().all(|candidate| {
        candidate.shared_token_count >= 8
            && candidate.overlap_bps >= 1_000
            && VideoSequenceFingerprint::decode(&candidate.fingerprint_data).is_ok()
    }));

    let wrong_version = repository
        .list_video_fingerprint_candidates(incoming.media.id, "other_v1", &tokens)
        .await
        .unwrap();
    assert!(wrong_version.is_empty());
    let unrelated = repository
        .list_video_fingerprint_candidates(incoming.media.id, "video_sequence_v1", &[1, 2, 3])
        .await
        .unwrap();
    assert!(unrelated.is_empty());
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn video_fingerprint_shortlist_is_capped_at_twenty(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let repository = database.library();
    let incoming = repository
        .resolve_media(ingest(vec![31_u8; 32], "https://example.test/cap-incoming"))
        .await
        .unwrap();
    let fingerprint = test_sequence(0xfedc_ba98_7654_3210);
    let tokens = fingerprint.search_tokens();
    let mut media_ids = vec![incoming.media.id];

    for index in 0..21_u8 {
        let candidate = repository
            .resolve_media(ingest(
                vec![100_u8 + index; 32],
                &format!("https://example.test/cap-{index}"),
            ))
            .await
            .unwrap();
        repository
            .record_video_sequence_fingerprint(candidate.media.id, &fingerprint)
            .await
            .unwrap();
        media_ids.push(candidate.media.id);
    }

    let candidates = repository
        .list_video_fingerprint_candidates(incoming.media.id, "video_sequence_v1", &tokens)
        .await
        .unwrap();
    assert_eq!(candidates.len(), 20);
    assert!(candidates.iter().all(|candidate| candidate.shared_token_count >= 8));
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn video_identity_reuses_exact_sha_and_stores_fingerprint_before_storage(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let repository = database.library();
    let fingerprint = test_sequence(0x1111_2222_3333_4444);
    let first = repository
        .resolve_video_identity(
            ingest(vec![41_u8; 32], "https://example.test/exact-first"),
            &fingerprint,
            SequenceAlignmentConfig::default(),
            false,
        )
        .await
        .unwrap();
    let media_id = match first {
        VideoIdentityOutcome::NewMedia { media_id } => media_id,
        other => panic!("expected a new media reservation, got {other:?}"),
    };
    let second = repository
        .resolve_video_identity(
            ingest(vec![41_u8; 32], "https://example.test/exact-second"),
            &fingerprint,
            SequenceAlignmentConfig::default(),
            false,
        )
        .await
        .unwrap();
    assert_eq!(second, VideoIdentityOutcome::ExactDuplicate { media_id });
    let (storage_state, fingerprint_version, fingerprint_data, tokens) = sqlx::query_as::<
        _,
        (String, Option<String>, Option<Vec<u8>>, Option<Vec<i64>>),
    >(
        "SELECT storage_state, fingerprint_version, fingerprint_data, fingerprint_search_tokens FROM media WHERE id = $1",
    )
    .bind(media_id)
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(storage_state, "pending_storage");
    assert_eq!(fingerprint_version, Some("video_sequence_v1".to_owned()));
    assert_eq!(VideoSequenceFingerprint::decode(&fingerprint_data.unwrap()).unwrap(), fingerprint);
    assert_eq!(tokens.unwrap(), fingerprint.search_tokens());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM media WHERE canonical_sha256 = $1")
            .bind(vec![41_u8; 32])
            .fetch_one(database.pool())
            .await
            .unwrap(),
        1
    );
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn strong_video_match_stops_before_media_insertion_and_force_save_bypasses_it(
    pool: sqlx::PgPool,
) {
    let database = Database::from_pool(pool);
    let repository = database.library();
    let fingerprint = test_sequence(0x5555_6666_7777_8888);
    let first = repository
        .resolve_video_identity(
            ingest(vec![42_u8; 32], "https://example.test/perceptual-first"),
            &fingerprint,
            SequenceAlignmentConfig::default(),
            false,
        )
        .await
        .unwrap();
    let first_id = match first {
        VideoIdentityOutcome::NewMedia { media_id } => media_id,
        other => panic!("expected a new media reservation, got {other:?}"),
    };
    let pending = repository
        .resolve_video_identity(
            ingest(vec![43_u8; 32], "https://example.test/perceptual-second"),
            &fingerprint,
            SequenceAlignmentConfig::default(),
            false,
        )
        .await
        .unwrap();
    let pending_id = match pending {
        VideoIdentityOutcome::DuplicatePending { evidence } => {
            assert!(!evidence.matches.is_empty());
            assert!(evidence.matches.len() <= 3);
            assert!(serde_json::to_vec(&evidence).unwrap().len() <= 16 * 1024);
            evidence.matches[0].media_id
        }
        other => panic!("expected duplicate_pending, got {other:?}"),
    };
    assert_eq!(pending_id, first_id);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM media WHERE canonical_sha256 = $1")
            .bind(vec![43_u8; 32])
            .fetch_one(database.pool())
            .await
            .unwrap(),
        0
    );

    let forced = repository
        .resolve_video_identity(
            ingest(vec![43_u8; 32], "https://example.test/perceptual-second"),
            &fingerprint,
            SequenceAlignmentConfig::default(),
            true,
        )
        .await
        .unwrap();
    let forced_id = match forced {
        VideoIdentityOutcome::NewMedia { media_id } => media_id,
        other => panic!("expected force-save to create a new reservation, got {other:?}"),
    };
    assert_ne!(forced_id, first_id);
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn concurrent_equivalent_videos_share_the_identity_barrier(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let repository = database.library();
    let fingerprint = test_sequence(0x9999_aaaa_bbbb_cccc);
    let left = ingest(vec![51_u8; 32], "https://example.test/concurrent-left");
    let right = ingest(vec![52_u8; 32], "https://example.test/concurrent-right");
    let (left, right) = tokio::join!(
        repository.resolve_video_identity(
            left,
            &fingerprint,
            SequenceAlignmentConfig::default(),
            false,
        ),
        repository.resolve_video_identity(
            right,
            &fingerprint,
            SequenceAlignmentConfig::default(),
            false,
        )
    );
    let left = left.unwrap();
    let right = right.unwrap();
    let outcomes = [left, right];
    let new_ids = outcomes
        .iter()
        .filter_map(|outcome| match outcome {
            VideoIdentityOutcome::NewMedia { media_id } => Some(*media_id),
            VideoIdentityOutcome::ExactDuplicate { .. }
            | VideoIdentityOutcome::DuplicatePending { .. } => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(new_ids.len(), 1, "equivalent videos must reserve at most one media row");
    assert!(
        outcomes
            .iter()
            .any(|outcome| { matches!(outcome, VideoIdentityOutcome::DuplicatePending { .. }) })
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM media WHERE canonical_sha256 IN ($1, $2)",
        )
        .bind(vec![51_u8; 32])
        .bind(vec![52_u8; 32])
        .fetch_one(database.pool())
        .await
        .unwrap(),
        1
    );
}

fn test_sequence(seed: u64) -> VideoSequenceFingerprint {
    VideoSequenceFingerprint::new(
        5_000,
        500,
        (0..10)
            .map(|index| VideoSequenceSample {
                phash: seed.wrapping_add(index),
                dhash: seed.rotate_left(index as u32),
                mean_luma: 100,
                mean_chroma_u: 3,
                mean_chroma_v: -2,
                information_bps: 8_000,
                transition_bps: if index == 0 { 0 } else { 500 },
            })
            .collect(),
    )
    .unwrap()
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn storage_completion_requeues_caption_when_metadata_changes_during_upload(
    pool: sqlx::PgPool,
) {
    let database = Database::from_pool(pool);
    let mut incoming = ingest(vec![53_u8; 32], "https://example.test/upload-race");
    incoming.media.description = Some("before upload".to_owned());
    let media = database.library().resolve_media(incoming).await.unwrap().media;
    let reservation = database
        .library()
        .reserve_storage_upload(StorageUploadReservationRequest {
            media_id: media.id,
            generation: 0,
        })
        .await
        .unwrap();
    let (owner_token, caption_metadata) = match reservation {
        StorageUploadReservation::Reserved { owner_token, caption_metadata, .. } => {
            (owner_token, caption_metadata)
        }
        other => panic!("expected a fresh storage reservation, got {other:?}"),
    };

    database
        .library()
        .update_media(
            media.id,
            MediaUpdate {
                description: Some(Some("after upload started".to_owned())),
                ..MediaUpdate::default()
            },
        )
        .await
        .unwrap();
    database
        .library()
        .complete_storage_upload(
            media.id,
            owner_token,
            StorageUploadAttachment {
                storage_chat_id: -100123,
                storage_message_id: 43,
                telegram_file_id: Some("file-43".to_owned()),
                telegram_file_unique_id: Some("unique-43".to_owned()),
                caption_metadata: Some(caption_metadata),
            },
        )
        .await
        .unwrap();

    let completed = database.library().find_media(media.id).await.unwrap().unwrap();
    assert_eq!(completed.caption_sync_generation, 1);
    assert_eq!(completed.caption_sync_state, CaptionSyncState::Pending);
    assert_eq!(completed.description.as_deref(), Some("after upload started"));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM queue.jobs WHERE kind = 'sync_storage_caption' AND payload->>'media_id' = $1 AND payload->>'generation' = '1'",
        )
        .bind(media.id.to_string())
        .fetch_one(database.pool())
        .await
        .unwrap(),
        1
    );
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn storage_reconciliation_reopens_and_completes_linked_ingest(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
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
                caption_metadata: None,
            },
        )
        .await
        .unwrap();
    // Storage completion now completes linked ingests and enqueues cleanup in
    // the same transaction as the durable ready transition. The compatibility
    // helper is intentionally idempotent when called by the worker afterward.
    assert_eq!(database.inbox().complete_storage_for_media(media.media.id).await.unwrap(), 0);
    let media_details =
        database.library().find_media_details(media.media.id).await.unwrap().unwrap();
    assert!(media_details.media.local_work_path.is_none());
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
                caption_metadata: None,
            },
        )
        .await
        .unwrap();
    let attached = database.inbox().find(ingest.ingest.id).await.unwrap().unwrap();
    assert_eq!(attached.status.as_str(), "completed");

    assert!(matches!(
        database.library().reset_storage_upload(media.media.id).await,
        Err(LibraryRepositoryError::WorkspaceReclaimed(id)) if id == media.media.id
    ));
    let still_ready = database.inbox().find(ingest.ingest.id).await.unwrap().unwrap();
    assert_eq!(still_ready.status.as_str(), "completed");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM queue.jobs WHERE kind = 'upload_storage_asset' AND payload->>'media_id' = $1",
        )
        .bind(media.media.id.to_string())
        .fetch_one(database.pool())
        .await
        .unwrap(),
        0
    );
}
