use std::env;

use serde_json::json;
use sooqa_inbox::{IngestSubmission, IngestSubmissionInput, SubmittedVia};
use sooqa_jobs::NewJob;
use sooqa_library::{
    MediaIngest, MediaKind, MediaMetadata, MediaSourceInput, NewMedia, SourceKind,
};
use sooqa_persistence::{Database, WorkspaceCleanupStart};
use uuid::Uuid;

async fn database() -> Database {
    let url = env::var("DATABASE_URL").expect("DATABASE_URL must point to PostgreSQL");
    let database = Database::connect(&url, 10).await.expect("database should connect");
    database.migrate().await.expect("migration should apply");
    database
}

fn media_ingest(source: &str) -> MediaIngest {
    MediaIngest {
        media: NewMedia {
            kind: MediaKind::Video,
            title: Some("workspace-test".to_owned()),
            description: None,
            notes: None,
        },
        metadata: MediaMetadata {
            kind: MediaKind::Video,
            mime_type: Some("video/mp4".to_owned()),
            container: Some("mp4".to_owned()),
            video_codec: Some("h264".to_owned()),
            audio_codec: None,
            width: Some(320),
            height: Some(240),
            duration_ms: Some(1_000),
            bit_rate: Some(100_000),
            file_size_bytes: Some(1_024),
            sha256: Some(vec![17_u8; 32]),
            local_work_path: Some("/tmp/workspace-test.mp4".to_owned()),
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
            metadata: json!({}),
        },
        tags: Vec::new(),
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn ready_storage_clears_local_bytes_and_coalesces_cleanup() {
    let database = database().await;
    let source = format!("https://example.test/workspace-{}", Uuid::new_v4());
    let media = database.library().resolve_media(media_ingest(&source)).await.unwrap();
    let ingest = database
        .inbox()
        .create_ingest(
            IngestSubmission::try_new(IngestSubmissionInput::new(&source, SubmittedVia::Api))
                .unwrap(),
        )
        .await
        .unwrap();
    let ingest_id = ingest.ingest.id;
    let workspace_id = ingest.ingest.workspace_id;

    sqlx::query(
        "UPDATE ingests SET media_id = $2, state = 'storing', completed_at = NULL WHERE id = $1",
    )
    .bind(ingest_id)
    .bind(media.media.id)
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE queue.jobs SET state = 'succeeded', completed_at = now() WHERE payload->>'ingest_id' = $1",
    )
    .bind(ingest_id.to_string())
    .execute(database.pool())
    .await
    .unwrap();

    assert_eq!(
        database
            .inbox()
            .begin_workspace_cleanup(Uuid::new_v4(), ingest_id, workspace_id)
            .await
            .unwrap(),
        WorkspaceCleanupStart::Deferred
    );
    assert!(database.jobs().protected_workspace_ids().await.unwrap().contains(&workspace_id));

    sqlx::query(
        "UPDATE media SET storage_state = 'ready', telegram_storage_chat_id = -100123, telegram_storage_message_id = 701, telegram_file_id = 'workspace-ready' WHERE id = $1",
    )
    .bind(media.media.id)
    .execute(database.pool())
    .await
    .unwrap();

    assert_eq!(database.inbox().complete_storage_for_media(media.media.id).await.unwrap(), 1);
    assert_eq!(database.inbox().complete_storage_for_media(media.media.id).await.unwrap(), 0);
    let completed = database.inbox().find(ingest_id).await.unwrap().unwrap();
    assert_eq!(completed.status.as_str(), "completed");
    assert_eq!(completed.workspace_id, workspace_id);
    assert!(
        database
            .library()
            .find_media_details(media.media.id)
            .await
            .unwrap()
            .unwrap()
            .media
            .local_work_path
            .is_none()
    );

    let cleanup_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM queue.jobs WHERE kind = 'cleanup_workspace' AND payload->>'ingest_id' = $1",
    )
    .bind(ingest_id.to_string())
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM queue.jobs WHERE kind = 'cleanup_workspace' AND payload->>'ingest_id' = $1 AND payload->>'workspace_id' = $2",
        )
        .bind(ingest_id.to_string())
        .bind(workspace_id.to_string())
        .fetch_one(database.pool())
        .await
        .unwrap(),
        1
    );
    assert!(database.jobs().protected_workspace_ids().await.unwrap().contains(&workspace_id));

    sqlx::query("UPDATE queue.jobs SET state = 'succeeded', completed_at = now() WHERE id = $1")
        .bind(cleanup_id)
        .execute(database.pool())
        .await
        .unwrap();
    assert!(!database.jobs().protected_workspace_ids().await.unwrap().contains(&workspace_id));

    sqlx::query(
        "DELETE FROM queue.jobs WHERE payload->>'ingest_id' = $1 OR payload->>'media_id' = $2",
    )
    .bind(ingest_id.to_string())
    .bind(media.media.id.to_string())
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query("DELETE FROM ingests WHERE id = $1")
        .bind(ingest_id)
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM media WHERE id = $1")
        .bind(media.media.id)
        .execute(database.pool())
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn force_save_changes_workspace_generation_before_old_cleanup_runs() {
    let database = database().await;
    let source = format!("https://example.test/force-save-workspace-{}", Uuid::new_v4());
    let ingest = database
        .inbox()
        .create_ingest(
            IngestSubmission::try_new(IngestSubmissionInput::new(&source, SubmittedVia::Api))
                .unwrap(),
        )
        .await
        .unwrap();
    let ingest_id = ingest.ingest.id;
    let old_workspace_id = ingest.ingest.workspace_id;
    sqlx::query(
        "UPDATE ingests SET state = 'duplicate_pending', duplicate_evidence = $2 WHERE id = $1",
    )
    .bind(ingest_id)
    .bind(json!({"matches": []}))
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE queue.jobs SET state = 'succeeded', completed_at = now() WHERE kind = 'inspect_source' AND payload->>'ingest_id' = $1",
    )
    .bind(ingest_id.to_string())
    .execute(database.pool())
    .await
    .unwrap();

    let old_cleanup = database
        .jobs()
        .enqueue(
            NewJob::cleanup_workspace(ingest_id, old_workspace_id)
                .dedupe_key(format!("test:cleanup:{ingest_id}:{old_workspace_id}")),
        )
        .await
        .unwrap();
    let resumed = database.inbox().force_save(ingest_id).await.unwrap();
    assert!(resumed.resumed);
    assert_ne!(resumed.ingest.workspace_id, old_workspace_id);
    assert_eq!(
        database
            .inbox()
            .begin_workspace_cleanup(old_cleanup.id, ingest_id, old_workspace_id)
            .await
            .unwrap(),
        WorkspaceCleanupStart::Ready
    );
    assert!(
        database
            .jobs()
            .protected_workspace_ids()
            .await
            .unwrap()
            .contains(&resumed.ingest.workspace_id)
    );

    sqlx::query("DELETE FROM queue.jobs WHERE payload->>'ingest_id' = $1")
        .bind(ingest_id.to_string())
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM ingests WHERE id = $1")
        .bind(ingest_id)
        .execute(database.pool())
        .await
        .unwrap();
}
