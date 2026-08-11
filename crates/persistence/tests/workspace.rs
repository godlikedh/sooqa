use std::env;

use serde_json::json;
use sooqa_inbox::{IngestSubmission, IngestSubmissionInput, SubmittedVia};
use sooqa_jobs::{JobAttempt, NewJob};
use sooqa_library::{
    MediaIngest, MediaKind, MediaMetadata, MediaSourceInput, NewMedia, SourceKind,
};
use sooqa_media::{MediaWorkspace, WorkspaceArea};
use sooqa_persistence::{Database, LibraryRepositoryError, WorkspaceCleanupStart};
use tokio::fs;
use uuid::Uuid;

async fn database() -> Database {
    let url = env::var("DATABASE_URL").expect("DATABASE_URL must point to PostgreSQL");
    let database = Database::connect(&url, 10).await.expect("database should connect");
    database.migrate().await.expect("migration should apply");
    database
}

fn media_ingest(source: &str) -> MediaIngest {
    media_ingest_with_sha(source, vec![17_u8; 32])
}

fn media_ingest_with_sha(source: &str, sha256: Vec<u8>) -> MediaIngest {
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
            sha256: Some(sha256),
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

async fn prepare_completed_storage(
    database: &Database,
    source: &str,
    sha_seed: u8,
) -> (Uuid, Uuid, Uuid) {
    let media = database
        .library()
        .resolve_media(media_ingest_with_sha(source, vec![sha_seed; 32]))
        .await
        .unwrap();
    let ingest = database
        .inbox()
        .create_ingest(
            IngestSubmission::try_new(IngestSubmissionInput::new(source, SubmittedVia::Api))
                .unwrap(),
        )
        .await
        .unwrap();
    let workspace_id = ingest.ingest.workspace_id;
    sqlx::query(
        "UPDATE ingests SET media_id = $2, state = 'completed', completed_at = now() WHERE id = $1",
    )
    .bind(ingest.ingest.id)
    .bind(media.media.id)
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE media SET storage_state = 'ready', telegram_storage_chat_id = -100123, telegram_storage_message_id = $2, telegram_file_id = $3, local_work_path = '/tmp/workspace-test.mp4' WHERE id = $1",
    )
    .bind(media.media.id)
    .bind(700_i64 + i64::from(sha_seed))
    .bind(format!("workspace-ready-{sha_seed}"))
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE queue.jobs SET state = 'succeeded', completed_at = now() WHERE payload->>'ingest_id' = $1",
    )
    .bind(ingest.ingest.id.to_string())
    .execute(database.pool())
    .await
    .unwrap();
    (ingest.ingest.id, media.media.id, workspace_id)
}

async fn remove_storage_fixture(database: &Database, ingest_id: Uuid, media_id: Uuid) {
    sqlx::query(
        "DELETE FROM queue.jobs WHERE payload->>'ingest_id' = $1 OR payload->>'media_id' = $2",
    )
    .bind(ingest_id.to_string())
    .bind(media_id.to_string())
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query("DELETE FROM ingests WHERE id = $1")
        .bind(ingest_id)
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM media WHERE id = $1")
        .bind(media_id)
        .execute(database.pool())
        .await
        .unwrap();
}

async fn mark_cleanup_running(database: &Database, job_id: Uuid, worker_id: &str) -> JobAttempt {
    let lease_token = Uuid::new_v4();
    sqlx::query(
        "UPDATE queue.jobs SET state = 'running', lease_token = $2, lease_owner = $3, lease_expires_at = now() + interval '30 seconds', last_heartbeat_at = now(), attempt_count = 1 WHERE id = $1",
    )
    .bind(job_id)
    .bind(lease_token)
    .bind(worker_id)
    .execute(database.pool())
    .await
    .unwrap();
    JobAttempt {
        job_id,
        attempt_number: 1,
        worker_id: worker_id.to_owned(),
        lease_owner: worker_id.to_owned(),
        lease_token,
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
    // Reconciliation protects current workspace IDs even after the explicit
    // cleanup job succeeds. This closes the snapshot-to-delete race with a
    // later storage reset; only old generations become scavenger orphans.
    assert!(database.jobs().protected_workspace_ids().await.unwrap().contains(&workspace_id));
    assert!(matches!(
        database.library().reset_storage_upload(media.media.id).await,
        Err(LibraryRepositoryError::WorkspaceReclaimed(id)) if id == media.media.id
    ));

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
async fn storage_reset_wins_and_cleanup_defers_on_the_durable_fence() {
    let database = database().await;
    let source = format!("https://example.test/workspace-reset-wins-{}", Uuid::new_v4());
    let (ingest_id, media_id, workspace_id) =
        prepare_completed_storage(&database, &source, 31).await;
    let cleanup = database
        .jobs()
        .enqueue(
            NewJob::cleanup_workspace(ingest_id, workspace_id)
                .dedupe_key(format!("test:cleanup-reset-wins:{ingest_id}")),
        )
        .await
        .unwrap();

    database.library().reset_storage_upload(media_id).await.unwrap();
    let attempt = mark_cleanup_running(&database, cleanup.id, "workspace-reset-wins").await;
    assert_eq!(
        database.inbox().begin_workspace_cleanup(&attempt, ingest_id, workspace_id).await.unwrap(),
        WorkspaceCleanupStart::Deferred
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT storage_state FROM media WHERE id = $1")
            .bind(media_id)
            .fetch_one(database.pool())
            .await
            .unwrap(),
        "pending_storage"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM queue.jobs WHERE kind = 'upload_storage_asset' AND payload->>'media_id' = $1",
        )
        .bind(media_id.to_string())
        .fetch_one(database.pool())
        .await
        .unwrap(),
        1
    );
    remove_storage_fixture(&database, ingest_id, media_id).await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn cleanup_claim_wins_and_storage_reset_requires_reconstruction() {
    let database = database().await;
    let source = format!("https://example.test/workspace-cleanup-wins-{}", Uuid::new_v4());
    let (ingest_id, media_id, workspace_id) =
        prepare_completed_storage(&database, &source, 32).await;
    let cleanup = database
        .jobs()
        .enqueue(
            NewJob::cleanup_workspace(ingest_id, workspace_id)
                .dedupe_key(format!("test:cleanup-cleanup-wins:{ingest_id}")),
        )
        .await
        .unwrap();
    let work_root =
        std::env::temp_dir().join(format!("sooqa-persistence-cleanup-{}", Uuid::new_v4()));
    let workspace = MediaWorkspace::create(&work_root, workspace_id).await.unwrap();
    let source_path = workspace.path(WorkspaceArea::Source, "source.bin").unwrap();
    fs::write(&source_path, b"cleanup-me").await.unwrap();
    sqlx::query("UPDATE media SET local_work_path = $2 WHERE id = $1")
        .bind(media_id)
        .bind(source_path.to_string_lossy().as_ref())
        .execute(database.pool())
        .await
        .unwrap();
    let attempt = mark_cleanup_running(&database, cleanup.id, "workspace-cleanup-race").await;
    assert_eq!(
        database.inbox().begin_workspace_cleanup(&attempt, ingest_id, workspace_id).await.unwrap(),
        WorkspaceCleanupStart::Ready
    );
    assert!(
        database
            .library()
            .find_media_details(media_id)
            .await
            .unwrap()
            .unwrap()
            .media
            .local_work_path
            .is_none()
    );
    MediaWorkspace::cleanup_existing(&work_root, workspace_id).await.unwrap();
    assert!(!workspace.root().exists());
    sqlx::query(
        "UPDATE queue.jobs SET state = 'succeeded', lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL, last_heartbeat_at = NULL, completed_at = now() WHERE id = $1",
    )
    .bind(cleanup.id)
    .execute(database.pool())
    .await
        .unwrap();
    let _ = fs::remove_dir_all(&work_root).await;
    assert!(matches!(
        database.library().reset_storage_upload(media_id).await,
        Err(LibraryRepositoryError::WorkspaceReclaimed(id)) if id == media_id
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM queue.jobs WHERE kind = 'upload_storage_asset' AND payload->>'media_id' = $1",
        )
        .bind(media_id.to_string())
        .fetch_one(database.pool())
        .await
        .unwrap(),
        0
    );
    remove_storage_fixture(&database, ingest_id, media_id).await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn cleanup_reclaim_marker_survives_lease_recovery() {
    let database = database().await;
    let source = format!("https://example.test/workspace-cleanup-recovery-{}", Uuid::new_v4());
    let (ingest_id, media_id, workspace_id) =
        prepare_completed_storage(&database, &source, 33).await;
    let work_root =
        std::env::temp_dir().join(format!("sooqa-persistence-cleanup-recovery-{}", Uuid::new_v4()));
    let workspace = MediaWorkspace::create(&work_root, workspace_id).await.unwrap();
    let source_path = workspace.path(WorkspaceArea::Source, "source.bin").unwrap();
    fs::write(&source_path, b"cleanup-recovery").await.unwrap();
    sqlx::query("UPDATE media SET local_work_path = $2 WHERE id = $1")
        .bind(media_id)
        .bind(source_path.to_string_lossy().as_ref())
        .execute(database.pool())
        .await
        .unwrap();

    let cleanup = database
        .jobs()
        .enqueue(
            NewJob::cleanup_workspace(ingest_id, workspace_id)
                .dedupe_key(format!("test:cleanup-recovery:{ingest_id}")),
        )
        .await
        .unwrap();
    let attempt = mark_cleanup_running(&database, cleanup.id, "workspace-cleanup-recovery").await;
    assert_eq!(
        database.inbox().begin_workspace_cleanup(&attempt, ingest_id, workspace_id).await.unwrap(),
        WorkspaceCleanupStart::Ready
    );
    MediaWorkspace::cleanup_existing(&work_root, workspace_id).await.unwrap();
    assert!(!workspace.root().exists());

    sqlx::query(
        "UPDATE queue.jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1",
    )
    .bind(cleanup.id)
    .execute(database.pool())
    .await
    .unwrap();
    database.jobs().recover_stale_leases().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM queue.jobs WHERE id = $1")
            .bind(cleanup.id)
            .fetch_one(database.pool())
            .await
            .unwrap(),
        "queued"
    );
    assert!(matches!(
        database.library().reset_storage_upload(media_id).await,
        Err(LibraryRepositoryError::WorkspaceReclaimed(id)) if id == media_id
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM queue.jobs WHERE kind = 'upload_storage_asset' AND payload->>'media_id' = $1",
        )
        .bind(media_id.to_string())
        .fetch_one(database.pool())
        .await
        .unwrap(),
        0
    );
    let _ = fs::remove_dir_all(&work_root).await;
    remove_storage_fixture(&database, ingest_id, media_id).await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn stale_cleanup_attempt_cannot_delete_after_storage_reset_wins() {
    let database = database().await;
    let source = format!("https://example.test/workspace-stale-cleanup-{}", Uuid::new_v4());
    let (ingest_id, media_id, workspace_id) =
        prepare_completed_storage(&database, &source, 34).await;
    let work_root =
        std::env::temp_dir().join(format!("sooqa-persistence-stale-cleanup-{}", Uuid::new_v4()));
    let workspace = MediaWorkspace::create(&work_root, workspace_id).await.unwrap();
    let source_path = workspace.path(WorkspaceArea::Source, "source.bin").unwrap();
    fs::write(&source_path, b"stale-cleanup").await.unwrap();
    sqlx::query("UPDATE media SET local_work_path = $2 WHERE id = $1")
        .bind(media_id)
        .bind(source_path.to_string_lossy().as_ref())
        .execute(database.pool())
        .await
        .unwrap();

    let cleanup = database
        .jobs()
        .enqueue(
            NewJob::cleanup_workspace(ingest_id, workspace_id)
                .dedupe_key(format!("test:stale-cleanup:{ingest_id}")),
        )
        .await
        .unwrap();
    let stale_attempt =
        mark_cleanup_running(&database, cleanup.id, "stale-workspace-cleanup").await;
    sqlx::query(
        "UPDATE queue.jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1",
    )
    .bind(cleanup.id)
    .execute(database.pool())
    .await
    .unwrap();
    database.jobs().recover_stale_leases().await.unwrap();

    database.library().reset_storage_upload(media_id).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT storage_state FROM media WHERE id = $1")
            .bind(media_id)
            .fetch_one(database.pool())
            .await
            .unwrap(),
        "pending_storage"
    );
    assert_eq!(
        database
            .inbox()
            .begin_workspace_cleanup(&stale_attempt, ingest_id, workspace_id)
            .await
            .unwrap(),
        WorkspaceCleanupStart::AlreadyAdvanced
    );
    assert!(workspace.root().exists());
    assert!(
        database
            .library()
            .find_media_details(media_id)
            .await
            .unwrap()
            .unwrap()
            .media
            .local_work_path
            .is_some()
    );
    MediaWorkspace::cleanup_existing(&work_root, workspace_id).await.unwrap();
    let _ = fs::remove_dir_all(&work_root).await;
    remove_storage_fixture(&database, ingest_id, media_id).await;
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
    let attempt = mark_cleanup_running(&database, old_cleanup.id, "old-workspace-cleanup").await;
    assert_eq!(
        database
            .inbox()
            .begin_workspace_cleanup(&attempt, ingest_id, old_workspace_id)
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
