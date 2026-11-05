use std::{
    sync::{Arc, Mutex},
    time::Duration as StdDuration,
};

use async_trait::async_trait;
use serde_json::json;
use sooqa_jobs::{Job, JobStatus, JobType};
use sooqa_library::{
    CaptionSyncState, Media, MediaIngest, MediaKind, MediaMetadata, MediaSourceInput, MediaUpdate,
    NewMedia, SourceKind,
};
use sooqa_persistence::{Database, JobSettlement};
use sooqa_telegram::{StorageCaptionEditRequest, TelegramStorageCaptionApi};
use sooqa_worker::sync_storage_caption_handler;
use thiserror::Error;
use time::OffsetDateTime;
use tokio::sync::Notify;
use uuid::Uuid;

#[derive(Debug, Error, Clone, Copy, Eq, PartialEq)]
enum FakeCaptionApiError {
    #[error("temporary Telegram caption failure")]
    Retryable,
    #[error("permanent Telegram caption failure")]
    Permanent,
}

#[derive(Clone)]
enum FakeCaptionApiMode {
    Success,
    RetryableFailure,
    PermanentFailure,
    WaitForRelease { started: Arc<Notify>, release: Arc<Notify> },
}

#[derive(Clone)]
struct FakeCaptionApi {
    mode: FakeCaptionApiMode,
    calls: Arc<Mutex<Vec<StorageCaptionEditRequest>>>,
}

impl FakeCaptionApi {
    fn new(mode: FakeCaptionApiMode) -> Self {
        Self { mode, calls: Arc::new(Mutex::new(Vec::new())) }
    }

    fn calls(&self) -> Vec<StorageCaptionEditRequest> {
        self.calls.lock().expect("caption call lock should not be poisoned").clone()
    }
}

#[async_trait]
impl TelegramStorageCaptionApi for FakeCaptionApi {
    type Error = FakeCaptionApiError;

    async fn edit_storage_caption(
        &self,
        request: StorageCaptionEditRequest,
    ) -> Result<(), Self::Error> {
        {
            self.calls.lock().expect("caption call lock should not be poisoned").push(request);
        }
        match &self.mode {
            FakeCaptionApiMode::Success => Ok(()),
            FakeCaptionApiMode::RetryableFailure => Err(FakeCaptionApiError::Retryable),
            FakeCaptionApiMode::PermanentFailure => Err(FakeCaptionApiError::Permanent),
            FakeCaptionApiMode::WaitForRelease { started, release } => {
                started.notify_one();
                release.notified().await;
                Ok(())
            }
        }
    }

    fn is_retryable_error(error: &Self::Error) -> bool {
        matches!(error, FakeCaptionApiError::Retryable)
    }
}

fn media_ingest(hash_byte: u8, source_url: &str) -> MediaIngest {
    MediaIngest {
        media: NewMedia {
            kind: MediaKind::Video,
            title: Some("caption sync test".to_owned()),
            description: None,
        },
        metadata: MediaMetadata {
            kind: MediaKind::Video,
            mime_type: Some("video/mp4".to_owned()),
            container: Some("mp4".to_owned()),
            video_codec: Some("h264".to_owned()),
            audio_codec: None,
            width: Some(640),
            height: Some(360),
            duration_ms: Some(1_000),
            bit_rate: None,
            file_size_bytes: Some(100),
            sha256: Some(vec![hash_byte; 32]),
            local_work_path: None,
            preview: None,
        },
        source: MediaSourceInput {
            ingest_id: None,
            kind: SourceKind::DirectUrl,
            original_url: Some(source_url.to_owned()),
            normalized_url: Some(source_url.to_owned()),
            platform: None,
            platform_content_id: None,
            author_name: None,
            title: None,
            description: None,
            published_at: None,
            metadata: json!({}),
        },
        tags: vec!["seed".to_owned()],
    }
}

async fn ready_media(database: &Database, suffix: u8) -> (Media, String, i64) {
    let source_url = format!("https://example.test/caption-sync/{suffix}");
    let media = database
        .library()
        .resolve_media(media_ingest(suffix, &source_url))
        .await
        .expect("media should be inserted")
        .media;
    let storage_message_id = i64::from(suffix) + 40;
    sqlx::query(
        "UPDATE media SET storage_state = 'ready', telegram_storage_chat_id = -100123, telegram_storage_message_id = $2, telegram_file_id = 'caption-file', caption_sync_state = 'synced' WHERE id = $1",
    )
    .bind(media.id)
    .bind(storage_message_id)
    .execute(database.pool())
    .await
    .expect("media should have a ready storage receipt");
    let media = database
        .library()
        .find_media(media.id)
        .await
        .expect("media should be readable")
        .expect("media should exist");
    (media, source_url, storage_message_id)
}

async fn change_caption_metadata(
    database: &Database,
    current: &Media,
    description: &str,
    tags: &[&str],
) -> Media {
    database
        .library()
        .update_media(
            current.id,
            MediaUpdate {
                description: Some(description.to_owned()),
                tags: tags.iter().map(|tag| (*tag).to_owned()).collect(),
                expected_updated_at: current.updated_at,
            },
        )
        .await
        .expect("caption metadata should update")
}

fn expected_api_request(
    media: &Media,
    source_url: &str,
    storage_message_id: i64,
) -> StorageCaptionEditRequest {
    StorageCaptionEditRequest {
        storage_chat_id: -100123,
        storage_message_id,
        caption: format!(
            "description: {}\ntags: {}\nsource: {source_url}",
            media.description.as_deref().expect("description should be set"),
            media.tags.join(", ")
        ),
    }
}

async fn claim_caption_job(database: &Database, worker_id: &str) -> Job {
    database
        .jobs()
        .claim_next(worker_id, StdDuration::from_secs(30), &[JobType::SyncStorageCaption])
        .await
        .expect("caption job should claim")
        .expect("caption job should be queued")
}

async fn caption_jobs(
    database: &Database,
    media_id: Uuid,
) -> Vec<(Uuid, String, i32, Option<String>)> {
    sqlx::query_as(
        "SELECT id, state, (payload->>'generation')::integer, dedupe_key FROM queue.jobs WHERE kind = 'sync_storage_caption' AND payload->>'media_id' = $1 ORDER BY created_at, id",
    )
    .bind(media_id.to_string())
    .fetch_all(database.pool())
    .await
    .expect("caption jobs should be readable")
}

fn lease(job: &Job) -> sooqa_jobs::JobLease {
    job.lease().expect("claimed caption job should carry a lease")
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn caption_sync_success_edits_storage_caption_and_completes_durable_state(
    pool: sqlx::PgPool,
) {
    let database = Database::from_pool(pool);
    let (initial, source_url, storage_message_id) = ready_media(&database, 1).await;
    let media =
        change_caption_metadata(&database, &initial, "internal description", &["cats", "reviewed"])
            .await;
    assert_eq!(media.caption_sync_generation, 1);
    assert_eq!(media.caption_sync_state, CaptionSyncState::Pending);

    let job = claim_caption_job(&database, "caption-sync-success").await;
    let api = FakeCaptionApi::new(FakeCaptionApiMode::Success);
    let handler = sync_storage_caption_handler(database.library(), api.clone());
    handler(job.clone()).await.expect("caption sync should succeed");

    assert_eq!(api.calls(), vec![expected_api_request(&media, &source_url, storage_message_id)]);
    let completed = database
        .library()
        .find_media(media.id)
        .await
        .expect("media should be readable")
        .expect("media should exist");
    assert_eq!(completed.caption_sync_generation, 1);
    assert_eq!(completed.caption_sync_state, CaptionSyncState::Synced);
    assert_eq!(completed.caption_sync_error, None);
    let completed_job = database.jobs().complete_lease(&lease(&job)).await.unwrap();
    assert_eq!(completed_job.status, JobStatus::Succeeded);
    assert_eq!(
        caption_jobs(&database, media.id).await,
        vec![(job.id, "succeeded".to_owned(), 1, job.dedupe_key.clone())]
    );
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn stale_caption_generation_completes_without_calling_telegram(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let (initial, _, _) = ready_media(&database, 2).await;
    let generation_one =
        change_caption_metadata(&database, &initial, "generation one", &["one"]).await;
    let stale_job = claim_caption_job(&database, "caption-sync-stale-generation").await;
    assert!(matches!(
        &stale_job.command,
        sooqa_jobs::JobCommand::SyncStorageCaption(payload)
            if payload.media_id == generation_one.id && payload.generation == 1
    ));

    let generation_two =
        change_caption_metadata(&database, &generation_one, "generation two", &["two"]).await;
    assert_eq!(generation_two.caption_sync_generation, 2);
    assert_eq!(generation_two.caption_sync_state, CaptionSyncState::Pending);

    let api = FakeCaptionApi::new(FakeCaptionApiMode::Success);
    let handler = sync_storage_caption_handler(database.library(), api.clone());
    handler(stale_job.clone()).await.expect("stale caption job should be an idempotent no-op");
    assert!(api.calls().is_empty());
    let current = database
        .library()
        .find_media(generation_two.id)
        .await
        .expect("media should be readable")
        .expect("media should exist");
    assert_eq!(current.caption_sync_generation, 2);
    assert_eq!(current.caption_sync_state, CaptionSyncState::Pending);
    assert_eq!(current.description.as_deref(), Some("generation two"));

    let completed_job = database.jobs().complete_lease(&lease(&stale_job)).await.unwrap();
    assert_eq!(completed_job.status, JobStatus::Succeeded);
    let jobs = caption_jobs(&database, current.id).await;
    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[0], (stale_job.id, "succeeded".to_owned(), 1, stale_job.dedupe_key.clone()));
    assert_eq!(jobs[1].1, "queued");
    assert_eq!(jobs[1].2, 2);
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn retryable_caption_failure_keeps_media_pending_and_requeues_job(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let (initial, source_url, storage_message_id) = ready_media(&database, 3).await;
    let media =
        change_caption_metadata(&database, &initial, "retry this caption", &["retry"]).await;
    let job = claim_caption_job(&database, "caption-sync-retryable").await;
    let api = FakeCaptionApi::new(FakeCaptionApiMode::RetryableFailure);
    let handler = sync_storage_caption_handler(database.library(), api.clone());
    let failure = handler(job.clone()).await.expect_err("fake Telegram outage should retry");

    assert!(failure.retryable);
    assert_eq!(failure.class, "caption_sync");
    assert_eq!(failure.message, "temporary Telegram caption failure");
    assert_eq!(api.calls(), vec![expected_api_request(&media, &source_url, storage_message_id)]);
    let pending = database
        .library()
        .find_media(media.id)
        .await
        .expect("media should be readable")
        .expect("media should exist");
    assert_eq!(pending.caption_sync_state, CaptionSyncState::Pending);
    assert_eq!(pending.caption_sync_error, None);
    let settled = database
        .jobs()
        .settle_lease(
            &job,
            &lease(&job),
            JobSettlement::retry(
                OffsetDateTime::now_utc() + time::Duration::minutes(1),
                failure.class,
                failure.message,
            ),
        )
        .await
        .unwrap();
    assert_eq!(settled.status, JobStatus::Queued);
    assert_eq!(settled.last_error_class.as_deref(), Some("caption_sync"));
    assert_eq!(settled.last_error_message.as_deref(), Some("temporary Telegram caption failure"));
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn permanent_caption_failure_marks_media_and_job_failed(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let (initial, source_url, storage_message_id) = ready_media(&database, 4).await;
    let media =
        change_caption_metadata(&database, &initial, "permanent failure caption", &["fixed"]).await;
    let job = claim_caption_job(&database, "caption-sync-permanent").await;
    let api = FakeCaptionApi::new(FakeCaptionApiMode::PermanentFailure);
    let handler = sync_storage_caption_handler(database.library(), api.clone());
    let failure = handler(job.clone()).await.expect_err("permanent Telegram error should fail");

    assert!(!failure.retryable);
    assert_eq!(failure.class, "caption_sync");
    assert_eq!(failure.message, "permanent Telegram caption failure");
    assert_eq!(api.calls(), vec![expected_api_request(&media, &source_url, storage_message_id)]);
    let failed = database
        .library()
        .find_media(media.id)
        .await
        .expect("media should be readable")
        .expect("media should exist");
    assert_eq!(failed.caption_sync_state, CaptionSyncState::Failed);
    assert_eq!(failed.caption_sync_error.as_deref(), Some("permanent Telegram caption failure"));
    let settled = database
        .jobs()
        .settle_lease(&job, &lease(&job), JobSettlement::fail(&failure.class, &failure.message))
        .await
        .unwrap();
    assert_eq!(settled.status, JobStatus::Failed);
    assert_eq!(settled.last_error_class.as_deref(), Some("caption_sync"));
    assert_eq!(settled.last_error_message.as_deref(), Some("permanent Telegram caption failure"));
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn stale_caption_completion_enqueues_reapply_for_current_generation(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let (initial, source_url, storage_message_id) = ready_media(&database, 5).await;
    let old_metadata =
        change_caption_metadata(&database, &initial, "old in-flight caption", &["old"]).await;
    let job = claim_caption_job(&database, "caption-sync-stale-completion").await;
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let api = FakeCaptionApi::new(FakeCaptionApiMode::WaitForRelease {
        started: Arc::clone(&started),
        release: Arc::clone(&release),
    });
    let handler = sync_storage_caption_handler(database.library(), api.clone());
    let old_job_lease = lease(&job);
    let running = tokio::spawn(async move { handler(job).await });
    tokio::time::timeout(StdDuration::from_secs(2), started.notified())
        .await
        .expect("fake API call should start");
    assert_eq!(
        api.calls(),
        vec![expected_api_request(&old_metadata, &source_url, storage_message_id)]
    );

    let syncing = database
        .library()
        .find_media(old_metadata.id)
        .await
        .expect("media should be readable")
        .expect("media should exist");
    assert_eq!(syncing.caption_sync_state, CaptionSyncState::Syncing);
    let current = change_caption_metadata(
        &database,
        &syncing,
        "new current caption",
        &["current", "reviewed"],
    )
    .await;
    assert_eq!(current.caption_sync_generation, 2);
    assert_eq!(current.caption_sync_state, CaptionSyncState::Pending);

    release.notify_one();
    running
        .await
        .expect("caption handler should not panic")
        .expect("stale completion should not fail the handler");
    let completed_old_job = database.jobs().complete_lease(&old_job_lease).await.unwrap();
    assert_eq!(completed_old_job.status, JobStatus::Succeeded);

    let pending = database
        .library()
        .find_media(current.id)
        .await
        .expect("media should be readable")
        .expect("media should exist");
    assert_eq!(pending.caption_sync_generation, 2);
    assert_eq!(pending.caption_sync_state, CaptionSyncState::Pending);
    assert_eq!(pending.caption_sync_error, None);
    let jobs = caption_jobs(&database, pending.id).await;
    assert_eq!(jobs.len(), 3);
    assert!(jobs.iter().any(|(_, state, generation, dedupe_key)| {
        *generation == 2
            && state == "queued"
            && dedupe_key
                .as_deref()
                .is_some_and(|key| key == format!("media:{}:caption_sync:v1:2", pending.id))
    }));
    assert!(jobs.iter().any(|(_, state, generation, dedupe_key)| {
        *generation == 2
            && state == "queued"
            && dedupe_key.as_deref().is_some_and(|key| {
                key.starts_with(&format!("media:{}:caption_sync:v1:2:after:1:claim:", pending.id))
            })
    }));
    assert_eq!(api.calls().len(), 1);
}
