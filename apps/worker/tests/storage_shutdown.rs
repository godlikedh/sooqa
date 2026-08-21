use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use serde_json::json;
use sooqa_inbox::{IngestStatus, IngestSubmission, IngestSubmissionInput, SubmittedVia};
use sooqa_jobs::{JobType, NewJob};
use sooqa_library::{
    MediaIngest, MediaKind, MediaMetadata, MediaSourceInput, NewMedia, SourceKind,
    StorageCaptionMetadata, StorageReceipt, StorageUploadAttachment, StorageUploadReservation,
    StorageUploadReservationRequest, StorageUploadStore,
};
use sooqa_media::sha256_file;
use sooqa_persistence::{Database, LibraryRepository, LibraryRepositoryError};
use sooqa_telegram::{
    StorageUploadProvider, StorageUploadRequest, StorageUploadResult, TelegramStorageApi,
};
use sooqa_worker::{HandlerRegistry, Worker, upload_storage_asset_cancellable_handler};
use thiserror::Error;
use tokio::sync::Notify;
use uuid::Uuid;

#[derive(Clone)]
struct ControllableTelegram {
    request_started: Arc<Notify>,
    release_request: Arc<Notify>,
    calls: Arc<AtomicUsize>,
}

#[derive(Debug, Error)]
#[error("controllable Telegram API failed")]
struct ControllableTelegramError;

#[async_trait]
impl TelegramStorageApi for ControllableTelegram {
    type Error = ControllableTelegramError;

    async fn upload_media(
        &self,
        _request: StorageUploadRequest,
    ) -> Result<StorageUploadResult, Self::Error> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.request_started.notify_one();
        self.release_request.notified().await;
        Ok(StorageUploadResult {
            storage_message_id: 42,
            telegram_file_id: "telegram-file".to_owned(),
            telegram_file_unique_id: "telegram-unique".to_owned(),
        })
    }

    async fn verify_storage_chat(&self, _chat_id: i64) -> Result<(), Self::Error> {
        Ok(())
    }

    fn is_ambiguous_error(_error: &Self::Error) -> bool {
        false
    }
}

#[derive(Clone)]
struct ReservationGateStore {
    inner: LibraryRepository,
    reserved: Arc<Notify>,
    release: Arc<Notify>,
}

#[async_trait]
impl StorageUploadStore for ReservationGateStore {
    type Error = LibraryRepositoryError;

    async fn find_media(
        &self,
        media_id: Uuid,
    ) -> Result<Option<sooqa_library::Media>, Self::Error> {
        self.inner.find_media(media_id).await
    }

    async fn find_media_preview(
        &self,
        media_id: Uuid,
    ) -> Result<Option<sooqa_library::MediaPreviewData>, Self::Error> {
        self.inner.find_media_preview(media_id).await
    }

    async fn find_storage_caption_metadata(
        &self,
        media_id: Uuid,
    ) -> Result<StorageCaptionMetadata, Self::Error> {
        self.inner.find_storage_caption_metadata(media_id).await
    }

    async fn find_storage_receipt(
        &self,
        media_id: Uuid,
    ) -> Result<Option<StorageReceipt>, Self::Error> {
        self.inner.find_storage_receipt(media_id).await
    }

    async fn reserve_storage_upload(
        &self,
        request: StorageUploadReservationRequest,
    ) -> Result<StorageUploadReservation, Self::Error> {
        let reservation = self.inner.reserve_storage_upload(request).await?;
        if matches!(reservation, StorageUploadReservation::Reserved { .. }) {
            self.reserved.notify_one();
            self.release.notified().await;
        }
        Ok(reservation)
    }

    async fn renew_storage_upload(
        &self,
        media_id: Uuid,
        owner_token: Uuid,
        lease_duration: Duration,
    ) -> Result<time::OffsetDateTime, Self::Error> {
        self.inner.renew_storage_upload(media_id, owner_token, lease_duration).await
    }

    async fn complete_storage_upload(
        &self,
        media_id: Uuid,
        owner_token: Uuid,
        attachment: StorageUploadAttachment,
    ) -> Result<StorageReceipt, Self::Error> {
        self.inner.complete_storage_upload(media_id, owner_token, attachment).await
    }

    async fn release_storage_upload(
        &self,
        media_id: Uuid,
        owner_token: Uuid,
    ) -> Result<(), Self::Error> {
        self.inner.release_storage_upload(media_id, owner_token).await
    }

    async fn mark_storage_upload_unknown(
        &self,
        media_id: Uuid,
        owner_token: Uuid,
    ) -> Result<(), Self::Error> {
        StorageUploadStore::mark_storage_upload_unknown(&self.inner, media_id, owner_token).await
    }
}

struct StorageFixture {
    root: PathBuf,
    ingest_id: Uuid,
    media_id: Uuid,
    job_id: Uuid,
}

async fn seed_storage_upload(database: &Database) -> StorageFixture {
    let root = std::env::temp_dir().join(format!("sooqa-storage-shutdown-{}", Uuid::new_v4()));
    tokio::fs::create_dir_all(&root).await.unwrap();
    let path = root.join("canonical.mp4");
    tokio::fs::write(&path, b"canonical storage fixture").await.unwrap();
    let digest = sha256_file(&path).await.unwrap();

    let created = database
        .inbox()
        .create_ingest(
            IngestSubmission::try_new(IngestSubmissionInput::new(
                format!("https://example.test/storage-shutdown-{}", Uuid::new_v4()),
                SubmittedVia::Api,
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    sqlx::query("DELETE FROM queue.jobs WHERE payload->>'ingest_id' = $1")
        .bind(created.ingest.id.to_string())
        .execute(database.pool())
        .await
        .unwrap();

    let media = database
        .library()
        .resolve_media(MediaIngest {
            media: NewMedia {
                kind: MediaKind::Video,
                title: Some("storage shutdown fixture".to_owned()),
                description: None,
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
                file_size_bytes: Some(digest.bytes),
                sha256: Some(hex_bytes(&digest.sha256)),
                local_work_path: Some(path.to_string_lossy().into_owned()),
                preview: None,
            },
            source: MediaSourceInput {
                ingest_id: None,
                kind: SourceKind::DirectUrl,
                original_url: Some("https://example.test/storage-shutdown".to_owned()),
                normalized_url: Some("https://example.test/storage-shutdown".to_owned()),
                platform: None,
                platform_content_id: None,
                author_name: None,
                title: None,
                description: None,
                published_at: None,
                metadata: json!({"test": "storage_shutdown"}),
            },
            tags: Vec::new(),
        })
        .await
        .unwrap()
        .media;
    sqlx::query("UPDATE ingests SET state = 'storing', media_id = $2 WHERE id = $1")
        .bind(created.ingest.id)
        .bind(media.id)
        .execute(database.pool())
        .await
        .unwrap();
    let job = database
        .jobs()
        .enqueue(
            NewJob::upload_storage_asset_generation(media.id, 0)
                .dedupe_key(format!("test:storage-shutdown:{}", media.id)),
        )
        .await
        .unwrap();

    StorageFixture { root, ingest_id: created.ingest.id, media_id: media.id, job_id: job.id }
}

fn hex_bytes(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

async fn assert_storage_unknown(database: &Database, fixture: &StorageFixture) {
    let media = database.library().find_media(fixture.media_id).await.unwrap().unwrap();
    assert_eq!(media.storage_state, sooqa_library::MediaStorageState::Unknown);
    assert_eq!(
        sqlx::query_scalar::<_, Option<Uuid>>("SELECT storage_token FROM media WHERE id = $1")
            .bind(fixture.media_id)
            .fetch_one(database.pool())
            .await
            .unwrap(),
        None
    );
    let ingest = database.inbox().find(fixture.ingest_id).await.unwrap().unwrap();
    assert_eq!(ingest.status, IngestStatus::FailedTerminal);
    assert_eq!(ingest.error_code.as_deref(), Some("storage_unknown"));
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM queue.jobs WHERE id = $1")
            .bind(fixture.job_id)
            .fetch_one(database.pool())
            .await
            .unwrap(),
        "failed"
    );
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn worker_shutdown_before_storage_dispatch_releases_and_requeues(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let fixture = seed_storage_upload(&database).await;
    sqlx::query("UPDATE queue.jobs SET max_attempts = 1 WHERE id = $1")
        .bind(fixture.job_id)
        .execute(database.pool())
        .await
        .unwrap();
    let api = ControllableTelegram {
        request_started: Arc::new(Notify::new()),
        release_request: Arc::new(Notify::new()),
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let gate = ReservationGateStore {
        inner: database.library(),
        reserved: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
    };
    let reserved = Arc::clone(&gate.reserved);
    let release = Arc::clone(&gate.release);
    let cancelled = Arc::new(Notify::new());
    let cancelled_signal = Arc::clone(&cancelled);
    let provider = StorageUploadProvider::new(api.clone(), gate, -100123)
        .unwrap()
        .with_work_root(fixture.root.clone());
    let mut registry = HandlerRegistry::new();
    let handler = upload_storage_asset_cancellable_handler(database.inbox(), provider);
    registry.register_cancellable(JobType::UploadStorageAsset, move |job, cancellation| {
        let storage_cancellation = cancellation.storage_upload();
        let cancelled_signal = Arc::clone(&cancelled_signal);
        tokio::spawn(async move {
            storage_cancellation.cancelled().await;
            cancelled_signal.notify_one();
        });
        handler(job, cancellation)
    });
    let worker = Worker::new(
        database.jobs(),
        registry,
        "storage-shutdown-before-dispatch",
        Duration::from_millis(10),
        Duration::from_secs(60),
    )
    .unwrap();
    let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
    let worker_task = tokio::spawn(async move {
        worker
            .run(async move {
                let _ = shutdown_receiver.await;
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), reserved.notified())
        .await
        .expect("storage reservation should be reached");
    shutdown_sender.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), cancelled.notified())
        .await
        .expect("worker should signal cancellation before dispatch is released");
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), worker_task)
        .await
        .expect("worker should stop after safe cancellation")
        .expect("worker task should join")
        .expect("safe cancellation should not fail the worker");

    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT storage_state FROM media WHERE id = $1")
            .bind(fixture.media_id)
            .fetch_one(database.pool())
            .await
            .unwrap(),
        "pending_storage"
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<Uuid>>("SELECT storage_token FROM media WHERE id = $1")
            .bind(fixture.media_id)
            .fetch_one(database.pool())
            .await
            .unwrap(),
        None
    );
    assert_eq!(api.calls.load(Ordering::Relaxed), 0);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM queue.jobs WHERE id = $1")
            .bind(fixture.job_id)
            .fetch_one(database.pool())
            .await
            .unwrap(),
        "queued"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT attempt_count FROM queue.jobs WHERE id = $1")
            .bind(fixture.job_id)
            .fetch_one(database.pool())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        database.inbox().find(fixture.ingest_id).await.unwrap().unwrap().status,
        IngestStatus::Storing
    );
    tokio::fs::remove_dir_all(fixture.root).await.unwrap();
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn heartbeat_loss_before_storage_dispatch_releases_and_requeues_final_attempt(
    pool: sqlx::PgPool,
) {
    let database = Database::from_pool(pool);
    let fixture = seed_storage_upload(&database).await;
    sqlx::query("UPDATE queue.jobs SET max_attempts = 1 WHERE id = $1")
        .bind(fixture.job_id)
        .execute(database.pool())
        .await
        .unwrap();
    let api = ControllableTelegram {
        request_started: Arc::new(Notify::new()),
        release_request: Arc::new(Notify::new()),
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let gate = ReservationGateStore {
        inner: database.library(),
        reserved: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
    };
    let reserved = Arc::clone(&gate.reserved);
    let release = Arc::clone(&gate.release);
    let cancelled = Arc::new(Notify::new());
    let cancelled_signal = Arc::clone(&cancelled);
    let provider = StorageUploadProvider::new(api.clone(), gate, -100123)
        .unwrap()
        .with_work_root(fixture.root.clone());
    let mut registry = HandlerRegistry::new();
    let handler = upload_storage_asset_cancellable_handler(database.inbox(), provider);
    registry.register_cancellable(JobType::UploadStorageAsset, move |job, cancellation| {
        let storage_cancellation = cancellation.storage_upload();
        let cancelled_signal = Arc::clone(&cancelled_signal);
        tokio::spawn(async move {
            storage_cancellation.cancelled().await;
            cancelled_signal.notify_one();
        });
        handler(job, cancellation)
    });
    let worker = Worker::new(
        database.jobs(),
        registry,
        "storage-heartbeat-before-dispatch",
        Duration::from_millis(10),
        Duration::from_secs(3),
    )
    .unwrap();
    let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
    let worker_task = tokio::spawn(async move {
        worker
            .run(async move {
                let _ = shutdown_receiver.await;
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), reserved.notified())
        .await
        .expect("storage reservation should be reached");
    sqlx::query(
        "UPDATE queue.jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1",
    )
    .bind(fixture.job_id)
    .execute(database.pool())
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), cancelled.notified())
        .await
        .expect("heartbeat loss should cancel the upload before dispatch");
    release.notify_one();
    shutdown_sender.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(8), worker_task)
        .await
        .expect("worker should stop after safe heartbeat cancellation")
        .expect("worker task should join")
        .expect("safe heartbeat cancellation should not fail the worker");

    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT storage_state FROM media WHERE id = $1")
            .bind(fixture.media_id)
            .fetch_one(database.pool())
            .await
            .unwrap(),
        "pending_storage"
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<Uuid>>("SELECT storage_token FROM media WHERE id = $1")
            .bind(fixture.media_id)
            .fetch_one(database.pool())
            .await
            .unwrap(),
        None
    );
    assert_eq!(api.calls.load(Ordering::Relaxed), 0);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM queue.jobs WHERE id = $1")
            .bind(fixture.job_id)
            .fetch_one(database.pool())
            .await
            .unwrap(),
        "queued"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT attempt_count FROM queue.jobs WHERE id = $1")
            .bind(fixture.job_id)
            .fetch_one(database.pool())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        database.inbox().find(fixture.ingest_id).await.unwrap().unwrap().status,
        IngestStatus::Storing
    );
    tokio::fs::remove_dir_all(fixture.root).await.unwrap();
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn worker_shutdown_after_storage_dispatch_marks_unknown(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let fixture = seed_storage_upload(&database).await;
    let api = ControllableTelegram {
        request_started: Arc::new(Notify::new()),
        release_request: Arc::new(Notify::new()),
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let provider = StorageUploadProvider::new(api.clone(), database.library(), -100123)
        .unwrap()
        .with_work_root(fixture.root.clone());
    let mut registry = HandlerRegistry::new();
    let handler = upload_storage_asset_cancellable_handler(database.inbox(), provider);
    registry.register_cancellable(JobType::UploadStorageAsset, move |job, cancellation| {
        handler(job, cancellation)
    });
    let worker = Worker::new(
        database.jobs(),
        registry,
        "storage-shutdown-after-dispatch",
        Duration::from_millis(10),
        Duration::from_secs(60),
    )
    .unwrap();
    let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
    let worker_task = tokio::spawn(async move {
        worker
            .run(async move {
                let _ = shutdown_receiver.await;
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), api.request_started.notified())
        .await
        .expect("Telegram API should observe the upload");
    let queued_duplicate = database
        .jobs()
        .enqueue(
            NewJob::upload_storage_asset_generation(fixture.media_id, 0)
                .dedupe_key(format!("test:storage-shutdown:queued-duplicate:{}", fixture.media_id)),
        )
        .await
        .expect("a queued duplicate should be inserted for settlement coverage");
    shutdown_sender.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), worker_task)
        .await
        .expect("worker should stop after ambiguous cancellation")
        .expect("worker task should join")
        .expect("ambiguous cancellation should be settled, not crash the worker");

    assert_storage_unknown(&database, &fixture).await;
    assert_eq!(api.calls.load(Ordering::Relaxed), 1);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM queue.jobs WHERE id = $1")
            .bind(queued_duplicate.id)
            .fetch_one(database.pool())
            .await
            .unwrap(),
        "failed"
    );
    tokio::fs::remove_dir_all(fixture.root).await.unwrap();
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn heartbeat_loss_during_storage_dispatch_marks_unknown(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let fixture = seed_storage_upload(&database).await;
    let api = ControllableTelegram {
        request_started: Arc::new(Notify::new()),
        release_request: Arc::new(Notify::new()),
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let provider = StorageUploadProvider::new(api.clone(), database.library(), -100123)
        .unwrap()
        .with_work_root(fixture.root.clone());
    let mut registry = HandlerRegistry::new();
    let handler = upload_storage_asset_cancellable_handler(database.inbox(), provider);
    registry.register_cancellable(JobType::UploadStorageAsset, move |job, cancellation| {
        handler(job, cancellation)
    });
    let worker = Worker::new(
        database.jobs(),
        registry,
        "storage-heartbeat-loss",
        Duration::from_millis(10),
        Duration::from_secs(3),
    )
    .unwrap();
    let worker_task = tokio::spawn(async move { worker.run(std::future::pending()).await });
    tokio::time::timeout(Duration::from_secs(5), api.request_started.notified())
        .await
        .expect("Telegram API should observe the upload");
    sqlx::query(
        "UPDATE queue.jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1",
    )
    .bind(fixture.job_id)
    .execute(database.pool())
    .await
    .unwrap();
    let worker_result = tokio::time::timeout(Duration::from_secs(8), worker_task)
        .await
        .expect("heartbeat loss should stop the worker")
        .expect("worker task should join");
    assert!(worker_result.is_err(), "lost heartbeat should be surfaced to the supervisor");

    database.jobs().recover_stale_leases().await.unwrap();
    assert_storage_unknown(&database, &fixture).await;
    assert_eq!(api.calls.load(Ordering::Relaxed), 1);
    tokio::fs::remove_dir_all(fixture.root).await.unwrap();
}
