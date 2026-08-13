use std::{
    error::Error,
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use serde_json::json;
use sooqa_jobs::JobType;
use sooqa_library::{
    MediaIngest, MediaKind, MediaMetadata, MediaSourceInput, NewMedia, SourceKind,
};
use sooqa_persistence::Database;
use sooqa_publisher::{NewChannel, NewPost, PostSchedule, PostState, PostUpdate};
use sooqa_telegram::{TelegramPublicationApi, TelegramPublicationRequest};
use sooqa_worker::publish_post_handler;
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum FakeErrorKind {
    CopyUnavailable,
    Caption,
    Ambiguous,
    Retryable,
}

#[derive(Debug, Clone, Copy)]
struct FakeError {
    kind: FakeErrorKind,
}

impl fmt::Display for FakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "fake Telegram {:?} error", self.kind)
    }
}

impl Error for FakeError {}

#[derive(Debug, Clone, Copy)]
enum FakeOutcome {
    Success(i64),
    CopyUnavailable,
    Caption,
    Ambiguous,
    Retryable,
}

impl FakeOutcome {
    fn result(self) -> Result<i64, FakeError> {
        match self {
            Self::Success(message_id) => Ok(message_id),
            Self::CopyUnavailable => Err(FakeError { kind: FakeErrorKind::CopyUnavailable }),
            Self::Caption => Err(FakeError { kind: FakeErrorKind::Caption }),
            Self::Ambiguous => Err(FakeError { kind: FakeErrorKind::Ambiguous }),
            Self::Retryable => Err(FakeError { kind: FakeErrorKind::Retryable }),
        }
    }
}

#[derive(Debug, Clone)]
enum FakeCall {
    Copy(TelegramPublicationRequest),
    Send(TelegramPublicationRequest),
}

#[derive(Clone)]
struct FakeTelegram {
    state: Arc<Mutex<FakeTelegramState>>,
}

struct FakeTelegramState {
    copy: FakeOutcome,
    fallback: FakeOutcome,
    calls: Vec<FakeCall>,
}

impl FakeTelegram {
    fn new(copy: FakeOutcome, fallback: FakeOutcome) -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeTelegramState { copy, fallback, calls: Vec::new() })),
        }
    }

    fn calls(&self) -> Vec<FakeCall> {
        self.state.lock().unwrap().calls.clone()
    }
}

#[async_trait]
impl TelegramPublicationApi for FakeTelegram {
    type Error = FakeError;

    async fn copy_from_storage(
        &self,
        request: &TelegramPublicationRequest,
    ) -> Result<i64, Self::Error> {
        let mut state = self.state.lock().unwrap();
        state.calls.push(FakeCall::Copy(request.clone()));
        state.copy.result()
    }

    async fn send_storage_file(
        &self,
        request: &TelegramPublicationRequest,
    ) -> Result<i64, Self::Error> {
        let mut state = self.state.lock().unwrap();
        state.calls.push(FakeCall::Send(request.clone()));
        state.fallback.result()
    }

    fn is_copy_unavailable(error: &Self::Error) -> bool {
        error.kind == FakeErrorKind::CopyUnavailable
    }

    fn is_known_caption_error(error: &Self::Error) -> bool {
        error.kind == FakeErrorKind::Caption
    }

    fn is_retryable_no_effect(error: &Self::Error) -> bool {
        error.kind == FakeErrorKind::Retryable
    }

    fn is_ambiguous_error(error: &Self::Error) -> bool {
        error.kind == FakeErrorKind::Ambiguous
    }
}

async fn stored_media(database: &Database) -> Uuid {
    let seed = Uuid::new_v4();
    let mut sha256 = [0_u8; 32];
    sha256[..16].copy_from_slice(seed.as_bytes());
    sha256[16..].copy_from_slice(seed.as_bytes());
    let media = database
        .library()
        .resolve_media(MediaIngest {
            media: NewMedia::new(MediaKind::Video),
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
                sha256: Some(sha256.to_vec()),
                local_work_path: None,
            },
            source: MediaSourceInput {
                ingest_id: None,
                kind: SourceKind::DirectUrl,
                original_url: Some(format!("https://example.test/{seed}")),
                normalized_url: None,
                platform: None,
                platform_content_id: None,
                author_name: None,
                title: None,
                description: None,
                published_at: None,
                metadata: json!({}),
            },
            tags: Vec::new(),
        })
        .await
        .unwrap();
    sqlx::query("UPDATE media SET storage_state = 'ready', telegram_storage_chat_id = -100123, telegram_storage_message_id = 17, telegram_file_id = 'file-id' WHERE id = $1")
        .bind(media.media.id)
        .execute(database.pool())
        .await
        .unwrap();
    media.media.id
}

async fn due_job(
    database: &Database,
    caption: Option<&str>,
) -> (sooqa_jobs::Job, sooqa_publisher::Post) {
    let mut channel =
        NewChannel::try_new(format!("test-{}", Uuid::new_v4()), -1000000000100).unwrap();
    channel.window_start = time::Time::from_hms(0, 0, 0).unwrap();
    channel.window_end = time::Time::from_hms(23, 59, 0).unwrap();
    channel.interval_minutes = 1;
    let channel = database.publisher().create_channel(channel).await.unwrap();
    let media_id = stored_media(database).await;
    let created = database
        .publisher()
        .create_post_idempotent(
            NewPost {
                media_id,
                channel_id: channel.id,
                caption: caption.map(ToOwned::to_owned),
                parse_mode: None,
                disable_notification: false,
            },
            format!("post-{}", Uuid::new_v4()),
            b"publication-test",
        )
        .await
        .unwrap()
        .post;
    let queued = database
        .publisher()
        .schedule_post(
            PostSchedule::try_new(
                created.id,
                OffsetDateTime::now_utc(),
                format!("schedule-{}", Uuid::new_v4()),
                0,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    database
        .publisher()
        .publish_now(queued.id, format!("now-{}", Uuid::new_v4()), queued.revision)
        .await
        .unwrap();
    let job = database
        .jobs()
        .claim_next("publication-test", Duration::from_secs(60), &[JobType::PublishPost])
        .await
        .unwrap()
        .expect("publish job should be due");
    (job, created)
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn publication_success_copies_storage_and_overrides_caption(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let (job, created) = due_job(&database, Some("public caption")).await;
    let telegram = FakeTelegram::new(FakeOutcome::Success(91), FakeOutcome::Success(92));
    let handler = publish_post_handler(database.publisher(), database.library(), telegram.clone());

    handler(job.clone()).await.unwrap();
    database.jobs().complete_lease(&job.lease().unwrap()).await.unwrap();
    let published = database.publisher().find_post(created.id).await.unwrap().unwrap();
    assert_eq!(published.state, PostState::Published);
    assert_eq!(published.telegram_message_id, Some(91));
    assert!(
        matches!(telegram.calls().as_slice(), [FakeCall::Copy(request)] if request.caption.as_deref() == Some("public caption"))
    );
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn unavailable_copy_falls_back_to_file_id_and_clears_missing_caption(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let (job, created) = due_job(&database, None).await;
    let telegram = FakeTelegram::new(FakeOutcome::CopyUnavailable, FakeOutcome::Success(92));
    let handler = publish_post_handler(database.publisher(), database.library(), telegram.clone());

    handler(job.clone()).await.unwrap();
    database.jobs().complete_lease(&job.lease().unwrap()).await.unwrap();
    let published = database.publisher().find_post(created.id).await.unwrap().unwrap();
    assert_eq!(published.state, PostState::Published);
    assert!(
        matches!(telegram.calls().as_slice(), [FakeCall::Copy(_), FakeCall::Send(request)] if request.caption.is_none() && request.telegram_file_id.as_deref() == Some("file-id"))
    );
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn ambiguous_publication_becomes_unknown_without_fallback(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let (job, created) = due_job(&database, Some("caption")).await;
    let telegram = FakeTelegram::new(FakeOutcome::Ambiguous, FakeOutcome::Success(92));
    let handler = publish_post_handler(database.publisher(), database.library(), telegram.clone());

    assert!(handler(job.clone()).await.is_err());
    database
        .jobs()
        .fail_lease(&job.lease().unwrap(), "publication_unknown", "ambiguous")
        .await
        .unwrap();
    let post = database.publisher().find_post(created.id).await.unwrap().unwrap();
    assert_eq!(post.state, PostState::Unknown);
    assert!(matches!(telegram.calls().as_slice(), [FakeCall::Copy(_)]));
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn caption_rejection_is_failed_and_can_be_corrected_and_requeued(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let (job, created) = due_job(&database, Some("bad <caption")).await;
    let telegram = FakeTelegram::new(FakeOutcome::Caption, FakeOutcome::Success(92));
    let handler = publish_post_handler(database.publisher(), database.library(), telegram);

    assert!(handler(job.clone()).await.is_err());
    database
        .jobs()
        .fail_lease(&job.lease().unwrap(), "caption_rejected", "bad entities")
        .await
        .unwrap();
    let failed = database.publisher().find_post(created.id).await.unwrap().unwrap();
    assert_eq!(failed.state, PostState::Failed);
    let corrected = database
        .publisher()
        .update_post(
            created.id,
            PostUpdate {
                caption: Some(Some("corrected".to_owned())),
                parse_mode: None,
                disable_notification: None,
                expected_updated_at: None,
                expected_revision: failed.revision,
            },
        )
        .await
        .unwrap();
    assert_eq!(corrected.state, PostState::Failed);
    assert_eq!(corrected.caption.as_deref(), Some("corrected"));
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM queue.jobs WHERE id = $1")
            .bind(job.id)
            .fetch_one(database.pool())
            .await
            .unwrap(),
        "failed"
    );
    let requeued = database
        .publisher()
        .schedule_post(
            PostSchedule::try_new(
                corrected.id,
                OffsetDateTime::now_utc(),
                "caption-correction-requeue",
                corrected.revision,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(requeued.state, PostState::Queued);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM queue.jobs WHERE id = $1")
            .bind(job.id)
            .fetch_one(database.pool())
            .await
            .unwrap(),
        "queued"
    );
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn explicitly_safe_telegram_failure_is_retried_without_duplicate_send(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let (job, created) = due_job(&database, Some("caption")).await;
    let telegram = FakeTelegram::new(FakeOutcome::Retryable, FakeOutcome::Success(92));
    let handler = publish_post_handler(database.publisher(), database.library(), telegram);

    assert!(handler(job.clone()).await.is_err());
    let queued = database.publisher().find_post(created.id).await.unwrap().unwrap();
    assert_eq!(queued.state, PostState::Queued);
    assert_eq!(queued.error_class.as_deref(), Some("telegram_retryable"));
    let payload: serde_json::Value =
        sqlx::query_scalar("SELECT payload FROM queue.jobs WHERE id = $1")
            .bind(job.id)
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert_eq!(payload["expected_revision"], queued.revision);
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn recovered_publication_reconciles_sending_before_any_second_send(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let (job, created) = due_job(&database, Some("caption")).await;
    let old_attempt = job.lease().unwrap();
    let expected_revision = match &job.command {
        sooqa_jobs::JobCommand::PublishPost(payload) => payload.expected_revision,
        _ => unreachable!("due_job creates a publish job"),
    };
    let claim = database
        .publisher()
        .claim_publish(created.id, expected_revision, &old_attempt)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE queue.jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1",
    )
    .bind(job.id)
    .execute(database.pool())
    .await
    .unwrap();
    assert_eq!(database.jobs().recover_stale_leases().await.unwrap(), 1);

    let recovered_job = database
        .jobs()
        .claim_next("publication-recovery", Duration::from_secs(60), &[JobType::PublishPost])
        .await
        .unwrap()
        .expect("recovered publication should be claimable");
    let telegram = FakeTelegram::new(FakeOutcome::Success(99), FakeOutcome::Success(100));
    let handler = publish_post_handler(database.publisher(), database.library(), telegram.clone());
    handler(recovered_job.clone()).await.unwrap();
    database.jobs().complete_lease(&recovered_job.lease().unwrap()).await.unwrap();

    let reconciled = database.publisher().find_post(created.id).await.unwrap().unwrap();
    assert_eq!(reconciled.state, PostState::Unknown);
    assert_eq!(reconciled.error_class.as_deref(), Some("publication_interrupted"));
    assert!(telegram.calls().is_empty());
    let stale_lease = sooqa_persistence::PublishLease {
        generation: claim.post.send_generation,
        token: claim.post.send_token.unwrap(),
        attempt: old_attempt,
    };
    assert!(matches!(
        database.publisher().complete_publish(created.id, &stale_lease, 99).await,
        Err(sooqa_persistence::PublisherRepositoryError::PublishLeaseLost(_))
    ));
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn final_retry_fails_post_before_job_is_failed(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let (job, created) = due_job(&database, Some("caption")).await;
    sqlx::query("UPDATE queue.jobs SET max_attempts = 1 WHERE id = $1")
        .bind(job.id)
        .execute(database.pool())
        .await
        .unwrap();
    let telegram = FakeTelegram::new(FakeOutcome::Retryable, FakeOutcome::Success(92));
    let handler = publish_post_handler(database.publisher(), database.library(), telegram);

    assert!(handler(job.clone()).await.is_err());
    let failed = database.publisher().find_post(created.id).await.unwrap().unwrap();
    assert_eq!(failed.state, PostState::Failed);
    assert_eq!(
        database
            .jobs()
            .fail_lease(&job.lease().unwrap(), "telegram_retryable", "final retry")
            .await
            .unwrap()
            .status,
        sooqa_jobs::JobStatus::Failed
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM queue.jobs WHERE id = $1")
            .bind(job.id)
            .fetch_one(database.pool())
            .await
            .unwrap(),
        "failed"
    );
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn malformed_ready_storage_receipt_fails_before_telegram_call(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let (job, created) = due_job(&database, Some("caption")).await;
    let media_id: Uuid = sqlx::query_scalar("SELECT media_id FROM posts WHERE id = $1")
        .bind(created.id)
        .fetch_one(database.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE media SET telegram_storage_message_id = 0 WHERE id = $1")
        .bind(media_id)
        .execute(database.pool())
        .await
        .unwrap();
    let telegram = FakeTelegram::new(FakeOutcome::Success(91), FakeOutcome::Success(92));
    let handler = publish_post_handler(database.publisher(), database.library(), telegram.clone());

    assert!(handler(job.clone()).await.is_err());
    assert!(telegram.calls().is_empty());
    assert_eq!(
        database.publisher().find_post(created.id).await.unwrap().unwrap().state,
        PostState::Failed
    );
}
