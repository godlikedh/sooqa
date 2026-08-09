use chrono::{DateTime, Duration as ChronoDuration, NaiveDateTime, NaiveTime, TimeZone, Utc};
use chrono_tz::Tz;
use sha2::{Digest, Sha256};
use sooqa_publisher::{
    Channel, ChannelValidationError, NewChannel, NewPost, Post, PostSchedule, PostState,
    PostUpdate, PublishClaim, PublishedMessage, PublisherValidationError,
};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use thiserror::Error;
use time::{OffsetDateTime, Time};
use uuid::Uuid;

#[derive(Clone)]
pub struct PublisherRepository {
    pool: PgPool,
}

#[derive(Debug, Clone)]
pub struct CreatePostResult {
    pub post: Post,
    pub created: bool,
}

#[derive(Debug, Clone, FromRow)]
struct ChannelRow {
    id: Uuid,
    telegram_chat_id: i64,
    name: String,
    is_enabled: bool,
    time_zone: String,
    window_start: Time,
    window_end: Time,
    interval_minutes: i32,
    default_parse_mode: Option<String>,
    default_disable_notification: bool,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, FromRow)]
struct PostRow {
    id: Uuid,
    request_hash: Option<Vec<u8>>,
    schedule_request_key: Option<String>,
    schedule_request_hash: Option<Vec<u8>>,
    media_id: Uuid,
    channel_id: Uuid,
    state: String,
    caption: Option<String>,
    parse_mode: Option<String>,
    disable_notification: bool,
    scheduled_at: OffsetDateTime,
    cadence_slot_at: Option<OffsetDateTime>,
    send_generation: i32,
    send_token: Option<Uuid>,
    send_started_at: Option<OffsetDateTime>,
    telegram_message_id: Option<i64>,
    published_at: Option<OffsetDateTime>,
    error_class: Option<String>,
    error_message: Option<String>,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
}

impl PublisherRepository {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create_channel(
        &self,
        channel: NewChannel,
    ) -> Result<Channel, PublisherRepositoryError> {
        channel.validate()?;
        let id = Uuid::now_v7();
        let row = sqlx::query_as::<_, ChannelRow>(
            "INSERT INTO channels (id, name, telegram_chat_id, time_zone, window_start, window_end, interval_minutes, default_parse_mode, default_disable_notification) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING *",
        )
        .bind(id)
        .bind(channel.name)
        .bind(channel.telegram_chat_id)
        .bind(channel.time_zone)
        .bind(channel.window_start)
        .bind(channel.window_end)
        .bind(channel.interval_minutes)
        .bind(channel.default_parse_mode)
        .bind(channel.default_disable_notification)
        .fetch_one(&self.pool)
        .await?;
        row.into_channel()
    }

    pub async fn find_channel(
        &self,
        id: Uuid,
    ) -> Result<Option<Channel>, PublisherRepositoryError> {
        sqlx::query_as::<_, ChannelRow>("SELECT * FROM channels WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(ChannelRow::into_channel)
            .transpose()
    }

    pub async fn list_channels(
        &self,
        include_disabled: bool,
    ) -> Result<Vec<Channel>, PublisherRepositoryError> {
        let rows = sqlx::query_as::<_, ChannelRow>(
            "SELECT * FROM channels WHERE $1 OR is_enabled ORDER BY name, id",
        )
        .bind(include_disabled)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(ChannelRow::into_channel).collect()
    }

    pub async fn create_post_idempotent(
        &self,
        post: NewPost,
        request_key: String,
        request_hash: &[u8],
    ) -> Result<CreatePostResult, PublisherRepositoryError> {
        validate_post(&post)?;
        let request_key = sooqa_publisher::normalize_request_key(request_key)?;
        validate_media_and_channel(&self.pool, post.media_id, post.channel_id).await?;
        let id = Uuid::now_v7();
        let inserted = sqlx::query_as::<_, PostRow>(
            "INSERT INTO posts (id, request_key, request_hash, media_id, channel_id, caption, parse_mode, disable_notification) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (request_key) WHERE request_key IS NOT NULL DO NOTHING RETURNING *",
        )
        .bind(id)
        .bind(&request_key)
        .bind(request_hash)
        .bind(post.media_id)
        .bind(post.channel_id)
        .bind(post.caption)
        .bind(post.parse_mode)
        .bind(post.disable_notification)
        .fetch_optional(&self.pool)
        .await?;
        if let Some(row) = inserted {
            return Ok(CreatePostResult { post: row.into_post()?, created: true });
        }
        let existing = sqlx::query_as::<_, PostRow>("SELECT * FROM posts WHERE request_key = $1")
            .bind(&request_key)
            .fetch_one(&self.pool)
            .await?;
        if existing.request_hash.as_deref() != Some(request_hash) {
            return Err(PublisherRepositoryError::RequestKeyConflict(request_key));
        }
        Ok(CreatePostResult { post: existing.into_post()?, created: false })
    }

    pub async fn find_post(&self, id: Uuid) -> Result<Option<Post>, PublisherRepositoryError> {
        sqlx::query_as::<_, PostRow>("SELECT * FROM posts WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(PostRow::into_post)
            .transpose()
    }

    pub async fn update_post(
        &self,
        id: Uuid,
        update: PostUpdate,
    ) -> Result<Post, PublisherRepositoryError> {
        if update.caption.is_none()
            && update.parse_mode.is_none()
            && update.disable_notification.is_none()
        {
            return Err(PublisherRepositoryError::EmptyUpdate);
        }
        validate_post_update(&update)?;
        let mut transaction = self.pool.begin().await?;
        let current = lock_post(&mut transaction, id).await?;
        let state = current.post_state()?;
        if !state.is_editable() {
            return Err(PublisherRepositoryError::PostNotEditable { id, state });
        }
        if update.expected_updated_at.is_some_and(|expected| expected != current.updated_at) {
            return Err(PublisherRepositoryError::OptimisticConflict(id));
        }
        let caption = update.caption.unwrap_or(current.caption.clone());
        let parse_mode = update.parse_mode.unwrap_or(current.parse_mode.clone());
        let disable_notification =
            update.disable_notification.unwrap_or(current.disable_notification);
        let row = sqlx::query_as::<_, PostRow>(
            "UPDATE posts SET caption = $2, parse_mode = $3, disable_notification = $4, updated_at = now() WHERE id = $1 RETURNING *",
        )
        .bind(id)
        .bind(caption)
        .bind(parse_mode)
        .bind(disable_notification)
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
        row.into_post()
    }

    pub async fn schedule_post(
        &self,
        schedule: PostSchedule,
    ) -> Result<Post, PublisherRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let current = lock_post(&mut transaction, schedule.post_id).await?;
        let current_state = current.post_state()?;
        let schedule_hash = schedule_request_hash(&schedule);
        if current.schedule_request_key.as_deref() == Some(schedule.request_key.as_str()) {
            if current.schedule_request_hash.as_deref() != Some(schedule_hash.as_slice()) {
                return Err(PublisherRepositoryError::RequestKeyConflict(schedule.request_key));
            }
            transaction.commit().await?;
            return current.into_post();
        }
        if matches!(
            current_state,
            PostState::Sending | PostState::Unknown | PostState::Published | PostState::Cancelled
        ) {
            return Err(PublisherRepositoryError::PostCannotBeScheduled {
                id: schedule.post_id,
                state: current_state,
            });
        }
        let channel =
            sqlx::query_as::<_, ChannelRow>("SELECT * FROM channels WHERE id = $1 FOR UPDATE")
                .bind(current.channel_id)
                .fetch_optional(&mut *transaction)
                .await?
                .ok_or(PublisherRepositoryError::ChannelMissing(current.channel_id))?;
        if !channel.is_enabled {
            return Err(PublisherRepositoryError::ChannelDisabled(channel.id));
        }
        ensure_media_ready(&mut transaction, current.media_id).await?;
        let previous = sqlx::query_scalar::<_, Option<OffsetDateTime>>(
            "SELECT MAX(cadence_slot_at) FROM posts WHERE channel_id = $1 AND id <> $2 AND state <> 'cancelled'",
        )
        .bind(channel.id)
        .bind(current.id)
        .fetch_one(&mut *transaction)
        .await?;
        let earliest = previous
            .map(|value| value + time::Duration::minutes(i64::from(channel.interval_minutes)))
            .map_or(schedule.requested_at, |value| value.max(schedule.requested_at));
        let slot = next_allowed_slot(earliest, &channel)?;
        let row = sqlx::query_as::<_, PostRow>(
            "UPDATE posts SET schedule_request_key = $2, schedule_request_hash = $3, state = 'queued', scheduled_at = $4, cadence_slot_at = $4, error_class = NULL, error_message = NULL, updated_at = now() WHERE id = $1 AND state IN ('draft', 'queued', 'failed') RETURNING *",
        )
        .bind(current.id)
        .bind(&schedule.request_key)
        .bind(schedule_hash.as_slice())
        .bind(slot)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PublisherRepositoryError::PostCannotBeScheduled {
            id: current.id,
            state: current_state,
        })?;
        enqueue_publish_job(&mut transaction, row.id, slot).await?;
        transaction.commit().await?;
        row.into_post()
    }

    pub async fn claim_publish(&self, id: Uuid) -> Result<PublishClaim, PublisherRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let current = lock_post(&mut transaction, id).await?;
        let state = current.post_state()?;
        if state != PostState::Queued {
            return Err(PublisherRepositoryError::PostNotClaimable { id, state });
        }
        let channel = sqlx::query_as::<_, ChannelRow>("SELECT * FROM channels WHERE id = $1")
            .bind(current.channel_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(PublisherRepositoryError::ChannelMissing(current.channel_id))?;
        if !channel.is_enabled {
            return Err(PublisherRepositoryError::ChannelDisabled(channel.id));
        }
        ensure_media_ready(&mut transaction, current.media_id).await?;
        let token = Uuid::now_v7();
        let row = sqlx::query_as::<_, PostRow>(
            "UPDATE posts SET state = 'sending', send_generation = send_generation + 1, send_token = $2, send_started_at = now(), updated_at = now() WHERE id = $1 AND state = 'queued' RETURNING *",
        )
        .bind(id)
        .bind(token)
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(PublishClaim { post: row.into_post()?, channel_chat_id: channel.telegram_chat_id })
    }

    pub async fn complete_publish(
        &self,
        id: Uuid,
        generation: i32,
        token: Uuid,
        telegram_message_id: i64,
    ) -> Result<PublishedMessage, PublisherRepositoryError> {
        if telegram_message_id <= 0 {
            return Err(PublisherRepositoryError::InvalidTelegramMessageId(telegram_message_id));
        }
        let mut transaction = self.pool.begin().await?;
        let current = lock_post(&mut transaction, id).await?;
        if current.post_state()? == PostState::Published {
            if current.send_generation != generation
                || current.telegram_message_id != Some(telegram_message_id)
            {
                return Err(PublisherRepositoryError::PublishConflict(id));
            }
            let channel_id = current.channel_id;
            transaction.commit().await?;
            let chat_id = self.channel_chat_id(channel_id).await?;
            return Ok(PublishedMessage { post: current.into_post()?, channel_chat_id: chat_id });
        }
        if current.post_state()? != PostState::Sending
            || current.send_generation != generation
            || current.send_token != Some(token)
        {
            return Err(PublisherRepositoryError::PublishLeaseLost(id));
        }
        let row = sqlx::query_as::<_, PostRow>(
            "UPDATE posts SET state = 'published', telegram_message_id = $2, published_at = COALESCE(published_at, now()), send_token = NULL, send_started_at = NULL, error_class = NULL, error_message = NULL, updated_at = now() WHERE id = $1 AND state = 'sending' AND send_generation = $3 AND send_token = $4 RETURNING *",
        )
        .bind(id)
        .bind(telegram_message_id)
        .bind(generation)
        .bind(token)
        .fetch_one(&mut *transaction)
        .await?;
        let channel_id = row.channel_id;
        transaction.commit().await?;
        let chat_id = self.channel_chat_id(channel_id).await?;
        Ok(PublishedMessage { post: row.into_post()?, channel_chat_id: chat_id })
    }

    pub async fn fail_publish(
        &self,
        id: Uuid,
        generation: i32,
        token: Uuid,
        state: PostState,
        error_class: &str,
        error_message: &str,
    ) -> Result<Post, PublisherRepositoryError> {
        if !matches!(state, PostState::Failed | PostState::Unknown) {
            return Err(PublisherRepositoryError::InvalidPublishFailureState(state));
        }
        let row = sqlx::query_as::<_, PostRow>(
            "UPDATE posts SET state = $2, send_token = NULL, send_started_at = NULL, error_class = $3, error_message = $4, updated_at = now() WHERE id = $1 AND state = 'sending' AND send_generation = $5 AND send_token = $6 RETURNING *",
        )
        .bind(id)
        .bind(state.as_str())
        .bind(error_class)
        .bind(error_message)
        .bind(generation)
        .bind(token)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(PublisherRepositoryError::PublishLeaseLost(id))?;
        row.into_post()
    }

    async fn channel_chat_id(&self, id: Uuid) -> Result<i64, PublisherRepositoryError> {
        sqlx::query_scalar::<_, i64>("SELECT telegram_chat_id FROM channels WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or(PublisherRepositoryError::ChannelMissing(id))
    }
}

async fn lock_post(
    transaction: &mut Transaction<'_, Postgres>,
    id: Uuid,
) -> Result<PostRow, PublisherRepositoryError> {
    sqlx::query_as::<_, PostRow>("SELECT * FROM posts WHERE id = $1 FOR UPDATE")
        .bind(id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(PublisherRepositoryError::PostMissing(id))
}

async fn validate_media_and_channel(
    pool: &PgPool,
    media_id: Uuid,
    channel_id: Uuid,
) -> Result<(), PublisherRepositoryError> {
    let exists = sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM media WHERE id = $1)")
        .bind(media_id)
        .fetch_one(pool)
        .await?;
    if !exists {
        return Err(PublisherRepositoryError::MediaMissing(media_id));
    }
    let channel = sqlx::query_as::<_, ChannelRow>("SELECT * FROM channels WHERE id = $1")
        .bind(channel_id)
        .fetch_optional(pool)
        .await?
        .ok_or(PublisherRepositoryError::ChannelMissing(channel_id))?;
    if !channel.is_enabled {
        return Err(PublisherRepositoryError::ChannelDisabled(channel_id));
    }
    Ok(())
}

async fn ensure_media_ready(
    transaction: &mut Transaction<'_, Postgres>,
    media_id: Uuid,
) -> Result<(), PublisherRepositoryError> {
    let state = sqlx::query_scalar::<_, String>("SELECT storage_state FROM media WHERE id = $1")
        .bind(media_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(PublisherRepositoryError::MediaMissing(media_id))?;
    if state != "ready" {
        return Err(PublisherRepositoryError::MediaNotReady { media_id, state });
    }
    Ok(())
}

async fn enqueue_publish_job(
    transaction: &mut Transaction<'_, Postgres>,
    post_id: Uuid,
    run_at: OffsetDateTime,
) -> Result<(), PublisherRepositoryError> {
    sqlx::query(
        "INSERT INTO queue.jobs (kind, payload, state, run_at, dedupe_key) VALUES ('publish_post', $1, 'queued', $2, $3) ON CONFLICT (dedupe_key) WHERE dedupe_key IS NOT NULL DO UPDATE SET payload = EXCLUDED.payload, run_at = EXCLUDED.run_at, state = 'queued', attempt_count = CASE WHEN queue.jobs.state = 'failed' THEN 0 ELSE queue.jobs.attempt_count END, error_class = NULL, error_message = NULL, completed_at = NULL, updated_at = now() WHERE queue.jobs.state IN ('queued', 'failed')",
    )
    .bind(serde_json::json!({ "post_id": post_id }))
    .bind(run_at)
    .bind(format!("post:{post_id}:publish:v1"))
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn schedule_request_hash(schedule: &PostSchedule) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(schedule.requested_at.unix_timestamp_nanos().to_be_bytes());
    hasher.finalize().to_vec()
}

fn next_allowed_slot(
    requested: OffsetDateTime,
    channel: &ChannelRow,
) -> Result<OffsetDateTime, PublisherRepositoryError> {
    let timezone: Tz = channel
        .time_zone
        .parse()
        .map_err(|_| PublisherRepositoryError::InvalidTimeZone(channel.time_zone.clone()))?;
    let mut candidate = to_chrono(requested)?;
    let start = to_chrono_time(channel.window_start);
    let end = to_chrono_time(channel.window_end);
    let interval = i64::from(channel.interval_minutes) * 60;
    for _ in 0..370 {
        let local = candidate.with_timezone(&timezone);
        let date = local.date_naive();
        let time = local.time();
        let target = if time < start {
            NaiveDateTime::new(date, start)
        } else if time >= end {
            NaiveDateTime::new(date + ChronoDuration::days(1), start)
        } else {
            let elapsed = time.signed_duration_since(start).num_seconds();
            let offset = ((elapsed + interval - 1) / interval) * interval;
            let target_time = start + ChronoDuration::seconds(offset);
            if target_time >= end {
                NaiveDateTime::new(date + ChronoDuration::days(1), start)
            } else {
                NaiveDateTime::new(date, target_time)
            }
        };
        let zoned = timezone
            .from_local_datetime(&target)
            .earliest()
            .or_else(|| timezone.from_local_datetime(&target).latest())
            .ok_or(PublisherRepositoryError::InvalidTimeZone(channel.time_zone.clone()))?;
        let result = zoned.with_timezone(&Utc);
        if result >= candidate {
            return from_chrono(result);
        }
        candidate += ChronoDuration::days(1);
    }
    Err(PublisherRepositoryError::CadenceSearchExhausted)
}

fn to_chrono(value: OffsetDateTime) -> Result<DateTime<Utc>, PublisherRepositoryError> {
    let seconds = value.unix_timestamp();
    let nanos = value.nanosecond();
    DateTime::from_timestamp(seconds, nanos).ok_or(PublisherRepositoryError::InvalidTimestamp)
}

fn from_chrono(value: DateTime<Utc>) -> Result<OffsetDateTime, PublisherRepositoryError> {
    OffsetDateTime::from_unix_timestamp_nanos(
        value.timestamp_nanos_opt().ok_or(PublisherRepositoryError::InvalidTimestamp)? as i128,
    )
    .map_err(|_| PublisherRepositoryError::InvalidTimestamp)
}

fn to_chrono_time(value: Time) -> NaiveTime {
    NaiveTime::from_hms_nano_opt(
        u32::from(value.hour()),
        u32::from(value.minute()),
        u32::from(value.second()),
        value.nanosecond(),
    )
    .expect("database time values must be valid")
}

impl ChannelRow {
    fn into_channel(self) -> Result<Channel, PublisherRepositoryError> {
        Ok(Channel {
            id: self.id,
            name: self.name,
            telegram_chat_id: self.telegram_chat_id,
            is_enabled: self.is_enabled,
            time_zone: self.time_zone,
            window_start: self.window_start,
            window_end: self.window_end,
            interval_minutes: self.interval_minutes,
            default_parse_mode: self.default_parse_mode,
            default_disable_notification: self.default_disable_notification,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}

impl PostRow {
    fn post_state(&self) -> Result<PostState, PublisherRepositoryError> {
        PostState::try_from(self.state.as_str()).map_err(PublisherRepositoryError::InvalidState)
    }

    fn into_post(self) -> Result<Post, PublisherRepositoryError> {
        let state = self.post_state()?;
        Ok(Post {
            id: self.id,
            media_id: self.media_id,
            channel_id: self.channel_id,
            caption: self.caption,
            parse_mode: self.parse_mode,
            disable_notification: self.disable_notification,
            state,
            scheduled_at: self.scheduled_at,
            cadence_slot_at: self.cadence_slot_at,
            send_generation: self.send_generation,
            send_token: self.send_token,
            send_started_at: self.send_started_at,
            telegram_message_id: self.telegram_message_id,
            published_at: self.published_at,
            error_class: self.error_class,
            error_message: self.error_message,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}

fn validate_post(post: &NewPost) -> Result<(), PublisherRepositoryError> {
    if let Some(caption) = &post.caption
        && caption.chars().count() > 1_024
    {
        return Err(PublisherRepositoryError::Validation(
            PublisherValidationError::CaptionTooLong { max: 1_024 },
        ));
    }
    if let Some(parse_mode) = &post.parse_mode
        && !matches!(parse_mode.as_str(), "HTML" | "MarkdownV2")
    {
        return Err(PublisherRepositoryError::Validation(
            PublisherValidationError::InvalidParseMode,
        ));
    }
    Ok(())
}

fn validate_post_update(update: &PostUpdate) -> Result<(), PublisherRepositoryError> {
    if update
        .caption
        .as_ref()
        .and_then(Option::as_ref)
        .is_some_and(|caption| caption.chars().count() > 1_024)
    {
        return Err(PublisherRepositoryError::Validation(
            PublisherValidationError::CaptionTooLong { max: 1_024 },
        ));
    }
    if update
        .parse_mode
        .as_ref()
        .and_then(Option::as_ref)
        .is_some_and(|parse_mode| !matches!(parse_mode.as_str(), "HTML" | "MarkdownV2"))
    {
        return Err(PublisherRepositoryError::Validation(
            PublisherValidationError::InvalidParseMode,
        ));
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum PublisherRepositoryError {
    #[error("publisher validation failed: {0}")]
    Validation(#[from] PublisherValidationError),
    #[error("channel validation failed: {0}")]
    ChannelValidation(#[from] ChannelValidationError),
    #[error("database returned an unknown post state: {0}")]
    InvalidState(String),
    #[error("channel {0} was not found")]
    ChannelMissing(Uuid),
    #[error("channel {0} is disabled")]
    ChannelDisabled(Uuid),
    #[error("media {0} was not found")]
    MediaMissing(Uuid),
    #[error("media {media_id} is not ready for publication: {state}")]
    MediaNotReady { media_id: Uuid, state: String },
    #[error("post {0} was not found")]
    PostMissing(Uuid),
    #[error("post {id} is not editable in state {state:?}")]
    PostNotEditable { id: Uuid, state: PostState },
    #[error("post {id} cannot be scheduled in state {state:?}")]
    PostCannotBeScheduled { id: Uuid, state: PostState },
    #[error("post {id} is not claimable in state {state:?}")]
    PostNotClaimable { id: Uuid, state: PostState },
    #[error("post request key conflicts with another post: {0}")]
    RequestKeyConflict(String),
    #[error("post {0} was updated by another request")]
    OptimisticConflict(Uuid),
    #[error("post update must change at least one field")]
    EmptyUpdate,
    #[error("post {0} publication lease was lost")]
    PublishLeaseLost(Uuid),
    #[error("post {0} publication result conflicts with the stored result")]
    PublishConflict(Uuid),
    #[error("invalid publication failure state {0:?}")]
    InvalidPublishFailureState(PostState),
    #[error("Telegram message ID must be positive, got {0}")]
    InvalidTelegramMessageId(i64),
    #[error("invalid channel time zone: {0}")]
    InvalidTimeZone(String),
    #[error("could not find an allowed cadence slot")]
    CadenceSearchExhausted,
    #[error("invalid publication timestamp")]
    InvalidTimestamp,
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}
