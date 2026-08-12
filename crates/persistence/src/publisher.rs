use chrono::{DateTime, Duration as ChronoDuration, NaiveDateTime, NaiveTime, TimeZone, Utc};
use chrono_tz::Tz;
use sha2::{Digest, Sha256};
use sooqa_publisher::{
    Channel, ChannelValidationError, NewChannel, NewPost, Post, PostSchedule, PostState,
    PostUpdate, PublishClaim, PublishedMessage, PublisherValidationError, QueueDirection,
    validate_caption,
};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use std::collections::HashMap;
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
    revision: i64,
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
        let channel_id = post_channel_id(&mut transaction, id).await?;
        lock_channel(&mut transaction, channel_id).await?;
        let current = lock_post(&mut transaction, id).await?;
        let state = current.post_state()?;
        if !state.is_queue_mutable() {
            return Err(PublisherRepositoryError::PostNotEditable { id, state });
        }
        check_expected_revision(&current, update.expected_revision)?;
        if update.expected_updated_at.is_some_and(|expected| expected != current.updated_at) {
            return Err(PublisherRepositoryError::OptimisticConflict(id));
        }
        let caption = update.caption.unwrap_or(current.caption.clone());
        let parse_mode = update.parse_mode.unwrap_or(current.parse_mode.clone());
        let disable_notification =
            update.disable_notification.unwrap_or(current.disable_notification);
        let row = sqlx::query_as::<_, PostRow>(
            "UPDATE posts SET caption = $2, parse_mode = $3, disable_notification = $4, revision = revision + 1, updated_at = now() WHERE id = $1 RETURNING *",
        )
        .bind(id)
        .bind(caption)
        .bind(parse_mode)
        .bind(disable_notification)
        .fetch_one(&mut *transaction)
        .await?;
        if state != PostState::Draft {
            update_publish_job(&mut transaction, id, row.scheduled_at, row.revision).await?;
        }
        transaction.commit().await?;
        row.into_post()
    }

    pub async fn schedule_post(
        &self,
        schedule: PostSchedule,
    ) -> Result<Post, PublisherRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let channel_id = post_channel_id(&mut transaction, schedule.post_id).await?;
        let channel = lock_channel(&mut transaction, channel_id).await?;
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
        if !current_state.is_queue_mutable() {
            return Err(PublisherRepositoryError::PostCannotBeScheduled {
                id: schedule.post_id,
                state: current_state,
            });
        }
        check_expected_revision(&current, schedule.expected_revision)?;
        ensure_channel_enabled(&channel)?;
        ensure_media_ready(&mut transaction, current.media_id).await?;
        let slot =
            next_free_slot(&mut transaction, &channel, current.id, schedule.requested_at).await?;
        let revision = next_revision(current.revision)?;
        let row = sqlx::query_as::<_, PostRow>(
            "UPDATE posts SET schedule_request_key = $2, schedule_request_hash = $3, state = 'queued', scheduled_at = $4, cadence_slot_at = $4, revision = $5, error_class = NULL, error_message = NULL, updated_at = now() WHERE id = $1 AND state IN ('draft', 'queued', 'failed') RETURNING *",
        )
        .bind(current.id)
        .bind(&schedule.request_key)
        .bind(schedule_hash.as_slice())
        .bind(slot)
        .bind(revision)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PublisherRepositoryError::PostCannotBeScheduled {
            id: current.id,
            state: current_state,
        })?;
        if current_state == PostState::Draft {
            insert_publish_job(&mut transaction, row.id, slot, row.revision).await?;
        } else {
            update_publish_job(&mut transaction, row.id, slot, row.revision).await?;
        }
        transaction.commit().await?;
        row.into_post()
    }

    pub async fn enqueue_post(
        &self,
        schedule: PostSchedule,
    ) -> Result<Post, PublisherRepositoryError> {
        self.schedule_post(schedule).await
    }

    pub async fn publish_now(
        &self,
        id: Uuid,
        request_key: String,
        expected_revision: Option<i64>,
    ) -> Result<Post, PublisherRepositoryError> {
        let request_key = sooqa_publisher::normalize_request_key(request_key)?;
        let operation_key = format!("publish_now:{request_key}");
        let request_hash = publish_now_request_hash(&request_key);
        let mut transaction = self.pool.begin().await?;
        let channel_id = post_channel_id(&mut transaction, id).await?;
        let channel = lock_channel(&mut transaction, channel_id).await?;
        let current = lock_post(&mut transaction, id).await?;
        let state = current.post_state()?;
        if current.schedule_request_key.as_deref() == Some(operation_key.as_str()) {
            if current.schedule_request_hash.as_deref() != Some(request_hash.as_slice()) {
                return Err(PublisherRepositoryError::RequestKeyConflict(operation_key));
            }
            transaction.commit().await?;
            return current.into_post();
        }
        if !state.is_queue_mutable() {
            return Err(PublisherRepositoryError::PostCannotBeScheduled { id, state });
        }
        check_expected_revision(&current, expected_revision)?;
        ensure_channel_enabled(&channel)?;
        ensure_media_ready(&mut transaction, current.media_id).await?;
        let now = OffsetDateTime::now_utc();
        let revision = next_revision(current.revision)?;
        let row = sqlx::query_as::<_, PostRow>(
            "UPDATE posts SET schedule_request_key = $2, schedule_request_hash = $3, state = 'queued', scheduled_at = $4, revision = $5, error_class = NULL, error_message = NULL, updated_at = now() WHERE id = $1 AND state IN ('draft', 'queued', 'failed') RETURNING *",
        )
        .bind(id)
        .bind(&operation_key)
        .bind(request_hash.as_slice())
        .bind(now)
        .bind(revision)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PublisherRepositoryError::PostCannotBeScheduled { id, state })?;
        if state == PostState::Draft {
            insert_publish_job(&mut transaction, id, now, row.revision).await?;
        } else {
            update_publish_job(&mut transaction, id, now, row.revision).await?;
        }
        transaction.commit().await?;
        row.into_post()
    }

    pub async fn move_adjacent(
        &self,
        id: Uuid,
        direction: QueueDirection,
        expected_revision: Option<i64>,
    ) -> Result<Post, PublisherRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let channel_id = post_channel_id(&mut transaction, id).await?;
        let _channel = lock_channel(&mut transaction, channel_id).await?;
        let current_snapshot = lock_post(&mut transaction, id).await?;
        let current_state = current_snapshot.post_state()?;
        if current_state != PostState::Queued {
            return Err(PublisherRepositoryError::PostNotEditable { id, state: current_state });
        }
        check_expected_revision(&current_snapshot, expected_revision)?;
        let adjacent_id = adjacent_post_id(&mut transaction, &current_snapshot, direction).await?;
        let Some(adjacent_id) = adjacent_id else {
            transaction.commit().await?;
            return current_snapshot.into_post();
        };
        let mut rows = lock_posts(&mut transaction, &[id, adjacent_id]).await?;
        let current = rows.remove(&id).ok_or(PublisherRepositoryError::PostMissing(id))?;
        let adjacent =
            rows.remove(&adjacent_id).ok_or(PublisherRepositoryError::PostMissing(adjacent_id))?;
        if adjacent.post_state()? != PostState::Queued {
            return Err(PublisherRepositoryError::PostNotEditable {
                id: adjacent.id,
                state: adjacent.post_state()?,
            });
        }
        let current_slot =
            current.cadence_slot_at.ok_or(PublisherRepositoryError::PostNotScheduled(id))?;
        let adjacent_slot = adjacent
            .cadence_slot_at
            .ok_or(PublisherRepositoryError::PostNotScheduled(adjacent.id))?;
        let current_revision = next_revision(current.revision)?;
        let adjacent_revision = next_revision(adjacent.revision)?;
        let current_row =
            update_post_slot(&mut transaction, &current, adjacent_slot, current_revision).await?;
        let adjacent_row =
            update_post_slot(&mut transaction, &adjacent, current_slot, adjacent_revision).await?;
        update_publish_job(&mut transaction, current.id, adjacent_slot, current_row.revision)
            .await?;
        update_publish_job(&mut transaction, adjacent.id, current_slot, adjacent_row.revision)
            .await?;
        transaction.commit().await?;
        current_row.into_post()
    }

    pub async fn set_slot(
        &self,
        id: Uuid,
        slot: OffsetDateTime,
        expected_revision: Option<i64>,
    ) -> Result<Post, PublisherRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let channel_id = post_channel_id(&mut transaction, id).await?;
        let channel = lock_channel(&mut transaction, channel_id).await?;
        let current_snapshot = lock_post(&mut transaction, id).await?;
        let current_state = current_snapshot.post_state()?;
        if current_state != PostState::Queued {
            return Err(PublisherRepositoryError::PostNotEditable { id, state: current_state });
        }
        check_expected_revision(&current_snapshot, expected_revision)?;
        validate_slot(slot, &channel)?;
        let occupied_id = sqlx::query_scalar::<_, Uuid>(
            "SELECT id FROM posts WHERE channel_id = $1 AND state = 'queued' AND cadence_slot_at = $2 AND id <> $3 FOR UPDATE",
        )
        .bind(channel.id)
        .bind(slot)
        .bind(id)
        .fetch_optional(&mut *transaction)
        .await?;
        if current_snapshot.cadence_slot_at == Some(slot) {
            transaction.commit().await?;
            return current_snapshot.into_post();
        }
        if let Some(occupied_id) = occupied_id {
            let mut rows = lock_posts(&mut transaction, &[id, occupied_id]).await?;
            let current = rows.remove(&id).ok_or(PublisherRepositoryError::PostMissing(id))?;
            let occupied = rows
                .remove(&occupied_id)
                .ok_or(PublisherRepositoryError::PostMissing(occupied_id))?;
            let current_slot = current
                .cadence_slot_at
                .ok_or(PublisherRepositoryError::PostNotScheduled(current.id))?;
            occupied
                .cadence_slot_at
                .ok_or(PublisherRepositoryError::PostNotScheduled(occupied.id))?;
            let current_row = update_post_slot(
                &mut transaction,
                &current,
                slot,
                next_revision(current.revision)?,
            )
            .await?;
            let occupied_row = update_post_slot(
                &mut transaction,
                &occupied,
                current_slot,
                next_revision(occupied.revision)?,
            )
            .await?;
            update_publish_job(&mut transaction, id, slot, current_row.revision).await?;
            update_publish_job(&mut transaction, occupied_id, current_slot, occupied_row.revision)
                .await?;
            transaction.commit().await?;
            return current_row.into_post();
        }
        let row = update_post_slot(
            &mut transaction,
            &current_snapshot,
            slot,
            next_revision(current_snapshot.revision)?,
        )
        .await?;
        update_publish_job(&mut transaction, id, slot, row.revision).await?;
        transaction.commit().await?;
        row.into_post()
    }

    pub async fn cancel_post(
        &self,
        id: Uuid,
        expected_revision: Option<i64>,
    ) -> Result<Post, PublisherRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let channel_id = post_channel_id(&mut transaction, id).await?;
        let _channel = lock_channel(&mut transaction, channel_id).await?;
        let current = lock_post(&mut transaction, id).await?;
        let state = current.post_state()?;
        if !state.is_queue_mutable() {
            return Err(PublisherRepositoryError::PostCannotBeScheduled { id, state });
        }
        check_expected_revision(&current, expected_revision)?;
        if state != PostState::Draft {
            cancel_publish_job(&mut transaction, id).await?;
        }
        let row = sqlx::query_as::<_, PostRow>(
            "UPDATE posts SET state = 'cancelled', cadence_slot_at = NULL, revision = $2, updated_at = now() WHERE id = $1 AND state IN ('draft', 'queued', 'failed') RETURNING *",
        )
        .bind(id)
        .bind(next_revision(current.revision)?)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PublisherRepositoryError::PostCannotBeScheduled { id, state })?;
        transaction.commit().await?;
        row.into_post()
    }

    pub async fn claim_publish(
        &self,
        id: Uuid,
        expected_revision: i64,
    ) -> Result<PublishClaim, PublisherRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let channel_id = post_channel_id(&mut transaction, id).await?;
        let channel = lock_channel(&mut transaction, channel_id).await?;
        let current = lock_post(&mut transaction, id).await?;
        let state = current.post_state()?;
        if state != PostState::Queued {
            return Err(PublisherRepositoryError::PostNotClaimable { id, state });
        }
        if current.revision != expected_revision {
            return Err(PublisherRepositoryError::StalePublicationJob { id });
        }
        ensure_channel_enabled(&channel)?;
        ensure_media_ready(&mut transaction, current.media_id).await?;
        let token = Uuid::now_v7();
        let revision = next_revision(current.revision)?;
        let row = sqlx::query_as::<_, PostRow>(
            "UPDATE posts SET state = 'sending', send_generation = send_generation + 1, send_token = $2, send_started_at = now(), revision = $3, updated_at = now() WHERE id = $1 AND state = 'queued' AND revision = $4 RETURNING *",
        )
        .bind(id)
        .bind(token)
        .bind(revision)
        .bind(expected_revision)
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
            let chat_id = channel_chat_id_in_transaction(&mut transaction, channel_id).await?;
            transaction.commit().await?;
            return Ok(PublishedMessage { post: current.into_post()?, channel_chat_id: chat_id });
        }
        if current.post_state()? != PostState::Sending
            || current.send_generation != generation
            || current.send_token != Some(token)
        {
            return Err(PublisherRepositoryError::PublishLeaseLost(id));
        }
        let row = sqlx::query_as::<_, PostRow>(
            "UPDATE posts SET state = 'published', telegram_message_id = $2, published_at = COALESCE(published_at, now()), send_token = NULL, send_started_at = NULL, error_class = NULL, error_message = NULL, revision = revision + 1, updated_at = now() WHERE id = $1 AND state = 'sending' AND send_generation = $3 AND send_token = $4 RETURNING *",
        )
        .bind(id)
        .bind(telegram_message_id)
        .bind(generation)
        .bind(token)
        .fetch_one(&mut *transaction)
        .await?;
        let channel_id = row.channel_id;
        let chat_id = channel_chat_id_in_transaction(&mut transaction, channel_id).await?;
        transaction.commit().await?;
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
            "UPDATE posts SET state = $2, send_token = NULL, send_started_at = NULL, error_class = $3, error_message = $4, revision = revision + 1, updated_at = now() WHERE id = $1 AND state = 'sending' AND send_generation = $5 AND send_token = $6 RETURNING *",
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
}

async fn post_channel_id(
    transaction: &mut Transaction<'_, Postgres>,
    id: Uuid,
) -> Result<Uuid, PublisherRepositoryError> {
    sqlx::query_scalar::<_, Uuid>("SELECT channel_id FROM posts WHERE id = $1")
        .bind(id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(PublisherRepositoryError::PostMissing(id))
}

async fn lock_channel(
    transaction: &mut Transaction<'_, Postgres>,
    id: Uuid,
) -> Result<ChannelRow, PublisherRepositoryError> {
    sqlx::query_as::<_, ChannelRow>("SELECT * FROM channels WHERE id = $1 FOR UPDATE")
        .bind(id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(PublisherRepositoryError::ChannelMissing(id))
}

fn ensure_channel_enabled(channel: &ChannelRow) -> Result<(), PublisherRepositoryError> {
    if !channel.is_enabled {
        return Err(PublisherRepositoryError::ChannelDisabled(channel.id));
    }
    Ok(())
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

async fn lock_posts(
    transaction: &mut Transaction<'_, Postgres>,
    ids: &[Uuid],
) -> Result<HashMap<Uuid, PostRow>, PublisherRepositoryError> {
    let mut ids = ids.to_vec();
    ids.sort_unstable();
    ids.dedup();
    let rows = sqlx::query_as::<_, PostRow>(
        "SELECT * FROM posts WHERE id = ANY($1::uuid[]) ORDER BY id FOR UPDATE",
    )
    .bind(&ids)
    .fetch_all(&mut **transaction)
    .await?;
    let rows = rows.into_iter().map(|row| (row.id, row)).collect::<HashMap<_, _>>();
    if rows.len() != ids.len() {
        let missing = ids.into_iter().find(|id| !rows.contains_key(id)).unwrap_or_default();
        return Err(PublisherRepositoryError::PostMissing(missing));
    }
    Ok(rows)
}

async fn adjacent_post_id(
    transaction: &mut Transaction<'_, Postgres>,
    current: &PostRow,
    direction: QueueDirection,
) -> Result<Option<Uuid>, PublisherRepositoryError> {
    let (comparison, ordering) = match direction {
        QueueDirection::Earlier => ("<", "DESC"),
        QueueDirection::Later => (">", "ASC"),
    };
    let query = format!(
        "SELECT id FROM posts WHERE channel_id = $1 AND state = 'queued' AND cadence_slot_at IS NOT NULL AND cadence_slot_at {comparison} $2 AND id <> $3 ORDER BY cadence_slot_at {ordering}, id {ordering} LIMIT 1"
    );
    Ok(sqlx::query_scalar::<_, Uuid>(&query)
        .bind(current.channel_id)
        .bind(current.cadence_slot_at)
        .bind(current.id)
        .fetch_optional(&mut **transaction)
        .await?)
}

async fn update_post_slot(
    transaction: &mut Transaction<'_, Postgres>,
    current: &PostRow,
    slot: OffsetDateTime,
    revision: i64,
) -> Result<PostRow, PublisherRepositoryError> {
    sqlx::query_as::<_, PostRow>(
        "UPDATE posts SET scheduled_at = $2, cadence_slot_at = $2, revision = $3, updated_at = now() WHERE id = $1 AND state = 'queued' RETURNING *",
    )
    .bind(current.id)
    .bind(slot)
    .bind(revision)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(PublisherRepositoryError::PostNotEditable {
        id: current.id,
        state: current.post_state()?,
    })
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

async fn insert_publish_job(
    transaction: &mut Transaction<'_, Postgres>,
    post_id: Uuid,
    run_at: OffsetDateTime,
    expected_revision: i64,
) -> Result<(), PublisherRepositoryError> {
    if let Some(row) = lock_publish_job(transaction, post_id).await? {
        if row.state == "running" {
            return Err(PublisherRepositoryError::PublishJobRunning(post_id));
        }
        return update_publish_job(transaction, post_id, run_at, expected_revision).await;
    }
    let inserted = sqlx::query(
        "INSERT INTO queue.jobs (kind, payload, state, run_at, dedupe_key) VALUES ('publish_post', $1, 'queued', $2, $3) ON CONFLICT (dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING",
    )
    .bind(serde_json::json!({ "post_id": post_id, "expected_revision": expected_revision }))
    .bind(run_at)
    .bind(format!("post:{post_id}:publish:v1"))
    .execute(&mut **transaction)
    .await?;
    if inserted.rows_affected() != 1 {
        return Err(PublisherRepositoryError::PublishJobUpdateLost(post_id));
    }
    Ok(())
}

async fn update_publish_job(
    transaction: &mut Transaction<'_, Postgres>,
    post_id: Uuid,
    run_at: OffsetDateTime,
    expected_revision: i64,
) -> Result<(), PublisherRepositoryError> {
    let row = lock_publish_job(transaction, post_id).await?;
    let Some(row) = row else {
        return Err(PublisherRepositoryError::PublishJobMissing(post_id));
    };
    if row.state == "running" {
        return Err(PublisherRepositoryError::PublishJobRunning(post_id));
    }
    if !matches!(row.state.as_str(), "queued" | "failed") {
        return Err(PublisherRepositoryError::PublishJobUnavailable { post_id, state: row.state });
    }
    let updated = sqlx::query(
        "UPDATE queue.jobs SET payload = $2, run_at = $3, state = 'queued', attempt_count = CASE WHEN state = 'failed' THEN 0 ELSE attempt_count END, error_class = NULL, error_message = NULL, completed_at = NULL, updated_at = now() WHERE id = $1 AND state IN ('queued', 'failed')",
    )
    .bind(row.id)
    .bind(serde_json::json!({ "post_id": post_id, "expected_revision": expected_revision }))
    .bind(run_at)
    .execute(&mut **transaction)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(PublisherRepositoryError::PublishJobUpdateLost(post_id));
    }
    Ok(())
}

async fn cancel_publish_job(
    transaction: &mut Transaction<'_, Postgres>,
    post_id: Uuid,
) -> Result<(), PublisherRepositoryError> {
    let row = lock_publish_job(transaction, post_id)
        .await?
        .ok_or(PublisherRepositoryError::PublishJobMissing(post_id))?;
    if row.state == "running" {
        return Err(PublisherRepositoryError::PublishJobRunning(post_id));
    }
    if !matches!(row.state.as_str(), "queued" | "failed") {
        return Err(PublisherRepositoryError::PublishJobUnavailable { post_id, state: row.state });
    }
    let updated = sqlx::query(
        "UPDATE queue.jobs SET state = 'cancelled', lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL, last_heartbeat_at = NULL, completed_at = now(), updated_at = now() WHERE id = $1 AND state IN ('queued', 'failed')",
    )
    .bind(row.id)
    .execute(&mut **transaction)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(PublisherRepositoryError::PublishJobUpdateLost(post_id));
    }
    Ok(())
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct PublishJobRow {
    id: Uuid,
    state: String,
}

async fn lock_publish_job(
    transaction: &mut Transaction<'_, Postgres>,
    post_id: Uuid,
) -> Result<Option<PublishJobRow>, PublisherRepositoryError> {
    Ok(sqlx::query_as::<_, PublishJobRow>(
        "SELECT id, state FROM queue.jobs WHERE dedupe_key = $1 FOR UPDATE",
    )
    .bind(format!("post:{post_id}:publish:v1"))
    .fetch_optional(&mut **transaction)
    .await?)
}

fn schedule_request_hash(schedule: &PostSchedule) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(schedule.requested_at.unix_timestamp_nanos().to_be_bytes());
    hasher.finalize().to_vec()
}

fn publish_now_request_hash(request_key: &str) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(b"publish_now:v1:");
    hasher.update(request_key.as_bytes());
    hasher.finalize().to_vec()
}

fn check_expected_revision(
    current: &PostRow,
    expected_revision: Option<i64>,
) -> Result<(), PublisherRepositoryError> {
    if expected_revision.is_some_and(|expected| expected != current.revision) {
        return Err(PublisherRepositoryError::OptimisticConflict(current.id));
    }
    Ok(())
}

fn next_revision(current: i64) -> Result<i64, PublisherRepositoryError> {
    current.checked_add(1).ok_or(PublisherRepositoryError::RevisionOverflow)
}

fn validate_slot(
    slot: OffsetDateTime,
    channel: &ChannelRow,
) -> Result<(), PublisherRepositoryError> {
    if slot < OffsetDateTime::now_utc() {
        return Err(PublisherRepositoryError::CadenceSlotInPast);
    }
    let timezone: Tz = channel
        .time_zone
        .parse()
        .map_err(|_| PublisherRepositoryError::InvalidTimeZone(channel.time_zone.clone()))?;
    let local = to_chrono(slot)?.with_timezone(&timezone);
    let start = to_chrono_time(channel.window_start);
    let end = to_chrono_time(channel.window_end);
    if local.time() < start || local.time() >= end {
        return Err(PublisherRepositoryError::InvalidCadenceSlot);
    }
    let elapsed = local.time().signed_duration_since(start).num_seconds();
    if elapsed < 0 || elapsed % (i64::from(channel.interval_minutes) * 60) != 0 {
        return Err(PublisherRepositoryError::InvalidCadenceSlot);
    }
    Ok(())
}

async fn next_free_slot(
    transaction: &mut Transaction<'_, Postgres>,
    channel: &ChannelRow,
    post_id: Uuid,
    requested_at: OffsetDateTime,
) -> Result<OffsetDateTime, PublisherRepositoryError> {
    let occupied = sqlx::query_scalar::<_, OffsetDateTime>(
        "SELECT cadence_slot_at FROM posts WHERE channel_id = $1 AND id <> $2 AND state IN ('queued', 'sending') AND cadence_slot_at IS NOT NULL",
    )
    .bind(channel.id)
    .bind(post_id)
    .fetch_all(&mut **transaction)
    .await?;
    let occupied = occupied.into_iter().collect::<std::collections::HashSet<_>>();
    let mut candidate = next_allowed_slot(requested_at.max(OffsetDateTime::now_utc()), channel)?;
    for _ in 0..10_000 {
        if !occupied.contains(&candidate) {
            return Ok(candidate);
        }
        candidate = next_allowed_slot(candidate + time::Duration::seconds(1), channel)?;
    }
    Err(PublisherRepositoryError::CadenceSearchExhausted)
}

async fn channel_chat_id_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    id: Uuid,
) -> Result<i64, PublisherRepositoryError> {
    sqlx::query_scalar::<_, i64>("SELECT telegram_chat_id FROM channels WHERE id = $1")
        .bind(id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(PublisherRepositoryError::ChannelMissing(id))
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
            revision: self.revision,
        })
    }
}

fn validate_post(post: &NewPost) -> Result<(), PublisherRepositoryError> {
    if let Some(caption) = &post.caption {
        validate_caption(caption).map_err(PublisherRepositoryError::Validation)?;
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
    if let Some(caption) = update.caption.as_ref().and_then(Option::as_ref) {
        validate_caption(caption).map_err(PublisherRepositoryError::Validation)?;
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
    #[error("post {0} has no cadence slot")]
    PostNotScheduled(Uuid),
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
    #[error("post {id} has a stale publication job revision")]
    StalePublicationJob { id: Uuid },
    #[error("post {0} publication job is missing")]
    PublishJobMissing(Uuid),
    #[error("post {0} publication job is already running")]
    PublishJobRunning(Uuid),
    #[error("post {post_id} publication job is unavailable in state {state}")]
    PublishJobUnavailable { post_id: Uuid, state: String },
    #[error("post {0} publication job update affected no rows")]
    PublishJobUpdateLost(Uuid),
    #[error("post revision overflowed")]
    RevisionOverflow,
    #[error("invalid publication failure state {0:?}")]
    InvalidPublishFailureState(PostState),
    #[error("Telegram message ID must be positive, got {0}")]
    InvalidTelegramMessageId(i64),
    #[error("invalid channel time zone: {0}")]
    InvalidTimeZone(String),
    #[error("could not find an allowed cadence slot")]
    CadenceSearchExhausted,
    #[error("cadence slot is in the past")]
    CadenceSlotInPast,
    #[error("cadence slot is outside the channel window or interval")]
    InvalidCadenceSlot,
    #[error("invalid publication timestamp")]
    InvalidTimestamp,
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}
