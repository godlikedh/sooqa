use chrono::{DateTime, Duration as ChronoDuration, NaiveDateTime, NaiveTime, TimeZone, Utc};
use chrono_tz::Tz;
use sha2::{Digest, Sha256};
use sooqa_jobs::JobLease;
use sooqa_publisher::{
    Channel, ChannelUpdate, ChannelValidationError, MAX_REPEAT_EVIDENCE_BYTES,
    MAX_REPEAT_EVIDENCE_CONFLICTS, NewChannel, NewPost, Post, PostCursor, PostExactSchedule,
    PostListItem, PostPage, PostPreview, PostSchedule, PostState, PostUpdate, PublicationAction,
    PublicationDecision, PublicationIntent, PublishClaim, PublishRetry, PublishedMessage,
    PublisherValidationError, RepeatConflict, RepeatEvidence, validate_caption,
};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use thiserror::Error;
use time::{OffsetDateTime, Time};
use uuid::Uuid;

#[derive(Clone)]
pub struct PublisherRepository {
    pool: PgPool,
}

const CHANNEL_CONFIGURATION_ADVISORY_LOCK: i64 = 0x736f_6f71_615f_6368;

#[derive(Debug, Clone)]
pub struct CreatePostResult {
    pub post: Post,
    pub created: bool,
}

#[derive(Debug, Clone)]
pub struct MaterializationResult {
    pub post: Option<Post>,
    pub created: bool,
}

#[derive(Debug, Clone)]
pub struct PublishLease {
    pub generation: i32,
    pub token: Uuid,
    pub attempt: JobLease,
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
struct PostCursorRow {
    id: Uuid,
    scheduled_at: OffsetDateTime,
}

#[derive(Debug, Clone, FromRow)]
struct PostListMetadataRow {
    channel_name: String,
    media_kind: String,
    source_url: Option<String>,
    telegram_storage_chat_id: Option<i64>,
    telegram_storage_message_id: Option<i64>,
    preview_mime_type: Option<String>,
    preview_width: Option<i32>,
    preview_height: Option<i32>,
    preview_size_bytes: Option<i32>,
    preview_sha256: Option<Vec<u8>>,
}

#[derive(Debug, Clone, FromRow)]
struct PostRow {
    id: Uuid,
    request_hash: Option<Vec<u8>>,
    origin_ingest_id: Option<Uuid>,
    requested_action: String,
    requested_publish_at: Option<OffsetDateTime>,
    repeat_evidence: Option<serde_json::Value>,
    decision_request_key: Option<String>,
    decision_request_hash: Option<Vec<u8>>,
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

#[derive(Debug, Clone, FromRow)]
struct MaterializationIngestRow {
    id: Uuid,
    state: String,
    media_id: Option<Uuid>,
    requested_action: String,
    requested_publish_at: Option<OffsetDateTime>,
    requested_post_caption: Option<String>,
    requested_channel_id: Option<Uuid>,
}

#[derive(Debug, Clone, FromRow)]
struct RepeatConflictRow {
    id: Uuid,
    state: String,
    scheduled_at: Option<OffsetDateTime>,
    published_at: Option<OffsetDateTime>,
    telegram_message_id: Option<i64>,
    telegram_chat_id: i64,
}

struct IntendedPostInput<'a> {
    request_key: &'a str,
    request_hash: &'a [u8],
    origin_ingest_id: Option<Uuid>,
    media_id: Uuid,
    channel: &'a ChannelRow,
    intent: &'a PublicationIntent,
    post_id: Uuid,
    now: OffsetDateTime,
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

    /// Create a settings-managed channel only when it leaves one enabled
    /// publication target. The lower-level create path remains available for
    /// diagnostics and fixtures that intentionally model ambiguity.
    pub async fn create_channel_unambiguous(
        &self,
        channel: NewChannel,
    ) -> Result<Channel, PublisherRepositoryError> {
        channel.validate()?;
        let mut transaction = self.pool.begin().await?;
        lock_channel_configuration(&mut transaction).await?;
        let other_enabled =
            sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM channels WHERE is_enabled)")
                .fetch_one(&mut *transaction)
                .await?;
        if other_enabled {
            return Err(PublisherRepositoryError::ChannelEnablementAmbiguous);
        }
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
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
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

    pub async fn update_channel(
        &self,
        id: Uuid,
        update: ChannelUpdate,
    ) -> Result<Channel, PublisherRepositoryError> {
        if update.name.is_none()
            && update.telegram_chat_id.is_none()
            && update.is_enabled.is_none()
            && update.time_zone.is_none()
            && update.window_start.is_none()
            && update.window_end.is_none()
            && update.interval_minutes.is_none()
            && update.default_parse_mode.is_none()
            && update.default_disable_notification.is_none()
        {
            return Err(PublisherRepositoryError::EmptyChannelUpdate);
        }
        let mut transaction = self.pool.begin().await?;
        lock_channel_configuration(&mut transaction).await?;
        let current =
            sqlx::query_as::<_, ChannelRow>("SELECT * FROM channels WHERE id = $1 FOR UPDATE")
                .bind(id)
                .fetch_optional(&mut *transaction)
                .await?
                .ok_or(PublisherRepositoryError::ChannelMissing(id))?;
        if update.expected_updated_at.is_some_and(|expected| expected != current.updated_at) {
            return Err(PublisherRepositoryError::ChannelOptimisticConflict(id));
        }
        let candidate = NewChannel {
            name: update.name.map(|name| name.trim().to_owned()).unwrap_or(current.name),
            telegram_chat_id: update.telegram_chat_id.unwrap_or(current.telegram_chat_id),
            time_zone: update
                .time_zone
                .map(|time_zone| time_zone.trim().to_owned())
                .unwrap_or(current.time_zone),
            window_start: update.window_start.unwrap_or(current.window_start),
            window_end: update.window_end.unwrap_or(current.window_end),
            interval_minutes: update.interval_minutes.unwrap_or(current.interval_minutes),
            default_parse_mode: update.default_parse_mode.unwrap_or(current.default_parse_mode),
            default_disable_notification: update
                .default_disable_notification
                .unwrap_or(current.default_disable_notification),
        };
        candidate.validate()?;
        let is_enabled = update.is_enabled.unwrap_or(current.is_enabled);
        if is_enabled {
            let other_enabled = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM channels WHERE is_enabled AND id <> $1)",
            )
            .bind(id)
            .fetch_one(&mut *transaction)
            .await?;
            if other_enabled {
                return Err(PublisherRepositoryError::ChannelEnablementAmbiguous);
            }
        }
        let row = sqlx::query_as::<_, ChannelRow>(
            "UPDATE channels SET name = $2, telegram_chat_id = $3, is_enabled = $4, time_zone = $5, window_start = $6, window_end = $7, interval_minutes = $8, default_parse_mode = $9, default_disable_notification = $10, updated_at = now() WHERE id = $1 RETURNING *",
        )
        .bind(id)
        .bind(candidate.name)
        .bind(candidate.telegram_chat_id)
        .bind(is_enabled)
        .bind(candidate.time_zone)
        .bind(candidate.window_start)
        .bind(candidate.window_end)
        .bind(candidate.interval_minutes)
        .bind(candidate.default_parse_mode)
        .bind(candidate.default_disable_notification)
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
        row.into_channel()
    }

    pub async fn count_future_queued_posts(&self) -> Result<u64, PublisherRepositoryError> {
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM posts WHERE state = 'queued' AND scheduled_at > now()",
        )
        .fetch_one(&self.pool)
        .await?;
        u64::try_from(count).map_err(|_| PublisherRepositoryError::InvalidCount)
    }

    pub async fn count_repeat_decisions(&self) -> Result<u64, PublisherRepositoryError> {
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM posts WHERE state = 'draft' AND repeat_evidence IS NOT NULL",
        )
        .fetch_one(&self.pool)
        .await?;
        u64::try_from(count).map_err(|_| PublisherRepositoryError::InvalidCount)
    }

    pub async fn list_repeat_decisions(
        &self,
        limit: u32,
    ) -> Result<Vec<Post>, PublisherRepositoryError> {
        if !(1..=50).contains(&limit) {
            return Err(PublisherRepositoryError::InvalidLimit { value: limit });
        }
        let rows = sqlx::query_as::<_, PostRow>(
            "SELECT * FROM posts WHERE state = 'draft' AND repeat_evidence IS NOT NULL ORDER BY updated_at DESC, id DESC LIMIT $1",
        )
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(PostRow::into_post).collect()
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

    /// Create the one intended post for a direct media action. The channel is
    /// selected inside the same transaction as the post so a request cannot
    /// observe one enabled channel and persist another target.
    pub async fn create_publication_intent(
        &self,
        media_id: Uuid,
        intent: PublicationIntent,
        request_key: String,
    ) -> Result<CreatePostResult, PublisherRepositoryError> {
        let request_key = sooqa_publisher::normalize_request_key(request_key)?;
        let request_hash = publication_intent_hash(media_id, &intent)?;
        let mut transaction = self.pool.begin().await?;
        lock_publication_request(&mut transaction, &request_key).await?;
        if let Some(existing) =
            sqlx::query_as::<_, PostRow>("SELECT * FROM posts WHERE request_key = $1 FOR UPDATE")
                .bind(&request_key)
                .fetch_optional(&mut *transaction)
                .await?
        {
            if existing.request_hash.as_deref() != Some(request_hash.as_slice()) {
                return Err(PublisherRepositoryError::RequestKeyConflict(request_key));
            }
            transaction.commit().await?;
            return Ok(CreatePostResult { post: existing.into_post()?, created: false });
        }

        let media_state = lock_media_state(&mut transaction, media_id).await?;
        if media_state != "ready" {
            return Err(PublisherRepositoryError::MediaNotReady { media_id, state: media_state });
        }
        let channel_id = single_enabled_channel(&mut transaction).await?;
        let channel = lock_channel(&mut transaction, channel_id).await?;
        let post = insert_intended_post(
            &mut transaction,
            IntendedPostInput {
                request_key: &request_key,
                request_hash: &request_hash,
                origin_ingest_id: None,
                media_id,
                channel: &channel,
                intent: &intent,
                post_id: Uuid::now_v7(),
                now: OffsetDateTime::now_utc(),
            },
        )
        .await?;
        transaction.commit().await?;
        Ok(CreatePostResult { post: post.into_post()?, created: true })
    }

    /// Materialize an ingest's captured publication intent. This operation is
    /// deliberately database-only and is safe to replay: both the origin
    /// ingest and the stable request key fence duplicate jobs.
    pub async fn materialize_ingest(
        &self,
        ingest_id: Uuid,
    ) -> Result<MaterializationResult, PublisherRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let ingest = sqlx::query_as::<_, MaterializationIngestRow>(
            "SELECT id, state, media_id, requested_action, requested_publish_at, requested_post_caption, requested_channel_id FROM ingests WHERE id = $1 FOR UPDATE",
        )
        .bind(ingest_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PublisherRepositoryError::IngestMissing(ingest_id))?;

        if ingest.requested_action == "save" {
            transaction.commit().await?;
            return Ok(MaterializationResult { post: None, created: false });
        }
        if ingest.state != "completed" {
            return Err(PublisherRepositoryError::MaterializationNotReady { ingest_id });
        }
        if let Some(existing) = sqlx::query_as::<_, PostRow>(
            "SELECT * FROM posts WHERE origin_ingest_id = $1 FOR UPDATE",
        )
        .bind(ingest.id)
        .fetch_optional(&mut *transaction)
        .await?
        {
            transaction.commit().await?;
            return Ok(MaterializationResult { post: Some(existing.into_post()?), created: false });
        }

        let media_id =
            ingest.media_id.ok_or(PublisherRepositoryError::IngestMediaMissing(ingest_id))?;
        let media_state = lock_media_state(&mut transaction, media_id).await?;
        if media_state != "ready" {
            return Err(PublisherRepositoryError::MediaNotReady { media_id, state: media_state });
        }
        let channel_id = ingest
            .requested_channel_id
            .ok_or(PublisherRepositoryError::PublicationChannelNotConfigured)?;
        let channel = lock_channel(&mut transaction, channel_id).await?;
        ensure_channel_enabled(&channel)?;
        let action =
            PublicationAction::try_from(ingest.requested_action.as_str()).map_err(|_| {
                PublisherRepositoryError::InvalidPublicationAction(ingest.requested_action.clone())
            })?;
        let intent = PublicationIntent::try_new(
            action,
            ingest.requested_publish_at,
            ingest.requested_post_caption.clone(),
        )?;
        let request_key = format!("ingest:{}:publication:v1", ingest.id);
        let request_hash = publication_intent_hash(media_id, &intent)?;
        let post = insert_intended_post(
            &mut transaction,
            IntendedPostInput {
                request_key: &request_key,
                request_hash: &request_hash,
                origin_ingest_id: Some(ingest.id),
                media_id,
                channel: &channel,
                intent: &intent,
                post_id: Uuid::now_v7(),
                now: OffsetDateTime::now_utc(),
            },
        )
        .await?;
        transaction.commit().await?;
        Ok(MaterializationResult { post: Some(post.into_post()?), created: true })
    }

    pub async fn find_post(&self, id: Uuid) -> Result<Option<Post>, PublisherRepositoryError> {
        sqlx::query_as::<_, PostRow>("SELECT * FROM posts WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(PostRow::into_post)
            .transpose()
    }

    /// List bounded schedule cards. The default hides published/cancelled
    /// history; the cursor follows the same ordered `(scheduled_at, id)` pair
    /// used by the UI and never loads the posts table wholesale.
    pub async fn list_posts(
        &self,
        limit: u32,
        cursor: Option<PostCursor>,
        include_history: bool,
    ) -> Result<PostPage, PublisherRepositoryError> {
        if !(1..=50).contains(&limit) {
            return Err(PublisherRepositoryError::InvalidLimit { value: limit });
        }
        let rows = sqlx::query_as::<_, PostCursorRow>(
            r#"
            SELECT id, scheduled_at
            FROM posts
            WHERE ($1 OR state NOT IN ('published', 'cancelled'))
              AND ($2::timestamptz IS NULL OR (scheduled_at, id) > ($2, $3))
            ORDER BY scheduled_at ASC, id ASC
            LIMIT $4
            "#,
        )
        .bind(include_history)
        .bind(cursor.as_ref().map(|value| value.scheduled_at))
        .bind(cursor.as_ref().map(|value| value.id))
        .bind(i64::from(limit) + 1)
        .fetch_all(&self.pool)
        .await?;
        let has_more = rows.len() > limit as usize;
        let rows = rows.into_iter().take(limit as usize).collect::<Vec<_>>();
        let next_cursor = has_more
            .then(|| rows.last())
            .flatten()
            .map(|row| PostCursor { scheduled_at: row.scheduled_at, id: row.id });
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            if let Some(item) = self.load_post_list_item(row.id).await? {
                items.push(item);
            }
        }
        Ok(PostPage { items, next_cursor })
    }

    async fn load_post_list_item(
        &self,
        id: Uuid,
    ) -> Result<Option<PostListItem>, PublisherRepositoryError> {
        let Some(post) = self.find_post(id).await? else { return Ok(None) };
        let row = sqlx::query_as::<_, PostListMetadataRow>(
            r#"
            SELECT c.name AS channel_name, m.kind AS media_kind, m.source_url,
                   m.telegram_storage_chat_id, m.telegram_storage_message_id,
                   m.preview_mime_type, m.preview_width, m.preview_height,
                   octet_length(m.preview_bytes) AS preview_size_bytes,
                   m.preview_sha256
            FROM posts AS p
            JOIN channels AS c ON c.id = p.channel_id
            JOIN media AS m ON m.id = p.media_id
            WHERE p.id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else { return Ok(None) };
        let preview = match (
            row.preview_mime_type,
            row.preview_width,
            row.preview_height,
            row.preview_size_bytes,
            row.preview_sha256,
        ) {
            (None, None, None, None, None) => None,
            (Some(mime_type), Some(width), Some(height), Some(size_bytes), Some(sha256))
                if matches!(mime_type.as_str(), "image/jpeg" | "image/png")
                    && (1..=320).contains(&width)
                    && (1..=320).contains(&height)
                    && (1..=131_072).contains(&size_bytes)
                    && sha256.len() == 32 =>
            {
                Some(PostPreview {
                    mime_type,
                    width: u32::try_from(width).expect("preview width is positive and bounded"),
                    height: u32::try_from(height).expect("preview height is positive and bounded"),
                    size_bytes: u32::try_from(size_bytes)
                        .expect("preview size is positive and bounded"),
                    sha256,
                })
            }
            _ => return Err(PublisherRepositoryError::InvalidPreviewMetadata(id)),
        };
        Ok(Some(PostListItem {
            post,
            channel_name: row.channel_name,
            media_kind: row.media_kind,
            source_url: row.source_url,
            storage_url: telegram_message_link_optional(
                row.telegram_storage_chat_id,
                row.telegram_storage_message_id,
            ),
            preview,
        }))
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
        match state {
            PostState::Draft => {}
            PostState::Queued => {
                update_publish_job(&mut transaction, id, row.scheduled_at, row.revision).await?
            }
            PostState::Failed => {
                update_failed_publish_job(&mut transaction, id, row.revision).await?
            }
            _ => unreachable!("queue mutable states are limited to draft, queued, and failed"),
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
        ensure_repeat_decision_resolved(&current)?;
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

    /// Schedule one post at an explicit future instant. This path intentionally
    /// does not inspect unrelated cadence posts or the channel window, and it
    /// allows multiple posts to share the same instant.
    pub async fn schedule_post_exact(
        &self,
        schedule: PostExactSchedule,
    ) -> Result<Post, PublisherRepositoryError> {
        let operation_key = format!("exact:{}", schedule.request_key);
        let request_hash = exact_schedule_request_hash(schedule.requested_at);
        let mut transaction = self.pool.begin().await?;
        let channel_id = post_channel_id(&mut transaction, schedule.post_id).await?;
        let channel = lock_channel(&mut transaction, channel_id).await?;
        let current = lock_post(&mut transaction, schedule.post_id).await?;
        if current.schedule_request_key.as_deref() == Some(operation_key.as_str()) {
            if current.schedule_request_hash.as_deref() != Some(request_hash.as_slice()) {
                return Err(PublisherRepositoryError::RequestKeyConflict(operation_key));
            }
            transaction.commit().await?;
            return current.into_post();
        }
        if schedule.requested_at <= OffsetDateTime::now_utc() {
            return Err(PublisherRepositoryError::ExactScheduleInPast);
        }
        let state = current.post_state()?;
        if !state.is_queue_mutable() {
            return Err(PublisherRepositoryError::PostCannotBeScheduled { id: current.id, state });
        }
        check_expected_revision(&current, schedule.expected_revision)?;
        ensure_repeat_decision_resolved(&current)?;
        ensure_channel_enabled(&channel)?;
        ensure_media_ready(&mut transaction, current.media_id).await?;
        let row = apply_exact_schedule(
            &mut transaction,
            &current,
            schedule.requested_at,
            &operation_key,
            &request_hash,
            next_revision(current.revision)?,
        )
        .await?;
        if state == PostState::Draft {
            insert_publish_job(&mut transaction, row.id, schedule.requested_at, row.revision)
                .await?;
        } else {
            update_publish_job(&mut transaction, row.id, schedule.requested_at, row.revision)
                .await?;
        }
        transaction.commit().await?;
        row.into_post()
    }

    /// Consume one persisted repeat-review decision. The decision request key
    /// is stored on the post so a replay can return the same row even after its
    /// revision has advanced, while a different stale command is rejected.
    pub async fn decide_post(
        &self,
        id: Uuid,
        decision: PublicationDecision,
        request_key: String,
        expected_revision: i64,
    ) -> Result<Post, PublisherRepositoryError> {
        let request_key = sooqa_publisher::normalize_request_key(request_key)?;
        let request_hash = decision_request_hash(decision, expected_revision);
        let mut transaction = self.pool.begin().await?;
        let channel_id = post_channel_id(&mut transaction, id).await?;
        let channel = lock_channel(&mut transaction, channel_id).await?;
        let current = lock_post(&mut transaction, id).await?;
        if current.decision_request_key.as_deref() == Some(request_key.as_str()) {
            if current.decision_request_hash.as_deref() != Some(request_hash.as_slice()) {
                return Err(PublisherRepositoryError::RequestKeyConflict(request_key));
            }
            transaction.commit().await?;
            return current.into_post();
        }
        let state = current.post_state()?;
        if state != PostState::Draft {
            return Err(PublisherRepositoryError::PostDecisionNotAllowed { id, state });
        }
        if current.repeat_evidence.is_none() {
            return Err(PublisherRepositoryError::PostDecisionNotAllowed { id, state });
        }
        check_expected_revision(&current, expected_revision)?;
        let action =
            PublicationAction::try_from(current.requested_action.as_str()).map_err(|_| {
                PublisherRepositoryError::InvalidPublicationAction(current.requested_action.clone())
            })?;
        if !decision_allowed(action, current.requested_publish_at, decision) {
            return Err(PublisherRepositoryError::InvalidPublicationDecision { id, decision });
        }

        let revision = next_revision(current.revision)?;
        let row = match decision {
            PublicationDecision::Cancel => {
                sqlx::query_as::<_, PostRow>(
                    "UPDATE posts SET state = 'cancelled', repeat_evidence = NULL, decision_request_key = $2, decision_request_hash = $3, cadence_slot_at = NULL, revision = $4, updated_at = now() WHERE id = $1 AND state = 'draft' AND revision = $5 RETURNING *",
                )
                .bind(id)
                .bind(&request_key)
                .bind(request_hash.as_slice())
                .bind(revision)
                .bind(expected_revision)
                .fetch_one(&mut *transaction)
                .await?
            }
            PublicationDecision::PostNowAnyway => {
                ensure_channel_enabled(&channel)?;
                ensure_media_ready(&mut transaction, current.media_id).await?;
                let now = OffsetDateTime::now_utc();
                let row = sqlx::query_as::<_, PostRow>(
                    "UPDATE posts SET state = 'queued', scheduled_at = $2, cadence_slot_at = NULL, repeat_evidence = NULL, decision_request_key = $3, decision_request_hash = $4, revision = $5, error_class = NULL, error_message = NULL, updated_at = now() WHERE id = $1 AND state = 'draft' AND revision = $6 RETURNING *",
                )
                .bind(id)
                .bind(now)
                .bind(&request_key)
                .bind(request_hash.as_slice())
                .bind(revision)
                .bind(expected_revision)
                .fetch_one(&mut *transaction)
                .await?;
                insert_publish_job(&mut transaction, id, now, row.revision).await?;
                row
            }
            PublicationDecision::QueueAnyway => {
                ensure_channel_enabled(&channel)?;
                ensure_media_ready(&mut transaction, current.media_id).await?;
                let slot = next_free_slot(&mut transaction, &channel, id, OffsetDateTime::now_utc())
                    .await?;
                let row = sqlx::query_as::<_, PostRow>(
                    "UPDATE posts SET state = 'queued', scheduled_at = $2, cadence_slot_at = $2, repeat_evidence = NULL, decision_request_key = $3, decision_request_hash = $4, revision = $5, error_class = NULL, error_message = NULL, updated_at = now() WHERE id = $1 AND state = 'draft' AND revision = $6 RETURNING *",
                )
                .bind(id)
                .bind(slot)
                .bind(&request_key)
                .bind(request_hash.as_slice())
                .bind(revision)
                .bind(expected_revision)
                .fetch_one(&mut *transaction)
                .await?;
                insert_publish_job(&mut transaction, id, slot, row.revision).await?;
                row
            }
            PublicationDecision::KeepExactTime => {
                ensure_channel_enabled(&channel)?;
                ensure_media_ready(&mut transaction, current.media_id).await?;
                let requested_at = current
                    .requested_publish_at
                    .ok_or(PublisherRepositoryError::ExactTimeMissing(id))?;
                if requested_at <= OffsetDateTime::now_utc() {
                    return Err(PublisherRepositoryError::ExactScheduleInPast);
                }
                let row = sqlx::query_as::<_, PostRow>(
                    "UPDATE posts SET state = 'queued', scheduled_at = $2, cadence_slot_at = NULL, repeat_evidence = NULL, decision_request_key = $3, decision_request_hash = $4, revision = $5, error_class = NULL, error_message = NULL, updated_at = now() WHERE id = $1 AND state = 'draft' AND revision = $6 RETURNING *",
                )
                .bind(id)
                .bind(requested_at)
                .bind(&request_key)
                .bind(request_hash.as_slice())
                .bind(revision)
                .bind(expected_revision)
                .fetch_one(&mut *transaction)
                .await?;
                insert_publish_job(&mut transaction, id, requested_at, row.revision).await?;
                row
            }
        };
        transaction.commit().await?;
        row.into_post()
    }

    pub async fn publish_now(
        &self,
        id: Uuid,
        request_key: String,
        expected_revision: i64,
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
        ensure_repeat_decision_resolved(&current)?;
        ensure_channel_enabled(&channel)?;
        ensure_media_ready(&mut transaction, current.media_id).await?;
        let now = OffsetDateTime::now_utc();
        let revision = next_revision(current.revision)?;
        let row = sqlx::query_as::<_, PostRow>(
            "UPDATE posts SET schedule_request_key = $2, schedule_request_hash = $3, state = 'queued', scheduled_at = $4, cadence_slot_at = NULL, revision = $5, error_class = NULL, error_message = NULL, updated_at = now() WHERE id = $1 AND state IN ('draft', 'queued', 'failed') RETURNING *",
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

    pub async fn cancel_post(
        &self,
        id: Uuid,
        expected_revision: i64,
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
        attempt: &JobLease,
    ) -> Result<PublishClaim, PublisherRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let channel_id = post_channel_id(&mut transaction, id).await?;
        let channel = lock_channel(&mut transaction, channel_id).await?;
        let current = lock_post(&mut transaction, id).await?;
        lock_current_job_attempt(&mut transaction, id, attempt).await?;
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
        lease: &PublishLease,
        telegram_message_id: i64,
    ) -> Result<PublishedMessage, PublisherRepositoryError> {
        if telegram_message_id <= 0 {
            return Err(PublisherRepositoryError::InvalidTelegramMessageId(telegram_message_id));
        }
        let mut transaction = self.pool.begin().await?;
        let channel_id = post_channel_id(&mut transaction, id).await?;
        let channel = lock_channel(&mut transaction, channel_id).await?;
        let current = lock_post(&mut transaction, id).await?;
        lock_current_job_attempt(&mut transaction, id, &lease.attempt).await?;
        if current.post_state()? == PostState::Published {
            if current.send_generation != lease.generation
                || current.telegram_message_id != Some(telegram_message_id)
            {
                return Err(PublisherRepositoryError::PublishConflict(id));
            }
            transaction.commit().await?;
            return Ok(PublishedMessage {
                post: current.into_post()?,
                channel_chat_id: channel.telegram_chat_id,
            });
        }
        if current.post_state()? != PostState::Sending
            || current.send_generation != lease.generation
            || current.send_token != Some(lease.token)
        {
            return Err(PublisherRepositoryError::PublishLeaseLost(id));
        }
        let row = sqlx::query_as::<_, PostRow>(
            "UPDATE posts SET state = 'published', telegram_message_id = $2, published_at = COALESCE(published_at, now()), send_token = NULL, send_started_at = NULL, error_class = NULL, error_message = NULL, revision = revision + 1, updated_at = now() WHERE id = $1 AND state = 'sending' AND send_generation = $3 AND send_token = $4 RETURNING *",
        )
        .bind(id)
        .bind(telegram_message_id)
        .bind(lease.generation)
        .bind(lease.token)
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(PublishedMessage { post: row.into_post()?, channel_chat_id: channel.telegram_chat_id })
    }

    pub async fn fail_publish(
        &self,
        id: Uuid,
        lease: &PublishLease,
        state: PostState,
        error_class: &str,
        error_message: &str,
    ) -> Result<Post, PublisherRepositoryError> {
        if !matches!(state, PostState::Failed | PostState::Unknown) {
            return Err(PublisherRepositoryError::InvalidPublishFailureState(state));
        }
        let mut transaction = self.pool.begin().await?;
        let channel_id = post_channel_id(&mut transaction, id).await?;
        lock_channel(&mut transaction, channel_id).await?;
        lock_post(&mut transaction, id).await?;
        lock_current_job_attempt(&mut transaction, id, &lease.attempt).await?;
        let row = sqlx::query_as::<_, PostRow>(
            "UPDATE posts SET state = $2, send_token = NULL, send_started_at = NULL, error_class = $3, error_message = $4, revision = revision + 1, updated_at = now() WHERE id = $1 AND state = 'sending' AND send_generation = $5 AND send_token = $6 RETURNING *",
        )
        .bind(id)
        .bind(state.as_str())
        .bind(error_class)
        .bind(error_message)
        .bind(lease.generation)
        .bind(lease.token)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PublisherRepositoryError::PublishLeaseLost(id))?;
        transaction.commit().await?;
        row.into_post()
    }

    pub async fn retry_publish(
        &self,
        id: Uuid,
        lease: &PublishLease,
        error_class: &str,
        error_message: &str,
    ) -> Result<PublishRetry, PublisherRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let channel_id = post_channel_id(&mut transaction, id).await?;
        lock_channel(&mut transaction, channel_id).await?;
        let current = lock_post(&mut transaction, id).await?;
        let job = lock_current_job_attempt(&mut transaction, id, &lease.attempt).await?;
        if current.post_state()? != PostState::Sending
            || current.send_generation != lease.generation
            || current.send_token != Some(lease.token)
        {
            return Err(PublisherRepositoryError::PublishLeaseLost(id));
        }
        let revision = next_revision(current.revision)?;
        let terminal = job.attempt_count >= job.max_attempts;
        let next_state = if terminal { PostState::Failed } else { PostState::Queued };
        let row = sqlx::query_as::<_, PostRow>(
            "UPDATE posts SET state = $2, send_token = NULL, send_started_at = NULL, error_class = $3, error_message = $4, revision = $5, updated_at = now() WHERE id = $1 AND state = 'sending' AND send_generation = $6 AND send_token = $7 RETURNING *",
        )
        .bind(id)
        .bind(next_state.as_str())
        .bind(error_class)
        .bind(error_message)
        .bind(revision)
        .bind(lease.generation)
        .bind(lease.token)
        .fetch_one(&mut *transaction)
        .await?;
        if !terminal {
            let updated = sqlx::query(
                "UPDATE queue.jobs SET payload = $2, updated_at = now() WHERE id = $1 AND state = 'running'",
            )
            .bind(job.id)
            .bind(serde_json::json!({ "post_id": id, "expected_revision": row.revision }))
            .execute(&mut *transaction)
            .await?;
            if updated.rows_affected() != 1 {
                return Err(PublisherRepositoryError::PublishJobUpdateLost(id));
            }
        }
        transaction.commit().await?;
        Ok(PublishRetry { post: row.into_post()?, terminal })
    }

    pub async fn reconcile_interrupted_publish(
        &self,
        id: Uuid,
        attempt: &JobLease,
    ) -> Result<bool, PublisherRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let channel_id = post_channel_id(&mut transaction, id).await?;
        lock_channel(&mut transaction, channel_id).await?;
        lock_post(&mut transaction, id).await?;
        lock_current_job_attempt(&mut transaction, id, attempt).await?;
        let updated = sqlx::query(
            "UPDATE posts SET state = 'unknown', send_token = NULL, send_started_at = NULL, error_class = 'publication_interrupted', error_message = 'a previous publication attempt lost its job lease', revision = revision + 1, updated_at = now() WHERE id = $1 AND state = 'sending'",
        )
        .bind(id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(updated.rows_affected() == 1)
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

async fn lock_media_state(
    transaction: &mut Transaction<'_, Postgres>,
    media_id: Uuid,
) -> Result<String, PublisherRepositoryError> {
    sqlx::query_scalar::<_, String>("SELECT storage_state FROM media WHERE id = $1 FOR UPDATE")
        .bind(media_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(PublisherRepositoryError::MediaMissing(media_id))
}

async fn lock_publication_request(
    transaction: &mut Transaction<'_, Postgres>,
    request_key: &str,
) -> Result<(), PublisherRepositoryError> {
    let digest = Sha256::digest(request_key.as_bytes());
    let mut key_bytes = [0_u8; 8];
    key_bytes.copy_from_slice(&digest[..8]);
    let lock_key = i64::from_be_bytes(key_bytes);
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(lock_key)
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

async fn lock_channel_configuration(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(CHANNEL_CONFIGURATION_ADVISORY_LOCK)
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

fn ensure_repeat_decision_resolved(current: &PostRow) -> Result<(), PublisherRepositoryError> {
    if current.repeat_evidence.is_some() {
        return Err(PublisherRepositoryError::RepeatDecisionRequired { id: current.id });
    }
    Ok(())
}

async fn single_enabled_channel(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<Uuid, PublisherRepositoryError> {
    match select_enabled_channel_candidates(transaction).await?.as_slice() {
        [] => Err(PublisherRepositoryError::PublicationChannelNotConfigured),
        [channel_id] => Ok(*channel_id),
        _ => Err(PublisherRepositoryError::PublicationChannelAmbiguous),
    }
}

async fn insert_intended_post(
    transaction: &mut Transaction<'_, Postgres>,
    input: IntendedPostInput<'_>,
) -> Result<PostRow, PublisherRepositoryError> {
    let IntendedPostInput {
        request_key,
        request_hash,
        origin_ingest_id,
        media_id,
        channel,
        intent,
        post_id,
        now,
    } = input;
    let evaluation_at = match (intent.action, intent.requested_publish_at) {
        (PublicationAction::PostNow, None) => now,
        (PublicationAction::PostNow, Some(_)) => {
            return Err(PublisherRepositoryError::Validation(
                PublisherValidationError::PostNowTimeNotAllowed,
            ));
        }
        (PublicationAction::Queue, Some(requested_at)) => {
            if requested_at <= now {
                return Err(PublisherRepositoryError::ExactScheduleInPast);
            }
            requested_at
        }
        (PublicationAction::Queue, None) => {
            next_free_slot(transaction, channel, post_id, now).await?
        }
    };
    let conflicts = find_repeat_conflicts(transaction, media_id, evaluation_at, post_id).await?;
    let repeat_evidence = if conflicts.is_empty() {
        None
    } else {
        let evidence = RepeatEvidence { conflicts };
        let value = serde_json::to_value(&evidence)?;
        let encoded = serde_json::to_vec(&value)?;
        if encoded.len() > MAX_REPEAT_EVIDENCE_BYTES {
            return Err(PublisherRepositoryError::RepeatEvidenceTooLarge {
                max: MAX_REPEAT_EVIDENCE_BYTES,
            });
        }
        Some(value)
    };
    let needs_decision = repeat_evidence.is_some();
    let state = if needs_decision { "draft" } else { "queued" };
    let cadence_slot_at = (!needs_decision && intent.action == PublicationAction::Queue)
        .then_some(intent.requested_publish_at.is_none().then_some(evaluation_at))
        .flatten();
    let row = sqlx::query_as::<_, PostRow>(
        r#"
        INSERT INTO posts (
            id, request_key, request_hash, origin_ingest_id, requested_action,
            requested_publish_at, repeat_evidence, media_id, channel_id, state,
            caption, parse_mode, disable_notification, scheduled_at, cadence_slot_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        RETURNING *
        "#,
    )
    .bind(post_id)
    .bind(request_key)
    .bind(request_hash)
    .bind(origin_ingest_id)
    .bind(intent.action.as_str())
    .bind(intent.requested_publish_at)
    .bind(&repeat_evidence)
    .bind(media_id)
    .bind(channel.id)
    .bind(state)
    .bind(&intent.caption)
    .bind(&channel.default_parse_mode)
    .bind(channel.default_disable_notification)
    .bind(evaluation_at)
    .bind(cadence_slot_at)
    .fetch_one(&mut **transaction)
    .await?;
    if !needs_decision {
        insert_publish_job(transaction, row.id, evaluation_at, row.revision).await?;
    }
    Ok(row)
}

async fn find_repeat_conflicts(
    transaction: &mut Transaction<'_, Postgres>,
    media_id: Uuid,
    intended_at: OffsetDateTime,
    post_id: Uuid,
) -> Result<Vec<RepeatConflict>, PublisherRepositoryError> {
    let rows = sqlx::query_as::<_, RepeatConflictRow>(
        r#"
        SELECT posts.id, posts.state, posts.scheduled_at, posts.published_at,
               posts.telegram_message_id, channels.telegram_chat_id
        FROM posts
        JOIN channels ON channels.id = posts.channel_id
        WHERE posts.media_id = $1
          AND posts.id <> $2
          AND (
              posts.state IN ('queued', 'sending')
              OR (
                  posts.state = 'published'
                  AND posts.published_at IS NOT NULL
                  AND posts.published_at > $3 - interval '14 days'
                  AND posts.published_at < $3
              )
          )
        ORDER BY COALESCE(posts.published_at, posts.scheduled_at), posts.id
        LIMIT $4
        "#,
    )
    .bind(media_id)
    .bind(post_id)
    .bind(intended_at)
    .bind(i64::try_from(MAX_REPEAT_EVIDENCE_CONFLICTS).expect("evidence limit fits i64"))
    .fetch_all(&mut **transaction)
    .await?;
    rows.into_iter()
        .map(|row| {
            let state = PostState::try_from(row.state.as_str())
                .map_err(PublisherRepositoryError::InvalidState)?;
            let at = row
                .published_at
                .or(row.scheduled_at)
                .ok_or(PublisherRepositoryError::InvalidRepeatEvidenceTimestamp(row.id))?;
            let target_message_link = row
                .telegram_message_id
                .filter(|_| state == PostState::Published)
                .map(|message_id| telegram_message_link(row.telegram_chat_id, message_id));
            Ok(RepeatConflict { post_id: row.id, state, at, target_message_link })
        })
        .collect()
}

fn telegram_message_link(chat_id: i64, message_id: i64) -> String {
    let chat = chat_id.to_string();
    let chat = chat.strip_prefix("-100").unwrap_or(chat.as_str());
    format!("https://t.me/c/{chat}/{message_id}")
}

fn telegram_message_link_optional(chat_id: Option<i64>, message_id: Option<i64>) -> Option<String> {
    let (chat_id, message_id) = chat_id.zip(message_id)?;
    (chat_id < 0 && message_id > 0).then(|| telegram_message_link(chat_id, message_id))
}

async fn apply_exact_schedule(
    transaction: &mut Transaction<'_, Postgres>,
    current: &PostRow,
    requested_at: OffsetDateTime,
    request_key: &str,
    request_hash: &[u8],
    revision: i64,
) -> Result<PostRow, PublisherRepositoryError> {
    sqlx::query_as::<_, PostRow>(
        "UPDATE posts SET schedule_request_key = $2, schedule_request_hash = $3, requested_action = 'queue', requested_publish_at = $4, repeat_evidence = NULL, state = 'queued', scheduled_at = $4, cadence_slot_at = NULL, revision = $5, error_class = NULL, error_message = NULL, updated_at = now() WHERE id = $1 AND state IN ('draft', 'queued', 'failed') AND revision = $6 RETURNING *",
    )
    .bind(current.id)
    .bind(request_key)
    .bind(request_hash)
    .bind(requested_at)
    .bind(revision)
    .bind(current.revision)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(PublisherRepositoryError::PostCannotBeScheduled {
        id: current.id,
        state: current.post_state()?,
    })
}

fn decision_allowed(
    action: PublicationAction,
    requested_publish_at: Option<OffsetDateTime>,
    decision: PublicationDecision,
) -> bool {
    match action {
        PublicationAction::PostNow => {
            matches!(
                decision,
                PublicationDecision::PostNowAnyway
                    | PublicationDecision::QueueAnyway
                    | PublicationDecision::Cancel
            )
        }
        PublicationAction::Queue => {
            matches!(
                decision,
                PublicationDecision::QueueAnyway
                    | PublicationDecision::KeepExactTime
                    | PublicationDecision::Cancel
            ) && (requested_publish_at.is_some()
                || !matches!(decision, PublicationDecision::KeepExactTime))
        }
    }
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

async fn update_failed_publish_job(
    transaction: &mut Transaction<'_, Postgres>,
    post_id: Uuid,
    expected_revision: i64,
) -> Result<(), PublisherRepositoryError> {
    let row = lock_publish_job(transaction, post_id).await?;
    let Some(row) = row else {
        return Err(PublisherRepositoryError::PublishJobMissing(post_id));
    };
    if row.state != "failed" {
        return Err(PublisherRepositoryError::PublishJobUnavailable { post_id, state: row.state });
    }
    let updated = sqlx::query(
        "UPDATE queue.jobs SET payload = $2, updated_at = now() WHERE id = $1 AND state = 'failed'",
    )
    .bind(row.id)
    .bind(serde_json::json!({ "post_id": post_id, "expected_revision": expected_revision }))
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

#[derive(Debug, Clone, sqlx::FromRow)]
struct PublishJobLeaseRow {
    id: Uuid,
    attempt_count: i32,
    max_attempts: i32,
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

async fn lock_current_job_attempt(
    transaction: &mut Transaction<'_, Postgres>,
    post_id: Uuid,
    attempt: &JobLease,
) -> Result<PublishJobLeaseRow, PublisherRepositoryError> {
    sqlx::query_as::<_, PublishJobLeaseRow>(
        "SELECT id, attempt_count, max_attempts FROM queue.jobs WHERE id = $1 AND kind = 'publish_post' AND payload->>'post_id' = $5 AND state = 'running' AND attempt_count = $2 AND lease_owner = $3 AND lease_token = $4 AND lease_expires_at > clock_timestamp() FOR UPDATE",
    )
    .bind(attempt.job_id)
    .bind(attempt.attempt_number)
    .bind(&attempt.lease_owner)
    .bind(attempt.lease_token)
    .bind(post_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(PublisherRepositoryError::PublishLeaseLost(post_id))
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

fn exact_schedule_request_hash(requested_at: OffsetDateTime) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(b"exact_schedule:v1:");
    hasher.update(requested_at.unix_timestamp_nanos().to_be_bytes());
    hasher.finalize().to_vec()
}

fn decision_request_hash(decision: PublicationDecision, expected_revision: i64) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(b"publication_decision:v1:");
    hasher.update(decision.as_str().as_bytes());
    hasher.update(expected_revision.to_be_bytes());
    hasher.finalize().to_vec()
}

fn publication_intent_hash(
    media_id: Uuid,
    intent: &PublicationIntent,
) -> Result<Vec<u8>, PublisherRepositoryError> {
    let mut hasher = Sha256::new();
    hasher.update(b"publication_intent:v1:");
    hasher.update(media_id.as_bytes());
    hasher.update(serde_json::to_vec(intent)?);
    Ok(hasher.finalize().to_vec())
}

fn check_expected_revision(
    current: &PostRow,
    expected_revision: i64,
) -> Result<(), PublisherRepositoryError> {
    if expected_revision < 0 {
        return Err(PublisherRepositoryError::Validation(
            PublisherValidationError::InvalidExpectedRevision,
        ));
    }
    if expected_revision != current.revision {
        return Err(PublisherRepositoryError::OptimisticConflict(current.id));
    }
    Ok(())
}

fn next_revision(current: i64) -> Result<i64, PublisherRepositoryError> {
    current.checked_add(1).ok_or(PublisherRepositoryError::RevisionOverflow)
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

fn next_allowed_slot(
    requested: OffsetDateTime,
    channel: &ChannelRow,
) -> Result<OffsetDateTime, PublisherRepositoryError> {
    let timezone: Tz = channel
        .time_zone
        .parse()
        .map_err(|_| PublisherRepositoryError::InvalidTimeZone(channel.time_zone.clone()))?;
    let candidate = to_chrono(requested)?;
    let start = to_chrono_time(channel.window_start);
    let end = to_chrono_time(channel.window_end);
    let interval = i64::from(channel.interval_minutes);
    if interval <= 0 {
        return Err(PublisherRepositoryError::CadenceSearchExhausted);
    }
    let mut date = candidate.with_timezone(&timezone).date_naive();
    for _ in 0..370 {
        let mut target = NaiveDateTime::new(date, start);
        let end_target = NaiveDateTime::new(date, end);
        while target < end_target {
            let mut resolved = match timezone.from_local_datetime(&target) {
                chrono::LocalResult::Single(value) => vec![value.with_timezone(&Utc)],
                chrono::LocalResult::Ambiguous(earliest, latest) => {
                    vec![earliest.with_timezone(&Utc), latest.with_timezone(&Utc)]
                }
                chrono::LocalResult::None => Vec::new(),
            };
            resolved.sort_unstable();
            if let Some(result) = resolved.into_iter().find(|result| *result >= candidate) {
                return from_chrono(result);
            }
            target += ChronoDuration::minutes(interval);
        }
        date += ChronoDuration::days(1);
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
        let requested_action = PublicationAction::try_from(self.requested_action.as_str())
            .map_err(|_| {
                PublisherRepositoryError::InvalidPublicationAction(self.requested_action.clone())
            })?;
        let repeat_evidence = self.repeat_evidence.map(serde_json::from_value).transpose()?;
        Ok(Post {
            id: self.id,
            media_id: self.media_id,
            channel_id: self.channel_id,
            origin_ingest_id: self.origin_ingest_id,
            requested_action,
            requested_publish_at: self.requested_publish_at,
            repeat_evidence,
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

/// Return enough enabled channels to distinguish the valid single-target
/// configuration from both missing and ambiguous configuration. The row locks
/// keep a selected channel enabled until the caller commits its snapshot.
pub(crate) async fn select_enabled_channel_candidates(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<Vec<Uuid>, sqlx::Error> {
    sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM channels WHERE is_enabled ORDER BY id LIMIT 2 FOR SHARE",
    )
    .fetch_all(&mut **transaction)
    .await
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
    sooqa_publisher::validate_expected_revision(update.expected_revision)
        .map_err(PublisherRepositoryError::Validation)?;
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
    #[error("channel {0} was modified by another request")]
    ChannelOptimisticConflict(Uuid),
    #[error("channel update must change at least one field")]
    EmptyChannelUpdate,
    #[error("enabling channel would create an ambiguous publication target")]
    ChannelEnablementAmbiguous,
    #[error("publication requires exactly one enabled target channel")]
    PublicationChannelNotConfigured,
    #[error("publication target channel configuration is ambiguous")]
    PublicationChannelAmbiguous,
    #[error("media {0} was not found")]
    MediaMissing(Uuid),
    #[error("media {media_id} is not ready for publication: {state}")]
    MediaNotReady { media_id: Uuid, state: String },
    #[error("post {0} was not found")]
    PostMissing(Uuid),
    #[error("post list limit must be between 1 and 50, got {value}")]
    InvalidLimit { value: u32 },
    #[error("media preview metadata for post {0} is invalid")]
    InvalidPreviewMetadata(Uuid),
    #[error("database count was negative")]
    InvalidCount,
    #[error("post {id} is not editable in state {state:?}")]
    PostNotEditable { id: Uuid, state: PostState },
    #[error("post {id} cannot be scheduled in state {state:?}")]
    PostCannotBeScheduled { id: Uuid, state: PostState },
    #[error("ingest {0} was not found for publication materialization")]
    IngestMissing(Uuid),
    #[error("ingest {0} has no resolved media for publication materialization")]
    IngestMediaMissing(Uuid),
    #[error("ingest {ingest_id} is not ready for publication materialization")]
    MaterializationNotReady { ingest_id: Uuid },
    #[error("invalid publication action in database: {0}")]
    InvalidPublicationAction(String),
    #[error("publication decision {decision:?} is not allowed for post {id}")]
    InvalidPublicationDecision { id: Uuid, decision: PublicationDecision },
    #[error("post {id} cannot consume a publication decision in state {state:?}")]
    PostDecisionNotAllowed { id: Uuid, state: PostState },
    #[error("post {id} requires a repeat publication decision")]
    RepeatDecisionRequired { id: Uuid },
    #[error("post {0} has no exact requested publication time")]
    ExactTimeMissing(Uuid),
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
    #[error("explicit publication time is in the past")]
    ExactScheduleInPast,
    #[error("cadence slot is outside the channel window or interval")]
    InvalidCadenceSlot,
    #[error("invalid publication timestamp")]
    InvalidTimestamp,
    #[error("repeat evidence for post {0} has no timestamp")]
    InvalidRepeatEvidenceTimestamp(Uuid),
    #[error("repeat evidence exceeds the {max}-byte limit")]
    RepeatEvidenceTooLarge { max: usize },
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn channel(time_zone: &str, window_start: u8, window_end: u8) -> ChannelRow {
        let now = OffsetDateTime::now_utc();
        ChannelRow {
            id: Uuid::now_v7(),
            telegram_chat_id: -100123,
            name: "test".to_owned(),
            is_enabled: true,
            time_zone: time_zone.to_owned(),
            window_start: Time::from_hms(window_start, 0, 0).expect("valid test start"),
            window_end: Time::from_hms(window_end, 0, 0).expect("valid test end"),
            interval_minutes: 30,
            default_parse_mode: None,
            default_disable_notification: false,
            created_at: now,
            updated_at: now,
        }
    }

    fn timestamp(value: &str) -> OffsetDateTime {
        OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
            .expect("valid test timestamp")
    }

    #[test]
    fn next_allowed_slot_skips_nonexistent_spring_grid_points() {
        let channel = channel("America/New_York", 1, 4);
        let next = next_allowed_slot(timestamp("2026-03-08T06:40:00Z"), &channel)
            .expect("a later grid point should exist");
        assert_eq!(next, timestamp("2026-03-08T07:00:00Z"));
    }

    #[test]
    fn next_allowed_slot_chooses_the_later_fall_fold_occurrence_when_needed() {
        let channel = channel("America/New_York", 1, 4);
        let next = next_allowed_slot(timestamp("2026-11-01T06:10:00Z"), &channel)
            .expect("the second fold occurrence should be available");
        assert_eq!(next, timestamp("2026-11-01T06:30:00Z"));
    }
}
