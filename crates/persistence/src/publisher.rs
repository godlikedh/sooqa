use sha2::{Digest, Sha256};
use sooqa_library::{ContentStatus, StorageState};
use sooqa_publisher::{
    ChannelPolicy, NewChannelPolicy, NewPostDraft, NewPublicationSchedule, NewTargetChannel,
    PostDraft, PostDraftStatus, PostDraftUpdate, PublicationAttempt, PublicationAttemptStatus,
    PublicationCompletion, PublicationSchedule, PublicationScheduleScope,
    PublicationScheduleStatus, PublishedPost, PublishedPostStatus, PublisherValidationError,
    TargetChannel, transition_post_draft_status, transition_publication_schedule_status,
};
use sqlx::{FromRow, PgPool};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Clone)]
pub struct PublisherRepository {
    pool: PgPool,
}

#[derive(Debug, Clone)]
pub struct CreatePostDraftResult {
    pub draft: PostDraft,
    pub created: bool,
}

#[derive(Debug, FromRow, Clone)]
struct ChannelRow {
    id: Uuid,
    name: String,
    telegram_chat_id: i64,
    is_enabled: bool,
    default_parse_mode: Option<String>,
    default_disable_notification: bool,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
}

#[derive(Debug, FromRow, Clone)]
struct PostRow {
    id: Uuid,
    request_key: Option<String>,
    request_hash: Option<Vec<u8>>,
    media_id: Uuid,
    channel_id: Uuid,
    state: String,
    caption: Option<String>,
    parse_mode: Option<String>,
    scheduled_at: OffsetDateTime,
    send_generation: i32,
    send_started_at: Option<OffsetDateTime>,
    telegram_message_id: Option<i64>,
    published_at: Option<OffsetDateTime>,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
}

impl PublisherRepository {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create_target_channel(
        &self,
        channel: NewTargetChannel,
    ) -> Result<TargetChannel, PublisherRepositoryError> {
        let row = sqlx::query_as::<_, ChannelRow>(
            "INSERT INTO channels (name, telegram_chat_id, default_parse_mode, default_disable_notification) VALUES ($1, $2, $3, $4) RETURNING *",
        )
        .bind(channel.name).bind(channel.telegram_chat_id).bind(channel.default_parse_mode).bind(channel.default_disable_notification)
        .fetch_one(&self.pool).await?;
        row.into_channel()
    }

    pub async fn find_target_channel(
        &self,
        id: Uuid,
    ) -> Result<Option<TargetChannel>, PublisherRepositoryError> {
        sqlx::query_as::<_, ChannelRow>("SELECT * FROM channels WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(ChannelRow::into_channel)
            .transpose()
    }

    pub async fn list_target_channels(
        &self,
        enabled_only: bool,
    ) -> Result<Vec<TargetChannel>, PublisherRepositoryError> {
        let rows = if enabled_only {
            sqlx::query_as::<_, ChannelRow>(
                "SELECT * FROM channels WHERE is_enabled ORDER BY name, id",
            )
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as::<_, ChannelRow>("SELECT * FROM channels ORDER BY name, id")
                .fetch_all(&self.pool)
                .await?
        };
        rows.into_iter().map(ChannelRow::into_channel).collect()
    }

    pub async fn set_target_channel_enabled(
        &self,
        id: Uuid,
        enabled: bool,
    ) -> Result<TargetChannel, PublisherRepositoryError> {
        let row = sqlx::query_as::<_, ChannelRow>(
            "UPDATE channels SET is_enabled = $2, updated_at = now() WHERE id = $1 RETURNING *",
        )
        .bind(id)
        .bind(enabled)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(PublisherRepositoryError::TargetChannelMissing(id))?;
        row.into_channel()
    }

    pub async fn upsert_channel_policy(
        &self,
        policy: NewChannelPolicy,
    ) -> Result<ChannelPolicy, PublisherRepositoryError> {
        policy.validate()?;
        let minutes =
            i32::try_from(policy.minimum_post_interval_seconds.div_ceil(60)).map_err(|_| {
                PublisherRepositoryError::NumberOverflow { field: "minimum_post_interval_seconds" }
            })?;
        let row = sqlx::query_as::<_, ChannelRow>("UPDATE channels SET interval_minutes = $2, updated_at = now() WHERE id = $1 RETURNING *")
            .bind(policy.target_channel_id).bind(minutes.max(1)).fetch_optional(&self.pool).await?
            .ok_or(PublisherRepositoryError::TargetChannelMissing(policy.target_channel_id))?;
        let channel = row.into_channel()?;
        Ok(policy_from_channel(&channel, &policy))
    }

    pub async fn find_channel_policy(
        &self,
        id: Uuid,
    ) -> Result<Option<ChannelPolicy>, PublisherRepositoryError> {
        let Some(channel) = self.find_target_channel(id).await? else { return Ok(None) };
        Ok(Some(policy_from_channel(&channel, &NewChannelPolicy::default_for(id))))
    }

    pub async fn create_post_draft(
        &self,
        draft: NewPostDraft,
    ) -> Result<PostDraft, PublisherRepositoryError> {
        validate_draft(&self.pool, &draft).await?;
        let row = sqlx::query_as::<_, PostRow>(
            "INSERT INTO posts (media_id, channel_id, state, caption, parse_mode) VALUES ($1, $2, 'draft', $3, $4) RETURNING *",
        )
        .bind(draft.content_item_id).bind(draft.target_channel_id).bind(draft.caption).bind(draft.parse_mode)
        .fetch_one(&self.pool).await?;
        row.into_draft()
    }

    pub async fn create_post_draft_idempotent(
        &self,
        draft: NewPostDraft,
        key: impl Into<String>,
        request_hash: &[u8],
    ) -> Result<CreatePostDraftResult, PublisherRepositoryError> {
        let key = normalize_key(key.into())?;
        validate_draft(&self.pool, &draft).await?;
        let id = Uuid::now_v7();
        let mut tx = self.pool.begin().await?;
        let inserted = sqlx::query_as::<_, PostRow>(
            "INSERT INTO posts (id, request_key, request_hash, media_id, channel_id, state, caption, parse_mode) VALUES ($1, $2, $3, $4, $5, 'draft', $6, $7) ON CONFLICT (request_key) WHERE request_key IS NOT NULL DO NOTHING RETURNING *",
        )
        .bind(id).bind(&key).bind(request_hash).bind(draft.content_item_id).bind(draft.target_channel_id).bind(draft.caption).bind(draft.parse_mode)
        .fetch_optional(&mut *tx).await?;
        if let Some(row) = inserted {
            tx.commit().await?;
            return Ok(CreatePostDraftResult { draft: row.into_draft()?, created: true });
        }
        let existing =
            sqlx::query_as::<_, PostRow>("SELECT * FROM posts WHERE request_key = $1 FOR UPDATE")
                .bind(&key)
                .fetch_one(&mut *tx)
                .await?;
        if existing.request_hash.as_deref() != Some(request_hash) {
            return Err(PublisherRepositoryError::DraftIdempotencyConflict(key));
        }
        tx.commit().await?;
        Ok(CreatePostDraftResult { draft: existing.into_draft()?, created: false })
    }

    pub async fn replay_post_draft_create(
        &self,
        key: &str,
        request_hash: &[u8],
        _: Uuid,
        _: Uuid,
        _: Option<&str>,
        _: Option<&str>,
    ) -> Result<Option<PostDraft>, PublisherRepositoryError> {
        let key = normalize_key(key.to_owned())?;
        let Some(row) = sqlx::query_as::<_, PostRow>("SELECT * FROM posts WHERE request_key = $1")
            .bind(key)
            .fetch_optional(&self.pool)
            .await?
        else {
            return Ok(None);
        };
        if row.request_hash.as_deref() != Some(request_hash) {
            return Err(PublisherRepositoryError::DraftIdempotencyConflict(
                row.request_key.unwrap_or_default(),
            ));
        }
        Ok(Some(row.into_draft()?))
    }

    pub async fn find_post_draft(
        &self,
        id: Uuid,
    ) -> Result<Option<PostDraft>, PublisherRepositoryError> {
        sqlx::query_as::<_, PostRow>("SELECT * FROM posts WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(PostRow::into_draft)
            .transpose()
    }

    pub async fn update_post_draft(
        &self,
        id: Uuid,
        update: PostDraftUpdate,
    ) -> Result<PostDraft, PublisherRepositoryError> {
        let mut tx = self.pool.begin().await?;
        let draft = update_draft_tx(&mut tx, id, update).await?;
        tx.commit().await?;
        Ok(draft)
    }

    pub async fn update_post_draft_idempotent(
        &self,
        id: Uuid,
        update: PostDraftUpdate,
        key: impl Into<String>,
    ) -> Result<PostDraft, PublisherRepositoryError> {
        let key = normalize_key(format!("post:{id}:update:{}", key.into()))?;
        let request_hash = post_draft_update_request_hash(id, &update);
        let mut tx = self.pool.begin().await?;
        if let Some(row) =
            sqlx::query_as::<_, PostRow>("SELECT * FROM posts WHERE request_key = $1 FOR UPDATE")
                .bind(&key)
                .fetch_optional(&mut *tx)
                .await?
        {
            if row.request_hash.as_deref() != Some(request_hash.as_slice()) {
                return Err(PublisherRepositoryError::DraftIdempotencyConflict(key));
            }
            tx.commit().await?;
            return row.into_draft();
        }
        let draft = update_draft_tx(&mut tx, id, update).await?;
        sqlx::query("UPDATE posts SET request_key = $2, request_hash = $3, updated_at = now() WHERE id = $1 AND request_key IS NULL")
            .bind(id).bind(&key).bind(&request_hash).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(draft)
    }

    pub async fn replay_post_draft_update(
        &self,
        key: &str,
        request_hash: &[u8],
    ) -> Result<Option<PostDraft>, PublisherRepositoryError> {
        let Some(row) = sqlx::query_as::<_, PostRow>("SELECT * FROM posts WHERE request_key = $1")
            .bind(key)
            .fetch_optional(&self.pool)
            .await?
        else {
            return Ok(None);
        };
        if row.request_hash.as_deref() != Some(request_hash) {
            return Err(PublisherRepositoryError::DraftIdempotencyConflict(key.to_owned()));
        }
        Ok(Some(row.into_draft()?))
    }

    pub async fn create_publication_schedule(
        &self,
        schedule: NewPublicationSchedule,
    ) -> Result<PublicationSchedule, PublisherRepositoryError> {
        self.create_publication_schedule_with_scope(schedule, PublicationScheduleScope::Schedule)
            .await
    }

    pub async fn create_publication_schedule_with_scope(
        &self,
        schedule: NewPublicationSchedule,
        _: PublicationScheduleScope,
    ) -> Result<PublicationSchedule, PublisherRepositoryError> {
        schedule.validate()?;
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query_as::<_, PostRow>("SELECT * FROM posts WHERE id = $1 FOR UPDATE")
            .bind(schedule.post_draft_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(PublisherRepositoryError::PostDraftMissing(schedule.post_draft_id))?;
        if row.state == "published" || row.state == "cancelled" {
            return Err(PublisherRepositoryError::DraftNotReady {
                id: row.id,
                status: row.into_draft()?.status,
            });
        }
        let updated = sqlx::query_as::<_, PostRow>("UPDATE posts SET state = 'queued', scheduled_at = $2, cadence_slot_at = $2, updated_at = now() WHERE id = $1 RETURNING *")
            .bind(schedule.post_draft_id).bind(schedule.publish_at).fetch_one(&mut *transaction).await?;
        enqueue_publish_job(&mut transaction, updated.id, updated.scheduled_at).await?;
        transaction.commit().await?;
        updated.into_schedule()
    }

    pub async fn find_publication_schedule(
        &self,
        id: Uuid,
    ) -> Result<Option<PublicationSchedule>, PublisherRepositoryError> {
        sqlx::query_as::<_, PostRow>("SELECT * FROM posts WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(PostRow::into_schedule)
            .transpose()
    }

    pub async fn list_due_publication_schedules(
        &self,
        now: OffsetDateTime,
        limit: u32,
    ) -> Result<Vec<PublicationSchedule>, PublisherRepositoryError> {
        if !(1..=100).contains(&limit) {
            return Err(PublisherRepositoryError::InvalidLimit(limit));
        }
        let rows = sqlx::query_as::<_, PostRow>("SELECT p.* FROM posts p JOIN channels c ON c.id = p.channel_id WHERE p.state IN ('queued', 'failed') AND p.scheduled_at <= $1 AND c.is_enabled ORDER BY p.scheduled_at, p.created_at, p.id LIMIT $2")
            .bind(now).bind(i64::from(limit)).fetch_all(&self.pool).await?;
        rows.into_iter().map(PostRow::into_schedule).collect()
    }

    pub async fn transition_publication_schedule(
        &self,
        id: Uuid,
        target: PublicationScheduleStatus,
    ) -> Result<PublicationSchedule, PublisherRepositoryError> {
        let current = self
            .find_publication_schedule(id)
            .await?
            .ok_or(PublisherRepositoryError::ScheduleMissing(id))?;
        if matches!(
            target,
            PublicationScheduleStatus::Publishing | PublicationScheduleStatus::Published
        ) {
            return Err(PublisherRepositoryError::ManagedScheduleTransitionRequired { id, target });
        }
        let next = transition_publication_schedule_status(current.status, target)?;
        let state = db_post_state(next);
        let row = sqlx::query_as::<_, PostRow>(
            "UPDATE posts SET state = $2, updated_at = now() WHERE id = $1 RETURNING *",
        )
        .bind(id)
        .bind(state)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(PublisherRepositoryError::ScheduleMissing(id))?;
        row.into_schedule()
    }

    pub async fn start_publication_attempt(
        &self,
        id: Uuid,
        telegram_request_key: Option<String>,
    ) -> Result<PublicationAttempt, PublisherRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let current = sqlx::query_as::<_, PostRow>("SELECT * FROM posts WHERE id = $1 FOR UPDATE")
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(PublisherRepositoryError::ScheduleMissing(id))?;
        if !matches!(current.state.as_str(), "queued" | "failed") {
            return Err(PublisherRepositoryError::InvalidScheduleState {
                id,
                status: domain_schedule_status(&current.state),
            });
        }
        let channel = sqlx::query_as::<_, ChannelRow>("SELECT * FROM channels WHERE id = $1")
            .bind(current.channel_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(PublisherRepositoryError::TargetChannelMissing(current.channel_id))?;
        if !channel.is_enabled {
            return Err(PublisherRepositoryError::TargetChannelDisabled(channel.id));
        }
        let storage_state =
            sqlx::query_scalar::<_, String>("SELECT storage_state FROM media WHERE id = $1")
                .bind(current.media_id)
                .fetch_optional(&mut *transaction)
                .await?
                .ok_or(PublisherRepositoryError::ContentItemMissing(current.media_id))?;
        if storage_state != "ready" {
            return Err(PublisherRepositoryError::AssetNotPublishable {
                asset_id: current.media_id,
                storage_state: domain_storage_state(&storage_state),
            });
        }
        let row = sqlx::query_as::<_, PostRow>("UPDATE posts SET state = 'sending', send_generation = send_generation + 1, send_token = gen_random_uuid(), send_started_at = now(), updated_at = now() WHERE id = $1 AND state IN ('queued', 'failed') RETURNING *")
            .bind(id).fetch_one(&mut *transaction).await?;
        transaction.commit().await?;
        Ok(attempt_from_row(
            &row,
            telegram_request_key,
            PublicationAttemptStatus::Running,
            None,
            None,
            None,
        ))
    }

    pub async fn finish_publication_attempt(
        &self,
        id: Uuid,
        attempt_number: i32,
        status: PublicationAttemptStatus,
        error_class: Option<&str>,
        error_message: Option<&str>,
        response_json: Option<serde_json::Value>,
    ) -> Result<PublicationAttempt, PublisherRepositoryError> {
        if status == PublicationAttemptStatus::Running {
            return Err(PublisherRepositoryError::AttemptMustFinish);
        }
        if status == PublicationAttemptStatus::Succeeded {
            return Err(PublisherRepositoryError::PublicationCompletionRequired);
        }
        let state = match status {
            PublicationAttemptStatus::Failed => "failed",
            PublicationAttemptStatus::Unknown => "unknown",
            _ => unreachable!(),
        };
        let row = sqlx::query_as::<_, PostRow>("UPDATE posts SET state = $2, send_token = NULL, send_started_at = NULL, error_class = $3, error_message = $4, updated_at = now() WHERE id = $1 AND state = 'sending' AND send_generation = $5 RETURNING *")
            .bind(id).bind(state).bind(error_class).bind(error_message).bind(attempt_number).fetch_optional(&self.pool).await?.ok_or(PublisherRepositoryError::AttemptMissing { schedule_id: id, attempt_number })?;
        Ok(attempt_from_row(
            &row,
            None,
            status,
            error_class.map(str::to_owned),
            error_message.map(str::to_owned),
            response_json,
        ))
    }

    pub async fn complete_publication_attempt(
        &self,
        id: Uuid,
        attempt_number: i32,
        telegram_message_id: i64,
        caption_snapshot: Option<String>,
        response_json: Option<serde_json::Value>,
    ) -> Result<PublicationCompletion, PublisherRepositoryError> {
        if telegram_message_id <= 0 {
            return Err(PublisherRepositoryError::InvalidTelegramMessageId(telegram_message_id));
        }
        let mut transaction = self.pool.begin().await?;
        let current = sqlx::query_as::<_, PostRow>("SELECT * FROM posts WHERE id = $1 FOR UPDATE")
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(PublisherRepositoryError::AttemptMissing { schedule_id: id, attempt_number })?;
        if current.state == "published" {
            if current.send_generation != attempt_number
                || current.telegram_message_id != Some(telegram_message_id)
                || current.caption != caption_snapshot
            {
                return Err(PublisherRepositoryError::PublishedPostConflict(id));
            }
            transaction.commit().await?;
            let attempt = attempt_from_row(
                &current,
                None,
                PublicationAttemptStatus::Succeeded,
                None,
                None,
                response_json,
            );
            let published = published_from_row(
                &current,
                self.channel_chat_id(current.channel_id).await?,
                caption_snapshot,
            )?;
            return Ok(PublicationCompletion { attempt, published_post: published });
        }
        if current.state != "sending" || current.send_generation != attempt_number {
            return Err(PublisherRepositoryError::AttemptMissing {
                schedule_id: id,
                attempt_number,
            });
        }
        if current.caption != caption_snapshot {
            return Err(PublisherRepositoryError::PublishedPostConflict(id));
        }
        let row = sqlx::query_as::<_, PostRow>("UPDATE posts SET state = 'published', telegram_message_id = $2, published_at = COALESCE(published_at, now()), send_token = NULL, send_started_at = NULL, error_class = NULL, error_message = NULL, updated_at = now() WHERE id = $1 AND send_generation = $3 AND state = 'sending' RETURNING *")
            .bind(id).bind(telegram_message_id).bind(attempt_number).fetch_one(&mut *transaction).await?;
        transaction.commit().await?;
        let attempt = attempt_from_row(
            &row,
            None,
            PublicationAttemptStatus::Succeeded,
            None,
            None,
            response_json,
        );
        let published = published_from_row(
            &row,
            self.channel_chat_id(row.channel_id).await?,
            caption_snapshot,
        )?;
        Ok(PublicationCompletion { attempt, published_post: published })
    }

    async fn channel_chat_id(&self, id: Uuid) -> Result<i64, PublisherRepositoryError> {
        sqlx::query_scalar::<_, i64>("SELECT telegram_chat_id FROM channels WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or(PublisherRepositoryError::TargetChannelMissing(id))
    }
}

async fn validate_draft(
    pool: &PgPool,
    draft: &NewPostDraft,
) -> Result<(), PublisherRepositoryError> {
    if draft.content_item_id != draft.asset_id {
        return Err(PublisherRepositoryError::AssetContentMismatch {
            content_item_id: draft.content_item_id,
            asset_id: draft.asset_id,
        });
    }
    let media_exists =
        sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM media WHERE id = $1)")
            .bind(draft.content_item_id)
            .fetch_one(pool)
            .await?;
    if !media_exists {
        return Err(PublisherRepositoryError::ContentItemMissing(draft.content_item_id));
    }
    let channel = sqlx::query_as::<_, ChannelRow>("SELECT * FROM channels WHERE id = $1")
        .bind(draft.target_channel_id)
        .fetch_optional(pool)
        .await?
        .ok_or(PublisherRepositoryError::TargetChannelMissing(draft.target_channel_id))?;
    if !channel.is_enabled {
        return Err(PublisherRepositoryError::TargetChannelDisabled(channel.id));
    }
    Ok(())
}

async fn update_draft_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    id: Uuid,
    update: PostDraftUpdate,
) -> Result<PostDraft, PublisherRepositoryError> {
    let row = sqlx::query_as::<_, PostRow>("SELECT * FROM posts WHERE id = $1 FOR UPDATE")
        .bind(id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(PublisherRepositoryError::PostDraftMissing(id))?;
    let mut draft = row.clone().into_draft()?;
    if update.expected_updated_at.is_some_and(|expected| expected != draft.updated_at) {
        return Err(PublisherRepositoryError::OptimisticConflict(id));
    }
    if let Some(caption) = update.caption {
        draft.caption = caption;
    }
    if let Some(parse_mode) = update.parse_mode {
        draft.parse_mode = parse_mode;
    }
    if let Some(status) = update.status {
        draft.status = transition_post_draft_status(draft.status, status)?;
    }
    let state = match draft.status {
        PostDraftStatus::Cancelled => "cancelled",
        _ => "draft",
    };
    let updated = sqlx::query_as::<_, PostRow>("UPDATE posts SET caption = $2, parse_mode = $3, state = $4, updated_at = now() WHERE id = $1 RETURNING *")
        .bind(id).bind(draft.caption).bind(draft.parse_mode).bind(state).fetch_one(&mut **tx).await?;
    updated.into_draft()
}

async fn enqueue_publish_job(
    connection: &mut sqlx::PgConnection,
    post_id: Uuid,
    run_at: OffsetDateTime,
) -> Result<(), PublisherRepositoryError> {
    sqlx::query("INSERT INTO queue.jobs (kind, payload, state, run_at, dedupe_key) VALUES ('publish_post', $1, 'queued', $2, $3) ON CONFLICT (dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING")
        .bind(serde_json::json!({ "post_id": post_id })).bind(run_at).bind(format!("post:{post_id}:publish:v1"))
        .execute(&mut *connection).await?;
    Ok(())
}

impl ChannelRow {
    fn into_channel(self) -> Result<TargetChannel, PublisherRepositoryError> {
        Ok(TargetChannel {
            id: self.id,
            name: self.name,
            telegram_chat_id: self.telegram_chat_id,
            is_enabled: self.is_enabled,
            default_parse_mode: self.default_parse_mode,
            default_disable_notification: self.default_disable_notification,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}

impl PostRow {
    fn into_draft(self) -> Result<PostDraft, PublisherRepositoryError> {
        Ok(PostDraft {
            id: self.id,
            content_item_id: self.media_id,
            asset_id: self.media_id,
            target_channel_id: self.channel_id,
            caption: self.caption,
            parse_mode: self.parse_mode,
            status: domain_draft_status(&self.state),
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
    fn into_schedule(self) -> Result<PublicationSchedule, PublisherRepositoryError> {
        Ok(PublicationSchedule {
            id: self.id,
            post_draft_id: self.id,
            status: domain_schedule_status(&self.state),
            publish_at: self.scheduled_at,
            not_before: None,
            not_after: None,
            priority: 0,
            cooldown_override: None,
            idempotency_key: self.request_key.unwrap_or_else(|| self.id.to_string()),
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}

fn domain_draft_status(state: &str) -> PostDraftStatus {
    match state {
        "published" => PostDraftStatus::Published,
        "cancelled" => PostDraftStatus::Cancelled,
        _ => PostDraftStatus::Editing,
    }
}
fn domain_schedule_status(state: &str) -> PublicationScheduleStatus {
    match state {
        "queued" => PublicationScheduleStatus::Queued,
        "sending" => PublicationScheduleStatus::Publishing,
        "published" => PublicationScheduleStatus::Published,
        "failed" => PublicationScheduleStatus::Failed,
        "unknown" => PublicationScheduleStatus::Unknown,
        "cancelled" => PublicationScheduleStatus::Cancelled,
        _ => PublicationScheduleStatus::Pending,
    }
}
fn db_post_state(status: PublicationScheduleStatus) -> &'static str {
    match status {
        PublicationScheduleStatus::Pending => "draft",
        PublicationScheduleStatus::Queued => "queued",
        PublicationScheduleStatus::Publishing => "sending",
        PublicationScheduleStatus::Published => "published",
        PublicationScheduleStatus::Failed => "failed",
        PublicationScheduleStatus::Unknown => "unknown",
        PublicationScheduleStatus::Cancelled => "cancelled",
    }
}
fn domain_storage_state(value: &str) -> StorageState {
    match value {
        "ready" => StorageState::Uploaded,
        "missing" | "storage_unknown" => StorageState::Missing,
        _ => StorageState::Local,
    }
}
fn policy_from_channel(channel: &TargetChannel, requested: &NewChannelPolicy) -> ChannelPolicy {
    ChannelPolicy {
        target_channel_id: channel.id,
        minimum_post_interval_seconds: requested.minimum_post_interval_seconds,
        same_content_cooldown_seconds: requested.same_content_cooldown_seconds,
        similar_content_cooldown_seconds: requested.similar_content_cooldown_seconds,
        similarity_threshold: requested.similarity_threshold,
        on_cooldown_violation: requested.on_cooldown_violation,
        allowed_windows_json: requested.allowed_windows_json.clone(),
        max_posts_per_day: requested.max_posts_per_day,
        jitter_seconds: requested.jitter_seconds,
        updated_at: channel.updated_at,
    }
}
fn normalize_key(value: String) -> Result<String, PublisherRepositoryError> {
    let value = value.trim().to_owned();
    if value.is_empty() {
        return Err(PublisherRepositoryError::Validation(
            PublisherValidationError::EmptyIdempotencyKey,
        ));
    }
    if value.chars().count() > 255 {
        return Err(PublisherRepositoryError::Validation(
            PublisherValidationError::IdempotencyKeyTooLong { max: 255 },
        ));
    }
    Ok(value)
}
fn attempt_from_row(
    row: &PostRow,
    key: Option<String>,
    status: PublicationAttemptStatus,
    error_class: Option<String>,
    error_message: Option<String>,
    response_json: Option<serde_json::Value>,
) -> PublicationAttempt {
    PublicationAttempt {
        id: row.id,
        publication_schedule_id: row.id,
        attempt_number: row.send_generation,
        status,
        started_at: row.send_started_at.unwrap_or(row.updated_at),
        finished_at: (status != PublicationAttemptStatus::Running).then_some(row.updated_at),
        telegram_request_key: key,
        error_class,
        error_message,
        response_json,
    }
}
fn published_from_row(
    row: &PostRow,
    chat_id: i64,
    caption: Option<String>,
) -> Result<PublishedPost, PublisherRepositoryError> {
    Ok(PublishedPost {
        id: row.id,
        publication_schedule_id: row.id,
        content_item_id: row.media_id,
        asset_id: row.media_id,
        target_channel_id: row.channel_id,
        telegram_chat_id: chat_id,
        telegram_message_id: row
            .telegram_message_id
            .ok_or(PublisherRepositoryError::PublishedPostMissing(row.id))?,
        caption_snapshot: caption.or_else(|| row.caption.clone()),
        published_at: row.published_at.unwrap_or(row.updated_at),
        status: PublishedPostStatus::Active,
    })
}

pub fn post_draft_create_request_hash(
    content_item_id: Uuid,
    target_channel_id: Uuid,
    caption: Option<&str>,
    parse_mode: Option<&str>,
) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(content_item_id.as_bytes());
    hasher.update(target_channel_id.as_bytes());
    hash_optional(&mut hasher, caption);
    hash_optional(&mut hasher, parse_mode);
    hasher.finalize().to_vec()
}
pub fn post_draft_update_request_hash(id: Uuid, update: &PostDraftUpdate) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(id.as_bytes());
    match &update.caption {
        None => hasher.update([0]),
        Some(None) => hasher.update([1, 0]),
        Some(Some(value)) => {
            hasher.update([1, 1]);
            hasher.update(value.as_bytes());
        }
    }
    match &update.parse_mode {
        None => hasher.update([0]),
        Some(None) => hasher.update([1, 0]),
        Some(Some(value)) => {
            hasher.update([1, 1]);
            hasher.update(value.as_bytes());
        }
    }
    if let Some(status) = update.status {
        hasher.update(status.as_str().as_bytes());
    }
    if let Some(timestamp) = update.expected_updated_at {
        hasher.update(timestamp.unix_timestamp_nanos().to_be_bytes());
    }
    hasher.finalize().to_vec()
}
fn hash_optional(hasher: &mut Sha256, value: Option<&str>) {
    if let Some(value) = value {
        hasher.update([1]);
        hasher.update(value.as_bytes());
    } else {
        hasher.update([0]);
    }
}

#[derive(Debug, Error)]
pub enum PublisherRepositoryError {
    #[error("publisher validation failed: {0}")]
    Validation(#[from] PublisherValidationError),
    #[error("database returned an unknown {field} value: {value}")]
    InvalidEnum { field: &'static str, value: String },
    #[error("publisher number {field} does not fit the database type")]
    NumberOverflow { field: &'static str },
    #[error("target channel {0} was not found")]
    TargetChannelMissing(Uuid),
    #[error("target channel {0} is disabled")]
    TargetChannelDisabled(Uuid),
    #[error("content item {0} was not found")]
    ContentItemMissing(Uuid),
    #[error("content item {id} is not publishable in status {status:?}")]
    ContentItemNotPublishable { id: Uuid, status: ContentStatus },
    #[error("post draft {0} was not found")]
    PostDraftMissing(Uuid),
    #[error("publication schedule {0} was not found")]
    ScheduleMissing(Uuid),
    #[error("publication attempt {schedule_id}/{attempt_number} was not found or already finished")]
    AttemptMissing { schedule_id: Uuid, attempt_number: i32 },
    #[error("post draft {id} is not ready for scheduling; current status is {status:?}")]
    DraftNotReady { id: Uuid, status: PostDraftStatus },
    #[error("publication schedule idempotency key conflicts with another request: {0}")]
    ScheduleIdempotencyConflict(String),
    #[error("post draft idempotency key conflicts with another request: {0}")]
    DraftIdempotencyConflict(String),
    #[error("idempotency record is missing its resource")]
    IncompleteIdempotencyRecord,
    #[error("post draft {0} was updated by another request")]
    OptimisticConflict(Uuid),
    #[error("asset {asset_id} does not belong to content item {content_item_id}")]
    AssetContentMismatch { content_item_id: Uuid, asset_id: Uuid },
    #[error("asset {asset_id} is not the canonical asset for content item {content_item_id}")]
    CanonicalAssetMismatch { content_item_id: Uuid, asset_id: Uuid },
    #[error("asset {asset_id} is not publishable in storage state {storage_state:?}")]
    AssetNotPublishable { asset_id: Uuid, storage_state: StorageState },
    #[error("publication schedule {id} cannot transition from status {status:?}")]
    InvalidScheduleState { id: Uuid, status: PublicationScheduleStatus },
    #[error("publication schedule {id} requires its attempt operation to transition to {target:?}")]
    ManagedScheduleTransitionRequired { id: Uuid, target: PublicationScheduleStatus },
    #[error("publication attempt must finish with a terminal status")]
    AttemptMustFinish,
    #[error("successful publication must atomically record its Telegram message")]
    PublicationCompletionRequired,
    #[error("publication schedule {schedule_id} already has running attempt {attempt_number}")]
    AttemptAlreadyRunning { schedule_id: Uuid, attempt_number: i32 },
    #[error("publication limit must be between 1 and 100, got {0}")]
    InvalidLimit(u32),
    #[error("Telegram message ID must be positive, got {0}")]
    InvalidTelegramMessageId(i64),
    #[error("published post for schedule {0} was not found")]
    PublishedPostMissing(Uuid),
    #[error("published post replay conflicts with the existing Telegram message for schedule {0}")]
    PublishedPostConflict(Uuid),
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("publisher response serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
}
