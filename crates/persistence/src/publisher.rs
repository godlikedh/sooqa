use serde_json::Value;
use sha2::{Digest, Sha256};
use sooqa_jobs::NewJob;
use sooqa_library::{ContentStatus, StorageState};
use sooqa_publisher::{
    ChannelPolicy, NewChannelPolicy, NewPostDraft, NewPublicationSchedule, NewTargetChannel,
    PostDraft, PostDraftStatus, PostDraftUpdate, PublicationAttempt, PublicationAttemptStatus,
    PublicationCompletion, PublicationSchedule, PublicationScheduleScope,
    PublicationScheduleStatus, PublishedPost, PublisherValidationError, TargetChannel,
    next_cadence_eligible_at, publication_job_idempotency_key, transition_post_draft_status,
    transition_publication_schedule_status,
};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

const DRAFT_CREATE_IDEMPOTENCY_SCOPE: &str = "publisher:draft:create";
const DRAFT_UPDATE_IDEMPOTENCY_SCOPE: &str = "publisher:draft:update";

#[derive(Clone)]
pub struct PublisherRepository {
    pool: PgPool,
}

#[derive(Debug, Clone)]
pub struct CreatePostDraftResult {
    pub draft: PostDraft,
    pub created: bool,
}

impl PublisherRepository {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create_target_channel(
        &self,
        new_channel: NewTargetChannel,
    ) -> Result<TargetChannel, PublisherRepositoryError> {
        let row = sqlx::query_as::<_, TargetChannelRow>(
            r#"
            INSERT INTO target_channels (
                name, telegram_chat_id, default_parse_mode, default_disable_notification
            )
            VALUES ($1, $2, $3, $4)
            RETURNING id, name, telegram_chat_id, is_enabled, default_parse_mode,
                      default_disable_notification, created_at, updated_at
            "#,
        )
        .bind(new_channel.name)
        .bind(new_channel.telegram_chat_id)
        .bind(new_channel.default_parse_mode)
        .bind(new_channel.default_disable_notification)
        .fetch_one(&self.pool)
        .await?;
        row.into_target_channel()
    }

    pub async fn find_target_channel(
        &self,
        id: Uuid,
    ) -> Result<Option<TargetChannel>, PublisherRepositoryError> {
        let row = sqlx::query_as::<_, TargetChannelRow>(
            r#"
            SELECT id, name, telegram_chat_id, is_enabled, default_parse_mode,
                   default_disable_notification, created_at, updated_at
            FROM target_channels
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(TargetChannelRow::into_target_channel).transpose()
    }

    pub async fn list_target_channels(
        &self,
        enabled_only: bool,
    ) -> Result<Vec<TargetChannel>, PublisherRepositoryError> {
        let rows = if enabled_only {
            sqlx::query_as::<_, TargetChannelRow>(
                r#"
                SELECT id, name, telegram_chat_id, is_enabled, default_parse_mode,
                       default_disable_notification, created_at, updated_at
                FROM target_channels
                WHERE is_enabled
                ORDER BY name, id
                "#,
            )
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as::<_, TargetChannelRow>(
                r#"
                SELECT id, name, telegram_chat_id, is_enabled, default_parse_mode,
                       default_disable_notification, created_at, updated_at
                FROM target_channels
                ORDER BY name, id
                "#,
            )
            .fetch_all(&self.pool)
            .await?
        };
        rows.into_iter().map(TargetChannelRow::into_target_channel).collect()
    }

    pub async fn set_target_channel_enabled(
        &self,
        id: Uuid,
        is_enabled: bool,
    ) -> Result<TargetChannel, PublisherRepositoryError> {
        let row = sqlx::query_as::<_, TargetChannelRow>(
            r#"
            UPDATE target_channels
            SET is_enabled = $2, updated_at = now()
            WHERE id = $1
            RETURNING id, name, telegram_chat_id, is_enabled, default_parse_mode,
                      default_disable_notification, created_at, updated_at
            "#,
        )
        .bind(id)
        .bind(is_enabled)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(PublisherRepositoryError::TargetChannelMissing(id))?;
        row.into_target_channel()
    }

    pub async fn upsert_channel_policy(
        &self,
        policy: NewChannelPolicy,
    ) -> Result<ChannelPolicy, PublisherRepositoryError> {
        policy.validate()?;
        let row = sqlx::query_as::<_, ChannelPolicyRow>(
            r#"
            INSERT INTO channel_policies (
                target_channel_id, minimum_post_interval_seconds,
                same_content_cooldown_seconds, similar_content_cooldown_seconds,
                similarity_threshold, on_cooldown_violation, allowed_windows_json,
                max_posts_per_day, jitter_seconds
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (target_channel_id) DO UPDATE SET
                minimum_post_interval_seconds = EXCLUDED.minimum_post_interval_seconds,
                same_content_cooldown_seconds = EXCLUDED.same_content_cooldown_seconds,
                similar_content_cooldown_seconds = EXCLUDED.similar_content_cooldown_seconds,
                similarity_threshold = EXCLUDED.similarity_threshold,
                on_cooldown_violation = EXCLUDED.on_cooldown_violation,
                allowed_windows_json = EXCLUDED.allowed_windows_json,
                max_posts_per_day = EXCLUDED.max_posts_per_day,
                jitter_seconds = EXCLUDED.jitter_seconds,
                updated_at = now()
            RETURNING target_channel_id, minimum_post_interval_seconds,
                      same_content_cooldown_seconds, similar_content_cooldown_seconds,
                      similarity_threshold, on_cooldown_violation, allowed_windows_json,
                      max_posts_per_day, jitter_seconds, updated_at
            "#,
        )
        .bind(policy.target_channel_id)
        .bind(to_i64(policy.minimum_post_interval_seconds, "minimum_post_interval_seconds")?)
        .bind(to_i64(policy.same_content_cooldown_seconds, "same_content_cooldown_seconds")?)
        .bind(to_i64(policy.similar_content_cooldown_seconds, "similar_content_cooldown_seconds")?)
        .bind(policy.similarity_threshold)
        .bind(policy.on_cooldown_violation.as_str())
        .bind(policy.allowed_windows_json)
        .bind(
            policy.max_posts_per_day.map(i32::try_from).transpose().map_err(|_| {
                PublisherRepositoryError::NumberOverflow { field: "max_posts_per_day" }
            })?,
        )
        .bind(to_i64(policy.jitter_seconds, "jitter_seconds")?)
        .fetch_one(&self.pool)
        .await?;
        row.into_channel_policy()
    }

    pub async fn find_channel_policy(
        &self,
        target_channel_id: Uuid,
    ) -> Result<Option<ChannelPolicy>, PublisherRepositoryError> {
        let row = sqlx::query_as::<_, ChannelPolicyRow>(
            r#"
            SELECT target_channel_id, minimum_post_interval_seconds,
                   same_content_cooldown_seconds, similar_content_cooldown_seconds,
                   similarity_threshold, on_cooldown_violation, allowed_windows_json,
                   max_posts_per_day, jitter_seconds, updated_at
            FROM channel_policies
            WHERE target_channel_id = $1
            "#,
        )
        .bind(target_channel_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(ChannelPolicyRow::into_channel_policy).transpose()
    }

    pub async fn create_post_draft(
        &self,
        new_draft: NewPostDraft,
    ) -> Result<PostDraft, PublisherRepositoryError> {
        ensure_asset_belongs_to_content(&self.pool, new_draft.content_item_id, new_draft.asset_id)
            .await?;
        let row = sqlx::query_as::<_, PostDraftRow>(
            r#"
            INSERT INTO post_drafts (
                content_item_id, asset_id, target_channel_id, caption, parse_mode
            )
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id, content_item_id, asset_id, target_channel_id, caption,
                      parse_mode, status, created_at, updated_at
            "#,
        )
        .bind(new_draft.content_item_id)
        .bind(new_draft.asset_id)
        .bind(new_draft.target_channel_id)
        .bind(new_draft.caption)
        .bind(new_draft.parse_mode)
        .fetch_one(&self.pool)
        .await?;
        row.into_post_draft()
    }

    pub async fn create_post_draft_idempotent(
        &self,
        new_draft: NewPostDraft,
        idempotency_key: impl Into<String>,
        request_hash: &[u8],
    ) -> Result<CreatePostDraftResult, PublisherRepositoryError> {
        let idempotency_key = normalize_idempotency_key(idempotency_key.into())?;
        let legacy_request_hash = post_draft_legacy_request_hash(&new_draft);
        let draft_id = Uuid::now_v7();
        let mut transaction = self.pool.begin().await?;
        let inserted = sqlx::query_scalar::<_, Uuid>(
            r#"
            INSERT INTO idempotency_records (
                scope, idempotency_key, request_hash, resource_type, resource_id,
                response_status, response_body
            )
            VALUES ($1, $2, $3, 'post_draft', $4, 201, $5)
            ON CONFLICT (scope, idempotency_key) DO NOTHING
            RETURNING id
            "#,
        )
        .bind(DRAFT_CREATE_IDEMPOTENCY_SCOPE)
        .bind(&idempotency_key)
        .bind(request_hash)
        .bind(draft_id)
        .bind(serde_json::json!({ "id": draft_id, "status": "editing" }))
        .fetch_optional(&mut *transaction)
        .await?;

        if inserted.is_none() {
            let existing = sqlx::query_as::<_, IdempotencyResourceRow>(
                r#"
                SELECT request_hash, resource_id, response_body
                FROM idempotency_records
                WHERE scope = $1 AND idempotency_key = $2
                FOR UPDATE
                "#,
            )
            .bind(DRAFT_CREATE_IDEMPOTENCY_SCOPE)
            .bind(&idempotency_key)
            .fetch_one(&mut *transaction)
            .await?;
            if existing.request_hash.as_slice() != request_hash {
                if existing.request_hash.as_slice() != legacy_request_hash.as_slice() {
                    return Err(PublisherRepositoryError::DraftIdempotencyConflict(
                        idempotency_key,
                    ));
                }
                let draft = idempotency_draft_snapshot(&existing)?;
                transaction.commit().await?;
                return Ok(CreatePostDraftResult { draft, created: false });
            }
            let draft = idempotency_draft_snapshot(&existing)?;
            transaction.commit().await?;
            return Ok(CreatePostDraftResult { draft, created: false });
        }

        ensure_publishable_asset(&mut transaction, new_draft.content_item_id, new_draft.asset_id)
            .await?;
        ensure_target_channel_enabled(&mut transaction, new_draft.target_channel_id).await?;
        let row = sqlx::query_as::<_, PostDraftRow>(
            r#"
            INSERT INTO post_drafts (
                id, content_item_id, asset_id, target_channel_id, caption, parse_mode
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id, content_item_id, asset_id, target_channel_id, caption,
                      parse_mode, status, created_at, updated_at
            "#,
        )
        .bind(draft_id)
        .bind(new_draft.content_item_id)
        .bind(new_draft.asset_id)
        .bind(new_draft.target_channel_id)
        .bind(new_draft.caption)
        .bind(new_draft.parse_mode)
        .fetch_one(&mut *transaction)
        .await?
        .into_post_draft()?;
        let response_body = serde_json::to_value(&row)?;
        sqlx::query(
            "UPDATE idempotency_records SET response_body = $3 WHERE scope = $1 AND idempotency_key = $2",
        )
        .bind(DRAFT_CREATE_IDEMPOTENCY_SCOPE)
        .bind(&idempotency_key)
        .bind(response_body)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(CreatePostDraftResult { draft: row, created: true })
    }

    pub async fn replay_post_draft_create(
        &self,
        idempotency_key: &str,
        request_hash: &[u8],
        content_item_id: Uuid,
        target_channel_id: Uuid,
        caption: Option<&str>,
        parse_mode: Option<&str>,
    ) -> Result<Option<PostDraft>, PublisherRepositoryError> {
        let idempotency_key = normalize_idempotency_key(idempotency_key.to_owned())?;
        let mut transaction = self.pool.begin().await?;
        let Some(existing) = sqlx::query_as::<_, IdempotencyResourceRow>(
            r#"
            SELECT request_hash, resource_id, response_body
            FROM idempotency_records
            WHERE scope = $1 AND idempotency_key = $2
            FOR UPDATE
            "#,
        )
        .bind(DRAFT_CREATE_IDEMPOTENCY_SCOPE)
        .bind(&idempotency_key)
        .fetch_optional(&mut *transaction)
        .await?
        else {
            transaction.commit().await?;
            return Ok(None);
        };
        if existing.request_hash.as_slice() != request_hash {
            let draft = idempotency_draft_snapshot(&existing)?;
            if existing.request_hash.as_slice()
                != post_draft_legacy_snapshot_hash(&draft).as_slice()
            {
                return Err(PublisherRepositoryError::DraftIdempotencyConflict(idempotency_key));
            }
            if !legacy_create_request_matches(
                &draft,
                content_item_id,
                target_channel_id,
                caption,
                parse_mode,
            ) {
                return Err(PublisherRepositoryError::DraftIdempotencyConflict(idempotency_key));
            }
            transaction.commit().await?;
            return Ok(Some(draft));
        }
        let draft = idempotency_draft_snapshot(&existing)?;
        transaction.commit().await?;
        Ok(Some(draft))
    }

    pub async fn find_post_draft(
        &self,
        id: Uuid,
    ) -> Result<Option<PostDraft>, PublisherRepositoryError> {
        let row = sqlx::query_as::<_, PostDraftRow>(
            r#"
            SELECT id, content_item_id, asset_id, target_channel_id, caption,
                   parse_mode, status, created_at, updated_at
            FROM post_drafts
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(PostDraftRow::into_post_draft).transpose()
    }

    pub async fn update_post_draft(
        &self,
        id: Uuid,
        update: PostDraftUpdate,
    ) -> Result<PostDraft, PublisherRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let draft = update_post_draft_in_transaction(&mut transaction, id, update).await?;
        transaction.commit().await?;
        Ok(draft)
    }

    pub async fn update_post_draft_idempotent(
        &self,
        id: Uuid,
        update: PostDraftUpdate,
        idempotency_key: impl Into<String>,
    ) -> Result<PostDraft, PublisherRepositoryError> {
        let idempotency_key = normalize_idempotency_key(idempotency_key.into())?;
        let request_hash = post_draft_update_request_hash(id, &update);
        let mut transaction = self.pool.begin().await?;
        let inserted = sqlx::query_scalar::<_, Uuid>(
            r#"
            INSERT INTO idempotency_records (
                scope, idempotency_key, request_hash, resource_type, resource_id,
                response_status, response_body
            )
            VALUES ($1, $2, $3, 'post_draft', $4, 200, $5)
            ON CONFLICT (scope, idempotency_key) DO NOTHING
            RETURNING id
            "#,
        )
        .bind(DRAFT_UPDATE_IDEMPOTENCY_SCOPE)
        .bind(&idempotency_key)
        .bind(&request_hash)
        .bind(id)
        .bind(serde_json::json!({ "id": id }))
        .fetch_optional(&mut *transaction)
        .await?;

        if inserted.is_none() {
            let existing = sqlx::query_as::<_, IdempotencyResourceRow>(
                r#"
                SELECT request_hash, resource_id, response_body
                FROM idempotency_records
                WHERE scope = $1 AND idempotency_key = $2
                FOR UPDATE
                "#,
            )
            .bind(DRAFT_UPDATE_IDEMPOTENCY_SCOPE)
            .bind(&idempotency_key)
            .fetch_one(&mut *transaction)
            .await?;
            if existing.request_hash.as_slice() != request_hash.as_slice()
                || existing.resource_id != Some(id)
            {
                return Err(PublisherRepositoryError::DraftIdempotencyConflict(idempotency_key));
            }
            let draft = idempotency_draft_snapshot(&existing)?;
            transaction.commit().await?;
            return Ok(draft);
        }

        let draft = update_post_draft_in_transaction(&mut transaction, id, update).await?;
        let response_body = serde_json::to_value(&draft)?;
        sqlx::query(
            "UPDATE idempotency_records SET response_body = $3 WHERE scope = $1 AND idempotency_key = $2",
        )
        .bind(DRAFT_UPDATE_IDEMPOTENCY_SCOPE)
        .bind(&idempotency_key)
        .bind(response_body)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(draft)
    }

    pub async fn replay_post_draft_update(
        &self,
        idempotency_key: &str,
        request_hash: &[u8],
    ) -> Result<Option<PostDraft>, PublisherRepositoryError> {
        let idempotency_key = normalize_idempotency_key(idempotency_key.to_owned())?;
        let mut transaction = self.pool.begin().await?;
        let Some(existing) = sqlx::query_as::<_, IdempotencyResourceRow>(
            r#"
            SELECT request_hash, resource_id, response_body
            FROM idempotency_records
            WHERE scope = $1 AND idempotency_key = $2
            FOR UPDATE
            "#,
        )
        .bind(DRAFT_UPDATE_IDEMPOTENCY_SCOPE)
        .bind(&idempotency_key)
        .fetch_optional(&mut *transaction)
        .await?
        else {
            transaction.commit().await?;
            return Ok(None);
        };
        if existing.request_hash.as_slice() != request_hash {
            return Err(PublisherRepositoryError::DraftIdempotencyConflict(idempotency_key));
        }
        let draft = idempotency_draft_snapshot(&existing)?;
        transaction.commit().await?;
        Ok(Some(draft))
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
        scope: PublicationScheduleScope,
    ) -> Result<PublicationSchedule, PublisherRepositoryError> {
        let schedule = NewPublicationSchedule {
            publish_at: truncate_to_microseconds(schedule.publish_at),
            not_before: schedule.not_before.map(truncate_to_microseconds),
            not_after: schedule.not_after.map(truncate_to_microseconds),
            idempotency_key: schedule.idempotency_key.trim().to_owned(),
            ..schedule
        };
        schedule.validate()?;
        let mut transaction = self.pool.begin().await?;
        // A unique index can reject a duplicate only after the caller has
        // already changed the draft. Serialize requests for the same key
        // before checking the existing schedule so a concurrent replay returns
        // the original row instead of DraftNotReady.
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0::bigint))")
            .bind(format!("{}:{}", scope.as_str(), schedule.idempotency_key))
            .fetch_one(&mut *transaction)
            .await?;
        if let Some(existing) = sqlx::query_as::<_, PublicationScheduleRow>(
            r#"
            SELECT id, post_draft_id, status, publish_at, not_before, not_after,
                   priority, cooldown_override, idempotency_scope, idempotency_key,
                   created_at, updated_at
            FROM publication_schedules
            WHERE idempotency_scope = $1 AND idempotency_key = $2
            FOR UPDATE
            "#,
        )
        .bind(scope.as_str())
        .bind(&schedule.idempotency_key)
        .fetch_optional(&mut *transaction)
        .await?
        {
            if !schedule_request_matches(&existing, &schedule, scope) {
                return Err(PublisherRepositoryError::ScheduleIdempotencyConflict(
                    schedule.idempotency_key,
                ));
            }
            transaction.commit().await?;
            return existing.into_publication_schedule();
        }
        if let Some(existing) = sqlx::query_as::<_, PublicationScheduleRow>(
            r#"
            SELECT id, post_draft_id, status, publish_at, not_before, not_after,
                   priority, cooldown_override, idempotency_scope, idempotency_key,
                   created_at, updated_at
            FROM publication_schedules
            WHERE idempotency_scope = 'legacy' AND idempotency_key = $1
            FOR UPDATE
            "#,
        )
        .bind(&schedule.idempotency_key)
        .fetch_optional(&mut *transaction)
        .await?
        {
            if !schedule_request_matches(&existing, &schedule, scope) {
                return Err(PublisherRepositoryError::ScheduleIdempotencyConflict(
                    schedule.idempotency_key,
                ));
            }
            transaction.commit().await?;
            return existing.into_publication_schedule();
        }
        let draft = load_post_draft(&mut transaction, schedule.post_draft_id).await?;
        if draft.status != PostDraftStatus::Ready {
            return Err(PublisherRepositoryError::DraftNotReady {
                id: draft.id,
                status: draft.status,
            });
        }
        ensure_publishable_asset(&mut transaction, draft.content_item_id, draft.asset_id).await?;
        ensure_target_channel_enabled(&mut transaction, draft.target_channel_id).await?;
        let inserted = sqlx::query_as::<_, PublicationScheduleRow>(
            r#"
            INSERT INTO publication_schedules (
                post_draft_id, publish_at, not_before, not_after, priority,
                cooldown_override, idempotency_scope, idempotency_key
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (idempotency_scope, idempotency_key) DO NOTHING
            RETURNING id, post_draft_id, status, publish_at, not_before, not_after,
                      priority, cooldown_override, idempotency_scope, idempotency_key,
                      created_at, updated_at
            "#,
        )
        .bind(schedule.post_draft_id)
        .bind(schedule.publish_at)
        .bind(schedule.not_before)
        .bind(schedule.not_after)
        .bind(schedule.priority)
        .bind(schedule.cooldown_override)
        .bind(scope.as_str())
        .bind(&schedule.idempotency_key)
        .fetch_optional(&mut *transaction)
        .await?;
        let row = if let Some(inserted) = inserted {
            let updated = sqlx::query_as::<_, PostDraftRow>(
                r#"
                UPDATE post_drafts
                SET status = 'scheduled', updated_at = now()
                WHERE id = $1 AND status = 'ready'
                RETURNING id, content_item_id, asset_id, target_channel_id, caption,
                          parse_mode, status, created_at, updated_at
                "#,
            )
            .bind(schedule.post_draft_id)
            .fetch_optional(&mut *transaction)
            .await?;
            if updated.is_none() {
                return Err(PublisherRepositoryError::DraftNotReady {
                    id: draft.id,
                    status: draft.status,
                });
            }
            inserted
        } else {
            let existing = sqlx::query_as::<_, PublicationScheduleRow>(
                r#"
                SELECT id, post_draft_id, status, publish_at, not_before, not_after,
                       priority, cooldown_override, idempotency_scope, idempotency_key,
                       created_at, updated_at
                FROM publication_schedules
                WHERE idempotency_scope = $1 AND idempotency_key = $2
                FOR UPDATE
                "#,
            )
            .bind(scope.as_str())
            .bind(&schedule.idempotency_key)
            .fetch_one(&mut *transaction)
            .await?;
            if !schedule_request_matches(&existing, &schedule, scope) {
                return Err(PublisherRepositoryError::ScheduleIdempotencyConflict(
                    schedule.idempotency_key,
                ));
            }
            existing
        };
        transaction.commit().await?;
        row.into_publication_schedule()
    }

    pub async fn find_publication_schedule(
        &self,
        id: Uuid,
    ) -> Result<Option<PublicationSchedule>, PublisherRepositoryError> {
        let row = sqlx::query_as::<_, PublicationScheduleRow>(
            r#"
            SELECT id, post_draft_id, status, publish_at, not_before, not_after,
                   priority, cooldown_override, idempotency_scope, idempotency_key,
                   created_at, updated_at
            FROM publication_schedules
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(PublicationScheduleRow::into_publication_schedule).transpose()
    }

    pub async fn list_due_publication_schedules(
        &self,
        now: OffsetDateTime,
        limit: u32,
    ) -> Result<Vec<PublicationSchedule>, PublisherRepositoryError> {
        if !(1..=100).contains(&limit) {
            return Err(PublisherRepositoryError::InvalidLimit(limit));
        }
        let rows = sqlx::query_as::<_, PublicationScheduleRow>(
            r#"
            SELECT id, post_draft_id, status, publish_at, not_before, not_after,
                   priority, cooldown_override, idempotency_scope, idempotency_key,
                   created_at, updated_at
            FROM publication_schedules
            WHERE status IN ('pending', 'queued', 'failed')
              AND publish_at <= $1
              AND (not_before IS NULL OR not_before <= $1)
              AND (not_after IS NULL OR not_after >= $1)
            ORDER BY priority DESC, publish_at ASC, created_at ASC, id ASC
            LIMIT $2
            "#,
        )
        .bind(now)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(PublicationScheduleRow::into_publication_schedule).collect()
    }

    /// Atomically claim due schedules and create their deterministic publish
    /// jobs. The schedule row lock is the scheduler lease: concurrent server
    /// instances skip rows already claimed by another transaction.
    pub async fn enqueue_due_publication_jobs(
        &self,
        now: OffsetDateTime,
        limit: u32,
    ) -> Result<Vec<PublicationSchedule>, PublisherRepositoryError> {
        if !(1..=100).contains(&limit) {
            return Err(PublisherRepositoryError::InvalidLimit(limit));
        }

        let mut transaction = self.pool.begin().await?;
        let schedule_ids = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT id
            FROM publication_schedules
            WHERE status = 'pending'
              AND publish_at <= $1
              AND (not_before IS NULL OR not_before <= $1)
              AND (not_after IS NULL OR not_after >= $1)
            ORDER BY priority DESC, publish_at ASC, created_at ASC, id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT $2
            "#,
        )
        .bind(now)
        .bind(i64::from(limit))
        .fetch_all(&mut *transaction)
        .await?;

        let mut enqueued = Vec::new();
        for schedule_id in schedule_ids {
            let schedule = load_publication_schedule(&mut transaction, schedule_id).await?;
            let draft = load_post_draft(&mut transaction, schedule.post_draft_id).await?;
            if draft.status != PostDraftStatus::Scheduled {
                continue;
            }

            let Some(target) =
                load_target_channel(&mut transaction, draft.target_channel_id).await?
            else {
                continue;
            };
            if !target.is_enabled {
                continue;
            }

            let policy = load_channel_policy(&mut transaction, target.id).await?;
            if let Some(policy) = policy {
                let cadence = load_publication_cadence(&mut transaction, target.id, now).await?;
                let published_today = u32::try_from(cadence.published_today).map_err(|_| {
                    PublisherRepositoryError::NumberOverflow { field: "published_posts_today" }
                })?;
                if let Some(next_eligible_at) = next_cadence_eligible_at(
                    now,
                    cadence.last_published_at,
                    published_today,
                    &policy,
                ) {
                    if next_eligible_at > schedule.publish_at
                        && schedule.not_after.is_none_or(|not_after| next_eligible_at <= not_after)
                    {
                        sqlx::query(
                            "UPDATE publication_schedules SET publish_at = $2, updated_at = now() WHERE id = $1 AND status = 'pending'",
                        )
                        .bind(schedule.id)
                        .bind(next_eligible_at)
                        .execute(&mut *transaction)
                        .await?;
                    }
                    continue;
                }
            }

            let job = NewJob::publish_post(schedule.id)
                .with_priority(schedule.priority)
                .available_at(now)
                .idempotency_key(publication_job_idempotency_key(schedule.id));
            sqlx::query(
                r#"
                INSERT INTO jobs (
                    job_type, payload_json, priority, available_at, max_attempts, idempotency_key
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
                "#,
            )
            .bind(job.job_type().as_str())
            .bind(job.payload_json())
            .bind(job.priority())
            .bind(job.available_at_value())
            .bind(job.max_attempts_value())
            .bind(job.idempotency_key_value())
            .execute(&mut *transaction)
            .await?;

            let queued = sqlx::query_as::<_, PublicationScheduleRow>(
                r#"
                UPDATE publication_schedules
                SET status = 'queued', updated_at = now()
                WHERE id = $1 AND status = 'pending'
                RETURNING id, post_draft_id, status, publish_at, not_before, not_after,
                          priority, cooldown_override, idempotency_scope, idempotency_key,
                          created_at, updated_at
                "#,
            )
            .bind(schedule.id)
            .fetch_one(&mut *transaction)
            .await?;
            enqueued.push(queued.into_publication_schedule()?);
        }

        transaction.commit().await?;
        Ok(enqueued)
    }

    pub async fn transition_publication_schedule(
        &self,
        id: Uuid,
        target: PublicationScheduleStatus,
    ) -> Result<PublicationSchedule, PublisherRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let mut schedule = load_publication_schedule(&mut transaction, id).await?;
        if schedule.status == PublicationScheduleStatus::Publishing
            || matches!(
                target,
                PublicationScheduleStatus::Publishing | PublicationScheduleStatus::Published
            )
        {
            return Err(PublisherRepositoryError::ManagedScheduleTransitionRequired { id, target });
        }
        schedule.status = transition_publication_schedule_status(schedule.status, target)?;
        schedule.updated_at = OffsetDateTime::now_utc();
        let row = sqlx::query_as::<_, PublicationScheduleRow>(
            r#"
            UPDATE publication_schedules
            SET status = $2, updated_at = $3
            WHERE id = $1
            RETURNING id, post_draft_id, status, publish_at, not_before, not_after,
                      priority, cooldown_override, idempotency_scope, idempotency_key,
                      created_at, updated_at
            "#,
        )
        .bind(id)
        .bind(schedule.status.as_str())
        .bind(schedule.updated_at)
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
        row.into_publication_schedule()
    }

    pub async fn start_publication_attempt(
        &self,
        schedule_id: Uuid,
        telegram_request_key: Option<String>,
    ) -> Result<PublicationAttempt, PublisherRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let mut schedule = load_publication_schedule(&mut transaction, schedule_id).await?;
        let draft = load_post_draft(&mut transaction, schedule.post_draft_id).await?;
        if draft.status != PostDraftStatus::Scheduled {
            return Err(PublisherRepositoryError::DraftNotReady {
                id: draft.id,
                status: draft.status,
            });
        }
        if schedule.status == PublicationScheduleStatus::Pending
            || schedule.status == PublicationScheduleStatus::Queued
            || schedule.status == PublicationScheduleStatus::Failed
        {
            schedule.status = transition_publication_schedule_status(
                schedule.status,
                PublicationScheduleStatus::Queued,
            )?;
            schedule.status = transition_publication_schedule_status(
                schedule.status,
                PublicationScheduleStatus::Publishing,
            )?;
            sqlx::query(
                "UPDATE publication_schedules SET status = 'publishing', updated_at = now() WHERE id = $1",
            )
            .bind(schedule_id)
            .execute(&mut *transaction)
            .await?;
        } else if schedule.status == PublicationScheduleStatus::Publishing {
            let running_attempt = sqlx::query_scalar::<_, i32>(
                "SELECT attempt_number FROM publication_attempts WHERE publication_schedule_id = $1 AND status = 'running' LIMIT 1 FOR UPDATE",
            )
            .bind(schedule_id)
            .fetch_optional(&mut *transaction)
            .await?;
            if let Some(attempt_number) = running_attempt {
                return Err(PublisherRepositoryError::AttemptAlreadyRunning {
                    schedule_id,
                    attempt_number,
                });
            }
        } else {
            return Err(PublisherRepositoryError::InvalidScheduleState {
                id: schedule_id,
                status: schedule.status,
            });
        }
        let attempt_number: i32 = sqlx::query_scalar(
            "SELECT COALESCE(max(attempt_number), 0) + 1 FROM publication_attempts WHERE publication_schedule_id = $1",
        )
        .bind(schedule_id)
        .fetch_one(&mut *transaction)
        .await?;
        let row = sqlx::query_as::<_, PublicationAttemptRow>(
            r#"
            INSERT INTO publication_attempts (
                publication_schedule_id, attempt_number, telegram_request_key
            )
            VALUES ($1, $2, $3)
            RETURNING id, publication_schedule_id, attempt_number, status, started_at,
                      finished_at, telegram_request_key, error_class, error_message, response_json
            "#,
        )
        .bind(schedule_id)
        .bind(attempt_number)
        .bind(telegram_request_key)
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
        row.into_publication_attempt()
    }

    pub async fn finish_publication_attempt(
        &self,
        schedule_id: Uuid,
        attempt_number: i32,
        status: PublicationAttemptStatus,
        error_class: Option<&str>,
        error_message: Option<&str>,
        response_json: Option<Value>,
    ) -> Result<PublicationAttempt, PublisherRepositoryError> {
        if status == PublicationAttemptStatus::Running {
            return Err(PublisherRepositoryError::AttemptMustFinish);
        }
        if status == PublicationAttemptStatus::Succeeded {
            return Err(PublisherRepositoryError::PublicationCompletionRequired);
        }
        let mut transaction = self.pool.begin().await?;
        let schedule = load_publication_schedule(&mut transaction, schedule_id).await?;
        if schedule.status != PublicationScheduleStatus::Publishing {
            return Err(PublisherRepositoryError::InvalidScheduleState {
                id: schedule_id,
                status: schedule.status,
            });
        }
        let row = sqlx::query_as::<_, PublicationAttemptRow>(
            r#"
            UPDATE publication_attempts
            SET status = $3, finished_at = now(), error_class = $4,
                error_message = $5, response_json = $6
            WHERE publication_schedule_id = $1
              AND attempt_number = $2
              AND status = 'running'
            RETURNING id, publication_schedule_id, attempt_number, status, started_at,
                      finished_at, telegram_request_key, error_class, error_message, response_json
            "#,
        )
        .bind(schedule_id)
        .bind(attempt_number)
        .bind(status.as_str())
        .bind(error_class)
        .bind(error_message)
        .bind(response_json)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PublisherRepositoryError::AttemptMissing { schedule_id, attempt_number })?;
        let schedule_status = match status {
            PublicationAttemptStatus::Failed => PublicationScheduleStatus::Failed,
            PublicationAttemptStatus::Unknown => PublicationScheduleStatus::Unknown,
            PublicationAttemptStatus::Running | PublicationAttemptStatus::Succeeded => {
                unreachable!("terminal status was validated before the transaction")
            }
        };
        sqlx::query(
            "UPDATE publication_schedules SET status = $2, updated_at = now() WHERE id = $1 AND status = 'publishing'",
        )
        .bind(schedule_id)
        .bind(schedule_status.as_str())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        row.into_publication_attempt()
    }

    pub async fn complete_publication_attempt(
        &self,
        schedule_id: Uuid,
        attempt_number: i32,
        telegram_message_id: i64,
        caption_snapshot: Option<String>,
        response_json: Option<Value>,
    ) -> Result<PublicationCompletion, PublisherRepositoryError> {
        if telegram_message_id <= 0 {
            return Err(PublisherRepositoryError::InvalidTelegramMessageId(telegram_message_id));
        }
        let mut transaction = self.pool.begin().await?;
        let schedule = load_publication_schedule(&mut transaction, schedule_id).await?;
        if schedule.status == PublicationScheduleStatus::Published {
            let attempt =
                load_publication_attempt(&mut transaction, schedule_id, attempt_number).await?;
            if attempt.status != PublicationAttemptStatus::Succeeded {
                return Err(PublisherRepositoryError::AttemptMissing {
                    schedule_id,
                    attempt_number,
                });
            }
            let published = load_published_post(&mut transaction, schedule_id)
                .await?
                .ok_or(PublisherRepositoryError::PublishedPostMissing(schedule_id))?;
            if published.telegram_message_id != telegram_message_id
                || published.caption_snapshot.as_ref() != caption_snapshot.as_ref()
            {
                return Err(PublisherRepositoryError::PublishedPostConflict(schedule_id));
            }
            transaction.commit().await?;
            return Ok(PublicationCompletion { attempt, published_post: published });
        }
        if schedule.status != PublicationScheduleStatus::Publishing {
            return Err(PublisherRepositoryError::InvalidScheduleState {
                id: schedule_id,
                status: schedule.status,
            });
        }
        let draft = load_post_draft(&mut transaction, schedule.post_draft_id).await?;
        if draft.status != PostDraftStatus::Scheduled {
            return Err(PublisherRepositoryError::DraftNotReady {
                id: draft.id,
                status: draft.status,
            });
        }
        let attempt = sqlx::query_as::<_, PublicationAttemptRow>(
            r#"
            UPDATE publication_attempts
            SET status = 'succeeded', finished_at = now(), response_json = $3
            WHERE publication_schedule_id = $1
              AND attempt_number = $2
              AND status = 'running'
            RETURNING id, publication_schedule_id, attempt_number, status, started_at,
                      finished_at, telegram_request_key, error_class, error_message, response_json
            "#,
        )
        .bind(schedule_id)
        .bind(attempt_number)
        .bind(response_json)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PublisherRepositoryError::AttemptMissing { schedule_id, attempt_number })?
        .into_publication_attempt()?;
        sqlx::query(
            "UPDATE publication_schedules SET status = 'published', updated_at = now() WHERE id = $1 AND status = 'publishing'",
        )
        .bind(schedule_id)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE post_drafts SET status = 'published', updated_at = now() WHERE id = $1 AND status = 'scheduled'",
        )
        .bind(draft.id)
        .execute(&mut *transaction)
        .await?;
        let published = insert_or_load_published_post(
            &mut transaction,
            schedule_id,
            telegram_message_id,
            caption_snapshot,
        )
        .await?;
        transaction.commit().await?;
        Ok(PublicationCompletion { attempt, published_post: published })
    }
}

async fn ensure_asset_belongs_to_content(
    pool: &PgPool,
    content_item_id: Uuid,
    asset_id: Uuid,
) -> Result<(), PublisherRepositoryError> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM media_assets WHERE id = $2 AND content_item_id = $1)",
    )
    .bind(content_item_id)
    .bind(asset_id)
    .fetch_one(pool)
    .await?;
    if !exists {
        return Err(PublisherRepositoryError::AssetContentMismatch { content_item_id, asset_id });
    }
    Ok(())
}

async fn ensure_publishable_asset(
    transaction: &mut Transaction<'_, Postgres>,
    content_item_id: Uuid,
    asset_id: Uuid,
) -> Result<(), PublisherRepositoryError> {
    let content = sqlx::query_as::<_, PublishableContentRow>(
        "SELECT status, canonical_asset_id FROM content_items WHERE id = $1 FOR UPDATE",
    )
    .bind(content_item_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(PublisherRepositoryError::ContentItemMissing(content_item_id))?;
    let status = parse_enum("content_items.status", &content.status)?;
    if status != ContentStatus::Active {
        return Err(PublisherRepositoryError::ContentItemNotPublishable {
            id: content_item_id,
            status,
        });
    }
    if content.canonical_asset_id != Some(asset_id) {
        return Err(PublisherRepositoryError::CanonicalAssetMismatch { content_item_id, asset_id });
    }
    let storage_state = sqlx::query_scalar::<_, String>(
        "SELECT storage_state FROM media_assets WHERE id = $1 AND content_item_id = $2 AND role = 'canonical' FOR UPDATE",
    )
    .bind(asset_id)
    .bind(content_item_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(PublisherRepositoryError::AssetContentMismatch {
        content_item_id,
        asset_id,
    })?;
    let storage_state = parse_enum("media_assets.storage_state", &storage_state)?;
    if storage_state != StorageState::Uploaded {
        return Err(PublisherRepositoryError::AssetNotPublishable { asset_id, storage_state });
    }
    Ok(())
}

async fn ensure_target_channel_enabled(
    transaction: &mut Transaction<'_, Postgres>,
    id: Uuid,
) -> Result<(), PublisherRepositoryError> {
    let enabled = sqlx::query_scalar::<_, bool>(
        "SELECT is_enabled FROM target_channels WHERE id = $1 FOR UPDATE",
    )
    .bind(id)
    .fetch_optional(&mut **transaction)
    .await?;
    match enabled {
        None => Err(PublisherRepositoryError::TargetChannelMissing(id)),
        Some(false) => Err(PublisherRepositoryError::TargetChannelDisabled(id)),
        Some(true) => Ok(()),
    }
}

async fn update_post_draft_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    id: Uuid,
    update: PostDraftUpdate,
) -> Result<PostDraft, PublisherRepositoryError> {
    let mut draft = load_post_draft(transaction, id).await?;
    if let Some(expected_updated_at) = update.expected_updated_at
        && draft.updated_at != expected_updated_at
    {
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
    if draft.status == PostDraftStatus::Ready {
        ensure_publishable_asset(transaction, draft.content_item_id, draft.asset_id).await?;
        ensure_target_channel_enabled(transaction, draft.target_channel_id).await?;
    }
    draft.updated_at = OffsetDateTime::now_utc();
    let row = sqlx::query_as::<_, PostDraftRow>(
        r#"
        UPDATE post_drafts
        SET caption = $2, parse_mode = $3, status = $4, updated_at = $5
        WHERE id = $1
        RETURNING id, content_item_id, asset_id, target_channel_id, caption,
                  parse_mode, status, created_at, updated_at
        "#,
    )
    .bind(draft.id)
    .bind(&draft.caption)
    .bind(&draft.parse_mode)
    .bind(draft.status.as_str())
    .bind(draft.updated_at)
    .fetch_one(&mut **transaction)
    .await?;
    row.into_post_draft()
}

async fn load_target_channel(
    transaction: &mut Transaction<'_, Postgres>,
    id: Uuid,
) -> Result<Option<TargetChannel>, PublisherRepositoryError> {
    let row = sqlx::query_as::<_, TargetChannelRow>(
        r#"
        SELECT id, name, telegram_chat_id, is_enabled, default_parse_mode,
               default_disable_notification, created_at, updated_at
        FROM target_channels
        WHERE id = $1
        FOR UPDATE
        "#,
    )
    .bind(id)
    .fetch_optional(&mut **transaction)
    .await?;
    row.map(TargetChannelRow::into_target_channel).transpose()
}

async fn load_channel_policy(
    transaction: &mut Transaction<'_, Postgres>,
    target_channel_id: Uuid,
) -> Result<Option<ChannelPolicy>, PublisherRepositoryError> {
    let row = sqlx::query_as::<_, ChannelPolicyRow>(
        r#"
        SELECT target_channel_id, minimum_post_interval_seconds,
               same_content_cooldown_seconds, similar_content_cooldown_seconds,
               similarity_threshold, on_cooldown_violation, allowed_windows_json,
               max_posts_per_day, jitter_seconds, updated_at
        FROM channel_policies
        WHERE target_channel_id = $1
        FOR UPDATE
        "#,
    )
    .bind(target_channel_id)
    .fetch_optional(&mut **transaction)
    .await?;
    row.map(ChannelPolicyRow::into_channel_policy).transpose()
}

async fn load_publication_cadence(
    transaction: &mut Transaction<'_, Postgres>,
    target_channel_id: Uuid,
    now: OffsetDateTime,
) -> Result<PublicationCadenceRow, PublisherRepositoryError> {
    let day_start = now.replace_time(time::Time::MIDNIGHT);
    Ok(sqlx::query_as::<_, PublicationCadenceRow>(
        r#"
        SELECT MAX(published_at) AS last_published_at,
               COUNT(*) FILTER (WHERE published_at >= $2)::bigint AS published_today
        FROM published_posts
        WHERE target_channel_id = $1
        "#,
    )
    .bind(target_channel_id)
    .bind(day_start)
    .fetch_one(&mut **transaction)
    .await?)
}

fn normalize_idempotency_key(key: String) -> Result<String, PublisherRepositoryError> {
    let key = key.trim().to_owned();
    if key.is_empty() {
        return Err(PublisherRepositoryError::Validation(
            PublisherValidationError::EmptyIdempotencyKey,
        ));
    }
    if key.chars().count() > 255 {
        return Err(PublisherRepositoryError::Validation(
            PublisherValidationError::IdempotencyKeyTooLong { max: 255 },
        ));
    }
    Ok(key)
}

fn schedule_request_matches(
    existing: &PublicationScheduleRow,
    requested: &NewPublicationSchedule,
    scope: PublicationScheduleScope,
) -> bool {
    if existing.post_draft_id != requested.post_draft_id {
        return false;
    }
    if scope == PublicationScheduleScope::PublishNow {
        return true;
    }
    existing.publish_at == requested.publish_at
        && existing.not_before == requested.not_before
        && existing.not_after == requested.not_after
        && existing.priority == requested.priority
        && existing.cooldown_override == requested.cooldown_override
}

pub fn post_draft_create_request_hash(
    content_item_id: Uuid,
    target_channel_id: Uuid,
    caption: Option<&str>,
    parse_mode: Option<&str>,
) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hash_uuid(&mut hasher, content_item_id);
    hash_uuid(&mut hasher, target_channel_id);
    hash_optional_string(&mut hasher, caption);
    hash_optional_string(&mut hasher, parse_mode);
    hasher.finalize().to_vec()
}

pub fn post_draft_update_request_hash(id: Uuid, update: &PostDraftUpdate) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hash_uuid(&mut hasher, id);
    hash_optional_optional_string(&mut hasher, &update.caption);
    hash_optional_optional_string(&mut hasher, &update.parse_mode);
    match update.status {
        Some(status) => {
            hasher.update([1]);
            hasher.update(status.as_str().as_bytes());
        }
        None => hasher.update([0]),
    }
    match update.expected_updated_at {
        Some(timestamp) => {
            hasher.update([1]);
            hasher.update(timestamp.unix_timestamp_nanos().to_be_bytes());
        }
        None => hasher.update([0]),
    }
    hasher.finalize().to_vec()
}

fn idempotency_draft_snapshot(
    record: &IdempotencyResourceRow,
) -> Result<PostDraft, PublisherRepositoryError> {
    let body = record
        .response_body
        .clone()
        .ok_or(PublisherRepositoryError::IncompleteIdempotencyRecord)?;
    Ok(serde_json::from_value(body)?)
}

fn legacy_create_request_matches(
    draft: &PostDraft,
    content_item_id: Uuid,
    target_channel_id: Uuid,
    caption: Option<&str>,
    parse_mode: Option<&str>,
) -> bool {
    draft.content_item_id == content_item_id
        && draft.target_channel_id == target_channel_id
        && draft.caption.as_deref() == caption
        && parse_mode.is_none_or(|parse_mode| draft.parse_mode.as_deref() == Some(parse_mode))
}

fn hash_uuid(hasher: &mut Sha256, value: Uuid) {
    hasher.update(value.as_bytes());
}

fn post_draft_legacy_request_hash(draft: &NewPostDraft) -> Vec<u8> {
    post_draft_legacy_hash(
        draft.content_item_id,
        draft.asset_id,
        draft.target_channel_id,
        draft.caption.as_deref(),
        draft.parse_mode.as_deref(),
    )
}

fn post_draft_legacy_snapshot_hash(draft: &PostDraft) -> Vec<u8> {
    post_draft_legacy_hash(
        draft.content_item_id,
        draft.asset_id,
        draft.target_channel_id,
        draft.caption.as_deref(),
        draft.parse_mode.as_deref(),
    )
}

fn post_draft_legacy_hash(
    content_item_id: Uuid,
    asset_id: Uuid,
    target_channel_id: Uuid,
    caption: Option<&str>,
    parse_mode: Option<&str>,
) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hash_uuid(&mut hasher, content_item_id);
    hash_uuid(&mut hasher, asset_id);
    hash_uuid(&mut hasher, target_channel_id);
    hash_optional_string(&mut hasher, caption);
    hash_optional_string(&mut hasher, parse_mode);
    hasher.finalize().to_vec()
}

fn hash_optional_string(hasher: &mut Sha256, value: Option<&str>) {
    match value {
        Some(value) => {
            hasher.update([1]);
            hasher.update((value.len() as u64).to_be_bytes());
            hasher.update(value.as_bytes());
        }
        None => hasher.update([0]),
    }
}

fn hash_optional_optional_string(hasher: &mut Sha256, value: &Option<Option<String>>) {
    match value {
        None => hasher.update([0]),
        Some(None) => hasher.update([1, 0]),
        Some(Some(value)) => {
            hasher.update([1, 1]);
            hasher.update((value.len() as u64).to_be_bytes());
            hasher.update(value.as_bytes());
        }
    }
}

async fn load_post_draft(
    transaction: &mut Transaction<'_, Postgres>,
    id: Uuid,
) -> Result<PostDraft, PublisherRepositoryError> {
    let row = sqlx::query_as::<_, PostDraftRow>(
        r#"
        SELECT id, content_item_id, asset_id, target_channel_id, caption,
               parse_mode, status, created_at, updated_at
        FROM post_drafts
        WHERE id = $1
        FOR UPDATE
        "#,
    )
    .bind(id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(PublisherRepositoryError::PostDraftMissing(id))?;
    row.into_post_draft()
}

async fn load_publication_schedule(
    transaction: &mut Transaction<'_, Postgres>,
    id: Uuid,
) -> Result<PublicationSchedule, PublisherRepositoryError> {
    let row = sqlx::query_as::<_, PublicationScheduleRow>(
        r#"
        SELECT id, post_draft_id, status, publish_at, not_before, not_after,
               priority, cooldown_override, idempotency_scope, idempotency_key,
               created_at, updated_at
        FROM publication_schedules
        WHERE id = $1
        FOR UPDATE
        "#,
    )
    .bind(id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(PublisherRepositoryError::ScheduleMissing(id))?;
    row.into_publication_schedule()
}

async fn load_publication_attempt(
    transaction: &mut Transaction<'_, Postgres>,
    schedule_id: Uuid,
    attempt_number: i32,
) -> Result<PublicationAttempt, PublisherRepositoryError> {
    let row = sqlx::query_as::<_, PublicationAttemptRow>(
        r#"
        SELECT id, publication_schedule_id, attempt_number, status, started_at,
               finished_at, telegram_request_key, error_class, error_message, response_json
        FROM publication_attempts
        WHERE publication_schedule_id = $1 AND attempt_number = $2
        FOR UPDATE
        "#,
    )
    .bind(schedule_id)
    .bind(attempt_number)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(PublisherRepositoryError::AttemptMissing { schedule_id, attempt_number })?;
    row.into_publication_attempt()
}

async fn load_published_post(
    transaction: &mut Transaction<'_, Postgres>,
    schedule_id: Uuid,
) -> Result<Option<PublishedPost>, PublisherRepositoryError> {
    let row = sqlx::query_as::<_, PublishedPostRow>(
        r#"
        SELECT id, publication_schedule_id, content_item_id, asset_id, target_channel_id,
               telegram_chat_id, telegram_message_id, caption_snapshot, published_at, status
        FROM published_posts
        WHERE publication_schedule_id = $1
        FOR UPDATE
        "#,
    )
    .bind(schedule_id)
    .fetch_optional(&mut **transaction)
    .await?;
    row.map(PublishedPostRow::into_published_post).transpose()
}

async fn insert_or_load_published_post(
    transaction: &mut Transaction<'_, Postgres>,
    schedule_id: Uuid,
    telegram_message_id: i64,
    caption_snapshot: Option<String>,
) -> Result<PublishedPost, PublisherRepositoryError> {
    let inserted = sqlx::query_as::<_, PublishedPostRow>(
        r#"
        INSERT INTO published_posts (
            publication_schedule_id, content_item_id, asset_id, target_channel_id,
            telegram_chat_id, telegram_message_id, caption_snapshot
        )
        SELECT ps.id, pd.content_item_id, pd.asset_id, pd.target_channel_id,
               tc.telegram_chat_id, $2, $3
        FROM publication_schedules ps
        JOIN post_drafts pd ON pd.id = ps.post_draft_id
        JOIN target_channels tc ON tc.id = pd.target_channel_id
        WHERE ps.id = $1
        ON CONFLICT (publication_schedule_id) DO NOTHING
        RETURNING id, publication_schedule_id, content_item_id, asset_id,
                  target_channel_id, telegram_chat_id, telegram_message_id,
                  caption_snapshot, published_at, status
        "#,
    )
    .bind(schedule_id)
    .bind(telegram_message_id)
    .bind(&caption_snapshot)
    .fetch_optional(&mut **transaction)
    .await?;
    if let Some(row) = inserted {
        return row.into_published_post();
    }
    let existing = load_published_post(transaction, schedule_id)
        .await?
        .ok_or(PublisherRepositoryError::PublishedPostMissing(schedule_id))?;
    if existing.telegram_message_id != telegram_message_id
        || existing.caption_snapshot.as_ref() != caption_snapshot.as_ref()
    {
        return Err(PublisherRepositoryError::PublishedPostConflict(schedule_id));
    }
    Ok(existing)
}

fn to_i64(value: u64, field: &'static str) -> Result<i64, PublisherRepositoryError> {
    i64::try_from(value).map_err(|_| PublisherRepositoryError::NumberOverflow { field })
}

fn truncate_to_microseconds(value: OffsetDateTime) -> OffsetDateTime {
    value
        .replace_nanosecond((value.nanosecond() / 1_000) * 1_000)
        .expect("truncating a valid timestamp's nanoseconds remains valid")
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

#[derive(Debug, FromRow)]
struct IdempotencyResourceRow {
    request_hash: Vec<u8>,
    resource_id: Option<Uuid>,
    response_body: Option<Value>,
}

#[derive(Debug, FromRow)]
struct PublishableContentRow {
    status: String,
    canonical_asset_id: Option<Uuid>,
}

#[derive(Debug, FromRow)]
struct PublicationCadenceRow {
    last_published_at: Option<OffsetDateTime>,
    published_today: i64,
}

#[derive(Debug, FromRow)]
struct TargetChannelRow {
    id: Uuid,
    name: String,
    telegram_chat_id: i64,
    is_enabled: bool,
    default_parse_mode: Option<String>,
    default_disable_notification: bool,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
}

impl TargetChannelRow {
    fn into_target_channel(self) -> Result<TargetChannel, PublisherRepositoryError> {
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

#[derive(Debug, FromRow)]
struct ChannelPolicyRow {
    target_channel_id: Uuid,
    minimum_post_interval_seconds: i64,
    same_content_cooldown_seconds: i64,
    similar_content_cooldown_seconds: i64,
    similarity_threshold: f64,
    on_cooldown_violation: String,
    allowed_windows_json: Value,
    max_posts_per_day: Option<i32>,
    jitter_seconds: i64,
    updated_at: OffsetDateTime,
}

impl ChannelPolicyRow {
    fn into_channel_policy(self) -> Result<ChannelPolicy, PublisherRepositoryError> {
        Ok(ChannelPolicy {
            target_channel_id: self.target_channel_id,
            minimum_post_interval_seconds: from_i64(
                self.minimum_post_interval_seconds,
                "minimum_post_interval_seconds",
            )?,
            same_content_cooldown_seconds: from_i64(
                self.same_content_cooldown_seconds,
                "same_content_cooldown_seconds",
            )?,
            similar_content_cooldown_seconds: from_i64(
                self.similar_content_cooldown_seconds,
                "similar_content_cooldown_seconds",
            )?,
            similarity_threshold: self.similarity_threshold,
            on_cooldown_violation: parse_enum(
                "channel_policies.on_cooldown_violation",
                &self.on_cooldown_violation,
            )?,
            allowed_windows_json: self.allowed_windows_json,
            max_posts_per_day: self
                .max_posts_per_day
                .map(|value| {
                    u32::try_from(value).map_err(|_| PublisherRepositoryError::NumberOverflow {
                        field: "max_posts_per_day",
                    })
                })
                .transpose()?,
            jitter_seconds: from_i64(self.jitter_seconds, "jitter_seconds")?,
            updated_at: self.updated_at,
        })
    }
}

#[derive(Debug, FromRow)]
struct PostDraftRow {
    id: Uuid,
    content_item_id: Uuid,
    asset_id: Uuid,
    target_channel_id: Uuid,
    caption: Option<String>,
    parse_mode: Option<String>,
    status: String,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
}

impl PostDraftRow {
    fn into_post_draft(self) -> Result<PostDraft, PublisherRepositoryError> {
        Ok(PostDraft {
            id: self.id,
            content_item_id: self.content_item_id,
            asset_id: self.asset_id,
            target_channel_id: self.target_channel_id,
            caption: self.caption,
            parse_mode: self.parse_mode,
            status: parse_enum("post_drafts.status", &self.status)?,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}

#[derive(Debug, FromRow)]
struct PublicationScheduleRow {
    id: Uuid,
    post_draft_id: Uuid,
    status: String,
    publish_at: OffsetDateTime,
    not_before: Option<OffsetDateTime>,
    not_after: Option<OffsetDateTime>,
    priority: i32,
    cooldown_override: Option<bool>,
    #[allow(dead_code)]
    idempotency_scope: String,
    idempotency_key: String,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
}

impl PublicationScheduleRow {
    fn into_publication_schedule(self) -> Result<PublicationSchedule, PublisherRepositoryError> {
        let Self {
            id,
            post_draft_id,
            status,
            publish_at,
            not_before,
            not_after,
            priority,
            cooldown_override,
            idempotency_scope: _,
            idempotency_key,
            created_at,
            updated_at,
        } = self;
        Ok(PublicationSchedule {
            id,
            post_draft_id,
            status: parse_enum("publication_schedules.status", &status)?,
            publish_at,
            not_before,
            not_after,
            priority,
            cooldown_override,
            idempotency_key,
            created_at,
            updated_at,
        })
    }
}

#[derive(Debug, FromRow)]
struct PublicationAttemptRow {
    id: Uuid,
    publication_schedule_id: Uuid,
    attempt_number: i32,
    status: String,
    started_at: OffsetDateTime,
    finished_at: Option<OffsetDateTime>,
    telegram_request_key: Option<String>,
    error_class: Option<String>,
    error_message: Option<String>,
    response_json: Option<Value>,
}

impl PublicationAttemptRow {
    fn into_publication_attempt(self) -> Result<PublicationAttempt, PublisherRepositoryError> {
        Ok(PublicationAttempt {
            id: self.id,
            publication_schedule_id: self.publication_schedule_id,
            attempt_number: self.attempt_number,
            status: parse_enum("publication_attempts.status", &self.status)?,
            started_at: self.started_at,
            finished_at: self.finished_at,
            telegram_request_key: self.telegram_request_key,
            error_class: self.error_class,
            error_message: self.error_message,
            response_json: self.response_json,
        })
    }
}

#[derive(Debug, FromRow)]
struct PublishedPostRow {
    id: Uuid,
    publication_schedule_id: Uuid,
    content_item_id: Uuid,
    asset_id: Uuid,
    target_channel_id: Uuid,
    telegram_chat_id: i64,
    telegram_message_id: i64,
    caption_snapshot: Option<String>,
    published_at: OffsetDateTime,
    status: String,
}

impl PublishedPostRow {
    fn into_published_post(self) -> Result<PublishedPost, PublisherRepositoryError> {
        Ok(PublishedPost {
            id: self.id,
            publication_schedule_id: self.publication_schedule_id,
            content_item_id: self.content_item_id,
            asset_id: self.asset_id,
            target_channel_id: self.target_channel_id,
            telegram_chat_id: self.telegram_chat_id,
            telegram_message_id: self.telegram_message_id,
            caption_snapshot: self.caption_snapshot,
            published_at: self.published_at,
            status: parse_enum("published_posts.status", &self.status)?,
        })
    }
}

fn parse_enum<T>(field: &'static str, value: &str) -> Result<T, PublisherRepositoryError>
where
    T: for<'value> TryFrom<&'value str, Error = String>,
{
    T::try_from(value).map_err(|value| PublisherRepositoryError::InvalidEnum { field, value })
}

fn from_i64(value: i64, field: &'static str) -> Result<u64, PublisherRepositoryError> {
    u64::try_from(value).map_err(|_| PublisherRepositoryError::NumberOverflow { field })
}
