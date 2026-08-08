use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sooqa_library::{
    AssetRole, ContentItem, ContentItemUpdate, ContentKind, ContentStatus, ExactDuplicateRequest,
    ExactDuplicateResolution, LibraryCursor, LibraryItemDetail, LibraryItemSummary,
    LibrarySearchPage, LibrarySearchQuery, MediaAsset, MediaKind, NewContentItem, NewMediaAsset,
    NewSourceRecord, NewStorageObject, NewTag, SourceRecord, SourceType, StorageObject,
    StorageObjectStatus, StorageState, StorageUploadAttachment, StorageUploadIntent,
    StorageUploadReservation, StorageUploadReservationRequest, StorageUploadStore, Tag,
};
use sqlx::{FromRow, PgPool};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

/// Persistence for the single normalized `media` row.
///
/// The public compatibility vocabulary is intentionally kept small while the
/// database has only one media record per normalized item.  A thumbnail or a
/// source is metadata on that row, not another durable entity.
#[derive(Clone)]
pub struct LibraryRepository {
    pool: PgPool,
}

#[derive(Debug, Clone)]
pub struct StoredVideoFingerprint {
    pub content_item_id: Uuid,
    pub width: Option<i32>,
    pub height: Option<i32>,
    pub audio_codec: Option<String>,
    pub fingerprint: Value,
}

#[derive(Debug, Clone, FromRow)]
struct MediaRow {
    id: Uuid,
    kind: String,
    storage_state: String,
    canonical_sha256: Option<Vec<u8>>,
    fingerprint: Option<Vec<u8>>,
    title: Option<String>,
    description: Option<String>,
    tags: Vec<String>,
    source_url: Option<String>,
    source_metadata: Value,
    mime_type: Option<String>,
    container: Option<String>,
    video_codec: Option<String>,
    audio_codec: Option<String>,
    width: Option<i32>,
    height: Option<i32>,
    duration_ms: Option<i64>,
    bit_rate: Option<i64>,
    file_size_bytes: Option<i64>,
    local_work_path: Option<String>,
    telegram_storage_chat_id: Option<i64>,
    telegram_storage_message_id: Option<i64>,
    telegram_file_id: Option<String>,
    telegram_file_unique_id: Option<String>,
    storage_generation: i32,
    storage_token: Option<Uuid>,
    storage_started_at: Option<OffsetDateTime>,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
    stored_at: Option<OffsetDateTime>,
}

impl LibraryRepository {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn list_stored_video_fingerprints(
        &self,
        exclude_content_item_id: Uuid,
        algorithm_version: &str,
    ) -> Result<Vec<StoredVideoFingerprint>, LibraryRepositoryError> {
        let rows = sqlx::query_as::<_, MediaRow>(
            "SELECT * FROM media WHERE id <> $1 AND kind = 'video' AND fingerprint IS NOT NULL AND fingerprint_version = $2 ORDER BY id",
        )
        .bind(exclude_content_item_id)
        .bind(algorithm_version)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let bytes = row
                    .fingerprint
                    .as_deref()
                    .ok_or(LibraryRepositoryError::Invariant("media fingerprint is missing"))?;
                let fingerprint = serde_json::from_slice(bytes)?;
                Ok(StoredVideoFingerprint {
                    content_item_id: row.id,
                    width: row.width,
                    height: row.height,
                    audio_codec: row.audio_codec,
                    fingerprint,
                })
            })
            .collect()
    }

    pub async fn record_fingerprint(
        &self,
        media_id: Uuid,
        algorithm_version: &str,
        fingerprint: &Value,
    ) -> Result<(), LibraryRepositoryError> {
        let encoded = serde_json::to_vec(fingerprint)?;
        let updated = sqlx::query(
            "UPDATE media SET fingerprint_version = $2, fingerprint = $3, updated_at = now() WHERE id = $1",
        )
        .bind(media_id)
        .bind(algorithm_version)
        .bind(encoded)
        .execute(&self.pool)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(LibraryRepositoryError::ResourceMissing(media_id));
        }
        Ok(())
    }

    pub async fn find_library_item(
        &self,
        id: Uuid,
    ) -> Result<Option<LibraryItemDetail>, LibraryRepositoryError> {
        let Some(row) = self.media(id).await? else { return Ok(None) };
        Ok(Some(self.detail_from_row(row).await?))
    }

    pub async fn search_library(
        &self,
        query: LibrarySearchQuery,
    ) -> Result<LibrarySearchPage, LibraryRepositoryError> {
        if !(1..=100).contains(&query.limit) {
            return Err(LibraryRepositoryError::InvalidLimit { value: query.limit });
        }
        let text = query.text.filter(|value| !value.trim().is_empty());
        let kind = query.kind.map(|value| value.as_str().to_owned());
        let tags = query.tags;
        let rows = sqlx::query_as::<_, MediaRow>(
            r#"
            SELECT * FROM media
            WHERE ($1::text IS NULL OR kind = $1)
              AND ($2::text IS NULL OR title ILIKE '%' || $2 || '%' OR description ILIKE '%' || $2 || '%')
              AND ($3::text[] IS NULL OR tags @> $3)
              AND ($4::timestamptz IS NULL OR (updated_at, id) < ($4, $5))
            ORDER BY updated_at DESC, id DESC
            LIMIT $6
            "#,
        )
        .bind(kind)
        .bind(text)
        .bind((!tags.is_empty()).then_some(tags))
        .bind(query.cursor.as_ref().map(|cursor| cursor.updated_at))
        .bind(query.cursor.as_ref().map(|cursor| cursor.id))
        .bind(i64::from(query.limit) + 1)
        .fetch_all(&self.pool)
        .await?;

        let has_more = rows.len() > query.limit as usize;
        let rows = rows.into_iter().take(query.limit as usize).collect::<Vec<_>>();
        let next_cursor = has_more
            .then(|| rows.last())
            .flatten()
            .map(|row| LibraryCursor { updated_at: row.updated_at, id: row.id });
        let items = rows
            .into_iter()
            .map(|row| self.summary_from_row(row))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(LibrarySearchPage { items, next_cursor })
    }

    pub async fn update_content_item(
        &self,
        id: Uuid,
        update: ContentItemUpdate,
    ) -> Result<ContentItem, LibraryRepositoryError> {
        if update.preferred_title.is_none()
            && update.editorial_description.is_none()
            && update.notes.is_none()
        {
            return Err(LibraryRepositoryError::EmptyUpdate);
        }
        let current = self.media(id).await?.ok_or(LibraryRepositoryError::ResourceMissing(id))?;
        if update.expected_updated_at.is_some_and(|expected| expected != current.updated_at) {
            return Err(LibraryRepositoryError::OptimisticConflict(id));
        }
        let title = update.preferred_title.unwrap_or(current.title.clone());
        let description = update.editorial_description.unwrap_or(current.description.clone());
        let notes = update.notes.unwrap_or_else(|| {
            current.source_metadata.get("notes").and_then(Value::as_str).map(str::to_owned)
        });
        let metadata = with_notes(current.source_metadata, notes);
        let row = sqlx::query_as::<_, MediaRow>(
            "UPDATE media SET title = $2, description = $3, source_metadata = $4, updated_at = now() WHERE id = $1 RETURNING *",
        )
        .bind(id)
        .bind(title)
        .bind(description)
        .bind(metadata)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(LibraryRepositoryError::ResourceMissing(id))?;
        row.into_content_item()
    }

    pub async fn archive_content_item(
        &self,
        id: Uuid,
    ) -> Result<ContentItem, LibraryRepositoryError> {
        let row = sqlx::query_as::<_, MediaRow>(
            "UPDATE media SET source_metadata = source_metadata || jsonb_build_object('archived', true), updated_at = now() WHERE id = $1 RETURNING *",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(LibraryRepositoryError::ResourceMissing(id))?;
        row.into_content_item()
    }

    pub async fn add_tag(&self, id: Uuid, tag: NewTag) -> Result<Tag, LibraryRepositoryError> {
        let row = sqlx::query_as::<_, MediaRow>(
            "UPDATE media SET tags = array_append(array_remove(tags, $2), $2), updated_at = now() WHERE id = $1 RETURNING *",
        )
        .bind(id)
        .bind(&tag.normalized_name)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(LibraryRepositoryError::ResourceMissing(id))?;
        Ok(tag_from_name(&tag.normalized_name, row.updated_at))
    }

    pub async fn remove_tag(&self, id: Uuid, tag: &str) -> Result<(), LibraryRepositoryError> {
        let updated = sqlx::query("UPDATE media SET tags = array_remove(tags, $2), updated_at = now() WHERE id = $1 AND $2 = ANY(tags)")
            .bind(id)
            .bind(tag)
            .execute(&self.pool)
            .await?;
        if updated.rows_affected() == 0 {
            let exists =
                sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM media WHERE id = $1)")
                    .bind(id)
                    .fetch_one(&self.pool)
                    .await?;
            if !exists {
                return Err(LibraryRepositoryError::ResourceMissing(id));
            }
            return Err(LibraryRepositoryError::TagNotAttached);
        }
        Ok(())
    }

    pub async fn create_content_item(
        &self,
        new_item: NewContentItem,
    ) -> Result<ContentItem, LibraryRepositoryError> {
        let id = Uuid::now_v7();
        let row = sqlx::query_as::<_, MediaRow>(
            "INSERT INTO media (id, kind, title, description, source_metadata) VALUES ($1, $2, $3, $4, $5) RETURNING *",
        )
        .bind(id)
        .bind(new_item.kind.as_str())
        .bind(new_item.preferred_title)
        .bind(new_item.editorial_description)
        .bind(with_notes(json!({}), new_item.notes))
        .fetch_one(&self.pool)
        .await?;
        row.into_content_item()
    }

    pub async fn find_content_item(
        &self,
        id: Uuid,
    ) -> Result<Option<ContentItem>, LibraryRepositoryError> {
        self.media(id).await?.map(MediaRow::into_content_item).transpose()
    }

    pub async fn create_media_asset(
        &self,
        asset: NewMediaAsset,
    ) -> Result<MediaAsset, LibraryRepositoryError> {
        self.upsert_media(asset.content_item_id, asset).await
    }

    pub async fn record_canonical_asset(
        &self,
        content_item_id: Uuid,
        asset: NewMediaAsset,
    ) -> Result<MediaAsset, LibraryRepositoryError> {
        if asset.content_item_id != content_item_id {
            return Err(LibraryRepositoryError::ContentItemMismatch {
                expected: content_item_id,
                actual: asset.content_item_id,
            });
        }
        if asset.role != AssetRole::Canonical {
            return Err(LibraryRepositoryError::InvalidCanonicalAssetRole);
        }
        self.upsert_media(content_item_id, asset).await
    }

    pub async fn record_thumbnail_asset(
        &self,
        content_item_id: Uuid,
        asset: NewMediaAsset,
    ) -> Result<MediaAsset, LibraryRepositoryError> {
        if asset.content_item_id != content_item_id {
            return Err(LibraryRepositoryError::ContentItemMismatch {
                expected: content_item_id,
                actual: asset.content_item_id,
            });
        }
        // The reset deliberately keeps one normalized media row.  Thumbnail
        // metadata is retained in source_metadata by the caller if needed;
        // it does not become another media identity.
        self.find_media_asset(content_item_id)
            .await?
            .ok_or(LibraryRepositoryError::ResourceMissing(content_item_id))
    }

    pub async fn find_media_asset(
        &self,
        id: Uuid,
    ) -> Result<Option<MediaAsset>, LibraryRepositoryError> {
        self.media(id).await?.map(MediaRow::into_media_asset).transpose()
    }

    pub async fn create_source_record(
        &self,
        source: NewSourceRecord,
    ) -> Result<SourceRecord, LibraryRepositoryError> {
        self.write_source(source.content_item_id, source_to_value(&source)).await
    }

    pub async fn list_source_records(
        &self,
        id: Uuid,
    ) -> Result<Vec<SourceRecord>, LibraryRepositoryError> {
        let Some(row) = self.media(id).await? else { return Ok(Vec::new()) };
        if row.source_url.is_none() && row.source_metadata.get("source_type").is_none() {
            return Ok(Vec::new());
        }
        Ok(vec![source_from_row(&row)?])
    }

    pub async fn upsert_tag(&self, tag: NewTag) -> Result<Tag, LibraryRepositoryError> {
        Ok(tag_from_name(&tag.normalized_name, OffsetDateTime::now_utc()))
    }

    pub async fn attach_tag(
        &self,
        content_item_id: Uuid,
        tag: NewTag,
    ) -> Result<(), LibraryRepositoryError> {
        self.add_tag(content_item_id, tag).await.map(|_| ())
    }

    pub async fn list_tags(&self, id: Uuid) -> Result<Vec<Tag>, LibraryRepositoryError> {
        let Some(row) = self.media(id).await? else {
            return Err(LibraryRepositoryError::ResourceMissing(id));
        };
        Ok(row.tags.iter().map(|tag| tag_from_name(tag, row.updated_at)).collect())
    }

    pub async fn resolve_exact_duplicate(
        &self,
        request: ExactDuplicateRequest,
    ) -> Result<ExactDuplicateResolution, LibraryRepositoryError> {
        validate_exact_duplicate_request(&request)?;
        let sha256 =
            request.asset.sha256.as_deref().ok_or(LibraryRepositoryError::MissingSha256)?;
        if let Some(row) = sqlx::query_as::<_, MediaRow>(
            "SELECT * FROM media WHERE canonical_sha256 = $1 FOR UPDATE",
        )
        .bind(sha256)
        .fetch_optional(&self.pool)
        .await?
        {
            let source = self
                .write_source(row.id, source_to_value(&request.source.for_content_item(row.id)))
                .await?;
            let current =
                self.media(row.id).await?.ok_or(LibraryRepositoryError::ResourceMissing(row.id))?;
            return Ok(ExactDuplicateResolution {
                content_item: current.clone().into_content_item()?,
                canonical_asset: current.into_media_asset()?,
                source_record: source,
                content_created: false,
                source_created: true,
            });
        }

        let id = Uuid::now_v7();
        let asset = request.asset.for_content_item(id);
        let source = request.source.for_content_item(id);
        let source_value = source_to_value(&source);
        let row = sqlx::query_as::<_, MediaRow>(
            r#"INSERT INTO media (
                id, kind, storage_state, canonical_sha256, title, description, tags,
                source_url, source_metadata, mime_type, container, video_codec,
                audio_codec, width, height, duration_ms, bit_rate, file_size_bytes,
                local_work_path
            ) VALUES ($1, $2, $3, $4, $5, $6, '{}', $7, $8, $9, $10, $11, $12, $13,
                      $14, $15, $16, $17, $18) RETURNING *"#,
        )
        .bind(id)
        .bind(asset.media_kind.as_str())
        .bind(db_storage_state(asset.storage_state))
        .bind(asset.sha256)
        .bind(request.content_item.preferred_title)
        .bind(request.content_item.editorial_description)
        .bind(source.normalized_url.clone().or(source.original_url.clone()))
        .bind(source_value)
        .bind(asset.mime_type)
        .bind(asset.container)
        .bind(asset.video_codec)
        .bind(asset.audio_codec)
        .bind(asset.width)
        .bind(asset.height)
        .bind(asset.duration_ms.and_then(|value| i64::try_from(value).ok()))
        .bind(asset.bit_rate.and_then(|value| i64::try_from(value).ok()))
        .bind(asset.file_size_bytes.and_then(|value| i64::try_from(value).ok()))
        .bind(asset.local_work_path)
        .fetch_one(&self.pool)
        .await?;
        let source_record = source_from_row(&row)?;
        let content_item = row.clone().into_content_item()?;
        let canonical_asset = row.into_media_asset()?;
        Ok(ExactDuplicateResolution {
            content_item,
            canonical_asset,
            source_record,
            content_created: true,
            source_created: true,
        })
    }

    pub async fn list_storage_upload_intents(
        &self,
    ) -> Result<Vec<StorageUploadIntent>, LibraryRepositoryError> {
        let rows = sqlx::query_as::<_, MediaRow>("SELECT * FROM media WHERE storage_state IN ('pending_storage', 'storage_unknown') ORDER BY created_at, id")
            .fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(intent_from_row).collect())
    }

    pub async fn mark_storage_upload_intent_unknown(
        &self,
        id: Uuid,
        force: bool,
    ) -> Result<(), LibraryRepositoryError> {
        let row =
            self.media(id).await?.ok_or(LibraryRepositoryError::StorageUploadIntentMissing(id))?;
        if row.storage_token.is_some() && !force {
            return Err(LibraryRepositoryError::StorageUploadIntentActive(id));
        }
        let updated = sqlx::query("UPDATE media SET storage_state = 'storage_unknown', storage_token = NULL, storage_started_at = NULL, updated_at = now() WHERE id = $1")
            .bind(id).execute(&self.pool).await?;
        if updated.rows_affected() != 1 {
            return Err(LibraryRepositoryError::StorageUploadIntentMissing(id));
        }
        Ok(())
    }

    pub async fn reset_storage_upload_intent(
        &self,
        id: Uuid,
    ) -> Result<(), LibraryRepositoryError> {
        let row =
            self.media(id).await?.ok_or(LibraryRepositoryError::StorageUploadIntentMissing(id))?;
        if row.storage_token.is_some() {
            return Err(LibraryRepositoryError::StorageUploadIntentActive(id));
        }
        let next_generation = row
            .storage_generation
            .checked_add(1)
            .ok_or(LibraryRepositoryError::StorageUploadGenerationOverflow(id))?;
        sqlx::query("UPDATE media SET storage_state = 'pending_storage', storage_generation = $2, storage_token = NULL, storage_started_at = NULL, updated_at = now() WHERE id = $1")
            .bind(id).bind(next_generation).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn attach_storage_upload(
        &self,
        id: Uuid,
        attachment: StorageUploadAttachment,
    ) -> Result<StorageObject, LibraryRepositoryError> {
        let attachment = validate_storage_upload_attachment(attachment)?;
        let updated = sqlx::query_as::<_, MediaRow>(
            "UPDATE media SET storage_state = 'ready', telegram_storage_chat_id = $2, telegram_storage_message_id = $3, telegram_file_id = $4, telegram_file_unique_id = $5, storage_token = NULL, storage_started_at = NULL, stored_at = now(), updated_at = now() WHERE id = $1 AND storage_state = 'storage_unknown' RETURNING *",
        )
        .bind(id).bind(attachment.storage_chat_id).bind(attachment.storage_message_id)
        .bind(attachment.telegram_file_id).bind(attachment.telegram_file_unique_id)
        .fetch_optional(&self.pool).await?
        .ok_or(LibraryRepositoryError::StorageUploadIntentNotUnknown(id))?;
        updated.into_storage_object()
    }

    async fn media(&self, id: Uuid) -> Result<Option<MediaRow>, LibraryRepositoryError> {
        Ok(sqlx::query_as::<_, MediaRow>("SELECT * FROM media WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?)
    }

    async fn detail_from_row(
        &self,
        row: MediaRow,
    ) -> Result<LibraryItemDetail, LibraryRepositoryError> {
        let tags = row.tags.iter().map(|tag| tag_from_name(tag, row.updated_at)).collect();
        let source = if row.source_url.is_some() || row.source_metadata.get("source_type").is_some()
        {
            vec![source_from_row(&row)?]
        } else {
            Vec::new()
        };
        let asset = row.clone().into_media_asset()?;
        Ok(LibraryItemDetail {
            content_item: row.clone().into_content_item()?,
            canonical_asset: Some(asset),
            tags,
            sources: source,
        })
    }

    fn summary_from_row(
        &self,
        row: MediaRow,
    ) -> Result<LibraryItemSummary, LibraryRepositoryError> {
        let tags =
            row.tags.iter().map(|tag| tag_from_name(tag, row.updated_at)).collect::<Vec<_>>();
        let source_count = u64::from(
            (row.source_url.is_some() || row.source_metadata.get("source_type").is_some()) as u8,
        );
        Ok(LibraryItemSummary {
            content_item: row.clone().into_content_item()?,
            canonical_asset: Some(row.into_media_asset()?),
            tags,
            source_count,
        })
    }

    async fn upsert_media(
        &self,
        id: Uuid,
        asset: NewMediaAsset,
    ) -> Result<MediaAsset, LibraryRepositoryError> {
        if asset.role == AssetRole::Thumbnail {
            return self
                .find_media_asset(id)
                .await?
                .ok_or(LibraryRepositoryError::ResourceMissing(id));
        }
        let row = sqlx::query_as::<_, MediaRow>(
            r#"UPDATE media SET kind = $2,
                storage_state = CASE WHEN media.storage_state IN ('ready', 'storage_unknown')
                    THEN media.storage_state ELSE $3 END,
                canonical_sha256 = $4,
                mime_type = $5, container = $6, video_codec = $7, audio_codec = $8,
                width = $9, height = $10, duration_ms = $11, bit_rate = $12,
                file_size_bytes = $13, local_work_path = $14, updated_at = now()
                WHERE id = $1 RETURNING *"#,
        )
        .bind(id)
        .bind(asset.media_kind.as_str())
        .bind(db_storage_state(asset.storage_state))
        .bind(asset.sha256)
        .bind(asset.mime_type)
        .bind(asset.container)
        .bind(asset.video_codec)
        .bind(asset.audio_codec)
        .bind(asset.width)
        .bind(asset.height)
        .bind(asset.duration_ms.and_then(|value| i64::try_from(value).ok()))
        .bind(asset.bit_rate.and_then(|value| i64::try_from(value).ok()))
        .bind(asset.file_size_bytes.and_then(|value| i64::try_from(value).ok()))
        .bind(asset.local_work_path)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(LibraryRepositoryError::ResourceMissing(id))?;
        row.into_media_asset()
    }

    async fn write_source(
        &self,
        id: Uuid,
        source: Value,
    ) -> Result<SourceRecord, LibraryRepositoryError> {
        let source_url = source
            .get("normalized_url")
            .and_then(Value::as_str)
            .or_else(|| source.get("original_url").and_then(Value::as_str))
            .map(str::to_owned);
        let row = sqlx::query_as::<_, MediaRow>("UPDATE media SET source_url = $2, source_metadata = $3, updated_at = now() WHERE id = $1 RETURNING *")
            .bind(id).bind(source_url).bind(source).fetch_optional(&self.pool).await?
            .ok_or(LibraryRepositoryError::ResourceMissing(id))?;
        source_from_row(&row)
    }
}

#[async_trait]
impl StorageUploadStore for LibraryRepository {
    type Error = LibraryRepositoryError;

    async fn find_canonical_asset(
        &self,
        asset_id: Uuid,
    ) -> Result<Option<MediaAsset>, Self::Error> {
        self.find_media_asset(asset_id).await
    }

    async fn find_active_storage_object(
        &self,
        asset_id: Uuid,
        _: &str,
    ) -> Result<Option<StorageObject>, Self::Error> {
        let Some(row) = self.media(asset_id).await? else { return Ok(None) };
        if row.storage_state != "ready" {
            return Ok(None);
        }
        Ok(Some(row.into_storage_object()?))
    }

    async fn reserve_storage_upload(
        &self,
        request: StorageUploadReservationRequest,
    ) -> Result<StorageUploadReservation, Self::Error> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query_as::<_, MediaRow>("SELECT * FROM media WHERE id = $1 FOR UPDATE")
            .bind(request.asset_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(LibraryRepositoryError::ResourceMissing(request.asset_id))?;
        if row.storage_state == "ready" {
            transaction.commit().await?;
            return Ok(StorageUploadReservation::Reused(row.into_storage_object()?));
        }
        if row.storage_token.is_some() {
            transaction.commit().await?;
            return Ok(StorageUploadReservation::InProgress {
                retry_at: row.storage_started_at.map(|time| time + time::Duration::minutes(1)),
            });
        }
        let owner_token = Uuid::new_v4();
        let updated = sqlx::query("UPDATE media SET storage_state = 'pending_storage', storage_generation = $2, storage_token = $3, storage_started_at = now(), updated_at = now() WHERE id = $1")
            .bind(request.asset_id).bind(request.generation).bind(owner_token).execute(&mut *transaction).await?;
        if updated.rows_affected() != 1 {
            return Err(LibraryRepositoryError::ResourceMissing(request.asset_id));
        }
        transaction.commit().await?;
        Ok(StorageUploadReservation::Reserved { intent_id: request.asset_id, owner_token })
    }

    async fn renew_storage_upload(
        &self,
        intent_id: Uuid,
        owner_token: Uuid,
        lease_duration: Duration,
    ) -> Result<OffsetDateTime, Self::Error> {
        if lease_duration.is_zero() {
            return Err(LibraryRepositoryError::InvalidStorageLeaseDuration);
        }
        let updated = sqlx::query_scalar::<_, OffsetDateTime>("UPDATE media SET storage_started_at = now(), updated_at = now() WHERE id = $1 AND storage_token = $2 RETURNING storage_started_at")
            .bind(intent_id).bind(owner_token).fetch_optional(&self.pool).await?
            .ok_or(LibraryRepositoryError::StorageUploadIntentMissing(intent_id))?;
        Ok(updated)
    }

    async fn complete_storage_upload(
        &self,
        intent_id: Uuid,
        owner_token: Uuid,
        object: NewStorageObject,
    ) -> Result<StorageObject, Self::Error> {
        if object.asset_id != intent_id {
            return Err(LibraryRepositoryError::StorageUploadCompletionBindingConflict {
                intent_id,
            });
        }
        let row = sqlx::query_as::<_, MediaRow>(
            "UPDATE media SET storage_state = 'ready', telegram_storage_chat_id = $3, telegram_storage_message_id = $4, telegram_file_id = $5, telegram_file_unique_id = $6, storage_token = NULL, storage_started_at = NULL, stored_at = now(), updated_at = now() WHERE id = $1 AND storage_token = $2 RETURNING *",
        )
        .bind(intent_id).bind(owner_token).bind(object.storage_chat_id).bind(object.storage_message_id)
        .bind(object.telegram_file_id).bind(object.telegram_file_unique_id).fetch_optional(&self.pool).await?
        .ok_or(LibraryRepositoryError::StorageUploadIntentMissing(intent_id))?;
        row.into_storage_object()
    }

    async fn release_storage_upload(
        &self,
        intent_id: Uuid,
        owner_token: Uuid,
    ) -> Result<(), Self::Error> {
        let updated = sqlx::query("UPDATE media SET storage_state = 'storage_unknown', storage_token = NULL, storage_started_at = NULL, updated_at = now() WHERE id = $1 AND storage_token = $2")
            .bind(intent_id).bind(owner_token).execute(&self.pool).await?;
        if updated.rows_affected() != 1 {
            return Err(LibraryRepositoryError::StorageUploadIntentMissing(intent_id));
        }
        Ok(())
    }

    async fn mark_storage_upload_unknown(
        &self,
        intent_id: Uuid,
        owner_token: Uuid,
    ) -> Result<(), Self::Error> {
        let updated = sqlx::query("UPDATE media SET storage_state = 'storage_unknown', storage_token = NULL, storage_started_at = NULL, updated_at = now() WHERE id = $1 AND storage_token = $2")
            .bind(intent_id).bind(owner_token).execute(&self.pool).await?;
        if updated.rows_affected() != 1 {
            return Err(LibraryRepositoryError::StorageUploadIntentMissing(intent_id));
        }
        Ok(())
    }
}

fn media_kind(value: &str) -> Result<MediaKind, LibraryRepositoryError> {
    MediaKind::try_from(value).map_err(LibraryRepositoryError::UnknownMediaKind)
}

fn db_storage_state(value: StorageState) -> &'static str {
    match value {
        StorageState::Local => "pending_storage",
        StorageState::Uploaded => "pending_storage",
        StorageState::Missing => "missing",
    }
}

fn domain_storage_state(value: &str) -> Result<StorageState, LibraryRepositoryError> {
    Ok(match value {
        "ready" => StorageState::Uploaded,
        "missing" | "storage_unknown" => StorageState::Missing,
        _ => StorageState::Local,
    })
}

fn checked_u64(
    value: Option<i64>,
    field: &'static str,
) -> Result<Option<u64>, LibraryRepositoryError> {
    value
        .map(|value| {
            u64::try_from(value).map_err(|_| LibraryRepositoryError::InvalidNumber { field })
        })
        .transpose()
}

impl MediaRow {
    fn into_content_item(self) -> Result<ContentItem, LibraryRepositoryError> {
        let status =
            if self.source_metadata.get("archived").and_then(Value::as_bool).unwrap_or(false) {
                ContentStatus::Archived
            } else {
                ContentStatus::Active
            };
        Ok(ContentItem {
            id: self.id,
            kind: ContentKind::try_from(self.kind.as_str()).map_err(|value| {
                LibraryRepositoryError::InvalidEnum { field: "media.kind", value }
            })?,
            status,
            canonical_asset_id: Some(self.id),
            preferred_title: self.title,
            editorial_description: self.description,
            notes: self.source_metadata.get("notes").and_then(Value::as_str).map(str::to_owned),
            created_at: self.created_at,
            updated_at: self.updated_at,
            archived_at: (status == ContentStatus::Archived).then_some(self.updated_at),
        })
    }

    fn into_media_asset(self) -> Result<MediaAsset, LibraryRepositoryError> {
        Ok(MediaAsset {
            id: self.id,
            content_item_id: self.id,
            role: AssetRole::Canonical,
            media_kind: media_kind(&self.kind)?,
            mime_type: self.mime_type,
            container: self.container,
            video_codec: self.video_codec,
            audio_codec: self.audio_codec,
            width: self.width,
            height: self.height,
            duration_ms: checked_u64(self.duration_ms, "duration_ms")?,
            bit_rate: checked_u64(self.bit_rate, "bit_rate")?,
            file_size_bytes: checked_u64(self.file_size_bytes, "file_size_bytes")?,
            sha256: self.canonical_sha256,
            local_work_path: self.local_work_path,
            storage_state: domain_storage_state(&self.storage_state)?,
            created_at: self.created_at,
        })
    }

    fn into_storage_object(self) -> Result<StorageObject, LibraryRepositoryError> {
        let storage_chat_id = self
            .telegram_storage_chat_id
            .ok_or(LibraryRepositoryError::StorageUploadObjectMissing(self.id))?;
        let storage_message_id = self
            .telegram_storage_message_id
            .ok_or(LibraryRepositoryError::StorageUploadObjectMissing(self.id))?;
        Ok(StorageObject {
            id: self.id,
            asset_id: self.id,
            provider: "telegram".to_owned(),
            storage_chat_id,
            storage_message_id,
            telegram_file_id: self.telegram_file_id,
            telegram_file_unique_id: self.telegram_file_unique_id,
            media_kind: media_kind(&self.kind)?,
            stored_at: self.stored_at.unwrap_or(self.updated_at),
            verified_at: self.stored_at,
            status: if self.storage_state == "ready" {
                StorageObjectStatus::Active
            } else {
                StorageObjectStatus::Missing
            },
        })
    }
}

fn source_to_value(source: &NewSourceRecord) -> Value {
    json!({
        "source_type": source.source_type.as_str(),
        "original_url": source.original_url,
        "normalized_url": source.normalized_url,
        "platform": source.platform,
        "platform_content_id": source.platform_content_id,
        "author_name": source.author_name,
        "source_title": source.source_title,
        "source_description": source.source_description,
        "source_published_at": source.source_published_at,
        "ingest_request_id": source.ingest_request_id,
        "metadata": source.metadata_json,
    })
}

fn source_from_row(row: &MediaRow) -> Result<SourceRecord, LibraryRepositoryError> {
    let metadata = &row.source_metadata;
    let source_type = metadata.get("source_type").and_then(Value::as_str).unwrap_or("direct_url");
    Ok(SourceRecord {
        id: row.id,
        content_item_id: row.id,
        ingest_request_id: metadata
            .get("ingest_request_id")
            .and_then(Value::as_str)
            .and_then(|value| value.parse().ok()),
        source_type: SourceType::try_from(source_type).map_err(|value| {
            LibraryRepositoryError::InvalidEnum { field: "media.source_type", value }
        })?,
        original_url: metadata.get("original_url").and_then(Value::as_str).map(str::to_owned),
        normalized_url: row.source_url.clone(),
        platform: metadata.get("platform").and_then(Value::as_str).map(str::to_owned),
        platform_content_id: metadata
            .get("platform_content_id")
            .and_then(Value::as_str)
            .map(str::to_owned),
        author_name: metadata.get("author_name").and_then(Value::as_str).map(str::to_owned),
        source_title: metadata.get("source_title").and_then(Value::as_str).map(str::to_owned),
        source_description: metadata
            .get("source_description")
            .and_then(Value::as_str)
            .map(str::to_owned),
        source_published_at: metadata.get("source_published_at").and_then(Value::as_str).and_then(
            |value| {
                time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
                    .ok()
            },
        ),
        retrieved_at: row.updated_at,
        metadata_json: metadata.get("metadata").cloned().unwrap_or_else(|| json!({})),
    })
}

fn tag_from_name(name: &str, created_at: OffsetDateTime) -> Tag {
    let digest = Sha256::digest(name.as_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest[..16]);
    Tag {
        id: Uuid::from_bytes(bytes),
        normalized_name: name.to_owned(),
        display_name: name.to_owned(),
        created_at,
    }
}

fn with_notes(mut metadata: Value, notes: Option<String>) -> Value {
    if let Some(object) = metadata.as_object_mut() {
        match notes {
            Some(notes) => {
                object.insert("notes".to_owned(), Value::String(notes));
            }
            None => {
                object.remove("notes");
            }
        }
    }
    metadata
}

fn intent_from_row(row: MediaRow) -> StorageUploadIntent {
    let state = row.storage_state.clone();
    StorageUploadIntent {
        id: row.id,
        asset_id: Some(row.id),
        job_id: None,
        generation: row.storage_generation,
        provider: Some("telegram".to_owned()),
        storage_chat_id: row.telegram_storage_chat_id,
        idempotency_key: format!("media:{}:upload_storage:v1:{}", row.id, row.storage_generation),
        state,
        resource_id: (row.storage_state == "ready").then_some(row.id),
        created_at: row.created_at,
        reservation_expires_at: row
            .storage_started_at
            .map(|value| value + time::Duration::minutes(1)),
    }
}

fn validate_exact_duplicate_request(
    request: &ExactDuplicateRequest,
) -> Result<(), LibraryRepositoryError> {
    if request.asset.role != AssetRole::Canonical {
        return Err(LibraryRepositoryError::InvalidCanonicalAssetRole);
    }
    let sha256 = request.asset.sha256.as_deref().ok_or(LibraryRepositoryError::MissingSha256)?;
    if sha256.len() != 32 {
        return Err(LibraryRepositoryError::InvalidSha256Length { actual: sha256.len() });
    }
    if request.source.platform.is_some() != request.source.platform_content_id.is_some() {
        return Err(LibraryRepositoryError::InvalidSourceIdentity);
    }
    Ok(())
}

fn validate_storage_upload_attachment(
    attachment: StorageUploadAttachment,
) -> Result<StorageUploadAttachment, LibraryRepositoryError> {
    if attachment.storage_message_id <= 0 {
        return Err(LibraryRepositoryError::StorageUploadMessageIdInvalid {
            value: attachment.storage_message_id,
        });
    }
    if attachment.telegram_file_id.as_deref().is_some_and(str::is_empty) {
        return Err(LibraryRepositoryError::StorageUploadAttachmentFieldEmpty {
            field: "telegram_file_id",
        });
    }
    if attachment.telegram_file_unique_id.as_deref().is_some_and(str::is_empty) {
        return Err(LibraryRepositoryError::StorageUploadAttachmentFieldEmpty {
            field: "telegram_file_unique_id",
        });
    }
    Ok(attachment)
}

#[derive(Debug, Error)]
pub enum LibraryRepositoryError {
    #[error("database operation failed: {0}")]
    Database(#[from] sqlx::Error),
    #[error("media metadata serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("database returned invalid {field} value {value:?}")]
    InvalidEnum { field: &'static str, value: String },
    #[error("database returned an invalid non-negative {field} value")]
    InvalidNumber { field: &'static str },
    #[error("exact duplicate resolution requires a canonical asset")]
    InvalidCanonicalAssetRole,
    #[error("thumbnail asset recording requires a thumbnail asset")]
    InvalidThumbnailAssetRole,
    #[error("canonical asset belongs to content item {actual}, expected {expected}")]
    ContentItemMismatch { expected: Uuid, actual: Uuid },
    #[error("exact duplicate resolution requires a SHA-256 digest")]
    MissingSha256,
    #[error("SHA-256 digest must contain exactly 32 bytes, got {actual}")]
    InvalidSha256Length { actual: usize },
    #[error("platform and platform content ID must be supplied together")]
    InvalidSourceIdentity,
    #[error("database invariant violated: {0}")]
    Invariant(&'static str),
    #[error("library item {0} was not found")]
    ResourceMissing(Uuid),
    #[error("library item {0} was changed by another request")]
    OptimisticConflict(Uuid),
    #[error("library update contains no editable fields")]
    EmptyUpdate,
    #[error("library item {id} cannot be {operation} in its current state")]
    InvalidState { id: Uuid, operation: &'static str },
    #[error("tag is not attached to the library item")]
    TagNotAttached,
    #[error("library search limit must be between 1 and 100, got {value}")]
    InvalidLimit { value: u32 },
    #[error("storage upload idempotency key conflicts with an earlier upload: {key}")]
    StorageUploadIdempotencyConflict { key: String },
    #[error("storage upload object {0} was not found after a completed intent")]
    StorageUploadObjectMissing(Uuid),
    #[error("storage upload intent {0} was not found or already completed")]
    StorageUploadIntentMissing(Uuid),
    #[error("storage upload intent {0} is still active")]
    StorageUploadIntentActive(Uuid),
    #[error("storage upload intent {0} has no bound asset")]
    StorageUploadAssetMissing(Uuid),
    #[error("storage upload intent {0} generation overflowed")]
    StorageUploadGenerationOverflow(Uuid),
    #[error("storage upload intent {0} is not unknown")]
    StorageUploadIntentNotUnknown(Uuid),
    #[error("storage upload intent {intent_id} is bound to a different Telegram chat")]
    StorageUploadChatMismatch { intent_id: Uuid },
    #[error("storage upload completion does not match intent {intent_id}")]
    StorageUploadCompletionBindingConflict { intent_id: Uuid },
    #[error("storage upload asset {0} is not canonical")]
    StorageUploadAssetNotCanonical(Uuid),
    #[error("storage upload asset {0} does not match the intent digest")]
    StorageUploadAssetHashMismatch(Uuid),
    #[error("storage upload attachment message ID must be positive, got {value}")]
    StorageUploadMessageIdInvalid { value: i64 },
    #[error("storage upload attachment {field} must not be empty")]
    StorageUploadAttachmentFieldEmpty { field: &'static str },
    #[error("database returned unknown media kind: {0}")]
    UnknownMediaKind(String),
    #[error("storage upload lease duration must be greater than zero")]
    InvalidStorageLeaseDuration,
}
