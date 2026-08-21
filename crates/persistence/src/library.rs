use crate::cleanup::{enqueue_workspace_cleanup_for_media, lock_workspace_fence};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sooqa_jobs::{Job, JobCommand, JobLease, NewJob};
use sooqa_library::{
    CaptionSyncClaim, CaptionSyncCompletion, CaptionSyncFailure, CaptionSyncState,
    MAX_MEDIA_PREVIEW_BYTES, MAX_MEDIA_PREVIEW_HEIGHT, MAX_MEDIA_PREVIEW_WIDTH,
    MAX_VIDEO_DUPLICATE_EVIDENCE_BYTES, MAX_VIDEO_DUPLICATE_MATCHES, Media, MediaCursor,
    MediaDetails, MediaIngest, MediaKind, MediaLookup, MediaMetadata, MediaPage, MediaPreviewData,
    MediaPreviewMetadata, MediaSearchQuery, MediaSource, MediaSourceInput, MediaStorageState,
    MediaSummary, MediaUpdate, SourceKind, StorageCaptionMetadata, StorageReceipt,
    StorageUploadAttachment, StorageUploadInfo, StorageUploadReservation,
    StorageUploadReservationRequest, StorageUploadStore, TagValidationError,
    VideoDuplicateClassification, VideoFingerprintInput, VideoIdentityDecision,
    VideoIdentityOutcome, normalize_tag,
};
use sqlx::{Connection, FromRow, PgPool, Postgres, Transaction, pool::PoolConnection};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    jobs::{JobRepositoryError, JobSettlement},
    settlement::{lock_expired_job, lock_running_job, queue_parameters, update_locked_job},
};

pub use sooqa_library::VideoFingerprintCandidate;

const VIDEO_IDENTITY_ADVISORY_LOCK: i64 = 0x736f_6f71_615f_6964;
type CaptionSyncValues = (i32, &'static str, Option<String>, Option<Uuid>);

/// The bounded data read while the global identity session is held.  It is
/// intentionally limited to database retrieval; candidate decoding and
/// alignment belong to the worker/media boundary.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct VideoIdentityPreparation {
    pub exact_media_id: Option<Uuid>,
    pub candidates: Vec<VideoFingerprintCandidate>,
}

/// A session-level advisory lock held on one dedicated PostgreSQL connection.
/// The worker keeps this guard while it performs CPU alignment, then uses the
/// same connection for the short final transaction.  Callers must release it
/// explicitly so the pooled connection never retains a session lock.
pub struct VideoIdentitySession {
    connection: PoolConnection<Postgres>,
}

impl VideoIdentitySession {
    pub(crate) async fn acquire(pool: &PgPool) -> Result<Self, LibraryRepositoryError> {
        let mut connection = pool.acquire().await?;
        // A session advisory lock cannot be released from Drop.  Closing the
        // checked-out connection on cancellation is therefore safer than
        // returning a still-locked session to the pool.
        connection.close_on_drop();
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(VIDEO_IDENTITY_ADVISORY_LOCK)
            .execute(&mut *connection)
            .await?;
        Ok(Self { connection })
    }

    pub(crate) async fn begin(
        &mut self,
    ) -> Result<Transaction<'_, Postgres>, LibraryRepositoryError> {
        Ok((*self.connection).begin().await?)
    }

    pub(crate) async fn release(mut self) -> Result<(), LibraryRepositoryError> {
        let unlock = sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(VIDEO_IDENTITY_ADVISORY_LOCK)
            .execute(&mut *self.connection)
            .await;
        let close = self.connection.close().await;
        unlock?;
        close?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct LibraryRepository {
    pool: PgPool,
}

#[derive(Debug, Clone, FromRow)]
struct MediaRow {
    id: Uuid,
    kind: String,
    storage_state: String,
    canonical_sha256: Option<Vec<u8>>,
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
    preview_bytes: Option<Vec<u8>>,
    preview_mime_type: Option<String>,
    preview_width: Option<i32>,
    preview_height: Option<i32>,
    preview_sha256: Option<Vec<u8>>,
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
    caption_sync_generation: i32,
    caption_sync_state: String,
    caption_sync_error: Option<String>,
    caption_sync_claim_token: Option<Uuid>,
}

#[derive(Debug, Clone, FromRow)]
struct StorageCaptionMetadataRow {
    description: Option<String>,
    tags: Vec<String>,
    source_url: Option<String>,
}

#[derive(Debug, Clone, FromRow)]
struct MediaPreviewRow {
    preview_bytes: Option<Vec<u8>>,
    preview_mime_type: Option<String>,
    preview_width: Option<i32>,
    preview_height: Option<i32>,
    preview_sha256: Option<Vec<u8>>,
}

#[derive(Debug, Clone, FromRow)]
struct VideoFingerprintCandidateRow {
    media_id: Uuid,
    width: Option<i32>,
    height: Option<i32>,
    audio_codec: Option<String>,
    fingerprint_version: String,
    fingerprint_data: Vec<u8>,
    search_tokens: Vec<i64>,
    shared_token_count: i64,
    overlap_bps: i64,
}

impl VideoFingerprintCandidateRow {
    fn into_candidate(self) -> VideoFingerprintCandidate {
        VideoFingerprintCandidate {
            media_id: self.media_id,
            width: self.width,
            height: self.height,
            audio_codec: self.audio_codec,
            fingerprint_version: self.fingerprint_version,
            fingerprint_data: self.fingerprint_data,
            search_tokens: self.search_tokens,
            shared_token_count: self.shared_token_count,
            overlap_bps: self.overlap_bps,
        }
    }
}

impl LibraryRepository {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn find_media(&self, id: Uuid) -> Result<Option<Media>, LibraryRepositoryError> {
        self.load(id).await?.map(MediaRow::into_media).transpose()
    }

    pub async fn find_media_preview(
        &self,
        id: Uuid,
    ) -> Result<Option<MediaPreviewData>, LibraryRepositoryError> {
        let row = sqlx::query_as::<_, MediaPreviewRow>(
            "SELECT preview_bytes, preview_mime_type, preview_width, preview_height, preview_sha256 FROM media WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(LibraryRepositoryError::ResourceMissing(id))?;
        let (Some(bytes), Some(mime_type), Some(width), Some(height), Some(sha256)) = (
            row.preview_bytes,
            row.preview_mime_type,
            row.preview_width,
            row.preview_height,
            row.preview_sha256,
        ) else {
            return Ok(None);
        };
        let width = u32::try_from(width)
            .map_err(|_| LibraryRepositoryError::InvalidPreview("preview width is invalid"))?;
        let height = u32::try_from(height)
            .map_err(|_| LibraryRepositoryError::InvalidPreview("preview height is invalid"))?;
        if !matches!(mime_type.as_str(), "image/jpeg" | "image/png")
            || bytes.is_empty()
            || bytes.len() > MAX_MEDIA_PREVIEW_BYTES
            || width == 0
            || width > MAX_MEDIA_PREVIEW_WIDTH
            || height == 0
            || height > MAX_MEDIA_PREVIEW_HEIGHT
            || sha256.len() != 32
        {
            return Err(LibraryRepositoryError::InvalidPreview("preview violates its bounds"));
        }
        if Sha256::digest(&bytes).as_slice() != sha256.as_slice() {
            return Err(LibraryRepositoryError::InvalidPreview(
                "preview SHA-256 does not match bytes",
            ));
        }
        let (encoded_width, encoded_height) =
            sooqa_media::validate_bounded_preview_for_mime(&bytes, Some(&mime_type))
                .map_err(|error| LibraryRepositoryError::InvalidPreviewOwned(error.to_string()))?;
        if encoded_width != width || encoded_height != height {
            return Err(LibraryRepositoryError::InvalidPreview(
                "preview dimensions do not match its encoded image",
            ));
        }
        Ok(Some(MediaPreviewData {
            metadata: MediaPreviewMetadata {
                mime_type,
                width,
                height,
                size_bytes: u32::try_from(bytes.len()).map_err(|_| {
                    LibraryRepositoryError::InvalidPreview("preview size is invalid")
                })?,
                sha256,
            },
            bytes,
        }))
    }

    pub async fn find_storage_caption_metadata(
        &self,
        id: Uuid,
    ) -> Result<StorageCaptionMetadata, LibraryRepositoryError> {
        let row = sqlx::query_as::<_, StorageCaptionMetadataRow>(
            "SELECT description, tags, source_url FROM media WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(LibraryRepositoryError::ResourceMissing(id))?;
        Ok(StorageCaptionMetadata {
            description: row.description,
            tags: row.tags,
            source_url: row.source_url,
        })
    }

    pub async fn find_media_details(
        &self,
        id: Uuid,
    ) -> Result<Option<MediaDetails>, LibraryRepositoryError> {
        let Some(row) = self.load(id).await? else { return Ok(None) };
        let media = row.clone().into_media()?;
        let source = source_from_row(&row)?;
        Ok(Some(MediaDetails {
            storage_url: storage_message_url(
                row.telegram_storage_chat_id,
                row.telegram_storage_message_id,
            ),
            media,
            source,
        }))
    }

    pub async fn search_media(
        &self,
        query: MediaSearchQuery,
    ) -> Result<MediaPage, LibraryRepositoryError> {
        if !(1..=50).contains(&query.limit) {
            return Err(LibraryRepositoryError::InvalidLimit { value: query.limit });
        }
        let rows = sqlx::query_as::<_, MediaRow>(
            r#"
            SELECT * FROM media
            WHERE ($1::timestamptz IS NULL OR (updated_at, id) < ($1, $2))
            ORDER BY updated_at DESC, id DESC
            LIMIT $3
            "#,
        )
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
            .map(|row| MediaCursor { updated_at: row.updated_at, id: row.id });
        let items = rows
            .into_iter()
            .map(|row| self.summary_from_row(row))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(MediaPage { items, next_cursor })
    }

    /// Resolve one of the exact admin lookup forms without loading the media
    /// catalogue into memory. Source URL lookups accept the bounded list of
    /// official 2ch mirror variants prepared by the API layer.
    pub async fn lookup_media(
        &self,
        lookup: MediaLookup,
        limit: u32,
        cursor: Option<MediaCursor>,
    ) -> Result<MediaPage, LibraryRepositoryError> {
        if !(1..=50).contains(&limit) {
            return Err(LibraryRepositoryError::InvalidLimit { value: limit });
        }
        let (media_id, ingest_id, source_urls, storage_chat_id, storage_message_id) = match lookup {
            MediaLookup::Identifier(id) => (Some(id), Some(id), None, None, None),
            MediaLookup::MediaId(id) => (Some(id), None, None, None, None),
            MediaLookup::IngestId(id) => (None, Some(id), None, None, None),
            MediaLookup::SourceUrls(urls) => (None, None, Some(urls), None, None),
            MediaLookup::StorageMessage { chat_id, message_id } => {
                (None, None, None, Some(chat_id), Some(message_id))
            }
        };
        let rows = sqlx::query_as::<_, MediaRow>(
            r#"
            SELECT m.*
            FROM media AS m
            WHERE (
                ($1::uuid IS NOT NULL AND m.id = $1)
                OR ($2::uuid IS NOT NULL AND EXISTS (
                    SELECT 1 FROM ingests AS i WHERE i.id = $2 AND i.media_id = m.id
                ))
                OR ($3::text[] IS NOT NULL AND m.source_url = ANY($3::text[]))
                OR ($4::bigint IS NOT NULL AND m.telegram_storage_chat_id = $4
                    AND m.telegram_storage_message_id = $5)
            )
              AND ($6::timestamptz IS NULL OR (m.updated_at, m.id) < ($6, $7))
            ORDER BY m.updated_at DESC, m.id DESC
            LIMIT $8
            "#,
        )
        .bind(media_id)
        .bind(ingest_id)
        .bind(source_urls)
        .bind(storage_chat_id)
        .bind(storage_message_id)
        .bind(cursor.as_ref().map(|value| value.updated_at))
        .bind(cursor.as_ref().map(|value| value.id))
        .bind(i64::from(limit) + 1)
        .fetch_all(&self.pool)
        .await?;
        let has_more = rows.len() > limit as usize;
        let rows = rows.into_iter().take(limit as usize).collect::<Vec<_>>();
        let next_cursor = has_more
            .then(|| rows.last())
            .flatten()
            .map(|row| MediaCursor { updated_at: row.updated_at, id: row.id });
        let items = rows
            .into_iter()
            .map(|row| self.summary_from_row(row))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(MediaPage { items, next_cursor })
    }

    pub async fn count_ready_media(&self) -> Result<u64, LibraryRepositoryError> {
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM media WHERE storage_state = 'ready'",
        )
        .fetch_one(&self.pool)
        .await?;
        u64::try_from(count).map_err(|_| LibraryRepositoryError::InvalidCount)
    }

    pub async fn count_caption_sync_failures(&self) -> Result<u64, LibraryRepositoryError> {
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM media WHERE caption_sync_state = 'failed'",
        )
        .fetch_one(&self.pool)
        .await?;
        u64::try_from(count).map_err(|_| LibraryRepositoryError::InvalidCount)
    }

    pub async fn list_caption_sync_failures(
        &self,
        limit: u32,
    ) -> Result<Vec<CaptionSyncFailure>, LibraryRepositoryError> {
        if !(1..=50).contains(&limit) {
            return Err(LibraryRepositoryError::InvalidLimit { value: limit });
        }
        #[derive(Debug, sqlx::FromRow)]
        struct CaptionSyncFailureRow {
            media_id: Uuid,
            error_message: Option<String>,
        }

        let rows = sqlx::query_as::<_, CaptionSyncFailureRow>(
            "SELECT id AS media_id, left(caption_sync_error, 512) AS error_message FROM media WHERE caption_sync_state = 'failed' ORDER BY updated_at DESC, id DESC LIMIT $1",
        )
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| CaptionSyncFailure {
                media_id: row.media_id,
                error_message: row.error_message,
            })
            .collect())
    }

    pub async fn retry_caption_sync(&self, id: Uuid) -> Result<Media, LibraryRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query_as::<_, MediaRow>("SELECT * FROM media WHERE id = $1 FOR UPDATE")
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(LibraryRepositoryError::ResourceMissing(id))?;
        if !has_storage_message(&row) {
            return Err(LibraryRepositoryError::CaptionSyncUnavailable(id));
        }
        if row.caption_sync_state != "failed" {
            transaction.commit().await?;
            return row.into_media();
        }
        let generation = row
            .caption_sync_generation
            .checked_add(1)
            .ok_or(LibraryRepositoryError::CaptionSyncGenerationOverflow(id))?;
        let row = sqlx::query_as::<_, MediaRow>(
            "UPDATE media SET caption_sync_generation = $2, caption_sync_state = 'pending', caption_sync_error = NULL, caption_sync_claim_token = NULL, updated_at = now() WHERE id = $1 RETURNING *",
        )
        .bind(id)
        .bind(generation)
        .fetch_one(&mut *transaction)
        .await?;
        enqueue_caption_sync_job(&mut transaction, id, generation).await?;
        transaction.commit().await?;
        row.into_media()
    }

    pub async fn begin_caption_sync(
        &self,
        id: Uuid,
        generation: i32,
        claim_token: Uuid,
    ) -> Result<Option<CaptionSyncClaim>, LibraryRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query_as::<_, MediaRow>("SELECT * FROM media WHERE id = $1 FOR UPDATE")
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?;
        let Some(row) = row else {
            return Err(LibraryRepositoryError::ResourceMissing(id));
        };
        if row.caption_sync_generation != generation
            || !has_storage_message(&row)
            || row.caption_sync_state != "pending"
        {
            transaction.commit().await?;
            return Ok(None);
        }
        sqlx::query(
            "UPDATE media SET caption_sync_state = 'syncing', caption_sync_error = NULL, caption_sync_claim_token = $3, updated_at = now() WHERE id = $1 AND caption_sync_generation = $2 AND caption_sync_state = 'pending'",
        )
        .bind(id)
        .bind(generation)
        .bind(claim_token)
        .execute(&mut *transaction)
        .await?;
        let claim = CaptionSyncClaim {
            media_id: id,
            generation,
            claim_token,
            storage_chat_id: row.telegram_storage_chat_id.expect("storage message was checked"),
            storage_message_id: row
                .telegram_storage_message_id
                .expect("storage message was checked"),
            metadata: StorageCaptionMetadata {
                description: row.description,
                tags: row.tags,
                source_url: row.source_url,
            },
        };
        transaction.commit().await?;
        Ok(Some(claim))
    }

    pub async fn complete_caption_sync(
        &self,
        id: Uuid,
        generation: i32,
        claim_token: Uuid,
        succeeded: bool,
        retryable: bool,
        error_message: Option<&str>,
    ) -> Result<CaptionSyncCompletion, LibraryRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query_as::<_, MediaRow>("SELECT * FROM media WHERE id = $1 FOR UPDATE")
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(LibraryRepositoryError::ResourceMissing(id))?;
        if !has_storage_message(&row) || row.caption_sync_state == "not_required" {
            transaction.commit().await?;
            return Ok(CaptionSyncCompletion::Stale);
        }
        if row.caption_sync_generation != generation {
            sqlx::query(
                "UPDATE media SET caption_sync_state = 'pending', caption_sync_error = NULL, caption_sync_claim_token = NULL, updated_at = now() WHERE id = $1 AND caption_sync_generation = $2",
            )
            .bind(id)
            .bind(row.caption_sync_generation)
            .execute(&mut *transaction)
            .await?;
            enqueue_caption_sync_reapply_job(
                &mut transaction,
                id,
                row.caption_sync_generation,
                generation,
                claim_token,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CaptionSyncCompletion::Stale);
        }
        if row.caption_sync_state != "syncing" || row.caption_sync_claim_token != Some(claim_token)
        {
            transaction.commit().await?;
            return Ok(CaptionSyncCompletion::Stale);
        }
        let (state, error) = if succeeded {
            ("synced", None)
        } else if retryable {
            ("pending", None)
        } else {
            ("failed", error_message.map(|value| truncate_sync_error(value, 512)))
        };
        sqlx::query(
            "UPDATE media SET caption_sync_state = $2, caption_sync_error = $3, caption_sync_claim_token = NULL, updated_at = now() WHERE id = $1 AND caption_sync_generation = $4 AND caption_sync_claim_token = $5 AND caption_sync_state = 'syncing'",
        )
        .bind(id)
        .bind(state)
        .bind(error)
        .bind(generation)
        .bind(claim_token)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(CaptionSyncCompletion::Applied)
    }

    pub async fn update_media(
        &self,
        id: Uuid,
        update: MediaUpdate,
    ) -> Result<Media, LibraryRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let current = sqlx::query_as::<_, MediaRow>("SELECT * FROM media WHERE id = $1 FOR UPDATE")
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(LibraryRepositoryError::ResourceMissing(id))?;
        if current.updated_at != update.expected_updated_at {
            return Err(LibraryRepositoryError::OptimisticConflict(id));
        }
        let tags = normalize_tags(update.tags)?;
        let description = update.description;
        let caption_changed =
            description.as_ref() != current.description.as_ref() || tags.as_slice() != current.tags;
        let (
            caption_sync_generation,
            caption_sync_state,
            caption_sync_error,
            caption_sync_claim_token,
        ) = next_caption_sync_values(&current, caption_changed)?;
        let row = sqlx::query_as::<_, MediaRow>(
            "UPDATE media SET description = $2, tags = $3, caption_sync_generation = $4, caption_sync_state = $5, caption_sync_error = $6, caption_sync_claim_token = $7, updated_at = now() WHERE id = $1 RETURNING *",
        )
        .bind(id)
        .bind(description)
        .bind(tags)
        .bind(caption_sync_generation)
        .bind(caption_sync_state)
        .bind(caption_sync_error)
        .bind(caption_sync_claim_token)
        .fetch_one(&mut *transaction)
        .await?;
        if caption_changed && caption_sync_state == "pending" {
            enqueue_caption_sync_job(&mut transaction, id, caption_sync_generation).await?;
        }
        transaction.commit().await?;
        row.into_media()
    }

    pub async fn resolve_media(
        &self,
        ingest: MediaIngest,
    ) -> Result<MediaResolutionResult, LibraryRepositoryError> {
        let mut ingest = ingest;
        ingest.tags = normalize_tags(ingest.tags)?;
        validate_media_ingest(&ingest)?;
        let sha256 =
            ingest.metadata.sha256.as_deref().ok_or(LibraryRepositoryError::MissingSha256)?;
        let preview = preview_bindings(ingest.metadata.kind, &ingest.metadata.preview)?;
        let mut transaction = self.pool.begin().await?;
        let id = Uuid::now_v7();
        let source_value = source_to_value(&ingest.source);
        let inserted = sqlx::query_as::<_, MediaRow>(
            r#"INSERT INTO media (
                id, kind, storage_state, canonical_sha256, title, description,
                tags, source_url, source_metadata, mime_type, container,
                video_codec, audio_codec, width, height, duration_ms, bit_rate,
                file_size_bytes, local_work_path, preview_bytes, preview_mime_type,
                preview_width, preview_height, preview_sha256
            ) VALUES ($1, $2, 'pending_storage', $3, $4, $5, $6, $7, $8, $9,
                      $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                      $21, $22, $23)
            ON CONFLICT (canonical_sha256) DO NOTHING
            RETURNING *"#,
        )
        .bind(id)
        .bind(ingest.metadata.kind.as_str())
        .bind(sha256)
        .bind(&ingest.media.title)
        .bind(&ingest.media.description)
        .bind(&ingest.tags)
        .bind(ingest.source.normalized_url.clone().or(ingest.source.original_url.clone()))
        .bind(&source_value)
        .bind(&ingest.metadata.mime_type)
        .bind(&ingest.metadata.container)
        .bind(&ingest.metadata.video_codec)
        .bind(&ingest.metadata.audio_codec)
        .bind(ingest.metadata.width)
        .bind(ingest.metadata.height)
        .bind(to_i64(ingest.metadata.duration_ms, "duration_ms")?)
        .bind(to_i64(ingest.metadata.bit_rate, "bit_rate")?)
        .bind(to_i64(ingest.metadata.file_size_bytes, "file_size_bytes")?)
        .bind(&ingest.metadata.local_work_path)
        .bind(preview.0)
        .bind(preview.1)
        .bind(preview.2)
        .bind(preview.3)
        .bind(preview.4)
        .fetch_optional(&mut *transaction)
        .await?;

        let (row, media_created) = match inserted {
            Some(row) => (row, true),
            None => {
                let row = sqlx::query_as::<_, MediaRow>(
                    "SELECT * FROM media WHERE canonical_sha256 = $1 FOR UPDATE",
                )
                .bind(sha256)
                .fetch_one(&mut *transaction)
                .await?;
                let merged_tags = merge_tags(&row.tags, &ingest.tags);
                let description_changed = ingest
                    .media
                    .description
                    .as_ref()
                    .is_some_and(|description| Some(description) != row.description.as_ref());
                let caption_changed = description_changed || merged_tags != row.tags;
                let (
                    caption_sync_generation,
                    caption_sync_state,
                    caption_sync_error,
                    caption_sync_claim_token,
                ) = next_caption_sync_values(&row, caption_changed)?;
                let row = sqlx::query_as::<_, MediaRow>(
                    "UPDATE media SET tags = $2, title = COALESCE(title, $3), description = CASE WHEN $4::text IS NOT NULL THEN $4 ELSE description END, source_url = COALESCE(source_url, $5), source_metadata = $6, caption_sync_generation = $7, caption_sync_state = $8, caption_sync_error = $9, caption_sync_claim_token = $10, updated_at = now() WHERE id = $1 RETURNING *",
                )
                .bind(row.id)
                .bind(merged_tags)
                .bind(&ingest.media.title)
                .bind(&ingest.media.description)
                .bind(ingest.source.normalized_url.clone().or(ingest.source.original_url.clone()))
                .bind(merge_missing_source_metadata(&row.source_metadata, &source_value))
                .bind(caption_sync_generation)
                .bind(caption_sync_state)
                .bind(caption_sync_error)
                .bind(caption_sync_claim_token)
                .fetch_one(&mut *transaction)
                .await?;
                if caption_changed && caption_sync_state == "pending" {
                    enqueue_caption_sync_job(&mut transaction, row.id, caption_sync_generation)
                        .await?;
                }
                (row, false)
            }
        };
        transaction.commit().await?;
        let media = row.clone().into_media()?;
        let source = source_from_row(&row)?
            .ok_or(LibraryRepositoryError::Invariant("resolved media source is missing"))?;
        Ok(MediaResolutionResult { media, source, media_created })
    }

    pub(crate) async fn prepare_video_identity_in_transaction(
        transaction: &mut Transaction<'_, Postgres>,
        ingest: &MediaIngest,
        fingerprint: Option<&VideoFingerprintInput>,
        force_save: bool,
    ) -> Result<VideoIdentityPreparation, LibraryRepositoryError> {
        validate_media_ingest(ingest)?;
        if ingest.metadata.kind != MediaKind::Video {
            return Err(LibraryRepositoryError::InvalidVideoIdentityKind);
        }
        let sha256 =
            ingest.metadata.sha256.as_deref().ok_or(LibraryRepositoryError::MissingSha256)?;
        let exact_media_id =
            sqlx::query_scalar::<_, Uuid>("SELECT id FROM media WHERE canonical_sha256 = $1")
                .bind(sha256)
                .fetch_optional(&mut **transaction)
                .await?;
        if exact_media_id.is_some() {
            return Ok(VideoIdentityPreparation { exact_media_id, candidates: Vec::new() });
        }

        let fingerprint = fingerprint.ok_or(LibraryRepositoryError::MissingFingerprint)?;
        validate_video_fingerprint(fingerprint)?;
        let candidates = if force_save {
            Vec::new()
        } else {
            fetch_video_fingerprint_candidates(
                transaction,
                fingerprint.version(),
                fingerprint.search_tokens(),
            )
            .await?
            .into_iter()
            .map(VideoFingerprintCandidateRow::into_candidate)
            .collect()
        };
        Ok(VideoIdentityPreparation { exact_media_id, candidates })
    }

    pub(crate) async fn persist_video_identity_in_transaction(
        transaction: &mut Transaction<'_, Postgres>,
        ingest: &MediaIngest,
        fingerprint: Option<&VideoFingerprintInput>,
        decision: &VideoIdentityDecision,
        force_save: bool,
    ) -> Result<VideoIdentityOutcome, LibraryRepositoryError> {
        validate_media_ingest(ingest)?;
        let tags = normalize_tags(ingest.tags.clone())?;
        if ingest.metadata.kind != MediaKind::Video {
            return Err(LibraryRepositoryError::InvalidVideoIdentityKind);
        }
        let sha256 =
            ingest.metadata.sha256.as_deref().ok_or(LibraryRepositoryError::MissingSha256)?;
        let source_value = source_to_value(&ingest.source);
        if let Some(row) = sqlx::query_as::<_, MediaRow>(
            "SELECT * FROM media WHERE canonical_sha256 = $1 FOR UPDATE",
        )
        .bind(sha256)
        .fetch_optional(&mut **transaction)
        .await?
        {
            let merged_tags = merge_tags(&row.tags, &tags);
            let description_changed = ingest
                .media
                .description
                .as_ref()
                .is_some_and(|description| Some(description) != row.description.as_ref());
            let caption_changed = description_changed || merged_tags != row.tags;
            let (
                caption_sync_generation,
                caption_sync_state,
                caption_sync_error,
                caption_sync_claim_token,
            ) = next_caption_sync_values(&row, caption_changed)?;
            sqlx::query(
                "UPDATE media SET tags = $2, title = COALESCE(title, $3), description = CASE WHEN $4::text IS NOT NULL THEN $4 ELSE description END, source_url = COALESCE(source_url, $5), source_metadata = $6, caption_sync_generation = $7, caption_sync_state = $8, caption_sync_error = $9, caption_sync_claim_token = $10, updated_at = now() WHERE id = $1",
            )
            .bind(row.id)
            .bind(merged_tags)
            .bind(&ingest.media.title)
            .bind(&ingest.media.description)
            .bind(ingest.source.normalized_url.clone().or(ingest.source.original_url.clone()))
            .bind(merge_missing_source_metadata(&row.source_metadata, &source_value))
            .bind(caption_sync_generation)
            .bind(caption_sync_state)
            .bind(caption_sync_error)
            .bind(caption_sync_claim_token)
            .execute(&mut **transaction)
            .await?;
            if caption_changed && caption_sync_state == "pending" {
                enqueue_caption_sync_job(transaction, row.id, caption_sync_generation).await?;
            }
            return Ok(VideoIdentityOutcome::ExactDuplicate { media_id: row.id });
        }

        let fingerprint = fingerprint.ok_or(LibraryRepositoryError::MissingFingerprint)?;
        validate_video_fingerprint(fingerprint)?;
        if let VideoIdentityDecision::DuplicatePending { evidence } = decision {
            validate_duplicate_evidence(evidence, fingerprint.version())?;
            if !force_save {
                let encoded = serde_json::to_vec(evidence)?;
                if encoded.len() > MAX_VIDEO_DUPLICATE_EVIDENCE_BYTES {
                    return Err(LibraryRepositoryError::DuplicateEvidenceTooLarge {
                        max: MAX_VIDEO_DUPLICATE_EVIDENCE_BYTES,
                    });
                }
                return Ok(VideoIdentityOutcome::DuplicatePending { evidence: evidence.clone() });
            }
        }

        let preview = preview_bindings(ingest.metadata.kind, &ingest.metadata.preview)?;
        let id = Uuid::now_v7();
        let inserted = sqlx::query_as::<_, MediaRow>(
            r#"INSERT INTO media (
                id, kind, storage_state, canonical_sha256, fingerprint_version,
                fingerprint_data, fingerprint_search_tokens, title, description,
                tags, source_url, source_metadata, mime_type, container,
                video_codec, audio_codec, width, height, duration_ms, bit_rate,
                file_size_bytes, local_work_path, preview_bytes, preview_mime_type,
                preview_width, preview_height, preview_sha256
            ) VALUES ($1, 'video', 'pending_storage', $2, $3, $4, $5, $6, $7,
                      $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18,
                      $19, $20, $21, $22, $23, $24, $25)
            ON CONFLICT (canonical_sha256) DO NOTHING
            RETURNING *"#,
        )
        .bind(id)
        .bind(sha256)
        .bind(fingerprint.version())
        .bind(fingerprint.data())
        .bind(fingerprint.search_tokens())
        .bind(&ingest.media.title)
        .bind(&ingest.media.description)
        .bind(&tags)
        .bind(ingest.source.normalized_url.clone().or(ingest.source.original_url.clone()))
        .bind(&source_value)
        .bind(&ingest.metadata.mime_type)
        .bind(&ingest.metadata.container)
        .bind(&ingest.metadata.video_codec)
        .bind(&ingest.metadata.audio_codec)
        .bind(ingest.metadata.width)
        .bind(ingest.metadata.height)
        .bind(to_i64(ingest.metadata.duration_ms, "duration_ms")?)
        .bind(to_i64(ingest.metadata.bit_rate, "bit_rate")?)
        .bind(to_i64(ingest.metadata.file_size_bytes, "file_size_bytes")?)
        .bind(&ingest.metadata.local_work_path)
        .bind(preview.0)
        .bind(preview.1)
        .bind(preview.2)
        .bind(preview.3)
        .bind(preview.4)
        .fetch_optional(&mut **transaction)
        .await?;
        if let Some(row) = inserted {
            let media_id = row.id;
            return Ok(VideoIdentityOutcome::NewMedia { media_id });
        }

        let media_id = sqlx::query_scalar::<_, Uuid>(
            "SELECT id FROM media WHERE canonical_sha256 = $1 FOR UPDATE",
        )
        .bind(sha256)
        .fetch_one(&mut **transaction)
        .await?;
        Ok(VideoIdentityOutcome::ExactDuplicate { media_id })
    }

    /// Resolve an already-known canonical byte identity before any video
    /// fingerprint extraction. The final video identity transaction still
    /// rechecks this value under its advisory lock because a concurrent ingest
    /// may insert the exact bytes after this preflight returns `None`.
    pub async fn resolve_exact_sha(
        &self,
        ingest: &MediaIngest,
    ) -> Result<Option<Uuid>, LibraryRepositoryError> {
        validate_media_ingest(ingest)?;
        let sha256 =
            ingest.metadata.sha256.as_deref().ok_or(LibraryRepositoryError::MissingSha256)?;
        sqlx::query_scalar::<_, Uuid>("SELECT id FROM media WHERE canonical_sha256 = $1")
            .bind(sha256)
            .fetch_optional(&self.pool)
            .await
            .map_err(Into::into)
    }

    pub async fn record_media_metadata(
        &self,
        id: Uuid,
        metadata: MediaMetadata,
    ) -> Result<Media, LibraryRepositoryError> {
        let preview = preview_bindings(metadata.kind, &metadata.preview)?;
        let row = sqlx::query_as::<_, MediaRow>(
            r#"UPDATE media SET kind = $2, canonical_sha256 = $3,
                mime_type = $4, container = $5, video_codec = $6, audio_codec = $7,
                width = $8, height = $9, duration_ms = $10, bit_rate = $11,
                file_size_bytes = $12, local_work_path = $13,
                preview_bytes = $14, preview_mime_type = $15, preview_width = $16,
                preview_height = $17, preview_sha256 = $18,
                storage_state = CASE WHEN storage_state IN ('ready', 'storage_unknown')
                    THEN storage_state ELSE 'pending_storage' END,
                updated_at = now() WHERE id = $1 RETURNING *"#,
        )
        .bind(id)
        .bind(metadata.kind.as_str())
        .bind(metadata.sha256)
        .bind(metadata.mime_type)
        .bind(metadata.container)
        .bind(metadata.video_codec)
        .bind(metadata.audio_codec)
        .bind(metadata.width)
        .bind(metadata.height)
        .bind(to_i64(metadata.duration_ms, "duration_ms")?)
        .bind(to_i64(metadata.bit_rate, "bit_rate")?)
        .bind(to_i64(metadata.file_size_bytes, "file_size_bytes")?)
        .bind(metadata.local_work_path)
        .bind(preview.0)
        .bind(preview.1)
        .bind(preview.2)
        .bind(preview.3)
        .bind(preview.4)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(LibraryRepositoryError::ResourceMissing(id))?;
        row.into_media()
    }

    pub async fn record_video_sequence_fingerprint(
        &self,
        media_id: Uuid,
        fingerprint: &VideoFingerprintInput,
    ) -> Result<(), LibraryRepositoryError> {
        validate_video_fingerprint(fingerprint)?;
        let updated = sqlx::query(
            "UPDATE media SET fingerprint_version = $2, fingerprint_data = $3, fingerprint_search_tokens = $4, updated_at = now() WHERE id = $1 AND kind = 'video'",
        )
        .bind(media_id)
        .bind(fingerprint.version())
        .bind(fingerprint.data())
        .bind(fingerprint.search_tokens())
        .execute(&self.pool)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(LibraryRepositoryError::ResourceMissing(media_id));
        }
        Ok(())
    }

    pub async fn list_video_fingerprint_candidates(
        &self,
        exclude_media_id: Uuid,
        fingerprint_version: &str,
        search_tokens: &[i64],
    ) -> Result<Vec<VideoFingerprintCandidate>, LibraryRepositoryError> {
        if search_tokens.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query_as::<_, VideoFingerprintCandidateRow>(
            r#"
            WITH candidates AS (
                SELECT
                    m.id AS media_id,
                    m.width,
                    m.height,
                    m.audio_codec,
                    m.fingerprint_version,
                    m.fingerprint_data,
                    m.fingerprint_search_tokens AS search_tokens,
                    cardinality(ARRAY(
                        SELECT DISTINCT candidate_token
                        FROM unnest(m.fingerprint_search_tokens) AS candidate_token
                        WHERE candidate_token = ANY($3::bigint[])
                    ))::bigint AS shared_token_count,
                    cardinality(ARRAY(
                        SELECT DISTINCT candidate_token
                        FROM unnest(m.fingerprint_search_tokens) AS candidate_token
                    ))::bigint AS candidate_token_count,
                    cardinality(ARRAY(
                        SELECT DISTINCT query_token
                        FROM unnest($3::bigint[]) AS query_token
                    ))::bigint AS query_token_count
                FROM media AS m
                WHERE m.id <> $1
                  AND m.kind = 'video'
                  AND m.fingerprint_version = $2
                  AND m.fingerprint_data IS NOT NULL
                  AND m.fingerprint_search_tokens IS NOT NULL
                  AND m.storage_state IN ('pending_storage', 'ready')
                  AND m.fingerprint_search_tokens && $3::bigint[]
            )
            SELECT
                media_id,
                width,
                height,
                audio_codec,
                fingerprint_version,
                fingerprint_data,
                search_tokens,
                shared_token_count,
                (
                    shared_token_count * 10000
                    / NULLIF(LEAST(candidate_token_count, query_token_count), 0)
                )::bigint AS overlap_bps
            FROM candidates
            WHERE shared_token_count >= 8
              AND shared_token_count * 10 >= LEAST(candidate_token_count, query_token_count)
            ORDER BY shared_token_count DESC, overlap_bps DESC, media_id
            LIMIT 20
            "#,
        )
        .bind(exclude_media_id)
        .bind(fingerprint_version)
        .bind(search_tokens)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(VideoFingerprintCandidateRow::into_candidate).collect())
    }

    pub async fn list_storage_uploads(
        &self,
    ) -> Result<Vec<StorageUploadInfo>, LibraryRepositoryError> {
        let rows = sqlx::query_as::<_, MediaRow>(
            "SELECT * FROM media WHERE storage_state IN ('pending_storage', 'storage_unknown') ORDER BY created_at, id",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(MediaRow::into_storage_info).collect()
    }

    pub async fn mark_storage_upload_unknown(
        &self,
        id: Uuid,
        force: bool,
    ) -> Result<(), LibraryRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query_as::<_, MediaRow>("SELECT * FROM media WHERE id = $1 FOR UPDATE")
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(LibraryRepositoryError::ResourceMissing(id))?;
        if row.storage_token.is_some() && !force {
            return Err(LibraryRepositoryError::StorageUploadActive(id));
        }
        sqlx::query(
            "UPDATE media SET storage_state = 'storage_unknown', storage_token = NULL, storage_started_at = NULL, updated_at = now() WHERE id = $1",
        )
        .bind(id)
        .execute(&mut *transaction)
        .await?;
        mark_linked_ingests_storage_unknown(&mut transaction, id).await?;
        settle_queued_storage_upload_jobs(&mut transaction, id).await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn reset_storage_upload(&self, id: Uuid) -> Result<(), LibraryRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        lock_workspace_fence(&mut transaction, id).await?;
        let row = sqlx::query_as::<_, MediaRow>("SELECT * FROM media WHERE id = $1 FOR UPDATE")
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(LibraryRepositoryError::ResourceMissing(id))?;
        if row.storage_token.is_some() {
            return Err(LibraryRepositoryError::StorageUploadActive(id));
        }
        if row.local_work_path.is_none() {
            return Err(LibraryRepositoryError::WorkspaceReclaimed(id));
        }
        let cleanup_running = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM queue.jobs AS cleanup
                JOIN ingests AS ingest
                  ON cleanup.payload->>'ingest_id' = ingest.id::text
                 AND cleanup.payload->>'workspace_id' = ingest.workspace_id::text
                WHERE cleanup.kind = 'cleanup_workspace'
                  AND cleanup.state = 'running'
                  AND ingest.media_id = $1
            )
            "#,
        )
        .bind(id)
        .fetch_one(&mut *transaction)
        .await?;
        if cleanup_running {
            return Err(LibraryRepositoryError::WorkspaceReclaimed(id));
        }
        let generation = row
            .storage_generation
            .checked_add(1)
            .ok_or(LibraryRepositoryError::StorageGenerationOverflow(id))?;
        sqlx::query(
            "UPDATE media SET storage_state = 'pending_storage', storage_generation = $2, telegram_storage_chat_id = NULL, telegram_storage_message_id = NULL, telegram_file_id = NULL, telegram_file_unique_id = NULL, storage_token = NULL, storage_started_at = NULL, stored_at = NULL, caption_sync_state = 'not_required', caption_sync_error = NULL, caption_sync_claim_token = NULL, updated_at = now() WHERE id = $1",
        )
        .bind(id)
        .bind(generation)
        .execute(&mut *transaction)
        .await?;
        reopen_linked_ingests_for_storage(&mut transaction, id).await?;
        let job = NewJob::upload_storage_asset_generation(id, generation)
            .dedupe_key(format!("media:{id}:upload_storage:v1:{generation}"));
        sqlx::query(
            "INSERT INTO queue.jobs (kind, payload, state, run_at, max_attempts, dedupe_key) VALUES ($1, $2, 'queued', COALESCE($3, now()), $4, $5) ON CONFLICT (dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING",
        )
        .bind(job.job_type().as_str())
        .bind(job.payload_json())
        .bind(job.run_at_value())
        .bind(job.max_attempts_value())
        .bind(job.dedupe_key_value())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn attach_storage_upload(
        &self,
        id: Uuid,
        generation: i32,
        attachment: StorageUploadAttachment,
    ) -> Result<StorageReceipt, LibraryRepositoryError> {
        validate_attachment(&attachment)?;
        let mut transaction = self.pool.begin().await?;
        let mut row = sqlx::query_as::<_, MediaRow>(
            "UPDATE media SET storage_state = 'ready', telegram_storage_chat_id = $3, telegram_storage_message_id = $4, telegram_file_id = $5, telegram_file_unique_id = $6, storage_token = NULL, storage_started_at = NULL, local_work_path = NULL, stored_at = now(), updated_at = now() WHERE id = $1 AND storage_generation = $2 AND storage_state = 'storage_unknown' RETURNING *",
        )
        .bind(id)
        .bind(generation)
        .bind(attachment.storage_chat_id)
        .bind(attachment.storage_message_id)
        .bind(attachment.telegram_file_id)
        .bind(attachment.telegram_file_unique_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(LibraryRepositoryError::StorageUploadNotUnknown(id))?;
        let (caption_generation, caption_state, caption_error, caption_claim_token) =
            caption_sync_values_after_storage_upload(&row, attachment.caption_metadata.as_ref())?;
        row = sqlx::query_as::<_, MediaRow>(
            "UPDATE media SET caption_sync_generation = $2, caption_sync_state = $3, caption_sync_error = $4, caption_sync_claim_token = $5, updated_at = now() WHERE id = $1 RETURNING *",
        )
        .bind(id)
        .bind(caption_generation)
        .bind(caption_state)
        .bind(caption_error)
        .bind(caption_claim_token)
        .fetch_one(&mut *transaction)
        .await?;
        if caption_state == "pending" {
            enqueue_caption_sync_job(&mut transaction, id, caption_generation).await?;
        }
        complete_linked_ingests_for_storage(&mut transaction, id).await?;
        enqueue_workspace_cleanup_for_media(&mut transaction, id, OffsetDateTime::now_utc())
            .await?;
        transaction.commit().await?;
        row.into_storage_receipt()
    }

    async fn load(&self, id: Uuid) -> Result<Option<MediaRow>, LibraryRepositoryError> {
        Ok(sqlx::query_as::<_, MediaRow>("SELECT * FROM media WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?)
    }

    fn summary_from_row(&self, row: MediaRow) -> Result<MediaSummary, LibraryRepositoryError> {
        let media = row.clone().into_media()?;
        let source = source_from_row(&row)?;
        Ok(MediaSummary {
            media,
            source_url: row.source_url,
            source_original_url: source.as_ref().and_then(|value| value.original_url.clone()),
            source_metadata: source.map(|source| source.metadata),
            storage_url: storage_message_url(
                row.telegram_storage_chat_id,
                row.telegram_storage_message_id,
            ),
        })
    }
}

fn validate_video_fingerprint(
    fingerprint: &VideoFingerprintInput,
) -> Result<(), LibraryRepositoryError> {
    fingerprint
        .validate()
        .map_err(|error| LibraryRepositoryError::InvalidFingerprint(error.to_string()))
}

fn validate_duplicate_evidence(
    evidence: &sooqa_library::VideoDuplicateEvidence,
    fingerprint_version: &str,
) -> Result<(), LibraryRepositoryError> {
    if evidence.matches.is_empty() {
        return Err(LibraryRepositoryError::DuplicateEvidenceEmpty);
    }
    if evidence.matches.len() > MAX_VIDEO_DUPLICATE_MATCHES {
        return Err(LibraryRepositoryError::DuplicateEvidenceTooManyMatches {
            max: MAX_VIDEO_DUPLICATE_MATCHES,
        });
    }
    if evidence.algorithm_version != fingerprint_version {
        return Err(LibraryRepositoryError::DuplicateEvidenceAlgorithmVersionMismatch {
            expected: fingerprint_version.to_owned(),
            actual: evidence.algorithm_version.clone(),
        });
    }
    if !evidence
        .matches
        .iter()
        .any(|item| item.classification == VideoDuplicateClassification::StrongDuplicate)
    {
        return Err(LibraryRepositoryError::DuplicateEvidenceMissingStrongMatch);
    }
    for item in &evidence.matches {
        if item.fingerprint_version != fingerprint_version {
            return Err(LibraryRepositoryError::DuplicateEvidenceFingerprintVersionMismatch {
                expected: fingerprint_version.to_owned(),
                actual: item.fingerprint_version.clone(),
            });
        }
        for (field, value) in [
            ("incoming_coverage_bps", i64::from(item.incoming_coverage_bps)),
            ("candidate_coverage_bps", i64::from(item.candidate_coverage_bps)),
            ("median_distance_bps", i64::from(item.median_distance_bps)),
            ("high_percentile_distance_bps", i64::from(item.high_percentile_distance_bps)),
            ("score_bps", i64::from(item.score_bps)),
            ("token_overlap_bps", item.token_overlap_bps),
        ] {
            if !(0..=10_000).contains(&value) {
                return Err(LibraryRepositoryError::DuplicateEvidenceInvalidBasisPoints {
                    field,
                    value,
                });
            }
        }
        if item.shared_token_count < 0 {
            return Err(LibraryRepositoryError::DuplicateEvidenceInvalidSharedTokenCount {
                value: item.shared_token_count,
            });
        }
    }
    Ok(())
}

async fn fetch_video_fingerprint_candidates(
    transaction: &mut Transaction<'_, Postgres>,
    fingerprint_version: &str,
    search_tokens: &[i64],
) -> Result<Vec<VideoFingerprintCandidateRow>, sqlx::Error> {
    if search_tokens.is_empty() {
        return Ok(Vec::new());
    }
    sqlx::query_as::<_, VideoFingerprintCandidateRow>(
        r#"
        WITH candidates AS (
            SELECT
                m.id AS media_id,
                m.width,
                m.height,
                m.audio_codec,
                m.fingerprint_version,
                m.fingerprint_data,
                m.fingerprint_search_tokens AS search_tokens,
                cardinality(ARRAY(
                    SELECT DISTINCT candidate_token
                    FROM unnest(m.fingerprint_search_tokens) AS candidate_token
                    WHERE candidate_token = ANY($2::bigint[])
                ))::bigint AS shared_token_count,
                cardinality(ARRAY(
                    SELECT DISTINCT candidate_token
                    FROM unnest(m.fingerprint_search_tokens) AS candidate_token
                ))::bigint AS candidate_token_count,
                cardinality(ARRAY(
                    SELECT DISTINCT query_token
                    FROM unnest($2::bigint[]) AS query_token
                ))::bigint AS query_token_count
            FROM media AS m
            WHERE m.kind = 'video'
              AND m.fingerprint_version = $1
              AND m.fingerprint_data IS NOT NULL
              AND m.fingerprint_search_tokens IS NOT NULL
              AND m.storage_state IN ('pending_storage', 'ready')
              AND m.fingerprint_search_tokens && $2::bigint[]
        )
        SELECT
            media_id,
            width,
            height,
            audio_codec,
            fingerprint_version,
            fingerprint_data,
            search_tokens,
            shared_token_count,
            (
                shared_token_count * 10000
                / NULLIF(LEAST(candidate_token_count, query_token_count), 0)
            )::bigint AS overlap_bps
        FROM candidates
        WHERE shared_token_count >= 8
          AND shared_token_count * 10 >= LEAST(candidate_token_count, query_token_count)
        ORDER BY shared_token_count DESC, overlap_bps DESC, media_id
        LIMIT 20
        "#,
    )
    .bind(fingerprint_version)
    .bind(search_tokens)
    .fetch_all(&mut **transaction)
    .await
}

pub type MediaResolutionResult = sooqa_library::MediaResolution;

#[async_trait]
impl StorageUploadStore for LibraryRepository {
    type Error = LibraryRepositoryError;

    async fn find_media(&self, media_id: Uuid) -> Result<Option<Media>, Self::Error> {
        LibraryRepository::find_media(self, media_id).await
    }

    async fn find_media_preview(
        &self,
        media_id: Uuid,
    ) -> Result<Option<MediaPreviewData>, Self::Error> {
        LibraryRepository::find_media_preview(self, media_id).await
    }

    async fn find_storage_caption_metadata(
        &self,
        media_id: Uuid,
    ) -> Result<StorageCaptionMetadata, Self::Error> {
        LibraryRepository::find_storage_caption_metadata(self, media_id).await
    }

    async fn find_storage_receipt(
        &self,
        media_id: Uuid,
    ) -> Result<Option<StorageReceipt>, Self::Error> {
        let row = self.load(media_id).await?;
        row.map(|row| {
            if row.storage_state == "ready" {
                row.into_storage_receipt().map(Some)
            } else {
                Ok(None)
            }
        })
        .transpose()
        .map(|value| value.flatten())
    }

    async fn reserve_storage_upload(
        &self,
        request: StorageUploadReservationRequest,
    ) -> Result<StorageUploadReservation, Self::Error> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query_as::<_, MediaRow>("SELECT * FROM media WHERE id = $1 FOR UPDATE")
            .bind(request.media_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(LibraryRepositoryError::ResourceMissing(request.media_id))?;
        if row.storage_state == "ready" {
            let receipt = row.into_storage_receipt()?;
            transaction.commit().await?;
            return Ok(StorageUploadReservation::Reused(receipt));
        }
        if row.storage_generation != request.generation {
            transaction.commit().await?;
            return Ok(StorageUploadReservation::StaleGeneration {
                current_generation: row.storage_generation,
            });
        }
        if matches!(row.storage_state.as_str(), "storage_unknown" | "missing") {
            transaction.commit().await?;
            return Ok(StorageUploadReservation::ReconciliationRequired);
        }
        if row.storage_token.is_some() {
            let retry_at =
                row.storage_started_at.map(|started| started + time::Duration::seconds(600));
            transaction.commit().await?;
            return Ok(StorageUploadReservation::InProgress { retry_at });
        }
        let owner_token = Uuid::now_v7();
        let updated = sqlx::query(
            "UPDATE media SET storage_token = $2, storage_started_at = now(), updated_at = now() WHERE id = $1 AND storage_state = 'pending_storage' AND storage_generation = $3 AND storage_token IS NULL",
        )
        .bind(request.media_id)
        .bind(owner_token)
        .bind(request.generation)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        if updated.rows_affected() != 1 {
            return Ok(StorageUploadReservation::InProgress { retry_at: None });
        }
        Ok(StorageUploadReservation::Reserved {
            media_id: request.media_id,
            owner_token,
            caption_metadata: storage_caption_metadata_from_row(&row),
        })
    }

    async fn renew_storage_upload(
        &self,
        media_id: Uuid,
        owner_token: Uuid,
        lease_duration: Duration,
    ) -> Result<OffsetDateTime, Self::Error> {
        let seconds = i64::try_from(lease_duration.as_secs())
            .map_err(|_| LibraryRepositoryError::InvalidLeaseDuration)?;
        let updated = sqlx::query_scalar::<_, OffsetDateTime>(
            "UPDATE media SET storage_started_at = now(), updated_at = now() WHERE id = $1 AND storage_token = $2 RETURNING storage_started_at + ($3 * interval '1 second')",
        )
        .bind(media_id)
        .bind(owner_token)
        .bind(seconds)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(LibraryRepositoryError::StorageUploadLeaseLost(media_id))?;
        Ok(updated)
    }

    async fn complete_storage_upload(
        &self,
        media_id: Uuid,
        owner_token: Uuid,
        attachment: StorageUploadAttachment,
    ) -> Result<StorageReceipt, Self::Error> {
        validate_attachment(&attachment)?;
        let mut transaction = self.pool.begin().await?;
        let mut row = sqlx::query_as::<_, MediaRow>(
            "UPDATE media SET storage_state = 'ready', telegram_storage_chat_id = $2, telegram_storage_message_id = $3, telegram_file_id = $4, telegram_file_unique_id = $5, storage_token = NULL, storage_started_at = NULL, local_work_path = NULL, stored_at = now(), updated_at = now() WHERE id = $1 AND storage_token = $6 AND storage_state = 'pending_storage' RETURNING *",
        )
        .bind(media_id)
        .bind(attachment.storage_chat_id)
        .bind(attachment.storage_message_id)
        .bind(attachment.telegram_file_id)
        .bind(attachment.telegram_file_unique_id)
        .bind(owner_token)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(LibraryRepositoryError::StorageUploadLeaseLost(media_id))?;
        let (caption_generation, caption_state, caption_error, caption_claim_token) =
            caption_sync_values_after_storage_upload(&row, attachment.caption_metadata.as_ref())?;
        row = sqlx::query_as::<_, MediaRow>(
            "UPDATE media SET caption_sync_generation = $2, caption_sync_state = $3, caption_sync_error = $4, caption_sync_claim_token = $5, updated_at = now() WHERE id = $1 RETURNING *",
        )
        .bind(media_id)
        .bind(caption_generation)
        .bind(caption_state)
        .bind(caption_error)
        .bind(caption_claim_token)
        .fetch_one(&mut *transaction)
        .await?;
        if caption_state == "pending" {
            enqueue_caption_sync_job(&mut transaction, media_id, caption_generation).await?;
        }
        complete_linked_ingests_for_storage(&mut transaction, media_id).await?;
        enqueue_workspace_cleanup_for_media(&mut transaction, media_id, OffsetDateTime::now_utc())
            .await?;
        transaction.commit().await?;
        row.into_storage_receipt()
    }

    async fn release_storage_upload(
        &self,
        media_id: Uuid,
        owner_token: Uuid,
    ) -> Result<(), Self::Error> {
        sqlx::query(
            "UPDATE media SET storage_state = 'pending_storage', storage_token = NULL, storage_started_at = NULL, updated_at = now() WHERE id = $1 AND storage_token = $2",
        )
        .bind(media_id)
        .bind(owner_token)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn mark_storage_upload_unknown(
        &self,
        media_id: Uuid,
        owner_token: Uuid,
    ) -> Result<(), Self::Error> {
        let mut transaction = self.pool.begin().await?;
        let updated = sqlx::query(
            "UPDATE media SET storage_state = 'storage_unknown', storage_token = NULL, storage_started_at = NULL, updated_at = now() WHERE id = $1 AND storage_token = $2 AND storage_state = 'pending_storage'",
        )
        .bind(media_id)
        .bind(owner_token)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(LibraryRepositoryError::StorageUploadLeaseLost(media_id));
        }
        mark_linked_ingests_storage_unknown(&mut transaction, media_id).await?;
        settle_queued_storage_upload_jobs(&mut transaction, media_id).await?;
        transaction.commit().await?;
        Ok(())
    }
}

async fn mark_linked_ingests_storage_unknown(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    media_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE ingests SET state = 'failed_terminal', error_code = 'storage_unknown', error_message = 'media storage requires explicit reconciliation', completed_at = now(), updated_at = now() WHERE media_id = $1 AND (state NOT IN ('cancelled', 'failed_terminal') OR error_code IN ('storage_upload', 'storage_unknown'))",
    )
    .bind(media_id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn settle_queued_storage_upload_jobs(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    media_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE queue.jobs SET state = 'failed', lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL, last_heartbeat_at = NULL, error_class = 'storage_upload_unknown', error_message = 'media storage requires explicit reconciliation', completed_at = now(), updated_at = now() WHERE kind = 'upload_storage_asset' AND state = 'queued' AND payload->>'media_id' = $1",
    )
    .bind(media_id.to_string())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

/// Retention fence for media-owned technical jobs.  A storage upload is only
/// replay-safe once the media row says the effect is definitively present or
/// definitively absent.  `storage_unknown` and a pending generation are kept
/// for explicit reconciliation.  Caption jobs are replay-safe only when the
/// current generation is no longer actionable (`synced`, `not_required`, or
/// an explicit `failed` state); `syncing` remains an unresolved external
/// effect.
pub(crate) async fn terminal_job_retention_eligible(
    transaction: &mut Transaction<'_, Postgres>,
    job: &Job,
) -> Result<bool, sqlx::Error> {
    match &job.command {
        JobCommand::UploadStorageAsset(payload) => {
            let Some(state) = sqlx::query_scalar::<_, String>(
                "SELECT storage_state FROM media WHERE id = $1 FOR UPDATE",
            )
            .bind(payload.media_id)
            .fetch_optional(&mut **transaction)
            .await?
            else {
                return Ok(false);
            };
            Ok(matches!(state.as_str(), "ready" | "missing"))
        }
        JobCommand::SyncStorageCaption(payload) => {
            let Some(state) = sqlx::query_scalar::<_, String>(
                "SELECT caption_sync_state FROM media WHERE id = $1 FOR UPDATE",
            )
            .bind(payload.media_id)
            .fetch_optional(&mut **transaction)
            .await?
            else {
                return Ok(false);
            };
            Ok(matches!(state.as_str(), "not_required" | "synced" | "failed"))
        }
        _ => Ok(false),
    }
}

async fn reopen_linked_ingests_for_storage(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    media_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE ingests SET state = 'storing', error_code = NULL, error_message = NULL, completed_at = NULL, updated_at = now() WHERE media_id = $1 AND state <> 'cancelled' AND (state IN ('completed', 'storing') OR error_code IN ('storage_upload', 'storage_unknown'))",
    )
    .bind(media_id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn complete_linked_ingests_for_storage(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    media_id: Uuid,
) -> Result<(), sqlx::Error> {
    let updated = sqlx::query_as::<_, (Uuid, String)>(
        "UPDATE ingests SET state = 'completed', error_code = NULL, error_message = NULL, completed_at = now(), updated_at = now() WHERE media_id = $1 AND state <> 'cancelled' AND (state = 'storing' OR (state = 'failed_retryable' AND error_code IN ('storage_upload', 'storage_unknown')) OR (state = 'failed_terminal' AND error_code IN ('storage_upload', 'storage_unknown'))) RETURNING id, requested_action",
    )
    .bind(media_id)
    .fetch_all(&mut **transaction)
    .await?;
    for (ingest_id, requested_action) in updated {
        if requested_action == "save" {
            continue;
        }
        let job = NewJob::materialize_publication(ingest_id)
            .dedupe_key(format!("ingest:{ingest_id}:materialize_publication:v1"));
        sqlx::query(
            "INSERT INTO queue.jobs (kind, payload, state, run_at, max_attempts, dedupe_key) VALUES ($1, $2, 'queued', COALESCE($3, now()), $4, $5) ON CONFLICT (dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING",
        )
        .bind(job.job_type().as_str())
        .bind(job.payload_json())
        .bind(job.run_at_value())
        .bind(job.max_attempts_value())
        .bind(job.dedupe_key_value())
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}

impl MediaRow {
    fn into_media(self) -> Result<Media, LibraryRepositoryError> {
        let preview = self.preview_metadata()?;
        Ok(Media {
            id: self.id,
            kind: MediaKind::try_from(self.kind.as_str())
                .map_err(LibraryRepositoryError::UnknownMediaKind)?,
            title: self.title,
            description: self.description,
            tags: self.tags,
            mime_type: self.mime_type,
            container: self.container,
            video_codec: self.video_codec,
            audio_codec: self.audio_codec,
            width: self.width,
            height: self.height,
            duration_ms: to_u64(self.duration_ms, "duration_ms")?,
            bit_rate: to_u64(self.bit_rate, "bit_rate")?,
            file_size_bytes: to_u64(self.file_size_bytes, "file_size_bytes")?,
            sha256: self.canonical_sha256,
            local_work_path: self.local_work_path,
            storage_state: MediaStorageState::try_from(self.storage_state.as_str())
                .map_err(LibraryRepositoryError::UnknownStorageState)?,
            preview,
            caption_sync_generation: self.caption_sync_generation,
            caption_sync_state: CaptionSyncState::try_from(self.caption_sync_state.as_str())
                .map_err(LibraryRepositoryError::UnknownCaptionSyncState)?,
            caption_sync_error: self.caption_sync_error,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }

    fn preview_metadata(&self) -> Result<Option<MediaPreviewMetadata>, LibraryRepositoryError> {
        let fields = (
            self.preview_bytes.as_ref(),
            self.preview_mime_type.as_ref(),
            self.preview_width,
            self.preview_height,
            self.preview_sha256.as_ref(),
        );
        if fields.0.is_none()
            && fields.1.is_none()
            && fields.2.is_none()
            && fields.3.is_none()
            && fields.4.is_none()
        {
            return Ok(None);
        }
        let (Some(bytes), Some(mime_type), Some(width), Some(height), Some(sha256)) = fields else {
            return Err(LibraryRepositoryError::InvalidPreview("preview fields are incomplete"));
        };
        let width = u32::try_from(width)
            .map_err(|_| LibraryRepositoryError::InvalidPreview("preview width is invalid"))?;
        let height = u32::try_from(height)
            .map_err(|_| LibraryRepositoryError::InvalidPreview("preview height is invalid"))?;
        if !matches!(mime_type.as_str(), "image/jpeg" | "image/png")
            || bytes.is_empty()
            || bytes.len() > MAX_MEDIA_PREVIEW_BYTES
            || width == 0
            || width > MAX_MEDIA_PREVIEW_WIDTH
            || height == 0
            || height > MAX_MEDIA_PREVIEW_HEIGHT
            || sha256.len() != 32
        {
            return Err(LibraryRepositoryError::InvalidPreview("preview violates its bounds"));
        }
        if Sha256::digest(bytes).as_slice() != sha256.as_slice() {
            return Err(LibraryRepositoryError::InvalidPreview(
                "preview SHA-256 does not match bytes",
            ));
        }
        let (encoded_width, encoded_height) =
            sooqa_media::validate_bounded_preview_for_mime(bytes, Some(mime_type))
                .map_err(|error| LibraryRepositoryError::InvalidPreviewOwned(error.to_string()))?;
        if encoded_width != width || encoded_height != height {
            return Err(LibraryRepositoryError::InvalidPreview(
                "preview dimensions do not match its encoded image",
            ));
        }
        Ok(Some(MediaPreviewMetadata {
            mime_type: mime_type.clone(),
            width,
            height,
            size_bytes: u32::try_from(bytes.len()).map_err(|_| {
                LibraryRepositoryError::InvalidPreview("preview size does not fit the domain")
            })?,
            sha256: sha256.clone(),
        }))
    }

    fn into_storage_info(self) -> Result<StorageUploadInfo, LibraryRepositoryError> {
        Ok(StorageUploadInfo {
            media_id: self.id,
            state: self.storage_state,
            generation: self.storage_generation,
            storage_chat_id: self.telegram_storage_chat_id,
            storage_message_id: self.telegram_storage_message_id,
            file_id: self.telegram_file_id,
            file_unique_id: self.telegram_file_unique_id,
            updated_at: self.updated_at,
        })
    }

    fn into_storage_receipt(self) -> Result<StorageReceipt, LibraryRepositoryError> {
        let storage_chat_id = self
            .telegram_storage_chat_id
            .filter(|chat_id| *chat_id < 0)
            .ok_or(LibraryRepositoryError::StorageReceiptMissing(self.id))?;
        let storage_message_id = self
            .telegram_storage_message_id
            .filter(|message_id| *message_id > 0)
            .ok_or(LibraryRepositoryError::StorageReceiptMissing(self.id))?;
        Ok(StorageReceipt {
            media_id: self.id,
            storage_chat_id,
            storage_message_id,
            telegram_file_id: self.telegram_file_id,
            telegram_file_unique_id: self.telegram_file_unique_id,
            media_kind: MediaKind::try_from(self.kind.as_str())
                .map_err(LibraryRepositoryError::UnknownMediaKind)?,
            stored_at: self.stored_at.unwrap_or(self.updated_at),
        })
    }
}

fn source_from_row(row: &MediaRow) -> Result<Option<MediaSource>, LibraryRepositoryError> {
    if row.source_url.is_none()
        && (row.source_metadata.is_null()
            || row.source_metadata.as_object().is_some_and(|object| object.is_empty()))
    {
        return Ok(None);
    }
    let kind = row
        .source_metadata
        .get("kind")
        .or_else(|| row.source_metadata.get("source_kind"))
        .and_then(Value::as_str)
        .unwrap_or("direct_url");
    let kind = SourceKind::try_from(kind).map_err(LibraryRepositoryError::UnknownSourceKind)?;
    Ok(Some(MediaSource {
        ingest_id: row
            .source_metadata
            .get("ingest_id")
            .and_then(Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok()),
        kind,
        original_url: row
            .source_metadata
            .get("original_url")
            .and_then(Value::as_str)
            .map(str::to_owned),
        normalized_url: row.source_url.clone(),
        platform: row.source_metadata.get("platform").and_then(Value::as_str).map(str::to_owned),
        platform_content_id: row
            .source_metadata
            .get("platform_content_id")
            .and_then(Value::as_str)
            .map(str::to_owned),
        author_name: row
            .source_metadata
            .get("author_name")
            .and_then(Value::as_str)
            .map(str::to_owned),
        title: row.source_metadata.get("title").and_then(Value::as_str).map(str::to_owned),
        description: row
            .source_metadata
            .get("description")
            .and_then(Value::as_str)
            .map(str::to_owned),
        published_at: row.source_metadata.get("published_at").and_then(Value::as_str).and_then(
            |value| {
                time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
                    .ok()
            },
        ),
        retrieved_at: row.updated_at,
        metadata: row
            .source_metadata
            .get("metadata")
            .cloned()
            .unwrap_or_else(|| row.source_metadata.clone()),
    }))
}

fn storage_message_url(chat_id: Option<i64>, message_id: Option<i64>) -> Option<String> {
    let (chat_id, message_id) = chat_id.zip(message_id)?;
    if chat_id >= 0 || message_id <= 0 {
        return None;
    }
    let raw_id = chat_id.to_string();
    let internal_id = raw_id.strip_prefix("-100").unwrap_or_else(|| raw_id.trim_start_matches('-'));
    (!internal_id.is_empty()).then(|| format!("https://t.me/c/{internal_id}/{message_id}"))
}

fn source_to_value(source: &MediaSourceInput) -> Value {
    json!({
        "ingest_id": source.ingest_id.map(|id| id.to_string()),
        "kind": source.kind.as_str(),
        "original_url": source.original_url,
        "platform": source.platform,
        "platform_content_id": source.platform_content_id,
        "author_name": source.author_name,
        "title": source.title,
        "description": source.description,
        "published_at": source.published_at.map(|value| value.to_string()),
        "retrieved_at": OffsetDateTime::now_utc().to_string(),
        "metadata": source.metadata,
    })
}

fn merge_missing_source_metadata(existing: &Value, incoming: &Value) -> Value {
    match (existing, incoming) {
        (Value::Object(existing), Value::Object(incoming)) => {
            let mut merged = existing.clone();
            for (key, incoming_value) in incoming {
                match merged.get(key) {
                    Some(existing_value)
                        if existing_value.is_object() && incoming_value.is_object() =>
                    {
                        merged.insert(
                            key.clone(),
                            merge_missing_source_metadata(existing_value, incoming_value),
                        );
                    }
                    Some(existing_value) if !existing_value.is_null() => {}
                    Some(_) | None if !incoming_value.is_null() => {
                        merged.insert(key.clone(), incoming_value.clone());
                    }
                    _ => {}
                }
            }
            Value::Object(merged)
        }
        (Value::Null, incoming) => incoming.clone(),
        (existing, _) => existing.clone(),
    }
}

fn merge_tags(existing: &[String], incoming: &[String]) -> Vec<String> {
    let mut tags = existing.to_vec();
    for tag in incoming {
        if !tags.iter().any(|current| current == tag) {
            tags.push(tag.clone());
        }
    }
    tags
}

fn normalize_tags(tags: Vec<String>) -> Result<Vec<String>, LibraryRepositoryError> {
    let mut normalized = Vec::with_capacity(tags.len());
    for tag in tags {
        let tag = normalize_tag(tag)?;
        if !normalized.iter().any(|current| current == &tag) {
            normalized.push(tag);
        }
    }
    Ok(normalized)
}

fn next_caption_sync_values(
    row: &MediaRow,
    changed: bool,
) -> Result<CaptionSyncValues, LibraryRepositoryError> {
    if !changed {
        return Ok((
            row.caption_sync_generation,
            match row.caption_sync_state.as_str() {
                "not_required" | "pending" | "syncing" | "synced" | "failed" => {
                    // The value is checked when the row is converted to the
                    // domain object; preserving it here avoids rewriting an
                    // unrelated metadata edit into a sync transition.
                    match row.caption_sync_state.as_str() {
                        "not_required" => "not_required",
                        "pending" => "pending",
                        "syncing" => "syncing",
                        "synced" => "synced",
                        "failed" => "failed",
                        _ => unreachable!(),
                    }
                }
                _ => {
                    return Err(LibraryRepositoryError::UnknownCaptionSyncState(
                        row.caption_sync_state.clone(),
                    ));
                }
            },
            row.caption_sync_error.clone(),
            row.caption_sync_claim_token,
        ));
    }
    if !has_storage_message(row) {
        return Ok((row.caption_sync_generation, "not_required", None, None));
    }
    let generation = row
        .caption_sync_generation
        .checked_add(1)
        .ok_or(LibraryRepositoryError::CaptionSyncGenerationOverflow(row.id))?;
    Ok((generation, "pending", None, None))
}

fn has_storage_message(row: &MediaRow) -> bool {
    row.storage_state == "ready"
        && row.telegram_storage_chat_id.is_some()
        && row.telegram_storage_message_id.is_some()
}

fn storage_caption_metadata_from_row(row: &MediaRow) -> StorageCaptionMetadata {
    StorageCaptionMetadata {
        description: row.description.clone(),
        tags: row.tags.clone(),
        source_url: row.source_url.clone(),
    }
}

fn storage_caption_metadata_matches(row: &MediaRow, metadata: &StorageCaptionMetadata) -> bool {
    row.description == metadata.description
        && row.tags == metadata.tags
        && row.source_url == metadata.source_url
}

fn caption_sync_values_after_storage_upload(
    row: &MediaRow,
    caption_metadata: Option<&StorageCaptionMetadata>,
) -> Result<CaptionSyncValues, LibraryRepositoryError> {
    if caption_metadata.is_some_and(|metadata| storage_caption_metadata_matches(row, metadata)) {
        return Ok((row.caption_sync_generation, "synced", None, None));
    }
    next_caption_sync_values(row, true)
}

fn truncate_sync_error(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

async fn enqueue_caption_sync_job(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    media_id: Uuid,
    generation: i32,
) -> Result<(), sqlx::Error> {
    enqueue_caption_sync_job_with_key(
        transaction,
        media_id,
        generation,
        format!("media:{media_id}:caption_sync:v1:{generation}"),
    )
    .await
}

async fn enqueue_caption_sync_reapply_job(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    media_id: Uuid,
    generation: i32,
    stale_generation: i32,
    stale_claim_token: Uuid,
) -> Result<(), sqlx::Error> {
    enqueue_caption_sync_job_with_key(
        transaction,
        media_id,
        generation,
        format!(
            "media:{media_id}:caption_sync:v1:{generation}:after:{stale_generation}:claim:{stale_claim_token}"
        ),
    )
    .await
}

async fn enqueue_caption_sync_job_with_key(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    media_id: Uuid,
    generation: i32,
    dedupe_key: String,
) -> Result<(), sqlx::Error> {
    let job = NewJob::sync_storage_caption(media_id, generation).dedupe_key(dedupe_key);
    sqlx::query(
        "INSERT INTO queue.jobs (kind, payload, state, run_at, max_attempts, dedupe_key) VALUES ($1, $2, 'queued', COALESCE($3, now()), $4, $5) ON CONFLICT (dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING",
    )
    .bind(job.job_type().as_str())
    .bind(job.payload_json())
    .bind(job.run_at_value())
    .bind(job.max_attempts_value())
    .bind(job.dedupe_key_value())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn validate_media_ingest(ingest: &MediaIngest) -> Result<(), LibraryRepositoryError> {
    let Some(sha256) = ingest.metadata.sha256.as_deref() else {
        return Err(LibraryRepositoryError::MissingSha256);
    };
    if sha256.len() != 32 {
        return Err(LibraryRepositoryError::InvalidSha256Length { actual: sha256.len() });
    }
    let _ = preview_bindings(ingest.metadata.kind, &ingest.metadata.preview)?;
    Ok(())
}

type PreviewBindings = (Option<Vec<u8>>, Option<String>, Option<i32>, Option<i32>, Option<Vec<u8>>);

fn preview_bindings(
    kind: MediaKind,
    preview: &Option<sooqa_library::MediaPreviewInput>,
) -> Result<PreviewBindings, LibraryRepositoryError> {
    let Some(preview) = preview else {
        return Ok((None, None, None, None, None));
    };
    if kind == MediaKind::Audio {
        return Err(LibraryRepositoryError::InvalidPreview(
            "audio media must not have a bitmap preview",
        ));
    }
    if !matches!(preview.mime_type.as_str(), "image/jpeg" | "image/png")
        || preview.bytes.is_empty()
        || preview.bytes.len() > MAX_MEDIA_PREVIEW_BYTES
        || preview.width == 0
        || preview.width > MAX_MEDIA_PREVIEW_WIDTH
        || preview.height == 0
        || preview.height > MAX_MEDIA_PREVIEW_HEIGHT
        || preview.sha256.len() != 32
    {
        return Err(LibraryRepositoryError::InvalidPreview("preview violates its bounds"));
    }
    let digest = Sha256::digest(&preview.bytes);
    if digest.as_slice() != preview.sha256.as_slice() {
        return Err(LibraryRepositoryError::InvalidPreview("preview SHA-256 does not match bytes"));
    }
    let (width, height) =
        sooqa_media::validate_bounded_preview_for_mime(&preview.bytes, Some(&preview.mime_type))
            .map_err(|error| LibraryRepositoryError::InvalidPreviewOwned(error.to_string()))?;
    if width != preview.width || height != preview.height {
        return Err(LibraryRepositoryError::InvalidPreview(
            "preview dimensions do not match its encoded image",
        ));
    }
    Ok((
        Some(preview.bytes.clone()),
        Some(preview.mime_type.clone()),
        Some(i32::try_from(preview.width).map_err(|_| {
            LibraryRepositoryError::InvalidPreview("preview width does not fit the schema")
        })?),
        Some(i32::try_from(preview.height).map_err(|_| {
            LibraryRepositoryError::InvalidPreview("preview height does not fit the schema")
        })?),
        Some(preview.sha256.clone()),
    ))
}

fn validate_attachment(attachment: &StorageUploadAttachment) -> Result<(), LibraryRepositoryError> {
    if attachment.storage_message_id <= 0 {
        return Err(LibraryRepositoryError::InvalidStorageMessageId(attachment.storage_message_id));
    }
    if attachment.telegram_file_id.as_deref().is_some_and(str::is_empty) {
        return Err(LibraryRepositoryError::EmptyStorageField("telegram_file_id"));
    }
    if attachment.telegram_file_unique_id.as_deref().is_some_and(str::is_empty) {
        return Err(LibraryRepositoryError::EmptyStorageField("telegram_file_unique_id"));
    }
    Ok(())
}

fn to_i64(value: Option<u64>, field: &'static str) -> Result<Option<i64>, LibraryRepositoryError> {
    value
        .map(|value| {
            i64::try_from(value).map_err(|_| LibraryRepositoryError::InvalidNumber { field })
        })
        .transpose()
}

fn to_u64(value: Option<i64>, field: &'static str) -> Result<Option<u64>, LibraryRepositoryError> {
    value
        .map(|value| {
            u64::try_from(value).map_err(|_| LibraryRepositoryError::InvalidNumber { field })
        })
        .transpose()
}

/// Storage-caption and storage-upload settlement/recovery policy. These
/// transitions deliberately stay with the media aggregate, while the shared
/// queue helpers only perform lease fencing and low-level queue updates.
pub(crate) async fn settle_caption_job(
    pool: &PgPool,
    lease: &JobLease,
    expected: &JobCommand,
    settlement: JobSettlement,
) -> Result<Job, JobRepositoryError> {
    let media_id = match expected {
        JobCommand::SyncStorageCaption(payload) => payload.media_id,
        _ => return Err(JobRepositoryError::LeaseLost),
    };
    let mut transaction = pool.begin().await?;
    // Media is the aggregate owner for caption state. Lock it before the
    // queue row, matching all other library transitions and stale recovery.
    lock_media_for_job(&mut transaction, media_id).await?;
    let job = lock_running_job(&mut transaction, lease, settlement.allows_expired_lease()).await?;
    let current_command = crate::settlement::validate_locked_command(&job, expected)?;
    let generation = match current_command {
        JobCommand::SyncStorageCaption(payload) => payload.generation,
        _ => return Err(JobRepositoryError::LeaseLost),
    };
    let (state, run_at, error_class, error_message, terminal, non_consuming) =
        queue_parameters(&job, settlement);
    let caption_state = if terminal { "failed" } else { "pending" };
    let caption_error = terminal.then(|| error_message.chars().take(512).collect::<String>());
    sqlx::query(
        "UPDATE media SET caption_sync_state = $2, caption_sync_error = $3, caption_sync_claim_token = NULL, updated_at = now() WHERE id = $1 AND caption_sync_generation = $4 AND caption_sync_state = 'syncing'",
    )
    .bind(media_id)
    .bind(caption_state)
    .bind(caption_error.as_deref())
    .bind(generation)
    .execute(&mut *transaction)
    .await?;
    let row = update_locked_job(
        &mut transaction,
        job.id,
        state,
        run_at,
        &error_class,
        &error_message,
        terminal,
        non_consuming,
    )
    .await?;
    transaction.commit().await?;
    row.into_job()
}

pub(crate) async fn settle_storage_job(
    pool: &PgPool,
    lease: &JobLease,
    expected: &JobCommand,
    settlement: JobSettlement,
) -> Result<Job, JobRepositoryError> {
    let media_id = match expected {
        JobCommand::UploadStorageAsset(payload) => payload.media_id,
        _ => return Err(JobRepositoryError::LeaseLost),
    };
    // Upload handlers already persist the media result. On a terminal queue
    // settlement, preserve the conservative unknown state if a reservation is
    // still active; a Telegram effect is never resent implicitly.
    let mut transaction = pool.begin().await?;
    lock_media_for_job(&mut transaction, media_id).await?;
    let job = lock_running_job(&mut transaction, lease, settlement.allows_expired_lease()).await?;
    let current_media_id = match crate::settlement::validate_locked_command(&job, expected)? {
        JobCommand::UploadStorageAsset(payload) => payload.media_id,
        _ => return Err(JobRepositoryError::LeaseLost),
    };
    let (state, run_at, error_class, error_message, terminal, non_consuming) =
        queue_parameters(&job, settlement);
    if terminal {
        reconcile_storage_terminal(
            &mut transaction,
            current_media_id,
            &error_class,
            &error_message,
        )
        .await?;
    }
    let row = update_locked_job(
        &mut transaction,
        job.id,
        state,
        run_at,
        &error_class,
        &error_message,
        terminal,
        non_consuming,
    )
    .await?;
    transaction.commit().await?;
    row.into_job()
}

async fn reconcile_storage_terminal(
    transaction: &mut Transaction<'_, Postgres>,
    media_id: Uuid,
    error_class: &str,
    error_message: &str,
) -> Result<(), sqlx::Error> {
    let state =
        sqlx::query_scalar::<_, String>("SELECT storage_state FROM media WHERE id = $1 FOR UPDATE")
            .bind(media_id)
            .fetch_optional(&mut **transaction)
            .await?;
    match state.as_deref() {
        Some("ready") => {
            sqlx::query(
                "UPDATE ingests SET state = 'completed', error_code = NULL, error_message = NULL, completed_at = now(), updated_at = now() WHERE media_id = $1 AND state <> 'cancelled' AND (state = 'storing' OR (state = 'failed_retryable' AND error_code IN ('storage_upload', 'storage_unknown')) OR (state = 'failed_terminal' AND error_code IN ('storage_upload', 'storage_unknown')))",
            )
            .bind(media_id)
            .execute(&mut **transaction)
            .await?;
        }
        Some("pending_storage") => {
            sqlx::query(
                "UPDATE media SET storage_state = 'storage_unknown', storage_token = NULL, storage_started_at = NULL, updated_at = now() WHERE id = $1 AND storage_state <> 'ready'",
            )
            .bind(media_id)
            .execute(&mut **transaction)
            .await?;
            sqlx::query(
                "UPDATE ingests SET state = 'failed_terminal', error_code = 'storage_unknown', error_message = $2, completed_at = now(), updated_at = now() WHERE media_id = $1 AND state <> 'cancelled' AND (state NOT IN ('failed_terminal') OR error_code IN ('storage_upload', 'storage_unknown'))",
            )
            .bind(media_id)
            .bind(error_message)
            .execute(&mut **transaction)
            .await?;
        }
        Some("storage_unknown") => {
            sqlx::query(
                "UPDATE ingests SET state = 'failed_terminal', error_code = 'storage_unknown', error_message = $2, completed_at = now(), updated_at = now() WHERE media_id = $1 AND state <> 'cancelled' AND state NOT IN ('failed_terminal')",
            )
            .bind(media_id)
            .bind(error_message)
            .execute(&mut **transaction)
            .await?;
        }
        // `missing` is the explicit, known-no-effect terminal result from the
        // storage handler. Preserve its storage_upload diagnostic; only an
        // unresolved reservation is promoted to storage_unknown here.
        Some("missing") => {}
        _ => {
            let _ = (error_class, error_message);
        }
    }
    Ok(())
}

pub(crate) async fn recover_caption_job(
    pool: &PgPool,
    job_id: Uuid,
    expected: &JobCommand,
) -> Result<bool, JobRepositoryError> {
    let media_id = match expected {
        JobCommand::SyncStorageCaption(payload) => payload.media_id,
        _ => return Err(JobRepositoryError::LeaseLost),
    };
    let mut transaction = pool.begin().await?;
    lock_media_for_job(&mut transaction, media_id).await?;
    let Some(job) = lock_expired_job(&mut transaction, job_id).await? else {
        transaction.commit().await?;
        return Ok(false);
    };
    let generation = match crate::settlement::validate_locked_command(&job, expected)? {
        JobCommand::SyncStorageCaption(payload) => payload.generation,
        _ => return Err(JobRepositoryError::LeaseLost),
    };
    let terminal = job.attempt_count >= job.max_attempts;
    sqlx::query(
        "UPDATE media SET caption_sync_state = CASE WHEN $3 THEN 'failed' ELSE 'pending' END, caption_sync_error = CASE WHEN $3 THEN 'caption sync job lease expired after the final attempt' ELSE NULL END, caption_sync_claim_token = NULL, updated_at = now() WHERE id = $1 AND caption_sync_generation = $2 AND caption_sync_state = 'syncing'",
    )
    .bind(media_id)
    .bind(generation)
    .bind(terminal)
    .execute(&mut *transaction)
    .await?;
    update_locked_job(
        &mut transaction,
        job.id,
        if terminal { "failed" } else { "queued" },
        OffsetDateTime::now_utc(),
        job.error_class.as_deref().unwrap_or("lease_expired"),
        job.error_message.as_deref().unwrap_or("job lease expired"),
        terminal,
        false,
    )
    .await?;
    transaction.commit().await?;
    Ok(true)
}

pub(crate) async fn recover_storage_job(
    pool: &PgPool,
    job_id: Uuid,
    expected: &JobCommand,
) -> Result<bool, JobRepositoryError> {
    let media_id = match expected {
        JobCommand::UploadStorageAsset(payload) => payload.media_id,
        _ => return Err(JobRepositoryError::LeaseLost),
    };
    let mut transaction = pool.begin().await?;
    lock_media_for_job(&mut transaction, media_id).await?;
    let Some(job) = lock_expired_job(&mut transaction, job_id).await? else {
        transaction.commit().await?;
        return Ok(false);
    };
    let media_id = match crate::settlement::validate_locked_command(&job, expected)? {
        JobCommand::UploadStorageAsset(payload) => payload.media_id,
        _ => return Err(JobRepositoryError::LeaseLost),
    };
    let state = sqlx::query_as::<_, StorageRecoveryRow>(
        "SELECT storage_state, storage_token, storage_started_at FROM media WHERE id = $1 FOR UPDATE",
    )
    .bind(media_id)
    .fetch_optional(&mut *transaction)
    .await?;
    if let Some(state) = state {
        if state.storage_state == "pending_storage"
            && state.storage_token.is_some()
            && state.storage_started_at.is_some_and(|started| {
                started > OffsetDateTime::now_utc() - time::Duration::minutes(1)
            })
        {
            transaction.commit().await?;
            return Ok(false);
        }
        let terminal = job.attempt_count >= job.max_attempts
            || state.storage_state != "pending_storage"
            || state.storage_token.is_some();
        let (queue_state, error_class, error_message, consume) = match state.storage_state.as_str()
        {
            "ready" => ("succeeded", "", "", false),
            "pending_storage" if state.storage_token.is_none() => (
                "queued",
                "storage_upload_cancelled",
                "storage upload was safely cancelled before Telegram dispatch",
                true,
            ),
            _ => (
                "failed",
                "storage_unknown",
                "storage job lease expired; external storage result requires reconciliation",
                false,
            ),
        };
        if queue_state == "failed" {
            sqlx::query(
                "UPDATE media SET storage_state = 'storage_unknown', storage_token = NULL, storage_started_at = NULL, updated_at = now() WHERE id = $1 AND storage_state <> 'ready'",
            )
            .bind(media_id)
            .execute(&mut *transaction)
            .await?;
            sqlx::query(
                "UPDATE ingests SET state = 'failed_terminal', error_code = 'storage_unknown', error_message = $2, completed_at = now(), updated_at = now() WHERE media_id = $1 AND state <> 'cancelled' AND (state NOT IN ('failed_terminal') OR error_code IN ('storage_upload', 'storage_unknown'))",
            )
            .bind(media_id)
            .bind(error_message)
            .execute(&mut *transaction)
            .await?;
        } else if queue_state == "succeeded" {
            sqlx::query(
                "UPDATE ingests SET state = 'completed', error_code = NULL, error_message = NULL, completed_at = now(), updated_at = now() WHERE media_id = $1 AND state <> 'cancelled' AND (state = 'storing' OR (state = 'failed_retryable' AND error_code IN ('storage_upload', 'storage_unknown')) OR (state = 'failed_terminal' AND error_code IN ('storage_upload', 'storage_unknown')))",
            )
            .bind(media_id)
            .execute(&mut *transaction)
            .await?;
        }
        let terminal = if queue_state == "queued" { false } else { terminal };
        update_locked_job(
            &mut transaction,
            job.id,
            queue_state,
            if queue_state == "succeeded" { job.run_at } else { OffsetDateTime::now_utc() },
            error_class,
            error_message,
            terminal || queue_state == "succeeded",
            queue_state == "queued" && consume,
        )
        .await?;
        if queue_state == "succeeded" {
            sqlx::query(
                "UPDATE queue.jobs SET error_class = NULL, error_message = NULL WHERE id = $1",
            )
            .bind(job.id)
            .execute(&mut *transaction)
            .await?;
        }
    } else {
        update_locked_job(
            &mut transaction,
            job.id,
            "failed",
            job.run_at,
            "storage_unknown",
            "storage media row is missing; external storage result requires reconciliation",
            true,
            false,
        )
        .await?;
    }
    transaction.commit().await?;
    Ok(true)
}

async fn lock_media_for_job(
    transaction: &mut Transaction<'_, Postgres>,
    media_id: Uuid,
) -> Result<(), JobRepositoryError> {
    // A missing media row is handled by the caller's queue-only recovery
    // policy, but the lookup still occurs before the queue lock whenever the
    // aggregate exists.
    let _ = sqlx::query_scalar::<_, Uuid>("SELECT id FROM media WHERE id = $1 FOR UPDATE")
        .bind(media_id)
        .fetch_optional(&mut **transaction)
        .await?;
    Ok(())
}

#[derive(Debug, FromRow)]
struct StorageRecoveryRow {
    storage_state: String,
    storage_token: Option<Uuid>,
    storage_started_at: Option<OffsetDateTime>,
}

#[derive(Debug, Error)]
pub enum LibraryRepositoryError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("invalid video fingerprint: {0}")]
    InvalidFingerprint(String),
    #[error("video identity requires a video media kind")]
    InvalidVideoIdentityKind,
    #[error("duplicate evidence exceeds the {max}-byte limit")]
    DuplicateEvidenceTooLarge { max: usize },
    #[error("duplicate evidence must contain at least one match")]
    DuplicateEvidenceEmpty,
    #[error("duplicate evidence contains more than {max} matches")]
    DuplicateEvidenceTooManyMatches { max: usize },
    #[error("duplicate evidence does not contain a strong duplicate match")]
    DuplicateEvidenceMissingStrongMatch,
    #[error(
        "duplicate evidence algorithm version {actual} does not match incoming version {expected}"
    )]
    DuplicateEvidenceAlgorithmVersionMismatch { expected: String, actual: String },
    #[error(
        "duplicate evidence match fingerprint version {actual} does not match incoming version {expected}"
    )]
    DuplicateEvidenceFingerprintVersionMismatch { expected: String, actual: String },
    #[error("duplicate evidence field {field} has invalid basis points {value}")]
    DuplicateEvidenceInvalidBasisPoints { field: &'static str, value: i64 },
    #[error("duplicate evidence shared token count must not be negative: {value}")]
    DuplicateEvidenceInvalidSharedTokenCount { value: i64 },
    #[error("media {0} was not found")]
    ResourceMissing(Uuid),
    #[error("media {0} was modified by another request")]
    OptimisticConflict(Uuid),
    #[error("invalid media tag: {0}")]
    InvalidTag(#[from] TagValidationError),
    #[error("media search limit must be between 1 and 50, got {value}")]
    InvalidLimit { value: u32 },
    #[error("database count was negative")]
    InvalidCount,
    #[error("media SHA-256 is required")]
    MissingSha256,
    #[error("video identity requires a fingerprint when the exact SHA is absent")]
    MissingFingerprint,
    #[error("media SHA-256 must contain 32 bytes, got {actual}")]
    InvalidSha256Length { actual: usize },
    #[error("media preview is invalid: {0}")]
    InvalidPreview(&'static str),
    #[error("media preview is invalid: {0}")]
    InvalidPreviewOwned(String),
    #[error("media {0} has no ready Telegram storage message for caption synchronization")]
    CaptionSyncUnavailable(Uuid),
    #[error("caption-sync generation for media {0} overflowed")]
    CaptionSyncGenerationOverflow(Uuid),
    #[error("media {0} has an invalid kind")]
    UnknownMediaKind(String),
    #[error("media has an invalid storage state: {0}")]
    UnknownStorageState(String),
    #[error("media has an invalid caption-sync state: {0}")]
    UnknownCaptionSyncState(String),
    #[error("media source has an invalid kind: {0}")]
    UnknownSourceKind(String),
    #[error("media violates a repository invariant: {0}")]
    Invariant(&'static str),
    #[error("invalid numeric value for {field}")]
    InvalidNumber { field: &'static str },
    #[error("storage upload for media {0} is active")]
    StorageUploadActive(Uuid),
    #[error("media {0} workspace was reclaimed; reconstruction is required before storage reset")]
    WorkspaceReclaimed(Uuid),
    #[error("storage generation for media {0} overflowed")]
    StorageGenerationOverflow(Uuid),
    #[error("storage upload for media {0} is not unknown")]
    StorageUploadNotUnknown(Uuid),
    #[error("storage upload lease for media {0} was lost")]
    StorageUploadLeaseLost(Uuid),
    #[error("storage receipt for media {0} is incomplete")]
    StorageReceiptMissing(Uuid),
    #[error("Telegram storage message ID must be positive, got {0}")]
    InvalidStorageMessageId(i64),
    #[error("storage attachment field {0} must not be empty")]
    EmptyStorageField(&'static str),
    #[error("storage lease duration is invalid")]
    InvalidLeaseDuration,
}
