use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use sooqa_jobs::NewJob;
use sooqa_library::{
    MAX_VIDEO_DUPLICATE_EVIDENCE_BYTES, MAX_VIDEO_DUPLICATE_MATCHES, Media, MediaCursor,
    MediaDetails, MediaIngest, MediaKind, MediaMetadata, MediaPage, MediaSearchQuery, MediaSource,
    MediaSourceInput, MediaStatus, MediaStorageState, MediaSummary, MediaUpdate, NewTag,
    SourceKind, StorageReceipt, StorageUploadAttachment, StorageUploadInfo,
    StorageUploadReservation, StorageUploadReservationRequest, StorageUploadStore, Tag,
    VideoDuplicateClassification, VideoDuplicateEvidence, VideoDuplicateMatch,
    VideoIdentityOutcome,
};
use sooqa_media::{
    SequenceAlignmentConfig, SequenceClassification, VideoSequenceFingerprint,
    align_video_sequences,
};
use sqlx::{FromRow, PgPool};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

pub use sooqa_library::VideoFingerprintCandidate;

const VIDEO_IDENTITY_ADVISORY_LOCK: i64 = 0x736f_6f71_615f_6964;

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

    pub async fn find_media_details(
        &self,
        id: Uuid,
    ) -> Result<Option<MediaDetails>, LibraryRepositoryError> {
        let Some(row) = self.load(id).await? else { return Ok(None) };
        let media = row.clone().into_media()?;
        let tags = row.tags.iter().map(|tag| tag_from_name(tag, row.updated_at)).collect();
        let source = source_from_row(&row)?;
        Ok(Some(MediaDetails { media, tags, source }))
    }

    pub async fn search_media(
        &self,
        query: MediaSearchQuery,
    ) -> Result<MediaPage, LibraryRepositoryError> {
        if !(1..=100).contains(&query.limit) {
            return Err(LibraryRepositoryError::InvalidLimit { value: query.limit });
        }
        let text = query.text.filter(|value| !value.trim().is_empty());
        let kind = query.kind.map(|value| value.as_str().to_owned());
        let status = query.status.map(|value| value.as_str().to_owned());
        let tags = (!query.tags.is_empty()).then_some(query.tags);
        let rows = sqlx::query_as::<_, MediaRow>(
            r#"
            SELECT * FROM media
            WHERE ($1::text IS NULL OR kind = $1)
              AND ($2::text IS NULL OR title ILIKE '%' || $2 || '%' OR description ILIKE '%' || $2 || '%')
              AND ($3::text[] IS NULL OR tags @> $3)
              AND ($4::timestamptz IS NULL OR (updated_at, id) < ($4, $5))
              AND ($6::text IS NULL OR ($6 = 'active' AND source_metadata->>'archived' IS DISTINCT FROM 'true') OR ($6 = 'archived' AND source_metadata->>'archived' = 'true'))
            ORDER BY updated_at DESC, id DESC
            LIMIT $7
            "#,
        )
        .bind(kind)
        .bind(text)
        .bind(tags)
        .bind(query.cursor.as_ref().map(|cursor| cursor.updated_at))
        .bind(query.cursor.as_ref().map(|cursor| cursor.id))
        .bind(status)
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

    pub async fn update_media(
        &self,
        id: Uuid,
        update: MediaUpdate,
    ) -> Result<Media, LibraryRepositoryError> {
        if update.title.is_none() && update.description.is_none() && update.notes.is_none() {
            return Err(LibraryRepositoryError::EmptyUpdate);
        }
        let current = self.load(id).await?.ok_or(LibraryRepositoryError::ResourceMissing(id))?;
        if update.expected_updated_at.is_some_and(|expected| expected != current.updated_at) {
            return Err(LibraryRepositoryError::OptimisticConflict(id));
        }
        let title = update.title.unwrap_or(current.title.clone());
        let description = update.description.unwrap_or(current.description.clone());
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
        row.into_media()
    }

    pub async fn archive_media(&self, id: Uuid) -> Result<Media, LibraryRepositoryError> {
        let row = sqlx::query_as::<_, MediaRow>(
            "UPDATE media SET source_metadata = source_metadata || jsonb_build_object('archived', true), updated_at = now() WHERE id = $1 RETURNING *",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(LibraryRepositoryError::ResourceMissing(id))?;
        row.into_media()
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
        let updated = sqlx::query(
            "UPDATE media SET tags = array_remove(tags, $2), updated_at = now() WHERE id = $1 AND $2 = ANY(tags)",
        )
        .bind(id)
        .bind(tag)
        .execute(&self.pool)
        .await?;
        if updated.rows_affected() != 0 {
            return Ok(());
        }
        let exists =
            sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM media WHERE id = $1)")
                .bind(id)
                .fetch_one(&self.pool)
                .await?;
        if !exists {
            return Err(LibraryRepositoryError::ResourceMissing(id));
        }
        Err(LibraryRepositoryError::TagNotAttached)
    }

    pub async fn list_tags(&self, id: Uuid) -> Result<Vec<Tag>, LibraryRepositoryError> {
        let row = self.load(id).await?.ok_or(LibraryRepositoryError::ResourceMissing(id))?;
        Ok(row.tags.iter().map(|tag| tag_from_name(tag, row.updated_at)).collect())
    }

    pub async fn resolve_media(
        &self,
        ingest: MediaIngest,
    ) -> Result<MediaResolutionResult, LibraryRepositoryError> {
        validate_media_ingest(&ingest)?;
        let sha256 =
            ingest.metadata.sha256.as_deref().ok_or(LibraryRepositoryError::MissingSha256)?;
        let mut transaction = self.pool.begin().await?;
        let id = Uuid::now_v7();
        let source_value = source_to_value(&ingest.source);
        let inserted = sqlx::query_as::<_, MediaRow>(
            r#"INSERT INTO media (
                id, kind, storage_state, canonical_sha256, title, description,
                tags, source_url, source_metadata, mime_type, container,
                video_codec, audio_codec, width, height, duration_ms, bit_rate,
                file_size_bytes, local_work_path
            ) VALUES ($1, $2, 'pending_storage', $3, $4, $5, $6, $7, $8, $9,
                      $10, $11, $12, $13, $14, $15, $16, $17, $18)
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
                let row = sqlx::query_as::<_, MediaRow>(
                    "UPDATE media SET tags = $2, title = COALESCE(title, $3), description = COALESCE(description, $4), source_url = COALESCE(source_url, $5), source_metadata = $6, updated_at = now() WHERE id = $1 RETURNING *",
                )
                .bind(row.id)
                .bind(merged_tags)
                .bind(&ingest.media.title)
                .bind(&ingest.media.description)
                .bind(ingest.source.normalized_url.clone().or(ingest.source.original_url.clone()))
                .bind(merge_missing_source_metadata(&row.source_metadata, &source_value))
                .fetch_one(&mut *transaction)
                .await?;
                (row, false)
            }
        };
        transaction.commit().await?;
        let media = row.clone().into_media()?;
        let source = source_from_row(&row)?
            .ok_or(LibraryRepositoryError::Invariant("resolved media source is missing"))?;
        Ok(MediaResolutionResult { media, source, media_created })
    }

    pub async fn resolve_video_identity(
        &self,
        ingest: MediaIngest,
        fingerprint: &VideoSequenceFingerprint,
        config: SequenceAlignmentConfig,
        force_save: bool,
    ) -> Result<VideoIdentityOutcome, LibraryRepositoryError> {
        validate_media_ingest(&ingest)?;
        if ingest.metadata.kind != MediaKind::Video {
            return Err(LibraryRepositoryError::InvalidVideoIdentityKind);
        }
        config
            .validate()
            .map_err(|error| LibraryRepositoryError::InvalidAlignment(error.to_string()))?;
        let sha256 =
            ingest.metadata.sha256.as_deref().ok_or(LibraryRepositoryError::MissingSha256)?;
        let fingerprint_data = fingerprint
            .encode()
            .map_err(|error| LibraryRepositoryError::InvalidFingerprint(error.to_string()))?;
        let search_tokens = fingerprint.search_tokens();
        let source_value = source_to_value(&ingest.source);
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(VIDEO_IDENTITY_ADVISORY_LOCK)
            .execute(&mut *transaction)
            .await?;

        if let Some(row) = sqlx::query_as::<_, MediaRow>(
            "SELECT * FROM media WHERE canonical_sha256 = $1 FOR UPDATE",
        )
        .bind(sha256)
        .fetch_optional(&mut *transaction)
        .await?
        {
            let merged_tags = merge_tags(&row.tags, &ingest.tags);
            sqlx::query(
                "UPDATE media SET tags = $2, title = COALESCE(title, $3), description = COALESCE(description, $4), source_url = COALESCE(source_url, $5), source_metadata = $6, updated_at = now() WHERE id = $1",
            )
            .bind(row.id)
            .bind(merged_tags)
            .bind(&ingest.media.title)
            .bind(&ingest.media.description)
            .bind(ingest.source.normalized_url.clone().or(ingest.source.original_url.clone()))
            .bind(merge_missing_source_metadata(&row.source_metadata, &source_value))
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
            return Ok(VideoIdentityOutcome::ExactDuplicate { media_id: row.id });
        }

        if !force_save {
            let candidates = fetch_video_fingerprint_candidates(
                &mut transaction,
                fingerprint.version.as_str(),
                &search_tokens,
            )
            .await?;
            let mut matches = candidates
                .into_iter()
                .filter_map(|candidate| {
                    let stored = VideoSequenceFingerprint::decode(&candidate.fingerprint_data)
                        .map_err(|error| {
                            LibraryRepositoryError::InvalidFingerprint(format!(
                                "media {} has an invalid stored fingerprint: {error}",
                                candidate.media_id
                            ))
                        });
                    let stored = match stored {
                        Ok(stored) => stored,
                        Err(error) => return Some(Err(error)),
                    };
                    let alignment =
                        align_video_sequences(fingerprint, &stored, config).map_err(|error| {
                            LibraryRepositoryError::InvalidAlignment(error.to_string())
                        });
                    match alignment {
                        Ok(alignment) => duplicate_match(&candidate, alignment).map(Ok),
                        Err(error) => Some(Err(error)),
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            matches.sort_by(|left, right| {
                classification_rank(right.classification)
                    .cmp(&classification_rank(left.classification))
                    .then_with(|| right.score_bps.cmp(&left.score_bps))
                    .then_with(|| right.shared_token_count.cmp(&left.shared_token_count))
                    .then_with(|| left.media_id.cmp(&right.media_id))
            });
            if matches
                .iter()
                .any(|item| item.classification == VideoDuplicateClassification::StrongDuplicate)
            {
                matches.truncate(MAX_VIDEO_DUPLICATE_MATCHES);
                let evidence = VideoDuplicateEvidence {
                    algorithm_version: fingerprint.version.as_str().to_owned(),
                    matches,
                };
                let encoded = serde_json::to_vec(&evidence)?;
                if encoded.len() > MAX_VIDEO_DUPLICATE_EVIDENCE_BYTES {
                    return Err(LibraryRepositoryError::DuplicateEvidenceTooLarge {
                        max: MAX_VIDEO_DUPLICATE_EVIDENCE_BYTES,
                    });
                }
                transaction.commit().await?;
                return Ok(VideoIdentityOutcome::DuplicatePending { evidence });
            }
        }

        let id = Uuid::now_v7();
        let inserted = sqlx::query_as::<_, MediaRow>(
            r#"INSERT INTO media (
                id, kind, storage_state, canonical_sha256, fingerprint_version,
                fingerprint_data, fingerprint_search_tokens, title, description,
                tags, source_url, source_metadata, mime_type, container,
                video_codec, audio_codec, width, height, duration_ms, bit_rate,
                file_size_bytes, local_work_path
            ) VALUES ($1, 'video', 'pending_storage', $2, $3, $4, $5, $6, $7,
                      $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18,
                      $19, $20)
            ON CONFLICT (canonical_sha256) DO NOTHING
            RETURNING *"#,
        )
        .bind(id)
        .bind(sha256)
        .bind(fingerprint.version.as_str())
        .bind(&fingerprint_data)
        .bind(&search_tokens)
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
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(row) = inserted {
            let media_id = row.id;
            transaction.commit().await?;
            return Ok(VideoIdentityOutcome::NewMedia { media_id });
        }

        let media_id = sqlx::query_scalar::<_, Uuid>(
            "SELECT id FROM media WHERE canonical_sha256 = $1 FOR UPDATE",
        )
        .bind(sha256)
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
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
        let source_value = source_to_value(&ingest.source);
        let mut transaction = self.pool.begin().await?;
        let Some(row) = sqlx::query_as::<_, MediaRow>(
            "SELECT * FROM media WHERE canonical_sha256 = $1 FOR UPDATE",
        )
        .bind(sha256)
        .fetch_optional(&mut *transaction)
        .await?
        else {
            transaction.commit().await?;
            return Ok(None);
        };
        let merged_tags = merge_tags(&row.tags, &ingest.tags);
        sqlx::query(
            "UPDATE media SET tags = $2, title = COALESCE(title, $3), description = COALESCE(description, $4), source_url = COALESCE(source_url, $5), source_metadata = $6, updated_at = now() WHERE id = $1",
        )
        .bind(row.id)
        .bind(merged_tags)
        .bind(&ingest.media.title)
        .bind(&ingest.media.description)
        .bind(ingest.source.normalized_url.clone().or(ingest.source.original_url.clone()))
        .bind(merge_missing_source_metadata(&row.source_metadata, &source_value))
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(Some(row.id))
    }

    pub async fn record_media_metadata(
        &self,
        id: Uuid,
        metadata: MediaMetadata,
    ) -> Result<Media, LibraryRepositoryError> {
        let row = sqlx::query_as::<_, MediaRow>(
            r#"UPDATE media SET kind = $2, canonical_sha256 = $3,
                mime_type = $4, container = $5, video_codec = $6, audio_codec = $7,
                width = $8, height = $9, duration_ms = $10, bit_rate = $11,
                file_size_bytes = $12, local_work_path = $13,
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
        .fetch_optional(&self.pool)
        .await?
        .ok_or(LibraryRepositoryError::ResourceMissing(id))?;
        row.into_media()
    }

    pub async fn record_video_sequence_fingerprint(
        &self,
        media_id: Uuid,
        fingerprint: &VideoSequenceFingerprint,
    ) -> Result<(), LibraryRepositoryError> {
        let encoded = fingerprint
            .encode()
            .map_err(|error| LibraryRepositoryError::InvalidFingerprint(error.to_string()))?;
        let tokens = fingerprint.search_tokens();
        let updated = sqlx::query(
            "UPDATE media SET fingerprint_version = $2, fingerprint_data = $3, fingerprint_search_tokens = $4, updated_at = now() WHERE id = $1 AND kind = 'video'",
        )
        .bind(media_id)
        .bind(fingerprint.version.as_str())
        .bind(encoded)
        .bind(tokens)
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
        transaction.commit().await?;
        Ok(())
    }

    pub async fn reset_storage_upload(&self, id: Uuid) -> Result<(), LibraryRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query_as::<_, MediaRow>("SELECT * FROM media WHERE id = $1 FOR UPDATE")
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(LibraryRepositoryError::ResourceMissing(id))?;
        if row.storage_token.is_some() {
            return Err(LibraryRepositoryError::StorageUploadActive(id));
        }
        let generation = row
            .storage_generation
            .checked_add(1)
            .ok_or(LibraryRepositoryError::StorageGenerationOverflow(id))?;
        sqlx::query(
            "UPDATE media SET storage_state = 'pending_storage', storage_generation = $2, telegram_storage_chat_id = NULL, telegram_storage_message_id = NULL, telegram_file_id = NULL, telegram_file_unique_id = NULL, storage_token = NULL, storage_started_at = NULL, stored_at = NULL, updated_at = now() WHERE id = $1",
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
        let row = sqlx::query_as::<_, MediaRow>(
            "UPDATE media SET storage_state = 'ready', telegram_storage_chat_id = $3, telegram_storage_message_id = $4, telegram_file_id = $5, telegram_file_unique_id = $6, storage_token = NULL, storage_started_at = NULL, stored_at = now(), updated_at = now() WHERE id = $1 AND storage_generation = $2 AND storage_state = 'storage_unknown' RETURNING *",
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
        complete_linked_ingests_for_storage(&mut transaction, id).await?;
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
        let tags = row.tags.iter().map(|tag| tag_from_name(tag, row.updated_at)).collect();
        let source = source_from_row(&row)?;
        Ok(MediaSummary {
            media,
            tags,
            source_count: u64::from(source.is_some()),
            source_url: row.source_url,
            source_metadata: source.map(|source| source.metadata),
        })
    }
}

async fn fetch_video_fingerprint_candidates(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
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

fn duplicate_match(
    candidate: &VideoFingerprintCandidateRow,
    alignment: sooqa_media::SequenceAlignment,
) -> Option<VideoDuplicateMatch> {
    let classification = match alignment.classification {
        SequenceClassification::StrongDuplicate => VideoDuplicateClassification::StrongDuplicate,
        SequenceClassification::PartialMatch => VideoDuplicateClassification::PartialMatch,
        SequenceClassification::NotDuplicate => return None,
    };
    let evidence = alignment.evidence;
    Some(VideoDuplicateMatch {
        media_id: candidate.media_id,
        fingerprint_version: candidate.fingerprint_version.clone(),
        classification,
        aligned_offset_ms: evidence.aligned_offset_ms,
        informative_matched_samples: evidence.informative_matched_samples,
        incoming_coverage_bps: evidence.incoming_coverage_bps,
        candidate_coverage_bps: evidence.candidate_coverage_bps,
        median_distance_bps: evidence.median_distance_bps,
        high_percentile_distance_bps: evidence.high_percentile_distance_bps,
        longest_temporally_consistent_run: evidence.longest_temporally_consistent_run,
        unmatched_incoming_prefix: evidence.unmatched_incoming_prefix,
        unmatched_incoming_suffix: evidence.unmatched_incoming_suffix,
        unmatched_candidate_prefix: evidence.unmatched_candidate_prefix,
        unmatched_candidate_suffix: evidence.unmatched_candidate_suffix,
        gap_count: evidence.gap_count,
        score_bps: evidence.score_bps,
        shared_token_count: candidate.shared_token_count,
        token_overlap_bps: candidate.overlap_bps,
    })
}

fn classification_rank(classification: VideoDuplicateClassification) -> u8 {
    match classification {
        VideoDuplicateClassification::StrongDuplicate => 2,
        VideoDuplicateClassification::PartialMatch => 1,
    }
}

pub type MediaResolutionResult = sooqa_library::MediaResolution;

#[async_trait]
impl StorageUploadStore for LibraryRepository {
    type Error = LibraryRepositoryError;

    async fn find_media(&self, media_id: Uuid) -> Result<Option<Media>, Self::Error> {
        LibraryRepository::find_media(self, media_id).await
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
        Ok(StorageUploadReservation::Reserved { media_id: request.media_id, owner_token })
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
        let row = sqlx::query_as::<_, MediaRow>(
            "UPDATE media SET storage_state = 'ready', telegram_storage_chat_id = $2, telegram_storage_message_id = $3, telegram_file_id = $4, telegram_file_unique_id = $5, storage_token = NULL, storage_started_at = NULL, stored_at = now(), updated_at = now() WHERE id = $1 AND storage_token = $6 AND storage_state = 'pending_storage' RETURNING *",
        )
        .bind(media_id)
        .bind(attachment.storage_chat_id)
        .bind(attachment.storage_message_id)
        .bind(attachment.telegram_file_id)
        .bind(attachment.telegram_file_unique_id)
        .bind(owner_token)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(LibraryRepositoryError::StorageUploadLeaseLost(media_id))?;
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
    sqlx::query(
        "UPDATE ingests SET state = 'completed', error_code = NULL, error_message = NULL, completed_at = now(), updated_at = now() WHERE media_id = $1 AND state <> 'cancelled' AND (state = 'storing' OR (state = 'failed_retryable' AND error_code IN ('storage_upload', 'storage_unknown')) OR (state = 'failed_terminal' AND error_code IN ('storage_upload', 'storage_unknown')))",
    )
    .bind(media_id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

impl MediaRow {
    fn into_media(self) -> Result<Media, LibraryRepositoryError> {
        Ok(Media {
            id: self.id,
            kind: MediaKind::try_from(self.kind.as_str())
                .map_err(LibraryRepositoryError::UnknownMediaKind)?,
            status: if self.source_metadata.get("archived").and_then(Value::as_bool) == Some(true) {
                MediaStatus::Archived
            } else {
                MediaStatus::Active
            },
            title: self.title,
            description: self.description,
            notes: self.source_metadata.get("notes").and_then(Value::as_str).map(str::to_owned),
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
            created_at: self.created_at,
            updated_at: self.updated_at,
            archived_at: (self.source_metadata.get("archived").and_then(Value::as_bool)
                == Some(true))
            .then_some(self.updated_at),
        })
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
        Ok(StorageReceipt {
            media_id: self.id,
            storage_chat_id: self
                .telegram_storage_chat_id
                .ok_or(LibraryRepositoryError::StorageReceiptMissing(self.id))?,
            storage_message_id: self
                .telegram_storage_message_id
                .ok_or(LibraryRepositoryError::StorageReceiptMissing(self.id))?,
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

fn tag_from_name(name: &str, created_at: OffsetDateTime) -> Tag {
    Tag { normalized_name: name.to_owned(), display_name: name.to_owned(), created_at }
}

fn with_notes(mut metadata: Value, notes: Option<String>) -> Value {
    if !metadata.is_object() {
        metadata = json!({ "metadata": metadata });
    }
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

fn validate_media_ingest(ingest: &MediaIngest) -> Result<(), LibraryRepositoryError> {
    let Some(sha256) = ingest.metadata.sha256.as_deref() else {
        return Err(LibraryRepositoryError::MissingSha256);
    };
    if sha256.len() != 32 {
        return Err(LibraryRepositoryError::InvalidSha256Length { actual: sha256.len() });
    }
    Ok(())
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
    #[error("invalid video identity alignment: {0}")]
    InvalidAlignment(String),
    #[error("duplicate evidence exceeds the {max}-byte limit")]
    DuplicateEvidenceTooLarge { max: usize },
    #[error("media {0} was not found")]
    ResourceMissing(Uuid),
    #[error("media update must change at least one field")]
    EmptyUpdate,
    #[error("media {0} was modified by another request")]
    OptimisticConflict(Uuid),
    #[error("tag is not attached")]
    TagNotAttached,
    #[error("media search limit must be between 1 and 100, got {value}")]
    InvalidLimit { value: u32 },
    #[error("media SHA-256 is required")]
    MissingSha256,
    #[error("media SHA-256 must contain 32 bytes, got {actual}")]
    InvalidSha256Length { actual: usize },
    #[error("media {0} has an invalid kind")]
    UnknownMediaKind(String),
    #[error("media has an invalid storage state: {0}")]
    UnknownStorageState(String),
    #[error("media source has an invalid kind: {0}")]
    UnknownSourceKind(String),
    #[error("media violates a repository invariant: {0}")]
    Invariant(&'static str),
    #[error("invalid numeric value for {field}")]
    InvalidNumber { field: &'static str },
    #[error("storage upload for media {0} is active")]
    StorageUploadActive(Uuid),
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
