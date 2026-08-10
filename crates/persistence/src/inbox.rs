use crate::library::LibraryRepositoryError;
use serde_json::json;
use sooqa_inbox::{
    AssetNormalization, Ingest, IngestFinalization, IngestKind, IngestStateError, IngestStatus,
    IngestSubmission, SourceDownload, SourceInspection, SourceMediaKind, SubmittedVia,
};
use sooqa_jobs::{JobAttempt, NewJob};
use sooqa_library::{
    MAX_VIDEO_DUPLICATE_EVIDENCE_BYTES, MAX_VIDEO_DUPLICATE_MATCHES, MediaIngest,
    VideoIdentityOutcome,
};
use sooqa_media::{SequenceAlignmentConfig, VideoSequenceFingerprint};
use sqlx::{FromRow, PgPool};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Clone)]
pub struct InboxRepository {
    pool: PgPool,
}

impl InboxRepository {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create_ingest(
        &self,
        submission: IngestSubmission,
    ) -> Result<CreateIngestResult, InboxRepositoryError> {
        let request_hash = submission.request_hash();
        let request_id = Uuid::now_v7();
        let mut transaction = self.pool.begin().await?;
        let mut request = Ingest::from_submission(request_id, &submission);
        request
            .transition_to(IngestStatus::Queued)
            .expect("received ingest requests must be queueable");
        let input_key = submission.idempotency_key.clone().unwrap_or_else(|| {
            format!("{}:{}", submission.kind.as_str(), submission.normalized_url)
        });
        let inserted_id = sqlx::query_scalar::<_, Uuid>(
            r#"
            INSERT INTO ingests (
                id, input_key, request_hash, input_kind, state, submitted_via,
                input_json, source_url, page_url, page_title, supplied_caption,
                supplied_description, supplied_tags, media_id, error_code, error_message, created_at,
                updated_at, completed_at
            )
            VALUES ($1, $2, $3, $4, 'queued', $5, $6, $7, $8, $9, $10, $11,
                    $12, $13, $14, $15, $16, $17, $18)
            ON CONFLICT (input_key) DO NOTHING
            RETURNING id
            "#,
        )
        .bind(request.id)
        .bind(&input_key)
        .bind(request_hash.as_slice())
        .bind(request.kind.as_str())
        .bind(request.submitted_via.as_str())
        .bind(&request.original_input)
        .bind(&request.source_url)
        .bind(&request.page_url)
        .bind(&request.page_title)
        .bind(&request.supplied_caption)
        .bind(&request.supplied_description)
        .bind(&request.supplied_tags)
        .bind(request.media_id)
        .bind(&request.error_code)
        .bind(&request.error_message)
        .bind(request.created_at)
        .bind(request.updated_at)
        .bind(request.completed_at)
        .fetch_optional(&mut *transaction)
        .await?;

        if inserted_id.is_none() {
            let existing = sqlx::query_as::<_, IngestIdentityRow>(
                "SELECT id, request_hash FROM ingests WHERE input_key = $1 FOR UPDATE",
            )
            .bind(&input_key)
            .fetch_one(&mut *transaction)
            .await?;
            if existing.request_hash.as_slice() != request_hash.as_slice() {
                return Err(InboxRepositoryError::IdempotencyConflict { key: input_key });
            }
            let request = load_request(&mut transaction, existing.id).await?;
            transaction.commit().await?;
            return Ok(CreateIngestResult { ingest: request, created: false });
        }

        match request.kind {
            IngestKind::Url => insert_inspect_job(&mut transaction, &request).await?,
            IngestKind::TelegramMessage | IngestKind::Upload => {
                insert_probe_job(&mut transaction, &request).await?
            }
        }

        transaction.commit().await?;
        Ok(CreateIngestResult { ingest: request, created: true })
    }

    pub async fn find(&self, id: Uuid) -> Result<Option<Ingest>, InboxRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let request = load_request(&mut transaction, id).await;
        match request {
            Ok(request) => {
                transaction.commit().await?;
                Ok(Some(request))
            }
            Err(InboxRepositoryError::ResourceMissing(_)) => {
                transaction.rollback().await?;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    pub async fn begin_source_inspection(
        &self,
        id: Uuid,
        attempt: &JobAttempt,
    ) -> Result<SourceInspectionStart, InboxRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let mut request = load_request(&mut transaction, id).await?;
        let start = if !lock_current_job_attempt(&mut transaction, attempt).await? {
            SourceInspectionStart::AlreadyAdvanced(request)
        } else {
            match request.status {
                IngestStatus::Queued => SourceInspectionStart::Ready(request),
                IngestStatus::FailedRetryable => {
                    request.transition_to(IngestStatus::Queued)?;
                    request.error_code = None;
                    request.error_message = None;
                    request.completed_at = None;
                    request.updated_at = OffsetDateTime::now_utc();
                    update_ingest_state(&mut transaction, &request).await?;
                    SourceInspectionStart::Ready(request)
                }
                _ => SourceInspectionStart::AlreadyAdvanced(request),
            }
        };
        transaction.commit().await?;
        Ok(start)
    }

    pub async fn begin_asset_probe(
        &self,
        id: Uuid,
        attempt: &JobAttempt,
    ) -> Result<AssetProbeStart, InboxRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let mut request = load_request(&mut transaction, id).await?;
        let start = if !lock_current_job_attempt(&mut transaction, attempt).await? {
            AssetProbeStart::AlreadyAdvanced(request)
        } else {
            match request.status {
                IngestStatus::Queued => {
                    request.transition_to(IngestStatus::Downloading)?;
                    request.transition_to(IngestStatus::Probing)?;
                    request.error_code = None;
                    request.error_message = None;
                    request.completed_at = None;
                    request.updated_at = OffsetDateTime::now_utc();
                    update_ingest_state(&mut transaction, &request).await?;
                    AssetProbeStart::Ready(request)
                }
                IngestStatus::Downloading => {
                    request.transition_to(IngestStatus::Probing)?;
                    request.error_code = None;
                    request.error_message = None;
                    request.completed_at = None;
                    request.updated_at = OffsetDateTime::now_utc();
                    update_ingest_state(&mut transaction, &request).await?;
                    AssetProbeStart::Ready(request)
                }
                IngestStatus::Probing => AssetProbeStart::Ready(request),
                IngestStatus::FailedRetryable => {
                    request.transition_to(IngestStatus::Queued)?;
                    request.transition_to(IngestStatus::Downloading)?;
                    request.transition_to(IngestStatus::Probing)?;
                    request.error_code = None;
                    request.error_message = None;
                    request.completed_at = None;
                    request.updated_at = OffsetDateTime::now_utc();
                    update_ingest_state(&mut transaction, &request).await?;
                    AssetProbeStart::Ready(request)
                }
                _ => AssetProbeStart::AlreadyAdvanced(request),
            }
        };
        transaction.commit().await?;
        Ok(start)
    }

    pub async fn begin_source_download(
        &self,
        id: Uuid,
        attempt: &JobAttempt,
    ) -> Result<SourceDownloadStart, InboxRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let mut request = load_request(&mut transaction, id).await?;
        let start = if request.original_input.get("download").is_some()
            || !lock_current_job_attempt(&mut transaction, attempt).await?
        {
            SourceDownloadStart::AlreadyAdvanced(request)
        } else {
            match request.status {
                IngestStatus::Downloading => SourceDownloadStart::Ready(request),
                IngestStatus::FailedRetryable => {
                    request.transition_to(IngestStatus::Queued)?;
                    request.transition_to(IngestStatus::Downloading)?;
                    request.error_code = None;
                    request.error_message = None;
                    request.completed_at = None;
                    request.updated_at = OffsetDateTime::now_utc();
                    update_ingest_state(&mut transaction, &request).await?;
                    SourceDownloadStart::Ready(request)
                }
                _ => SourceDownloadStart::AlreadyAdvanced(request),
            }
        };
        transaction.commit().await?;
        Ok(start)
    }

    pub async fn complete_asset_probe(
        &self,
        id: Uuid,
        attempt: &JobAttempt,
        probe: serde_json::Value,
    ) -> Result<Ingest, InboxRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let mut request = load_request(&mut transaction, id).await?;
        if request.status != IngestStatus::Probing {
            transaction.commit().await?;
            return Ok(request);
        }
        if !lock_current_job_attempt(&mut transaction, attempt).await? {
            transaction.commit().await?;
            return Ok(request);
        }
        let declared_media_kind = request_media_kind(&request);
        let detected_media_kind = probed_media_kind(&probe);
        let media_kind = detected_media_kind.or(declared_media_kind);
        let probed_format = probed_image_format(&probe);
        let unsupported_image_format = media_kind == Some(SourceMediaKind::Image)
            && !request_image_format_is_supported(&request, &probe);
        if let Some(object) = request.original_input.as_object_mut() {
            object.insert("probe".to_owned(), probe);
            if let Some(media_kind) = detected_media_kind {
                object.insert(
                    "probed_media_kind".to_owned(),
                    serde_json::to_value(media_kind).expect("source media kind is serializable"),
                );
            }
        } else {
            request.original_input = json!({
                "source": request.original_input,
                "probe": probe,
                "probed_media_kind": detected_media_kind,
            });
        }
        request.updated_at = OffsetDateTime::now_utc();
        sqlx::query("UPDATE ingests SET input_json = $2, updated_at = $3 WHERE id = $1")
            .bind(request.id)
            .bind(&request.original_input)
            .bind(request.updated_at)
            .execute(&mut *transaction)
            .await?;

        if !matches!(
            media_kind,
            Some(
                SourceMediaKind::Video
                    | SourceMediaKind::Image
                    | SourceMediaKind::Animation
                    | SourceMediaKind::Audio
            )
        ) || unsupported_image_format
        {
            request.transition_to(IngestStatus::FailedTerminal)?;
            request.error_code = Some(match media_kind {
                Some(SourceMediaKind::Image) if unsupported_image_format => {
                    "unsupported_image_format".to_owned()
                }
                Some(_) => "unsupported_media_kind".to_owned(),
                None => "invalid_ingest_state".to_owned(),
            });
            request.error_message = Some(match media_kind {
                Some(SourceMediaKind::Image) if unsupported_image_format => format!(
                    "image input is not a supported JPEG/PNG (declared MIME {:?}, file name {:?}, probed format {:?})",
                    request_mime_type(&request),
                    request_file_name(&request),
                    probed_format
                ),
                Some(media_kind) => format!(
                    "asset media kind {media_kind:?} is not supported by the composed normalizers"
                ),
                None => "ingest request has no stored source media kind".to_owned(),
            });
            request.completed_at = Some(OffsetDateTime::now_utc());
            request.updated_at = OffsetDateTime::now_utc();
            update_ingest_state(&mut transaction, &request).await?;
            succeed_current_job_attempt(&mut transaction, attempt).await?;
            transaction.commit().await?;
            return Ok(request);
        }

        request.transition_to(IngestStatus::Normalizing)?;
        request.error_code = None;
        request.error_message = None;
        request.completed_at = None;
        request.updated_at = OffsetDateTime::now_utc();
        update_ingest_state(&mut transaction, &request).await?;
        insert_normalize_job(&mut transaction, &request, request.force_save).await?;
        succeed_current_job_attempt(&mut transaction, attempt).await?;
        transaction.commit().await?;
        Ok(request)
    }

    pub async fn begin_asset_normalization(
        &self,
        id: Uuid,
        attempt: &JobAttempt,
    ) -> Result<AssetNormalizationStart, InboxRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let mut request = load_request(&mut transaction, id).await?;
        let start = if (request.original_input.get("normalization").is_some()
            && !request.force_save)
            || !lock_current_job_attempt(&mut transaction, attempt).await?
        {
            AssetNormalizationStart::AlreadyAdvanced(request)
        } else {
            match request.status {
                IngestStatus::Normalizing => AssetNormalizationStart::Ready(request),
                IngestStatus::FailedRetryable => {
                    request.transition_to(IngestStatus::Queued)?;
                    request.transition_to(IngestStatus::Downloading)?;
                    request.transition_to(IngestStatus::Probing)?;
                    request.transition_to(IngestStatus::Normalizing)?;
                    request.error_code = None;
                    request.error_message = None;
                    request.completed_at = None;
                    request.updated_at = OffsetDateTime::now_utc();
                    update_ingest_state(&mut transaction, &request).await?;
                    AssetNormalizationStart::Ready(request)
                }
                _ => AssetNormalizationStart::AlreadyAdvanced(request),
            }
        };
        transaction.commit().await?;
        Ok(start)
    }

    pub async fn complete_asset_normalization(
        &self,
        id: Uuid,
        attempt: &JobAttempt,
        normalization: AssetNormalization,
    ) -> Result<Ingest, InboxRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let mut request = load_request(&mut transaction, id).await?;
        if request.status != IngestStatus::Normalizing
            || (request.original_input.get("normalization").is_some() && !request.force_save)
            || !lock_current_job_attempt(&mut transaction, attempt).await?
        {
            transaction.commit().await?;
            return Ok(request);
        }

        let normalization =
            serde_json::to_value(normalization).expect("asset normalization is serializable");
        if let Some(object) = request.original_input.as_object_mut() {
            object.insert("normalization".to_owned(), normalization);
        } else {
            request.original_input =
                json!({ "source": request.original_input, "normalization": normalization });
        }
        sqlx::query("UPDATE ingests SET input_json = $2, updated_at = $3 WHERE id = $1")
            .bind(request.id)
            .bind(&request.original_input)
            .bind(OffsetDateTime::now_utc())
            .execute(&mut *transaction)
            .await?;

        let is_video = normalized_media_kind(&request) == Some(SourceMediaKind::Video);
        request.transition_to(if is_video {
            IngestStatus::Fingerprinting
        } else {
            IngestStatus::Storing
        })?;
        request.error_code = None;
        request.error_message = None;
        request.completed_at = None;
        request.updated_at = OffsetDateTime::now_utc();
        update_ingest_state(&mut transaction, &request).await?;
        if is_video {
            insert_fingerprint_job(&mut transaction, &request, request.force_save).await?;
        } else {
            insert_finalize_job(&mut transaction, &request).await?;
        }
        succeed_current_job_attempt(&mut transaction, attempt).await?;
        transaction.commit().await?;
        Ok(request)
    }

    pub async fn fail_asset_normalization(
        &self,
        id: Uuid,
        attempt: &JobAttempt,
        status: IngestStatus,
        error_code: &str,
        error_message: &str,
    ) -> Result<Ingest, InboxRepositoryError> {
        self.fail_ingest_step(
            id,
            status,
            error_code,
            error_message,
            IngestFailureGuard {
                ignore_completed_download: false,
                attempt: Some(attempt),
                expected_status: Some(IngestStatus::Normalizing),
            },
        )
        .await
    }

    pub async fn begin_ingest_finalization(
        &self,
        id: Uuid,
        attempt: &JobAttempt,
    ) -> Result<IngestFinalizationStart, InboxRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let mut request = load_request(&mut transaction, id).await?;
        let start = if !lock_current_job_attempt(&mut transaction, attempt).await? {
            IngestFinalizationStart::AlreadyAdvanced(request)
        } else {
            match request.status {
                IngestStatus::Storing => IngestFinalizationStart::Ready(request),
                IngestStatus::FailedRetryable => {
                    request.transition_to(IngestStatus::Storing)?;
                    request.error_code = None;
                    request.error_message = None;
                    request.completed_at = None;
                    request.updated_at = OffsetDateTime::now_utc();
                    update_ingest_state(&mut transaction, &request).await?;
                    IngestFinalizationStart::Ready(request)
                }
                _ => IngestFinalizationStart::AlreadyAdvanced(request),
            }
        };
        transaction.commit().await?;
        Ok(start)
    }

    pub async fn complete_ingest_finalization(
        &self,
        id: Uuid,
        attempt: &JobAttempt,
        finalization: IngestFinalization,
    ) -> Result<Ingest, InboxRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let mut request = load_request(&mut transaction, id).await?;
        if request.status != IngestStatus::Storing
            || request.original_input.get("finalization").is_some()
            || !lock_current_job_attempt(&mut transaction, attempt).await?
        {
            transaction.commit().await?;
            return Ok(request);
        }

        if normalized_media_kind(&request) == Some(SourceMediaKind::Video) {
            return Err(InboxRepositoryError::VideoFinalizationNotAllowed);
        }

        let media_id = finalization.media_id;
        request.media_id = Some(media_id);
        let finalization =
            serde_json::to_value(finalization).expect("ingest finalization is serializable");
        if let Some(object) = request.original_input.as_object_mut() {
            object.insert("finalization".to_owned(), finalization);
        } else {
            request.original_input =
                json!({ "source": request.original_input, "finalization": finalization });
        }
        request.error_code = None;
        request.error_message = None;
        request.updated_at = OffsetDateTime::now_utc();
        sqlx::query(
            "UPDATE ingests SET input_json = $2, media_id = $3, updated_at = $4 WHERE id = $1",
        )
        .bind(request.id)
        .bind(&request.original_input)
        .bind(request.media_id)
        .bind(request.updated_at)
        .execute(&mut *transaction)
        .await?;
        advance_after_media_processing(&mut transaction, &mut request, media_id).await?;
        succeed_current_job_attempt(&mut transaction, attempt).await?;
        transaction.commit().await?;
        Ok(request)
    }

    pub async fn begin_ingest_fingerprinting(
        &self,
        id: Uuid,
        attempt: &JobAttempt,
    ) -> Result<IngestFingerprintStart, InboxRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let mut request = load_request(&mut transaction, id).await?;
        let start = if !lock_current_job_attempt(&mut transaction, attempt).await? {
            IngestFingerprintStart::AlreadyAdvanced(request)
        } else {
            match request.status {
                IngestStatus::Fingerprinting => IngestFingerprintStart::Ready(request),
                IngestStatus::FailedRetryable => {
                    request.transition_to(IngestStatus::Fingerprinting)?;
                    request.error_code = None;
                    request.error_message = None;
                    request.completed_at = None;
                    request.updated_at = OffsetDateTime::now_utc();
                    update_ingest_state(&mut transaction, &request).await?;
                    IngestFingerprintStart::Ready(request)
                }
                _ => IngestFingerprintStart::AlreadyAdvanced(request),
            }
        };
        transaction.commit().await?;
        Ok(start)
    }

    pub async fn finalize_video_identity(
        &self,
        id: Uuid,
        attempt: &JobAttempt,
        ingest: MediaIngest,
        fingerprint: Option<&VideoSequenceFingerprint>,
        config: SequenceAlignmentConfig,
    ) -> Result<Ingest, InboxRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let mut request = load_request(&mut transaction, id).await?;
        if request.status != IngestStatus::Fingerprinting
            || !lock_current_job_attempt(&mut transaction, attempt).await?
        {
            transaction.commit().await?;
            return Ok(request);
        }

        let outcome = crate::library::LibraryRepository::resolve_video_identity_in_transaction(
            &mut transaction,
            &ingest,
            fingerprint,
            config,
            request.force_save,
        )
        .await?;
        match outcome {
            VideoIdentityOutcome::DuplicatePending { evidence } => {
                if evidence.matches.len() > MAX_VIDEO_DUPLICATE_MATCHES {
                    return Err(InboxRepositoryError::DuplicateEvidenceTooManyMatches {
                        max: MAX_VIDEO_DUPLICATE_MATCHES,
                    });
                }
                let evidence = serde_json::to_value(evidence)?;
                let encoded = serde_json::to_vec(&evidence)?;
                if encoded.len() > MAX_VIDEO_DUPLICATE_EVIDENCE_BYTES {
                    return Err(InboxRepositoryError::DuplicateEvidenceTooLarge {
                        max: MAX_VIDEO_DUPLICATE_EVIDENCE_BYTES,
                    });
                }
                request.transition_to(IngestStatus::DuplicatePending)?;
                request.media_id = None;
                request.duplicate_evidence = Some(evidence);
                request.error_code = None;
                request.error_message = None;
                request.completed_at = None;
            }
            VideoIdentityOutcome::ExactDuplicate { media_id }
            | VideoIdentityOutcome::NewMedia { media_id } => {
                request.duplicate_evidence = None;
                advance_after_media_processing(&mut transaction, &mut request, media_id).await?;
            }
        }
        request.updated_at = OffsetDateTime::now_utc();
        update_ingest_state(&mut transaction, &request).await?;
        succeed_current_job_attempt(&mut transaction, attempt).await?;
        transaction.commit().await?;
        Ok(request)
    }

    pub async fn force_save(&self, id: Uuid) -> Result<ForceSaveResult, InboxRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let mut request = load_request(&mut transaction, id).await?;
        let mut resumed = false;
        match request.status {
            IngestStatus::DuplicatePending => {
                request.force_save = true;
                request.duplicate_evidence = None;
                clear_pipeline_artifacts(&mut request);
                request.transition_to(IngestStatus::Queued)?;
                request.error_code = None;
                request.error_message = None;
                request.completed_at = None;
                request.updated_at = OffsetDateTime::now_utc();
                update_ingest_state(&mut transaction, &request).await?;
                match request.kind {
                    IngestKind::Url => insert_inspect_job(&mut transaction, &request).await?,
                    IngestKind::TelegramMessage | IngestKind::Upload => {
                        insert_probe_job(&mut transaction, &request).await?
                    }
                }
                resumed = true;
            }
            IngestStatus::Queued
            | IngestStatus::Downloading
            | IngestStatus::Probing
            | IngestStatus::Normalizing
            | IngestStatus::Fingerprinting
            | IngestStatus::Storing
            | IngestStatus::Completed
                if request.force_save => {}
            _ => return Err(InboxRepositoryError::ForceSaveNotAllowed(request.status)),
        }
        transaction.commit().await?;
        Ok(ForceSaveResult { ingest: request, resumed })
    }

    pub async fn complete_storage_for_media(
        &self,
        media_id: Uuid,
    ) -> Result<u64, InboxRepositoryError> {
        let now = OffsetDateTime::now_utc();
        let updated = sqlx::query(
            "UPDATE ingests SET state = 'completed', completed_at = $2, error_code = NULL, error_message = NULL, updated_at = $2 WHERE media_id = $1 AND EXISTS (SELECT 1 FROM media WHERE id = $1 AND storage_state = 'ready') AND (state = 'storing' OR (state = 'failed_retryable' AND error_code IN ('storage_upload', 'storage_unknown')) OR (state = 'failed_terminal' AND error_code IN ('storage_upload', 'storage_unknown')))",
        )
        .bind(media_id)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(updated.rows_affected())
    }

    pub async fn fail_storage_for_media(
        &self,
        media_id: Uuid,
        status: IngestStatus,
        error_code: &str,
        error_message: &str,
    ) -> Result<u64, InboxRepositoryError> {
        if !matches!(status, IngestStatus::FailedRetryable | IngestStatus::FailedTerminal) {
            return Err(InboxRepositoryError::InvalidFailureStatus(status));
        }
        let now = OffsetDateTime::now_utc();
        let completed_at = (status == IngestStatus::FailedTerminal).then_some(now);
        let updated = sqlx::query(
            "UPDATE ingests SET state = $2, error_code = $3, error_message = $4, completed_at = $5, updated_at = $6 WHERE media_id = $1 AND (state = 'storing' OR (state = 'failed_retryable' AND error_code = 'storage_upload'))",
        )
        .bind(media_id)
        .bind(status.as_str())
        .bind(error_code)
        .bind(error_message)
        .bind(completed_at)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(updated.rows_affected())
    }

    pub async fn fail_ingest_fingerprint(
        &self,
        id: Uuid,
        attempt: &JobAttempt,
        status: IngestStatus,
        error_code: &str,
        error_message: &str,
    ) -> Result<Ingest, InboxRepositoryError> {
        self.fail_ingest_step(
            id,
            status,
            error_code,
            error_message,
            IngestFailureGuard {
                ignore_completed_download: false,
                attempt: Some(attempt),
                expected_status: Some(IngestStatus::Fingerprinting),
            },
        )
        .await
    }

    pub async fn fail_ingest_finalization(
        &self,
        id: Uuid,
        attempt: &JobAttempt,
        status: IngestStatus,
        error_code: &str,
        error_message: &str,
    ) -> Result<Ingest, InboxRepositoryError> {
        self.fail_ingest_step(
            id,
            status,
            error_code,
            error_message,
            IngestFailureGuard {
                ignore_completed_download: false,
                attempt: Some(attempt),
                expected_status: Some(IngestStatus::Storing),
            },
        )
        .await
    }

    pub async fn complete_source_inspection(
        &self,
        id: Uuid,
        attempt: &JobAttempt,
        inspection: SourceInspection,
    ) -> Result<Ingest, InboxRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let mut request = load_request(&mut transaction, id).await?;
        if request.status != IngestStatus::Queued
            || !lock_current_job_attempt(&mut transaction, attempt).await?
        {
            transaction.commit().await?;
            return Ok(request);
        }

        request.transition_to(IngestStatus::Downloading)?;
        request.error_code = None;
        request.error_message = None;
        request.completed_at = None;
        request.updated_at = OffsetDateTime::now_utc();
        update_ingest_state(&mut transaction, &request).await?;

        let job = NewJob::download_source(id, inspection);
        sqlx::query(
            r#"
            INSERT INTO queue.jobs (kind, payload, state, dedupe_key)
            VALUES ($1, $2, 'queued', $3)
            ON CONFLICT (dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING
            "#,
        )
        .bind(job.job_type().as_str())
        .bind(job.payload_json())
        .bind(stage_dedupe_key(&request, &format!("ingest:{id}:download_source:v1")))
        .execute(&mut *transaction)
        .await?;

        succeed_current_job_attempt(&mut transaction, attempt).await?;
        transaction.commit().await?;
        Ok(request)
    }

    pub async fn complete_source_download(
        &self,
        id: Uuid,
        attempt: &JobAttempt,
        download: SourceDownload,
    ) -> Result<Ingest, InboxRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let mut request = load_request(&mut transaction, id).await?;
        if request.status != IngestStatus::Downloading
            || request.original_input.get("download").is_some()
            || !lock_current_job_attempt(&mut transaction, attempt).await?
        {
            transaction.commit().await?;
            return Ok(request);
        }

        let download = serde_json::to_value(download).expect("source download is serializable");
        if let Some(object) = request.original_input.as_object_mut() {
            object.insert("download".to_owned(), download);
        } else {
            request.original_input =
                json!({ "source": request.original_input, "download": download });
        }
        request.error_code = None;
        request.error_message = None;
        request.updated_at = OffsetDateTime::now_utc();
        sqlx::query(
            "UPDATE ingests SET input_json = $2, error_code = $3, error_message = $4, updated_at = $5 WHERE id = $1",
        )
        .bind(request.id)
        .bind(&request.original_input)
        .bind(&request.error_code)
        .bind(&request.error_message)
        .bind(request.updated_at)
        .execute(&mut *transaction)
        .await?;

        insert_probe_job(&mut transaction, &request).await?;

        succeed_current_job_attempt(&mut transaction, attempt).await?;
        transaction.commit().await?;
        Ok(request)
    }

    pub async fn fail_source_download(
        &self,
        id: Uuid,
        attempt: &JobAttempt,
        status: IngestStatus,
        error_code: &str,
        error_message: &str,
    ) -> Result<Ingest, InboxRepositoryError> {
        self.fail_ingest_step(
            id,
            status,
            error_code,
            error_message,
            IngestFailureGuard {
                ignore_completed_download: true,
                attempt: Some(attempt),
                expected_status: Some(IngestStatus::Downloading),
            },
        )
        .await
    }

    pub async fn fail_source_inspection(
        &self,
        id: Uuid,
        attempt: &JobAttempt,
        status: IngestStatus,
        error_code: &str,
        error_message: &str,
    ) -> Result<Ingest, InboxRepositoryError> {
        self.fail_ingest_step(
            id,
            status,
            error_code,
            error_message,
            IngestFailureGuard {
                ignore_completed_download: false,
                attempt: Some(attempt),
                expected_status: Some(IngestStatus::Queued),
            },
        )
        .await
    }

    pub async fn fail_asset_probe(
        &self,
        id: Uuid,
        attempt: &JobAttempt,
        status: IngestStatus,
        error_code: &str,
        error_message: &str,
    ) -> Result<Ingest, InboxRepositoryError> {
        self.fail_ingest_step(
            id,
            status,
            error_code,
            error_message,
            IngestFailureGuard {
                ignore_completed_download: false,
                attempt: Some(attempt),
                expected_status: Some(IngestStatus::Probing),
            },
        )
        .await
    }

    async fn fail_ingest_step(
        &self,
        id: Uuid,
        status: IngestStatus,
        error_code: &str,
        error_message: &str,
        guard: IngestFailureGuard<'_>,
    ) -> Result<Ingest, InboxRepositoryError> {
        if !matches!(status, IngestStatus::FailedRetryable | IngestStatus::FailedTerminal) {
            return Err(InboxRepositoryError::InvalidFailureStatus(status));
        }

        let mut transaction = self.pool.begin().await?;
        let mut request = load_request(&mut transaction, id).await?;
        if guard.ignore_completed_download && request.original_input.get("download").is_some() {
            transaction.commit().await?;
            return Ok(request);
        }
        if guard.expected_status.is_some_and(|expected| request.status != expected) {
            transaction.commit().await?;
            return Ok(request);
        }
        if let Some(attempt) = guard.attempt
            && !lock_current_job_attempt(&mut transaction, attempt).await?
        {
            transaction.commit().await?;
            return Ok(request);
        }
        if request.status.is_terminal() {
            transaction.commit().await?;
            return Ok(request);
        }

        request.transition_to(status)?;
        request.error_code = Some(error_code.to_owned());
        request.error_message = Some(error_message.to_owned());
        request.completed_at =
            (status == IngestStatus::FailedTerminal).then(OffsetDateTime::now_utc);
        request.updated_at = OffsetDateTime::now_utc();
        update_ingest_state(&mut transaction, &request).await?;
        transaction.commit().await?;
        Ok(request)
    }
}

#[derive(Debug, Clone)]
pub enum SourceInspectionStart {
    Ready(Ingest),
    AlreadyAdvanced(Ingest),
}

#[derive(Debug, Clone)]
pub enum SourceDownloadStart {
    Ready(Ingest),
    AlreadyAdvanced(Ingest),
}

#[derive(Debug, Clone)]
pub enum AssetProbeStart {
    Ready(Ingest),
    AlreadyAdvanced(Ingest),
}

#[derive(Debug, Clone)]
pub enum AssetNormalizationStart {
    Ready(Ingest),
    AlreadyAdvanced(Ingest),
}

#[derive(Debug, Clone)]
pub enum IngestFinalizationStart {
    Ready(Ingest),
    AlreadyAdvanced(Ingest),
}

#[derive(Debug, Clone)]
pub enum IngestFingerprintStart {
    Ready(Ingest),
    AlreadyAdvanced(Ingest),
}

#[derive(Debug, Clone)]
pub struct ForceSaveResult {
    pub ingest: Ingest,
    pub resumed: bool,
}

struct IngestFailureGuard<'a> {
    ignore_completed_download: bool,
    attempt: Option<&'a JobAttempt>,
    expected_status: Option<IngestStatus>,
}

#[derive(Debug, Clone)]
pub struct CreateIngestResult {
    pub ingest: Ingest,
    pub created: bool,
}

async fn update_ingest_state(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    request: &Ingest,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE ingests
        SET state = $2,
            input_json = $3,
            media_id = $4,
            force_save = $5,
            duplicate_evidence = $6,
            error_code = $7,
            error_message = $8,
            updated_at = $9,
            completed_at = $10
        WHERE id = $1
        "#,
    )
    .bind(request.id)
    .bind(request.status.as_str())
    .bind(&request.original_input)
    .bind(request.media_id)
    .bind(request.force_save)
    .bind(&request.duplicate_evidence)
    .bind(&request.error_code)
    .bind(&request.error_message)
    .bind(request.updated_at)
    .bind(request.completed_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn advance_after_media_processing(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    request: &mut Ingest,
    media_id: Uuid,
) -> Result<(), InboxRepositoryError> {
    let storage_state =
        sqlx::query_scalar::<_, String>("SELECT storage_state FROM media WHERE id = $1")
            .bind(media_id)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(InboxRepositoryError::MissingMediaId(request.id))?;

    request.media_id = Some(media_id);
    request.error_code = None;
    request.error_message = None;
    request.completed_at = None;
    match storage_state.as_str() {
        "ready" => {
            request.transition_to(IngestStatus::Completed)?;
            request.completed_at = Some(OffsetDateTime::now_utc());
        }
        "pending_storage" => {
            request.transition_to(IngestStatus::Storing)?;
            insert_storage_job(transaction, media_id).await?;
        }
        "storage_unknown" | "missing" => {
            request.transition_to(IngestStatus::FailedTerminal)?;
            request.error_code = Some("storage_unknown".to_owned());
            request.error_message = Some(
                "media storage requires explicit reconciliation before ingest can complete"
                    .to_owned(),
            );
            request.completed_at = Some(OffsetDateTime::now_utc());
        }
        state => return Err(InboxRepositoryError::UnknownStorageState(state.to_owned())),
    }
    request.updated_at = OffsetDateTime::now_utc();
    update_ingest_state(transaction, request).await?;
    Ok(())
}

async fn insert_job(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    job: NewJob,
    dedupe_key: String,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO queue.jobs (kind, payload, state, run_at, max_attempts, dedupe_key)
        VALUES ($1, $2, 'queued', COALESCE($3, now()), $4, $5)
        ON CONFLICT (dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING
        "#,
    )
    .bind(job.job_type().as_str())
    .bind(job.payload_json())
    .bind(job.run_at_value())
    .bind(job.max_attempts_value())
    .bind(dedupe_key)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_inspect_job(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    request: &Ingest,
) -> Result<(), sqlx::Error> {
    insert_job(
        transaction,
        NewJob::inspect_source(request.id),
        stage_dedupe_key(request, &format!("ingest:{}:inspect_source:v1", request.id)),
    )
    .await
}

async fn insert_probe_job(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    request: &Ingest,
) -> Result<(), sqlx::Error> {
    insert_job(
        transaction,
        NewJob::probe_asset(request.id),
        stage_dedupe_key(request, &format!("ingest:{}:probe_asset:v1", request.id)),
    )
    .await
}

async fn insert_storage_job(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    media_id: Uuid,
) -> Result<(), sqlx::Error> {
    let Some(generation) = sqlx::query_scalar::<_, i32>(
        "SELECT storage_generation FROM media WHERE id = $1 AND storage_state = 'pending_storage'",
    )
    .bind(media_id)
    .fetch_optional(&mut **transaction)
    .await?
    else {
        return Ok(());
    };
    insert_job(
        transaction,
        NewJob::upload_storage_asset_generation(media_id, generation),
        format!("media:{media_id}:upload_storage:v1:{generation}"),
    )
    .await
}

async fn insert_normalize_job(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    request: &Ingest,
    force_save: bool,
) -> Result<(), sqlx::Error> {
    insert_job(
        transaction,
        NewJob::normalize_asset(request.id),
        if force_save {
            format!("ingest:{}:normalize_asset:v1:force_save", request.id)
        } else {
            format!("ingest:{}:normalize_asset:v1", request.id)
        },
    )
    .await
}

async fn insert_finalize_job(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    request: &Ingest,
) -> Result<(), sqlx::Error> {
    insert_job(
        transaction,
        NewJob::finalize_ingest(request.id),
        stage_dedupe_key(request, &format!("ingest:{}:finalize_ingest:v1", request.id)),
    )
    .await
}

async fn insert_fingerprint_job(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    request: &Ingest,
    force_save: bool,
) -> Result<(), sqlx::Error> {
    insert_job(
        transaction,
        NewJob::compute_fingerprint(request.id),
        if force_save {
            format!("ingest:{}:compute_fingerprint:v2:force_save", request.id)
        } else {
            format!("ingest:{}:compute_fingerprint:v2", request.id)
        },
    )
    .await
}

fn stage_dedupe_key(request: &Ingest, initial_key: &str) -> String {
    if request.force_save { format!("{initial_key}:force_save") } else { initial_key.to_owned() }
}

fn clear_pipeline_artifacts(request: &mut Ingest) {
    if let Some(object) = request.original_input.as_object_mut() {
        for key in ["download", "probe", "probed_media_kind", "normalization", "finalization"] {
            object.remove(key);
        }
    }
}

fn request_media_kind(request: &Ingest) -> Option<SourceMediaKind> {
    if let Some(value) = request.original_input.get("probed_media_kind")
        && let Ok(media_kind) = serde_json::from_value(value.clone())
    {
        return Some(media_kind);
    }
    let value = if request.kind == IngestKind::Url {
        request.original_input.get("download")?.get("media_kind")?
    } else {
        request.original_input.get("media_kind")?
    };
    serde_json::from_value(value.clone()).ok()
}

fn normalized_media_kind(request: &Ingest) -> Option<SourceMediaKind> {
    request
        .original_input
        .get("normalization")
        .and_then(|value| value.get("media_kind"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
}

fn request_mime_type(request: &Ingest) -> Option<&str> {
    let value = if request.kind == IngestKind::Url {
        request.original_input.get("download")?.get("mime_type")?
    } else {
        request.original_input.get("mime_type")?
    };
    value.as_str()
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum ImageFormatKind {
    Jpeg,
    Png,
    Unsupported,
    Unknown,
}

fn request_file_name(request: &Ingest) -> Option<&str> {
    if request.kind == IngestKind::Url {
        None
    } else {
        request.original_input.get("file_name")?.as_str()
    }
}

fn request_image_format_is_supported(request: &Ingest, probe: &serde_json::Value) -> bool {
    let declared = request_mime_type(request)
        .map(image_format_from_mime)
        .filter(|format| *format != ImageFormatKind::Unknown)
        .or_else(|| request_file_name(request).map(image_format_from_file_name))
        .unwrap_or(ImageFormatKind::Unknown);
    let probed = probed_image_format(probe);
    match probed {
        ImageFormatKind::Jpeg | ImageFormatKind::Png => true,
        ImageFormatKind::Unsupported => false,
        ImageFormatKind::Unknown => {
            matches!(declared, ImageFormatKind::Jpeg | ImageFormatKind::Png)
        }
    }
}

fn image_format_from_mime(mime_type: &str) -> ImageFormatKind {
    let mime_type = mime_type.split(';').next().map(str::trim).unwrap_or_default();
    if mime_type.eq_ignore_ascii_case("image/jpeg") {
        ImageFormatKind::Jpeg
    } else if mime_type.eq_ignore_ascii_case("image/png") {
        ImageFormatKind::Png
    } else if mime_type.to_ascii_lowercase().starts_with("image/") {
        ImageFormatKind::Unsupported
    } else {
        ImageFormatKind::Unknown
    }
}

fn image_format_from_file_name(file_name: &str) -> ImageFormatKind {
    match file_name.rsplit('.').next().map(str::to_ascii_lowercase).as_deref() {
        Some("jpg" | "jpeg") => ImageFormatKind::Jpeg,
        Some("png") => ImageFormatKind::Png,
        Some("gif" | "webp" | "avif") => ImageFormatKind::Unsupported,
        _ => ImageFormatKind::Unknown,
    }
}

fn probed_image_format(probe: &serde_json::Value) -> ImageFormatKind {
    let container = probe.get("container_format").and_then(serde_json::Value::as_str);
    let codec = probe.get("streams").and_then(serde_json::Value::as_array).and_then(|streams| {
        streams.iter().find_map(|stream| {
            (stream.get("kind").and_then(serde_json::Value::as_str) == Some("video"))
                .then(|| stream.get("codec").and_then(serde_json::Value::as_str))
                .flatten()
        })
    });
    for value in [container, codec].into_iter().flatten() {
        let value = value.to_ascii_lowercase();
        let kind = if value.contains("jpeg") || value.contains("mjpeg") {
            ImageFormatKind::Jpeg
        } else if value.contains("png") {
            ImageFormatKind::Png
        } else if value.contains("gif") || value.contains("webp") || value.contains("avif") {
            ImageFormatKind::Unsupported
        } else {
            ImageFormatKind::Unknown
        };
        if kind != ImageFormatKind::Unknown {
            return kind;
        }
    }
    ImageFormatKind::Unknown
}

fn probed_media_kind(probe: &serde_json::Value) -> Option<SourceMediaKind> {
    let container = probe.get("container_format").and_then(serde_json::Value::as_str);
    let codecs = probe
        .get("streams")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|stream| stream.get("kind").and_then(serde_json::Value::as_str) == Some("video"))
        .filter_map(|stream| stream.get("codec").and_then(serde_json::Value::as_str));
    let codecs = codecs.collect::<Vec<_>>();
    let container = container.map(str::to_ascii_lowercase);
    let is_gif = container.as_deref().is_some_and(|value| value.contains("gif"))
        || codecs.iter().any(|value| value.to_ascii_lowercase().contains("gif"));
    if is_gif {
        return Some(SourceMediaKind::Animation);
    }

    let is_image_container = container.as_deref().is_some_and(|value| {
        ["image2", "png", "jpeg", "jpg", "webp", "avif", "mjpeg"]
            .iter()
            .any(|format| value.contains(format))
    });
    let is_image_codec = codecs
        .iter()
        .any(|value| ["png", "webp"].iter().any(|format| value.eq_ignore_ascii_case(format)))
        || (container.is_none() && codecs.iter().any(|value| value.eq_ignore_ascii_case("mjpeg")));
    if is_image_container || is_image_codec {
        return Some(SourceMediaKind::Image);
    }

    let streams = probe.get("streams").and_then(serde_json::Value::as_array);
    if streams.is_some_and(|streams| {
        streams
            .iter()
            .any(|stream| stream.get("kind").and_then(serde_json::Value::as_str) == Some("video"))
    }) {
        return Some(SourceMediaKind::Video);
    }
    if streams.is_some_and(|streams| {
        streams
            .iter()
            .any(|stream| stream.get("kind").and_then(serde_json::Value::as_str) == Some("audio"))
    }) {
        return Some(SourceMediaKind::Audio);
    }
    None
}

async fn load_request(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    id: Uuid,
) -> Result<Ingest, InboxRepositoryError> {
    let row = sqlx::query_as::<_, IngestRow>(
        r#"
        SELECT id, input_kind, state, submitted_via, input_json, source_url, page_url,
               page_title, supplied_caption, supplied_description, supplied_tags, input_key, media_id,
               force_save, duplicate_evidence, error_code, error_message,
               created_at, updated_at, completed_at
        FROM ingests
        WHERE id = $1
        FOR UPDATE
        "#,
    )
    .bind(id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(InboxRepositoryError::ResourceMissing(id))?;

    row.into_ingest()
}

async fn lock_current_job_attempt(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    attempt: &JobAttempt,
) -> Result<bool, sqlx::Error> {
    let current_job = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT id
        FROM queue.jobs
        WHERE id = $1
          AND state = 'running'
          AND attempt_count = $2
          AND lease_owner = $3
          AND lease_token = $4
          AND lease_expires_at > clock_timestamp()
        FOR UPDATE
        "#,
    )
    .bind(attempt.job_id)
    .bind(attempt.attempt_number)
    .bind(&attempt.lease_owner)
    .bind(attempt.lease_token)
    .fetch_optional(&mut **transaction)
    .await?;
    Ok(current_job.is_some())
}

async fn succeed_current_job_attempt(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    attempt: &JobAttempt,
) -> Result<(), InboxRepositoryError> {
    let result = sqlx::query(
        r#"
        UPDATE queue.jobs
        SET state = 'succeeded', lease_token = NULL, lease_owner = NULL,
            lease_expires_at = NULL, last_heartbeat_at = NULL,
            completed_at = now(), updated_at = now()
        WHERE id = $1
          AND state = 'running'
          AND attempt_count = $2
          AND lease_owner = $3
          AND lease_token = $4
          AND lease_expires_at > clock_timestamp()
        "#,
    )
    .bind(attempt.job_id)
    .bind(attempt.attempt_number)
    .bind(&attempt.lease_owner)
    .bind(attempt.lease_token)
    .execute(&mut **transaction)
    .await?;

    if result.rows_affected() != 1 {
        return Err(InboxRepositoryError::JobLeaseLost);
    }
    Ok(())
}

#[derive(Debug, FromRow)]
struct IngestRow {
    id: Uuid,
    input_kind: String,
    state: String,
    submitted_via: String,
    input_json: serde_json::Value,
    source_url: Option<String>,
    page_url: Option<String>,
    page_title: Option<String>,
    supplied_caption: Option<String>,
    supplied_description: Option<String>,
    supplied_tags: Vec<String>,
    input_key: String,
    media_id: Option<Uuid>,
    force_save: bool,
    duplicate_evidence: Option<serde_json::Value>,
    error_code: Option<String>,
    error_message: Option<String>,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
    completed_at: Option<OffsetDateTime>,
}

impl IngestRow {
    fn into_ingest(self) -> Result<Ingest, InboxRepositoryError> {
        Ok(Ingest {
            id: self.id,
            kind: IngestKind::try_from(self.input_kind.as_str())
                .map_err(InboxRepositoryError::UnknownIngestKind)?,
            status: IngestStatus::try_from(self.state.as_str())
                .map_err(InboxRepositoryError::UnknownIngestStatus)?,
            submitted_via: SubmittedVia::try_from(self.submitted_via.as_str())
                .map_err(InboxRepositoryError::UnknownSubmittedVia)?,
            submitted_by_admin_id: None,
            original_input: self.input_json,
            source_url: self.source_url.ok_or(InboxRepositoryError::MissingSourceUrl(self.id))?,
            page_url: self.page_url,
            page_title: self.page_title,
            supplied_caption: self.supplied_caption,
            supplied_description: self.supplied_description,
            supplied_tags: self.supplied_tags,
            idempotency_key: Some(self.input_key),
            media_id: self.media_id,
            force_save: self.force_save,
            duplicate_evidence: self.duplicate_evidence,
            error_code: self.error_code,
            error_message: self.error_message,
            created_at: self.created_at,
            updated_at: self.updated_at,
            completed_at: self.completed_at,
        })
    }
}

#[derive(Debug, FromRow)]
struct IngestIdentityRow {
    id: Uuid,
    request_hash: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum InboxRepositoryError {
    #[error("idempotency key already belongs to a different request: {key}")]
    IdempotencyConflict { key: String },
    #[error("ingest {0} was not found")]
    ResourceMissing(Uuid),
    #[error("ingest request {0} has no source URL")]
    MissingSourceUrl(Uuid),
    #[error("ingest {0} has no media ID")]
    MissingMediaId(Uuid),
    #[error("media has an invalid storage state: {0}")]
    UnknownStorageState(String),
    #[error("unknown ingest kind in database: {0}")]
    UnknownIngestKind(String),
    #[error("unknown ingest status in database: {0}")]
    UnknownIngestStatus(String),
    #[error("unknown submission source in database: {0}")]
    UnknownSubmittedVia(String),
    #[error("invalid ingest failure status: {0:?}")]
    InvalidFailureStatus(IngestStatus),
    #[error("force-save is not allowed while ingest is in {0:?}")]
    ForceSaveNotAllowed(IngestStatus),
    #[error("video ingests must complete the identity gate before storage finalization")]
    VideoFinalizationNotAllowed,
    #[error("duplicate evidence exceeds the {max}-byte limit")]
    DuplicateEvidenceTooLarge { max: usize },
    #[error("duplicate evidence contains more than {max} matches")]
    DuplicateEvidenceTooManyMatches { max: usize },
    #[error("invalid ingest state transition: {0}")]
    InvalidStateTransition(#[from] IngestStateError),
    #[error("job lease was lost before ingest completion could be committed")]
    JobLeaseLost,
    #[error("database operation failed: {0}")]
    Database(#[from] sqlx::Error),
    #[error("serialization operation failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("library identity operation failed: {0}")]
    Library(#[from] LibraryRepositoryError),
}
