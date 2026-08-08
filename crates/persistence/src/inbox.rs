use serde_json::{Value, json};
use sooqa_inbox::{
    AssetNormalization, IngestFinalization, IngestKind, IngestRequest, IngestStateError,
    IngestStatus, IngestSubmission, SourceDownload, SourceInspection, SourceMediaKind,
    SubmittedVia,
};
use sooqa_jobs::{JobAttempt, NewJob};
use sqlx::{FromRow, PgPool};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

const IDEMPOTENCY_SCOPE: &str = "ingest:create";

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

        if let Some(idempotency_key) = submission.idempotency_key.as_deref() {
            let inserted_id = sqlx::query_scalar::<_, Uuid>(
                r#"
                INSERT INTO idempotency_records (
                    scope, idempotency_key, request_hash, resource_type, resource_id,
                    response_status, response_body
                )
                VALUES ($1, $2, $3, 'ingest_request', $4, 202, $5)
                ON CONFLICT (scope, idempotency_key) DO NOTHING
                RETURNING id
                "#,
            )
            .bind(IDEMPOTENCY_SCOPE)
            .bind(idempotency_key)
            .bind(request_hash.as_slice())
            .bind(request_id)
            .bind(json!({ "id": request_id, "status": IngestStatus::Queued.as_str() }))
            .fetch_optional(&mut *transaction)
            .await?;

            if inserted_id.is_none() {
                let existing = sqlx::query_as::<_, IdempotencyRow>(
                    r#"
                    SELECT request_hash, resource_id
                    FROM idempotency_records
                    WHERE scope = $1 AND idempotency_key = $2
                    "#,
                )
                .bind(IDEMPOTENCY_SCOPE)
                .bind(idempotency_key)
                .fetch_one(&mut *transaction)
                .await?;

                if existing.request_hash.as_slice() != request_hash.as_slice() {
                    return Err(InboxRepositoryError::IdempotencyConflict {
                        key: idempotency_key.to_owned(),
                    });
                }

                let existing_id = existing
                    .resource_id
                    .ok_or(InboxRepositoryError::IncompleteIdempotencyRecord)?;
                let request = load_request(&mut transaction, existing_id).await?;
                transaction.commit().await?;
                return Ok(CreateIngestResult { request, created: false });
            }
        }

        let mut request = IngestRequest::from_submission(request_id, &submission);
        request
            .transition_to(IngestStatus::Queued)
            .expect("received ingest requests must be queueable");
        insert_request(&mut transaction, &request).await?;
        match request.kind {
            IngestKind::Url => insert_inspect_job(&mut transaction, &request).await?,
            IngestKind::TelegramMessage | IngestKind::Upload => {
                insert_probe_job(&mut transaction, &request).await?
            }
        }

        transaction.commit().await?;
        Ok(CreateIngestResult { request, created: true })
    }

    pub async fn find(&self, id: Uuid) -> Result<Option<IngestRequest>, InboxRepositoryError> {
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
    ) -> Result<SourceInspectionStart, InboxRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let mut request = load_request(&mut transaction, id).await?;
        let start = match request.status {
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
    ) -> Result<IngestRequest, InboxRepositoryError> {
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
        sqlx::query(
            "UPDATE ingest_requests SET original_input = $2, updated_at = $3 WHERE id = $1",
        )
        .bind(request.id)
        .bind(&request.original_input)
        .bind(request.updated_at)
        .execute(&mut *transaction)
        .await?;

        if !matches!(media_kind, Some(SourceMediaKind::Video | SourceMediaKind::Image))
            || unsupported_image_format
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
            transaction.commit().await?;
            return Ok(request);
        }

        request.transition_to(IngestStatus::Normalizing)?;
        request.error_code = None;
        request.error_message = None;
        request.completed_at = None;
        request.updated_at = OffsetDateTime::now_utc();
        update_ingest_state(&mut transaction, &request).await?;
        insert_normalize_job(&mut transaction, &request).await?;
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
        let start = if request.original_input.get("normalization").is_some()
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
    ) -> Result<IngestRequest, InboxRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let mut request = load_request(&mut transaction, id).await?;
        if request.status != IngestStatus::Normalizing
            || request.original_input.get("normalization").is_some()
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
        sqlx::query(
            "UPDATE ingest_requests SET original_input = $2, updated_at = $3 WHERE id = $1",
        )
        .bind(request.id)
        .bind(&request.original_input)
        .bind(OffsetDateTime::now_utc())
        .execute(&mut *transaction)
        .await?;

        request.transition_to(IngestStatus::Storing)?;
        request.error_code = None;
        request.error_message = None;
        request.completed_at = None;
        request.updated_at = OffsetDateTime::now_utc();
        update_ingest_state(&mut transaction, &request).await?;
        insert_finalize_job(&mut transaction, &request).await?;
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
    ) -> Result<IngestRequest, InboxRepositoryError> {
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
    ) -> Result<IngestRequest, InboxRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let mut request = load_request(&mut transaction, id).await?;
        if request.status != IngestStatus::Storing
            || request.original_input.get("finalization").is_some()
            || !lock_current_job_attempt(&mut transaction, attempt).await?
        {
            transaction.commit().await?;
            return Ok(request);
        }

        let finalization =
            serde_json::to_value(finalization).expect("ingest finalization is serializable");
        if let Some(object) = request.original_input.as_object_mut() {
            object.insert("finalization".to_owned(), finalization);
        } else {
            request.original_input =
                json!({ "source": request.original_input, "finalization": finalization });
        }
        request.transition_to(IngestStatus::Fingerprinting)?;
        request.error_code = None;
        request.error_message = None;
        request.completed_at = None;
        request.updated_at = OffsetDateTime::now_utc();
        sqlx::query(
            "UPDATE ingest_requests SET original_input = $2, updated_at = $3 WHERE id = $1",
        )
        .bind(request.id)
        .bind(&request.original_input)
        .bind(request.updated_at)
        .execute(&mut *transaction)
        .await?;
        update_ingest_state(&mut transaction, &request).await?;
        insert_fingerprint_job(&mut transaction, &request).await?;
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
        let start = if request.original_input.get("fingerprint").is_some()
            || !lock_current_job_attempt(&mut transaction, attempt).await?
        {
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

    pub async fn complete_ingest_fingerprint(
        &self,
        id: Uuid,
        attempt: &JobAttempt,
        fingerprint: Option<Value>,
    ) -> Result<IngestRequest, InboxRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let mut request = load_request(&mut transaction, id).await?;
        if request.status != IngestStatus::Fingerprinting
            || request.original_input.get("fingerprint").is_some()
            || !lock_current_job_attempt(&mut transaction, attempt).await?
        {
            transaction.commit().await?;
            return Ok(request);
        }

        if let Some(fingerprint) = fingerprint {
            if let Some(object) = request.original_input.as_object_mut() {
                object.insert("fingerprint".to_owned(), fingerprint);
            } else {
                request.original_input = json!({
                    "source": request.original_input,
                    "fingerprint": fingerprint,
                });
            }
        }
        request.transition_to(IngestStatus::Completed)?;
        request.error_code = None;
        request.error_message = None;
        request.completed_at = Some(OffsetDateTime::now_utc());
        request.updated_at = OffsetDateTime::now_utc();
        sqlx::query(
            "UPDATE ingest_requests SET original_input = $2, updated_at = $3 WHERE id = $1",
        )
        .bind(request.id)
        .bind(&request.original_input)
        .bind(request.updated_at)
        .execute(&mut *transaction)
        .await?;
        update_ingest_state(&mut transaction, &request).await?;
        transaction.commit().await?;
        Ok(request)
    }

    pub async fn fail_ingest_fingerprint(
        &self,
        id: Uuid,
        attempt: &JobAttempt,
        status: IngestStatus,
        error_code: &str,
        error_message: &str,
    ) -> Result<IngestRequest, InboxRepositoryError> {
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
    ) -> Result<IngestRequest, InboxRepositoryError> {
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
        inspection: SourceInspection,
    ) -> Result<IngestRequest, InboxRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let mut request = load_request(&mut transaction, id).await?;
        if request.status != IngestStatus::Queued {
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
            INSERT INTO jobs (job_type, payload_json, idempotency_key)
            VALUES ($1, $2, $3)
            ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
            "#,
        )
        .bind(job.job_type().as_str())
        .bind(job.payload_json())
        .bind(format!("ingest:{id}:download_source:v1"))
        .execute(&mut *transaction)
        .await?;

        transaction.commit().await?;
        Ok(request)
    }

    pub async fn complete_source_download(
        &self,
        id: Uuid,
        attempt: &JobAttempt,
        download: SourceDownload,
    ) -> Result<IngestRequest, InboxRepositoryError> {
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
            "UPDATE ingest_requests SET original_input = $2, error_code = $3, error_message = $4, updated_at = $5 WHERE id = $1",
        )
        .bind(request.id)
        .bind(&request.original_input)
        .bind(&request.error_code)
        .bind(&request.error_message)
        .bind(request.updated_at)
        .execute(&mut *transaction)
        .await?;

        insert_probe_job(&mut transaction, &request).await?;

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
    ) -> Result<IngestRequest, InboxRepositoryError> {
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
        status: IngestStatus,
        error_code: &str,
        error_message: &str,
    ) -> Result<IngestRequest, InboxRepositoryError> {
        self.fail_ingest_step(
            id,
            status,
            error_code,
            error_message,
            IngestFailureGuard {
                ignore_completed_download: false,
                attempt: None,
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
    ) -> Result<IngestRequest, InboxRepositoryError> {
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
    ) -> Result<IngestRequest, InboxRepositoryError> {
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
    Ready(IngestRequest),
    AlreadyAdvanced(IngestRequest),
}

#[derive(Debug, Clone)]
pub enum SourceDownloadStart {
    Ready(IngestRequest),
    AlreadyAdvanced(IngestRequest),
}

#[derive(Debug, Clone)]
pub enum AssetProbeStart {
    Ready(IngestRequest),
    AlreadyAdvanced(IngestRequest),
}

#[derive(Debug, Clone)]
pub enum AssetNormalizationStart {
    Ready(IngestRequest),
    AlreadyAdvanced(IngestRequest),
}

#[derive(Debug, Clone)]
pub enum IngestFinalizationStart {
    Ready(IngestRequest),
    AlreadyAdvanced(IngestRequest),
}

#[derive(Debug, Clone)]
pub enum IngestFingerprintStart {
    Ready(IngestRequest),
    AlreadyAdvanced(IngestRequest),
}

struct IngestFailureGuard<'a> {
    ignore_completed_download: bool,
    attempt: Option<&'a JobAttempt>,
    expected_status: Option<IngestStatus>,
}

#[derive(Debug, Clone)]
pub struct CreateIngestResult {
    pub request: IngestRequest,
    pub created: bool,
}

async fn insert_request(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    request: &IngestRequest,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO ingest_requests (
            id, kind, status, submitted_via, submitted_by_admin_id, original_input,
            source_url, page_url, page_title, supplied_caption, supplied_tags,
            idempotency_key, error_code, error_message, created_at, updated_at, completed_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
        "#,
    )
    .bind(request.id)
    .bind(request.kind.as_str())
    .bind(request.status.as_str())
    .bind(request.submitted_via.as_str())
    .bind(request.submitted_by_admin_id)
    .bind(&request.original_input)
    .bind(&request.source_url)
    .bind(&request.page_url)
    .bind(&request.page_title)
    .bind(&request.supplied_caption)
    .bind(&request.supplied_tags)
    .bind(&request.idempotency_key)
    .bind(&request.error_code)
    .bind(&request.error_message)
    .bind(request.created_at)
    .bind(request.updated_at)
    .bind(request.completed_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn update_ingest_state(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    request: &IngestRequest,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE ingest_requests
        SET status = $2,
            error_code = $3,
            error_message = $4,
            updated_at = $5,
            completed_at = $6
        WHERE id = $1
        "#,
    )
    .bind(request.id)
    .bind(request.status.as_str())
    .bind(&request.error_code)
    .bind(&request.error_message)
    .bind(request.updated_at)
    .bind(request.completed_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_inspect_job(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    request: &IngestRequest,
) -> Result<(), sqlx::Error> {
    let job = NewJob::inspect_source(request.id);
    sqlx::query(
        r#"
        INSERT INTO jobs (job_type, payload_json, idempotency_key)
        VALUES ($1, $2, $3)
        "#,
    )
    .bind(job.job_type().as_str())
    .bind(job.payload_json())
    .bind(format!("ingest:{}:inspect_source:v1", request.id))
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_probe_job(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    request: &IngestRequest,
) -> Result<(), sqlx::Error> {
    let job = NewJob::probe_asset(request.id);
    sqlx::query(
        r#"
        INSERT INTO jobs (job_type, payload_json, idempotency_key)
        VALUES ($1, $2, $3)
        ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
        "#,
    )
    .bind(job.job_type().as_str())
    .bind(job.payload_json())
    .bind(format!("ingest:{}:probe_asset:v1", request.id))
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_normalize_job(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    request: &IngestRequest,
) -> Result<(), sqlx::Error> {
    let job = NewJob::normalize_asset(request.id);
    sqlx::query(
        r#"
        INSERT INTO jobs (job_type, payload_json, idempotency_key)
        VALUES ($1, $2, $3)
        ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
        "#,
    )
    .bind(job.job_type().as_str())
    .bind(job.payload_json())
    .bind(format!("ingest:{}:normalize_asset:v1", request.id))
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_finalize_job(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    request: &IngestRequest,
) -> Result<(), sqlx::Error> {
    let job = NewJob::finalize_ingest(request.id);
    sqlx::query(
        r#"
        INSERT INTO jobs (job_type, payload_json, idempotency_key)
        VALUES ($1, $2, $3)
        ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
        "#,
    )
    .bind(job.job_type().as_str())
    .bind(job.payload_json())
    .bind(format!("ingest:{}:finalize_ingest:v1", request.id))
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_fingerprint_job(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    request: &IngestRequest,
) -> Result<(), sqlx::Error> {
    let job = NewJob::compute_fingerprint(request.id);
    sqlx::query(
        r#"
        INSERT INTO jobs (job_type, payload_json, idempotency_key)
        VALUES ($1, $2, $3)
        ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
        "#,
    )
    .bind(job.job_type().as_str())
    .bind(job.payload_json())
    .bind(format!("ingest:{}:compute_fingerprint:v1", request.id))
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn request_media_kind(request: &IngestRequest) -> Option<SourceMediaKind> {
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

fn request_mime_type(request: &IngestRequest) -> Option<&str> {
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

fn request_file_name(request: &IngestRequest) -> Option<&str> {
    if request.kind == IngestKind::Url {
        None
    } else {
        request.original_input.get("file_name")?.as_str()
    }
}

fn request_image_format_is_supported(request: &IngestRequest, probe: &serde_json::Value) -> bool {
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
) -> Result<IngestRequest, InboxRepositoryError> {
    let row = sqlx::query_as::<_, IngestRequestRow>(
        r#"
        SELECT id, kind, status, submitted_via, submitted_by_admin_id, original_input,
               source_url, page_url, page_title, supplied_caption, supplied_tags,
               idempotency_key, error_code, error_message, created_at, updated_at, completed_at
        FROM ingest_requests
        WHERE id = $1
        FOR UPDATE
        "#,
    )
    .bind(id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(InboxRepositoryError::ResourceMissing(id))?;

    row.into_request()
}

async fn lock_current_job_attempt(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    attempt: &JobAttempt,
) -> Result<bool, sqlx::Error> {
    let current_job = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT id
        FROM jobs
        WHERE id = $1
          AND status = 'running'
          AND attempt_count = $2
          AND lease_owner = $3
          AND lease_expires_at > now()
        FOR UPDATE
        "#,
    )
    .bind(attempt.job_id)
    .bind(attempt.attempt_number)
    .bind(&attempt.lease_owner)
    .fetch_optional(&mut **transaction)
    .await?;
    Ok(current_job.is_some())
}

#[derive(Debug, FromRow)]
struct IngestRequestRow {
    id: Uuid,
    kind: String,
    status: String,
    submitted_via: String,
    submitted_by_admin_id: Option<Uuid>,
    original_input: serde_json::Value,
    source_url: Option<String>,
    page_url: Option<String>,
    page_title: Option<String>,
    supplied_caption: Option<String>,
    supplied_tags: Vec<String>,
    idempotency_key: Option<String>,
    error_code: Option<String>,
    error_message: Option<String>,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
    completed_at: Option<OffsetDateTime>,
}

impl IngestRequestRow {
    fn into_request(self) -> Result<IngestRequest, InboxRepositoryError> {
        Ok(IngestRequest {
            id: self.id,
            kind: IngestKind::try_from(self.kind.as_str())
                .map_err(InboxRepositoryError::UnknownIngestKind)?,
            status: IngestStatus::try_from(self.status.as_str())
                .map_err(InboxRepositoryError::UnknownIngestStatus)?,
            submitted_via: SubmittedVia::try_from(self.submitted_via.as_str())
                .map_err(InboxRepositoryError::UnknownSubmittedVia)?,
            submitted_by_admin_id: self.submitted_by_admin_id,
            original_input: self.original_input,
            source_url: self.source_url.ok_or(InboxRepositoryError::MissingSourceUrl(self.id))?,
            page_url: self.page_url,
            page_title: self.page_title,
            supplied_caption: self.supplied_caption,
            supplied_tags: self.supplied_tags,
            idempotency_key: self.idempotency_key,
            error_code: self.error_code,
            error_message: self.error_message,
            created_at: self.created_at,
            updated_at: self.updated_at,
            completed_at: self.completed_at,
        })
    }
}

#[derive(Debug, FromRow)]
struct IdempotencyRow {
    request_hash: Vec<u8>,
    resource_id: Option<Uuid>,
}

#[derive(Debug, Error)]
pub enum InboxRepositoryError {
    #[error("idempotency key already belongs to a different request: {key}")]
    IdempotencyConflict { key: String },
    #[error("idempotency record does not reference an ingest request")]
    IncompleteIdempotencyRecord,
    #[error("idempotency record references missing ingest request {0}")]
    ResourceMissing(Uuid),
    #[error("ingest request {0} has no source URL")]
    MissingSourceUrl(Uuid),
    #[error("unknown ingest kind in database: {0}")]
    UnknownIngestKind(String),
    #[error("unknown ingest status in database: {0}")]
    UnknownIngestStatus(String),
    #[error("unknown submission source in database: {0}")]
    UnknownSubmittedVia(String),
    #[error("invalid ingest failure status: {0:?}")]
    InvalidFailureStatus(IngestStatus),
    #[error("invalid ingest state transition: {0}")]
    InvalidStateTransition(#[from] IngestStateError),
    #[error("database operation failed: {0}")]
    Database(#[from] sqlx::Error),
}
