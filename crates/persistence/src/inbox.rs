use crate::library::{LibraryRepositoryError, VideoIdentityPreparation, VideoIdentitySession};
use crate::publisher::select_enabled_channel_candidates;
use crate::{
    WORKSPACE_CLEANUP_RETENTION,
    cleanup::{
        enqueue_workspace_cleanup, enqueue_workspace_cleanup_for_media, lock_workspace_fence,
    },
};
use sooqa_inbox::{
    AssetNormalization, Ingest, IngestCursor, IngestData, IngestDataError, IngestFinalization,
    IngestKind, IngestListItem, IngestPage, IngestProbe, IngestStateError, IngestStatus,
    IngestSubmission, RequestedAction, SourceDownload, SourceInspection, SourceMediaKind,
    SubmittedVia,
};
use sooqa_jobs::{Job, JobCommand, JobLease, NewJob};
use sooqa_library::{
    MAX_VIDEO_DUPLICATE_EVIDENCE_BYTES, MAX_VIDEO_DUPLICATE_MATCHES, MediaIngest,
    VideoDuplicateClassification, VideoDuplicateEvidence, VideoFingerprintInput,
    VideoIdentityDecision, VideoIdentityOutcome,
};
use sqlx::{FromRow, PgPool};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    jobs::{JobRepositoryError, JobSettlement},
    settlement::{lock_expired_job, lock_running_job, queue_parameters, update_locked_job},
};

#[derive(Clone)]
pub struct InboxRepository {
    pool: PgPool,
}

/// A candidate exposed to the private-admin decision surface. The
/// classification and score come directly from the bounded evidence persisted
/// on the ingest; this query never recomputes identity.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DuplicateCandidate {
    pub media_id: Uuid,
    pub classification: VideoDuplicateClassification,
    pub score_bps: u16,
    pub storage_state: String,
    pub storage_chat_id: Option<i64>,
    pub storage_message_id: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct DuplicatePendingIngest {
    pub ingest: Ingest,
    pub candidates: Vec<DuplicateCandidate>,
}

#[derive(Debug, Clone)]
pub struct AcceptDuplicateResult {
    pub ingest: Ingest,
    pub replayed: bool,
}

/// The short preparation transaction has completed and the global identity
/// session remains held while the worker performs CPU alignment.
pub enum IngestVideoIdentityStart {
    AlreadyAdvanced(Ingest),
    Ready { ingest: Ingest, preparation: VideoIdentityPreparation, session: VideoIdentitySession },
}

impl InboxRepository {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn job_repository(&self) -> crate::JobRepository {
        crate::JobRepository::new(self.pool.clone())
    }

    pub async fn create_ingest(
        &self,
        submission: IngestSubmission,
    ) -> Result<CreateIngestResult, InboxRepositoryError> {
        self.create_ingest_at(submission, OffsetDateTime::now_utc()).await
    }

    /// Create an ingest using an explicit clock value. The production entry
    /// point uses the current UTC time; the seam keeps replay tests
    /// deterministic without waiting for a scheduled request to expire.
    pub async fn create_ingest_at(
        &self,
        submission: IngestSubmission,
        now: OffsetDateTime,
    ) -> Result<CreateIngestResult, InboxRepositoryError> {
        let request_hash = submission.request_hash();
        let mut transaction = self.pool.begin().await?;

        let input_key = submission.idempotency_key.clone().unwrap_or_else(|| {
            format!("{}:{}", submission.kind.as_str(), submission.normalized_url)
        });
        if let Some(existing) = sqlx::query_as::<_, IngestIdentityRow>(
            "SELECT id, request_hash FROM ingests WHERE input_key = $1 FOR UPDATE",
        )
        .bind(&input_key)
        .fetch_optional(&mut *transaction)
        .await?
        {
            if existing.request_hash.as_slice() != request_hash.as_slice() {
                return Err(InboxRepositoryError::IdempotencyConflict { key: input_key });
            }
            let request = load_request(&mut transaction, existing.id).await?;
            transaction.commit().await?;
            return Ok(CreateIngestResult { ingest: request, created: false });
        }

        let request_id = Uuid::now_v7();
        let mut request = Ingest::from_submission(request_id, &submission)
            .map_err(InboxRepositoryError::InputEnvelope)?;
        request
            .transition_to(IngestStatus::Queued)
            .expect("received ingest requests must be queueable");
        request.workspace_id = request
            .input_data()
            .map_err(InboxRepositoryError::InputEnvelope)?
            .source
            .telegram_workspace_id
            .as_deref()
            .and_then(|value| Uuid::parse_str(value).ok())
            .unwrap_or(request.id);

        if request.requested_action == RequestedAction::Queue
            && request
                .requested_publish_at
                .is_some_and(|requested_publish_at| requested_publish_at <= now)
        {
            return Err(InboxRepositoryError::RequestedPublishAtNotFuture);
        }

        if request.requested_action != RequestedAction::Save {
            let candidates = select_enabled_channel_candidates(&mut transaction).await?;
            request.requested_channel_id = match candidates.as_slice() {
                [] => return Err(InboxRepositoryError::RequestedChannelNotConfigured),
                [channel_id] => Some(*channel_id),
                _ => return Err(InboxRepositoryError::RequestedChannelAmbiguous),
            };
        }

        let inserted_id = sqlx::query_scalar::<_, Uuid>(
            r#"
            INSERT INTO ingests (
                id, input_key, request_hash, input_kind, state, submitted_via,
                input_json, source_url, page_url, page_title, supplied_caption,
                supplied_description, supplied_tags, requested_action, requested_publish_at,
                requested_post_caption, requested_channel_id, workspace_id, media_id,
                error_code, error_message, created_at, updated_at, completed_at
            )
            VALUES ($1, $2, $3, $4, 'queued', $5, $6, $7, $8, $9, $10, $11,
                    $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)
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
        .bind(request.requested_action.as_str())
        .bind(request.requested_publish_at)
        .bind(&request.requested_post_caption)
        .bind(request.requested_channel_id)
        .bind(request.workspace_id)
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

    /// Return a bounded newest-first operational page. The cursor is based on
    /// the immutable creation timestamp plus UUID, so inserts after the first
    /// page cannot shift rows into or out of a later page.
    pub async fn list_admin(
        &self,
        limit: u32,
        cursor: Option<IngestCursor>,
    ) -> Result<IngestPage, InboxRepositoryError> {
        if !(1..=50).contains(&limit) {
            return Err(InboxRepositoryError::InvalidLimit { value: limit, max: 50 });
        }
        let rows = sqlx::query_as::<_, IngestListRow>(
            r#"
            SELECT i.id, i.source_url, i.page_url, i.requested_action, i.state,
                   i.created_at, i.updated_at, i.completed_at, i.media_id,
                   i.error_code, i.error_message,
                   m.telegram_storage_chat_id, m.telegram_storage_message_id
            FROM ingests AS i
            LEFT JOIN media AS m ON m.id = i.media_id
            WHERE ($1::timestamptz IS NULL OR (i.created_at, i.id) < ($1, $2))
            ORDER BY i.created_at DESC, i.id DESC
            LIMIT $3
            "#,
        )
        .bind(cursor.as_ref().map(|value| value.created_at))
        .bind(cursor.as_ref().map(|value| value.id))
        .bind(i64::from(limit) + 1)
        .fetch_all(&self.pool)
        .await?;
        let has_more = rows.len() > limit as usize;
        let rows = rows.into_iter().take(limit as usize).collect::<Vec<_>>();
        let next_cursor = has_more
            .then(|| rows.last())
            .flatten()
            .map(|row| IngestCursor { created_at: row.created_at, id: row.id });
        let items =
            rows.into_iter().map(IngestListRow::into_item).collect::<Result<Vec<_>, _>>()?;
        Ok(IngestPage { items, next_cursor })
    }

    pub async fn count_active(&self) -> Result<u64, InboxRepositoryError> {
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM ingests WHERE state NOT IN ('completed', 'failed_terminal', 'cancelled')",
        )
        .fetch_one(&self.pool)
        .await?;
        u64::try_from(count).map_err(|_| InboxRepositoryError::InvalidCount)
    }

    pub async fn count_duplicate_pending(&self) -> Result<u64, InboxRepositoryError> {
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM ingests WHERE state = 'duplicate_pending'",
        )
        .fetch_one(&self.pool)
        .await?;
        u64::try_from(count).map_err(|_| InboxRepositoryError::InvalidCount)
    }

    /// Accept one already-evidenced media candidate without creating a media
    /// row or starting another storage upload. The ingest row is the decision
    /// fence: force-save and accept-duplicate serialize on its row lock, so
    /// exactly one decision wins a race.
    pub async fn accept_duplicate(
        &self,
        id: Uuid,
        media_id: Uuid,
    ) -> Result<AcceptDuplicateResult, InboxRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let mut request = load_request(&mut transaction, id).await?;

        // Inspect the durable decision before looking at the current pipeline
        // state. Storage may fail after a successful accept, and the original
        // command must remain replay-safe while that downstream state changes.
        let input_data = request.input_data().map_err(InboxRepositoryError::InputEnvelope)?;
        if input_data.duplicate_decision.is_some() {
            let accepted_media_id = input_data
                .duplicate_decision
                .as_ref()
                .filter(|decision| {
                    decision.version == 1
                        && decision.kind == sooqa_inbox::DuplicateDecisionKind::Accepted
                })
                .map(|decision| decision.media_id);
            if !request.force_save
                && accepted_media_id.is_some()
                && request.media_id == accepted_media_id
                && accepted_media_id == Some(media_id)
            {
                transaction.commit().await?;
                return Ok(AcceptDuplicateResult { ingest: request, replayed: true });
            }
            return Err(InboxRepositoryError::DuplicateDecisionNotAllowed(request.status));
        }

        if !request.force_save
            && matches!(request.status, IngestStatus::Storing | IngestStatus::Completed)
        {
            return Err(InboxRepositoryError::DuplicateDecisionNotAllowed(request.status));
        }

        if request.status != IngestStatus::DuplicatePending {
            return Err(InboxRepositoryError::DuplicateDecisionNotAllowed(request.status));
        }

        let evidence = request
            .duplicate_evidence
            .clone()
            .ok_or(InboxRepositoryError::DuplicateEvidenceMissing(id))
            .and_then(|value| {
                serde_json::from_value::<VideoDuplicateEvidence>(value).map_err(Into::into)
            })?;
        if !evidence.matches.iter().any(|candidate| candidate.media_id == media_id) {
            return Err(InboxRepositoryError::DuplicateCandidateNotEvidenced(media_id));
        }

        let candidate = sqlx::query_as::<_, DuplicateMediaRow>(
            r#"
            SELECT storage_state, tags
            FROM media
            WHERE id = $1
            FOR UPDATE
            "#,
        )
        .bind(media_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(InboxRepositoryError::DuplicateCandidateMissing(media_id))?;

        if !matches!(candidate.storage_state.as_str(), "ready" | "pending_storage") {
            return Err(InboxRepositoryError::DuplicateCandidateUnavailable {
                media_id,
                state: candidate.storage_state,
            });
        }

        let merged_tags = merge_duplicate_tags(&candidate.tags, &request.supplied_tags);
        let supplied_description = request
            .supplied_description
            .as_deref()
            .map(str::trim)
            .filter(|description| !description.is_empty());
        sqlx::query(
            r#"
            UPDATE media
            SET tags = $2,
                description = CASE WHEN $3::text IS NOT NULL THEN $3 ELSE description END,
                updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(media_id)
        .bind(merged_tags)
        .bind(supplied_description)
        .execute(&mut *transaction)
        .await?;

        request.media_id = Some(media_id);
        let mut input_data = request.input_data().map_err(InboxRepositoryError::InputEnvelope)?;
        input_data.duplicate_decision = Some(sooqa_inbox::DuplicateDecisionData {
            version: 1,
            kind: sooqa_inbox::DuplicateDecisionKind::Accepted,
            media_id,
        });
        request.set_input_data(input_data).map_err(InboxRepositoryError::InputEnvelope)?;
        // Persist the decision marker together with the state transition before
        // clearing the evidence. It is the durable idempotency fence for a
        // successful duplicate-accept command.
        request.duplicate_evidence = None;
        request.error_code = None;
        request.error_message = None;
        request.completed_at = None;
        request.transition_to(IngestStatus::Storing)?;
        if candidate.storage_state == "ready" {
            request.transition_to(IngestStatus::Completed)?;
            request.completed_at = Some(OffsetDateTime::now_utc());
            if request.requested_action != RequestedAction::Save {
                insert_materialization_job(&mut transaction, request.id).await?;
            }
        }
        request.updated_at = OffsetDateTime::now_utc();
        update_ingest_state(&mut transaction, &request).await?;
        transaction.commit().await?;
        Ok(AcceptDuplicateResult { ingest: request, replayed: false })
    }

    /// Return a bounded private-admin review page. Candidate state is read
    /// from `media`; evidence ordering, classification, and score remain the
    /// persisted identity decision and are not recalculated here.
    pub async fn list_duplicate_pending(
        &self,
        limit: u32,
    ) -> Result<Vec<DuplicatePendingIngest>, InboxRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let ids = sqlx::query_scalar::<_, Uuid>(
            "SELECT id FROM ingests WHERE state = 'duplicate_pending' ORDER BY created_at, id LIMIT $1",
        )
        .bind(i64::from(limit.min(20)))
        .fetch_all(&mut *transaction)
        .await?;
        let mut pending = Vec::with_capacity(ids.len());

        for id in ids {
            let request = load_request(&mut transaction, id).await?;
            let evidence = request
                .duplicate_evidence
                .clone()
                .ok_or(InboxRepositoryError::DuplicateEvidenceMissing(id))
                .and_then(|value| {
                    serde_json::from_value::<VideoDuplicateEvidence>(value).map_err(Into::into)
                })?;
            let mut candidates = Vec::with_capacity(evidence.matches.len());
            for candidate in evidence.matches.into_iter().take(MAX_VIDEO_DUPLICATE_MATCHES) {
                let storage = sqlx::query_as::<_, DuplicateMediaStorageRow>(
                    r#"
                    SELECT storage_state, telegram_storage_chat_id, telegram_storage_message_id
                    FROM media
                    WHERE id = $1
                    "#,
                )
                .bind(candidate.media_id)
                .fetch_optional(&mut *transaction)
                .await?;
                let (storage_state, storage_chat_id, storage_message_id) =
                    storage.map_or(("missing".to_owned(), None, None), |row| {
                        (
                            row.storage_state,
                            row.telegram_storage_chat_id,
                            row.telegram_storage_message_id,
                        )
                    });
                candidates.push(DuplicateCandidate {
                    media_id: candidate.media_id,
                    classification: candidate.classification,
                    score_bps: candidate.score_bps,
                    storage_state,
                    storage_chat_id,
                    storage_message_id,
                });
            }
            pending.push(DuplicatePendingIngest { ingest: request, candidates });
        }

        transaction.commit().await?;
        Ok(pending)
    }

    pub async fn begin_source_inspection(
        &self,
        id: Uuid,
        attempt: &JobLease,
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
        attempt: &JobLease,
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
        attempt: &JobLease,
    ) -> Result<SourceDownloadStart, InboxRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let mut request = load_request(&mut transaction, id).await?;
        let start = if request
            .input_data()
            .map_err(InboxRepositoryError::InputEnvelope)?
            .download
            .is_some()
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
        attempt: &JobLease,
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
        let mut input_data = request.input_data().map_err(InboxRepositoryError::InputEnvelope)?;
        let probe = IngestProbe::from_value(probe);
        let declared_media_kind = request_media_kind(&request)?;
        let detected_media_kind = probe.media_kind();
        let media_kind = detected_media_kind.or(declared_media_kind);
        let probed_format = probed_image_format(&probe);
        let unsupported_image_format = media_kind == Some(SourceMediaKind::Image)
            && !request_image_format_is_supported(&request, &probe)?;
        input_data.probe = Some(probe);
        input_data.probed_media_kind = detected_media_kind;
        request.set_input_data(input_data).map_err(InboxRepositoryError::InputEnvelope)?;
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
            enqueue_workspace_cleanup(
                &mut transaction,
                request.id,
                request.workspace_id,
                OffsetDateTime::now_utc() + WORKSPACE_CLEANUP_RETENTION,
            )
            .await?;
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
        attempt: &JobLease,
    ) -> Result<AssetNormalizationStart, InboxRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let mut request = load_request(&mut transaction, id).await?;
        let has_normalization = request
            .input_data()
            .map_err(InboxRepositoryError::InputEnvelope)?
            .normalization
            .is_some();
        let start = if (has_normalization && !request.force_save)
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
        attempt: &JobLease,
        normalization: AssetNormalization,
    ) -> Result<Ingest, InboxRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let mut request = load_request(&mut transaction, id).await?;
        let has_normalization = request
            .input_data()
            .map_err(InboxRepositoryError::InputEnvelope)?
            .normalization
            .is_some();
        if request.status != IngestStatus::Normalizing
            || (has_normalization && !request.force_save)
            || !lock_current_job_attempt(&mut transaction, attempt).await?
        {
            transaction.commit().await?;
            return Ok(request);
        }

        let mut input_data = request.input_data().map_err(InboxRepositoryError::InputEnvelope)?;
        input_data.normalization = Some(normalization);
        request.set_input_data(input_data).map_err(InboxRepositoryError::InputEnvelope)?;

        let is_video = normalized_media_kind(&request)? == Some(SourceMediaKind::Video);
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
        attempt: &JobLease,
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
        attempt: &JobLease,
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
        attempt: &JobLease,
        finalization: IngestFinalization,
    ) -> Result<Ingest, InboxRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let mut request = load_request(&mut transaction, id).await?;
        let has_finalization = request
            .input_data()
            .map_err(InboxRepositoryError::InputEnvelope)?
            .finalization
            .is_some();
        if request.status != IngestStatus::Storing
            || has_finalization
            || !lock_current_job_attempt(&mut transaction, attempt).await?
        {
            transaction.commit().await?;
            return Ok(request);
        }

        if normalized_media_kind(&request)? == Some(SourceMediaKind::Video) {
            return Err(InboxRepositoryError::VideoFinalizationNotAllowed);
        }

        let media_id = finalization.media_id;
        request.media_id = Some(media_id);
        let mut input_data = request.input_data().map_err(InboxRepositoryError::InputEnvelope)?;
        input_data.finalization = Some(finalization);
        request.set_input_data(input_data).map_err(InboxRepositoryError::InputEnvelope)?;
        request.error_code = None;
        request.error_message = None;
        request.updated_at = OffsetDateTime::now_utc();
        advance_after_media_processing(&mut transaction, &mut request, media_id).await?;
        succeed_current_job_attempt(&mut transaction, attempt).await?;
        transaction.commit().await?;
        Ok(request)
    }

    pub async fn begin_ingest_fingerprinting(
        &self,
        id: Uuid,
        attempt: &JobLease,
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

    pub async fn begin_video_identity(
        &self,
        id: Uuid,
        attempt: &JobLease,
        ingest: &MediaIngest,
        fingerprint: Option<&VideoFingerprintInput>,
    ) -> Result<IngestVideoIdentityStart, InboxRepositoryError> {
        let mut session = VideoIdentitySession::acquire(&self.pool).await?;
        let result = async {
            let mut transaction = session.begin().await?;
            let request = load_request(&mut transaction, id).await?;
            if request.status != IngestStatus::Fingerprinting
                || !lock_current_job_attempt(&mut transaction, attempt).await?
            {
                transaction.commit().await?;
                return Ok(Err(request));
            }
            let preparation =
                crate::library::LibraryRepository::prepare_video_identity_in_transaction(
                    &mut transaction,
                    ingest,
                    fingerprint,
                    request.force_save,
                )
                .await?;
            transaction.commit().await?;
            Ok(Ok((request, preparation)))
        }
        .await;
        match result {
            Ok(Ok((ingest, preparation))) => {
                Ok(IngestVideoIdentityStart::Ready { ingest, preparation, session })
            }
            Ok(Err(request)) => {
                session.release().await?;
                Ok(IngestVideoIdentityStart::AlreadyAdvanced(request))
            }
            Err(error) => {
                let _ = session.release().await;
                Err(error)
            }
        }
    }

    pub async fn abort_video_identity(
        &self,
        session: VideoIdentitySession,
    ) -> Result<(), InboxRepositoryError> {
        session.release().await?;
        Ok(())
    }

    pub async fn complete_video_identity(
        &self,
        id: Uuid,
        attempt: &JobLease,
        session: VideoIdentitySession,
        ingest: MediaIngest,
        fingerprint: Option<&VideoFingerprintInput>,
        decision: &VideoIdentityDecision,
    ) -> Result<Ingest, InboxRepositoryError> {
        let mut session = session;
        let result = self
            .complete_video_identity_while_locked(
                id,
                attempt,
                &mut session,
                &ingest,
                fingerprint,
                decision,
            )
            .await;
        let release = session.release().await;
        match result {
            Err(error) => Err(error),
            Ok(request) => {
                release?;
                Ok(request)
            }
        }
    }

    async fn complete_video_identity_while_locked(
        &self,
        id: Uuid,
        attempt: &JobLease,
        session: &mut VideoIdentitySession,
        ingest: &MediaIngest,
        fingerprint: Option<&VideoFingerprintInput>,
        decision: &VideoIdentityDecision,
    ) -> Result<Ingest, InboxRepositoryError> {
        let mut transaction = session.begin().await?;
        let mut request = load_request(&mut transaction, id).await?;
        if request.status != IngestStatus::Fingerprinting
            || !lock_current_job_attempt(&mut transaction, attempt).await?
        {
            transaction.commit().await?;
            return Ok(request);
        }

        let outcome = crate::library::LibraryRepository::persist_video_identity_in_transaction(
            &mut transaction,
            ingest,
            fingerprint,
            decision,
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
                enqueue_workspace_cleanup(
                    &mut transaction,
                    request.id,
                    request.workspace_id,
                    OffsetDateTime::now_utc() + WORKSPACE_CLEANUP_RETENTION,
                )
                .await?;
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
                request.workspace_id = Uuid::now_v7();
                request.duplicate_evidence = None;
                clear_pipeline_artifacts(&mut request)?;
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
        let mut transaction = self.pool.begin().await?;
        let updated = sqlx::query_as::<_, (Uuid, String)>(
            "UPDATE ingests SET state = 'completed', completed_at = $2, error_code = NULL, error_message = NULL, updated_at = $2 WHERE media_id = $1 AND EXISTS (SELECT 1 FROM media WHERE id = $1 AND storage_state = 'ready') AND (state = 'storing' OR (state = 'failed_retryable' AND error_code IN ('storage_upload', 'storage_unknown')) OR (state = 'failed_terminal' AND error_code IN ('storage_upload', 'storage_unknown'))) RETURNING id, requested_action",
        )
        .bind(media_id)
        .bind(now)
        .fetch_all(&mut *transaction)
        .await?;
        for (ingest_id, requested_action) in &updated {
            if requested_action != RequestedAction::Save.as_str() {
                insert_materialization_job(&mut transaction, *ingest_id).await?;
            }
        }
        sqlx::query(
            "UPDATE media SET local_work_path = NULL WHERE id = $1 AND storage_state = 'ready'",
        )
        .bind(media_id)
        .execute(&mut *transaction)
        .await?;
        enqueue_workspace_cleanup_for_media(&mut transaction, media_id, now).await?;
        transaction.commit().await?;
        Ok(updated.len() as u64)
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
        let mut transaction = self.pool.begin().await?;
        let updated = sqlx::query(
            "UPDATE ingests SET state = $2, error_code = $3, error_message = $4, completed_at = $5, updated_at = $6 WHERE media_id = $1 AND (state = 'storing' OR (state = 'failed_retryable' AND error_code = 'storage_upload'))",
        )
        .bind(media_id)
        .bind(status.as_str())
        .bind(error_code)
        .bind(error_message)
        .bind(completed_at)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        if status == IngestStatus::FailedTerminal {
            let media_changed = sqlx::query(
                "UPDATE media SET storage_state = 'missing', storage_token = NULL, storage_started_at = NULL, updated_at = $2 WHERE id = $1 AND storage_state = 'pending_storage'",
            )
            .bind(media_id)
            .bind(now)
            .execute(&mut *transaction)
            .await?;
            if media_changed.rows_affected() > 0 {
                enqueue_workspace_cleanup_for_media(
                    &mut transaction,
                    media_id,
                    now + WORKSPACE_CLEANUP_RETENTION,
                )
                .await?;
            }
        }
        transaction.commit().await?;
        Ok(updated.rows_affected())
    }

    pub async fn begin_workspace_cleanup(
        &self,
        attempt: &JobLease,
        ingest_id: Uuid,
        workspace_id: Uuid,
    ) -> Result<WorkspaceCleanupStart, InboxRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        if !lock_current_job_attempt(&mut transaction, attempt).await? {
            transaction.commit().await?;
            return Ok(WorkspaceCleanupStart::AlreadyAdvanced);
        }
        let fence_id =
            sqlx::query_scalar::<_, Option<Uuid>>("SELECT media_id FROM ingests WHERE id = $1")
                .bind(ingest_id)
                .fetch_optional(&mut *transaction)
                .await?
                .flatten()
                .unwrap_or(ingest_id);
        lock_workspace_fence(&mut transaction, fence_id).await?;
        let row = sqlx::query_as::<_, (Uuid, String, Option<Uuid>)>(
            "SELECT workspace_id, state, media_id FROM ingests WHERE id = $1 FOR UPDATE",
        )
        .bind(ingest_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(InboxRepositoryError::ResourceMissing(ingest_id))?;

        // A force-save changes the workspace ID before it queues the new
        // generation. The old cleanup job is then safe to finish against its
        // orphaned directory without inspecting the replacement generation.
        if row.0 != workspace_id {
            transaction.commit().await?;
            return Ok(WorkspaceCleanupStart::Ready);
        }

        let has_active_job = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                SELECT 1
                FROM queue.jobs
                WHERE id <> $1
                  AND state IN ('queued', 'running')
                  AND kind <> 'cleanup_workspace'
                  AND (
                      payload->>'ingest_id' = $2
                      OR ($3 IS NOT NULL AND payload->>'media_id' = $3)
                  )
            )",
        )
        .bind(attempt.job_id)
        .bind(ingest_id.to_string())
        .bind(row.2.map(|media_id| media_id.to_string()))
        .fetch_one(&mut *transaction)
        .await?;
        if has_active_job {
            transaction.commit().await?;
            return Ok(WorkspaceCleanupStart::Deferred);
        }

        let storage_state = match row.2 {
            Some(media_id) => {
                sqlx::query_scalar::<_, String>(
                    "SELECT storage_state FROM media WHERE id = $1 FOR UPDATE",
                )
                .bind(media_id)
                .fetch_optional(&mut *transaction)
                .await?
            }
            None => None,
        };
        let storage_ready =
            storage_state.as_deref().is_none_or(|state| matches!(state, "ready" | "missing"));
        let state_allows_cleanup = matches!(
            row.1.as_str(),
            "completed" | "duplicate_pending" | "failed_terminal" | "cancelled" | "storing"
        );
        let result = if state_allows_cleanup && storage_ready {
            if let Some(media_id) = row.2 {
                // The cleanup claim is the durable hand-off to the filesystem.
                // Clear the path before committing so a later reset cannot
                // recreate an upload job after the worker has deleted bytes,
                // even if this cleanup lease succeeds or is recovered.
                sqlx::query(
                    "UPDATE media SET local_work_path = NULL, updated_at = now() WHERE id = $1 AND storage_state IN ('ready', 'missing') AND local_work_path IS NOT NULL",
                )
                .bind(media_id)
                .execute(&mut *transaction)
                .await?;
            }
            WorkspaceCleanupStart::Ready
        } else {
            WorkspaceCleanupStart::Deferred
        };
        transaction.commit().await?;
        Ok(result)
    }

    pub async fn fail_ingest_fingerprint(
        &self,
        id: Uuid,
        attempt: &JobLease,
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
        attempt: &JobLease,
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
        attempt: &JobLease,
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

        let mut input_data = request.input_data().map_err(InboxRepositoryError::InputEnvelope)?;
        input_data.inspection = Some(inspection.clone());
        request.set_input_data(input_data).map_err(InboxRepositoryError::InputEnvelope)?;
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
        attempt: &JobLease,
        download: SourceDownload,
    ) -> Result<Ingest, InboxRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let mut request = load_request(&mut transaction, id).await?;
        let has_download =
            request.input_data().map_err(InboxRepositoryError::InputEnvelope)?.download.is_some();
        if request.status != IngestStatus::Downloading
            || has_download
            || !lock_current_job_attempt(&mut transaction, attempt).await?
        {
            transaction.commit().await?;
            return Ok(request);
        }

        let mut input_data = request.input_data().map_err(InboxRepositoryError::InputEnvelope)?;
        input_data.download = Some(download);
        request.set_input_data(input_data).map_err(InboxRepositoryError::InputEnvelope)?;
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
        attempt: &JobLease,
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
        attempt: &JobLease,
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
        attempt: &JobLease,
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
        if guard.ignore_completed_download
            && request.input_data().map_err(InboxRepositoryError::InputEnvelope)?.download.is_some()
        {
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
        if status == IngestStatus::FailedTerminal {
            enqueue_workspace_cleanup(
                &mut transaction,
                request.id,
                request.workspace_id,
                OffsetDateTime::now_utc() + WORKSPACE_CLEANUP_RETENTION,
            )
            .await?;
        }
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

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum WorkspaceCleanupStart {
    Ready,
    Deferred,
    AlreadyAdvanced,
}

#[derive(Debug, Clone)]
pub struct ForceSaveResult {
    pub ingest: Ingest,
    pub resumed: bool,
}

struct IngestFailureGuard<'a> {
    ignore_completed_download: bool,
    attempt: Option<&'a JobLease>,
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
            workspace_id = $4,
            media_id = $5,
            force_save = $6,
            duplicate_evidence = $7,
            error_code = $8,
            error_message = $9,
            updated_at = $10,
            completed_at = $11
        WHERE id = $1
        "#,
    )
    .bind(request.id)
    .bind(request.status.as_str())
    .bind(&request.original_input)
    .bind(request.workspace_id)
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
            if request.requested_action != RequestedAction::Save {
                insert_materialization_job(transaction, request.id).await?;
            }
            enqueue_workspace_cleanup(
                transaction,
                request.id,
                request.workspace_id,
                OffsetDateTime::now_utc(),
            )
            .await?;
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
            enqueue_workspace_cleanup(
                transaction,
                request.id,
                request.workspace_id,
                OffsetDateTime::now_utc() + WORKSPACE_CLEANUP_RETENTION,
            )
            .await?;
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

async fn insert_materialization_job(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ingest_id: Uuid,
) -> Result<(), sqlx::Error> {
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
    Ok(())
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

fn clear_pipeline_artifacts(request: &mut Ingest) -> Result<(), InboxRepositoryError> {
    let mut data = request.input_data().map_err(InboxRepositoryError::InputEnvelope)?;
    data.inspection = None;
    data.download = None;
    data.probe = None;
    data.probed_media_kind = None;
    data.normalization = None;
    data.finalization = None;
    request.set_input_data(data).map_err(InboxRepositoryError::InputEnvelope)
}

fn request_media_kind(request: &Ingest) -> Result<Option<SourceMediaKind>, InboxRepositoryError> {
    Ok(request.input_data().map_err(InboxRepositoryError::InputEnvelope)?.media_kind())
}

fn normalized_media_kind(
    request: &Ingest,
) -> Result<Option<SourceMediaKind>, InboxRepositoryError> {
    Ok(request
        .input_data()
        .map_err(InboxRepositoryError::InputEnvelope)?
        .normalization
        .map(|normalization| normalization.media_kind))
}

fn request_mime_type(request: &Ingest) -> Result<Option<String>, InboxRepositoryError> {
    Ok(request
        .input_data()
        .map_err(InboxRepositoryError::InputEnvelope)?
        .mime_type()
        .map(str::to_owned))
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum ImageFormatKind {
    Jpeg,
    Png,
    Unsupported,
    Unknown,
}

fn request_file_name(request: &Ingest) -> Result<Option<String>, InboxRepositoryError> {
    Ok(request.input_data().map_err(InboxRepositoryError::InputEnvelope)?.source.file_name)
}

fn request_image_format_is_supported(
    request: &Ingest,
    probe: &IngestProbe,
) -> Result<bool, InboxRepositoryError> {
    let mime_type = request_mime_type(request)?;
    let file_name = request_file_name(request)?;
    let declared = mime_type
        .as_deref()
        .map(image_format_from_mime)
        .filter(|format| *format != ImageFormatKind::Unknown)
        .or_else(|| file_name.as_deref().map(image_format_from_file_name))
        .unwrap_or(ImageFormatKind::Unknown);
    let probed = probed_image_format(probe);
    Ok(match probed {
        ImageFormatKind::Jpeg | ImageFormatKind::Png => true,
        ImageFormatKind::Unsupported => false,
        ImageFormatKind::Unknown => {
            matches!(declared, ImageFormatKind::Jpeg | ImageFormatKind::Png)
        }
    })
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

fn probed_image_format(probe: &IngestProbe) -> ImageFormatKind {
    if let Some(value) = probe.image_format() {
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

fn merge_duplicate_tags(existing: &[String], incoming: &[String]) -> Vec<String> {
    let mut tags = existing.to_vec();
    for tag in incoming {
        if !tags.iter().any(|current| current == tag) {
            tags.push(tag.clone());
        }
    }
    tags
}

async fn load_request(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    id: Uuid,
) -> Result<Ingest, InboxRepositoryError> {
    let row = sqlx::query_as::<_, IngestRow>(
        r#"
        SELECT id, input_kind, state, submitted_via, input_json, source_url, page_url,
               page_title, supplied_caption, supplied_description, supplied_tags,
               requested_action, requested_publish_at, requested_post_caption,
               requested_channel_id, input_key,
               workspace_id, media_id, force_save, duplicate_evidence, error_code, error_message,
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
    attempt: &JobLease,
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
    attempt: &JobLease,
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
    requested_action: String,
    requested_publish_at: Option<OffsetDateTime>,
    requested_post_caption: Option<String>,
    requested_channel_id: Option<Uuid>,
    input_key: String,
    workspace_id: Uuid,
    media_id: Option<Uuid>,
    force_save: bool,
    duplicate_evidence: Option<serde_json::Value>,
    error_code: Option<String>,
    error_message: Option<String>,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
    completed_at: Option<OffsetDateTime>,
}

#[derive(Debug, FromRow)]
struct DuplicateMediaRow {
    storage_state: String,
    tags: Vec<String>,
}

#[derive(Debug, FromRow)]
struct DuplicateMediaStorageRow {
    storage_state: String,
    telegram_storage_chat_id: Option<i64>,
    telegram_storage_message_id: Option<i64>,
}

impl IngestRow {
    fn into_ingest(self) -> Result<Ingest, InboxRepositoryError> {
        let input_data =
            IngestData::decode(&self.input_json).map_err(InboxRepositoryError::InputEnvelope)?;
        let canonical_input = input_data.encode().map_err(InboxRepositoryError::InputEnvelope)?;
        Ok(Ingest {
            id: self.id,
            workspace_id: self.workspace_id,
            kind: IngestKind::try_from(self.input_kind.as_str())
                .map_err(InboxRepositoryError::UnknownIngestKind)?,
            status: IngestStatus::try_from(self.state.as_str())
                .map_err(InboxRepositoryError::UnknownIngestStatus)?,
            submitted_via: SubmittedVia::try_from(self.submitted_via.as_str())
                .map_err(InboxRepositoryError::UnknownSubmittedVia)?,
            original_input: canonical_input,
            source_url: self.source_url.ok_or(InboxRepositoryError::MissingSourceUrl(self.id))?,
            page_url: self.page_url,
            page_title: self.page_title,
            supplied_caption: self.supplied_caption,
            supplied_description: self.supplied_description,
            supplied_tags: self.supplied_tags,
            requested_action: RequestedAction::try_from(self.requested_action.as_str())
                .map_err(InboxRepositoryError::UnknownRequestedAction)?,
            requested_publish_at: self.requested_publish_at,
            requested_post_caption: self.requested_post_caption,
            requested_channel_id: self.requested_channel_id,
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
struct IngestListRow {
    id: Uuid,
    source_url: Option<String>,
    page_url: Option<String>,
    requested_action: String,
    state: String,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
    completed_at: Option<OffsetDateTime>,
    media_id: Option<Uuid>,
    error_code: Option<String>,
    error_message: Option<String>,
    telegram_storage_chat_id: Option<i64>,
    telegram_storage_message_id: Option<i64>,
}

impl IngestListRow {
    fn into_item(self) -> Result<IngestListItem, InboxRepositoryError> {
        Ok(IngestListItem {
            id: self.id,
            source_url: self.source_url,
            page_url: self.page_url,
            requested_action: RequestedAction::try_from(self.requested_action.as_str())
                .map_err(InboxRepositoryError::UnknownRequestedAction)?,
            status: IngestStatus::try_from(self.state.as_str())
                .map_err(InboxRepositoryError::UnknownIngestStatus)?,
            created_at: self.created_at,
            updated_at: self.updated_at,
            completed_at: self.completed_at,
            media_id: self.media_id,
            storage_url: self
                .telegram_storage_chat_id
                .zip(self.telegram_storage_message_id)
                .and_then(storage_message_url),
            error_code: self.error_code,
            error_message: self.error_message.map(|value| value.chars().take(512).collect()),
        })
    }
}

fn storage_message_url((chat_id, message_id): (i64, i64)) -> Option<String> {
    if chat_id >= 0 || message_id <= 0 {
        return None;
    }
    let raw_id = chat_id.to_string();
    let internal_id = raw_id.strip_prefix("-100").unwrap_or_else(|| raw_id.trim_start_matches('-'));
    (!internal_id.is_empty()).then(|| format!("https://t.me/c/{internal_id}/{message_id}"))
}

#[derive(Debug, FromRow)]
struct IngestIdentityRow {
    id: Uuid,
    request_hash: Vec<u8>,
}

/// Ingest-family queue settlement and stale recovery. Domain state and the
/// queue lease are locked in one short transaction; the worker's external
/// work remains outside this boundary.
pub(crate) async fn settle_job(
    pool: &PgPool,
    lease: &JobLease,
    expected: &JobCommand,
    settlement: JobSettlement,
) -> Result<Job, JobRepositoryError> {
    let ingest_id = match expected {
        JobCommand::InspectSource(payload) => payload.ingest_id,
        JobCommand::DownloadSource(payload) => payload.ingest_id,
        JobCommand::ProbeAsset(payload)
        | JobCommand::NormalizeAsset(payload)
        | JobCommand::ComputeFingerprint(payload)
        | JobCommand::FinalizeIngest(payload) => payload.ingest_id,
        _ => return Err(JobRepositoryError::LeaseLost),
    };
    let mut transaction = pool.begin().await?;
    // Inbox methods lock the ingest aggregate before the queue lease. This is
    // the same order used by atomic ingest transitions and avoids stale
    // recovery deadlocks.
    lock_ingest_for_job(&mut transaction, ingest_id).await?;
    let job = lock_running_job(&mut transaction, lease, settlement.allows_expired_lease()).await?;
    let current_command = crate::settlement::validate_locked_command(&job, expected)?;
    let current_ingest_id = match current_command {
        JobCommand::InspectSource(payload) => payload.ingest_id,
        JobCommand::DownloadSource(payload) => payload.ingest_id,
        JobCommand::ProbeAsset(payload)
        | JobCommand::NormalizeAsset(payload)
        | JobCommand::ComputeFingerprint(payload)
        | JobCommand::FinalizeIngest(payload) => payload.ingest_id,
        _ => return Err(JobRepositoryError::LeaseLost),
    };
    let (state, run_at, error_class, error_message, terminal, non_consuming) =
        queue_parameters(&job, settlement);
    if terminal {
        sqlx::query(
            "UPDATE ingests SET state = 'failed_terminal', error_code = 'job_lease_expired', error_message = 'job lease expired after the final attempt', completed_at = now(), updated_at = now() WHERE id = $1 AND state NOT IN ('completed', 'failed_terminal', 'cancelled')",
        )
        .bind(current_ingest_id)
        .execute(&mut *transaction)
        .await?;
        if let Some(workspace_id) =
            sqlx::query_scalar::<_, Uuid>("SELECT workspace_id FROM ingests WHERE id = $1")
                .bind(current_ingest_id)
                .fetch_optional(&mut *transaction)
                .await?
        {
            enqueue_workspace_cleanup(
                &mut transaction,
                current_ingest_id,
                workspace_id,
                OffsetDateTime::now_utc() + WORKSPACE_CLEANUP_RETENTION,
            )
            .await?;
        }
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

pub(crate) async fn recover_job(
    pool: &PgPool,
    job_id: Uuid,
    expected: &JobCommand,
) -> Result<bool, JobRepositoryError> {
    let ingest_id = match expected {
        JobCommand::InspectSource(payload) => payload.ingest_id,
        JobCommand::DownloadSource(payload) => payload.ingest_id,
        JobCommand::ProbeAsset(payload)
        | JobCommand::NormalizeAsset(payload)
        | JobCommand::ComputeFingerprint(payload)
        | JobCommand::FinalizeIngest(payload) => payload.ingest_id,
        _ => return Err(JobRepositoryError::LeaseLost),
    };
    let mut transaction = pool.begin().await?;
    // Keep stale recovery in the same ingest-then-queue lock order as normal
    // inbox settlement.
    lock_ingest_for_job(&mut transaction, ingest_id).await?;
    let Some(job) = lock_expired_job(&mut transaction, job_id).await? else {
        transaction.commit().await?;
        return Ok(false);
    };
    let current_command = crate::settlement::validate_locked_command(&job, expected)?;
    let current_ingest_id = match current_command {
        JobCommand::InspectSource(payload) => payload.ingest_id,
        JobCommand::DownloadSource(payload) => payload.ingest_id,
        JobCommand::ProbeAsset(payload)
        | JobCommand::NormalizeAsset(payload)
        | JobCommand::ComputeFingerprint(payload)
        | JobCommand::FinalizeIngest(payload) => payload.ingest_id,
        _ => return Err(JobRepositoryError::LeaseLost),
    };
    let terminal = job.attempt_count >= job.max_attempts;
    if terminal {
        sqlx::query(
            "UPDATE ingests SET state = 'failed_terminal', error_code = 'job_lease_expired', error_message = 'job lease expired after the final attempt', completed_at = now(), updated_at = now() WHERE id = $1 AND state NOT IN ('completed', 'failed_terminal', 'cancelled')",
        )
        .bind(current_ingest_id)
        .execute(&mut *transaction)
        .await?;
        if let Some(workspace_id) =
            sqlx::query_scalar::<_, Uuid>("SELECT workspace_id FROM ingests WHERE id = $1")
                .bind(current_ingest_id)
                .fetch_optional(&mut *transaction)
                .await?
        {
            enqueue_workspace_cleanup(
                &mut transaction,
                current_ingest_id,
                workspace_id,
                OffsetDateTime::now_utc() + WORKSPACE_CLEANUP_RETENTION,
            )
            .await?;
        }
    }
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

async fn lock_ingest_for_job(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ingest_id: Uuid,
) -> Result<(), JobRepositoryError> {
    let _ = sqlx::query_scalar::<_, Uuid>("SELECT id FROM ingests WHERE id = $1 FOR UPDATE")
        .bind(ingest_id)
        .fetch_optional(&mut **transaction)
        .await?;
    Ok(())
}

#[derive(Debug, Error)]
pub enum InboxRepositoryError {
    #[error("ingest list limit {value} exceeds the maximum {max}")]
    InvalidLimit { value: u32, max: u32 },
    #[error("database count was negative")]
    InvalidCount,
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
    #[error("unknown requested action in database: {0}")]
    UnknownRequestedAction(String),
    #[error("invalid ingest input envelope: {0}")]
    InputEnvelope(IngestDataError),
    #[error("requested publish time must be in the future")]
    RequestedPublishAtNotFuture,
    #[error("no enabled publication channel is configured")]
    RequestedChannelNotConfigured,
    #[error("multiple enabled publication channels are configured")]
    RequestedChannelAmbiguous,
    #[error("invalid ingest failure status: {0:?}")]
    InvalidFailureStatus(IngestStatus),
    #[error("force-save is not allowed while ingest is in {0:?}")]
    ForceSaveNotAllowed(IngestStatus),
    #[error("duplicate acceptance is not allowed while ingest is in {0:?}")]
    DuplicateDecisionNotAllowed(IngestStatus),
    #[error("ingest {0} has no persisted duplicate evidence")]
    DuplicateEvidenceMissing(Uuid),
    #[error("media candidate {0} is not present in persisted duplicate evidence")]
    DuplicateCandidateNotEvidenced(Uuid),
    #[error("media candidate {0} does not exist")]
    DuplicateCandidateMissing(Uuid),
    #[error("media candidate {media_id} is unavailable in storage state {state}")]
    DuplicateCandidateUnavailable { media_id: Uuid, state: String },
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
