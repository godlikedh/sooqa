use serde_json::json;
use sooqa_inbox::SourceInspection;
use sooqa_inbox::{
    IngestKind, IngestRequest, IngestStateError, IngestStatus, IngestSubmission, SubmittedVia,
};
use sooqa_jobs::NewJob;
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
    ) -> Result<AssetProbeStart, InboxRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let mut request = load_request(&mut transaction, id).await?;
        let start = match request.status {
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
        };
        transaction.commit().await?;
        Ok(start)
    }

    pub async fn complete_asset_probe(
        &self,
        id: Uuid,
        probe: serde_json::Value,
    ) -> Result<IngestRequest, InboxRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let mut request = load_request(&mut transaction, id).await?;
        if request.status != IngestStatus::Probing {
            transaction.commit().await?;
            return Ok(request);
        }
        if let Some(object) = request.original_input.as_object_mut() {
            object.insert("probe".to_owned(), probe);
        } else {
            request.original_input = json!({ "source": request.original_input, "probe": probe });
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
        transaction.commit().await?;
        Ok(request)
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

    pub async fn fail_source_inspection(
        &self,
        id: Uuid,
        status: IngestStatus,
        error_code: &str,
        error_message: &str,
    ) -> Result<IngestRequest, InboxRepositoryError> {
        if !matches!(status, IngestStatus::FailedRetryable | IngestStatus::FailedTerminal) {
            return Err(InboxRepositoryError::InvalidSourceInspectionFailureStatus(status));
        }

        self.fail_asset_probe(id, status, error_code, error_message).await
    }

    pub async fn fail_asset_probe(
        &self,
        id: Uuid,
        status: IngestStatus,
        error_code: &str,
        error_message: &str,
    ) -> Result<IngestRequest, InboxRepositoryError> {
        if !matches!(status, IngestStatus::FailedRetryable | IngestStatus::FailedTerminal) {
            return Err(InboxRepositoryError::InvalidSourceInspectionFailureStatus(status));
        }

        let mut transaction = self.pool.begin().await?;
        let mut request = load_request(&mut transaction, id).await?;
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
pub enum AssetProbeStart {
    Ready(IngestRequest),
    AlreadyAdvanced(IngestRequest),
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
        "#,
    )
    .bind(job.job_type().as_str())
    .bind(job.payload_json())
    .bind(format!("ingest:{}:probe_asset:v1", request.id))
    .execute(&mut **transaction)
    .await?;
    Ok(())
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
    #[error("invalid source inspection failure status: {0:?}")]
    InvalidSourceInspectionFailureStatus(IngestStatus),
    #[error("invalid ingest state transition: {0}")]
    InvalidStateTransition(#[from] IngestStateError),
    #[error("database operation failed: {0}")]
    Database(#[from] sqlx::Error),
}
