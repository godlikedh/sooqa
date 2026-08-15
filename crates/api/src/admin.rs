use axum::{
    Json, Router,
    extract::{Query, State},
    http::HeaderMap,
    routing::get,
};
use serde::{Deserialize, Serialize};
use sooqa_inbox::{IngestStatus, RequestedAction};
use sooqa_library::VideoDuplicateClassification;
use sooqa_publisher::{Post, PostState, PublicationAction, RepeatEvidence};
use time::OffsetDateTime;
use uuid::Uuid;

use super::{ApiError, ApiState, authorize};

pub(crate) fn routes() -> Router<ApiState> {
    Router::new().route("/api/v1/dashboard", get(get_dashboard))
}

pub(crate) async fn list_ingests(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<IngestListParams>,
) -> Result<Json<IngestPageResponse>, ApiError> {
    authorize(&state.api_token, &headers).await?;
    let limit = params.limit.unwrap_or(50);
    if !(1..=50).contains(&limit) {
        return Err(ApiError::bad_request(
            "invalid_limit",
            "The ingest list limit must be between 1 and 50",
            &headers,
        ));
    }
    let cursor = params.cursor.as_deref().map(decode_ingest_cursor).transpose().map_err(|_| {
        ApiError::bad_request("invalid_cursor", "The ingest cursor is invalid", &headers)
    })?;
    let page = state.inbox.list_admin(limit, cursor).await.map_err(|error| {
        tracing::error!(error = %error, "ingest admin list failed");
        ApiError::internal(&headers)
    })?;
    Ok(Json(IngestPageResponse::from_page(&page)))
}

async fn get_dashboard(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<DashboardParams>,
) -> Result<Json<DashboardResponse>, ApiError> {
    authorize(&state.api_token, &headers).await?;
    let limit = params.limit.unwrap_or(20);
    if !(1..=50).contains(&limit) {
        return Err(ApiError::bad_request(
            "invalid_limit",
            "The dashboard limit must be between 1 and 50",
            &headers,
        ));
    }

    let ready_media = state.library.count_ready_media().await.map_err(|error| {
        tracing::error!(error = %error, "dashboard media count failed");
        ApiError::internal(&headers)
    })?;
    let future_queued_posts =
        state.publisher.count_future_queued_posts().await.map_err(|error| {
            tracing::error!(error = %error, "dashboard post count failed");
            ApiError::internal(&headers)
        })?;
    let active_ingests = state.inbox.count_active().await.map_err(|error| {
        tracing::error!(error = %error, "dashboard ingest count failed");
        ApiError::internal(&headers)
    })?;
    let technical_jobs = state.jobs.count_technical_jobs().await.map_err(|error| {
        tracing::error!(error = %error, "dashboard job count failed");
        ApiError::internal(&headers)
    })?;
    let technical_duplicate_decisions =
        state.inbox.list_duplicate_pending(limit).await.map_err(|error| {
            tracing::error!(error = %error, "dashboard duplicate decisions failed");
            ApiError::internal(&headers)
        })?;
    let technical_duplicate_count =
        state.inbox.count_duplicate_pending().await.map_err(|error| {
            tracing::error!(error = %error, "dashboard duplicate count failed");
            ApiError::internal(&headers)
        })?;
    let repeat_decisions = state.publisher.list_repeat_decisions(limit).await.map_err(|error| {
        tracing::error!(error = %error, "dashboard repeat decisions failed");
        ApiError::internal(&headers)
    })?;
    let repeat_count = state.publisher.count_repeat_decisions().await.map_err(|error| {
        tracing::error!(error = %error, "dashboard repeat count failed");
        ApiError::internal(&headers)
    })?;
    let caption_sync_failures =
        state.library.count_caption_sync_failures().await.map_err(|error| {
            tracing::error!(error = %error, "dashboard caption sync count failed");
            ApiError::internal(&headers)
        })?;
    let caption_sync_failure_items =
        state.library.list_caption_sync_failures(limit).await.map_err(|error| {
            tracing::error!(error = %error, "dashboard caption sync items failed");
            ApiError::internal(&headers)
        })?;

    Ok(Json(DashboardResponse {
        counts: DashboardCounts {
            ready_media,
            future_queued_posts,
            active_ingests,
            technical_jobs_queued: technical_jobs.queued,
            technical_jobs_running: technical_jobs.running,
            technical_duplicate_decisions: technical_duplicate_count,
            publication_repeat_decisions: repeat_count,
            caption_sync_failures,
        },
        attention: DashboardAttention {
            technical_duplicates: technical_duplicate_decisions
                .into_iter()
                .map(TechnicalDuplicateResponse::from_pending)
                .collect(),
            publication_repeats: repeat_decisions
                .iter()
                .map(RepeatDecisionResponse::from_post)
                .collect(),
            caption_sync_failures: caption_sync_failure_items
                .into_iter()
                .map(CaptionSyncFailureResponse::from_failure)
                .collect(),
        },
    }))
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct IngestListParams {
    limit: Option<u32>,
    cursor: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct DashboardParams {
    limit: Option<u32>,
}

fn encode_ingest_cursor(cursor: &sooqa_inbox::IngestCursor) -> String {
    format!("{}:{}", cursor.created_at.unix_timestamp_nanos(), cursor.id)
}

fn decode_ingest_cursor(value: &str) -> Result<sooqa_inbox::IngestCursor, ()> {
    let (timestamp, id) = value.split_once(':').ok_or(())?;
    let timestamp = timestamp.parse::<i128>().map_err(|_| ())?;
    let created_at = OffsetDateTime::from_unix_timestamp_nanos(timestamp).map_err(|_| ())?;
    let id = Uuid::parse_str(id).map_err(|_| ())?;
    Ok(sooqa_inbox::IngestCursor { created_at, id })
}

#[derive(Debug, Serialize)]
pub(crate) struct IngestPageResponse {
    items: Vec<IngestListResponse>,
    next_cursor: Option<String>,
}

impl IngestPageResponse {
    fn from_page(page: &sooqa_inbox::IngestPage) -> Self {
        Self {
            items: page.items.iter().map(IngestListResponse::from_item).collect(),
            next_cursor: page.next_cursor.as_ref().map(encode_ingest_cursor),
        }
    }
}

#[derive(Debug, Serialize)]
struct IngestListResponse {
    id: Uuid,
    source_url: Option<String>,
    page_url: Option<String>,
    requested_action: RequestedAction,
    status: IngestStatus,
    #[serde(with = "time::serde::rfc3339")]
    created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    updated_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    completed_at: Option<OffsetDateTime>,
    media_id: Option<Uuid>,
    storage_url: Option<String>,
    error_code: Option<String>,
    error_message: Option<String>,
}

impl IngestListResponse {
    fn from_item(item: &sooqa_inbox::IngestListItem) -> Self {
        Self {
            id: item.id,
            source_url: item.source_url.clone(),
            page_url: item.page_url.clone(),
            requested_action: item.requested_action,
            status: item.status,
            created_at: item.created_at,
            updated_at: item.updated_at,
            completed_at: item.completed_at,
            media_id: item.media_id,
            storage_url: item.storage_url.clone(),
            error_code: item.error_code.clone(),
            error_message: item.error_message.clone(),
        }
    }
}

#[derive(Debug, Serialize)]
struct DashboardResponse {
    counts: DashboardCounts,
    attention: DashboardAttention,
}

#[derive(Debug, Serialize)]
struct DashboardCounts {
    ready_media: u64,
    future_queued_posts: u64,
    active_ingests: u64,
    technical_jobs_queued: u64,
    technical_jobs_running: u64,
    technical_duplicate_decisions: u64,
    publication_repeat_decisions: u64,
    caption_sync_failures: u64,
}

#[derive(Debug, Serialize)]
struct DashboardAttention {
    technical_duplicates: Vec<TechnicalDuplicateResponse>,
    publication_repeats: Vec<RepeatDecisionResponse>,
    caption_sync_failures: Vec<CaptionSyncFailureResponse>,
}

#[derive(Debug, Serialize)]
struct TechnicalDuplicateResponse {
    ingest_id: Uuid,
    source_url: Option<String>,
    candidates: Vec<TechnicalDuplicateCandidateResponse>,
}

impl TechnicalDuplicateResponse {
    fn from_pending(pending: sooqa_persistence::DuplicatePendingIngest) -> Self {
        Self {
            ingest_id: pending.ingest.id,
            source_url: Some(pending.ingest.source_url),
            candidates: pending
                .candidates
                .into_iter()
                .map(|candidate| TechnicalDuplicateCandidateResponse {
                    media_id: candidate.media_id,
                    classification: candidate.classification,
                    score_bps: candidate.score_bps,
                    storage_state: candidate.storage_state,
                    storage_url: candidate
                        .storage_chat_id
                        .zip(candidate.storage_message_id)
                        .and_then(storage_message_url),
                })
                .collect(),
        }
    }
}

#[derive(Debug, Serialize)]
struct TechnicalDuplicateCandidateResponse {
    media_id: Uuid,
    classification: VideoDuplicateClassification,
    score_bps: u16,
    storage_state: String,
    storage_url: Option<String>,
}

#[derive(Debug, Serialize)]
struct RepeatDecisionResponse {
    post_id: Uuid,
    media_id: Uuid,
    requested_action: PublicationAction,
    #[serde(with = "time::serde::rfc3339::option")]
    requested_publish_at: Option<OffsetDateTime>,
    caption: Option<String>,
    status: PostState,
    repeat_evidence: Option<RepeatEvidence>,
    revision: i64,
}

impl RepeatDecisionResponse {
    fn from_post(post: &Post) -> Self {
        Self {
            post_id: post.id,
            media_id: post.media_id,
            requested_action: post.requested_action,
            requested_publish_at: post.requested_publish_at,
            caption: post.caption.clone(),
            status: post.state,
            repeat_evidence: post.repeat_evidence.clone(),
            revision: post.revision,
        }
    }
}

#[derive(Debug, Serialize)]
struct CaptionSyncFailureResponse {
    media_id: Uuid,
    error_message: Option<String>,
}

impl CaptionSyncFailureResponse {
    fn from_failure(failure: sooqa_library::CaptionSyncFailure) -> Self {
        Self { media_id: failure.media_id, error_message: failure.error_message }
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
