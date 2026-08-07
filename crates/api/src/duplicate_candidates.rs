use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::HeaderMap,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sooqa_library::{
    DuplicateCandidate, DuplicateCandidateAction, DuplicateCandidateEvent, DuplicateCandidateStatus,
};
use time::OffsetDateTime;
use uuid::Uuid;

use super::{ApiError, ApiState, authorize, map_library_error};

pub(crate) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/v1/duplicate-candidates", get(list_candidates))
        .route("/api/v1/duplicate-candidates/{id}", get(get_candidate))
        .route("/api/v1/duplicate-candidates/{id}/confirm-variant", post(confirm_variant))
        .route("/api/v1/duplicate-candidates/{id}/keep-separate", post(keep_separate))
        .route("/api/v1/duplicate-candidates/{id}/dismiss", post(dismiss))
}

async fn list_candidates(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<CandidateListParams>,
) -> Result<Json<CandidateListResponse>, ApiError> {
    authorize(&state.device_tokens, &headers, "library:read").await?;
    let status =
        params.status.as_deref().map(DuplicateCandidateStatus::try_from).transpose().map_err(
            |_| {
                ApiError::bad_request(
                    "invalid_candidate_status",
                    "The candidate status is invalid",
                    &headers,
                )
            },
        )?;
    let limit = params
        .limit
        .as_deref()
        .map(|value| value.parse::<u32>())
        .transpose()
        .map_err(|_| {
            ApiError::bad_request("invalid_limit", "The limit must be between 1 and 100", &headers)
        })?
        .unwrap_or(20);
    let candidates = state
        .library
        .list_duplicate_candidates(status, limit)
        .await
        .map_err(|error| map_library_error(error, &headers))?;
    Ok(Json(CandidateListResponse {
        items: candidates.iter().map(CandidateResponse::from_candidate).collect(),
    }))
}

async fn get_candidate(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
) -> Result<Json<CandidateDetailResponse>, ApiError> {
    authorize(&state.device_tokens, &headers, "library:read").await?;
    let id = parse_candidate_id(&raw_id, &headers)?;
    let candidate = state
        .library
        .find_duplicate_candidate_by_id(id)
        .await
        .map_err(|error| map_library_error(error, &headers))?
        .ok_or_else(|| {
            ApiError::not_found(
                "duplicate_candidate_not_found",
                "The duplicate candidate was not found",
                &headers,
            )
        })?;
    let events = state
        .library
        .list_duplicate_candidate_events(id)
        .await
        .map_err(|error| map_library_error(error, &headers))?;
    Ok(Json(CandidateDetailResponse {
        candidate: CandidateResponse::from_candidate(&candidate),
        events: events.iter().map(CandidateEventResponse::from_event).collect(),
    }))
}

async fn confirm_variant(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
) -> Result<Json<CandidateDetailResponse>, ApiError> {
    decide(
        &state,
        &headers,
        parse_candidate_id(&raw_id, &headers)?,
        DuplicateCandidateAction::ConfirmVariant,
    )
    .await
}

async fn keep_separate(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
) -> Result<Json<CandidateDetailResponse>, ApiError> {
    decide(
        &state,
        &headers,
        parse_candidate_id(&raw_id, &headers)?,
        DuplicateCandidateAction::KeepSeparate,
    )
    .await
}

async fn dismiss(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
) -> Result<Json<CandidateDetailResponse>, ApiError> {
    decide(
        &state,
        &headers,
        parse_candidate_id(&raw_id, &headers)?,
        DuplicateCandidateAction::Dismiss,
    )
    .await
}

async fn decide(
    state: &ApiState,
    headers: &HeaderMap,
    id: Uuid,
    action: DuplicateCandidateAction,
) -> Result<Json<CandidateDetailResponse>, ApiError> {
    let actor = authorize(&state.device_tokens, headers, "library:write").await?;
    let idempotency_key = super::required_header(headers, "idempotency-key")?.trim();
    if idempotency_key.is_empty() || idempotency_key.chars().count() > 255 {
        return Err(ApiError::bad_request(
            "invalid_idempotency_key",
            "The Idempotency-Key header must be between 1 and 255 characters",
            headers,
        ));
    }
    let candidate = state
        .library
        .decide_duplicate_candidate(id, action, actor.id, idempotency_key)
        .await
        .map_err(|error| map_library_error(error, headers))?;
    let events = state
        .library
        .list_duplicate_candidate_events(id)
        .await
        .map_err(|error| map_library_error(error, headers))?;
    Ok(Json(CandidateDetailResponse {
        candidate: CandidateResponse::from_candidate(&candidate),
        events: events.iter().map(CandidateEventResponse::from_event).collect(),
    }))
}

fn parse_candidate_id(raw_id: &str, headers: &HeaderMap) -> Result<Uuid, ApiError> {
    Uuid::parse_str(raw_id).map_err(|_| {
        ApiError::bad_request(
            "invalid_candidate_id",
            "The duplicate candidate ID must be a UUID",
            headers,
        )
    })
}

#[derive(Debug, Default, Deserialize)]
struct CandidateListParams {
    status: Option<String>,
    limit: Option<String>,
}

#[derive(Debug, Serialize)]
struct CandidateListResponse {
    items: Vec<CandidateResponse>,
}

#[derive(Debug, Serialize)]
struct CandidateDetailResponse {
    candidate: CandidateResponse,
    events: Vec<CandidateEventResponse>,
}

#[derive(Debug, Serialize)]
struct CandidateResponse {
    id: Uuid,
    left_content_item_id: Uuid,
    right_content_item_id: Uuid,
    algorithm_version: String,
    score_basis_points: u16,
    evidence_json: Value,
    status: DuplicateCandidateStatus,
    #[serde(with = "time::serde::rfc3339")]
    created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    updated_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    resolved_at: Option<OffsetDateTime>,
}

impl CandidateResponse {
    fn from_candidate(candidate: &DuplicateCandidate) -> Self {
        Self {
            id: candidate.id,
            left_content_item_id: candidate.left_content_item_id,
            right_content_item_id: candidate.right_content_item_id,
            algorithm_version: candidate.algorithm_version.clone(),
            score_basis_points: candidate.score_basis_points,
            evidence_json: candidate.evidence_json.clone(),
            status: candidate.status,
            created_at: candidate.created_at,
            updated_at: candidate.updated_at,
            resolved_at: candidate.resolved_at,
        }
    }
}

#[derive(Debug, Serialize)]
struct CandidateEventResponse {
    id: Uuid,
    candidate_id: Uuid,
    action: DuplicateCandidateAction,
    actor_device_token_id: Option<Uuid>,
    #[serde(with = "time::serde::rfc3339")]
    created_at: OffsetDateTime,
}

impl CandidateEventResponse {
    fn from_event(event: &DuplicateCandidateEvent) -> Self {
        Self {
            id: event.id,
            candidate_id: event.candidate_id,
            action: event.action,
            actor_device_token_id: event.actor_device_token_id,
            created_at: event.created_at,
        }
    }
}
