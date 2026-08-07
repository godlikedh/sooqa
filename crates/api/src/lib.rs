//! HTTP API boundary for sooqa.

mod duplicate_candidates;
mod library;

use std::time::Duration;

use axum::{
    Json, Router,
    extract::{Json as JsonExtractor, Path, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sooqa_inbox::{
    IngestRequest, IngestSubmission, IngestSubmissionInput, IngestValidationError, SubmittedVia,
};
use sooqa_persistence::{
    DeviceToken, DeviceTokenRepository, DeviceTokenRepositoryError, InboxRepository,
    InboxRepositoryError, LibraryRepository, LibraryRepositoryError,
};
use time::OffsetDateTime;
use tower_http::{
    limit::RequestBodyLimitLayer,
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    timeout::TimeoutLayer,
    trace::TraceLayer,
};
use tracing::error;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ApiSettings {
    pub request_body_limit_bytes: usize,
    pub request_timeout_seconds: u64,
}

impl Default for ApiSettings {
    fn default() -> Self {
        Self { request_body_limit_bytes: 1_048_576, request_timeout_seconds: 30 }
    }
}

#[derive(Clone)]
pub struct ApiState {
    inbox: InboxRepository,
    device_tokens: DeviceTokenRepository,
    library: LibraryRepository,
}

impl ApiState {
    pub fn new(
        inbox: InboxRepository,
        device_tokens: DeviceTokenRepository,
        library: LibraryRepository,
    ) -> Self {
        Self { inbox, device_tokens, library }
    }
}

pub fn router(settings: ApiSettings, state: ApiState) -> Router {
    let router = Router::new()
        .route("/health/live", get(health_live))
        .route("/api/v1/ingest-requests", post(create_ingest))
        .route("/api/v1/ingest-requests/{id}", get(get_ingest))
        .merge(library::routes())
        .merge(duplicate_candidates::routes())
        .with_state(state);

    add_layers(router, settings)
}

pub fn health_router(settings: ApiSettings) -> Router {
    add_layers(Router::new().route("/health/live", get(health_live)), settings)
}

fn add_layers<S>(router: Router<S>, settings: ApiSettings) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router
        .layer(PropagateRequestIdLayer::x_request_id())
        .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
        .layer(TraceLayer::new_for_http())
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(settings.request_timeout_seconds),
        ))
        .layer(RequestBodyLimitLayer::new(settings.request_body_limit_bytes))
}

async fn health_live() -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(HealthResponse {
            status: "ok",
            service: "sooqa-server",
            build: BuildMetadata::current(),
        }),
    )
}

async fn create_ingest(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Result<JsonExtractor<IngestCreateRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<IngestAcceptedResponse>), ApiError> {
    authorize(&state.device_tokens, &headers, "ingest:create").await?;
    let idempotency_key = required_header(&headers, "idempotency-key")?;
    let JsonExtractor(payload) = body.map_err(|rejection| {
        if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
            ApiError::payload_too_large(&headers)
        } else {
            ApiError::bad_request("invalid_json", "The request body must be valid JSON", &headers)
        }
    })?;

    let mut input = IngestSubmissionInput::new(payload.url, SubmittedVia::Api);
    input.submitted_by_admin_id = None;
    input.page_url = payload.page_url;
    input.page_title = payload.page_title;
    input.supplied_caption = payload.selected_text;
    input.supplied_tags = payload.tags;
    input.idempotency_key = Some(idempotency_key.to_owned());

    let submission =
        IngestSubmission::try_new(input).map_err(|error| map_validation_error(error, &headers))?;
    let result = state
        .inbox
        .create_ingest(submission)
        .await
        .map_err(|error| map_repository_error(error, &headers))?;

    Ok((
        StatusCode::ACCEPTED,
        Json(IngestAcceptedResponse {
            id: result.request.id,
            status: result.request.status,
            links: IngestLinks::for_id(result.request.id),
        }),
    ))
}

async fn get_ingest(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<(StatusCode, Json<IngestResponse>), ApiError> {
    authorize(&state.device_tokens, &headers, "ingest:read").await?;
    let request = state
        .inbox
        .find(id)
        .await
        .map_err(|error| map_repository_error(error, &headers))?
        .ok_or_else(|| {
            ApiError::not_found("ingest_not_found", "The ingest request was not found", &headers)
        })?;

    Ok((StatusCode::OK, Json(IngestResponse::from_request(&request))))
}

async fn authorize(
    repository: &DeviceTokenRepository,
    headers: &HeaderMap,
    required_scope: &str,
) -> Result<DeviceToken, ApiError> {
    let token = bearer_token(headers)?;
    let device = repository
        .authenticate(token)
        .await
        .map_err(|error| map_token_error(error, headers))?
        .ok_or_else(|| {
            ApiError::unauthorized("invalid_token", "The bearer token is invalid", headers)
        })?;

    if !device.scopes.iter().any(|scope| scope == required_scope || scope == "admin") {
        return Err(ApiError::forbidden(
            "insufficient_scope",
            "The bearer token does not grant the required scope",
            headers,
        ));
    }

    Ok(device)
}

fn bearer_token(headers: &HeaderMap) -> Result<&str, ApiError> {
    let value = headers.get("authorization").ok_or_else(|| {
        ApiError::unauthorized(
            "authorization_required",
            "Bearer authorization is required",
            headers,
        )
    })?;
    let value = value.to_str().map_err(|_| {
        ApiError::unauthorized(
            "invalid_authorization",
            "The authorization header is invalid",
            headers,
        )
    })?;
    let mut parts = value.splitn(2, char::is_whitespace);
    let scheme = parts.next().unwrap_or_default();
    let token = parts.next().unwrap_or_default().trim();
    if !scheme.eq_ignore_ascii_case("bearer") || token.is_empty() {
        return Err(ApiError::unauthorized(
            "invalid_authorization",
            "The authorization header must use a bearer token",
            headers,
        ));
    }
    Ok(token)
}

fn required_header<'a>(headers: &'a HeaderMap, name: &'static str) -> Result<&'a str, ApiError> {
    let value = headers.get(name).ok_or_else(|| {
        ApiError::bad_request(
            "idempotency_key_required",
            "The Idempotency-Key header is required",
            headers,
        )
    })?;
    value.to_str().map_err(|_| {
        ApiError::bad_request(
            "invalid_idempotency_key",
            "The Idempotency-Key header is invalid",
            headers,
        )
    })
}

fn map_validation_error(error: IngestValidationError, headers: &HeaderMap) -> ApiError {
    let (code, message) = match error {
        IngestValidationError::EmptyUrl(_)
        | IngestValidationError::InvalidUrl(_)
        | IngestValidationError::MissingHost(_)
        | IngestValidationError::CredentialsNotAllowed(_) => {
            ("invalid_url", "The submitted URL is invalid")
        }
        IngestValidationError::UnsupportedScheme(_) => {
            ("unsupported_scheme", "The submitted URL must use HTTP or HTTPS")
        }
        IngestValidationError::EmptyIdempotencyKey
        | IngestValidationError::IdempotencyKeyTooLong => {
            ("invalid_idempotency_key", "The Idempotency-Key header is invalid")
        }
        IngestValidationError::EmptyTag => ("invalid_tag", "The submitted tags are invalid"),
    };
    ApiError::bad_request(code, message, headers)
}

fn map_repository_error(error: InboxRepositoryError, headers: &HeaderMap) -> ApiError {
    match error {
        InboxRepositoryError::IdempotencyConflict { .. } => ApiError::conflict(
            "idempotency_conflict",
            "The Idempotency-Key payload conflicts with the original request",
            headers,
        ),
        error => {
            error!(error = %error, "ingest API repository operation failed");
            ApiError::internal(headers)
        }
    }
}

fn map_token_error(error: DeviceTokenRepositoryError, headers: &HeaderMap) -> ApiError {
    error!(error = %error, "device token authentication failed");
    ApiError::internal(headers)
}

fn map_library_error(error: LibraryRepositoryError, headers: &HeaderMap) -> ApiError {
    match error {
        LibraryRepositoryError::ResourceMissing(_) => {
            ApiError::not_found("library_item_not_found", "The library item was not found", headers)
        }
        LibraryRepositoryError::DuplicateCandidateMissing(_) => ApiError::not_found(
            "duplicate_candidate_not_found",
            "The duplicate candidate was not found",
            headers,
        ),
        LibraryRepositoryError::OptimisticConflict(_) => ApiError::conflict(
            "library_item_changed",
            "The library item changed since it was read",
            headers,
        ),
        LibraryRepositoryError::EmptyUpdate => ApiError::bad_request(
            "empty_update",
            "The request must contain at least one editable field",
            headers,
        ),
        LibraryRepositoryError::InvalidState { operation, .. } => ApiError::conflict(
            "invalid_library_state",
            match operation {
                "archive" => "The library item cannot be archived in its current state",
                _ => "The library item cannot be changed in its current state",
            },
            headers,
        ),
        LibraryRepositoryError::InvalidCandidateState { .. } => ApiError::conflict(
            "invalid_candidate_state",
            "The duplicate candidate has already been resolved",
            headers,
        ),
        LibraryRepositoryError::TagNotAttached => ApiError::not_found(
            "tag_not_attached",
            "The tag is not attached to the library item",
            headers,
        ),
        LibraryRepositoryError::InvalidLimit { .. } => {
            ApiError::bad_request("invalid_limit", "The limit must be between 1 and 100", headers)
        }
        error => {
            error!(error = %error, "library API repository operation failed");
            ApiError::internal(headers)
        }
    }
}

#[derive(Debug, Deserialize)]
struct IngestCreateRequest {
    url: String,
    #[serde(default)]
    page_url: Option<String>,
    #[serde(default)]
    page_title: Option<String>,
    #[serde(default)]
    selected_text: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
}

#[derive(Debug, Serialize)]
struct IngestAcceptedResponse {
    id: Uuid,
    status: sooqa_inbox::IngestStatus,
    links: IngestLinks,
}

#[derive(Debug, Serialize)]
struct IngestResponse {
    id: Uuid,
    kind: sooqa_inbox::IngestKind,
    status: sooqa_inbox::IngestStatus,
    source_url: String,
    page_url: Option<String>,
    page_title: Option<String>,
    supplied_caption: Option<String>,
    supplied_tags: Vec<String>,
    error_code: Option<String>,
    error_message: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    updated_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    completed_at: Option<OffsetDateTime>,
    links: IngestLinks,
}

impl IngestResponse {
    fn from_request(request: &IngestRequest) -> Self {
        Self {
            id: request.id,
            kind: request.kind,
            status: request.status,
            source_url: request.source_url.clone(),
            page_url: request.page_url.clone(),
            page_title: request.page_title.clone(),
            supplied_caption: request.supplied_caption.clone(),
            supplied_tags: request.supplied_tags.clone(),
            error_code: request.error_code.clone(),
            error_message: request.error_message.clone(),
            created_at: request.created_at,
            updated_at: request.updated_at,
            completed_at: request.completed_at,
            links: IngestLinks::for_id(request.id),
        }
    }
}

#[derive(Debug, Serialize)]
struct IngestLinks {
    #[serde(rename = "self")]
    self_link: String,
}

impl IngestLinks {
    fn for_id(id: Uuid) -> Self {
        Self { self_link: format!("/api/v1/ingest-requests/{id}") }
    }
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: &'static str,
    request_id: String,
}

impl ApiError {
    fn bad_request(code: &'static str, message: &'static str, headers: &HeaderMap) -> Self {
        Self::new(StatusCode::BAD_REQUEST, code, message, headers)
    }

    fn unauthorized(code: &'static str, message: &'static str, headers: &HeaderMap) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, code, message, headers)
    }

    fn forbidden(code: &'static str, message: &'static str, headers: &HeaderMap) -> Self {
        Self::new(StatusCode::FORBIDDEN, code, message, headers)
    }

    fn payload_too_large(headers: &HeaderMap) -> Self {
        Self::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            "The request body is too large",
            headers,
        )
    }

    fn not_found(code: &'static str, message: &'static str, headers: &HeaderMap) -> Self {
        Self::new(StatusCode::NOT_FOUND, code, message, headers)
    }

    fn conflict(code: &'static str, message: &'static str, headers: &HeaderMap) -> Self {
        Self::new(StatusCode::CONFLICT, code, message, headers)
    }

    fn internal(headers: &HeaderMap) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "The server could not complete the request",
            headers,
        )
    }

    fn new(
        status: StatusCode,
        code: &'static str,
        message: &'static str,
        headers: &HeaderMap,
    ) -> Self {
        Self { status, code, message, request_id: request_id(headers) }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ApiErrorResponse {
                error: ApiErrorBody {
                    code: self.code,
                    message: self.message,
                    request_id: self.request_id,
                    details: json!({}),
                },
            }),
        )
            .into_response()
    }
}

#[derive(Debug, Serialize)]
struct ApiErrorResponse {
    error: ApiErrorBody,
}

#[derive(Debug, Serialize)]
struct ApiErrorBody {
    code: &'static str,
    message: &'static str,
    request_id: String,
    details: Value,
}

fn request_id(headers: &HeaderMap) -> String {
    headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("unknown")
        .to_owned()
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    service: &'static str,
    build: BuildMetadata,
}

#[derive(Debug, Serialize)]
struct BuildMetadata {
    version: &'static str,
    git_sha: &'static str,
}

impl BuildMetadata {
    fn current() -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION"),
            git_sha: option_env!("SOOQA_BUILD_GIT_SHA").unwrap_or("unknown"),
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::util::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn liveness_returns_build_metadata_and_request_id() {
        let response = health_router(ApiSettings::default())
            .oneshot(
                Request::builder()
                    .uri("/health/live")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().contains_key("x-request-id"));

        let body =
            to_bytes(response.into_body(), 16 * 1024).await.expect("body should be readable");
        let body = String::from_utf8(body.to_vec()).expect("response should be UTF-8");
        assert!(body.contains("\"status\":\"ok\""));
        assert!(body.contains("\"version\":\"0.1.0\""));
        assert!(body.contains("\"git_sha\":\"unknown\""));
    }
}
