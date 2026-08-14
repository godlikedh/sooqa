//! HTTP API boundary for sooqa.

mod library;
mod publisher;

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
use sha2::{Digest, Sha256};
use sooqa_inbox::{
    Ingest, IngestSubmission, IngestSubmissionInput, IngestValidationError, RequestedAction,
    SubmittedVia,
};
use sooqa_persistence::{
    InboxRepository, InboxRepositoryError, LibraryRepository, LibraryRepositoryError,
    PublisherRepository, PublisherRepositoryError,
};
use subtle::ConstantTimeEq;
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
    pub(crate) api_token: String,
    library: LibraryRepository,
    publisher: PublisherRepository,
}

impl ApiState {
    pub fn new(
        inbox: InboxRepository,
        api_token: impl Into<String>,
        library: LibraryRepository,
        publisher: PublisherRepository,
    ) -> Self {
        Self { inbox, api_token: api_token.into(), library, publisher }
    }
}

pub fn router(settings: ApiSettings, state: ApiState) -> Router {
    let router = Router::new()
        .route("/health/live", get(health_live))
        .route("/api/v1/ingests", post(create_ingest))
        .route("/api/v1/ingests/{id}", get(get_ingest))
        .route("/api/v1/ingests/{id}/accept-duplicate", post(accept_duplicate))
        .route("/api/v1/ingests/{id}/force-save", post(force_save))
        .merge(library::routes())
        .merge(publisher::routes())
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
    authorize(&state.api_token, &headers).await?;
    let idempotency_key = required_header(&headers, "idempotency-key")?;
    let JsonExtractor(payload) = body.map_err(|rejection| {
        if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
            ApiError::payload_too_large(&headers)
        } else {
            ApiError::bad_request("invalid_json", "The request body must be valid JSON", &headers)
        }
    })?;

    let requested_action =
        RequestedAction::try_from(payload.requested_action.as_str()).map_err(|_| {
            map_validation_error(IngestValidationError::InvalidRequestedAction, &headers)
        })?;
    let requested_publish_at = payload
        .requested_publish_at
        .as_deref()
        .map(|value| {
            OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339).map_err(
                |_| {
                    map_validation_error(IngestValidationError::InvalidRequestedPublishAt, &headers)
                },
            )
        })
        .transpose()?;

    let mut input = IngestSubmissionInput::new(payload.url, SubmittedVia::Api);
    input.page_url = payload.page_url;
    input.page_title = payload.page_title;
    input.supplied_caption = payload.selected_text;
    input.supplied_description = payload.description;
    input.supplied_tags = payload.tags;
    input.requested_action = requested_action;
    input.requested_publish_at = requested_publish_at;
    input.requested_post_caption = payload.requested_post_caption;
    input.idempotency_key = Some(idempotency_key.to_owned());

    let submission = IngestSubmission::try_new_for_idempotency_lookup(input)
        .map_err(|error| map_validation_error(error, &headers))?;
    let result = state
        .inbox
        .create_ingest(submission)
        .await
        .map_err(|error| map_repository_error(error, &headers))?;

    Ok((
        StatusCode::ACCEPTED,
        Json(IngestAcceptedResponse {
            id: result.ingest.id,
            status: result.ingest.status,
            requested_action: result.ingest.requested_action,
            requested_publish_at: result.ingest.requested_publish_at,
            requested_post_caption: result.ingest.requested_post_caption.clone(),
            requested_channel_id: result.ingest.requested_channel_id,
            links: IngestLinks::for_id(result.ingest.id),
        }),
    ))
}

async fn get_ingest(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<(StatusCode, Json<IngestResponse>), ApiError> {
    authorize(&state.api_token, &headers).await?;
    let request = state
        .inbox
        .find(id)
        .await
        .map_err(|error| map_repository_error(error, &headers))?
        .ok_or_else(|| {
            ApiError::not_found("ingest_not_found", "The ingest request was not found", &headers)
        })?;

    Ok((StatusCode::OK, Json(IngestResponse::from_ingest(&request))))
}

async fn force_save(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<(StatusCode, Json<IngestResponse>), ApiError> {
    authorize(&state.api_token, &headers).await?;
    let result =
        state.inbox.force_save(id).await.map_err(|error| map_repository_error(error, &headers))?;
    let status = if result.resumed { StatusCode::ACCEPTED } else { StatusCode::OK };
    Ok((status, Json(IngestResponse::from_ingest(&result.ingest))))
}

async fn accept_duplicate(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    body: Result<JsonExtractor<AcceptDuplicateRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<IngestResponse>), ApiError> {
    authorize(&state.api_token, &headers).await?;
    let JsonExtractor(payload) = body.map_err(|rejection| {
        if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
            ApiError::payload_too_large(&headers)
        } else {
            ApiError::bad_request("invalid_json", "The request body must be valid JSON", &headers)
        }
    })?;
    let result = state
        .inbox
        .accept_duplicate(id, payload.media_id)
        .await
        .map_err(|error| map_repository_error(error, &headers))?;
    let status = if result.ingest.status == sooqa_inbox::IngestStatus::Storing {
        StatusCode::ACCEPTED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(IngestResponse::from_ingest(&result.ingest))))
}

async fn authorize(expected_token: &str, headers: &HeaderMap) -> Result<(), ApiError> {
    let token = bearer_token(headers)?;
    let expected = Sha256::digest(expected_token.as_bytes());
    let actual = Sha256::digest(token.as_bytes());
    if expected.ct_eq(&actual).unwrap_u8() != 1 {
        return Err(ApiError::unauthorized(
            "invalid_token",
            "The bearer token is invalid",
            headers,
        ));
    }
    if expected_token.is_empty() {
        return Err(ApiError::forbidden(
            "api_token_not_configured",
            "The API bearer token is not configured",
            headers,
        ));
    }
    Ok(())
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
        IngestValidationError::InvalidRequestedAction => {
            ("invalid_requested_action", "The requested action must be save, queue, or post_now")
        }
        IngestValidationError::InvalidRequestedPublishAt => (
            "invalid_requested_publish_at",
            "The requested publish time must be an RFC3339 instant",
        ),
        IngestValidationError::RequestedPublishAtForbidden
        | IngestValidationError::RequestedPublishAtForbiddenForPostNow => (
            "requested_publish_at_not_allowed",
            "The requested publish time is not valid for this action",
        ),
        IngestValidationError::RequestedPublishAtNotFuture => {
            ("requested_publish_at_not_future", "An exact queue time must be in the future")
        }
        IngestValidationError::RequestedPostCaptionForbidden => (
            "requested_post_caption_not_allowed",
            "Save requests must not include public post text",
        ),
        IngestValidationError::RequestedPostCaptionTooLong { .. } => {
            ("requested_post_caption_too_long", "The requested public post text is too long")
        }
        IngestValidationError::RequestedPostCaptionControlCharacter => (
            "requested_post_caption_invalid",
            "The requested public post text contains a disallowed control character",
        ),
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
        InboxRepositoryError::ForceSaveNotAllowed(_) => ApiError::conflict(
            "force_save_not_allowed",
            "Force-save is only available for a pending perceptual duplicate",
            headers,
        ),
        InboxRepositoryError::DuplicateDecisionNotAllowed(_) => ApiError::conflict(
            "duplicate_decision_not_allowed",
            "The ingest is no longer waiting for a duplicate decision",
            headers,
        ),
        InboxRepositoryError::DuplicateEvidenceMissing(_)
        | InboxRepositoryError::DuplicateCandidateNotEvidenced(_) => ApiError::conflict(
            "duplicate_candidate_not_evidenced",
            "The selected media is not one of the persisted duplicate candidates",
            headers,
        ),
        InboxRepositoryError::DuplicateCandidateMissing(_)
        | InboxRepositoryError::DuplicateCandidateUnavailable { .. } => ApiError::conflict(
            "duplicate_candidate_unavailable",
            "The selected duplicate candidate is no longer available",
            headers,
        ),
        InboxRepositoryError::RequestedPublishAtNotFuture => ApiError::bad_request(
            "requested_publish_at_not_future",
            "An exact queue time must be in the future",
            headers,
        ),
        InboxRepositoryError::RequestedChannelNotConfigured => ApiError::conflict(
            "requested_channel_not_configured",
            "A queue or post-now request requires exactly one enabled publication channel",
            headers,
        ),
        InboxRepositoryError::RequestedChannelAmbiguous => ApiError::conflict(
            "requested_channel_ambiguous",
            "A queue or post-now request requires exactly one enabled publication channel",
            headers,
        ),
        InboxRepositoryError::ResourceMissing(_) => {
            ApiError::not_found("ingest_not_found", "The ingest request was not found", headers)
        }
        error => {
            error!(error = %error, "ingest API repository operation failed");
            ApiError::internal(headers)
        }
    }
}

fn map_library_error(error: LibraryRepositoryError, headers: &HeaderMap) -> ApiError {
    match error {
        LibraryRepositoryError::ResourceMissing(_) => {
            ApiError::not_found("media_not_found", "The media item was not found", headers)
        }
        LibraryRepositoryError::OptimisticConflict(_) => {
            ApiError::conflict("media_changed", "The media item changed since it was read", headers)
        }
        LibraryRepositoryError::EmptyUpdate => ApiError::bad_request(
            "empty_update",
            "The request must contain at least one editable field",
            headers,
        ),
        LibraryRepositoryError::TagNotAttached => ApiError::not_found(
            "tag_not_attached",
            "The tag is not attached to the media item",
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

fn map_publisher_error(error: PublisherRepositoryError, headers: &HeaderMap) -> ApiError {
    match error {
        PublisherRepositoryError::ChannelMissing(_) => {
            ApiError::not_found("channel_not_found", "The channel was not found", headers)
        }
        PublisherRepositoryError::ChannelDisabled(_) => {
            ApiError::conflict("channel_disabled", "The channel is disabled", headers)
        }
        PublisherRepositoryError::PostMissing(_) => {
            ApiError::not_found("post_not_found", "The post was not found", headers)
        }
        PublisherRepositoryError::MediaMissing(_) => {
            ApiError::not_found("media_not_found", "The media item was not found", headers)
        }
        PublisherRepositoryError::MediaNotReady { .. } => ApiError::conflict(
            "media_not_publishable",
            "The media item is not ready for publication",
            headers,
        ),
        PublisherRepositoryError::PostNotEditable { .. }
        | PublisherRepositoryError::PostCannotBeScheduled { .. }
        | PublisherRepositoryError::PostNotClaimable { .. }
        | PublisherRepositoryError::StalePublicationJob { .. }
        | PublisherRepositoryError::PublishJobMissing(_)
        | PublisherRepositoryError::PublishJobRunning(_)
        | PublisherRepositoryError::PublishJobUnavailable { .. }
        | PublisherRepositoryError::PublishJobUpdateLost(_)
        | PublisherRepositoryError::PublishLeaseLost(_)
        | PublisherRepositoryError::PublishConflict(_)
        | PublisherRepositoryError::InvalidPublishFailureState(_)
        | PublisherRepositoryError::CadenceSlotInPast
        | PublisherRepositoryError::InvalidCadenceSlot => ApiError::conflict(
            "invalid_publication_state",
            "The post cannot be changed in its current state",
            headers,
        ),
        PublisherRepositoryError::OptimisticConflict(_) => {
            ApiError::conflict("post_changed", "The post changed since it was read", headers)
        }
        PublisherRepositoryError::RequestKeyConflict(_) => ApiError::conflict(
            "idempotency_conflict",
            "The Idempotency-Key payload conflicts with the original request",
            headers,
        ),
        PublisherRepositoryError::Validation(validation) => match validation {
            sooqa_publisher::PublisherValidationError::EmptyRequestKey
            | sooqa_publisher::PublisherValidationError::RequestKeyTooLong { .. } => {
                ApiError::bad_request(
                    "invalid_idempotency_key",
                    "The Idempotency-Key header must be between 1 and 255 characters",
                    headers,
                )
            }
            _ => ApiError::bad_request(
                "invalid_publication_request",
                "The publication request is invalid",
                headers,
            ),
        },
        PublisherRepositoryError::ChannelValidation(_) => {
            ApiError::bad_request("invalid_channel", "The channel payload is invalid", headers)
        }
        error => {
            error!(error = %error, "publisher API repository operation failed");
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
    description: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default = "default_requested_action")]
    requested_action: String,
    #[serde(default)]
    requested_publish_at: Option<String>,
    #[serde(default)]
    requested_post_caption: Option<String>,
}

fn default_requested_action() -> String {
    RequestedAction::Save.as_str().to_owned()
}

#[derive(Debug, Deserialize)]
struct AcceptDuplicateRequest {
    media_id: Uuid,
}

#[derive(Debug, Serialize)]
struct IngestAcceptedResponse {
    id: Uuid,
    status: sooqa_inbox::IngestStatus,
    requested_action: RequestedAction,
    #[serde(with = "time::serde::rfc3339::option")]
    requested_publish_at: Option<OffsetDateTime>,
    requested_post_caption: Option<String>,
    requested_channel_id: Option<Uuid>,
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
    supplied_description: Option<String>,
    supplied_tags: Vec<String>,
    requested_action: RequestedAction,
    #[serde(with = "time::serde::rfc3339::option")]
    requested_publish_at: Option<OffsetDateTime>,
    requested_post_caption: Option<String>,
    requested_channel_id: Option<Uuid>,
    media_id: Option<Uuid>,
    force_save: bool,
    duplicate_evidence: Option<Value>,
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
    fn from_ingest(request: &Ingest) -> Self {
        Self {
            id: request.id,
            kind: request.kind,
            status: request.status,
            source_url: request.source_url.clone(),
            page_url: request.page_url.clone(),
            page_title: request.page_title.clone(),
            supplied_caption: request.supplied_caption.clone(),
            supplied_description: request.supplied_description.clone(),
            supplied_tags: request.supplied_tags.clone(),
            requested_action: request.requested_action,
            requested_publish_at: request.requested_publish_at,
            requested_post_caption: request.requested_post_caption.clone(),
            requested_channel_id: request.requested_channel_id,
            media_id: request.media_id,
            force_save: request.force_save,
            duplicate_evidence: request.duplicate_evidence.clone(),
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
        Self { self_link: format!("/api/v1/ingests/{id}") }
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
