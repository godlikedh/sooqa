use axum::{
    Json, Router,
    extract::{Json as JsonExtractor, Path, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use serde::{Deserialize, Serialize, de::Deserializer};
use sha2::{Digest, Sha256};
use sooqa_library::{MediaStatus, MediaStorageState};
use sooqa_publisher::{
    Channel, NewChannel, NewPost, Post, PostSchedule, PostState, PostUpdate, QueueDirection,
};
use time::{OffsetDateTime, Time};
use uuid::Uuid;

use super::{
    ApiError, ApiState, authorize, map_library_error, map_publisher_error, required_header,
};

const MAX_CAPTION_LENGTH: usize = 1_024;

pub(crate) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/v1/channels", get(list_channels).post(create_channel))
        .route("/api/v1/channels/{id}", get(get_channel))
        .route("/api/v1/posts", post(create_post))
        .route("/api/v1/posts/{id}", get(get_post).patch(update_post))
        .route("/api/v1/posts/{id}/schedule", post(schedule_post))
        .route("/api/v1/posts/{id}/publish", post(publish_now))
        .route("/api/v1/posts/{id}/earlier", post(move_earlier))
        .route("/api/v1/posts/{id}/later", post(move_later))
        .route("/api/v1/posts/{id}/slot", post(set_slot))
        .route("/api/v1/posts/{id}/cancel", post(cancel_post))
}

async fn list_channels(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Json<ChannelListResponse>, ApiError> {
    authorize(&state.api_token, &headers).await?;
    let channels = state
        .publisher
        .list_channels(false)
        .await
        .map_err(|error| map_publisher_error(error, &headers))?;
    Ok(Json(ChannelListResponse {
        items: channels.iter().map(ChannelResponse::from_channel).collect(),
    }))
}

async fn create_channel(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Result<JsonExtractor<CreateChannelRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ChannelResponse>), ApiError> {
    authorize(&state.api_token, &headers).await?;
    let JsonExtractor(payload) =
        body.map_err(|rejection| map_json_rejection(rejection, &headers))?;
    let mut channel =
        NewChannel::try_new(payload.name, payload.telegram_chat_id).map_err(|_| {
            ApiError::bad_request("invalid_channel", "The channel payload is invalid", &headers)
        })?;
    if let Some(time_zone) = payload.time_zone {
        channel.time_zone = time_zone;
    }
    if let Some(window_start) = payload.window_start {
        channel.window_start = parse_time(&window_start, &headers)?;
    }
    if let Some(window_end) = payload.window_end {
        channel.window_end = parse_time(&window_end, &headers)?;
    }
    if let Some(interval_minutes) = payload.interval_minutes {
        channel.interval_minutes = interval_minutes;
    }
    channel.default_parse_mode = normalize_parse_mode(payload.default_parse_mode, &headers)?;
    channel.default_disable_notification = payload.default_disable_notification;
    let channel = state
        .publisher
        .create_channel(channel)
        .await
        .map_err(|error| map_publisher_error(error, &headers))?;
    Ok((StatusCode::CREATED, Json(ChannelResponse::from_channel(&channel))))
}

async fn get_channel(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<ChannelResponse>, ApiError> {
    authorize(&state.api_token, &headers).await?;
    let channel = state
        .publisher
        .find_channel(id)
        .await
        .map_err(|error| map_publisher_error(error, &headers))?
        .ok_or_else(|| {
            ApiError::not_found("channel_not_found", "The channel was not found", &headers)
        })?;
    Ok(Json(ChannelResponse::from_channel(&channel)))
}

async fn create_post(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Result<JsonExtractor<CreatePostRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<PostResponse>), ApiError> {
    authorize(&state.api_token, &headers).await?;
    let request_key = idempotency_key(&headers)?;
    let JsonExtractor(payload) =
        body.map_err(|rejection| map_json_rejection(rejection, &headers))?;
    let media = state
        .library
        .find_media(payload.media_id)
        .await
        .map_err(|error| map_library_error(error, &headers))?
        .ok_or_else(|| {
            ApiError::not_found("media_not_found", "The media item was not found", &headers)
        })?;
    require_publishable(&media.status, media.storage_state, &headers)?;
    let channel = state
        .publisher
        .find_channel(payload.channel_id)
        .await
        .map_err(|error| map_publisher_error(error, &headers))?
        .ok_or_else(|| {
            ApiError::not_found("channel_not_found", "The channel was not found", &headers)
        })?;
    if !channel.is_enabled {
        return Err(ApiError::conflict("channel_disabled", "The channel is disabled", &headers));
    }
    let request_hash = post_request_hash(&payload);
    let parse_mode_input = payload.parse_mode.clone();
    let caption = normalize_caption(payload.caption, parse_mode_input.as_deref(), &headers)?;
    let parse_mode = match payload.parse_mode {
        Some(value) => normalize_parse_mode(Some(value), &headers)?,
        None => channel.default_parse_mode.clone(),
    };
    validate_caption(caption.as_deref(), parse_mode.as_deref(), &headers)?;
    let post = state
        .publisher
        .create_post_idempotent(
            NewPost {
                media_id: payload.media_id,
                channel_id: payload.channel_id,
                caption,
                parse_mode,
                disable_notification: channel.default_disable_notification,
            },
            request_key,
            &request_hash,
        )
        .await
        .map_err(|error| map_publisher_error(error, &headers))?;
    Ok((
        if post.created { StatusCode::CREATED } else { StatusCode::OK },
        Json(PostResponse::from_post(&post.post)),
    ))
}

async fn get_post(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<PostResponse>, ApiError> {
    authorize(&state.api_token, &headers).await?;
    let post = state
        .publisher
        .find_post(id)
        .await
        .map_err(|error| map_publisher_error(error, &headers))?
        .ok_or_else(|| ApiError::not_found("post_not_found", "The post was not found", &headers))?;
    Ok(Json(PostResponse::from_post(&post)))
}

async fn update_post(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    body: Result<JsonExtractor<UpdatePostRequest>, JsonRejection>,
) -> Result<Json<PostResponse>, ApiError> {
    authorize(&state.api_token, &headers).await?;
    let JsonExtractor(payload) =
        body.map_err(|rejection| map_json_rejection(rejection, &headers))?;
    let caption_input = payload.caption.into_option();
    let parse_mode_input = payload.parse_mode.into_option();
    let disable_notification = match payload.disable_notification.into_option() {
        None => None,
        Some(Some(value)) => Some(value),
        Some(None) => {
            return Err(ApiError::bad_request(
                "invalid_disable_notification",
                "disable_notification must be a boolean",
                &headers,
            ));
        }
    };
    if caption_input.is_none() && parse_mode_input.is_none() && disable_notification.is_none() {
        return Err(ApiError::bad_request(
            "empty_update",
            "The request must contain at least one editable field",
            &headers,
        ));
    }
    validate_expected_revision(payload.expected_revision, &headers)?;
    let current = state
        .publisher
        .find_post(id)
        .await
        .map_err(|error| map_publisher_error(error, &headers))?
        .ok_or_else(|| ApiError::not_found("post_not_found", "The post was not found", &headers))?;
    if !current.state.is_queue_mutable() {
        return Err(ApiError::conflict(
            "invalid_post_state",
            "The post cannot be changed in its current state",
            &headers,
        ));
    }
    let parse_mode_for_caption = parse_mode_input.as_ref().and_then(Option::as_deref);
    let caption = caption_input
        .map(|value| normalize_caption(value, parse_mode_for_caption, &headers))
        .transpose()?;
    let parse_mode =
        parse_mode_input.map(|value| normalize_parse_mode(value, &headers)).transpose()?;
    let effective_caption =
        caption.as_ref().map_or(current.caption.as_deref(), |value| value.as_deref());
    let effective_parse_mode =
        parse_mode.as_ref().map_or(current.parse_mode.as_deref(), |value| value.as_deref());
    validate_caption(effective_caption, effective_parse_mode, &headers)?;
    let post = state
        .publisher
        .update_post(
            id,
            PostUpdate {
                caption,
                parse_mode,
                disable_notification,
                expected_updated_at: payload.expected_updated_at,
                expected_revision: payload.expected_revision,
            },
        )
        .await
        .map_err(|error| map_publisher_error(error, &headers))?;
    Ok(Json(PostResponse::from_post(&post)))
}

async fn schedule_post(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    body: Result<JsonExtractor<ScheduleRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<PostResponse>), ApiError> {
    authorize(&state.api_token, &headers).await?;
    let JsonExtractor(payload) =
        body.map_err(|rejection| map_json_rejection(rejection, &headers))?;
    let requested_at = payload.publish_at.unwrap_or_else(OffsetDateTime::now_utc);
    validate_expected_revision(payload.expected_revision, &headers)?;
    let schedule = PostSchedule::try_new(
        id,
        requested_at,
        idempotency_key(&headers)?,
        payload.expected_revision,
    )
    .map_err(|error| map_publisher_error(error.into(), &headers))?;
    let post = state
        .publisher
        .schedule_post(schedule)
        .await
        .map_err(|error| map_publisher_error(error, &headers))?;
    Ok((StatusCode::ACCEPTED, Json(PostResponse::from_post(&post))))
}

async fn publish_now(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    body: Result<JsonExtractor<MutationRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<PostResponse>), ApiError> {
    authorize(&state.api_token, &headers).await?;
    let JsonExtractor(payload) =
        body.map_err(|rejection| map_json_rejection(rejection, &headers))?;
    validate_expected_revision(payload.expected_revision, &headers)?;
    let post = state
        .publisher
        .publish_now(id, idempotency_key(&headers)?, payload.expected_revision)
        .await
        .map_err(|error| map_publisher_error(error, &headers))?;
    Ok((StatusCode::ACCEPTED, Json(PostResponse::from_post(&post))))
}

async fn move_earlier(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    body: Result<JsonExtractor<MutationRequest>, JsonRejection>,
) -> Result<Json<PostResponse>, ApiError> {
    move_adjacent(state, headers, id, body, QueueDirection::Earlier).await
}

async fn move_later(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    body: Result<JsonExtractor<MutationRequest>, JsonRejection>,
) -> Result<Json<PostResponse>, ApiError> {
    move_adjacent(state, headers, id, body, QueueDirection::Later).await
}

async fn move_adjacent(
    state: ApiState,
    headers: HeaderMap,
    id: Uuid,
    body: Result<JsonExtractor<MutationRequest>, JsonRejection>,
    direction: QueueDirection,
) -> Result<Json<PostResponse>, ApiError> {
    authorize(&state.api_token, &headers).await?;
    let JsonExtractor(payload) =
        body.map_err(|rejection| map_json_rejection(rejection, &headers))?;
    validate_expected_revision(payload.expected_revision, &headers)?;
    let post = state
        .publisher
        .move_adjacent(id, direction, payload.expected_revision)
        .await
        .map_err(|error| map_publisher_error(error, &headers))?;
    Ok(Json(PostResponse::from_post(&post)))
}

async fn set_slot(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    body: Result<JsonExtractor<SetSlotRequest>, JsonRejection>,
) -> Result<Json<PostResponse>, ApiError> {
    authorize(&state.api_token, &headers).await?;
    let JsonExtractor(payload) =
        body.map_err(|rejection| map_json_rejection(rejection, &headers))?;
    validate_expected_revision(payload.expected_revision, &headers)?;
    let post = state
        .publisher
        .set_slot(id, payload.slot, payload.expected_revision)
        .await
        .map_err(|error| map_publisher_error(error, &headers))?;
    Ok(Json(PostResponse::from_post(&post)))
}

async fn cancel_post(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    body: Result<JsonExtractor<MutationRequest>, JsonRejection>,
) -> Result<Json<PostResponse>, ApiError> {
    authorize(&state.api_token, &headers).await?;
    let JsonExtractor(payload) =
        body.map_err(|rejection| map_json_rejection(rejection, &headers))?;
    validate_expected_revision(payload.expected_revision, &headers)?;
    let post = state
        .publisher
        .cancel_post(id, payload.expected_revision)
        .await
        .map_err(|error| map_publisher_error(error, &headers))?;
    Ok(Json(PostResponse::from_post(&post)))
}

fn require_publishable(
    status: &MediaStatus,
    storage_state: MediaStorageState,
    headers: &HeaderMap,
) -> Result<(), ApiError> {
    if *status != MediaStatus::Active || storage_state != MediaStorageState::Ready {
        return Err(ApiError::conflict(
            "media_not_publishable",
            "The media item is not ready for publication",
            headers,
        ));
    }
    Ok(())
}

fn validate_expected_revision(expected_revision: i64, headers: &HeaderMap) -> Result<(), ApiError> {
    if expected_revision < 0 {
        return Err(ApiError::bad_request(
            "invalid_expected_revision",
            "expected_revision must be non-negative",
            headers,
        ));
    }
    Ok(())
}

fn post_request_hash(payload: &CreatePostRequest) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(payload.media_id.as_bytes());
    hasher.update(payload.channel_id.as_bytes());
    if let Some(value) = &payload.caption {
        hasher.update(value.as_bytes());
    }
    if let Some(value) = &payload.parse_mode {
        hasher.update(value.as_bytes());
    }
    hasher.finalize().to_vec()
}

fn idempotency_key(headers: &HeaderMap) -> Result<String, ApiError> {
    let value = required_header(headers, "idempotency-key")?.trim();
    if value.is_empty() || value.chars().count() > 255 {
        return Err(ApiError::bad_request(
            "invalid_idempotency_key",
            "The Idempotency-Key header must be between 1 and 255 characters",
            headers,
        ));
    }
    Ok(value.to_owned())
}

fn normalize_caption(
    caption: Option<String>,
    parse_mode: Option<&str>,
    headers: &HeaderMap,
) -> Result<Option<String>, ApiError> {
    let caption = caption.map(|value| value.trim().to_owned()).filter(|value| !value.is_empty());
    validate_caption(caption.as_deref(), parse_mode, headers)?;
    Ok(caption)
}

fn normalize_parse_mode(
    parse_mode: Option<String>,
    headers: &HeaderMap,
) -> Result<Option<String>, ApiError> {
    let Some(value) = parse_mode else { return Ok(None) };
    let value = value.trim().to_owned();
    if !matches!(value.as_str(), "HTML" | "MarkdownV2") {
        return Err(ApiError::bad_request(
            "invalid_parse_mode",
            "The parse mode must be HTML or MarkdownV2",
            headers,
        ));
    }
    Ok(Some(value))
}

fn parse_time(value: &str, headers: &HeaderMap) -> Result<Time, ApiError> {
    let description =
        time::format_description::parse_borrowed::<1>("[hour]:[minute]").map_err(|_| {
            ApiError::bad_request(
                "invalid_channel_window",
                "The channel time must use HH:MM",
                headers,
            )
        })?;
    Time::parse(value, &description).map_err(|_| {
        ApiError::bad_request("invalid_channel_window", "The channel time must use HH:MM", headers)
    })
}

fn validate_caption(
    caption: Option<&str>,
    parse_mode: Option<&str>,
    headers: &HeaderMap,
) -> Result<(), ApiError> {
    if let Some(caption) = caption
        && (caption.chars().count() > MAX_CAPTION_LENGTH
            || caption.contains('\0')
            || caption.chars().any(|character| {
                character.is_control() && !matches!(character, '\n' | '\r' | '\t')
            }))
    {
        return Err(ApiError::bad_request(
            "invalid_caption",
            "The caption is invalid or too long",
            headers,
        ));
    }
    if let Some(parse_mode) = parse_mode
        && !matches!(parse_mode, "HTML" | "MarkdownV2")
    {
        return Err(ApiError::bad_request(
            "invalid_parse_mode",
            "The parse mode is invalid",
            headers,
        ));
    }
    Ok(())
}

fn map_json_rejection(rejection: JsonRejection, headers: &HeaderMap) -> ApiError {
    if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
        ApiError::payload_too_large(headers)
    } else {
        ApiError::bad_request("invalid_json", "The request body must be valid JSON", headers)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreatePostRequest {
    media_id: Uuid,
    channel_id: Uuid,
    #[serde(default)]
    caption: Option<String>,
    #[serde(default)]
    parse_mode: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdatePostRequest {
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_patch_field")]
    caption: PatchField<String>,
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_patch_field")]
    parse_mode: PatchField<String>,
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_patch_field")]
    disable_notification: PatchField<bool>,
    #[serde(default)]
    #[serde(with = "time::serde::rfc3339::option")]
    expected_updated_at: Option<OffsetDateTime>,
    expected_revision: i64,
}

#[derive(Debug, Default)]
enum PatchField<T> {
    #[default]
    Unset,
    Set(Option<T>),
}

impl<T> PatchField<T> {
    fn into_option(self) -> Option<Option<T>> {
        match self {
            Self::Unset => None,
            Self::Set(value) => Some(value),
        }
    }
}

fn deserialize_patch_field<'de, D, T>(deserializer: D) -> Result<PatchField<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(PatchField::Set(Option::<T>::deserialize(deserializer)?))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScheduleRequest {
    #[serde(default)]
    #[serde(with = "time::serde::rfc3339::option")]
    publish_at: Option<OffsetDateTime>,
    expected_revision: i64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MutationRequest {
    expected_revision: i64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SetSlotRequest {
    #[serde(with = "time::serde::rfc3339")]
    slot: OffsetDateTime,
    expected_revision: i64,
}

#[derive(Debug, Serialize)]
struct PostResponse {
    id: Uuid,
    media_id: Uuid,
    channel_id: Uuid,
    caption: Option<String>,
    parse_mode: Option<String>,
    disable_notification: bool,
    status: PostState,
    #[serde(with = "time::serde::rfc3339")]
    scheduled_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    cadence_slot_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339")]
    created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    updated_at: OffsetDateTime,
    revision: i64,
}

impl PostResponse {
    fn from_post(post: &Post) -> Self {
        Self {
            id: post.id,
            media_id: post.media_id,
            channel_id: post.channel_id,
            caption: post.caption.clone(),
            parse_mode: post.parse_mode.clone(),
            disable_notification: post.disable_notification,
            status: post.state,
            scheduled_at: post.scheduled_at,
            cadence_slot_at: post.cadence_slot_at,
            created_at: post.created_at,
            updated_at: post.updated_at,
            revision: post.revision,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateChannelRequest {
    name: String,
    telegram_chat_id: i64,
    #[serde(default)]
    time_zone: Option<String>,
    #[serde(default)]
    window_start: Option<String>,
    #[serde(default)]
    window_end: Option<String>,
    #[serde(default)]
    interval_minutes: Option<i32>,
    #[serde(default)]
    default_parse_mode: Option<String>,
    #[serde(default)]
    default_disable_notification: bool,
}

#[derive(Debug, Serialize)]
struct ChannelListResponse {
    items: Vec<ChannelResponse>,
}

#[derive(Debug, Serialize)]
struct ChannelResponse {
    id: Uuid,
    name: String,
    telegram_chat_id: i64,
    is_enabled: bool,
    time_zone: String,
    window_start: String,
    window_end: String,
    interval_minutes: i32,
    default_parse_mode: Option<String>,
    default_disable_notification: bool,
    #[serde(with = "time::serde::rfc3339")]
    created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    updated_at: OffsetDateTime,
}

impl ChannelResponse {
    fn from_channel(channel: &Channel) -> Self {
        Self {
            id: channel.id,
            name: channel.name.clone(),
            telegram_chat_id: channel.telegram_chat_id,
            is_enabled: channel.is_enabled,
            time_zone: channel.time_zone.clone(),
            window_start: channel.window_start.to_string(),
            window_end: channel.window_end.to_string(),
            interval_minutes: channel.interval_minutes,
            default_parse_mode: channel.default_parse_mode.clone(),
            default_disable_notification: channel.default_disable_notification,
            created_at: channel.created_at,
            updated_at: channel.updated_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_time_parser_accepts_minutes() {
        let headers = HeaderMap::new();
        assert_eq!(parse_time("08:30", &headers).expect("valid time").hour(), 8);
    }

    #[test]
    fn caption_normalization_is_idempotent_and_rejects_control_input() {
        let headers = HeaderMap::new();
        let normalized = normalize_caption(Some("  hello  ".to_owned()), None, &headers)
            .expect("caption should be valid");
        assert_eq!(normalized.as_deref(), Some("hello"));
        assert!(normalize_caption(Some("bad\0caption".to_owned()), None, &headers).is_err());
    }

    #[test]
    fn update_payload_can_explicitly_clear_caption() {
        let request: UpdatePostRequest =
            serde_json::from_str(r#"{"caption":null,"expected_revision":0}"#)
                .expect("null caption is valid");
        assert!(matches!(request.caption, PatchField::Set(None)));
    }
}
