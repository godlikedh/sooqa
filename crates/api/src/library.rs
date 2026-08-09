use axum::{
    Json, Router,
    extract::{Json as JsonExtractor, Path, Query, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode},
    routing::{delete, get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sooqa_library::{
    MediaCursor, MediaDetails, MediaKind, MediaPage, MediaSearchQuery, MediaStatus, MediaSummary,
    MediaUpdate, NewTag,
};
use time::OffsetDateTime;
use uuid::Uuid;

use super::{ApiError, ApiState, authorize, map_library_error};

pub(crate) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/v1/media", get(search_media))
        .route("/api/v1/media/{id}", get(get_media).patch(update_media))
        .route("/api/v1/media/{id}/archive", post(archive_media))
        .route("/api/v1/media/{id}/tags", post(add_tag))
        .route("/api/v1/media/{id}/tags/{tag}", delete(remove_tag))
}

async fn search_media(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<SearchParams>,
) -> Result<Json<MediaPageResponse>, ApiError> {
    authorize(&state.api_token, &headers, "media:read").await?;
    let query = params.into_domain(&headers)?;
    let page = state
        .library
        .search_media(query)
        .await
        .map_err(|error| map_library_error(error, &headers))?;
    Ok(Json(MediaPageResponse::from_page(&page)))
}

async fn get_media(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<MediaResponse>, ApiError> {
    authorize(&state.api_token, &headers, "media:read").await?;
    let media = state
        .library
        .find_media_details(id)
        .await
        .map_err(|error| map_library_error(error, &headers))?
        .ok_or_else(|| {
            ApiError::not_found("media_not_found", "The media item was not found", &headers)
        })?;
    Ok(Json(MediaResponse::from_details(&media)))
}

async fn update_media(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    body: Result<JsonExtractor<UpdateMediaRequest>, JsonRejection>,
) -> Result<Json<MediaResponse>, ApiError> {
    authorize(&state.api_token, &headers, "media:write").await?;
    let JsonExtractor(payload) =
        body.map_err(|rejection| map_json_rejection(rejection, &headers))?;
    let update = MediaUpdate {
        title: payload.title,
        description: payload.description,
        notes: payload.notes,
        expected_updated_at: payload.expected_updated_at,
    };
    state
        .library
        .update_media(id, update)
        .await
        .map_err(|error| map_library_error(error, &headers))?;
    let media = state
        .library
        .find_media_details(id)
        .await
        .map_err(|error| map_library_error(error, &headers))?
        .ok_or_else(|| {
            ApiError::not_found("media_not_found", "The media item was not found", &headers)
        })?;
    Ok(Json(MediaResponse::from_details(&media)))
}

async fn archive_media(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<MediaResponse>, ApiError> {
    authorize(&state.api_token, &headers, "media:write").await?;
    state.library.archive_media(id).await.map_err(|error| map_library_error(error, &headers))?;
    let media = state
        .library
        .find_media_details(id)
        .await
        .map_err(|error| map_library_error(error, &headers))?
        .ok_or_else(|| {
            ApiError::not_found("media_not_found", "The media item was not found", &headers)
        })?;
    Ok(Json(MediaResponse::from_details(&media)))
}

async fn add_tag(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    body: Result<JsonExtractor<TagRequest>, JsonRejection>,
) -> Result<Json<TagResponse>, ApiError> {
    authorize(&state.api_token, &headers, "media:write").await?;
    let JsonExtractor(payload) =
        body.map_err(|rejection| map_json_rejection(rejection, &headers))?;
    let tag = NewTag::try_new(payload.tag)
        .map_err(|_| ApiError::bad_request("invalid_tag", "The tag is invalid", &headers))?;
    let tag =
        state.library.add_tag(id, tag).await.map_err(|error| map_library_error(error, &headers))?;
    Ok(Json(TagResponse::from_tag(&tag)))
}

async fn remove_tag(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path((id, tag)): Path<(Uuid, String)>,
) -> Result<StatusCode, ApiError> {
    authorize(&state.api_token, &headers, "media:write").await?;
    let tag = NewTag::try_new(tag)
        .map_err(|_| ApiError::bad_request("invalid_tag", "The tag is invalid", &headers))?;
    state
        .library
        .remove_tag(id, &tag.normalized_name)
        .await
        .map_err(|error| map_library_error(error, &headers))?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Default, Deserialize)]
struct SearchParams {
    q: Option<String>,
    tags: Option<String>,
    kind: Option<String>,
    status: Option<String>,
    limit: Option<String>,
    cursor: Option<String>,
}

impl SearchParams {
    fn into_domain(self, headers: &HeaderMap) -> Result<MediaSearchQuery, ApiError> {
        let text = self.q.and_then(|value| {
            let value = value.trim().to_owned();
            (!value.is_empty()).then_some(value)
        });
        let tags = self
            .tags
            .unwrap_or_default()
            .split(',')
            .filter(|value| !value.trim().is_empty())
            .map(|value| {
                NewTag::try_new(value).map(|tag| tag.normalized_name).map_err(|_| {
                    ApiError::bad_request("invalid_tag", "The tag filter is invalid", headers)
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let kind = self.kind.as_deref().map(MediaKind::try_from).transpose().map_err(|_| {
            ApiError::bad_request("invalid_kind", "The kind filter is invalid", headers)
        })?;
        let status =
            self.status.as_deref().map(MediaStatus::try_from).transpose().map_err(|_| {
                ApiError::bad_request("invalid_status", "The status filter is invalid", headers)
            })?;
        let limit = self
            .limit
            .as_deref()
            .map(str::parse::<u32>)
            .transpose()
            .map_err(|_| {
                ApiError::bad_request(
                    "invalid_limit",
                    "The limit must be between 1 and 100",
                    headers,
                )
            })?
            .unwrap_or(20);
        if !(1..=100).contains(&limit) {
            return Err(ApiError::bad_request(
                "invalid_limit",
                "The limit must be between 1 and 100",
                headers,
            ));
        }
        let cursor = self.cursor.as_deref().map(decode_cursor).transpose().map_err(|_| {
            ApiError::bad_request("invalid_cursor", "The cursor is invalid", headers)
        })?;
        Ok(MediaSearchQuery {
            text,
            tags,
            kind,
            status: status.or(Some(MediaStatus::Active)),
            limit,
            cursor,
        })
    }
}

fn encode_cursor(cursor: &MediaCursor) -> String {
    format!("{}:{}", cursor.updated_at.unix_timestamp_nanos(), cursor.id)
}

fn decode_cursor(value: &str) -> Result<MediaCursor, ()> {
    let (timestamp, id) = value.split_once(':').ok_or(())?;
    let timestamp = timestamp.parse::<i128>().map_err(|_| ())?;
    let updated_at = OffsetDateTime::from_unix_timestamp_nanos(timestamp).map_err(|_| ())?;
    let id = Uuid::parse_str(id).map_err(|_| ())?;
    Ok(MediaCursor { updated_at, id })
}

#[derive(Debug, Deserialize)]
struct UpdateMediaRequest {
    #[serde(default)]
    title: Option<Option<String>>,
    #[serde(default)]
    description: Option<Option<String>>,
    #[serde(default)]
    notes: Option<Option<String>>,
    #[serde(default)]
    #[serde(with = "time::serde::rfc3339::option")]
    expected_updated_at: Option<OffsetDateTime>,
}

#[derive(Debug, Deserialize)]
struct TagRequest {
    tag: String,
}

#[derive(Debug, Serialize)]
struct MediaPageResponse {
    items: Vec<MediaResponse>,
    next_cursor: Option<String>,
}

impl MediaPageResponse {
    fn from_page(page: &MediaPage) -> Self {
        Self {
            items: page.items.iter().map(MediaResponse::from_summary).collect(),
            next_cursor: page.next_cursor.as_ref().map(encode_cursor),
        }
    }
}

#[derive(Debug, Serialize)]
struct MediaResponse {
    id: Uuid,
    kind: MediaKind,
    status: MediaStatus,
    title: Option<String>,
    description: Option<String>,
    notes: Option<String>,
    storage_state: String,
    source_url: Option<String>,
    source_metadata: Option<Value>,
    mime_type: Option<String>,
    container: Option<String>,
    video_codec: Option<String>,
    audio_codec: Option<String>,
    width: Option<i32>,
    height: Option<i32>,
    duration_ms: Option<u64>,
    bit_rate: Option<u64>,
    file_size_bytes: Option<u64>,
    sha256: Option<String>,
    tags: Vec<TagResponse>,
    #[serde(with = "time::serde::rfc3339")]
    created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    updated_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    archived_at: Option<OffsetDateTime>,
}

impl MediaResponse {
    fn from_summary(summary: &MediaSummary) -> Self {
        Self::from_media(
            &summary.media,
            &summary.tags,
            summary.source_url.clone(),
            summary.source_metadata.clone(),
        )
    }

    fn from_details(details: &MediaDetails) -> Self {
        Self::from_media(
            &details.media,
            &details.tags,
            details.source.as_ref().and_then(|source| source.normalized_url.clone()),
            details.source.as_ref().map(|source| source.metadata.clone()),
        )
    }

    fn from_media(
        media: &sooqa_library::Media,
        tags: &[sooqa_library::Tag],
        source_url: Option<String>,
        source_metadata: Option<Value>,
    ) -> Self {
        Self {
            id: media.id,
            kind: media.kind,
            status: media.status,
            title: media.title.clone(),
            description: media.description.clone(),
            notes: media.notes.clone(),
            storage_state: media.storage_state.as_str().to_owned(),
            source_url,
            source_metadata,
            mime_type: media.mime_type.clone(),
            container: media.container.clone(),
            video_codec: media.video_codec.clone(),
            audio_codec: media.audio_codec.clone(),
            width: media.width,
            height: media.height,
            duration_ms: media.duration_ms,
            bit_rate: media.bit_rate,
            file_size_bytes: media.file_size_bytes,
            sha256: media.sha256.as_deref().map(hex_digest),
            tags: tags.iter().map(TagResponse::from_tag).collect(),
            created_at: media.created_at,
            updated_at: media.updated_at,
            archived_at: media.archived_at,
        }
    }
}

fn hex_digest(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[derive(Debug, Serialize)]
struct TagResponse {
    normalized_name: String,
    display_name: String,
    #[serde(with = "time::serde::rfc3339")]
    created_at: OffsetDateTime,
}

impl TagResponse {
    fn from_tag(tag: &sooqa_library::Tag) -> Self {
        Self {
            normalized_name: tag.normalized_name.clone(),
            display_name: tag.display_name.clone(),
            created_at: tag.created_at,
        }
    }
}

fn map_json_rejection(rejection: JsonRejection, headers: &HeaderMap) -> ApiError {
    if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
        ApiError::payload_too_large(headers)
    } else {
        ApiError::bad_request("invalid_json", "The request body must be valid JSON", headers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_round_trips_with_timestamp_and_uuid() {
        let cursor = MediaCursor {
            updated_at: OffsetDateTime::from_unix_timestamp(42).expect("timestamp is valid"),
            id: Uuid::from_u128(7),
        };
        assert_eq!(decode_cursor(&encode_cursor(&cursor)), Ok(cursor));
    }

    #[test]
    fn search_defaults_to_active_media_and_normalizes_filters() {
        let headers = HeaderMap::new();
        let query = SearchParams {
            q: Some("  cat  ".to_owned()),
            tags: Some(" Rust,  MEDIA ".to_owned()),
            kind: Some("video".to_owned()),
            status: None,
            limit: None,
            cursor: None,
        }
        .into_domain(&headers)
        .expect("filters should be valid");
        assert_eq!(query.text.as_deref(), Some("cat"));
        assert_eq!(query.tags, ["rust", "media"]);
        assert_eq!(query.kind, Some(MediaKind::Video));
        assert_eq!(query.status, Some(MediaStatus::Active));
        assert_eq!(query.limit, 20);
    }

    #[test]
    fn invalid_search_limit_is_rejected_before_database_access() {
        let headers = HeaderMap::new();
        let error = SearchParams { limit: Some("101".to_owned()), ..SearchParams::default() }
            .into_domain(&headers)
            .expect_err("limit 101 must be rejected");
        assert_eq!(error.code, "invalid_limit");
    }
}
