use axum::{
    Json, Router,
    extract::{Json as JsonExtractor, Path, Query, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode},
    routing::get,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sooqa_library::{
    MediaCursor, MediaDetails, MediaKind, MediaPage, MediaSearchQuery, MediaSummary, MediaUpdate,
};
use time::OffsetDateTime;
use uuid::Uuid;

use super::{ApiError, ApiState, authorize, map_library_error};

pub(crate) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/v1/media", get(search_media))
        .route("/api/v1/media/{id}", get(get_media).patch(update_media))
}

async fn search_media(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<SearchParams>,
) -> Result<Json<MediaPageResponse>, ApiError> {
    authorize(&state.api_token, &headers).await?;
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
    authorize(&state.api_token, &headers).await?;
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
    authorize(&state.api_token, &headers).await?;
    let JsonExtractor(payload) =
        body.map_err(|rejection| map_json_rejection(rejection, &headers))?;
    let description = payload.description.into_present().map_err(|_| {
        ApiError::bad_request(
            "invalid_media_update",
            "The request must contain tags, description, and expected_updated_at",
            &headers,
        )
    })?;
    let update = MediaUpdate {
        description,
        tags: payload.tags,
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

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchParams {
    limit: Option<String>,
    cursor: Option<String>,
}

impl SearchParams {
    fn into_domain(self, headers: &HeaderMap) -> Result<MediaSearchQuery, ApiError> {
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
        Ok(MediaSearchQuery { limit, cursor })
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
#[serde(deny_unknown_fields)]
struct UpdateMediaRequest {
    tags: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_required_nullable")]
    description: RequiredNullable<String>,
    #[serde(with = "time::serde::rfc3339")]
    expected_updated_at: OffsetDateTime,
}

#[derive(Debug, Default)]
enum RequiredNullable<T> {
    #[default]
    Missing,
    Present(Option<T>),
}

impl<T> RequiredNullable<T> {
    fn into_present(self) -> Result<Option<T>, ()> {
        match self {
            Self::Missing => Err(()),
            Self::Present(value) => Ok(value),
        }
    }
}

fn deserialize_required_nullable<'de, D, T>(
    deserializer: D,
) -> Result<RequiredNullable<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(RequiredNullable::Present(Option::<T>::deserialize(deserializer)?))
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
    title: Option<String>,
    description: Option<String>,
    tags: Vec<String>,
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
    #[serde(with = "time::serde::rfc3339")]
    created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    updated_at: OffsetDateTime,
}

impl MediaResponse {
    fn from_summary(summary: &MediaSummary) -> Self {
        Self::from_media(
            &summary.media,
            summary.source_url.clone(),
            summary.source_metadata.clone(),
        )
    }

    fn from_details(details: &MediaDetails) -> Self {
        Self::from_media(
            &details.media,
            details.source.as_ref().and_then(|source| source.normalized_url.clone()),
            details.source.as_ref().map(|source| source.metadata.clone()),
        )
    }

    fn from_media(
        media: &sooqa_library::Media,
        source_url: Option<String>,
        source_metadata: Option<Value>,
    ) -> Self {
        Self {
            id: media.id,
            kind: media.kind,
            title: media.title.clone(),
            description: media.description.clone(),
            tags: media.tags.clone(),
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
            created_at: media.created_at,
            updated_at: media.updated_at,
        }
    }
}

fn hex_digest(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
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
    fn search_defaults_to_bounded_cursor_list() {
        let headers = HeaderMap::new();
        let query = SearchParams { limit: None, cursor: None }
            .into_domain(&headers)
            .expect("default query should be valid");
        assert_eq!(query.limit, 20);
        assert!(query.cursor.is_none());
    }

    #[test]
    fn invalid_search_limit_is_rejected_before_database_access() {
        let headers = HeaderMap::new();
        let error = SearchParams { limit: Some("101".to_owned()), ..SearchParams::default() }
            .into_domain(&headers)
            .expect_err("limit 101 must be rejected");
        assert_eq!(error.code, "invalid_limit");
    }

    #[test]
    fn media_update_requires_complete_metadata_and_accepts_null_description() {
        let request = serde_json::from_str::<UpdateMediaRequest>(
            r#"{"tags":[],"description":null,"expected_updated_at":"1970-01-01T00:00:42Z"}"#,
        )
        .expect("complete media update should deserialize");
        assert_eq!(request.description.into_present(), Ok(None));
        assert!(
            serde_json::from_str::<UpdateMediaRequest>(
                r#"{"tags":[],"expected_updated_at":"1970-01-01T00:00:42Z"}"#
            )
            .expect("missing nullable fields are represented for handler validation")
            .description
            .into_present()
            .is_err()
        );
    }
}
