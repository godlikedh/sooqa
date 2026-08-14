use axum::{
    Json, Router,
    body::Body,
    extract::{Json as JsonExtractor, Path, Query, State, rejection::JsonRejection},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::Response,
    routing::{delete, get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sooqa_library::{
    MediaCursor, MediaDetails, MediaKind, MediaLookup, MediaPage, MediaSearchQuery, MediaStatus,
    MediaSummary, MediaUpdate, NewTag,
};
use time::OffsetDateTime;
use url::Url;
use uuid::Uuid;

use super::{ApiError, ApiState, authorize, map_library_error};

pub(crate) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/v1/media", get(search_media))
        .route("/api/v1/media/{id}", get(get_media).patch(update_media))
        .route("/api/v1/media/{id}/preview", get(get_preview))
        .route("/api/v1/media/{id}/caption-sync/retry", post(retry_caption_sync))
        .route("/api/v1/media/{id}/archive", post(archive_media))
        .route("/api/v1/media/{id}/tags", post(add_tag))
        .route("/api/v1/media/{id}/tags/{tag}", delete(remove_tag))
}

async fn search_media(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<SearchParams>,
) -> Result<Json<MediaPageResponse>, ApiError> {
    authorize(&state.api_token, &headers).await?;
    let mut params = params;
    let lookup_input = take_lookup_input(&mut params, &headers)?;
    let query = params.into_domain(&headers)?;
    let page = if let Some(lookup_input) = lookup_input {
        state
            .library
            .lookup_media(parse_media_lookup(&lookup_input, &headers)?, query.limit, query.cursor)
            .await
            .map_err(|error| map_library_error(error, &headers))?
    } else {
        state
            .library
            .search_media(query)
            .await
            .map_err(|error| map_library_error(error, &headers))?
    };
    Ok(Json(MediaPageResponse::from_page(&page)))
}

fn take_lookup_input(
    params: &mut SearchParams,
    headers: &HeaderMap,
) -> Result<Option<String>, ApiError> {
    let lookup_input = params.q.take().filter(|value| !value.trim().is_empty());
    let has_catalogue_filter = params
        .tags
        .as_ref()
        .is_some_and(|value| value.split(',').any(|tag| !tag.trim().is_empty()))
        || params.kind.is_some()
        || params.status.is_some();
    if lookup_input.is_some() && has_catalogue_filter {
        return Err(ApiError::bad_request(
            "lookup_filters_not_allowed",
            "Exact media lookup cannot be combined with catalogue filters",
            headers,
        ));
    }
    Ok(lookup_input)
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

async fn get_preview(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Response, ApiError> {
    authorize(&state.api_token, &headers).await?;
    let preview = state
        .library
        .find_media_preview(id)
        .await
        .map_err(|error| map_library_error(error, &headers))?
        .ok_or_else(|| {
            ApiError::not_found(
                "preview_not_available",
                "The media item has no bounded preview",
                &headers,
            )
        })?;
    let etag = format!("\"{}\"", hex_digest(&preview.metadata.sha256));
    if headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == etag)
    {
        return Response::builder()
            .status(StatusCode::NOT_MODIFIED)
            .header(header::ETAG, etag)
            .header(header::CACHE_CONTROL, "private, max-age=3600")
            .body(Body::empty())
            .map_err(|_| ApiError::internal(&headers));
    }
    let content_type = HeaderValue::try_from(preview.metadata.mime_type.as_str())
        .map_err(|_| ApiError::internal(&headers))?;
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_LENGTH, preview.bytes.len())
        .header(header::CACHE_CONTROL, "private, max-age=3600")
        .header(header::ETAG, etag)
        .body(Body::from(preview.bytes))
        .map_err(|_| ApiError::internal(&headers))
}

async fn retry_caption_sync(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<MediaResponse>, ApiError> {
    authorize(&state.api_token, &headers).await?;
    state
        .library
        .retry_caption_sync(id)
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

async fn update_media(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    body: Result<JsonExtractor<UpdateMediaRequest>, JsonRejection>,
) -> Result<Json<MediaResponse>, ApiError> {
    authorize(&state.api_token, &headers).await?;
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
    authorize(&state.api_token, &headers).await?;
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
    authorize(&state.api_token, &headers).await?;
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
    authorize(&state.api_token, &headers).await?;
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
                    "The limit must be between 1 and 50",
                    headers,
                )
            })?
            .unwrap_or(50);
        if !(1..=50).contains(&limit) {
            return Err(ApiError::bad_request(
                "invalid_limit",
                "The limit must be between 1 and 50",
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

fn parse_media_lookup(value: &str, headers: &HeaderMap) -> Result<MediaLookup, ApiError> {
    if let Ok(id) = Uuid::parse_str(value.trim()) {
        return Ok(MediaLookup::Identifier(id));
    }
    let url = Url::parse(value.trim()).map_err(|_| {
        ApiError::bad_request(
            "invalid_media_lookup",
            "The media lookup must be a UUID, source URL, or private Telegram storage link",
            headers,
        )
    })?;
    if !url.username().is_empty() || url.password().is_some() {
        return Err(ApiError::bad_request(
            "invalid_media_lookup",
            "The source URL must not include credentials",
            headers,
        ));
    }
    if url.host_str().is_none() || !matches!(url.scheme(), "http" | "https") {
        return Err(ApiError::bad_request(
            "invalid_media_lookup",
            "The media lookup URL must use HTTP or HTTPS and include a host",
            headers,
        ));
    }
    if url.host_str().is_some_and(|host| host.eq_ignore_ascii_case("t.me")) {
        if url.scheme() != "https" || url.query().is_some() || url.fragment().is_some() {
            return Err(ApiError::bad_request(
                "invalid_media_lookup",
                "The Telegram storage link must be an exact HTTPS path",
                headers,
            ));
        }
        let segments = url.path_segments().map(|segments| segments.collect::<Vec<_>>());
        let Some(segments) = segments else {
            return Err(ApiError::bad_request(
                "invalid_media_lookup",
                "The Telegram storage link is invalid",
                headers,
            ));
        };
        if segments.len() == 3 && segments[0] == "c" {
            let internal_id = segments[1].parse::<i64>().map_err(|_| {
                ApiError::bad_request(
                    "invalid_media_lookup",
                    "The Telegram storage link is invalid",
                    headers,
                )
            })?;
            let message_id = segments[2].parse::<i64>().map_err(|_| {
                ApiError::bad_request(
                    "invalid_media_lookup",
                    "The Telegram storage link is invalid",
                    headers,
                )
            })?;
            if internal_id > 0 && message_id > 0 {
                let chat_id = format!("-100{internal_id}").parse::<i64>().map_err(|_| {
                    ApiError::bad_request(
                        "invalid_media_lookup",
                        "The Telegram storage link is invalid",
                        headers,
                    )
                })?;
                return Ok(MediaLookup::StorageMessage { chat_id, message_id });
            }
        }
        return Err(ApiError::bad_request(
            "invalid_media_lookup",
            "The Telegram storage link is invalid",
            headers,
        ));
    }
    let mut normalized = url;
    let scheme = normalized.scheme().to_ascii_lowercase();
    if normalized.scheme() != scheme {
        normalized.set_scheme(&scheme).map_err(|_| {
            ApiError::bad_request("invalid_media_lookup", "The source URL is invalid", headers)
        })?;
    }
    let host = normalized.host_str().unwrap_or_default().to_ascii_lowercase();
    normalized.set_host(Some(&host)).map_err(|_| {
        ApiError::bad_request("invalid_media_lookup", "The source URL is invalid", headers)
    })?;
    if (scheme == "http" && normalized.port() == Some(80))
        || (scheme == "https" && normalized.port() == Some(443))
    {
        normalized.set_port(None).map_err(|_| {
            ApiError::bad_request("invalid_media_lookup", "The source URL is invalid", headers)
        })?;
    }
    normalized.set_fragment(None);
    let query_pairs = normalized
        .query_pairs()
        .filter(|(name, _)| !is_tracking_parameter(name))
        .map(|(name, value)| (name.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    if query_pairs.is_empty() {
        normalized.set_query(None);
    } else {
        let mut query = normalized.query_pairs_mut();
        query.clear();
        query.extend_pairs(query_pairs.iter().map(|(name, value)| (&**name, &**value)));
    }
    if matches!(host.as_str(), "2ch.org" | "2ch.su" | "2ch.life") {
        let variants = ["2ch.org", "2ch.su", "2ch.life"]
            .into_iter()
            .map(|mirror| {
                let mut variant = normalized.clone();
                variant.set_host(Some(mirror)).expect("known mirror host is valid");
                variant.to_string()
            })
            .collect();
        Ok(MediaLookup::SourceUrls(variants))
    } else {
        Ok(MediaLookup::SourceUrls(vec![normalized.to_string()]))
    }
}

fn is_tracking_parameter(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name.starts_with("utm_")
        || matches!(name.as_str(), "fbclid" | "gclid" | "dclid" | "msclkid" | "mc_cid" | "mc_eid")
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
    storage_url: Option<String>,
    preview: Option<MediaPreviewResponse>,
    caption_sync: CaptionSyncResponse,
    source_url: Option<String>,
    source_original_url: Option<String>,
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

#[derive(Debug, Serialize)]
struct MediaPreviewResponse {
    url: String,
    mime_type: String,
    width: u32,
    height: u32,
    size_bytes: u32,
    etag: String,
}

#[derive(Debug, Serialize)]
struct CaptionSyncResponse {
    generation: i32,
    state: sooqa_library::CaptionSyncState,
    error: Option<String>,
}

impl MediaResponse {
    fn from_summary(summary: &MediaSummary) -> Self {
        Self::from_media(
            &summary.media,
            &summary.tags,
            summary.source_url.clone(),
            summary.source_original_url.clone(),
            summary.source_metadata.clone(),
            summary.storage_url.clone(),
        )
    }

    fn from_details(details: &MediaDetails) -> Self {
        Self::from_media(
            &details.media,
            &details.tags,
            details.source.as_ref().and_then(|source| source.normalized_url.clone()),
            details.source.as_ref().and_then(|source| source.original_url.clone()),
            details.source.as_ref().map(|source| source.metadata.clone()),
            details.storage_url.clone(),
        )
    }

    fn from_media(
        media: &sooqa_library::Media,
        tags: &[sooqa_library::Tag],
        source_url: Option<String>,
        source_original_url: Option<String>,
        source_metadata: Option<Value>,
        storage_url: Option<String>,
    ) -> Self {
        Self {
            id: media.id,
            kind: media.kind,
            status: media.status,
            title: media.title.clone(),
            description: media.description.clone(),
            notes: media.notes.clone(),
            storage_state: media.storage_state.as_str().to_owned(),
            storage_url,
            preview: media.preview.as_ref().map(|preview| MediaPreviewResponse {
                url: format!("/api/v1/media/{}/preview", media.id),
                mime_type: preview.mime_type.clone(),
                width: preview.width,
                height: preview.height,
                size_bytes: preview.size_bytes,
                etag: format!("\"{}\"", hex_digest(&preview.sha256)),
            }),
            caption_sync: CaptionSyncResponse {
                generation: media.caption_sync_generation,
                state: media.caption_sync_state,
                error: media.caption_sync_error.clone(),
            },
            source_url,
            source_original_url,
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
        assert_eq!(query.limit, 50);
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
    fn exact_media_lookup_rejects_catalogue_filters() {
        let headers = HeaderMap::new();
        let mut params = SearchParams {
            q: Some(Uuid::from_u128(7).to_string()),
            kind: Some("video".to_owned()),
            ..SearchParams::default()
        };
        let error = take_lookup_input(&mut params, &headers)
            .expect_err("exact lookup and catalogue filters must not be combined");
        assert_eq!(error.code, "lookup_filters_not_allowed");
    }

    #[test]
    fn media_lookup_parses_private_telegram_storage_links() {
        let headers = HeaderMap::new();
        assert_eq!(
            parse_media_lookup("https://t.me/c/3971341583/57", &headers).unwrap(),
            MediaLookup::StorageMessage { chat_id: -1003971341583, message_id: 57 }
        );
    }

    #[test]
    fn media_lookup_accepts_http_source_urls() {
        let headers = HeaderMap::new();
        assert_eq!(
            parse_media_lookup("http://example.test/video?id=7&utm_source=test", &headers).unwrap(),
            MediaLookup::SourceUrls(vec!["http://example.test/video?id=7".to_owned()])
        );
    }

    #[test]
    fn media_lookup_rejects_non_exact_telegram_storage_paths() {
        let headers = HeaderMap::new();
        assert!(parse_media_lookup("https://user@t.me/c/3971341583/57", &headers).is_err());
        assert!(parse_media_lookup("https://t.me/c/3971341583/57?preview=1", &headers).is_err());
    }
}
