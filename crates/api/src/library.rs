use axum::{
    Json, Router,
    extract::{Json as JsonExtractor, Path, Query, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode},
    routing::{delete, get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sooqa_library::{
    ContentItemUpdate, ContentKind, ContentStatus, LibraryCursor, LibraryItemDetail,
    LibraryItemSummary, LibrarySearchQuery, NewTag,
};
use time::OffsetDateTime;
use uuid::Uuid;

use super::{ApiError, ApiState, authorize, map_library_error};

pub(crate) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/v1/media", get(search_items))
        .route("/api/v1/media/{id}", get(get_item).patch(update_item))
        .route("/api/v1/media/{id}/archive", post(archive_item))
        .route("/api/v1/media/{id}/tags", post(add_tag))
        .route("/api/v1/media/{id}/tags/{tag}", delete(remove_tag))
}

async fn search_items(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<SearchParams>,
) -> Result<Json<LibrarySearchResponse>, ApiError> {
    authorize(&state.api_token, &headers, "media:read").await?;
    let query = params.into_domain(&headers)?;
    let page = state
        .library
        .search_library(query)
        .await
        .map_err(|error| map_library_error(error, &headers))?;
    Ok(Json(LibrarySearchResponse {
        items: page.items.iter().map(MediaResponse::from_summary).collect(),
        next_cursor: page.next_cursor.as_ref().map(encode_cursor),
    }))
}

async fn get_item(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<MediaResponse>, ApiError> {
    authorize(&state.api_token, &headers, "media:read").await?;
    let item = state
        .library
        .find_library_item(id)
        .await
        .map_err(|error| map_library_error(error, &headers))?
        .ok_or_else(|| {
            ApiError::not_found(
                "library_item_not_found",
                "The library item was not found",
                &headers,
            )
        })?;
    Ok(Json(MediaResponse::from_detail(&item)))
}

async fn update_item(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    body: Result<JsonExtractor<UpdateItemRequest>, JsonRejection>,
) -> Result<Json<MediaResponse>, ApiError> {
    authorize(&state.api_token, &headers, "media:write").await?;
    let JsonExtractor(payload) =
        body.map_err(|rejection| map_json_rejection(rejection, &headers))?;
    let update = ContentItemUpdate {
        preferred_title: payload.preferred_title,
        editorial_description: payload.editorial_description,
        notes: payload.notes,
        expected_updated_at: payload.expected_updated_at,
    };
    state
        .library
        .update_content_item(id, update)
        .await
        .map_err(|error| map_library_error(error, &headers))?;
    let item = state
        .library
        .find_library_item(id)
        .await
        .map_err(|error| map_library_error(error, &headers))?
        .ok_or_else(|| {
            ApiError::not_found(
                "library_item_not_found",
                "The library item was not found",
                &headers,
            )
        })?;
    Ok(Json(MediaResponse::from_detail(&item)))
}

async fn archive_item(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<MediaResponse>, ApiError> {
    authorize(&state.api_token, &headers, "media:write").await?;
    state
        .library
        .archive_content_item(id)
        .await
        .map_err(|error| map_library_error(error, &headers))?;
    let item = state
        .library
        .find_library_item(id)
        .await
        .map_err(|error| map_library_error(error, &headers))?
        .ok_or_else(|| {
            ApiError::not_found(
                "library_item_not_found",
                "The library item was not found",
                &headers,
            )
        })?;
    Ok(Json(MediaResponse::from_detail(&item)))
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

fn map_json_rejection(rejection: JsonRejection, headers: &HeaderMap) -> ApiError {
    if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
        ApiError::payload_too_large(headers)
    } else {
        ApiError::bad_request("invalid_json", "The request body must be valid JSON", headers)
    }
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
    fn into_domain(self, headers: &HeaderMap) -> Result<LibrarySearchQuery, ApiError> {
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
        let kind = self.kind.as_deref().map(ContentKind::try_from).transpose().map_err(|_| {
            ApiError::bad_request("invalid_kind", "The kind filter is invalid", headers)
        })?;
        let status =
            self.status.as_deref().map(ContentStatus::try_from).transpose().map_err(|_| {
                ApiError::bad_request("invalid_status", "The status filter is invalid", headers)
            })?;
        let limit = self
            .limit
            .as_deref()
            .map(|value| value.parse::<u32>())
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
        Ok(LibrarySearchQuery {
            text,
            tags,
            kind,
            status: status.or(Some(ContentStatus::Active)),
            limit,
            cursor,
        })
    }
}

fn encode_cursor(cursor: &LibraryCursor) -> String {
    format!("{}:{}", cursor.updated_at.unix_timestamp_nanos(), cursor.id)
}

fn decode_cursor(value: &str) -> Result<LibraryCursor, ()> {
    let (timestamp, id) = value.split_once(':').ok_or(())?;
    let timestamp = timestamp.parse::<i128>().map_err(|_| ())?;
    let updated_at = OffsetDateTime::from_unix_timestamp_nanos(timestamp).map_err(|_| ())?;
    let id = Uuid::parse_str(id).map_err(|_| ())?;
    Ok(LibraryCursor { updated_at, id })
}

#[derive(Debug, Deserialize)]
struct UpdateItemRequest {
    #[serde(default)]
    preferred_title: Option<Option<String>>,
    #[serde(default)]
    editorial_description: Option<Option<String>>,
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
struct LibrarySearchResponse {
    items: Vec<MediaResponse>,
    next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
struct MediaResponse {
    id: Uuid,
    kind: ContentKind,
    status: ContentStatus,
    title: Option<String>,
    description: Option<String>,
    notes: Option<String>,
    storage_state: Option<String>,
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
    fn from_summary(item: &LibraryItemSummary) -> Self {
        Self::from_parts(
            &item.content_item,
            item.canonical_asset.as_ref(),
            item.tags.as_slice(),
            &[],
        )
    }

    fn from_detail(item: &LibraryItemDetail) -> Self {
        Self::from_parts(
            &item.content_item,
            item.canonical_asset.as_ref(),
            item.tags.as_slice(),
            item.sources.as_slice(),
        )
    }

    fn from_parts(
        item: &sooqa_library::ContentItem,
        asset: Option<&sooqa_library::MediaAsset>,
        tags: &[sooqa_library::Tag],
        sources: &[sooqa_library::SourceRecord],
    ) -> Self {
        let source = sources.first();
        Self {
            id: item.id,
            kind: item.kind,
            status: item.status,
            title: item.preferred_title.clone(),
            description: item.editorial_description.clone(),
            notes: item.notes.clone(),
            storage_state: asset.map(|asset| {
                match asset.storage_state {
                    sooqa_library::StorageState::Uploaded => "ready",
                    sooqa_library::StorageState::Missing => "missing",
                    sooqa_library::StorageState::Local => "pending_storage",
                }
                .to_owned()
            }),
            source_url: source.and_then(|source| {
                source.normalized_url.clone().or_else(|| source.original_url.clone())
            }),
            source_metadata: source.map(|source| source.metadata_json.clone()),
            mime_type: asset.and_then(|asset| asset.mime_type.clone()),
            container: asset.and_then(|asset| asset.container.clone()),
            video_codec: asset.and_then(|asset| asset.video_codec.clone()),
            audio_codec: asset.and_then(|asset| asset.audio_codec.clone()),
            width: asset.and_then(|asset| asset.width),
            height: asset.and_then(|asset| asset.height),
            duration_ms: asset.and_then(|asset| asset.duration_ms),
            bit_rate: asset.and_then(|asset| asset.bit_rate),
            file_size_bytes: asset.and_then(|asset| asset.file_size_bytes),
            sha256: asset.and_then(|asset| asset.sha256.as_deref().map(hex_digest)),
            tags: tags.iter().map(TagResponse::from_tag).collect(),
            created_at: item.created_at,
            updated_at: item.updated_at,
            archived_at: item.archived_at,
        }
    }
}

fn hex_digest(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[derive(Debug, Serialize)]
struct TagResponse {
    id: Uuid,
    normalized_name: String,
    display_name: String,
    #[serde(with = "time::serde::rfc3339")]
    created_at: OffsetDateTime,
}

impl TagResponse {
    fn from_tag(tag: &sooqa_library::Tag) -> Self {
        Self {
            id: tag.id,
            normalized_name: tag.normalized_name.clone(),
            display_name: tag.display_name.clone(),
            created_at: tag.created_at,
        }
    }
}
