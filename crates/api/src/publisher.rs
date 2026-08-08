use axum::{
    Json, Router,
    extract::{Json as JsonExtractor, Path, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use sooqa_library::{ContentStatus, StorageState};
use sooqa_persistence::{post_draft_create_request_hash, post_draft_update_request_hash};
use sooqa_publisher::{
    NewPostDraft, NewPublicationSchedule, NewTargetChannel, PostDraft, PostDraftStatus,
    PostDraftUpdate, PublicationSchedule, PublicationScheduleScope, TargetChannel,
};
use time::OffsetDateTime;
use uuid::Uuid;

use super::{
    ApiError, ApiState, authorize, map_library_error, map_publisher_error, required_header,
};

const MAX_CAPTION_LENGTH: usize = 1_024;

pub(crate) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/v1/channels", get(list_channels).post(create_channel))
        .route("/api/v1/channels/{id}", get(get_channel))
        .route("/api/v1/posts", post(create_draft))
        .route("/api/v1/posts/{id}", get(get_draft).patch(update_draft))
        .route("/api/v1/posts/{id}/schedule", post(schedule_draft))
        .route("/api/v1/posts/{id}/publish", post(publish_now))
}

async fn list_channels(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Json<ChannelListResponse>, ApiError> {
    authorize(&state.api_token, &headers, "channels:read").await?;
    let channels = state
        .publisher
        .list_target_channels(false)
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
    authorize(&state.api_token, &headers, "channels:write").await?;
    let JsonExtractor(payload) =
        body.map_err(|rejection| map_json_rejection(rejection, &headers))?;
    let mut channel =
        NewTargetChannel::try_new(payload.name, payload.telegram_chat_id).map_err(|_| {
            ApiError::bad_request("invalid_channel", "The channel payload is invalid", &headers)
        })?;
    channel.default_parse_mode = normalize_parse_mode(payload.default_parse_mode, &headers)?;
    channel.default_disable_notification = payload.default_disable_notification;
    let channel = state
        .publisher
        .create_target_channel(channel)
        .await
        .map_err(|error| map_publisher_error(error, &headers))?;
    Ok((StatusCode::CREATED, Json(ChannelResponse::from_channel(&channel))))
}

async fn get_channel(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<ChannelResponse>, ApiError> {
    authorize(&state.api_token, &headers, "channels:read").await?;
    let channel = state
        .publisher
        .find_target_channel(id)
        .await
        .map_err(|error| map_publisher_error(error, &headers))?
        .ok_or_else(|| {
            ApiError::not_found("channel_not_found", "The channel was not found", &headers)
        })?;
    Ok(Json(ChannelResponse::from_channel(&channel)))
}

async fn create_draft(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Result<JsonExtractor<CreateDraftRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<PostResponse>), ApiError> {
    authorize(&state.api_token, &headers, "publisher:write").await?;
    let idempotency_key = idempotency_key(&headers)?;
    let JsonExtractor(payload) =
        body.map_err(|rejection| map_json_rejection(rejection, &headers))?;

    let caption = normalize_caption(payload.caption.clone(), None, &headers)?;
    let requested_parse_mode = normalize_parse_mode(payload.parse_mode.clone(), &headers)?;
    let request_hash = post_draft_create_request_hash(
        payload.media_id,
        payload.channel_id,
        caption.as_deref(),
        requested_parse_mode.as_deref(),
    );
    if let Some(draft) = state
        .publisher
        .replay_post_draft_create(
            &idempotency_key,
            &request_hash,
            payload.media_id,
            payload.channel_id,
            caption.as_deref(),
            requested_parse_mode.as_deref(),
        )
        .await
        .map_err(|error| map_publisher_error(error, &headers))?
    {
        return Ok((StatusCode::CREATED, Json(PostResponse::from_draft(&draft))));
    }

    let item = state
        .library
        .find_library_item(payload.media_id)
        .await
        .map_err(|error| map_library_error(error, &headers))?
        .ok_or_else(|| {
            ApiError::not_found("media_not_found", "The media item was not found", &headers)
        })?;
    let asset_id =
        publishable_asset(&item.content_item.status, item.canonical_asset.as_ref(), &headers)?;
    let target = state
        .publisher
        .find_target_channel(payload.channel_id)
        .await
        .map_err(|error| map_publisher_error(error, &headers))?
        .ok_or_else(|| {
            ApiError::not_found(
                "target_channel_not_found",
                "The target channel was not found",
                &headers,
            )
        })?;
    if !target.is_enabled {
        return Err(ApiError::conflict(
            "target_channel_disabled",
            "The target channel is disabled",
            &headers,
        ));
    }

    let parse_mode = match requested_parse_mode {
        Some(value) => Some(value),
        None => normalize_parse_mode(target.default_parse_mode, &headers)?,
    };
    validate_caption(caption.as_deref(), parse_mode.as_deref(), &headers)?;
    let draft = state
        .publisher
        .create_post_draft_idempotent(
            NewPostDraft {
                content_item_id: payload.media_id,
                asset_id,
                target_channel_id: payload.channel_id,
                caption,
                parse_mode,
            },
            idempotency_key,
            &request_hash,
        )
        .await
        .map_err(|error| map_publisher_error(error, &headers))?
        .draft;

    Ok((StatusCode::CREATED, Json(PostResponse::from_draft(&draft))))
}

async fn get_draft(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
) -> Result<Json<PostResponse>, ApiError> {
    authorize(&state.api_token, &headers, "publisher:read").await?;
    let id = parse_uuid(&raw_id, "post_id", "The post ID must be a UUID", &headers)?;
    let draft = state
        .publisher
        .find_post_draft(id)
        .await
        .map_err(|error| map_publisher_error(error, &headers))?
        .ok_or_else(|| ApiError::not_found("post_not_found", "The post was not found", &headers))?;
    Ok(Json(PostResponse::from_draft(&draft)))
}

async fn update_draft(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
    body: Result<JsonExtractor<UpdateDraftRequest>, JsonRejection>,
) -> Result<Json<PostResponse>, ApiError> {
    authorize(&state.api_token, &headers, "publisher:write").await?;
    let id = parse_uuid(&raw_id, "post_id", "The post ID must be a UUID", &headers)?;
    let idempotency_key = idempotency_key(&headers)?;
    let JsonExtractor(payload) =
        body.map_err(|rejection| map_json_rejection(rejection, &headers))?;
    if payload.caption.is_none() && payload.parse_mode.is_none() && payload.status.is_none() {
        return Err(ApiError::bad_request(
            "empty_update",
            "The request must contain at least one editable field",
            &headers,
        ));
    }

    let status = payload
        .status
        .as_deref()
        .map(PostDraftStatus::try_from)
        .transpose()
        .map_err(|_| {
            ApiError::bad_request("invalid_post_status", "The post status is invalid", &headers)
        })?
        .map(|status| match status {
            PostDraftStatus::Editing | PostDraftStatus::Ready | PostDraftStatus::Cancelled => {
                Ok(status)
            }
            PostDraftStatus::Scheduled | PostDraftStatus::Published => Err(ApiError::bad_request(
                "invalid_post_status",
                "The requested post status is managed by publication",
                &headers,
            )),
        })
        .transpose()?;

    let caption = match payload.caption.clone() {
        Some(value) => Some(normalize_caption(value, None, &headers)?),
        None => None,
    };
    let parse_mode = match payload.parse_mode.clone() {
        Some(value) => Some(normalize_parse_mode(value, &headers)?),
        None => None,
    };
    let update = PostDraftUpdate {
        caption: caption.clone(),
        parse_mode: parse_mode.clone(),
        status,
        expected_updated_at: payload.expected_updated_at,
    };
    let request_hash = post_draft_update_request_hash(id, &update);
    if let Some(draft) = state
        .publisher
        .replay_post_draft_update(&idempotency_key, &request_hash)
        .await
        .map_err(|error| map_publisher_error(error, &headers))?
    {
        return Ok(Json(PostResponse::from_draft(&draft)));
    }

    let current = state
        .publisher
        .find_post_draft(id)
        .await
        .map_err(|error| map_publisher_error(error, &headers))?
        .ok_or_else(|| ApiError::not_found("post_not_found", "The post was not found", &headers))?;
    if !matches!(current.status, PostDraftStatus::Editing | PostDraftStatus::Ready) {
        return Err(ApiError::conflict(
            "invalid_post_state",
            "Only editable posts can be changed",
            &headers,
        ));
    }
    let effective_caption = match &caption {
        Some(value) => value.as_deref(),
        None => current.caption.as_deref(),
    };
    let effective_parse_mode = match &parse_mode {
        Some(value) => value.as_deref(),
        None => current.parse_mode.as_deref(),
    };
    validate_caption(effective_caption, effective_parse_mode, &headers)?;
    if status == Some(PostDraftStatus::Ready) {
        let item = state
            .library
            .find_library_item(current.content_item_id)
            .await
            .map_err(|error| map_library_error(error, &headers))?
            .ok_or_else(|| {
                ApiError::not_found("media_not_found", "The media item was not found", &headers)
            })?;
        publishable_asset(&item.content_item.status, item.canonical_asset.as_ref(), &headers)?;
    }

    let draft = state
        .publisher
        .update_post_draft_idempotent(id, update, idempotency_key)
        .await
        .map_err(|error| map_publisher_error(error, &headers))?;
    Ok(Json(PostResponse::from_draft(&draft)))
}

async fn schedule_draft(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
    body: Result<JsonExtractor<ScheduleRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<PublicationScheduleResponse>), ApiError> {
    authorize(&state.api_token, &headers, "publisher:write").await?;
    let draft_id = parse_uuid(&raw_id, "post_id", "The post ID must be a UUID", &headers)?;
    let idempotency_key = idempotency_key(&headers)?;
    let JsonExtractor(payload) =
        body.map_err(|rejection| map_json_rejection(rejection, &headers))?;
    let schedule = NewPublicationSchedule::try_new(draft_id, payload.publish_at, idempotency_key)
        .map_err(|error| map_publisher_error(error.into(), &headers))?;
    let schedule = state
        .publisher
        .create_publication_schedule(NewPublicationSchedule {
            not_before: payload.not_before,
            not_after: payload.not_after,
            priority: payload.priority,
            cooldown_override: payload.cooldown_override,
            ..schedule
        })
        .await
        .map_err(|error| map_publisher_error(error, &headers))?;
    Ok((StatusCode::ACCEPTED, Json(PublicationScheduleResponse::from_schedule(&schedule))))
}

async fn publish_now(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
) -> Result<(StatusCode, Json<PublicationScheduleResponse>), ApiError> {
    authorize(&state.api_token, &headers, "publisher:write").await?;
    let draft_id = parse_uuid(&raw_id, "post_id", "The post ID must be a UUID", &headers)?;
    let idempotency_key = idempotency_key(&headers)?;
    let schedule = NewPublicationSchedule::try_new(
        draft_id,
        OffsetDateTime::now_utc(),
        idempotency_key.clone(),
    )
    .map_err(|error| map_publisher_error(error.into(), &headers))?;
    let schedule = state
        .publisher
        .create_publication_schedule_with_scope(schedule, PublicationScheduleScope::PublishNow)
        .await
        .map_err(|error| map_publisher_error(error, &headers))?;
    Ok((StatusCode::ACCEPTED, Json(PublicationScheduleResponse::from_schedule(&schedule))))
}

fn publishable_asset(
    status: &ContentStatus,
    asset: Option<&sooqa_library::MediaAsset>,
    headers: &HeaderMap,
) -> Result<Uuid, ApiError> {
    if *status != ContentStatus::Active {
        return Err(ApiError::conflict(
            "media_not_publishable",
            "Only active media can be published",
            headers,
        ));
    }
    let asset = asset.ok_or_else(|| {
        ApiError::conflict(
            "media_not_publishable",
            "The media item is not ready for publication",
            headers,
        )
    })?;
    if asset.storage_state != StorageState::Uploaded {
        return Err(ApiError::conflict(
            "media_not_publishable",
            "The media item is not stored in Telegram yet",
            headers,
        ));
    }
    Ok(asset.id)
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
    let Some(value) = parse_mode else {
        return Ok(None);
    };
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

fn validate_caption(
    caption: Option<&str>,
    parse_mode: Option<&str>,
    headers: &HeaderMap,
) -> Result<(), ApiError> {
    if let Some(caption) = caption {
        if caption.chars().count() > MAX_CAPTION_LENGTH {
            return Err(ApiError::bad_request(
                "caption_too_long",
                "The caption must be at most 1024 characters",
                headers,
            ));
        }
        if caption.contains('\0') {
            return Err(ApiError::bad_request(
                "invalid_caption",
                "The caption contains an invalid character",
                headers,
            ));
        }
    }
    if let Some(parse_mode) = parse_mode
        && !matches!(parse_mode, "HTML" | "MarkdownV2")
    {
        return Err(ApiError::bad_request(
            "invalid_parse_mode",
            "The parse mode must be HTML or MarkdownV2",
            headers,
        ));
    }
    if let (Some(caption), Some(parse_mode)) = (caption, parse_mode) {
        let valid = match parse_mode {
            "HTML" => valid_html_markup(caption),
            "MarkdownV2" => valid_markdown_v2(caption),
            _ => false,
        };
        if !valid {
            return Err(ApiError::bad_request(
                "invalid_caption_markup",
                "The caption contains invalid Telegram markup",
                headers,
            ));
        }
    }
    Ok(())
}

fn valid_html_markup(value: &str) -> bool {
    if !valid_html_entities(value) {
        return false;
    }
    let mut open_tags = Vec::new();
    let mut cursor = 0;
    while let Some(relative_start) = value[cursor..].find('<') {
        let start = cursor + relative_start;
        if value[cursor..start].contains('>') {
            return false;
        }
        let Some(end) = html_tag_end(value, start) else {
            return false;
        };
        let tag = value[start + 1..end].trim();
        if tag.is_empty() || tag.starts_with('!') || tag.ends_with('/') {
            return false;
        }
        if let Some(closing) = tag.strip_prefix('/') {
            if closing.is_empty()
                || closing != closing.trim()
                || !closing
                    .chars()
                    .all(|character| character.is_ascii_alphabetic() || character == '-')
            {
                return false;
            }
            let closing = closing.to_ascii_lowercase();
            if open_tags.pop().as_deref() != Some(closing.as_str()) {
                return false;
            }
        } else {
            let name_end = tag.find(char::is_whitespace).unwrap_or(tag.len());
            let name = &tag[..name_end];
            let attributes = tag[name_end..].trim();
            let name = name.to_ascii_lowercase();
            if name == "code"
                && !attributes.is_empty()
                && open_tags.last().map(String::as_str) != Some("pre")
            {
                return false;
            }
            if !matches!(
                name.as_str(),
                "b" | "strong"
                    | "i"
                    | "em"
                    | "u"
                    | "ins"
                    | "s"
                    | "strike"
                    | "del"
                    | "span"
                    | "tg-spoiler"
                    | "a"
                    | "code"
                    | "pre"
                    | "blockquote"
            ) || !valid_html_attributes(&name, attributes)
            {
                return false;
            }
            open_tags.push(name);
        }
        cursor = end + 1;
    }
    !value[cursor..].contains('>') && open_tags.is_empty()
}

fn html_tag_end(value: &str, start: usize) -> Option<usize> {
    let mut quote = None;
    for (relative, character) in value[start..].char_indices() {
        if let Some(current_quote) = quote {
            if character == current_quote {
                quote = None;
            }
        } else if matches!(character, '\'' | '"') {
            quote = Some(character);
        } else if character == '>' {
            return Some(start + relative);
        }
    }
    None
}

fn valid_html_entities(value: &str) -> bool {
    let mut cursor = 0;
    while let Some(relative_start) = value[cursor..].find('&') {
        let start = cursor + relative_start;
        let entity = &value[start..];
        if let Some(allowed) =
            ["&lt;", "&gt;", "&amp;", "&quot;"].iter().find(|allowed| entity.starts_with(**allowed))
        {
            cursor = start + allowed.len();
            continue;
        }
        let Some(relative_end) = entity.find(';') else {
            return false;
        };
        let numeric = &entity[1..relative_end];
        let digits = numeric.strip_prefix("#x").or_else(|| numeric.strip_prefix("#X"));
        let valid_numeric = match digits {
            Some(digits) => u32::from_str_radix(digits, 16)
                .ok()
                .and_then(char::from_u32)
                .is_some_and(|character| character != '\0'),
            None => numeric
                .strip_prefix('#')
                .and_then(|digits| digits.parse::<u32>().ok())
                .and_then(char::from_u32)
                .is_some_and(|character| character != '\0'),
        };
        if !valid_numeric {
            return false;
        }
        cursor = start + relative_end + 1;
    }
    true
}

fn valid_html_attributes(name: &str, attributes: &str) -> bool {
    match name {
        "a" => {
            let value = attributes
                .strip_prefix("href=\"")
                .and_then(|value| value.strip_suffix('"'))
                .or_else(|| {
                    attributes.strip_prefix("href='").and_then(|value| value.strip_suffix('\''))
                });
            let Some(value) = value else {
                return false;
            };
            !value.is_empty()
                && !value.contains('"')
                && !value.contains('\'')
                && (value.starts_with("http://")
                    || value.starts_with("https://")
                    || value.starts_with("tg://"))
        }
        "span" => attributes == "class=\"tg-spoiler\"" || attributes == "class='tg-spoiler'",
        "code" => attributes.is_empty() || valid_language_attribute(attributes),
        "pre" => attributes.is_empty(),
        _ => attributes.is_empty(),
    }
}

fn valid_language_attribute(attributes: &str) -> bool {
    let value = attributes
        .strip_prefix("class=\"language-")
        .and_then(|value| value.strip_suffix('"'))
        .or_else(|| {
            attributes.strip_prefix("class='language-").and_then(|value| value.strip_suffix('\''))
        });
    value.is_some_and(|value| {
        !value.is_empty() && value.chars().all(|character| !character.is_whitespace())
    })
}

fn valid_markdown_v2(value: &str) -> bool {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Delimiter {
        Asterisk,
        Underscore,
        Tilde,
        Backtick,
        CodeBlock,
        Spoiler,
        LinkText,
        LinkUrl,
    }

    let characters: Vec<char> = value.chars().collect();
    let mut delimiters = Vec::new();
    let mut link_url_allowed = false;
    let mut index = 0;
    while index < characters.len() {
        let character = characters[index];

        if matches!(delimiters.last(), Some(Delimiter::Backtick | Delimiter::CodeBlock)) {
            let code_delimiter = delimiters.last().copied().expect("code delimiter should exist");
            if character == '`' {
                let mut count = 1;
                while index + count < characters.len() && characters[index + count] == '`' {
                    count += 1;
                }
                if (code_delimiter == Delimiter::CodeBlock && count >= 3)
                    || (code_delimiter == Delimiter::Backtick && count == 1)
                {
                    delimiters.pop();
                }
                index += count;
                continue;
            }
            if character == '\\' {
                if index + 1 == characters.len() {
                    return false;
                }
                index += 2;
                continue;
            }
            index += 1;
            continue;
        }

        if delimiters.last() == Some(&Delimiter::LinkUrl) {
            if character == '\\' {
                if index + 1 == characters.len() {
                    return false;
                }
                index += 2;
            } else if character == ')' {
                delimiters.pop();
                index += 1;
            } else {
                index += 1;
            }
            continue;
        }

        if link_url_allowed {
            if character == '(' {
                delimiters.push(Delimiter::LinkUrl);
                link_url_allowed = false;
                index += 1;
                continue;
            }
            return false;
        }

        if character == '\\' {
            if index + 1 == characters.len() {
                return false;
            }
            index += 2;
            continue;
        }
        if character == '`' {
            let mut count = 1;
            while index + count < characters.len() && characters[index + count] == '`' {
                count += 1;
            }
            let delimiter = if count >= 3 { Delimiter::CodeBlock } else { Delimiter::Backtick };
            toggle_delimiter(&mut delimiters, delimiter);
            index += count;
            continue;
        }
        if character == '|' && characters.get(index + 1) == Some(&'|') {
            toggle_delimiter(&mut delimiters, Delimiter::Spoiler);
            index += 2;
            continue;
        }
        let delimiter = match character {
            '*' => Some(Delimiter::Asterisk),
            '_' => Some(Delimiter::Underscore),
            '~' => Some(Delimiter::Tilde),
            '[' => {
                delimiters.push(Delimiter::LinkText);
                None
            }
            ']' => {
                if delimiters.pop() != Some(Delimiter::LinkText) {
                    return false;
                }
                link_url_allowed = true;
                None
            }
            ')' | '>' | '#' | '+' | '-' | '=' | '|' | '{' | '}' | '.' | '!' => return false,
            _ => None,
        };
        if let Some(delimiter) = delimiter {
            toggle_delimiter(&mut delimiters, delimiter);
        }
        index += 1;
    }
    !link_url_allowed && delimiters.is_empty()
}

fn toggle_delimiter<T: PartialEq>(delimiters: &mut Vec<T>, delimiter: T) {
    if delimiters.last() == Some(&delimiter) {
        delimiters.pop();
    } else {
        delimiters.push(delimiter);
    }
}

fn parse_uuid(
    raw: &str,
    code: &'static str,
    message: &'static str,
    headers: &HeaderMap,
) -> Result<Uuid, ApiError> {
    Uuid::parse_str(raw).map_err(|_| ApiError::bad_request(code, message, headers))
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
struct CreateDraftRequest {
    media_id: Uuid,
    channel_id: Uuid,
    #[serde(default)]
    caption: Option<String>,
    #[serde(default)]
    parse_mode: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateDraftRequest {
    #[serde(default)]
    caption: Option<Option<String>>,
    #[serde(default)]
    parse_mode: Option<Option<String>>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    #[serde(with = "time::serde::rfc3339::option")]
    expected_updated_at: Option<OffsetDateTime>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScheduleRequest {
    #[serde(with = "time::serde::rfc3339")]
    publish_at: OffsetDateTime,
    #[serde(default)]
    #[serde(with = "time::serde::rfc3339::option")]
    not_before: Option<OffsetDateTime>,
    #[serde(default)]
    #[serde(with = "time::serde::rfc3339::option")]
    not_after: Option<OffsetDateTime>,
    #[serde(default)]
    priority: i32,
    #[serde(default)]
    cooldown_override: Option<bool>,
}

#[derive(Debug, Serialize)]
struct PostResponse {
    id: Uuid,
    media_id: Uuid,
    channel_id: Uuid,
    caption: Option<String>,
    parse_mode: Option<String>,
    status: PostDraftStatus,
    #[serde(with = "time::serde::rfc3339")]
    created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    updated_at: OffsetDateTime,
}

impl PostResponse {
    fn from_draft(draft: &PostDraft) -> Self {
        Self {
            id: draft.id,
            media_id: draft.content_item_id,
            channel_id: draft.target_channel_id,
            caption: draft.caption.clone(),
            parse_mode: draft.parse_mode.clone(),
            status: draft.status,
            created_at: draft.created_at,
            updated_at: draft.updated_at,
        }
    }
}

#[derive(Debug, Serialize)]
struct PublicationScheduleResponse {
    id: Uuid,
    post_id: Uuid,
    status: sooqa_publisher::PublicationScheduleStatus,
    #[serde(with = "time::serde::rfc3339")]
    publish_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    not_before: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    not_after: Option<OffsetDateTime>,
    priority: i32,
    cooldown_override: Option<bool>,
    idempotency_key: String,
    #[serde(with = "time::serde::rfc3339")]
    created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    updated_at: OffsetDateTime,
}

impl PublicationScheduleResponse {
    fn from_schedule(schedule: &PublicationSchedule) -> Self {
        Self {
            id: schedule.id,
            post_id: schedule.post_draft_id,
            status: schedule.status,
            publish_at: schedule.publish_at,
            not_before: schedule.not_before,
            not_after: schedule.not_after,
            priority: schedule.priority,
            cooldown_override: schedule.cooldown_override,
            idempotency_key: schedule.idempotency_key.clone(),
            created_at: schedule.created_at,
            updated_at: schedule.updated_at,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateChannelRequest {
    name: String,
    telegram_chat_id: i64,
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
    default_parse_mode: Option<String>,
    default_disable_notification: bool,
    #[serde(with = "time::serde::rfc3339")]
    created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    updated_at: OffsetDateTime,
}

impl ChannelResponse {
    fn from_channel(channel: &TargetChannel) -> Self {
        Self {
            id: channel.id,
            name: channel.name.clone(),
            telegram_chat_id: channel.telegram_chat_id,
            is_enabled: channel.is_enabled,
            default_parse_mode: channel.default_parse_mode.clone(),
            default_disable_notification: channel.default_disable_notification,
            created_at: channel.created_at,
            updated_at: channel.updated_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{valid_html_markup, valid_markdown_v2};

    #[test]
    fn validates_balanced_telegram_html() {
        assert!(valid_html_markup("<b>bold</b> <a href=\"https://example.test\">link</a>"));
        assert!(valid_html_markup("<pre><code class=\"language-rust\">*literal*</code></pre>"));
        assert!(valid_html_markup("escaped &lt;tag&gt; &amp; &quot;quote&quot; &#x3c; &#62;"));
        assert!(!valid_html_markup("<b>unclosed"));
        assert!(!valid_html_markup("<b>mismatched</i>"));
        assert!(!valid_html_markup("<script>unsafe</script>"));
        assert!(!valid_html_markup("unknown &entity;"));
        assert!(!valid_html_markup("invalid &#xD800; &#x110000;"));
        assert!(!valid_html_markup("raw > text"));
        assert!(!valid_html_markup("<code class=\"language-rust\">standalone</code>"));
    }

    #[test]
    fn validates_balanced_telegram_markdown_v2() {
        assert!(valid_markdown_v2("*bold* and _italic_ [link](https://example.test)"));
        assert!(valid_markdown_v2("escaped \\* punctuation"));
        assert!(valid_markdown_v2("`*literal*`"));
        assert!(!valid_markdown_v2("*unclosed"));
        assert!(!valid_markdown_v2("[link]"));
        assert!(!valid_markdown_v2("plain - dash"));
        assert!(!valid_markdown_v2("trailing escape\\"));
    }
}
