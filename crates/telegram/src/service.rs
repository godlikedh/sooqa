//! Admin message parsing and Telegram ingest interaction service.

use std::{
    collections::{BTreeSet, HashMap},
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use teloxide::types::{Message, Update, UpdateKind};
use tracing::warn;
use url::Url;

use crate::{
    ADD_USAGE_RESPONSE, Command, DEFAULT_TELEGRAM_SOURCE_DOWNLOAD_MAX_BYTES, HELP_RESPONSE,
    HandleOutcome, IncomingCallback, IncomingMessage, IngestService, IngestUnavailable,
    MediaIngestCommand, MediaKind, START_RESPONSE, STATUS_RESPONSE, TelegramApi, TelegramError,
    TelegramMedia, UNAUTHORIZED_RESPONSE, UrlIngestCommand,
};

const RESPONSE_RATE_LIMIT: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub struct TelegramService<A, I = ()> {
    pub(super) api: A,
    ingest_service: Option<I>,
    admin_user_ids: Arc<BTreeSet<i64>>,
    response_limiter: Arc<Mutex<HashMap<RateLimitKey, Instant>>>,
    source_download_max_bytes: u64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
struct RateLimitKey {
    user_id: Option<i64>,
    chat_id: i64,
}

impl<A> TelegramService<A, ()>
where
    A: TelegramApi,
{
    pub fn new(api: A, admin_user_ids: impl IntoIterator<Item = i64>) -> Self {
        Self {
            api,
            ingest_service: None,
            admin_user_ids: Arc::new(admin_user_ids.into_iter().collect()),
            response_limiter: Arc::new(Mutex::new(HashMap::new())),
            source_download_max_bytes: DEFAULT_TELEGRAM_SOURCE_DOWNLOAD_MAX_BYTES,
        }
    }
}

impl<A, I> TelegramService<A, I>
where
    A: TelegramApi,
    I: IngestService,
{
    pub fn with_ingest(
        api: A,
        admin_user_ids: impl IntoIterator<Item = i64>,
        ingest_service: I,
    ) -> Self {
        Self {
            api,
            ingest_service: Some(ingest_service),
            admin_user_ids: Arc::new(admin_user_ids.into_iter().collect()),
            response_limiter: Arc::new(Mutex::new(HashMap::new())),
            source_download_max_bytes: DEFAULT_TELEGRAM_SOURCE_DOWNLOAD_MAX_BYTES,
        }
    }
}

impl<A, I> TelegramService<A, I>
where
    A: TelegramApi,
    I: IngestService,
{
    pub fn with_source_download_max_bytes(mut self, max_bytes: u64) -> Self {
        self.source_download_max_bytes = max_bytes;
        self
    }

    pub async fn handle_message(
        &self,
        message: IncomingMessage,
    ) -> Result<HandleOutcome, TelegramError> {
        if !message.is_private {
            return Ok(HandleOutcome::NonPrivateIgnored);
        }
        if !message.user_id.is_some_and(|id| self.admin_user_ids.contains(&id)) {
            warn!(
                target: "sooqa.telegram",
                update_id = message.update_id,
                user_id = ?message.user_id,
                chat_id = message.chat_id,
                "unauthorized Telegram command attempt"
            );
            if !self.allow_response(message.user_id, message.chat_id) {
                return Ok(HandleOutcome::RateLimited);
            }
            self.send_response(
                message.chat_id,
                UNAUTHORIZED_RESPONSE,
                RateLimitKey { user_id: message.user_id, chat_id: message.chat_id },
            )
            .await?;
            return Ok(HandleOutcome::Unauthorized);
        }
        if let Some(media) = message.media.clone() {
            return self.handle_media_message(message, media).await;
        }
        let action =
            message.text.as_deref().map(parse_message_action).unwrap_or(MessageAction::Ignore);
        match action {
            MessageAction::Url(source_url) => {
                let Some(ingest_service) = self.ingest_service.as_ref() else {
                    return Err(TelegramError::Ingest(Box::new(IngestUnavailable)));
                };
                let user_id = message.user_id.expect("authorized Telegram messages have a user ID");
                let accepted = match ingest_service
                    .create_url(UrlIngestCommand {
                        source_url,
                        idempotency_key: format!("telegram:update:{}:v1", message.update_id),
                        submitted_by_user_id: user_id,
                    })
                    .await
                {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        self.clear_response(RateLimitKey {
                            user_id: message.user_id,
                            chat_id: message.chat_id,
                        });
                        return Err(TelegramError::Ingest(Box::new(error)));
                    }
                };
                let response = format!(
                    "✅ Ingest queued\nID: {}\nStatus: {}",
                    accepted.request_id, accepted.status
                );
                if !self.allow_response(message.user_id, message.chat_id) {
                    return Ok(HandleOutcome::RateLimited);
                }
                self.send_response(
                    message.chat_id,
                    &response,
                    RateLimitKey { user_id: message.user_id, chat_id: message.chat_id },
                )
                .await?;
                Ok(HandleOutcome::Responded(Command::Add))
            }
            MessageAction::Command(command) => {
                if !self.allow_response(message.user_id, message.chat_id) {
                    return Ok(HandleOutcome::RateLimited);
                }
                let response = match command {
                    Command::Start => START_RESPONSE,
                    Command::Help => HELP_RESPONSE,
                    Command::Status => STATUS_RESPONSE,
                    Command::Add => ADD_USAGE_RESPONSE,
                };
                self.send_response(
                    message.chat_id,
                    response,
                    RateLimitKey { user_id: message.user_id, chat_id: message.chat_id },
                )
                .await?;
                Ok(HandleOutcome::Responded(command))
            }
            MessageAction::Ignore => Ok(HandleOutcome::UnrecognizedIgnored),
        }
    }

    async fn handle_media_message(
        &self,
        message: IncomingMessage,
        media: TelegramMedia,
    ) -> Result<HandleOutcome, TelegramError> {
        let rate_limit_key = RateLimitKey { user_id: message.user_id, chat_id: message.chat_id };
        let (media_kind, file_id, file_unique_id, file_size, mime_type, file_name) = match media {
            TelegramMedia::UnsupportedDocument { file_name, mime_type } => {
                let response = format!(
                    "⚠️ Unsupported Telegram document{}{}",
                    file_name.map(|value| format!(": {value}")).unwrap_or_default(),
                    mime_type.map(|value| format!(" ({value})")).unwrap_or_default()
                );
                if !self.allow_response(message.user_id, message.chat_id) {
                    return Ok(HandleOutcome::RateLimited);
                }
                self.send_response(message.chat_id, &response, rate_limit_key).await?;
                return Ok(HandleOutcome::MediaRejected);
            }
            TelegramMedia::Supported {
                media_kind,
                file_id,
                file_unique_id,
                file_size,
                mime_type,
                file_name,
            } => (Some(media_kind), file_id, file_unique_id, file_size, mime_type, file_name),
            TelegramMedia::ProbeableDocument {
                file_id,
                file_unique_id,
                file_size,
                mime_type,
                file_name,
            } => (None, file_id, file_unique_id, file_size, mime_type, file_name),
        };
        let response = {
            if file_size.map(u64::from).is_some_and(|size| size > self.source_download_max_bytes) {
                let response = format!(
                    "⚠️ Telegram file exceeds the configured download limit of {} bytes",
                    self.source_download_max_bytes
                );
                if !self.allow_response(message.user_id, message.chat_id) {
                    return Ok(HandleOutcome::RateLimited);
                }
                self.send_response(message.chat_id, &response, rate_limit_key).await?;
                return Ok(HandleOutcome::MediaRejected);
            }
            let user_id = message.user_id.expect("authorized Telegram messages have a user ID");
            let Some(ingest_service) = self.ingest_service.as_ref() else {
                return Err(TelegramError::Ingest(Box::new(IngestUnavailable)));
            };
            let accepted = match ingest_service
                .create_media(MediaIngestCommand {
                    update_id: message.update_id,
                    message_id: message.message_id,
                    chat_id: message.chat_id,
                    submitted_by_user_id: user_id,
                    media_kind,
                    file_id,
                    file_unique_id,
                    file_size,
                    mime_type,
                    file_name,
                    caption: message.caption.clone(),
                    idempotency_key: format!("telegram:update:{}:v1", message.update_id),
                })
                .await
            {
                Ok(accepted) => accepted,
                Err(error) => {
                    self.clear_response(rate_limit_key);
                    return Err(TelegramError::Ingest(Box::new(error)));
                }
            };
            format!("✅ Ingest queued\nID: {}\nStatus: {}", accepted.request_id, accepted.status)
        };
        if !self.allow_response(message.user_id, message.chat_id) {
            return Ok(HandleOutcome::RateLimited);
        }
        self.send_response(message.chat_id, &response, rate_limit_key).await?;
        Ok(HandleOutcome::Responded(Command::Add))
    }

    pub async fn handle_update(&self, update: Update) -> Result<HandleOutcome, TelegramError> {
        let update_id = i64::from(update.id.0);
        match update.kind {
            UpdateKind::Message(message) => {
                self.handle_message(IncomingMessage {
                    update_id,
                    message_id: i64::from(message.id.0),
                    reply_to_message_id: message
                        .reply_to_message()
                        .map(|reply| i64::from(reply.id.0)),
                    user_id: message.from.as_ref().and_then(|user| i64::try_from(user.id.0).ok()),
                    chat_id: message.chat.id.0,
                    is_private: message.chat.is_private(),
                    text: message.text().map(str::to_owned),
                    caption: message.caption().map(str::to_owned),
                    media: message_media(&message),
                })
                .await
            }
            UpdateKind::CallbackQuery(callback) => {
                self.acknowledge_callback(IncomingCallback {
                    update_id,
                    callback_id: callback.id.0,
                })
                .await
            }
            _ => Ok(HandleOutcome::NonMessageIgnored),
        }
    }

    /// Acknowledge callback queries left by superseded Telegram keyboards.
    ///
    /// Callback payloads are intentionally never parsed or dispatched: stale
    /// buttons receive only a normal Telegram acknowledgement.
    pub async fn acknowledge_callback(
        &self,
        callback: IncomingCallback,
    ) -> Result<HandleOutcome, TelegramError> {
        // Telegram clients keep showing a spinner until this is answered. Do
        // this before any repository work, including for rejected callbacks.
        if let Err(error) = self.api.answer_callback_query(&callback.callback_id).await {
            return Err(TelegramError::Api(Box::new(error)));
        }

        Ok(HandleOutcome::CallbackHandled)
    }

    async fn send_response(
        &self,
        chat_id: i64,
        text: &str,
        rate_limit_key: RateLimitKey,
    ) -> Result<(), TelegramError> {
        if let Err(error) = self
            .api
            .send_text(chat_id, text)
            .await
            .map_err(|error| TelegramError::Api(Box::new(error)))
        {
            self.clear_response(rate_limit_key);
            return Err(error);
        }
        Ok(())
    }

    fn allow_response(&self, user_id: Option<i64>, chat_id: i64) -> bool {
        let now = Instant::now();
        let mut limiter =
            self.response_limiter.lock().expect("Telegram rate limiter is not poisoned");
        limiter.retain(|_, last_response| {
            now.saturating_duration_since(*last_response) < RESPONSE_RATE_LIMIT
        });
        let key = RateLimitKey { user_id, chat_id };
        if limiter.get(&key).is_some_and(|last_response| {
            now.saturating_duration_since(*last_response) < RESPONSE_RATE_LIMIT
        }) {
            return false;
        }
        limiter.insert(key, now);
        true
    }

    fn clear_response(&self, key: RateLimitKey) {
        self.response_limiter.lock().expect("Telegram rate limiter is not poisoned").remove(&key);
    }
}

fn message_media(message: &Message) -> Option<TelegramMedia> {
    if let Some(photo) = message.photo().and_then(|photos| photos.last()) {
        return Some(TelegramMedia::Supported {
            media_kind: MediaKind::Image,
            file_id: photo.file.id.to_string(),
            file_unique_id: photo.file.unique_id.to_string(),
            file_size: Some(photo.file.size),
            mime_type: Some("image/jpeg".to_owned()),
            file_name: None,
        });
    }
    if let Some(video) = message.video() {
        return Some(TelegramMedia::Supported {
            media_kind: MediaKind::Video,
            file_id: video.file.id.to_string(),
            file_unique_id: video.file.unique_id.to_string(),
            file_size: Some(video.file.size),
            mime_type: video.mime_type.as_ref().map(ToString::to_string),
            file_name: video.file_name.clone(),
        });
    }
    if let Some(animation) = message.animation() {
        return Some(TelegramMedia::Supported {
            media_kind: MediaKind::Animation,
            file_id: animation.file.id.to_string(),
            file_unique_id: animation.file.unique_id.to_string(),
            file_size: Some(animation.file.size),
            mime_type: animation.mime_type.as_ref().map(ToString::to_string),
            file_name: animation.file_name.clone(),
        });
    }
    let document = message.document()?;
    let mime_type = document.mime_type.as_ref().map(ToString::to_string);
    match classify_document(mime_type.as_deref(), document.file_name.as_deref()) {
        DocumentClassification::Supported(media_kind) => Some(TelegramMedia::Supported {
            media_kind,
            file_id: document.file.id.to_string(),
            file_unique_id: document.file.unique_id.to_string(),
            file_size: Some(document.file.size),
            mime_type,
            file_name: document.file_name.clone(),
        }),
        DocumentClassification::Probeable => Some(TelegramMedia::ProbeableDocument {
            file_id: document.file.id.to_string(),
            file_unique_id: document.file.unique_id.to_string(),
            file_size: Some(document.file.size),
            mime_type,
            file_name: document.file_name.clone(),
        }),
        DocumentClassification::Unsupported => Some(TelegramMedia::UnsupportedDocument {
            file_name: document.file_name.clone(),
            mime_type,
        }),
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum DocumentClassification {
    Supported(MediaKind),
    Probeable,
    Unsupported,
}

fn classify_document(mime_type: Option<&str>, file_name: Option<&str>) -> DocumentClassification {
    if let Some(mime_type) = mime_type {
        let mime_type =
            mime_type.split(';').next().map(str::trim).unwrap_or_default().to_ascii_lowercase();
        if mime_type.starts_with("video/") {
            return DocumentClassification::Supported(MediaKind::Video);
        }
        if mime_type == "image/gif" {
            return DocumentClassification::Supported(MediaKind::Animation);
        }
        if matches!(mime_type.as_str(), "image/jpeg" | "image/png") {
            return DocumentClassification::Supported(MediaKind::Image);
        }
        if mime_type.starts_with("image/") {
            return DocumentClassification::Unsupported;
        }
        if mime_type.starts_with("audio/") {
            return DocumentClassification::Supported(MediaKind::Audio);
        }
        if matches!(
            mime_type.as_str(),
            "application/pdf"
                | "application/zip"
                | "application/x-7z-compressed"
                | "application/x-rar-compressed"
                | "text/plain"
        ) {
            return DocumentClassification::Unsupported;
        }
        if mime_type.eq_ignore_ascii_case("application/octet-stream")
            || mime_type.eq_ignore_ascii_case("binary/octet-stream")
        {
            return classify_document_filename(file_name);
        }
    }

    classify_document_filename(file_name)
}

fn classify_document_filename(file_name: Option<&str>) -> DocumentClassification {
    let extension = file_name
        .and_then(|name| Path::new(name).extension())
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase);
    match extension.as_deref() {
        Some("gif") => DocumentClassification::Supported(MediaKind::Animation),
        Some("jpg" | "jpeg" | "png") => DocumentClassification::Supported(MediaKind::Image),
        Some("mp4" | "webm" | "mkv" | "mov" | "avi") => {
            DocumentClassification::Supported(MediaKind::Video)
        }
        Some("mp3" | "m4a" | "wav" | "flac" | "ogg") => {
            DocumentClassification::Supported(MediaKind::Audio)
        }
        Some(
            "avif" | "webp" | "zip" | "rar" | "7z" | "pdf" | "txt" | "doc" | "docx" | "xls"
            | "xlsx" | "ppt" | "pptx",
        ) => DocumentClassification::Unsupported,
        _ => DocumentClassification::Probeable,
    }
}

fn parse_command(text: &str) -> Option<Command> {
    let command = text.split_whitespace().next()?.strip_prefix('/')?;
    let command = command.split('@').next().unwrap_or(command);
    match command.to_ascii_lowercase().as_str() {
        "start" => Some(Command::Start),
        "help" => Some(Command::Help),
        "add" => Some(Command::Add),
        "status" => Some(Command::Status),
        _ => None,
    }
}

#[derive(Debug, Eq, PartialEq)]
enum MessageAction {
    Command(Command),
    Url(String),
    Ignore,
}

fn parse_message_action(text: &str) -> MessageAction {
    if let Some(command) = parse_command(text) {
        return if command == Command::Add {
            parse_single_url(text).map_or(MessageAction::Command(Command::Add), MessageAction::Url)
        } else {
            MessageAction::Command(command)
        };
    }
    if text.trim_start().starts_with('/') {
        MessageAction::Ignore
    } else {
        parse_single_url(text).map_or(MessageAction::Ignore, MessageAction::Url)
    }
}

fn parse_single_url(text: &str) -> Option<String> {
    let mut tokens = text.split_whitespace();
    if let Some(first) = tokens.next() {
        if first.starts_with('/') {
            let command = first.strip_prefix('/')?.split('@').next()?;
            if !command.eq_ignore_ascii_case("add") {
                return None;
            }
        } else {
            tokens = text.split_whitespace();
        }
    }

    let urls = tokens.filter_map(parse_http_url).collect::<Vec<_>>();
    (urls.len() == 1).then(|| urls.into_iter().next().expect("one URL exists"))
}

fn parse_http_url(token: &str) -> Option<String> {
    if is_safe_http_url(token) {
        return Some(token.to_owned());
    }
    let unwrapped = token.trim_matches(|character: char| "<>[](){}".contains(character));
    (unwrapped != token && is_safe_http_url(unwrapped)).then(|| unwrapped.to_owned())
}

fn is_safe_http_url(value: &str) -> bool {
    let Ok(url) = Url::parse(value) else { return false };
    matches!(url.scheme(), "http" | "https")
        && url.host_str().is_some()
        && url.username().is_empty()
        && url.password().is_none()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{BlockingDownloadApi, MockApi, MockIngestService, message};

    #[tokio::test]
    async fn authorized_commands_are_translated_and_rate_limited() {
        let api = MockApi::default();
        let service = TelegramService::new(api.clone(), [123]);

        assert_eq!(
            service.handle_message(message(1, Some(123), "/status")).await.unwrap(),
            HandleOutcome::Responded(Command::Status)
        );
        assert_eq!(
            service.handle_message(message(1, Some(123), "/status")).await.unwrap(),
            HandleOutcome::RateLimited
        );
        assert_eq!(api.messages.lock().unwrap().as_slice(), &[(42, STATUS_RESPONSE.to_owned())]);
    }

    #[tokio::test]
    async fn superseded_commands_and_callbacks_are_harmless() {
        let api = MockApi::default();
        let ingest = MockIngestService::default();
        let service = TelegramService::with_ingest(api.clone(), [123], ingest.clone());

        assert_eq!(
            service.handle_message(message(11, Some(123), "/queue")).await.unwrap(),
            HandleOutcome::UnrecognizedIgnored
        );
        assert_eq!(
            service.handle_message(message(12, Some(123), "/duplicates")).await.unwrap(),
            HandleOutcome::UnrecognizedIgnored
        );
        assert_eq!(
            service
                .handle_update(Update {
                    id: teloxide::types::UpdateId(13),
                    kind: UpdateKind::CallbackQuery(
                        serde_json::from_value(serde_json::json!({
                            "id": "stale-duplicate-card",
                            "from": {
                                "id": 123,
                                "is_bot": false,
                                "first_name": "Admin"
                            },
                            "message": {
                                "message_id": 44,
                                "chat": {
                                    "id": 123,
                                    "type": "private",
                                    "first_name": "Admin"
                                },
                                "date": 1
                            },
                            "chat_instance": "stale-card",
                            "data": "v1:duplicate_use:AAAAAAAAAAAAAAAAAAAAAQ:AAAAAAAAAAAAAAAAAAAAAg"
                        }))
                        .expect("callback fixture should deserialize"),
                    ),
                })
                .await
                .unwrap(),
            HandleOutcome::CallbackHandled
        );
        assert!(api.messages.lock().unwrap().is_empty());
        assert_eq!(api.callback_answers.lock().unwrap().as_slice(), &["stale-duplicate-card"]);
        assert!(ingest.commands.lock().unwrap().is_empty());
        assert!(ingest.media_commands.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn unauthorized_private_user_gets_generic_response() {
        let api = MockApi::default();
        let service = TelegramService::new(api.clone(), [123]);

        assert_eq!(
            service.handle_message(message(2, Some(456), "/status")).await.unwrap(),
            HandleOutcome::Unauthorized
        );
        assert_eq!(
            api.messages.lock().unwrap().as_slice(),
            &[(42, UNAUTHORIZED_RESPONSE.to_owned())]
        );
    }

    #[tokio::test]
    async fn group_messages_are_ignored_before_authorization() {
        let api = MockApi::default();
        let service = TelegramService::new(api.clone(), [123]);
        let mut update = message(3, Some(123), "/status");
        update.is_private = false;

        assert_eq!(service.handle_message(update).await.unwrap(), HandleOutcome::NonPrivateIgnored);
        assert!(api.messages.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn command_responses_are_rate_limited_per_user_and_chat() {
        let api = MockApi::default();
        let service = TelegramService::new(api.clone(), [123]);

        assert_eq!(
            service.handle_message(message(4, Some(123), "/status")).await.unwrap(),
            HandleOutcome::Responded(Command::Status)
        );
        assert_eq!(
            service.handle_message(message(5, Some(123), "/help")).await.unwrap(),
            HandleOutcome::RateLimited
        );
        assert_eq!(api.messages.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn failed_response_is_retryable() {
        let api = MockApi::default();
        *api.fail.lock().unwrap() = true;
        let service = TelegramService::new(api.clone(), [123]);

        assert!(service.handle_message(message(6, Some(123), "/status")).await.is_err());
        *api.fail.lock().unwrap() = false;
        assert_eq!(
            service.handle_message(message(6, Some(123), "/status")).await.unwrap(),
            HandleOutcome::Responded(Command::Status)
        );
    }

    #[tokio::test]
    async fn url_messages_create_ingest_and_return_status() {
        let api = MockApi::default();
        let ingest = MockIngestService::default();
        let service = TelegramService::with_ingest(api.clone(), [123], ingest.clone());

        assert_eq!(
            service
                .handle_message(message(7, Some(123), "/add https://example.test/video.webm"))
                .await
                .unwrap(),
            HandleOutcome::Responded(Command::Add)
        );
        assert_eq!(
            ingest.commands.lock().unwrap().as_slice(),
            &[UrlIngestCommand {
                source_url: "https://example.test/video.webm".to_owned(),
                idempotency_key: "telegram:update:7:v1".to_owned(),
                submitted_by_user_id: 123,
            }]
        );
        assert_eq!(
            api.messages.lock().unwrap().as_slice(),
            &[(
                42,
                "✅ Ingest queued\nID: 00000000-0000-0000-0000-000000000001\nStatus: queued"
                    .to_owned()
            )]
        );
    }

    #[tokio::test]
    async fn authorized_media_is_queued_without_downloading_bytes() {
        let api = MockApi::default();
        let ingest = MockIngestService::default();
        let service = TelegramService::with_ingest(api.clone(), [123], ingest.clone());
        let message = IncomingMessage {
            update_id: 11,
            message_id: 99,
            reply_to_message_id: None,
            user_id: Some(123),
            chat_id: 42,
            is_private: true,
            text: None,
            caption: Some("a caption".to_owned()),
            media: Some(TelegramMedia::Supported {
                media_kind: MediaKind::Video,
                file_id: "file-id".to_owned(),
                file_unique_id: "unique-id".to_owned(),
                file_size: Some(1234),
                mime_type: Some("video/webm".to_owned()),
                file_name: Some("clip.webm".to_owned()),
            }),
        };

        assert_eq!(
            service.handle_message(message).await.unwrap(),
            HandleOutcome::Responded(Command::Add)
        );
        let media_command = ingest.media_commands.lock().unwrap()[0].clone();
        assert_eq!(
            media_command,
            MediaIngestCommand {
                update_id: 11,
                message_id: 99,
                chat_id: 42,
                submitted_by_user_id: 123,
                media_kind: Some(MediaKind::Video),
                file_id: "file-id".to_owned(),
                file_unique_id: "unique-id".to_owned(),
                file_size: Some(1234),
                mime_type: Some("video/webm".to_owned()),
                file_name: Some("clip.webm".to_owned()),
                caption: Some("a caption".to_owned()),
                idempotency_key: "telegram:update:11:v1".to_owned(),
            }
        );
        assert!(api.downloads.lock().unwrap().is_empty());
        assert_eq!(api.messages.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn slow_media_download_cannot_delay_concurrent_metadata_acceptance() {
        let api = BlockingDownloadApi;
        let ingest = MockIngestService::default();
        let service = TelegramService::with_ingest(api, [123], ingest.clone());
        let message = |update_id, chat_id| IncomingMessage {
            update_id,
            message_id: update_id,
            reply_to_message_id: None,
            user_id: Some(123),
            chat_id,
            is_private: true,
            text: None,
            caption: None,
            media: Some(TelegramMedia::Supported {
                media_kind: MediaKind::Video,
                file_id: format!("file-{update_id}"),
                file_unique_id: format!("unique-{update_id}"),
                file_size: Some(2_000_000_000),
                mime_type: Some("video/mp4".to_owned()),
                file_name: Some("large.mp4".to_owned()),
            }),
        };

        let results = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(
                service.handle_message(message(15, 42)),
                service.handle_message(message(16, 43)),
            )
        })
        .await
        .expect("metadata acceptance must not wait for a media download");
        assert!(matches!(results.0, Ok(HandleOutcome::Responded(Command::Add))));
        assert!(matches!(results.1, Ok(HandleOutcome::Responded(Command::Add))));
        assert_eq!(ingest.media_commands.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn advertised_media_size_is_rejected_before_workspace_or_download() {
        let api = MockApi::default();
        let ingest = MockIngestService::default();
        let service = TelegramService::with_ingest(api.clone(), [123], ingest.clone())
            .with_source_download_max_bytes(1024);
        let message = IncomingMessage {
            update_id: 111,
            message_id: 199,
            reply_to_message_id: None,
            user_id: Some(123),
            chat_id: 42,
            is_private: true,
            text: None,
            caption: None,
            media: Some(TelegramMedia::Supported {
                media_kind: MediaKind::Video,
                file_id: "too-large".to_owned(),
                file_unique_id: "too-large-unique".to_owned(),
                file_size: Some(1025),
                mime_type: Some("video/webm".to_owned()),
                file_name: Some("large.webm".to_owned()),
            }),
        };

        assert_eq!(service.handle_message(message).await.unwrap(), HandleOutcome::MediaRejected);
        assert!(ingest.media_commands.lock().unwrap().is_empty());
        assert!(api.downloads.lock().unwrap().is_empty());
        assert!(api.messages.lock().unwrap()[0].1.contains("1024 bytes"));
    }

    #[tokio::test]
    async fn unsupported_telegram_document_is_rejected_without_download() {
        let api = MockApi::default();
        let ingest = MockIngestService::default();
        let service = TelegramService::with_ingest(api.clone(), [123], ingest.clone());
        let message = IncomingMessage {
            update_id: 12,
            message_id: 100,
            reply_to_message_id: None,
            user_id: Some(123),
            chat_id: 42,
            is_private: true,
            text: None,
            caption: None,
            media: Some(TelegramMedia::UnsupportedDocument {
                file_name: Some("archive.zip".to_owned()),
                mime_type: Some("application/zip".to_owned()),
            }),
        };

        assert_eq!(service.handle_message(message).await.unwrap(), HandleOutcome::MediaRejected);
        assert!(ingest.media_commands.lock().unwrap().is_empty());
        assert!(api.downloads.lock().unwrap().is_empty());
        assert_eq!(
            api.messages.lock().unwrap().as_slice(),
            &[(42, "⚠️ Unsupported Telegram document: archive.zip (application/zip)".to_owned())]
        );
    }

    #[tokio::test]
    async fn probeable_telegram_document_is_queued_without_declared_kind() {
        let api = MockApi::default();
        let ingest = MockIngestService::default();
        let service = TelegramService::with_ingest(api.clone(), [123], ingest.clone());
        let message = IncomingMessage {
            update_id: 14,
            message_id: 102,
            reply_to_message_id: None,
            user_id: Some(123),
            chat_id: 42,
            is_private: true,
            text: None,
            caption: None,
            media: Some(TelegramMedia::ProbeableDocument {
                file_id: "unknown-media".to_owned(),
                file_unique_id: "unknown-media-unique".to_owned(),
                file_size: Some(42),
                mime_type: Some("application/octet-stream".to_owned()),
                file_name: None,
            }),
        };

        assert_eq!(
            service.handle_message(message).await.unwrap(),
            HandleOutcome::Responded(Command::Add)
        );
        let command = ingest.media_commands.lock().unwrap().pop().expect("media should be queued");
        assert_eq!(command.media_kind, None);
        assert_eq!(command.mime_type.as_deref(), Some("application/octet-stream"));
        assert_eq!(command.file_name, None);
        assert!(api.downloads.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn replayed_media_update_reuses_the_durable_ingest_key_without_downloading() {
        let api = MockApi::default();
        let ingest = MockIngestService::default();
        let service = TelegramService::with_ingest(api.clone(), [123], ingest.clone());
        let message = IncomingMessage {
            update_id: 13,
            message_id: 101,
            reply_to_message_id: None,
            user_id: Some(123),
            chat_id: 42,
            is_private: true,
            text: None,
            caption: None,
            media: Some(TelegramMedia::Supported {
                media_kind: MediaKind::Image,
                file_id: "file-id".to_owned(),
                file_unique_id: "unique-id".to_owned(),
                file_size: Some(42),
                mime_type: Some("image/jpeg".to_owned()),
                file_name: None,
            }),
        };

        assert_eq!(
            service.handle_message(message.clone()).await.unwrap(),
            HandleOutcome::Responded(Command::Add)
        );
        // A fresh service instance models a process restart. The shared mock
        // Inbox keeps the durable input key, while the adapter has no receipt
        // state to carry over.
        let restarted = TelegramService::with_ingest(api.clone(), [123], ingest.clone());
        assert_eq!(
            restarted.handle_message(message).await.unwrap(),
            HandleOutcome::Responded(Command::Add)
        );
        assert_eq!(ingest.media_commands.lock().unwrap().len(), 1);
        assert!(api.downloads.lock().unwrap().is_empty());
        assert_eq!(api.messages.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn replayed_url_update_reuses_the_durable_ingest_key() {
        let api = MockApi::default();
        let ingest = MockIngestService::default();
        let service = TelegramService::with_ingest(api.clone(), [123], ingest.clone());
        let update = message(17, Some(123), "https://example.test/video.webm");

        assert_eq!(
            service.handle_message(update.clone()).await.unwrap(),
            HandleOutcome::Responded(Command::Add)
        );
        let restarted = TelegramService::with_ingest(api.clone(), [123], ingest.clone());
        assert_eq!(
            restarted.handle_message(update).await.unwrap(),
            HandleOutcome::Responded(Command::Add)
        );
        assert_eq!(ingest.commands.lock().unwrap().len(), 1);
        assert_eq!(api.messages.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn rate_limited_url_ack_still_creates_ingest_request() {
        let api = MockApi::default();
        let ingest = MockIngestService::default();
        let service = TelegramService::with_ingest(api.clone(), [123], ingest.clone());

        assert_eq!(
            service
                .handle_message(message(9, Some(123), "https://example.test/one"))
                .await
                .unwrap(),
            HandleOutcome::Responded(Command::Add)
        );
        assert_eq!(
            service
                .handle_message(message(10, Some(123), "https://example.test/two"))
                .await
                .unwrap(),
            HandleOutcome::RateLimited
        );
        assert_eq!(ingest.commands.lock().unwrap().len(), 2);
        assert_eq!(api.messages.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn teloxide_message_mapping_uses_private_chat_and_sender_id() {
        let message: teloxide::types::Message = serde_json::from_value(serde_json::json!({
            "message_id": 1,
            "from": {"id": 123, "is_bot": false, "first_name": "Admin"},
            "chat": {"id": 42, "type": "private", "first_name": "Admin"},
            "date": 0,
            "text": "/status"
        }))
        .expect("Telegram message fixture should deserialize");
        let update =
            Update { id: teloxide::types::UpdateId(7), kind: UpdateKind::Message(message) };
        let api = MockApi::default();
        let service = TelegramService::new(api.clone(), [123]);

        assert_eq!(
            service.handle_update(update).await.unwrap(),
            HandleOutcome::Responded(Command::Status)
        );
        assert_eq!(api.messages.lock().unwrap().len(), 1);
    }

    #[test]
    fn command_parser_accepts_bot_suffix_and_rejects_other_text() {
        assert_eq!(parse_command("/start@sooqa_bot"), Some(Command::Start));
        assert_eq!(parse_command("/HELP extra"), Some(Command::Help));
        assert_eq!(parse_command("hello /status"), None);
        assert_eq!(parse_command("/add"), Some(Command::Add));
        assert_eq!(parse_command("/duplicates"), None);
        assert_eq!(parse_command("/queue"), None);
    }

    #[test]
    fn document_classifier_prioritizes_explicit_mime_and_probes_unknowns() {
        assert_eq!(
            classify_document(Some("video/mp4"), Some("not-video.txt")),
            DocumentClassification::Supported(MediaKind::Video)
        );
        assert_eq!(
            classify_document(Some("IMAGE/PNG; charset=binary"), Some("photo.webp")),
            DocumentClassification::Supported(MediaKind::Image)
        );
        assert_eq!(
            classify_document(Some("image/webp"), Some("photo.png")),
            DocumentClassification::Unsupported
        );
        assert_eq!(
            classify_document(Some("application/pdf"), Some("clip.mp4")),
            DocumentClassification::Unsupported
        );
        assert_eq!(
            classify_document(Some("application/octet-stream"), Some("clip.WEBM")),
            DocumentClassification::Supported(MediaKind::Video)
        );
        assert_eq!(
            classify_document(Some("application/octet-stream"), Some("photo.webp")),
            DocumentClassification::Unsupported
        );
        assert_eq!(
            classify_document(Some("application/x-unknown"), None),
            DocumentClassification::Probeable
        );
        assert_eq!(classify_document(None, Some("mystery.bin")), DocumentClassification::Probeable);
        assert_eq!(
            classify_document(None, Some("archive.zip")),
            DocumentClassification::Unsupported
        );
        assert_eq!(
            classify_document(Some("image/gif"), Some("animation.bin")),
            DocumentClassification::Supported(MediaKind::Animation)
        );
    }

    #[test]
    fn message_parser_accepts_one_http_url_only() {
        assert_eq!(
            parse_message_action("/add https://example.test/video.webm"),
            MessageAction::Url("https://example.test/video.webm".to_owned())
        );
        assert_eq!(
            parse_message_action("https://example.test/video.webm"),
            MessageAction::Url("https://example.test/video.webm".to_owned())
        );
        assert_eq!(
            parse_message_action("https://example.test/path!"),
            MessageAction::Url("https://example.test/path!".to_owned())
        );
        assert_eq!(
            parse_message_action("https://example.test/a(b)"),
            MessageAction::Url("https://example.test/a(b)".to_owned())
        );
        assert_eq!(
            parse_message_action("https://example.test/path,"),
            MessageAction::Url("https://example.test/path,".to_owned())
        );
        assert_eq!(
            parse_message_action("https://example.test/path."),
            MessageAction::Url("https://example.test/path.".to_owned())
        );
        assert_eq!(
            parse_message_action("<https://example.test/video.webm>"),
            MessageAction::Url("https://example.test/video.webm".to_owned())
        );
        assert_eq!(parse_message_action("/add"), MessageAction::Command(Command::Add));
        assert_eq!(
            parse_message_action("https://one.test/a https://two.test/b"),
            MessageAction::Ignore
        );
        assert_eq!(parse_message_action("ftp://example.test/file"), MessageAction::Ignore);
    }
}
