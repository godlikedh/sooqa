//! Telegram adapter and editorial interaction boundaries for sooqa.

use std::{
    collections::{BTreeSet, HashMap},
    error::Error,
    future::Future,
    io,
    path::{Component, Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use sooqa_library::MediaKind;
use teloxide::{
    Bot,
    payloads::GetUpdatesSetters,
    prelude::{Request, Requester},
    types::{CallbackQueryId, ChatId, FileId, Message, Update, UpdateKind},
};
use thiserror::Error as ThisError;
use tracing::warn;
use url::Url;
use uuid::Uuid;

mod publication;
mod storage;

pub use publication::{TelegramPublicationApi, TelegramPublicationRequest};
pub use storage::{
    StorageCaptionApiError, StorageCaptionEditRequest, StorageUploadApiError, StorageUploadError,
    StorageUploadInput, StorageUploadOutcome, StorageUploadProvider, StorageUploadRequest,
    StorageUploadResult, TELEGRAM_STORAGE_PROVIDER, TelegramStorageApi, TelegramStorageCaptionApi,
    storage_caption,
};

pub const START_RESPONSE: &str = "sooqa is ready. You are authorized.";
pub const HELP_RESPONSE: &str = "Available commands:\n/start — show authorization\n/help — show this help\n/add <url> — queue a URL\n/status — show service status";
pub const STATUS_RESPONSE: &str = "sooqa is online.";
pub const UNAUTHORIZED_RESPONSE: &str = "This bot is restricted to its configured administrator.";
pub const ADD_USAGE_RESPONSE: &str = "Send one http(s) URL after /add, or send a bare URL.";
pub const DEFAULT_TELEGRAM_SOURCE_DOWNLOAD_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;
pub const DEFAULT_TELEGRAM_UPLOAD_TIMEOUT_SECONDS: u64 = 3_600;
const TELEGRAM_CLOUD_DOWNLOAD_LIMIT_BYTES: u64 = 20 * 1024 * 1024;
const TELEGRAM_CLOUD_UPLOAD_LIMIT_BYTES: u64 = 50 * 1024 * 1024;
const TELEGRAM_MEDIA_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const TELEGRAM_MEDIA_READ_TIMEOUT: Duration = Duration::from_secs(120);
const TELEGRAM_MAX_UPLOAD_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);
const RESPONSE_RATE_LIMIT: Duration = Duration::from_secs(1);
const RETRY_DELAY: Duration = Duration::from_secs(1);
const MAX_HANDLER_ATTEMPTS: usize = 5;
const MAX_POLL_RETRY_DELAY: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IncomingMessage {
    pub update_id: i64,
    pub message_id: i64,
    pub reply_to_message_id: Option<i64>,
    pub user_id: Option<i64>,
    pub chat_id: i64,
    pub is_private: bool,
    pub text: Option<String>,
    pub caption: Option<String>,
    pub media: Option<TelegramMedia>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IncomingCallback {
    pub update_id: i64,
    pub callback_id: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TelegramMedia {
    Supported {
        media_kind: MediaKind,
        file_id: String,
        file_unique_id: String,
        file_size: Option<u32>,
        mime_type: Option<String>,
        file_name: Option<String>,
    },
    ProbeableDocument {
        file_id: String,
        file_unique_id: String,
        file_size: Option<u32>,
        mime_type: Option<String>,
        file_name: Option<String>,
    },
    UnsupportedDocument {
        file_name: Option<String>,
        mime_type: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum HandleOutcome {
    NonMessageIgnored,
    NonPrivateIgnored,
    RateLimited,
    MediaRejected,
    Unauthorized,
    UnrecognizedIgnored,
    CallbackHandled,
    Responded(Command),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Command {
    Start,
    Help,
    Add,
    Status,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct UrlIngestCommand {
    pub source_url: String,
    pub idempotency_key: String,
    pub submitted_by_user_id: i64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MediaIngestCommand {
    pub update_id: i64,
    pub message_id: i64,
    pub chat_id: i64,
    pub submitted_by_user_id: i64,
    pub media_kind: Option<MediaKind>,
    pub file_id: String,
    pub file_unique_id: String,
    pub file_size: Option<u32>,
    pub mime_type: Option<String>,
    pub file_name: Option<String>,
    pub caption: Option<String>,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IngestAccepted {
    pub request_id: Uuid,
    pub status: String,
}

#[async_trait]
pub trait IngestService: Clone + Send + Sync + 'static {
    type Error: Error + Send + Sync + 'static;

    async fn create_url(&self, command: UrlIngestCommand) -> Result<IngestAccepted, Self::Error>;

    async fn create_media(
        &self,
        command: MediaIngestCommand,
    ) -> Result<IngestAccepted, Self::Error>;
}

#[derive(Debug, ThisError)]
#[error("Telegram URL ingest service is not configured")]
pub struct IngestUnavailable;

#[async_trait]
impl IngestService for () {
    type Error = IngestUnavailable;

    async fn create_url(&self, _command: UrlIngestCommand) -> Result<IngestAccepted, Self::Error> {
        Err(IngestUnavailable)
    }

    async fn create_media(
        &self,
        _command: MediaIngestCommand,
    ) -> Result<IngestAccepted, Self::Error> {
        Err(IngestUnavailable)
    }
}

#[derive(Debug, ThisError)]
pub enum TelegramError {
    #[error("Telegram API request failed: {0}")]
    Api(#[source] Box<dyn Error + Send + Sync>),
    #[error("Telegram API base URL is invalid: {0}")]
    InvalidApiBaseUrl(String),
    #[error("Telegram polling timeout must be greater than zero")]
    InvalidPollTimeout,
    #[error("Telegram polling backoff must be greater than zero and ordered initial <= maximum")]
    InvalidPollingBackoff,
    #[error("Telegram upload timeout must be greater than zero and at most 24 hours")]
    InvalidUploadTimeout,
    #[error("Telegram HTTP client could not be configured: {0}")]
    HttpClient(#[source] reqwest::Error),
    #[error("Telegram Ctrl-C handler could not be initialized: {0}")]
    Shutdown(#[source] std::io::Error),
    #[error("Telegram URL ingest failed: {0}")]
    Ingest(#[source] Box<dyn Error + Send + Sync>),
}

/// Errors returned by the Telegram control-plane calls used by the polling
/// supervisor.  Polling errors are kept separate from media and update
/// handling errors so remote outages can be retried without ending the
/// server process.
#[derive(Debug, ThisError)]
pub enum TelegramPollingError {
    #[error("Telegram polling API request failed: {0}")]
    Api(#[source] teloxide::RequestError),
    #[error("Telegram storage preflight failed: {0}")]
    Storage(#[source] StorageUploadApiError),
}

/// The small control-plane surface required by [`TelegramRuntime`].  Keeping
/// this separate from `TeloxideApi` makes supervisor tests deterministic and
/// ensures polling failures are not coupled to the HTTP server lifetime.
#[async_trait]
pub trait TelegramPollingApi: TelegramApi {
    type PollingError: Error + Send + Sync + 'static;

    async fn verify_storage_chat(&self, chat_id: i64) -> Result<(), Self::PollingError>;

    async fn delete_webhook(&self) -> Result<(), Self::PollingError>;

    async fn get_updates(
        &self,
        offset: i32,
        timeout_seconds: u32,
    ) -> Result<Vec<Update>, Self::PollingError>;

    /// Invalid tokens and static storage-chat permissions/configuration are
    /// terminal until local configuration or the remote account is fixed.
    fn is_terminal_error(error: &Self::PollingError) -> bool;

    fn retry_after(_error: &Self::PollingError) -> Option<Duration> {
        None
    }
}

#[derive(Debug, ThisError)]
pub enum TelegramApiError {
    #[error("Telegram API request failed: {0}")]
    Api(#[source] teloxide::RequestError),
    #[error("Telegram file download failed: {0}")]
    Download(#[source] teloxide::DownloadError),
    #[error("Telegram download destination could not be opened: {0}")]
    Io(#[source] io::Error),
    #[error("Telegram absolute file path requires a configured local Bot API file root")]
    LocalFileRootNotConfigured,
    #[error("Telegram local Bot API file root is unavailable")]
    LocalFileRootUnavailable,
    #[error("Telegram local Bot API file path is outside the configured file root")]
    LocalFilePathRejected,
    #[error("Telegram local Bot API file is not a regular file")]
    LocalFileNotRegular,
    #[error(
        "Telegram file size changed while copying: reported {reported} bytes, copied {actual} bytes"
    )]
    DownloadSizeMismatch { reported: u64, actual: u64 },
    #[error("Telegram Bot API download limit is {limit} bytes; file is {size} bytes")]
    DownloadLimit { size: u64, limit: u64 },
    #[error("Telegram storage receipt has no file reference for {media_kind:?}")]
    MissingFileReference { media_kind: MediaKind },
    #[error("Telegram publication parse mode is invalid")]
    InvalidParseMode,
    #[error("Telegram message ID is outside the supported range: {0}")]
    InvalidMessageId(i64),
}

#[async_trait]
pub trait TelegramApi: Clone + Send + Sync + 'static {
    type Error: Error + Send + Sync + 'static;

    async fn send_text(&self, chat_id: i64, text: &str) -> Result<(), Self::Error>;

    async fn answer_callback_query(&self, callback_id: &str) -> Result<(), Self::Error>;

    async fn download_file(&self, file_id: &str, destination: &Path) -> Result<(), Self::Error>;

    fn is_retryable_error(error: &Self::Error) -> bool;
}

#[derive(Clone)]
pub struct TelegramService<A, I = ()> {
    api: A,
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

#[derive(Clone)]
pub struct TeloxideApi {
    control_bot: Bot,
    media_bot: Bot,
    upload_bot: Bot,
    cloud_download_limit_bytes: Option<u64>,
    cloud_upload_limit_bytes: Option<u64>,
    source_download_max_bytes: u64,
    local_file_root: Option<PathBuf>,
}

impl TeloxideApi {
    pub fn new(
        token: impl Into<String>,
        api_base_url: &str,
        poll_timeout: Duration,
    ) -> Result<Self, TelegramError> {
        Self::new_with_upload_timeout(
            token,
            api_base_url,
            poll_timeout,
            Duration::from_secs(DEFAULT_TELEGRAM_UPLOAD_TIMEOUT_SECONDS),
        )
    }

    pub fn new_with_upload_timeout(
        token: impl Into<String>,
        api_base_url: &str,
        poll_timeout: Duration,
        upload_timeout: Duration,
    ) -> Result<Self, TelegramError> {
        if poll_timeout.is_zero() || poll_timeout.as_secs() > u64::from(u32::MAX) {
            return Err(TelegramError::InvalidPollTimeout);
        }
        if upload_timeout.is_zero() || upload_timeout > TELEGRAM_MAX_UPLOAD_TIMEOUT {
            return Err(TelegramError::InvalidUploadTimeout);
        }
        let api_base_url = Url::parse(api_base_url)
            .map_err(|error| TelegramError::InvalidApiBaseUrl(error.to_string()))?;
        if !is_safe_api_base_url(&api_base_url) {
            return Err(TelegramError::InvalidApiBaseUrl(
                "must be an HTTP(S) URL without credentials; HTTP URLs must use a private host"
                    .to_owned(),
            ));
        }
        let client_timeout = poll_timeout.checked_add(Duration::from_secs(5)).ok_or_else(|| {
            TelegramError::InvalidApiBaseUrl("poll timeout is too large".to_owned())
        })?;
        let token = token.into();
        let control_client = reqwest::Client::builder()
            .timeout(client_timeout)
            .build()
            .map_err(TelegramError::HttpClient)?;
        let media_client = reqwest::Client::builder()
            .connect_timeout(TELEGRAM_MEDIA_CONNECT_TIMEOUT)
            .read_timeout(TELEGRAM_MEDIA_READ_TIMEOUT)
            .build()
            .map_err(TelegramError::HttpClient)?;
        let is_cloud = api_base_url.host_str() == Some("api.telegram.org");
        let control_bot =
            Bot::with_client(token.clone(), control_client).set_api_url(api_base_url.clone());
        let media_bot =
            Bot::with_client(token.clone(), media_client).set_api_url(api_base_url.clone());
        let upload_bot = upload_bot_with_timeout(&control_bot, upload_timeout)?;
        Ok(Self {
            control_bot,
            media_bot,
            upload_bot,
            cloud_download_limit_bytes: is_cloud.then_some(TELEGRAM_CLOUD_DOWNLOAD_LIMIT_BYTES),
            cloud_upload_limit_bytes: is_cloud.then_some(TELEGRAM_CLOUD_UPLOAD_LIMIT_BYTES),
            source_download_max_bytes: DEFAULT_TELEGRAM_SOURCE_DOWNLOAD_MAX_BYTES,
            local_file_root: None,
        })
    }

    pub fn with_source_download_max_bytes(mut self, max_bytes: u64) -> Self {
        self.source_download_max_bytes = max_bytes;
        self
    }

    pub fn with_local_file_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.local_file_root = Some(root.into());
        self
    }

    pub fn with_upload_timeout(mut self, upload_timeout: Duration) -> Result<Self, TelegramError> {
        self.upload_bot = upload_bot_with_timeout(&self.control_bot, upload_timeout)?;
        Ok(self)
    }

    fn bot(&self) -> Bot {
        self.control_bot.clone()
    }

    fn media_bot(&self) -> Bot {
        self.media_bot.clone()
    }

    fn upload_bot(&self) -> Bot {
        self.upload_bot.clone()
    }

    #[cfg(test)]
    fn with_test_media_read_timeout(
        mut self,
        read_timeout: Duration,
    ) -> Result<Self, TelegramError> {
        let client = reqwest::Client::builder()
            .connect_timeout(TELEGRAM_MEDIA_CONNECT_TIMEOUT)
            .read_timeout(read_timeout)
            .build()
            .map_err(TelegramError::HttpClient)?;
        self.media_bot = Bot::with_client(self.media_bot.token().to_owned(), client)
            .set_api_url(self.media_bot.api_url());
        Ok(self)
    }

    #[cfg(test)]
    fn with_test_cloud_limits(mut self, limit: u64) -> Self {
        self.cloud_download_limit_bytes = Some(limit);
        self.cloud_upload_limit_bytes = Some(limit);
        self
    }
}

fn upload_bot_with_timeout(bot: &Bot, upload_timeout: Duration) -> Result<Bot, TelegramError> {
    if upload_timeout.is_zero() || upload_timeout > TELEGRAM_MAX_UPLOAD_TIMEOUT {
        return Err(TelegramError::InvalidUploadTimeout);
    }
    let client = reqwest::Client::builder()
        .connect_timeout(TELEGRAM_MEDIA_CONNECT_TIMEOUT)
        .timeout(upload_timeout)
        .build()
        .map_err(TelegramError::HttpClient)?;
    Ok(Bot::with_client(bot.token().to_owned(), client).set_api_url(bot.api_url()))
}

fn is_safe_api_base_url(url: &Url) -> bool {
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return false;
    }
    if url.scheme() == "https" {
        return true;
    }

    let Some(host) = url.host_str() else { return false };
    match host.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V4(address)) => {
            !address.is_unspecified()
                && (address.is_loopback() || address.is_private() || address.is_link_local())
        }
        Ok(std::net::IpAddr::V6(address)) => {
            !address.is_unspecified()
                && (address.is_loopback()
                    || address.is_unique_local()
                    || address.is_unicast_link_local())
        }
        Err(_) => {
            host.eq_ignore_ascii_case("localhost")
                || !host.contains('.')
                || host.ends_with(".local")
        }
    }
}

#[async_trait]
impl TelegramApi for TeloxideApi {
    type Error = TelegramApiError;

    fn is_retryable_error(error: &Self::Error) -> bool {
        match error {
            TelegramApiError::Api(teloxide::RequestError::Api(
                teloxide::errors::ApiError::Unknown(_),
            ))
            | TelegramApiError::Api(teloxide::RequestError::Network(_))
            | TelegramApiError::Api(teloxide::RequestError::InvalidJson { .. })
            | TelegramApiError::Api(teloxide::RequestError::RetryAfter(_))
            | TelegramApiError::Download(teloxide::DownloadError::Network(_)) => true,
            TelegramApiError::DownloadLimit { .. }
            | TelegramApiError::MissingFileReference { .. }
            | TelegramApiError::InvalidParseMode
            | TelegramApiError::InvalidMessageId(_)
            | TelegramApiError::LocalFileRootNotConfigured
            | TelegramApiError::LocalFileRootUnavailable
            | TelegramApiError::LocalFilePathRejected
            | TelegramApiError::LocalFileNotRegular
            | TelegramApiError::DownloadSizeMismatch { .. }
            | TelegramApiError::Api(_)
            | TelegramApiError::Download(_)
            | TelegramApiError::Io(_) => false,
        }
    }

    async fn send_text(&self, chat_id: i64, text: &str) -> Result<(), Self::Error> {
        self.bot()
            .send_message(ChatId(chat_id), text.to_owned())
            .await
            .map(|_| ())
            .map_err(TelegramApiError::Api)
    }

    async fn answer_callback_query(&self, callback_id: &str) -> Result<(), Self::Error> {
        self.bot()
            .answer_callback_query(CallbackQueryId(callback_id.to_owned()))
            .await
            .map(|_| ())
            .map_err(TelegramApiError::Api)
    }

    async fn download_file(&self, file_id: &str, destination: &Path) -> Result<(), Self::Error> {
        let file = self
            .media_bot()
            .get_file(FileId(file_id.to_owned()))
            .send()
            .await
            .map_err(TelegramApiError::Api)?;
        let size = u64::from(file.meta.size);
        if Path::new(&file.path).is_absolute() {
            let limit = self.source_download_max_bytes;
            if size > limit {
                return Err(TelegramApiError::DownloadLimit { size, limit });
            }
            let local_root = self
                .local_file_root
                .as_deref()
                .ok_or(TelegramApiError::LocalFileRootNotConfigured)?;
            let local_file = resolve_local_file(local_root, Path::new(&file.path)).await?;
            if local_file.size > limit {
                return Err(TelegramApiError::DownloadLimit { size: local_file.size, limit });
            }
            if local_file.size != size {
                return Err(TelegramApiError::DownloadSizeMismatch {
                    reported: size,
                    actual: local_file.size,
                });
            }
            return copy_local_file(&local_file.path, destination, size, limit).await;
        }
        let limit =
            self.cloud_download_limit_bytes.map_or(self.source_download_max_bytes, |cloud_limit| {
                cloud_limit.min(self.source_download_max_bytes)
            });
        if size > limit {
            return Err(TelegramApiError::DownloadLimit { size, limit });
        }
        let (temporary, mut output) = open_temporary_download(destination).await?;
        let (download_result, exceeded, written) = {
            let mut limited_output = LimitedWriter::new(&mut output, limit);
            let download_result = teloxide::net::Download::download_file(
                &self.media_bot(),
                &file.path,
                &mut limited_output,
            )
            .await;
            (download_result, limited_output.exceeded(), limited_output.written())
        };
        if exceeded {
            return Err(TelegramApiError::DownloadLimit { size: written.saturating_add(1), limit });
        }
        if let Err(error) = download_result {
            return Err(TelegramApiError::Download(error));
        }
        finish_temporary_download(temporary, output, destination, size, limit).await
    }
}

#[async_trait]
impl TelegramPollingApi for TeloxideApi {
    type PollingError = TelegramPollingError;

    async fn verify_storage_chat(&self, chat_id: i64) -> Result<(), Self::PollingError> {
        <Self as TelegramStorageApi>::verify_storage_chat(self, chat_id)
            .await
            .map_err(TelegramPollingError::Storage)
    }

    async fn delete_webhook(&self) -> Result<(), Self::PollingError> {
        self.bot().delete_webhook().send().await.map(|_| ()).map_err(TelegramPollingError::Api)
    }

    async fn get_updates(
        &self,
        offset: i32,
        timeout_seconds: u32,
    ) -> Result<Vec<Update>, Self::PollingError> {
        self.bot()
            .get_updates()
            .offset(offset)
            .timeout(timeout_seconds)
            .send()
            .await
            .map_err(TelegramPollingError::Api)
    }

    fn is_terminal_error(error: &Self::PollingError) -> bool {
        matches!(
            error,
            TelegramPollingError::Api(teloxide::RequestError::Api(
                teloxide::errors::ApiError::InvalidToken,
            )) | TelegramPollingError::Storage(
                StorageUploadApiError::StorageChatNotPrivateChannel
                    | StorageUploadApiError::StorageBotNotAdministrator
                    | StorageUploadApiError::StorageBotCannotPost,
            ) | TelegramPollingError::Storage(StorageUploadApiError::Api(
                teloxide::RequestError::Api(teloxide::errors::ApiError::InvalidToken),
            ))
        )
    }

    fn retry_after(error: &Self::PollingError) -> Option<Duration> {
        let TelegramPollingError::Api(teloxide::RequestError::RetryAfter(seconds)) = error else {
            return None;
        };
        Some(seconds.duration())
    }
}

struct LocalFile {
    path: PathBuf,
    size: u64,
}

async fn resolve_local_file(root: &Path, candidate: &Path) -> Result<LocalFile, TelegramApiError> {
    if candidate.components().any(|component| matches!(component, Component::ParentDir)) {
        return Err(TelegramApiError::LocalFilePathRejected);
    }
    let root = tokio::fs::canonicalize(root)
        .await
        .map_err(|_| TelegramApiError::LocalFileRootUnavailable)?;
    let root_metadata =
        tokio::fs::metadata(&root).await.map_err(|_| TelegramApiError::LocalFileRootUnavailable)?;
    if !root_metadata.is_dir() {
        return Err(TelegramApiError::LocalFileRootUnavailable);
    }
    let candidate = tokio::fs::canonicalize(candidate)
        .await
        .map_err(|_| TelegramApiError::LocalFilePathRejected)?;
    if candidate.strip_prefix(&root).is_err() {
        return Err(TelegramApiError::LocalFilePathRejected);
    }
    let metadata = tokio::fs::metadata(&candidate)
        .await
        .map_err(|_| TelegramApiError::LocalFilePathRejected)?;
    if !metadata.is_file() {
        return Err(TelegramApiError::LocalFileNotRegular);
    }
    Ok(LocalFile { path: candidate, size: metadata.len() })
}

async fn copy_local_file(
    source: &Path,
    destination: &Path,
    expected_size: u64,
    limit: u64,
) -> Result<(), TelegramApiError> {
    let mut input = tokio::fs::File::open(source).await.map_err(TelegramApiError::Io)?;
    let (temporary, mut output) = open_temporary_download(destination).await?;
    let (copy_result, exceeded, written) = {
        let mut limited_output = LimitedWriter::new(&mut output, limit);
        let copy_result = tokio::io::copy(&mut input, &mut limited_output).await;
        (copy_result, limited_output.exceeded(), limited_output.written())
    };
    if exceeded {
        return Err(TelegramApiError::DownloadLimit { size: written.saturating_add(1), limit });
    }
    if let Err(error) = copy_result {
        return Err(TelegramApiError::Io(error));
    }
    finish_temporary_download(temporary, output, destination, expected_size, limit).await
}

async fn open_temporary_download(
    destination: &Path,
) -> Result<(TemporaryDownload, tokio::fs::File), TelegramApiError> {
    let temporary_path =
        destination.with_file_name(format!(".sooqa-download-{}.tmp", Uuid::new_v4()));
    let temporary = TemporaryDownload::new(temporary_path);
    let output = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temporary.path())
        .await
        .map_err(TelegramApiError::Io)?;
    Ok((temporary, output))
}

async fn finish_temporary_download(
    mut temporary: TemporaryDownload,
    mut output: tokio::fs::File,
    destination: &Path,
    expected_size: u64,
    limit: u64,
) -> Result<(), TelegramApiError> {
    if let Err(error) = tokio::io::AsyncWriteExt::flush(&mut output).await {
        return Err(TelegramApiError::Io(error));
    }
    drop(output);
    let actual_size =
        tokio::fs::metadata(temporary.path()).await.map_err(TelegramApiError::Io)?.len();
    if actual_size > limit {
        return Err(TelegramApiError::DownloadLimit { size: actual_size, limit });
    }
    if actual_size != expected_size {
        return Err(TelegramApiError::DownloadSizeMismatch {
            reported: expected_size,
            actual: actual_size,
        });
    }
    tokio::fs::rename(temporary.path(), destination).await.map_err(TelegramApiError::Io)?;
    temporary.commit();
    Ok(())
}

struct TemporaryDownload {
    path: PathBuf,
    committed: bool,
}

impl TemporaryDownload {
    fn new(path: PathBuf) -> Self {
        Self { path, committed: false }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for TemporaryDownload {
    fn drop(&mut self) {
        if !self.committed {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

struct LimitedWriter<'a> {
    inner: &'a mut tokio::fs::File,
    limit: u64,
    written: u64,
    exceeded: bool,
}

impl<'a> LimitedWriter<'a> {
    fn new(inner: &'a mut tokio::fs::File, limit: u64) -> Self {
        Self { inner, limit, written: 0, exceeded: false }
    }

    fn exceeded(&self) -> bool {
        self.exceeded
    }

    fn written(&self) -> u64 {
        self.written
    }
}

impl tokio::io::AsyncWrite for LimitedWriter<'_> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let remaining = self.limit.saturating_sub(self.written);
        if buffer.len() as u64 > remaining {
            self.exceeded = true;
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Telegram download exceeds the configured limit",
            )));
        }
        let result = Pin::new(&mut *self.inner).poll_write(context, buffer);
        if let Poll::Ready(Ok(written)) = result {
            self.written = self.written.saturating_add(written as u64);
        }
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.inner).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.inner).poll_shutdown(context)
    }
}

pub struct TelegramRuntime<I, A = TeloxideApi> {
    api: A,
    service: TelegramService<A, I>,
    poll_timeout: Duration,
    storage_chat_id: Option<i64>,
    retry_initial_delay: Duration,
    retry_max_delay: Duration,
}

impl<I> TelegramRuntime<I, TeloxideApi>
where
    I: IngestService,
{
    pub fn new(
        token: impl Into<String>,
        api_base_url: &str,
        poll_timeout: Duration,
        admin_user_ids: impl IntoIterator<Item = i64>,
        storage_chat_id: Option<i64>,
        ingest_service: I,
    ) -> Result<Self, TelegramError> {
        let api = TeloxideApi::new(token, api_base_url, poll_timeout)?;
        Ok(Self::new_with_api(api, poll_timeout, admin_user_ids, storage_chat_id, ingest_service))
    }

    pub fn with_upload_timeout(mut self, upload_timeout: Duration) -> Result<Self, TelegramError> {
        self.api = self.api.with_upload_timeout(upload_timeout)?;
        self.service.api = self.api.clone();
        Ok(self)
    }

    pub fn with_source_download_max_bytes(mut self, max_bytes: u64) -> Self {
        self.api = self.api.with_source_download_max_bytes(max_bytes);
        self.service.api = self.api.clone();
        self.service = self.service.with_source_download_max_bytes(max_bytes);
        self
    }
}

impl<I, A> TelegramRuntime<I, A>
where
    A: TelegramApi + TelegramPollingApi,
    I: IngestService,
{
    pub fn new_with_api(
        api: A,
        poll_timeout: Duration,
        admin_user_ids: impl IntoIterator<Item = i64>,
        storage_chat_id: Option<i64>,
        ingest_service: I,
    ) -> Self {
        let service = TelegramService::with_ingest(api.clone(), admin_user_ids, ingest_service);
        Self {
            api,
            service,
            poll_timeout,
            storage_chat_id,
            retry_initial_delay: RETRY_DELAY,
            retry_max_delay: MAX_POLL_RETRY_DELAY,
        }
    }

    pub fn with_polling_backoff(
        mut self,
        initial_delay: Duration,
        max_delay: Duration,
    ) -> Result<Self, TelegramError> {
        if initial_delay.is_zero() || max_delay.is_zero() || initial_delay > max_delay {
            return Err(TelegramError::InvalidPollingBackoff);
        }
        self.retry_initial_delay = initial_delay;
        self.retry_max_delay = max_delay;
        Ok(self)
    }

    /// Run the Telegram control plane until Ctrl-C.  The server process uses
    /// [`Self::run_with_shutdown`] so the HTTP server owns the process signal
    /// and can drain before this task exits.
    pub async fn run(self) -> Result<(), TelegramError> {
        let shutdown = async {
            if let Err(error) = tokio::signal::ctrl_c().await {
                tracing::error!(
                    target: "sooqa.telegram",
                    status = "shutdown_error",
                    ?error,
                    "could not listen for Ctrl-C while supervising Telegram"
                );
            }
        };
        self.run_with_shutdown(shutdown).await
    }

    pub async fn run_with_shutdown<F>(self, shutdown: F) -> Result<(), TelegramError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if self.poll_timeout.is_zero() || self.poll_timeout.as_secs() > u64::from(u32::MAX) {
            return Err(TelegramError::InvalidPollTimeout);
        }
        let poll_timeout = self.poll_timeout.as_secs() as u32;
        let mut shutdown = Box::pin(shutdown);
        let mut backoff = PollBackoff::new(self.retry_initial_delay, self.retry_max_delay);

        if let Some(storage_chat_id) = self.storage_chat_id {
            let mut failures = 0_usize;
            loop {
                let result = tokio::select! {
                    _ = &mut shutdown => return Ok(()),
                    result = self.api.verify_storage_chat(storage_chat_id) => result,
                };
                match result {
                    Ok(()) => {
                        if failures > 0 {
                            tracing::info!(
                                target: "sooqa.telegram",
                                status = "recovered",
                                phase = "storage_preflight",
                                attempts = failures,
                                storage_chat_id,
                                "Telegram storage preflight recovered"
                            );
                        } else {
                            tracing::info!(
                                target: "sooqa.telegram",
                                status = "ready",
                                phase = "storage_preflight",
                                storage_chat_id,
                                "Telegram storage chat is reachable"
                            );
                        }
                        backoff.reset();
                        break;
                    }
                    Err(error) if A::is_terminal_error(&error) => {
                        tracing::error!(
                            target: "sooqa.telegram",
                            status = "terminally_misconfigured",
                            phase = "storage_preflight",
                            storage_chat_id,
                            error = %error,
                            "Telegram storage configuration or authentication is not usable"
                        );
                        return Err(TelegramError::Api(Box::new(error)));
                    }
                    Err(error) => {
                        failures += 1;
                        let delay = backoff.next(A::retry_after(&error));
                        tracing::warn!(
                            target: "sooqa.telegram",
                            status = if failures == 1 { "degraded" } else { "retrying" },
                            phase = "storage_preflight",
                            attempt = failures,
                            retry_in_ms = delay.as_millis() as u64,
                            storage_chat_id,
                            error = %error,
                            "Telegram storage preflight unavailable; retrying"
                        );
                        if !wait_for_retry(&mut shutdown, delay).await {
                            return Ok(());
                        }
                    }
                }
            }
        }

        let mut failures = 0_usize;
        loop {
            let result = tokio::select! {
                _ = &mut shutdown => return Ok(()),
                result = self.api.delete_webhook() => result,
            };
            match result {
                Ok(()) => {
                    if failures > 0 {
                        tracing::info!(
                            target: "sooqa.telegram",
                            status = "recovered",
                            phase = "startup",
                            attempts = failures,
                            "Telegram startup control call recovered"
                        );
                    }
                    backoff.reset();
                    break;
                }
                Err(error) if A::is_terminal_error(&error) => {
                    tracing::error!(
                        target: "sooqa.telegram",
                        status = "terminally_misconfigured",
                        phase = "startup",
                        error = %error,
                        "Telegram startup control call is not usable"
                    );
                    return Err(TelegramError::Api(Box::new(error)));
                }
                Err(error) => {
                    failures += 1;
                    let delay = backoff.next(A::retry_after(&error));
                    tracing::warn!(
                        target: "sooqa.telegram",
                        status = if failures == 1 { "degraded" } else { "retrying" },
                        phase = "startup",
                        attempt = failures,
                        retry_in_ms = delay.as_millis() as u64,
                        error = %error,
                        "Telegram startup control call unavailable; retrying"
                    );
                    if !wait_for_retry(&mut shutdown, delay).await {
                        return Ok(());
                    }
                }
            }
        }

        let service = self.service;
        let mut offset = 0_i32;
        let mut polling_failures = 0_usize;

        loop {
            let result = tokio::select! {
                _ = &mut shutdown => return Ok(()),
                result = self.api.get_updates(offset, poll_timeout) => result,
            };
            match result {
                Ok(updates) => {
                    if polling_failures > 0 {
                        tracing::info!(
                            target: "sooqa.telegram",
                            status = "recovered",
                            phase = "polling",
                            attempts = polling_failures,
                            offset,
                            "Telegram polling recovered"
                        );
                    }
                    polling_failures = 0;
                    backoff.reset();
                    for update in updates {
                        match handle_update_with_retries(&service, update).await {
                            Ok(next_offset) => offset = next_offset,
                            Err(error) if is_terminal_bot_error(&error) => {
                                tracing::error!(
                                    target: "sooqa.telegram",
                                    status = "terminally_misconfigured",
                                    phase = "update",
                                    offset,
                                    error = %error,
                                    "Telegram update handling reached a terminal configuration error"
                                );
                                return Err(error);
                            }
                            Err(error) => {
                                polling_failures += 1;
                                let delay = backoff.next(Some(retry_delay(&error)));
                                tracing::warn!(
                                    target: "sooqa.telegram",
                                    status = if polling_failures == 1 { "degraded" } else { "retrying" },
                                    phase = "update",
                                    attempt = polling_failures,
                                    retry_in_ms = delay.as_millis() as u64,
                                    offset,
                                    error = %error,
                                    "Telegram update handling failed; retaining offset"
                                );
                                if !wait_for_retry(&mut shutdown, delay).await {
                                    return Ok(());
                                }
                                break;
                            }
                        }
                    }
                }
                Err(error) if A::is_terminal_error(&error) => {
                    tracing::error!(
                        target: "sooqa.telegram",
                        status = "terminally_misconfigured",
                        phase = "polling",
                        offset,
                        error = %error,
                        "Telegram polling stopped because remote authentication or configuration is invalid"
                    );
                    return Err(TelegramError::Api(Box::new(error)));
                }
                Err(error) => {
                    polling_failures += 1;
                    let delay = backoff.next(A::retry_after(&error));
                    tracing::warn!(
                        target: "sooqa.telegram",
                        status = if polling_failures == 1 { "degraded" } else { "retrying" },
                        phase = "polling",
                        attempt = polling_failures,
                        retry_in_ms = delay.as_millis() as u64,
                        offset,
                        error = %error,
                        "Telegram polling failed; retaining offset and retrying"
                    );
                    if !wait_for_retry(&mut shutdown, delay).await {
                        return Ok(());
                    }
                }
            }
        }
    }
}

struct PollBackoff {
    next_delay: Duration,
    initial_delay: Duration,
    max_delay: Duration,
}

impl PollBackoff {
    fn new(initial_delay: Duration, max_delay: Duration) -> Self {
        Self { next_delay: initial_delay, initial_delay, max_delay }
    }

    fn reset(&mut self) {
        self.next_delay = self.initial_delay;
    }

    fn next(&mut self, suggested: Option<Duration>) -> Duration {
        let delay = self.next_delay.max(suggested.unwrap_or_default());
        self.next_delay =
            self.next_delay.checked_mul(2).unwrap_or(self.max_delay).min(self.max_delay);
        delay
    }
}

async fn wait_for_retry<F>(shutdown: &mut Pin<Box<F>>, delay: Duration) -> bool
where
    F: Future<Output = ()> + Send + 'static,
{
    tokio::select! {
        _ = shutdown.as_mut() => false,
        _ = tokio::time::sleep(delay) => true,
    }
}

async fn handle_update_with_retries<A, I>(
    service: &TelegramService<A, I>,
    update: Update,
) -> Result<i32, TelegramError>
where
    A: TelegramApi,
    I: IngestService,
{
    for attempt in 1..=MAX_HANDLER_ATTEMPTS {
        match service.handle_update(update.clone()).await {
            Ok(_) => return Ok(update.id.as_offset()),
            Err(error) if attempt < MAX_HANDLER_ATTEMPTS => {
                tracing::warn!(
                    ?error,
                    update_id = update.id.0,
                    attempt,
                    "Telegram update handling failed; retrying before advancing offset"
                );
                tokio::time::sleep(retry_delay(&error)).await;
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("the bounded Telegram retry loop always returns")
}

fn is_terminal_bot_error(error: &TelegramError) -> bool {
    let TelegramError::Api(source) = error else { return false };
    source.downcast_ref::<teloxide::RequestError>().is_some_and(|error| {
        matches!(error, teloxide::RequestError::Api(teloxide::ApiError::InvalidToken))
    }) || source.downcast_ref::<TelegramApiError>().is_some_and(|error| {
        matches!(
            error,
            TelegramApiError::Api(teloxide::RequestError::Api(teloxide::ApiError::InvalidToken))
        )
    })
}

fn retry_delay(error: &TelegramError) -> Duration {
    let TelegramError::Api(source) = error else { return RETRY_DELAY };
    source
        .downcast_ref::<teloxide::RequestError>()
        .and_then(|error| match error {
            teloxide::RequestError::RetryAfter(seconds) => Some(seconds.duration()),
            _ => None,
        })
        .or_else(|| {
            source.downcast_ref::<TelegramApiError>().and_then(|error| match error {
                TelegramApiError::Api(teloxide::RequestError::RetryAfter(seconds)) => {
                    Some(seconds.duration())
                }
                _ => None,
            })
        })
        .unwrap_or(RETRY_DELAY)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tokio::{
        io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
        net::{TcpListener, TcpStream},
    };

    use super::*;

    #[test]
    fn polling_backoff_honors_local_growth_and_retry_after_minimums() {
        let cases = vec![
            (
                "local exponential growth and cap",
                Duration::from_secs(1),
                Duration::from_secs(4),
                vec![None, None, None, None],
                vec![1, 2, 4, 4],
            ),
            (
                "short retry-after cannot bypass local backoff",
                Duration::from_secs(4),
                Duration::from_secs(8),
                vec![Some(Duration::from_secs(1)), Some(Duration::from_secs(1)), None],
                vec![4, 8, 8],
            ),
            (
                "long retry-after remains the minimum",
                Duration::from_secs(1),
                Duration::from_secs(4),
                vec![Some(Duration::from_secs(120)), None],
                vec![120, 2],
            ),
        ];

        for (name, initial, maximum, suggestions, expected_seconds) in cases {
            let mut backoff = PollBackoff::new(initial, maximum);
            for (suggestion, expected_seconds) in suggestions.into_iter().zip(expected_seconds) {
                assert_eq!(
                    backoff.next(suggestion),
                    Duration::from_secs(expected_seconds),
                    "{name}"
                );
            }
        }
    }

    #[derive(Clone, Default)]
    struct MockApi {
        messages: Arc<Mutex<Vec<(i64, String)>>>,
        callback_answers: Arc<Mutex<Vec<String>>>,
        downloads: Arc<Mutex<Vec<String>>>,
        fail: Arc<Mutex<bool>>,
        failures_remaining: Arc<Mutex<u8>>,
    }

    #[derive(Debug, ThisError)]
    #[error("mock failure")]
    struct MockError;

    #[async_trait]
    impl TelegramApi for MockApi {
        type Error = MockError;

        async fn send_text(&self, chat_id: i64, text: &str) -> Result<(), Self::Error> {
            let fail = *self.fail.lock().expect("mock mutex should not be poisoned");
            let mut failures_remaining =
                self.failures_remaining.lock().expect("mock mutex should not be poisoned");
            if fail || *failures_remaining > 0 {
                *failures_remaining = failures_remaining.saturating_sub(1);
                return Err(MockError);
            }
            self.messages
                .lock()
                .expect("mock mutex should not be poisoned")
                .push((chat_id, text.to_owned()));
            Ok(())
        }

        async fn answer_callback_query(&self, callback_id: &str) -> Result<(), Self::Error> {
            self.callback_answers
                .lock()
                .expect("mock mutex should not be poisoned")
                .push(callback_id.to_owned());
            Ok(())
        }

        async fn download_file(
            &self,
            file_id: &str,
            destination: &Path,
        ) -> Result<(), Self::Error> {
            self.downloads
                .lock()
                .expect("mock mutex should not be poisoned")
                .push(file_id.to_owned());
            tokio::fs::write(destination, b"mock media")
                .await
                .expect("mock media should be written");
            Ok(())
        }

        fn is_retryable_error(_error: &Self::Error) -> bool {
            false
        }
    }

    #[derive(Clone, Default)]
    struct BlockingDownloadApi;

    #[async_trait]
    impl TelegramApi for BlockingDownloadApi {
        type Error = MockError;

        async fn send_text(&self, _chat_id: i64, _text: &str) -> Result<(), Self::Error> {
            Ok(())
        }

        async fn answer_callback_query(&self, _callback_id: &str) -> Result<(), Self::Error> {
            Ok(())
        }

        async fn download_file(
            &self,
            _file_id: &str,
            _destination: &Path,
        ) -> Result<(), Self::Error> {
            std::future::pending().await
        }

        fn is_retryable_error(_error: &Self::Error) -> bool {
            true
        }
    }

    #[derive(Clone, Default)]
    struct MockIngestService {
        commands: Arc<Mutex<Vec<UrlIngestCommand>>>,
        media_commands: Arc<Mutex<Vec<MediaIngestCommand>>>,
        accepted_keys: Arc<Mutex<BTreeSet<String>>>,
        fail: Arc<Mutex<bool>>,
    }

    #[async_trait]
    impl IngestService for MockIngestService {
        type Error = MockError;

        async fn create_url(
            &self,
            command: UrlIngestCommand,
        ) -> Result<IngestAccepted, Self::Error> {
            if *self.fail.lock().expect("mock mutex should not be poisoned") {
                return Err(MockError);
            }
            if !self
                .accepted_keys
                .lock()
                .expect("mock mutex should not be poisoned")
                .insert(command.idempotency_key.clone())
            {
                return Ok(IngestAccepted {
                    request_id: Uuid::from_u128(1),
                    status: "queued".to_owned(),
                });
            }
            self.commands.lock().expect("mock mutex should not be poisoned").push(command);
            Ok(IngestAccepted { request_id: Uuid::from_u128(1), status: "queued".to_owned() })
        }

        async fn create_media(
            &self,
            command: MediaIngestCommand,
        ) -> Result<IngestAccepted, Self::Error> {
            if *self.fail.lock().expect("mock mutex should not be poisoned") {
                return Err(MockError);
            }
            if !self
                .accepted_keys
                .lock()
                .expect("mock mutex should not be poisoned")
                .insert(command.idempotency_key.clone())
            {
                return Ok(IngestAccepted {
                    request_id: Uuid::from_u128(1),
                    status: "queued".to_owned(),
                });
            }
            self.media_commands.lock().expect("mock mutex should not be poisoned").push(command);
            Ok(IngestAccepted { request_id: Uuid::from_u128(1), status: "queued".to_owned() })
        }
    }

    fn message(update_id: i64, user_id: Option<i64>, text: &str) -> IncomingMessage {
        IncomingMessage {
            update_id,
            message_id: update_id,
            reply_to_message_id: None,
            user_id,
            chat_id: 42,
            is_private: true,
            text: Some(text.to_owned()),
            caption: None,
            media: None,
        }
    }

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
    async fn ingest_failure_is_retryable() {
        let api = MockApi::default();
        let ingest = MockIngestService::default();
        *ingest.fail.lock().unwrap() = true;
        let service = TelegramService::with_ingest(api.clone(), [123], ingest.clone());

        assert!(
            service
                .handle_message(message(8, Some(123), "https://example.test/video.webm"))
                .await
                .is_err()
        );
        *ingest.fail.lock().unwrap() = false;
        assert_eq!(
            service
                .handle_message(message(8, Some(123), "https://example.test/video.webm"))
                .await
                .unwrap(),
            HandleOutcome::Responded(Command::Add)
        );
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
    async fn failed_update_retries_before_offset_advances() {
        let api = MockApi::default();
        *api.failures_remaining.lock().unwrap() = 1;
        let service = TelegramService::new(api.clone(), [123]);
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

        assert_eq!(handle_update_with_retries(&service, update).await.unwrap(), 8);
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

    #[derive(Debug)]
    struct HttpRequestSummary {
        target: String,
        body_bytes: u64,
        body_contains_marker: bool,
    }

    async fn read_http_request(
        reader: &mut BufReader<TcpStream>,
    ) -> std::io::Result<HttpRequestSummary> {
        read_http_request_with_options(reader, Duration::ZERO, None).await
    }

    async fn read_http_request_with_options(
        reader: &mut BufReader<TcpStream>,
        body_delay: Duration,
        marker: Option<&[u8]>,
    ) -> std::io::Result<HttpRequestSummary> {
        let mut line = Vec::new();
        reader.read_until(b'\n', &mut line).await?;
        if line.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "request line is missing",
            ));
        }
        let request_line = String::from_utf8_lossy(&line);
        let mut parts = request_line.split_whitespace();
        let _method = parts.next().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "request method is missing")
        })?;
        let target = parts
            .next()
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "request path is missing")
            })?
            .to_owned();

        let mut content_length = None;
        let mut chunked = false;
        loop {
            line.clear();
            reader.read_until(b'\n', &mut line).await?;
            if line == b"\r\n" || line == b"\n" {
                break;
            }
            let header = String::from_utf8_lossy(&line);
            let Some((name, value)) = header.split_once(':') else { continue };
            if name.eq_ignore_ascii_case("content-length") {
                content_length = Some(value.trim().parse::<u64>().map_err(|error| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, error)
                })?);
            } else if name.eq_ignore_ascii_case("transfer-encoding") {
                chunked = value
                    .split(',')
                    .any(|encoding| encoding.trim().eq_ignore_ascii_case("chunked"));
            }
        }

        let mut marker_position = 0;
        let mut body_contains_marker = marker.is_some_and(|marker| marker.is_empty());
        let body_bytes = if chunked {
            read_chunked_body(
                reader,
                body_delay,
                marker,
                &mut marker_position,
                &mut body_contains_marker,
            )
            .await?
        } else if let Some(content_length) = content_length {
            read_exact_bytes(
                reader,
                content_length,
                body_delay,
                marker,
                &mut marker_position,
                &mut body_contains_marker,
            )
            .await?
        } else {
            0
        };
        Ok(HttpRequestSummary { target, body_bytes, body_contains_marker })
    }

    async fn read_exact_bytes(
        reader: &mut BufReader<TcpStream>,
        mut remaining: u64,
        body_delay: Duration,
        marker: Option<&[u8]>,
        marker_position: &mut usize,
        body_contains_marker: &mut bool,
    ) -> std::io::Result<u64> {
        let mut buffer = [0_u8; 8 * 1024];
        let mut read = 0_u64;
        while remaining > 0 {
            let chunk = remaining.min(buffer.len() as u64) as usize;
            reader.read_exact(&mut buffer[..chunk]).await?;
            observe_marker(&buffer[..chunk], marker, marker_position, body_contains_marker);
            if !body_delay.is_zero() {
                tokio::time::sleep(body_delay).await;
            }
            remaining -= chunk as u64;
            read += chunk as u64;
        }
        Ok(read)
    }

    fn observe_marker(
        bytes: &[u8],
        marker: Option<&[u8]>,
        marker_position: &mut usize,
        body_contains_marker: &mut bool,
    ) {
        let Some(marker) = marker else { return };
        if marker.is_empty() || *body_contains_marker {
            *body_contains_marker = true;
            return;
        }
        for byte in bytes {
            if *byte == marker[*marker_position] {
                *marker_position += 1;
                if *marker_position == marker.len() {
                    *body_contains_marker = true;
                    return;
                }
            } else {
                *marker_position = usize::from(*byte == marker[0]);
            }
        }
    }

    async fn read_chunked_body(
        reader: &mut BufReader<TcpStream>,
        body_delay: Duration,
        marker: Option<&[u8]>,
        marker_position: &mut usize,
        body_contains_marker: &mut bool,
    ) -> std::io::Result<u64> {
        let mut line = Vec::new();
        let mut read = 0_u64;
        loop {
            line.clear();
            reader.read_until(b'\n', &mut line).await?;
            let size = String::from_utf8_lossy(&line);
            let size = size.split(';').next().unwrap_or_default().trim();
            let size = u64::from_str_radix(size, 16)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
            if size == 0 {
                loop {
                    line.clear();
                    reader.read_until(b'\n', &mut line).await?;
                    if line == b"\r\n" || line == b"\n" {
                        return Ok(read);
                    }
                }
            }
            read += read_exact_bytes(
                reader,
                size,
                body_delay,
                marker,
                marker_position,
                body_contains_marker,
            )
            .await?;
            let mut line_end = [0_u8; 2];
            reader.read_exact(&mut line_end).await?;
        }
    }

    async fn write_http_response(
        stream: &mut TcpStream,
        content_type: &str,
        body: &[u8],
    ) -> std::io::Result<()> {
        let headers = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(headers.as_bytes()).await?;
        stream.write_all(body).await
    }

    async fn serve_file_metadata(listener: TcpListener, file_size: usize) -> String {
        serve_file_metadata_at_path(listener, file_size, "files/payload.bin".to_owned()).await
    }

    async fn serve_file_metadata_at_path(
        listener: TcpListener,
        file_size: usize,
        file_path: String,
    ) -> String {
        let (stream, _) = listener.accept().await.expect("fake API should accept getFile");
        let mut reader = BufReader::new(stream);
        let request = read_http_request(&mut reader).await.expect("getFile should be readable");
        let response = format!(
            r#"{{"ok":true,"result":{{"file_id":"file-id","file_unique_id":"file-unique-id","file_size":{},"file_path":"{}"}}}}"#,
            file_size, file_path
        );
        let mut stream = reader.into_inner();
        write_http_response(&mut stream, "application/json", response.as_bytes())
            .await
            .expect("getFile response should be writable");
        stream.shutdown().await.expect("getFile connection should close");
        request.target
    }

    async fn serve_download(
        listener: TcpListener,
        payload: Vec<u8>,
        delay_before_payload: Duration,
    ) -> (String, String) {
        let (stream, _) = listener.accept().await.expect("fake API should accept getFile");
        let mut reader = BufReader::new(stream);
        let get_file = read_http_request(&mut reader).await.expect("getFile should be readable");
        let file_response = format!(
            r#"{{"ok":true,"result":{{"file_id":"file-id","file_unique_id":"file-unique-id","file_size":{},"file_path":"files/payload.bin"}}}}"#,
            payload.len()
        );
        let mut stream = reader.into_inner();
        write_http_response(&mut stream, "application/json", file_response.as_bytes())
            .await
            .expect("getFile response should be writable");
        stream.shutdown().await.expect("getFile connection should close");

        let (stream, _) = listener.accept().await.expect("fake API should accept file download");
        let mut reader = BufReader::new(stream);
        let file_download =
            read_http_request(&mut reader).await.expect("file download should be readable");
        let mut stream = reader.into_inner();
        let headers = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/octet-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            payload.len()
        );
        stream.write_all(headers.as_bytes()).await.expect("file headers should be writable");
        if !delay_before_payload.is_zero() {
            tokio::time::sleep(delay_before_payload).await;
        }
        for chunk in payload.chunks(3) {
            stream.write_all(chunk).await.expect("file payload should be writable");
        }
        (get_file.target, file_download.target)
    }

    async fn serve_upload(
        listener: TcpListener,
        body_delay: Duration,
        marker: &'static [u8],
    ) -> HttpRequestSummary {
        let (stream, _) = listener.accept().await.expect("fake API should accept upload");
        let mut reader = BufReader::new(stream);
        let request = read_http_request_with_options(&mut reader, body_delay, Some(marker))
            .await
            .expect("upload should be readable");
        let mut stream = reader.into_inner();
        let response = br#"{"ok":true,"result":{"message_id":7,"date":0,"chat":{"id":-100123,"type":"channel","title":"Storage"},"video":{"file_id":"stored-file-id","file_unique_id":"stored-file-unique-id","width":1,"height":1,"duration":1,"file_size":21,"mime_type":"video/mp4"}}}"#;
        let response_value: serde_json::Value =
            serde_json::from_slice(response).expect("upload fixture should deserialize");
        let video: teloxide::types::Video =
            serde_json::from_value(response_value["result"]["video"].clone())
                .expect("video fixture should deserialize");
        assert_eq!(video.file.id.0, "stored-file-id");
        let message: teloxide::types::Message = serde_json::from_value(
            response_value.get("result").expect("upload fixture should contain a result").clone(),
        )
        .expect("upload result fixture should deserialize");
        assert!(message.video().is_some(), "upload fixture should contain a video: {message:?}");
        write_http_response(&mut stream, "application/json", response)
            .await
            .expect("upload response should be writable");
        request
    }

    #[tokio::test]
    async fn teloxide_api_uses_configured_bot_api_url() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("fake API should bind");
        let address = listener.local_addr().expect("fake API address should be available");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("fake API should accept");
            let mut request = [0_u8; 4096];
            let bytes = stream.read(&mut request).await.expect("request should be readable");
            let request = String::from_utf8_lossy(&request[..bytes]).into_owned();
            let body = r#"{"ok":true,"result":{"message_id":1,"date":0,"chat":{"id":42,"type":"private","first_name":"Admin"},"text":"hello"}}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).await.expect("response should be writable");
            request
        });
        let api =
            TeloxideApi::new("test-token", &format!("http://{address}"), Duration::from_secs(1))
                .expect("fake API URL should be accepted");

        api.send_text(42, "hello").await.expect("fake API response should parse");
        let request = server.await.expect("fake API task should finish");
        assert!(
            request.contains("/bottest-token/SendMessage")
                && request.contains("\"text\":\"hello\""),
            "unexpected request: {request}"
        );
    }

    #[tokio::test]
    async fn local_bot_api_copies_confined_absolute_file_path_without_http_file_request() {
        const PAYLOAD: &[u8] = b"local transfer payload";
        const SOURCE_LIMIT: u64 = 64;
        const REDUCED_CLOUD_CEILING: u64 = 4;
        assert!(PAYLOAD.len() as u64 > REDUCED_CLOUD_CEILING);

        let directory =
            std::env::temp_dir().join(format!("sooqa-telegram-local-{}", Uuid::new_v4()));
        let root = directory.join("telegram-bot-api");
        let source = root.join("files/payload.bin");
        let output = directory.join("output");
        tokio::fs::create_dir_all(source.parent().expect("source should have a parent"))
            .await
            .expect("local Bot API fixture directory should be created");
        tokio::fs::create_dir(&output).await.expect("output directory should be created");
        tokio::fs::write(&source, PAYLOAD).await.expect("local fixture should be written");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("fake API should bind");
        let address = listener.local_addr().expect("fake API address should be available");
        let server = tokio::spawn(serve_file_metadata_at_path(
            listener,
            PAYLOAD.len(),
            source.to_string_lossy().into_owned(),
        ));
        let destination = output.join("download.webm");
        let api =
            TeloxideApi::new("test-token", &format!("http://{address}"), Duration::from_secs(1))
                .expect("fake API URL should be accepted")
                .with_source_download_max_bytes(SOURCE_LIMIT)
                .with_test_cloud_limits(REDUCED_CLOUD_CEILING)
                .with_local_file_root(root);

        api.download_file("file-id", &destination)
            .await
            .expect("local Bot API download should succeed");
        assert_eq!(
            tokio::fs::read(&destination).await.expect("download should be readable"),
            PAYLOAD
        );
        let mut entries =
            tokio::fs::read_dir(&output).await.expect("output directory should be readable");
        while let Some(entry) =
            entries.next_entry().await.expect("directory entry should be readable")
        {
            let name = entry.file_name();
            assert!(!name.to_string_lossy().starts_with(".sooqa-download-"));
        }
        let get_file_target = server.await.expect("getFile task should finish");
        assert!(get_file_target.contains("/bottest-token/GetFile"));
        assert!(!get_file_target.contains("/file/bottest-token/"));
        tokio::fs::remove_dir_all(directory).await.expect("test directory should be removed");
    }

    #[tokio::test]
    async fn absolute_local_file_path_without_root_is_terminal_and_never_downloaded_over_http() {
        let directory =
            std::env::temp_dir().join(format!("sooqa-telegram-local-{}", Uuid::new_v4()));
        let source = directory.join("telegram-bot-api/files/payload.bin");
        let output = directory.join("output");
        tokio::fs::create_dir_all(source.parent().expect("source should have a parent"))
            .await
            .expect("local Bot API fixture directory should be created");
        tokio::fs::create_dir(&output).await.expect("output directory should be created");
        tokio::fs::write(&source, b"local payload").await.expect("local fixture should be written");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("fake API should bind");
        let address = listener.local_addr().expect("fake API address should be available");
        let server = tokio::spawn(serve_file_metadata_at_path(
            listener,
            13,
            source.to_string_lossy().into_owned(),
        ));
        let destination = output.join("download.bin");
        let api =
            TeloxideApi::new("test-token", &format!("http://{address}"), Duration::from_secs(1))
                .expect("fake API URL should be accepted")
                .with_source_download_max_bytes(64);

        assert!(matches!(
            api.download_file("file-id", &destination).await,
            Err(TelegramApiError::LocalFileRootNotConfigured)
        ));
        let get_file_target = server.await.expect("getFile task should finish");
        assert!(get_file_target.contains("/bottest-token/GetFile"));
        assert!(tokio::fs::metadata(&destination).await.is_err());
        assert_no_temporary_downloads(&output).await;
        tokio::fs::remove_dir_all(directory).await.expect("test directory should be removed");
    }

    #[tokio::test]
    async fn local_file_path_must_remain_beneath_the_configured_root() {
        let directory =
            std::env::temp_dir().join(format!("sooqa-telegram-local-{}", Uuid::new_v4()));
        let root = directory.join("telegram-bot-api");
        let source = directory.join("outside/payload.bin");
        let output = directory.join("output");
        tokio::fs::create_dir_all(&root).await.expect("local root should be created");
        tokio::fs::create_dir_all(source.parent().expect("source should have a parent"))
            .await
            .expect("outside fixture directory should be created");
        tokio::fs::create_dir(&output).await.expect("output directory should be created");
        tokio::fs::write(&source, b"outside payload")
            .await
            .expect("outside fixture should be written");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("fake API should bind");
        let address = listener.local_addr().expect("fake API address should be available");
        let server = tokio::spawn(serve_file_metadata_at_path(
            listener,
            15,
            source.to_string_lossy().into_owned(),
        ));
        let destination = output.join("download.bin");
        let api =
            TeloxideApi::new("test-token", &format!("http://{address}"), Duration::from_secs(1))
                .expect("fake API URL should be accepted")
                .with_source_download_max_bytes(64)
                .with_local_file_root(root);

        assert!(matches!(
            api.download_file("file-id", &destination).await,
            Err(TelegramApiError::LocalFilePathRejected)
        ));
        server.await.expect("getFile task should finish");
        assert!(tokio::fs::metadata(&destination).await.is_err());
        assert_no_temporary_downloads(&output).await;
        tokio::fs::remove_dir_all(directory).await.expect("test directory should be removed");
    }

    #[tokio::test]
    async fn local_file_path_rejects_parent_directory_traversal() {
        let directory =
            std::env::temp_dir().join(format!("sooqa-telegram-local-{}", Uuid::new_v4()));
        let root = directory.join("telegram-bot-api");
        let source = directory.join("outside/payload.bin");
        let output = directory.join("output");
        tokio::fs::create_dir_all(&root).await.expect("local root should be created");
        tokio::fs::create_dir_all(source.parent().expect("source should have a parent"))
            .await
            .expect("outside fixture directory should be created");
        tokio::fs::create_dir(&output).await.expect("output directory should be created");
        tokio::fs::write(&source, b"outside payload")
            .await
            .expect("outside fixture should be written");
        let candidate = root.join("../outside/payload.bin");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("fake API should bind");
        let address = listener.local_addr().expect("fake API address should be available");
        let server = tokio::spawn(serve_file_metadata_at_path(
            listener,
            15,
            candidate.to_string_lossy().into_owned(),
        ));
        let destination = output.join("download.bin");
        let api =
            TeloxideApi::new("test-token", &format!("http://{address}"), Duration::from_secs(1))
                .expect("fake API URL should be accepted")
                .with_source_download_max_bytes(64)
                .with_local_file_root(root);

        assert!(matches!(
            api.download_file("file-id", &destination).await,
            Err(TelegramApiError::LocalFilePathRejected)
        ));
        server.await.expect("getFile task should finish");
        assert!(tokio::fs::metadata(&destination).await.is_err());
        assert_no_temporary_downloads(&output).await;
        tokio::fs::remove_dir_all(directory).await.expect("test directory should be removed");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_file_path_rejects_symlink_escape() {
        use std::os::unix::fs::symlink;

        let directory =
            std::env::temp_dir().join(format!("sooqa-telegram-local-{}", Uuid::new_v4()));
        let root = directory.join("telegram-bot-api");
        let outside = directory.join("outside/payload.bin");
        let link = root.join("files/escape.bin");
        let output = directory.join("output");
        tokio::fs::create_dir_all(link.parent().expect("link should have a parent"))
            .await
            .expect("local root should be created");
        tokio::fs::create_dir_all(outside.parent().expect("outside should have a parent"))
            .await
            .expect("outside fixture directory should be created");
        tokio::fs::create_dir(&output).await.expect("output directory should be created");
        tokio::fs::write(&outside, b"outside payload")
            .await
            .expect("outside fixture should be written");
        symlink(&outside, &link).expect("symlink fixture should be created");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("fake API should bind");
        let address = listener.local_addr().expect("fake API address should be available");
        let server = tokio::spawn(serve_file_metadata_at_path(
            listener,
            15,
            link.to_string_lossy().into_owned(),
        ));
        let destination = output.join("download.bin");
        let api =
            TeloxideApi::new("test-token", &format!("http://{address}"), Duration::from_secs(1))
                .expect("fake API URL should be accepted")
                .with_source_download_max_bytes(64)
                .with_local_file_root(root);

        assert!(matches!(
            api.download_file("file-id", &destination).await,
            Err(TelegramApiError::LocalFilePathRejected)
        ));
        server.await.expect("getFile task should finish");
        assert!(tokio::fs::metadata(&destination).await.is_err());
        assert_no_temporary_downloads(&output).await;
        tokio::fs::remove_dir_all(directory).await.expect("test directory should be removed");
    }

    #[tokio::test]
    async fn local_file_size_mismatch_is_terminal_and_cleans_temporary_output() {
        let directory =
            std::env::temp_dir().join(format!("sooqa-telegram-local-{}", Uuid::new_v4()));
        let root = directory.join("telegram-bot-api");
        let source = root.join("files/payload.bin");
        let output = directory.join("output");
        tokio::fs::create_dir_all(source.parent().expect("source should have a parent"))
            .await
            .expect("local root should be created");
        tokio::fs::create_dir(&output).await.expect("output directory should be created");
        tokio::fs::write(&source, b"actual payload").await.expect("fixture should be written");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("fake API should bind");
        let address = listener.local_addr().expect("fake API address should be available");
        let server = tokio::spawn(serve_file_metadata_at_path(
            listener,
            5,
            source.to_string_lossy().into_owned(),
        ));
        let destination = output.join("download.bin");
        let api =
            TeloxideApi::new("test-token", &format!("http://{address}"), Duration::from_secs(1))
                .expect("fake API URL should be accepted")
                .with_source_download_max_bytes(64)
                .with_local_file_root(root);

        assert!(matches!(
            api.download_file("file-id", &destination).await,
            Err(TelegramApiError::DownloadSizeMismatch { reported: 5, actual: 14 })
        ));
        server.await.expect("getFile task should finish");
        assert!(tokio::fs::metadata(&destination).await.is_err());
        assert_no_temporary_downloads(&output).await;
        tokio::fs::remove_dir_all(directory).await.expect("test directory should be removed");
    }

    #[tokio::test]
    async fn local_file_size_overflow_is_rejected_before_temporary_publication() {
        let directory =
            std::env::temp_dir().join(format!("sooqa-telegram-local-{}", Uuid::new_v4()));
        let root = directory.join("telegram-bot-api");
        let source = root.join("files/payload.bin");
        let output = directory.join("output");
        tokio::fs::create_dir_all(source.parent().expect("source should have a parent"))
            .await
            .expect("local root should be created");
        tokio::fs::create_dir(&output).await.expect("output directory should be created");
        tokio::fs::write(&source, b"oversized payload").await.expect("fixture should be written");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("fake API should bind");
        let address = listener.local_addr().expect("fake API address should be available");
        let server = tokio::spawn(serve_file_metadata_at_path(
            listener,
            17,
            source.to_string_lossy().into_owned(),
        ));
        let destination = output.join("download.bin");
        let api =
            TeloxideApi::new("test-token", &format!("http://{address}"), Duration::from_secs(1))
                .expect("fake API URL should be accepted")
                .with_source_download_max_bytes(8)
                .with_local_file_root(root);

        assert!(matches!(
            api.download_file("file-id", &destination).await,
            Err(TelegramApiError::DownloadLimit { size: 17, limit: 8 })
        ));
        server.await.expect("getFile task should finish");
        assert!(tokio::fs::metadata(&destination).await.is_err());
        assert_no_temporary_downloads(&output).await;
        tokio::fs::remove_dir_all(directory).await.expect("test directory should be removed");
    }

    #[tokio::test]
    async fn dropped_temporary_download_removes_partial_output() {
        let directory =
            std::env::temp_dir().join(format!("sooqa-telegram-local-{}", Uuid::new_v4()));
        tokio::fs::create_dir(&directory).await.expect("test directory should be created");
        let destination = directory.join("download.bin");
        let (temporary, mut output) =
            open_temporary_download(&destination).await.expect("temporary file should open");
        tokio::io::AsyncWriteExt::write_all(&mut output, b"partial")
            .await
            .expect("partial output should be writable");
        drop(output);
        let temporary_path = temporary.path().to_owned();
        drop(temporary);
        assert!(tokio::fs::metadata(temporary_path).await.is_err());
        tokio::fs::remove_dir_all(directory).await.expect("test directory should be removed");
    }

    #[tokio::test]
    async fn local_file_path_rejects_directories_and_missing_roots() {
        let directory =
            std::env::temp_dir().join(format!("sooqa-telegram-local-{}", Uuid::new_v4()));
        let root = directory.join("telegram-bot-api");
        let candidate = root.join("files");
        let output = directory.join("output");
        tokio::fs::create_dir_all(&candidate).await.expect("local root should be created");
        tokio::fs::create_dir(&output).await.expect("output directory should be created");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("fake API should bind");
        let address = listener.local_addr().expect("fake API address should be available");
        let server = tokio::spawn(serve_file_metadata_at_path(
            listener,
            0,
            candidate.to_string_lossy().into_owned(),
        ));
        let destination = output.join("download.bin");
        let api =
            TeloxideApi::new("test-token", &format!("http://{address}"), Duration::from_secs(1))
                .expect("fake API URL should be accepted")
                .with_source_download_max_bytes(64)
                .with_local_file_root(root);

        assert!(matches!(
            api.download_file("file-id", &destination).await,
            Err(TelegramApiError::LocalFileNotRegular)
        ));
        server.await.expect("getFile task should finish");

        let missing_root = directory.join("missing");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("fake API should bind");
        let address = listener.local_addr().expect("fake API address should be available");
        let server = tokio::spawn(serve_file_metadata_at_path(
            listener,
            0,
            candidate.to_string_lossy().into_owned(),
        ));
        let api =
            TeloxideApi::new("test-token", &format!("http://{address}"), Duration::from_secs(1))
                .expect("fake API URL should be accepted")
                .with_source_download_max_bytes(64)
                .with_local_file_root(missing_root);

        assert!(matches!(
            api.download_file("file-id", &destination).await,
            Err(TelegramApiError::LocalFileRootUnavailable)
        ));
        server.await.expect("getFile task should finish");
        assert_no_temporary_downloads(&output).await;
        tokio::fs::remove_dir_all(directory).await.expect("test directory should be removed");
    }

    async fn assert_no_temporary_downloads(directory: &Path) {
        let mut entries =
            tokio::fs::read_dir(directory).await.expect("directory should be readable");
        while let Some(entry) =
            entries.next_entry().await.expect("directory entry should be readable")
        {
            assert!(
                !entry.file_name().to_string_lossy().starts_with(".sooqa-download-"),
                "temporary download should be cleaned up"
            );
        }
    }

    #[tokio::test]
    async fn cloud_download_limit_rejects_before_streaming_file() {
        const PAYLOAD_SIZE: u64 = 21;
        const REDUCED_CLOUD_CEILING: u64 = 4;
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("fake API should bind");
        let address = listener.local_addr().expect("fake API address should be available");
        let server = tokio::spawn(serve_file_metadata(listener, PAYLOAD_SIZE as usize));
        let destination = std::env::temp_dir().join(format!("sooqa-telegram-{}", Uuid::new_v4()));
        let api =
            TeloxideApi::new("test-token", &format!("http://{address}"), Duration::from_secs(1))
                .expect("fake API URL should be accepted")
                .with_source_download_max_bytes(64)
                .with_test_cloud_limits(REDUCED_CLOUD_CEILING);

        assert!(matches!(
            api.download_file("file-id", &destination).await,
            Err(TelegramApiError::DownloadLimit {
                size: PAYLOAD_SIZE,
                limit: REDUCED_CLOUD_CEILING
            })
        ));
        let request_target = server.await.expect("fake API task should finish");
        assert!(request_target.contains("/bottest-token/GetFile"));
        assert!(tokio::fs::metadata(&destination).await.is_err());
    }

    #[tokio::test]
    async fn media_download_does_not_use_poll_timeout_for_slow_transfer() {
        const PAYLOAD: &[u8] = b"slow transfer";
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("fake API should bind");
        let address = listener.local_addr().expect("fake API address should be available");
        let server =
            tokio::spawn(serve_download(listener, PAYLOAD.to_vec(), Duration::from_secs(6)));
        let directory = std::env::temp_dir().join(format!("sooqa-telegram-{}", Uuid::new_v4()));
        tokio::fs::create_dir(&directory).await.expect("test directory should be created");
        let destination = directory.join("slow.webm");
        let api =
            TeloxideApi::new("test-token", &format!("http://{address}"), Duration::from_millis(1))
                .expect("fake API URL should be accepted")
                .with_source_download_max_bytes(64);

        let result = tokio::time::timeout(
            Duration::from_secs(8),
            api.download_file("file-id", &destination),
        )
        .await
        .expect("media transfer should finish within the test deadline");
        result.expect("media transfer should not inherit the polling timeout");
        assert_eq!(
            tokio::fs::read(&destination).await.expect("download should be readable"),
            PAYLOAD
        );
        server.await.expect("fake API task should finish");
        tokio::fs::remove_dir_all(directory).await.expect("test directory should be removed");
    }

    #[tokio::test]
    async fn local_bot_api_uploads_file_through_configured_endpoint_without_buffering() {
        const PAYLOAD: &[u8] = b"large upload payload";
        const REDUCED_CLOUD_CEILING: u64 = 4;
        const MEDIA_READ_TIMEOUT: Duration = Duration::from_millis(10);
        const UPLOAD_BODY_DELAY: Duration = Duration::from_millis(100);
        assert!(PAYLOAD.len() as u64 > REDUCED_CLOUD_CEILING);

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("fake API should bind");
        let address = listener.local_addr().expect("fake API address should be available");
        let server = tokio::spawn(serve_upload(
            listener,
            UPLOAD_BODY_DELAY,
            b"name=\"supports_streaming\"\r\n\r\ntrue",
        ));
        let directory = std::env::temp_dir().join(format!("sooqa-telegram-{}", Uuid::new_v4()));
        tokio::fs::create_dir(&directory).await.expect("test directory should be created");
        let path = directory.join("canonical.mp4");
        tokio::fs::write(&path, PAYLOAD).await.expect("upload fixture should be written");
        let api = TeloxideApi::new_with_upload_timeout(
            "test-token",
            &format!("http://{address}"),
            Duration::from_secs(1),
            Duration::from_secs(5),
        )
        .expect("fake API URL should be accepted")
        .with_test_media_read_timeout(MEDIA_READ_TIMEOUT)
        .expect("test media client should be configured");

        let started = std::time::Instant::now();
        let result = api
            .upload_media(StorageUploadRequest {
                storage_chat_id: -100123,
                media_kind: MediaKind::Video,
                local_work_path: path,
                caption: "sooqa".to_owned(),
                duration: Some(1),
                width: Some(320),
                height: Some(240),
                thumbnail_path: None,
            })
            .await
            .expect("local Bot API upload should succeed");
        assert!(started.elapsed() >= UPLOAD_BODY_DELAY);
        assert_eq!(result.storage_message_id, 7);
        assert_eq!(result.telegram_file_id, "stored-file-id");
        let request = server.await.expect("fake API task should finish");
        assert!(request.target.contains("/bottest-token/SendVideo"));
        assert!(request.body_bytes > REDUCED_CLOUD_CEILING);
        assert!(request.body_bytes >= PAYLOAD.len() as u64);
        assert!(
            request.body_contains_marker,
            "SendVideo request must include supports_streaming=true"
        );
        tokio::fs::remove_dir_all(directory).await.expect("test directory should be removed");
    }

    #[test]
    fn cloud_bot_api_limit_is_only_applied_to_cloud_endpoint() {
        let cloud =
            TeloxideApi::new("test-token", "https://api.telegram.org", Duration::from_secs(1))
                .expect("cloud Bot API URL should be accepted");
        assert_eq!(cloud.cloud_download_limit_bytes, Some(20 * 1024 * 1024));
        assert_eq!(cloud.cloud_upload_limit_bytes, Some(50 * 1024 * 1024));
        assert_eq!(cloud.source_download_max_bytes, DEFAULT_TELEGRAM_SOURCE_DOWNLOAD_MAX_BYTES);

        let local =
            TeloxideApi::new("test-token", "http://telegram-bot-api:8081", Duration::from_secs(1))
                .expect("Local Bot API URL should be accepted");
        assert_eq!(local.cloud_download_limit_bytes, None);
        assert_eq!(local.cloud_upload_limit_bytes, None);
        assert_eq!(local.source_download_max_bytes, DEFAULT_TELEGRAM_SOURCE_DOWNLOAD_MAX_BYTES);
    }

    #[test]
    fn upload_timeout_is_positive_and_bounded() {
        assert!(matches!(
            TeloxideApi::new_with_upload_timeout(
                "test-token",
                "http://telegram-bot-api:8081",
                Duration::from_secs(1),
                Duration::ZERO,
            ),
            Err(TelegramError::InvalidUploadTimeout)
        ));
        assert!(matches!(
            TeloxideApi::new_with_upload_timeout(
                "test-token",
                "http://telegram-bot-api:8081",
                Duration::from_secs(1),
                TELEGRAM_MAX_UPLOAD_TIMEOUT + Duration::from_secs(1),
            ),
            Err(TelegramError::InvalidUploadTimeout)
        ));
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
