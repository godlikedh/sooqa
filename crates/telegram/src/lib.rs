//! Telegram adapter and editorial interaction boundaries for sooqa.

use std::{
    collections::{BTreeSet, HashMap},
    error::Error,
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use sooqa_library::MediaKind;
use teloxide::{
    Bot,
    payloads::GetUpdatesSetters,
    prelude::{Request, Requester},
    types::{ChatId, FileId, Message, Update, UpdateKind},
};
use thiserror::Error as ThisError;
use tracing::warn;
use url::Url;
use uuid::Uuid;

mod storage;

pub use storage::{
    StorageUploadApiError, StorageUploadError, StorageUploadInput, StorageUploadOutcome,
    StorageUploadProvider, StorageUploadRequest, StorageUploadResult, TELEGRAM_STORAGE_PROVIDER,
    TelegramStorageApi,
};

pub const START_RESPONSE: &str = "sooqa is ready. You are authorized.";
pub const HELP_RESPONSE: &str = "Available commands:\n/start — show authorization\n/help — show this help\n/add <url> — queue a URL\n/status — show service status";
pub const STATUS_RESPONSE: &str = "sooqa is online.";
pub const UNAUTHORIZED_RESPONSE: &str = "This bot is restricted to its configured administrator.";
pub const ADD_USAGE_RESPONSE: &str = "Send one http(s) URL after /add, or send a bare URL.";
const TELEGRAM_CLOUD_DOWNLOAD_LIMIT_BYTES: u64 = 20 * 1024 * 1024;
const RESPONSE_RATE_LIMIT: Duration = Duration::from_secs(1);
const RETRY_DELAY: Duration = Duration::from_secs(1);
const MAX_HANDLER_ATTEMPTS: usize = 5;
const MAX_POLLING_ATTEMPTS: usize = 5;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IncomingMessage {
    pub update_id: i64,
    pub message_id: i64,
    pub user_id: Option<i64>,
    pub chat_id: i64,
    pub is_private: bool,
    pub text: Option<String>,
    pub caption: Option<String>,
    pub media: Option<TelegramMedia>,
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
    UnsupportedDocument {
        file_name: Option<String>,
        mime_type: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum HandleOutcome {
    DuplicateIgnored,
    NonMessageIgnored,
    NonPrivateIgnored,
    RateLimited,
    MediaRejected,
    Unauthorized,
    UnrecognizedIgnored,
    Responded(Command),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Command {
    Start,
    Help,
    Add,
    Status,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct UpdateClaim {
    pub update_id: i64,
    pub claim_token: Uuid,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum UpdateClaimResult {
    Claimed(UpdateClaim),
    Completed,
    InProgress,
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
    pub media_kind: MediaKind,
    pub file_id: String,
    pub file_unique_id: String,
    pub file_size: Option<u32>,
    pub mime_type: Option<String>,
    pub file_name: Option<String>,
    pub caption: Option<String>,
    pub local_work_path: PathBuf,
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
    #[error("Telegram update receipt store failed: {0}")]
    UpdateStore(#[source] Box<dyn Error + Send + Sync>),
    #[error("Telegram API base URL is invalid: {0}")]
    InvalidApiBaseUrl(String),
    #[error("Telegram polling timeout must be greater than zero")]
    InvalidPollTimeout,
    #[error("Telegram HTTP client could not be configured: {0}")]
    HttpClient(#[source] reqwest::Error),
    #[error("Telegram Ctrl-C handler could not be initialized: {0}")]
    Shutdown(#[source] std::io::Error),
    #[error("Telegram update is still being processed: {0}")]
    UpdateInProgress(i64),
    #[error("Telegram URL ingest failed: {0}")]
    Ingest(#[source] Box<dyn Error + Send + Sync>),
    #[error("Telegram media download failed: {0}")]
    MediaDownload(#[source] Box<dyn Error + Send + Sync>),
}

#[derive(Debug, ThisError)]
pub enum TelegramApiError {
    #[error("Telegram API request failed: {0}")]
    Api(#[source] teloxide::RequestError),
    #[error("Telegram file download failed: {0}")]
    Download(#[source] teloxide::DownloadError),
    #[error("Telegram download destination could not be opened: {0}")]
    Io(#[source] io::Error),
    #[error("Telegram Bot API download limit is {limit} bytes; file is {size} bytes")]
    DownloadLimit { size: u64, limit: u64 },
}

#[async_trait]
pub trait TelegramApi: Clone + Send + Sync + 'static {
    type Error: Error + Send + Sync + 'static;

    async fn send_text(&self, chat_id: i64, text: &str) -> Result<(), Self::Error>;

    async fn download_file(&self, file_id: &str, destination: &Path) -> Result<(), Self::Error>;

    fn is_retryable_error(error: &Self::Error) -> bool;
}

#[async_trait]
pub trait UpdateStore: Clone + Send + Sync + 'static {
    type Error: Error + Send + Sync + 'static;

    async fn claim_update(&self, update_id: i64) -> Result<UpdateClaimResult, Self::Error>;

    async fn complete_update(&self, claim: UpdateClaim) -> Result<(), Self::Error>;

    async fn release_update(&self, claim: UpdateClaim) -> Result<(), Self::Error>;
}

#[derive(Clone)]
pub struct TelegramService<A, S, I = ()> {
    api: A,
    update_store: S,
    ingest_service: Option<I>,
    admin_user_ids: Arc<BTreeSet<i64>>,
    response_limiter: Arc<Mutex<HashMap<RateLimitKey, Instant>>>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
struct RateLimitKey {
    user_id: Option<i64>,
    chat_id: i64,
}

impl<A, S> TelegramService<A, S, ()>
where
    A: TelegramApi,
    S: UpdateStore,
{
    pub fn new(api: A, update_store: S, admin_user_ids: impl IntoIterator<Item = i64>) -> Self {
        Self {
            api,
            update_store,
            ingest_service: None,
            admin_user_ids: Arc::new(admin_user_ids.into_iter().collect()),
            response_limiter: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<A, S, I> TelegramService<A, S, I>
where
    A: TelegramApi,
    S: UpdateStore,
    I: IngestService,
{
    pub fn with_ingest(
        api: A,
        update_store: S,
        admin_user_ids: impl IntoIterator<Item = i64>,
        ingest_service: I,
    ) -> Self {
        Self {
            api,
            update_store,
            ingest_service: Some(ingest_service),
            admin_user_ids: Arc::new(admin_user_ids.into_iter().collect()),
            response_limiter: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn handle_message(
        &self,
        message: IncomingMessage,
    ) -> Result<HandleOutcome, TelegramError> {
        let claim = match self.claim(message.update_id).await? {
            UpdateClaimResult::Claimed(claim) => claim,
            UpdateClaimResult::Completed => return Ok(HandleOutcome::DuplicateIgnored),
            UpdateClaimResult::InProgress => {
                return Err(TelegramError::UpdateInProgress(message.update_id));
            }
        };
        if !message.is_private {
            self.complete(claim).await?;
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
                self.complete(claim).await?;
                return Ok(HandleOutcome::RateLimited);
            }
            self.send_and_complete(
                claim,
                message.chat_id,
                UNAUTHORIZED_RESPONSE,
                RateLimitKey { user_id: message.user_id, chat_id: message.chat_id },
            )
            .await?;
            return Ok(HandleOutcome::Unauthorized);
        }
        if let Some(media) = message.media.clone() {
            return self.handle_media_message(message, claim, media).await;
        }
        let action =
            message.text.as_deref().map(parse_message_action).unwrap_or(MessageAction::Ignore);
        match action {
            MessageAction::Url(source_url) => {
                let Some(ingest_service) = self.ingest_service.as_ref() else {
                    self.release(claim).await?;
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
                        self.release(claim).await?;
                        return Err(TelegramError::Ingest(Box::new(error)));
                    }
                };
                let response = format!(
                    "✅ Ingest queued\nID: {}\nStatus: {}",
                    accepted.request_id, accepted.status
                );
                if !self.allow_response(message.user_id, message.chat_id) {
                    self.complete(claim).await?;
                    return Ok(HandleOutcome::RateLimited);
                }
                self.send_and_complete(
                    claim,
                    message.chat_id,
                    &response,
                    RateLimitKey { user_id: message.user_id, chat_id: message.chat_id },
                )
                .await?;
                Ok(HandleOutcome::Responded(Command::Add))
            }
            MessageAction::Command(command) => {
                if !self.allow_response(message.user_id, message.chat_id) {
                    self.complete(claim).await?;
                    return Ok(HandleOutcome::RateLimited);
                }
                let response = match command {
                    Command::Start => START_RESPONSE,
                    Command::Help => HELP_RESPONSE,
                    Command::Status => STATUS_RESPONSE,
                    Command::Add => ADD_USAGE_RESPONSE,
                };
                self.send_and_complete(
                    claim,
                    message.chat_id,
                    response,
                    RateLimitKey { user_id: message.user_id, chat_id: message.chat_id },
                )
                .await?;
                Ok(HandleOutcome::Responded(command))
            }
            MessageAction::Ignore => {
                self.complete(claim).await?;
                Ok(HandleOutcome::UnrecognizedIgnored)
            }
        }
    }

    async fn handle_media_message(
        &self,
        message: IncomingMessage,
        claim: UpdateClaim,
        media: TelegramMedia,
    ) -> Result<HandleOutcome, TelegramError> {
        let rate_limit_key = RateLimitKey { user_id: message.user_id, chat_id: message.chat_id };
        let response = match media {
            TelegramMedia::UnsupportedDocument { file_name, mime_type } => {
                let response = format!(
                    "⚠️ Unsupported Telegram document{}{}",
                    file_name.map(|value| format!(": {value}")).unwrap_or_default(),
                    mime_type.map(|value| format!(" ({value})")).unwrap_or_default()
                );
                if !self.allow_response(message.user_id, message.chat_id) {
                    self.complete(claim).await?;
                    return Ok(HandleOutcome::RateLimited);
                }
                self.send_and_complete(claim, message.chat_id, &response, rate_limit_key).await?;
                return Ok(HandleOutcome::MediaRejected);
            }
            TelegramMedia::Supported {
                media_kind,
                file_id,
                file_unique_id,
                file_size,
                mime_type,
                file_name,
            } => {
                let user_id = message.user_id.expect("authorized Telegram messages have a user ID");
                let local_work_path = telegram_download_path(message.update_id);
                if let Err(error) = self.api.download_file(&file_id, &local_work_path).await {
                    self.clear_response(rate_limit_key);
                    if A::is_retryable_error(&error) {
                        self.release(claim).await?;
                        return Err(TelegramError::MediaDownload(Box::new(error)));
                    }
                    let response = format!("⚠️ Telegram file cannot be downloaded: {error}");
                    if !self.allow_response(message.user_id, message.chat_id) {
                        self.complete(claim).await?;
                        return Ok(HandleOutcome::RateLimited);
                    }
                    self.send_and_complete(claim, message.chat_id, &response, rate_limit_key)
                        .await?;
                    return Ok(HandleOutcome::MediaRejected);
                }

                let Some(ingest_service) = self.ingest_service.as_ref() else {
                    self.release(claim).await?;
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
                        local_work_path,
                        idempotency_key: format!("telegram:update:{}:v1", message.update_id),
                    })
                    .await
                {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        self.clear_response(rate_limit_key);
                        self.release(claim).await?;
                        return Err(TelegramError::Ingest(Box::new(error)));
                    }
                };
                format!(
                    "✅ Ingest queued\nID: {}\nStatus: {}",
                    accepted.request_id, accepted.status
                )
            }
        };
        if !self.allow_response(message.user_id, message.chat_id) {
            self.complete(claim).await?;
            return Ok(HandleOutcome::RateLimited);
        }
        self.send_and_complete(claim, message.chat_id, &response, rate_limit_key).await?;
        Ok(HandleOutcome::Responded(Command::Add))
    }

    pub async fn handle_update(&self, update: Update) -> Result<HandleOutcome, TelegramError> {
        let update_id = i64::from(update.id.0);
        let UpdateKind::Message(message) = update.kind else {
            let claim = match self.claim(update_id).await? {
                UpdateClaimResult::Claimed(claim) => claim,
                UpdateClaimResult::Completed => return Ok(HandleOutcome::DuplicateIgnored),
                UpdateClaimResult::InProgress => {
                    return Err(TelegramError::UpdateInProgress(update_id));
                }
            };
            self.complete(claim).await?;
            return Ok(HandleOutcome::NonMessageIgnored);
        };
        self.handle_message(IncomingMessage {
            update_id,
            message_id: i64::from(message.id.0),
            user_id: message.from.as_ref().and_then(|user| i64::try_from(user.id.0).ok()),
            chat_id: message.chat.id.0,
            is_private: message.chat.is_private(),
            text: message.text().map(str::to_owned),
            caption: message.caption().map(str::to_owned),
            media: message_media(&message),
        })
        .await
    }

    async fn claim(&self, update_id: i64) -> Result<UpdateClaimResult, TelegramError> {
        self.update_store
            .claim_update(update_id)
            .await
            .map_err(|error| TelegramError::UpdateStore(Box::new(error)))
    }

    async fn complete(&self, claim: UpdateClaim) -> Result<(), TelegramError> {
        self.update_store
            .complete_update(claim)
            .await
            .map_err(|error| TelegramError::UpdateStore(Box::new(error)))
    }

    async fn release(&self, claim: UpdateClaim) -> Result<(), TelegramError> {
        self.update_store
            .release_update(claim)
            .await
            .map_err(|error| TelegramError::UpdateStore(Box::new(error)))
    }

    async fn send_and_complete(
        &self,
        claim: UpdateClaim,
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
            self.release(claim).await?;
            return Err(error);
        }
        self.complete(claim).await
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

fn telegram_download_path(update_id: i64) -> PathBuf {
    std::env::temp_dir().join(format!("sooqa-telegram-{update_id}.bin"))
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
    let media_kind = document_media_kind(mime_type.as_deref(), document.file_name.as_deref());
    match media_kind {
        Some(media_kind) => Some(TelegramMedia::Supported {
            media_kind,
            file_id: document.file.id.to_string(),
            file_unique_id: document.file.unique_id.to_string(),
            file_size: Some(document.file.size),
            mime_type,
            file_name: document.file_name.clone(),
        }),
        None => Some(TelegramMedia::UnsupportedDocument {
            file_name: document.file_name.clone(),
            mime_type,
        }),
    }
}

fn document_media_kind(mime_type: Option<&str>, file_name: Option<&str>) -> Option<MediaKind> {
    let mime_type = mime_type.unwrap_or_default().to_ascii_lowercase();
    if mime_type.starts_with("video/") {
        return Some(MediaKind::Video);
    }
    if mime_type == "image/gif" {
        return Some(MediaKind::Animation);
    }
    if mime_type.starts_with("image/") {
        return Some(MediaKind::Image);
    }
    if mime_type.starts_with("audio/") {
        return Some(MediaKind::Audio);
    }

    let extension = file_name
        .and_then(|name| Path::new(name).extension())
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase);
    match extension.as_deref() {
        Some("gif") => Some(MediaKind::Animation),
        Some("jpg" | "jpeg" | "png" | "webp" | "avif") => Some(MediaKind::Image),
        Some("mp4" | "webm" | "mkv" | "mov" | "avi") => Some(MediaKind::Video),
        Some("mp3" | "m4a" | "wav" | "flac" | "ogg") => Some(MediaKind::Audio),
        _ => None,
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

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum CallbackData {
    IngestStatus { request_id: Uuid },
}

impl CallbackData {
    pub fn encode(self) -> String {
        match self {
            Self::IngestStatus { request_id } => format!("v1:ingest_status:{request_id}"),
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        let mut parts = value.split(':');
        if parts.next()? != "v1" || parts.next()? != "ingest_status" {
            return None;
        }
        let request_id = parts.next()?.parse().ok()?;
        if parts.next().is_some() {
            return None;
        }
        Some(Self::IngestStatus { request_id })
    }
}

#[derive(Clone)]
pub struct TeloxideApi {
    bot: Bot,
    cloud_download_limit_bytes: Option<u64>,
}

impl TeloxideApi {
    pub fn new(
        token: impl Into<String>,
        api_base_url: &str,
        poll_timeout: Duration,
    ) -> Result<Self, TelegramError> {
        if poll_timeout.is_zero() || poll_timeout.as_secs() > u64::from(u32::MAX) {
            return Err(TelegramError::InvalidPollTimeout);
        }
        let api_base_url = Url::parse(api_base_url)
            .map_err(|error| TelegramError::InvalidApiBaseUrl(error.to_string()))?;
        if !matches!(api_base_url.scheme(), "http" | "https")
            || api_base_url.host_str().is_none()
            || !api_base_url.username().is_empty()
            || api_base_url.password().is_some()
        {
            return Err(TelegramError::InvalidApiBaseUrl(
                "must be an HTTP(S) URL without credentials".to_owned(),
            ));
        }
        let client_timeout = poll_timeout.checked_add(Duration::from_secs(5)).ok_or_else(|| {
            TelegramError::InvalidApiBaseUrl("poll timeout is too large".to_owned())
        })?;
        let client = reqwest::Client::builder()
            .timeout(client_timeout)
            .build()
            .map_err(TelegramError::HttpClient)?;
        let cloud_download_limit_bytes = (api_base_url.host_str() == Some("api.telegram.org"))
            .then_some(TELEGRAM_CLOUD_DOWNLOAD_LIMIT_BYTES);
        Ok(Self {
            bot: Bot::with_client(token, client).set_api_url(api_base_url),
            cloud_download_limit_bytes,
        })
    }

    fn bot(&self) -> Bot {
        self.bot.clone()
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
            | TelegramApiError::Api(_)
            | TelegramApiError::Download(_)
            | TelegramApiError::Io(_) => false,
        }
    }

    async fn send_text(&self, chat_id: i64, text: &str) -> Result<(), Self::Error> {
        self.bot
            .send_message(ChatId(chat_id), text.to_owned())
            .await
            .map(|_| ())
            .map_err(TelegramApiError::Api)
    }

    async fn download_file(&self, file_id: &str, destination: &Path) -> Result<(), Self::Error> {
        let file = self
            .bot()
            .get_file(FileId(file_id.to_owned()))
            .send()
            .await
            .map_err(TelegramApiError::Api)?;
        let size = u64::from(file.meta.size);
        if let Some(limit) = self.cloud_download_limit_bytes
            && size > limit
        {
            return Err(TelegramApiError::DownloadLimit { size, limit });
        }
        let mut output =
            tokio::fs::File::create(destination).await.map_err(TelegramApiError::Io)?;
        teloxide::net::Download::download_file(&self.bot(), &file.path, &mut output)
            .await
            .map_err(TelegramApiError::Download)
    }
}

pub struct TelegramRuntime<S, I> {
    api: TeloxideApi,
    service: TelegramService<TeloxideApi, S, I>,
    poll_timeout: Duration,
    storage_chat_id: Option<i64>,
}

impl<S, I> TelegramRuntime<S, I>
where
    S: UpdateStore,
    I: IngestService,
{
    pub fn new(
        token: impl Into<String>,
        api_base_url: &str,
        poll_timeout: Duration,
        update_store: S,
        admin_user_ids: impl IntoIterator<Item = i64>,
        storage_chat_id: Option<i64>,
        ingest_service: I,
    ) -> Result<Self, TelegramError> {
        let api = TeloxideApi::new(token, api_base_url, poll_timeout)?;
        let service =
            TelegramService::with_ingest(api.clone(), update_store, admin_user_ids, ingest_service);
        Ok(Self { api, service, poll_timeout, storage_chat_id })
    }

    pub async fn run(self) -> Result<(), TelegramError> {
        if let Some(storage_chat_id) = self.storage_chat_id {
            self.api
                .verify_storage_chat(storage_chat_id)
                .await
                .map_err(|error| TelegramError::Api(Box::new(error)))?;
            tracing::info!(storage_chat_id, "Telegram storage chat is reachable");
        }
        self.api
            .bot()
            .delete_webhook()
            .send()
            .await
            .map_err(|error| TelegramError::Api(Box::new(error)))?;
        let bot = self.api.bot();
        let service = self.service;
        let mut offset = 0_i32;
        let mut polling_failures = 0_usize;
        let ctrl_c = tokio::signal::ctrl_c();
        tokio::pin!(ctrl_c);

        loop {
            tokio::select! {
                result = &mut ctrl_c => {
                    result.map_err(TelegramError::Shutdown)?;
                    return Ok(());
                }
                result = bot
                    .get_updates()
                    .offset(offset)
                    .timeout(self.poll_timeout.as_secs() as u32)
                    .send() => {
                    match result {
                        Ok(updates) => {
                            polling_failures = 0;
                            for update in updates {
                                offset = handle_update_with_retries(&service, update).await?;
                            }
                        }
                        Err(error) => {
                            let error = TelegramError::Api(Box::new(error));
                            if is_terminal_bot_error(&error) {
                                return Err(error);
                            }
                            polling_failures += 1;
                            if polling_failures >= MAX_POLLING_ATTEMPTS {
                                return Err(error);
                            }
                            tracing::warn!(?error, offset, "Telegram polling failed; retaining offset for retry");
                            tokio::time::sleep(retry_delay(&error)).await;
                        }
                    }
                }
            }
        }
    }
}

async fn handle_update_with_retries<A, S, I>(
    service: &TelegramService<A, S, I>,
    update: Update,
) -> Result<i32, TelegramError>
where
    A: TelegramApi,
    S: UpdateStore,
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
        .unwrap_or(RETRY_DELAY)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::*;

    #[derive(Clone, Default)]
    struct MockApi {
        messages: Arc<Mutex<Vec<(i64, String)>>>,
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

        async fn download_file(
            &self,
            _file_id: &str,
            _destination: &Path,
        ) -> Result<(), Self::Error> {
            Ok(())
        }

        fn is_retryable_error(_error: &Self::Error) -> bool {
            false
        }
    }

    #[derive(Clone, Default)]
    struct MockStore {
        claimed: Arc<Mutex<BTreeSet<i64>>>,
        completed: Arc<Mutex<BTreeSet<i64>>>,
    }

    #[derive(Clone, Default)]
    struct MockIngestService {
        commands: Arc<Mutex<Vec<UrlIngestCommand>>>,
        media_commands: Arc<Mutex<Vec<MediaIngestCommand>>>,
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
            self.media_commands.lock().expect("mock mutex should not be poisoned").push(command);
            Ok(IngestAccepted { request_id: Uuid::from_u128(1), status: "queued".to_owned() })
        }
    }

    #[async_trait]
    impl UpdateStore for MockStore {
        type Error = MockError;

        async fn claim_update(&self, update_id: i64) -> Result<UpdateClaimResult, Self::Error> {
            if self
                .completed
                .lock()
                .expect("mock mutex should not be poisoned")
                .contains(&update_id)
            {
                return Ok(UpdateClaimResult::Completed);
            }
            if self.claimed.lock().expect("mock mutex should not be poisoned").contains(&update_id)
            {
                return Ok(UpdateClaimResult::InProgress);
            }
            self.claimed.lock().expect("mock mutex should not be poisoned").insert(update_id);
            Ok(UpdateClaimResult::Claimed(UpdateClaim { update_id, claim_token: Uuid::new_v4() }))
        }

        async fn complete_update(&self, claim: UpdateClaim) -> Result<(), Self::Error> {
            self.claimed
                .lock()
                .expect("mock mutex should not be poisoned")
                .remove(&claim.update_id);
            self.completed
                .lock()
                .expect("mock mutex should not be poisoned")
                .insert(claim.update_id);
            Ok(())
        }

        async fn release_update(&self, claim: UpdateClaim) -> Result<(), Self::Error> {
            self.claimed
                .lock()
                .expect("mock mutex should not be poisoned")
                .remove(&claim.update_id);
            Ok(())
        }
    }

    fn message(update_id: i64, user_id: Option<i64>, text: &str) -> IncomingMessage {
        IncomingMessage {
            update_id,
            message_id: update_id,
            user_id,
            chat_id: 42,
            is_private: true,
            text: Some(text.to_owned()),
            caption: None,
            media: None,
        }
    }

    #[tokio::test]
    async fn authorized_commands_are_translated_and_deduplicated() {
        let api = MockApi::default();
        let service = TelegramService::new(api.clone(), MockStore::default(), [123]);

        assert_eq!(
            service.handle_message(message(1, Some(123), "/status")).await.unwrap(),
            HandleOutcome::Responded(Command::Status)
        );
        assert_eq!(
            service.handle_message(message(1, Some(123), "/status")).await.unwrap(),
            HandleOutcome::DuplicateIgnored
        );
        assert_eq!(api.messages.lock().unwrap().as_slice(), &[(42, STATUS_RESPONSE.to_owned())]);
    }

    #[tokio::test]
    async fn unauthorized_private_user_gets_generic_response() {
        let api = MockApi::default();
        let service = TelegramService::new(api.clone(), MockStore::default(), [123]);

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
        let service = TelegramService::new(api.clone(), MockStore::default(), [123]);
        let mut update = message(3, Some(123), "/status");
        update.is_private = false;

        assert_eq!(service.handle_message(update).await.unwrap(), HandleOutcome::NonPrivateIgnored);
        assert!(api.messages.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn command_responses_are_rate_limited_per_user_and_chat() {
        let api = MockApi::default();
        let service = TelegramService::new(api.clone(), MockStore::default(), [123]);

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
    async fn failed_response_releases_update_for_retry() {
        let api = MockApi::default();
        *api.fail.lock().unwrap() = true;
        let store = MockStore::default();
        let service = TelegramService::new(api.clone(), store, [123]);

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
        let service =
            TelegramService::with_ingest(api.clone(), MockStore::default(), [123], ingest.clone());

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
    async fn authorized_media_is_downloaded_and_ingested_with_metadata() {
        let api = MockApi::default();
        let ingest = MockIngestService::default();
        let service =
            TelegramService::with_ingest(api.clone(), MockStore::default(), [123], ingest.clone());
        let message = IncomingMessage {
            update_id: 11,
            message_id: 99,
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
        assert_eq!(
            ingest.media_commands.lock().unwrap().as_slice(),
            &[MediaIngestCommand {
                update_id: 11,
                message_id: 99,
                chat_id: 42,
                submitted_by_user_id: 123,
                media_kind: MediaKind::Video,
                file_id: "file-id".to_owned(),
                file_unique_id: "unique-id".to_owned(),
                file_size: Some(1234),
                mime_type: Some("video/webm".to_owned()),
                file_name: Some("clip.webm".to_owned()),
                caption: Some("a caption".to_owned()),
                local_work_path: telegram_download_path(11),
                idempotency_key: "telegram:update:11:v1".to_owned(),
            }]
        );
        assert_eq!(api.messages.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn unsupported_telegram_document_is_rejected_without_download() {
        let api = MockApi::default();
        let ingest = MockIngestService::default();
        let service =
            TelegramService::with_ingest(api.clone(), MockStore::default(), [123], ingest.clone());
        let message = IncomingMessage {
            update_id: 12,
            message_id: 100,
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
        assert_eq!(
            api.messages.lock().unwrap().as_slice(),
            &[(42, "⚠️ Unsupported Telegram document: archive.zip (application/zip)".to_owned())]
        );
    }

    #[tokio::test]
    async fn ingest_failure_releases_update_and_rate_limit() {
        let api = MockApi::default();
        let ingest = MockIngestService::default();
        *ingest.fail.lock().unwrap() = true;
        let store = MockStore::default();
        let service = TelegramService::with_ingest(api.clone(), store, [123], ingest.clone());

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
        let service =
            TelegramService::with_ingest(api.clone(), MockStore::default(), [123], ingest.clone());

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
        let service = TelegramService::new(api.clone(), MockStore::default(), [123]);
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
        let service = TelegramService::new(api.clone(), MockStore::default(), [123]);

        assert_eq!(
            service.handle_update(update).await.unwrap(),
            HandleOutcome::Responded(Command::Status)
        );
        assert_eq!(api.messages.lock().unwrap().len(), 1);
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

    #[test]
    fn cloud_bot_api_limit_is_only_applied_to_cloud_endpoint() {
        let cloud =
            TeloxideApi::new("test-token", "https://api.telegram.org", Duration::from_secs(1))
                .expect("cloud Bot API URL should be accepted");
        assert_eq!(cloud.cloud_download_limit_bytes, Some(20 * 1024 * 1024));

        let local =
            TeloxideApi::new("test-token", "http://telegram-bot-api:8081", Duration::from_secs(1))
                .expect("Local Bot API URL should be accepted");
        assert_eq!(local.cloud_download_limit_bytes, None);
    }

    #[test]
    fn command_parser_accepts_bot_suffix_and_rejects_other_text() {
        assert_eq!(parse_command("/start@sooqa_bot"), Some(Command::Start));
        assert_eq!(parse_command("/HELP extra"), Some(Command::Help));
        assert_eq!(parse_command("hello /status"), None);
        assert_eq!(parse_command("/add"), Some(Command::Add));
    }

    #[test]
    fn document_media_kind_uses_mime_then_safe_filename_fallback() {
        assert_eq!(
            document_media_kind(Some("video/mp4"), Some("not-video.txt")),
            Some(MediaKind::Video)
        );
        assert_eq!(document_media_kind(None, Some("clip.WEBM")), Some(MediaKind::Video));
        assert_eq!(
            document_media_kind(Some("image/gif"), Some("animation.bin")),
            Some(MediaKind::Animation)
        );
        assert_eq!(document_media_kind(Some("application/zip"), Some("archive.zip")), None);
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

    #[test]
    fn callback_data_round_trips_and_rejects_unknown_shapes() {
        let request_id = Uuid::from_u128(2);
        let encoded = CallbackData::IngestStatus { request_id }.encode();
        assert_eq!(encoded, "v1:ingest_status:00000000-0000-0000-0000-000000000002");
        assert_eq!(CallbackData::parse(&encoded), Some(CallbackData::IngestStatus { request_id }));
        assert_eq!(
            CallbackData::parse("v2:ingest_status:00000000-0000-0000-0000-000000000002"),
            None
        );
        assert_eq!(CallbackData::parse(&format!("{encoded}:extra")), None);
    }
}
