//! Telegram adapter and editorial interaction boundaries for sooqa.

use std::{error::Error, io, path::Path, time::Duration};

use async_trait::async_trait;
use sooqa_library::MediaKind;
use teloxide::types::Update;
use thiserror::Error as ThisError;
use uuid::Uuid;

mod client;
mod polling;
mod publication;
mod service;
mod storage;

#[cfg(test)]
mod test_support;

pub use client::TeloxideApi;
pub use polling::TelegramRuntime;
pub use publication::{TelegramPublicationApi, TelegramPublicationRequest};
pub use service::TelegramService;
pub use storage::{
    StorageCaptionApiError, StorageCaptionEditRequest, StorageUploadApiError,
    StorageUploadCancellation, StorageUploadError, StorageUploadInput, StorageUploadOutcome,
    StorageUploadProvider, StorageUploadRequest, StorageUploadResult, TELEGRAM_STORAGE_PROVIDER,
    TelegramStorageApi, TelegramStorageCaptionApi, storage_caption,
};

pub const START_RESPONSE: &str = "sooqa is ready. You are authorized.";
pub const HELP_RESPONSE: &str = "Available commands:\n/start — show authorization\n/help — show this help\n/add <url> — queue a URL\n/status — show service status";
pub const STATUS_RESPONSE: &str = "sooqa is online.";
pub const UNAUTHORIZED_RESPONSE: &str = "This bot is restricted to its configured administrator.";
pub const ADD_USAGE_RESPONSE: &str = "Send one http(s) URL after /add, or send a bare URL.";
pub const DEFAULT_TELEGRAM_SOURCE_DOWNLOAD_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;
pub const DEFAULT_TELEGRAM_UPLOAD_TIMEOUT_SECONDS: u64 = 3_600;

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
