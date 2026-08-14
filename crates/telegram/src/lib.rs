//! Telegram adapter and editorial interaction boundaries for sooqa.

use std::{
    collections::{BTreeSet, HashMap},
    error::Error,
    io,
    path::Path,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use sooqa_library::MediaKind;
use sooqa_publisher::{PostState, QueueDirection, QueuePost};
use teloxide::{
    Bot,
    payloads::{GetUpdatesSetters, SendMessageSetters},
    prelude::{Request, Requester},
    types::{
        CallbackQueryId, ChatId, FileId, InlineKeyboardButton, InlineKeyboardMarkup, Message,
        MessageId, ReplyMarkup, Update, UpdateKind,
    },
};
use thiserror::Error as ThisError;
use time::OffsetDateTime;
use tracing::warn;
use url::Url;
use uuid::Uuid;

mod publication;
mod storage;

pub use publication::{TelegramPublicationApi, TelegramPublicationRequest};
pub use storage::{
    StorageUploadApiError, StorageUploadError, StorageUploadInput, StorageUploadOutcome,
    StorageUploadProvider, StorageUploadRequest, StorageUploadResult, TELEGRAM_STORAGE_PROVIDER,
    TelegramStorageApi,
};

pub const START_RESPONSE: &str = "sooqa is ready. You are authorized.";
pub const HELP_RESPONSE: &str = "Available commands:\n/start — show authorization\n/help — show this help\n/add <url> — queue a URL\n/status — show service status\n/duplicates — review pending duplicate candidates\n/queue — inspect the publication queue";
pub const STATUS_RESPONSE: &str = "sooqa is online.";
pub const UNAUTHORIZED_RESPONSE: &str = "This bot is restricted to its configured administrator.";
pub const ADD_USAGE_RESPONSE: &str = "Send one http(s) URL after /add, or send a bare URL.";
pub const QUEUE_MAX_POSTS: usize = 125;
pub const DEFAULT_TELEGRAM_SOURCE_DOWNLOAD_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;
pub const DEFAULT_TELEGRAM_UPLOAD_TIMEOUT_SECONDS: u64 = 3_600;
const TELEGRAM_CLOUD_DOWNLOAD_LIMIT_BYTES: u64 = 20 * 1024 * 1024;
const TELEGRAM_CLOUD_UPLOAD_LIMIT_BYTES: u64 = 50 * 1024 * 1024;
const TELEGRAM_MEDIA_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const TELEGRAM_MEDIA_READ_TIMEOUT: Duration = Duration::from_secs(120);
const TELEGRAM_MAX_UPLOAD_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);
const RESPONSE_RATE_LIMIT: Duration = Duration::from_secs(1);
const QUEUE_CARD_INTERVAL: Duration = Duration::from_secs(1);
const MAX_QUEUE_CARD_RETRIES: usize = 3;
const MAX_QUEUE_RETRY_AFTER: Duration = Duration::from_secs(30);
const QUEUE_PROMPT_TTL: Duration = Duration::from_secs(10 * 60);
const RETRY_DELAY: Duration = Duration::from_secs(1);
const MAX_HANDLER_ATTEMPTS: usize = 5;
const MAX_POLLING_ATTEMPTS: usize = 5;

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
    pub user_id: i64,
    pub chat_id: Option<i64>,
    pub is_private: bool,
    pub data: Option<String>,
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
    DuplicateIgnored,
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
    Duplicates,
    Queue,
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

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DuplicateCandidateStorage {
    Ready { open_url: Option<String> },
    PendingStorage,
    Unavailable { state: String },
    Missing,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DuplicateCandidateCard {
    pub media_id: Uuid,
    pub classification: String,
    pub score_bps: u16,
    pub storage: DuplicateCandidateStorage,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DuplicatePendingCard {
    pub request_id: Uuid,
    pub source_url: String,
    pub candidates: Vec<DuplicateCandidateCard>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DuplicateDecisionResult {
    pub request_id: Uuid,
    pub status: String,
    pub media_id: Option<Uuid>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum QueuePromptKind {
    Caption,
    Slot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuePrompt {
    pub post_id: Uuid,
    pub expected_revision: i64,
    pub kind: QueuePromptKind,
    pub prompt_message_id: i64,
}

#[async_trait]
pub trait PublisherService: Clone + Send + Sync + 'static {
    type Error: Error + Send + Sync + 'static;

    async fn queue_count(&self) -> Result<usize, Self::Error>;

    async fn list_queue(&self, limit: usize) -> Result<Vec<QueuePost>, Self::Error>;

    async fn get_queue_post(&self, post_id: Uuid) -> Result<QueuePost, Self::Error>;

    async fn move_queue_post(
        &self,
        post_id: Uuid,
        direction: QueueDirection,
        expected_revision: i64,
    ) -> Result<QueuePost, Self::Error>;

    async fn set_queue_post_slot(
        &self,
        post_id: Uuid,
        slot: OffsetDateTime,
        expected_revision: i64,
    ) -> Result<QueuePost, Self::Error>;

    async fn update_queue_caption(
        &self,
        post_id: Uuid,
        caption: Option<String>,
        expected_revision: i64,
    ) -> Result<QueuePost, Self::Error>;

    async fn publish_queue_post(
        &self,
        post_id: Uuid,
        expected_revision: i64,
    ) -> Result<QueuePost, Self::Error>;

    async fn cancel_queue_post(
        &self,
        post_id: Uuid,
        expected_revision: i64,
    ) -> Result<QueuePost, Self::Error>;

    fn is_conflict(error: &Self::Error) -> bool;
}

#[async_trait]
pub trait IngestService: Clone + Send + Sync + 'static {
    type Error: Error + Send + Sync + 'static;

    async fn create_url(&self, command: UrlIngestCommand) -> Result<IngestAccepted, Self::Error>;

    async fn create_media(
        &self,
        command: MediaIngestCommand,
    ) -> Result<IngestAccepted, Self::Error>;

    async fn list_duplicate_pending(
        &self,
        limit: usize,
    ) -> Result<Vec<DuplicatePendingCard>, Self::Error>;

    async fn accept_duplicate(
        &self,
        request_id: Uuid,
        media_id: Uuid,
    ) -> Result<DuplicateDecisionResult, Self::Error>;

    async fn force_save(&self, request_id: Uuid) -> Result<DuplicateDecisionResult, Self::Error>;
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

    async fn list_duplicate_pending(
        &self,
        _limit: usize,
    ) -> Result<Vec<DuplicatePendingCard>, Self::Error> {
        Err(IngestUnavailable)
    }

    async fn accept_duplicate(
        &self,
        _request_id: Uuid,
        _media_id: Uuid,
    ) -> Result<DuplicateDecisionResult, Self::Error> {
        Err(IngestUnavailable)
    }

    async fn force_save(&self, _request_id: Uuid) -> Result<DuplicateDecisionResult, Self::Error> {
        Err(IngestUnavailable)
    }
}

#[async_trait]
impl PublisherService for () {
    type Error = IngestUnavailable;

    async fn queue_count(&self) -> Result<usize, Self::Error> {
        Err(IngestUnavailable)
    }

    async fn list_queue(&self, _limit: usize) -> Result<Vec<QueuePost>, Self::Error> {
        Err(IngestUnavailable)
    }

    async fn get_queue_post(&self, _post_id: Uuid) -> Result<QueuePost, Self::Error> {
        Err(IngestUnavailable)
    }

    async fn move_queue_post(
        &self,
        _post_id: Uuid,
        _direction: QueueDirection,
        _expected_revision: i64,
    ) -> Result<QueuePost, Self::Error> {
        Err(IngestUnavailable)
    }

    async fn set_queue_post_slot(
        &self,
        _post_id: Uuid,
        _slot: OffsetDateTime,
        _expected_revision: i64,
    ) -> Result<QueuePost, Self::Error> {
        Err(IngestUnavailable)
    }

    async fn update_queue_caption(
        &self,
        _post_id: Uuid,
        _caption: Option<String>,
        _expected_revision: i64,
    ) -> Result<QueuePost, Self::Error> {
        Err(IngestUnavailable)
    }

    async fn publish_queue_post(
        &self,
        _post_id: Uuid,
        _expected_revision: i64,
    ) -> Result<QueuePost, Self::Error> {
        Err(IngestUnavailable)
    }

    async fn cancel_queue_post(
        &self,
        _post_id: Uuid,
        _expected_revision: i64,
    ) -> Result<QueuePost, Self::Error> {
        Err(IngestUnavailable)
    }

    fn is_conflict(_error: &Self::Error) -> bool {
        false
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
    #[error("Telegram upload timeout must be greater than zero and at most 24 hours")]
    InvalidUploadTimeout,
    #[error("Telegram HTTP client could not be configured: {0}")]
    HttpClient(#[source] reqwest::Error),
    #[error("Telegram Ctrl-C handler could not be initialized: {0}")]
    Shutdown(#[source] std::io::Error),
    #[error("Telegram update is still being processed: {0}")]
    UpdateInProgress(i64),
    #[error("Telegram URL ingest failed: {0}")]
    Ingest(#[source] Box<dyn Error + Send + Sync>),
    #[error("Telegram publisher operation failed: {0}")]
    Publisher(#[source] Box<dyn Error + Send + Sync>),
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

    async fn send_inline_keyboard(
        &self,
        chat_id: i64,
        text: &str,
        keyboard: Vec<Vec<InlineButton>>,
    ) -> Result<(), Self::Error>;

    async fn send_inline_keyboard_with_id(
        &self,
        chat_id: i64,
        text: &str,
        keyboard: Vec<Vec<InlineButton>>,
    ) -> Result<i64, Self::Error> {
        self.send_inline_keyboard(chat_id, text, keyboard).await?;
        Ok(0)
    }

    async fn send_force_reply(&self, chat_id: i64, text: &str) -> Result<i64, Self::Error> {
        self.send_text(chat_id, text).await?;
        Ok(0)
    }

    async fn delete_message(&self, _chat_id: i64, _message_id: i64) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Wait for the next queue card slot. Production Telegram calls pace this
    /// per chat; test adapters can override it with a no-op clock.
    async fn wait_for_queue_card(&self, _chat_id: i64) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn answer_callback_query(&self, callback_id: &str) -> Result<(), Self::Error>;

    async fn download_file(&self, file_id: &str, destination: &Path) -> Result<(), Self::Error>;

    fn is_retryable_error(error: &Self::Error) -> bool;

    fn retry_after(_error: &Self::Error) -> Option<Duration> {
        None
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum InlineButton {
    Callback { text: String, data: String },
    Url { text: String, url: String },
}

#[async_trait]
pub trait UpdateStore: Clone + Send + Sync + 'static {
    type Error: Error + Send + Sync + 'static;

    async fn claim_update(&self, update_id: i64) -> Result<UpdateClaimResult, Self::Error>;

    async fn complete_update(&self, claim: UpdateClaim) -> Result<(), Self::Error>;

    async fn release_update(&self, claim: UpdateClaim) -> Result<(), Self::Error>;
}

/// Process-local Telegram update deduplication. This intentionally belongs to
/// the Telegram adapter: it is only a delivery optimization, while business
/// effects remain durable in the five application tables and queue jobs.
#[derive(Clone, Default)]
pub struct MemoryUpdateStore {
    updates: Arc<Mutex<HashMap<i64, MemoryUpdate>>>,
}

#[derive(Debug, Clone, Copy)]
struct MemoryUpdate {
    claim_token: Option<Uuid>,
    claimed_at: Option<OffsetDateTime>,
    completed_at: Option<OffsetDateTime>,
}

#[derive(Debug, ThisError)]
pub enum MemoryUpdateStoreError {
    #[error("Telegram update ID must be positive: {0}")]
    InvalidUpdateId(i64),
    #[error("Telegram update claim was lost: {0}")]
    ClaimLost(i64),
    #[error("Telegram update store lock was poisoned")]
    LockPoisoned,
}

#[async_trait]
impl UpdateStore for MemoryUpdateStore {
    type Error = MemoryUpdateStoreError;

    async fn claim_update(&self, update_id: i64) -> Result<UpdateClaimResult, Self::Error> {
        if update_id <= 0 {
            return Err(MemoryUpdateStoreError::InvalidUpdateId(update_id));
        }
        let mut updates = self.updates.lock().map_err(|_| MemoryUpdateStoreError::LockPoisoned)?;
        let now = OffsetDateTime::now_utc();
        if let Some(update) = updates.get_mut(&update_id) {
            if update.completed_at.is_some() {
                return Ok(UpdateClaimResult::Completed);
            }
            if update
                .claimed_at
                .is_some_and(|claimed_at| claimed_at > now - time::Duration::minutes(5))
            {
                return Ok(UpdateClaimResult::InProgress);
            }
            let claim_token = Uuid::now_v7();
            update.claim_token = Some(claim_token);
            update.claimed_at = Some(now);
            return Ok(UpdateClaimResult::Claimed(UpdateClaim { update_id, claim_token }));
        }

        let claim_token = Uuid::now_v7();
        updates.insert(
            update_id,
            MemoryUpdate {
                claim_token: Some(claim_token),
                claimed_at: Some(now),
                completed_at: None,
            },
        );
        Ok(UpdateClaimResult::Claimed(UpdateClaim { update_id, claim_token }))
    }

    async fn complete_update(&self, claim: UpdateClaim) -> Result<(), Self::Error> {
        let mut updates = self.updates.lock().map_err(|_| MemoryUpdateStoreError::LockPoisoned)?;
        let update = updates
            .get_mut(&claim.update_id)
            .ok_or(MemoryUpdateStoreError::ClaimLost(claim.update_id))?;
        if update.completed_at.is_some() || update.claim_token != Some(claim.claim_token) {
            return Err(MemoryUpdateStoreError::ClaimLost(claim.update_id));
        }
        update.claim_token = None;
        update.claimed_at = None;
        update.completed_at = Some(OffsetDateTime::now_utc());
        Ok(())
    }

    async fn release_update(&self, claim: UpdateClaim) -> Result<(), Self::Error> {
        let mut updates = self.updates.lock().map_err(|_| MemoryUpdateStoreError::LockPoisoned)?;
        let update = updates
            .get_mut(&claim.update_id)
            .ok_or(MemoryUpdateStoreError::ClaimLost(claim.update_id))?;
        if update.completed_at.is_some() || update.claim_token != Some(claim.claim_token) {
            return Err(MemoryUpdateStoreError::ClaimLost(claim.update_id));
        }
        update.claim_token = None;
        update.claimed_at = None;
        Ok(())
    }
}

#[derive(Clone)]
pub struct TelegramService<A, S, I = (), P = ()> {
    api: A,
    update_store: S,
    ingest_service: Option<I>,
    publisher_service: Option<P>,
    admin_user_ids: Arc<BTreeSet<i64>>,
    response_limiter: Arc<Mutex<HashMap<RateLimitKey, Instant>>>,
    source_download_max_bytes: u64,
    views: Arc<Mutex<HashMap<i64, QueueViewState>>>,
}

#[derive(Debug, Clone, Default)]
struct QueueViewState {
    message_ids: Vec<i64>,
    prompt_message_id: Option<i64>,
    prompt: Option<QueuePrompt>,
    prompt_expires_at: Option<Instant>,
}

enum QueueMutation {
    Move { post_id: Uuid, expected_revision: i64, direction: QueueDirection },
    Caption { post_id: Uuid, expected_revision: i64, caption: Option<String> },
    Publish { post_id: Uuid, expected_revision: i64 },
    Cancel { post_id: Uuid, expected_revision: i64 },
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
            publisher_service: None,
            admin_user_ids: Arc::new(admin_user_ids.into_iter().collect()),
            response_limiter: Arc::new(Mutex::new(HashMap::new())),
            source_download_max_bytes: DEFAULT_TELEGRAM_SOURCE_DOWNLOAD_MAX_BYTES,
            views: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<A, S, I> TelegramService<A, S, I, ()>
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
            publisher_service: None,
            admin_user_ids: Arc::new(admin_user_ids.into_iter().collect()),
            response_limiter: Arc::new(Mutex::new(HashMap::new())),
            source_download_max_bytes: DEFAULT_TELEGRAM_SOURCE_DOWNLOAD_MAX_BYTES,
            views: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<A, S, I> TelegramService<A, S, I, ()>
where
    A: TelegramApi,
    S: UpdateStore,
    I: IngestService,
{
    pub fn with_publisher<P>(self, publisher_service: P) -> TelegramService<A, S, I, P>
    where
        P: PublisherService,
    {
        TelegramService {
            api: self.api,
            update_store: self.update_store,
            ingest_service: self.ingest_service,
            publisher_service: Some(publisher_service),
            admin_user_ids: self.admin_user_ids,
            response_limiter: self.response_limiter,
            source_download_max_bytes: self.source_download_max_bytes,
            views: self.views,
        }
    }
}

impl<A, S, I, P> TelegramService<A, S, I, P>
where
    A: TelegramApi,
    S: UpdateStore,
    I: IngestService,
    P: PublisherService,
{
    pub fn with_source_download_max_bytes(mut self, max_bytes: u64) -> Self {
        self.source_download_max_bytes = max_bytes;
        self
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
        if self.expire_queue_prompt(message.chat_id) {
            self.clear_queue_view(message.chat_id).await;
        }
        if message.media.is_none()
            && let Some(text) = message.text.as_deref()
            && (is_cancel_command(text)
                || (parse_command(text).is_none()
                    && self.queue_prompt_matches(message.chat_id, message.reply_to_message_id)))
        {
            return self.handle_queue_prompt(message, claim).await;
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
                if command == Command::Duplicates {
                    return self.handle_duplicate_command(message, claim).await;
                }
                if command == Command::Queue {
                    return self.handle_queue_command(message, claim).await;
                }
                if !self.allow_response(message.user_id, message.chat_id) {
                    self.complete(claim).await?;
                    return Ok(HandleOutcome::RateLimited);
                }
                let response = match command {
                    Command::Start => START_RESPONSE,
                    Command::Help => HELP_RESPONSE,
                    Command::Status => STATUS_RESPONSE,
                    Command::Add => ADD_USAGE_RESPONSE,
                    Command::Duplicates => unreachable!("duplicate command is handled above"),
                    Command::Queue => unreachable!("queue command is handled above"),
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
        let (media_kind, file_id, file_unique_id, file_size, mime_type, file_name) = match media {
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
                    self.complete(claim).await?;
                    return Ok(HandleOutcome::RateLimited);
                }
                self.send_and_complete(claim, message.chat_id, &response, rate_limit_key).await?;
                return Ok(HandleOutcome::MediaRejected);
            }
            let user_id = message.user_id.expect("authorized Telegram messages have a user ID");
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
            format!("✅ Ingest queued\nID: {}\nStatus: {}", accepted.request_id, accepted.status)
        };
        if !self.allow_response(message.user_id, message.chat_id) {
            self.complete(claim).await?;
            return Ok(HandleOutcome::RateLimited);
        }
        self.send_and_complete(claim, message.chat_id, &response, rate_limit_key).await?;
        Ok(HandleOutcome::Responded(Command::Add))
    }

    async fn handle_duplicate_command(
        &self,
        message: IncomingMessage,
        claim: UpdateClaim,
    ) -> Result<HandleOutcome, TelegramError> {
        let rate_limit_key = RateLimitKey { user_id: message.user_id, chat_id: message.chat_id };
        if !self.allow_response(message.user_id, message.chat_id) {
            self.complete(claim).await?;
            return Ok(HandleOutcome::RateLimited);
        }
        let Some(ingest_service) = self.ingest_service.as_ref() else {
            self.clear_response(rate_limit_key);
            self.release(claim).await?;
            return Err(TelegramError::Ingest(Box::new(IngestUnavailable)));
        };
        let cards = match ingest_service.list_duplicate_pending(3).await {
            Ok(cards) => cards,
            Err(error) => {
                self.clear_response(rate_limit_key);
                self.release(claim).await?;
                return Err(TelegramError::Ingest(Box::new(error)));
            }
        };
        let (text, keyboard) = render_duplicate_cards(&cards);
        if let Err(error) = self
            .api
            .send_inline_keyboard(message.chat_id, &text, keyboard)
            .await
            .map_err(|error| TelegramError::Api(Box::new(error)))
        {
            self.clear_response(rate_limit_key);
            self.release(claim).await?;
            return Err(error);
        }
        self.complete(claim).await?;
        Ok(HandleOutcome::Responded(Command::Duplicates))
    }

    async fn handle_queue_command(
        &self,
        message: IncomingMessage,
        claim: UpdateClaim,
    ) -> Result<HandleOutcome, TelegramError> {
        let rate_limit_key = RateLimitKey { user_id: message.user_id, chat_id: message.chat_id };
        if !self.allow_response(message.user_id, message.chat_id) {
            self.complete(claim).await?;
            return Ok(HandleOutcome::RateLimited);
        }
        self.clear_queue_view(message.chat_id).await;
        let Some(publisher) = self.publisher_service.as_ref() else {
            self.clear_response(rate_limit_key);
            self.release(claim).await?;
            return Err(TelegramError::Publisher(Box::new(IngestUnavailable)));
        };
        let total = match publisher.queue_count().await {
            Ok(total) => total,
            Err(error) => {
                self.clear_response(rate_limit_key);
                self.release(claim).await?;
                return Err(TelegramError::Publisher(Box::new(error)));
            }
        };
        if total == 0 {
            self.send_and_complete(claim, message.chat_id, "Queue is empty.", rate_limit_key)
                .await?;
            return Ok(HandleOutcome::Responded(Command::Queue));
        }
        let (text, keyboard) = render_queue_count(total);
        match self.send_queue_card(message.chat_id, &text, keyboard).await {
            Ok(message_id) => {
                self.store_queue_count_message(message.chat_id, message_id);
                self.complete(claim).await?;
                Ok(HandleOutcome::Responded(Command::Queue))
            }
            Err(error) => {
                self.clear_response(rate_limit_key);
                self.release(claim).await?;
                Err(error)
            }
        }
    }

    async fn send_queue_card(
        &self,
        chat_id: i64,
        text: &str,
        keyboard: Vec<Vec<InlineButton>>,
    ) -> Result<i64, TelegramError> {
        for attempt in 0..=MAX_QUEUE_CARD_RETRIES {
            self.api
                .wait_for_queue_card(chat_id)
                .await
                .map_err(|error| TelegramError::Api(Box::new(error)))?;
            match self.api.send_inline_keyboard_with_id(chat_id, text, keyboard.clone()).await {
                Ok(message_id) => return Ok(message_id),
                Err(error) => {
                    if attempt < MAX_QUEUE_CARD_RETRIES
                        && let Some(delay) = A::retry_after(&error)
                    {
                        tokio::time::sleep(delay.min(MAX_QUEUE_RETRY_AFTER)).await;
                        continue;
                    }
                    return Err(TelegramError::Api(Box::new(error)));
                }
            }
        }
        unreachable!("the bounded queue card retry loop always returns")
    }

    async fn handle_queue_prompt(
        &self,
        message: IncomingMessage,
        claim: UpdateClaim,
    ) -> Result<HandleOutcome, TelegramError> {
        let Some(prompt) = self.take_queue_prompt(message.chat_id) else {
            if message.text.as_deref().is_some_and(is_cancel_command) {
                self.clear_queue_view(message.chat_id).await;
                self.send_and_complete(
                    claim,
                    message.chat_id,
                    "No active queue prompt.",
                    RateLimitKey { user_id: message.user_id, chat_id: message.chat_id },
                )
                .await?;
                return Ok(HandleOutcome::Responded(Command::Queue));
            }
            self.complete(claim).await?;
            return Ok(HandleOutcome::UnrecognizedIgnored);
        };
        let text = message.text.as_deref().unwrap_or_default().trim();
        if is_cancel_command(text) {
            self.clear_queue_view(message.chat_id).await;
            self.send_and_complete(
                claim,
                message.chat_id,
                "Queue prompt cancelled.",
                RateLimitKey { user_id: message.user_id, chat_id: message.chat_id },
            )
            .await?;
            return Ok(HandleOutcome::Responded(Command::Queue));
        }
        let Some(publisher) = self.publisher_service.as_ref() else {
            self.release(claim).await?;
            return Err(TelegramError::Publisher(Box::new(IngestUnavailable)));
        };
        let result = match prompt.kind {
            QueuePromptKind::Caption => {
                let caption = if text.is_empty() || text.eq_ignore_ascii_case("/clear") {
                    None
                } else {
                    Some(text.to_owned())
                };
                publisher
                    .update_queue_caption(prompt.post_id, caption, prompt.expected_revision)
                    .await
                    .map(|_| "✅ Post text updated.".to_owned())
            }
            QueuePromptKind::Slot => {
                let slot =
                    OffsetDateTime::parse(text, &time::format_description::well_known::Rfc3339);
                match slot {
                    Ok(slot) => publisher
                        .set_queue_post_slot(prompt.post_id, slot, prompt.expected_revision)
                        .await
                        .map(|_| "✅ Queue slot updated.".to_owned()),
                    Err(_) => {
                        self.restore_queue_prompt(message.chat_id, prompt);
                        self.send_and_complete(
                            claim,
                            message.chat_id,
                            "⚠️ Send an RFC3339 slot, for example 2026-08-12T18:30:00Z, or /cancel.",
                            RateLimitKey { user_id: message.user_id, chat_id: message.chat_id },
                        )
                        .await?;
                        return Ok(HandleOutcome::Responded(Command::Queue));
                    }
                }
            }
        };
        let response = match result {
            Ok(response) => response,
            Err(error) if P::is_conflict(&error) => "Queue changed; run /queue again.".to_owned(),
            Err(error) => format!("⚠️ Queue operation failed: {error}"),
        };
        self.clear_queue_view(message.chat_id).await;
        self.send_and_complete(
            claim,
            message.chat_id,
            &response,
            RateLimitKey { user_id: message.user_id, chat_id: message.chat_id },
        )
        .await?;
        Ok(HandleOutcome::Responded(Command::Queue))
    }

    async fn handle_queue_callback(
        &self,
        chat_id: i64,
        data: CallbackData,
        claim: UpdateClaim,
    ) -> Result<HandleOutcome, TelegramError> {
        match data {
            CallbackData::QueueCount { count } => {
                self.handle_queue_count(chat_id, count, claim).await
            }
            CallbackData::QueueSetSlot { post_id, expected_revision } => {
                self.begin_queue_prompt(
                    chat_id,
                    QueuePrompt {
                        post_id,
                        expected_revision,
                        kind: QueuePromptKind::Slot,
                        prompt_message_id: 0,
                    },
                    "Send an RFC3339 queue slot, for example 2026-08-12T18:30:00Z, or /cancel.",
                    claim,
                )
                .await
            }
            CallbackData::QueueEditCaption { post_id, expected_revision } => {
                self.begin_queue_prompt(
                    chat_id,
                    QueuePrompt {
                        post_id,
                        expected_revision,
                        kind: QueuePromptKind::Caption,
                        prompt_message_id: 0,
                    },
                    "Send new post text, /clear to remove it, or /cancel.",
                    claim,
                )
                .await
            }
            CallbackData::QueueEarlier { post_id, expected_revision } => {
                self.apply_queue_mutation(
                    chat_id,
                    claim,
                    QueueMutation::Move {
                        post_id,
                        expected_revision,
                        direction: QueueDirection::Earlier,
                    },
                )
                .await
            }
            CallbackData::QueueLater { post_id, expected_revision } => {
                self.apply_queue_mutation(
                    chat_id,
                    claim,
                    QueueMutation::Move {
                        post_id,
                        expected_revision,
                        direction: QueueDirection::Later,
                    },
                )
                .await
            }
            CallbackData::QueueClearCaption { post_id, expected_revision } => {
                self.apply_queue_mutation(
                    chat_id,
                    claim,
                    QueueMutation::Caption { post_id, expected_revision, caption: None },
                )
                .await
            }
            CallbackData::QueuePublishNow { post_id, expected_revision } => {
                self.apply_queue_mutation(
                    chat_id,
                    claim,
                    QueueMutation::Publish { post_id, expected_revision },
                )
                .await
            }
            CallbackData::QueueCancel { post_id, expected_revision } => {
                self.apply_queue_mutation(
                    chat_id,
                    claim,
                    QueueMutation::Cancel { post_id, expected_revision },
                )
                .await
            }
            _ => unreachable!("non-queue callback routed to queue handler"),
        }
    }

    async fn handle_queue_count(
        &self,
        chat_id: i64,
        requested: usize,
        claim: UpdateClaim,
    ) -> Result<HandleOutcome, TelegramError> {
        let Some(publisher) = self.publisher_service.as_ref() else {
            self.release(claim).await?;
            return Err(TelegramError::Publisher(Box::new(IngestUnavailable)));
        };
        let total = match publisher.queue_count().await {
            Ok(total) => total,
            Err(error) => {
                self.release(claim).await?;
                return Err(TelegramError::Publisher(Box::new(error)));
            }
        };
        if total < requested {
            self.clear_queue_view(chat_id).await;
            self.send_queue_count_prompt(chat_id, total, claim).await?;
            return Ok(HandleOutcome::CallbackHandled);
        }
        let posts = match publisher.list_queue(requested.min(QUEUE_MAX_POSTS)).await {
            Ok(posts) => posts,
            Err(error) => {
                self.release(claim).await?;
                return Err(TelegramError::Publisher(Box::new(error)));
            }
        };
        if posts.len() != requested {
            self.clear_queue_view(chat_id).await;
            self.send_queue_count_prompt(chat_id, posts.len(), claim).await?;
            return Ok(HandleOutcome::CallbackHandled);
        }

        self.clear_queue_view(chat_id).await;
        for (index, post) in posts.iter().enumerate() {
            let (text, keyboard) = render_queue_post(index + 1, post);
            let message_id = match self.send_queue_card(chat_id, &text, keyboard).await {
                Ok(message_id) => message_id,
                Err(error) => {
                    self.clear_queue_view(chat_id).await;
                    let report_result = self
                        .api
                        .send_text(chat_id, "⚠️ Queue view could not be rendered completely.");
                    self.complete(claim).await?;
                    if let Err(report_error) = report_result.await {
                        return Err(TelegramError::Api(Box::new(report_error)));
                    }
                    tracing::warn!(
                        ?error,
                        chat_id,
                        "queue view rendering failed after partial send"
                    );
                    return Ok(HandleOutcome::CallbackHandled);
                }
            };
            self.store_queue_message(chat_id, message_id);
        }
        self.complete(claim).await?;
        Ok(HandleOutcome::CallbackHandled)
    }

    async fn send_queue_count_prompt(
        &self,
        chat_id: i64,
        total: usize,
        claim: UpdateClaim,
    ) -> Result<(), TelegramError> {
        if total == 0 {
            self.send_and_complete(
                claim,
                chat_id,
                "Queue is empty.",
                RateLimitKey { user_id: Some(chat_id), chat_id },
            )
            .await?;
            return Ok(());
        }
        let (text, keyboard) = render_queue_count(total);
        match self.send_queue_card(chat_id, &text, keyboard).await {
            Ok(message_id) => {
                self.store_queue_count_message(chat_id, message_id);
                self.complete(claim).await?;
                Ok(())
            }
            Err(error) => {
                self.release(claim).await?;
                Err(error)
            }
        }
    }

    async fn begin_queue_prompt(
        &self,
        chat_id: i64,
        mut prompt: QueuePrompt,
        text: &str,
        claim: UpdateClaim,
    ) -> Result<HandleOutcome, TelegramError> {
        let Some(publisher) = self.publisher_service.as_ref() else {
            self.release(claim).await?;
            return Err(TelegramError::Publisher(Box::new(IngestUnavailable)));
        };
        let post = match publisher.get_queue_post(prompt.post_id).await {
            Ok(post) => post,
            Err(error) if P::is_conflict(&error) => {
                self.clear_queue_view(chat_id).await;
                self.send_and_complete(
                    claim,
                    chat_id,
                    "Queue changed; run /queue again.",
                    RateLimitKey { user_id: Some(chat_id), chat_id },
                )
                .await?;
                return Ok(HandleOutcome::CallbackHandled);
            }
            Err(error) => {
                self.release(claim).await?;
                return Err(TelegramError::Publisher(Box::new(error)));
            }
        };
        let valid = post.revision == prompt.expected_revision
            && post.state.is_queue_mutable()
            && (prompt.kind != QueuePromptKind::Slot || post.state == PostState::Queued);
        if !valid {
            self.clear_queue_view(chat_id).await;
            self.send_and_complete(
                claim,
                chat_id,
                "Queue changed; run /queue again.",
                RateLimitKey { user_id: Some(chat_id), chat_id },
            )
            .await?;
            return Ok(HandleOutcome::CallbackHandled);
        }
        self.clear_queue_view(chat_id).await;
        match self.api.send_force_reply(chat_id, text).await {
            Ok(message_id) => {
                prompt.prompt_message_id = message_id;
                self.store_queue_prompt(chat_id, message_id, prompt);
                self.complete(claim).await?;
                Ok(HandleOutcome::CallbackHandled)
            }
            Err(error) => {
                self.release(claim).await?;
                Err(TelegramError::Api(Box::new(error)))
            }
        }
    }

    async fn apply_queue_mutation(
        &self,
        chat_id: i64,
        claim: UpdateClaim,
        mutation: QueueMutation,
    ) -> Result<HandleOutcome, TelegramError> {
        let Some(publisher) = self.publisher_service.as_ref() else {
            self.release(claim).await?;
            return Err(TelegramError::Publisher(Box::new(IngestUnavailable)));
        };
        let result = match mutation {
            QueueMutation::Move { post_id, direction, expected_revision } => publisher
                .move_queue_post(post_id, direction, expected_revision)
                .await
                .map(|_| match direction {
                    QueueDirection::Earlier => "✅ Post moved earlier.".to_owned(),
                    QueueDirection::Later => "✅ Post moved later.".to_owned(),
                }),
            QueueMutation::Caption { post_id, expected_revision, caption } => {
                let cleared = caption.is_none();
                publisher.update_queue_caption(post_id, caption, expected_revision).await.map(
                    |_| {
                        if cleared {
                            "✅ Post text cleared.".to_owned()
                        } else {
                            "✅ Post text updated.".to_owned()
                        }
                    },
                )
            }
            QueueMutation::Publish { post_id, expected_revision } => publisher
                .publish_queue_post(post_id, expected_revision)
                .await
                .map(|_| "✅ Post queued for immediate publication.".to_owned()),
            QueueMutation::Cancel { post_id, expected_revision } => publisher
                .cancel_queue_post(post_id, expected_revision)
                .await
                .map(|_| "✅ Post removed from the queue.".to_owned()),
        };
        let response = match result {
            Ok(response) => response,
            Err(error) if P::is_conflict(&error) => "Queue changed; run /queue again.".to_owned(),
            Err(error) => format!("⚠️ Queue operation failed: {error}"),
        };
        self.clear_queue_view(chat_id).await;
        self.send_and_complete(
            claim,
            chat_id,
            &response,
            RateLimitKey { user_id: Some(chat_id), chat_id },
        )
        .await?;
        Ok(HandleOutcome::CallbackHandled)
    }

    fn expire_queue_prompt(&self, chat_id: i64) -> bool {
        let mut views = self.views.lock().expect("Telegram queue view lock is not poisoned");
        let Some(view) = views.get_mut(&chat_id) else { return false };
        if view.prompt_expires_at.is_some_and(|expires_at| expires_at <= Instant::now()) {
            view.prompt = None;
            view.prompt_expires_at = None;
            return true;
        }
        false
    }

    fn queue_prompt_matches(&self, chat_id: i64, reply_to_message_id: Option<i64>) -> bool {
        let views = self.views.lock().expect("Telegram queue view lock is not poisoned");
        views
            .get(&chat_id)
            .and_then(|view| view.prompt.as_ref())
            .is_some_and(|prompt| reply_to_message_id == Some(prompt.prompt_message_id))
    }

    fn take_queue_prompt(&self, chat_id: i64) -> Option<QueuePrompt> {
        self.views
            .lock()
            .expect("Telegram queue view lock is not poisoned")
            .get_mut(&chat_id)
            .and_then(|view| view.prompt.take())
    }

    fn restore_queue_prompt(&self, chat_id: i64, prompt: QueuePrompt) {
        let mut views = self.views.lock().expect("Telegram queue view lock is not poisoned");
        let view = views.entry(chat_id).or_default();
        view.prompt = Some(prompt);
        view.prompt_expires_at = Some(Instant::now() + QUEUE_PROMPT_TTL);
    }

    fn store_queue_count_message(&self, chat_id: i64, message_id: i64) {
        if message_id <= 0 {
            return;
        }
        let mut views = self.views.lock().expect("Telegram queue view lock is not poisoned");
        let view = views.entry(chat_id).or_default();
        if view.message_ids.len() < QUEUE_MAX_POSTS + 1 {
            view.message_ids.push(message_id);
        }
    }

    fn store_queue_message(&self, chat_id: i64, message_id: i64) {
        self.store_queue_count_message(chat_id, message_id);
    }

    fn store_queue_prompt(&self, chat_id: i64, message_id: i64, prompt: QueuePrompt) {
        let mut views = self.views.lock().expect("Telegram queue view lock is not poisoned");
        let view = views.entry(chat_id).or_default();
        view.prompt_message_id = (message_id > 0).then_some(message_id);
        view.prompt = Some(prompt);
        view.prompt_expires_at = Some(Instant::now() + QUEUE_PROMPT_TTL);
    }

    async fn clear_queue_view(&self, chat_id: i64) {
        let ids = self
            .views
            .lock()
            .expect("Telegram queue view lock is not poisoned")
            .remove(&chat_id)
            .map(|view| {
                view.message_ids
                    .into_iter()
                    .chain(view.prompt_message_id)
                    .filter(|message_id| *message_id > 0)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for message_id in ids {
            let _ = self.api.delete_message(chat_id, message_id).await;
        }
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
                let (chat_id, is_private) =
                    callback.message.as_ref().map_or((None, false), |message| {
                        (Some(message.chat().id.0), message.chat().is_private())
                    });
                self.handle_callback(IncomingCallback {
                    update_id,
                    callback_id: callback.id.0,
                    user_id: i64::try_from(callback.from.id.0).unwrap_or_default(),
                    chat_id,
                    is_private,
                    data: callback.data,
                })
                .await
            }
            _ => {
                let claim = match self.claim(update_id).await? {
                    UpdateClaimResult::Claimed(claim) => claim,
                    UpdateClaimResult::Completed => return Ok(HandleOutcome::DuplicateIgnored),
                    UpdateClaimResult::InProgress => {
                        return Err(TelegramError::UpdateInProgress(update_id));
                    }
                };
                self.complete(claim).await?;
                Ok(HandleOutcome::NonMessageIgnored)
            }
        }
    }

    pub async fn handle_callback(
        &self,
        callback: IncomingCallback,
    ) -> Result<HandleOutcome, TelegramError> {
        let claim = match self.claim(callback.update_id).await? {
            UpdateClaimResult::Claimed(claim) => claim,
            UpdateClaimResult::Completed => return Ok(HandleOutcome::DuplicateIgnored),
            UpdateClaimResult::InProgress => {
                return Err(TelegramError::UpdateInProgress(callback.update_id));
            }
        };

        // Telegram clients keep showing a spinner until this is answered. Do
        // this before any repository work, including for rejected callbacks.
        if let Err(error) = self.api.answer_callback_query(&callback.callback_id).await {
            self.release(claim).await?;
            return Err(TelegramError::Api(Box::new(error)));
        }

        let authorized = callback.is_private
            && callback.chat_id == Some(callback.user_id)
            && self.admin_user_ids.contains(&callback.user_id);
        if !authorized {
            warn!(
                target: "sooqa.telegram",
                update_id = callback.update_id,
                user_id = callback.user_id,
                chat_id = ?callback.chat_id,
                "unauthorized Telegram callback attempt"
            );
            self.complete(claim).await?;
            return Ok(HandleOutcome::CallbackHandled);
        }
        let Some(data) = callback.data.as_deref().and_then(CallbackData::parse) else {
            self.complete(claim).await?;
            return Ok(HandleOutcome::CallbackHandled);
        };

        let Some(chat_id) = callback.chat_id else {
            self.complete(claim).await?;
            return Ok(HandleOutcome::CallbackHandled);
        };
        if is_queue_callback(&data) {
            return self.handle_queue_callback(chat_id, data, claim).await;
        }
        let Some(ingest_service) = self.ingest_service.as_ref() else {
            self.release(claim).await?;
            return Err(TelegramError::Ingest(Box::new(IngestUnavailable)));
        };
        let response = match data {
            CallbackData::DuplicateUse { request_id, media_id } => {
                match ingest_service.accept_duplicate(request_id, media_id).await {
                    Ok(result) => {
                        format_duplicate_decision_response("✅ Duplicate accepted", &result)
                    }
                    Err(error) => format!("⚠️ Duplicate decision was not applied: {error}"),
                }
            }
            CallbackData::DuplicateForceSave { request_id } => {
                match ingest_service.force_save(request_id).await {
                    Ok(result) => {
                        format_duplicate_decision_response("✅ Save anyway queued", &result)
                    }
                    Err(error) => format!("⚠️ Save anyway was not applied: {error}"),
                }
            }
            _ => unreachable!("queue callback was routed before ingest callbacks"),
        };
        if let Err(error) = self.api.send_text(chat_id, &response).await {
            self.release(claim).await?;
            return Err(TelegramError::Api(Box::new(error)));
        }
        self.complete(claim).await?;
        Ok(HandleOutcome::CallbackHandled)
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
        "duplicates" => Some(Command::Duplicates),
        "queue" => Some(Command::Queue),
        _ => None,
    }
}

fn is_cancel_command(text: &str) -> bool {
    let Some(command) = text.split_whitespace().next().and_then(|value| value.strip_prefix('/'))
    else {
        return false;
    };
    command.split('@').next().is_some_and(|value| value.eq_ignore_ascii_case("cancel"))
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
    DuplicateUse { request_id: Uuid, media_id: Uuid },
    DuplicateForceSave { request_id: Uuid },
    QueueCount { count: usize },
    QueueEarlier { post_id: Uuid, expected_revision: i64 },
    QueueLater { post_id: Uuid, expected_revision: i64 },
    QueueSetSlot { post_id: Uuid, expected_revision: i64 },
    QueueEditCaption { post_id: Uuid, expected_revision: i64 },
    QueueClearCaption { post_id: Uuid, expected_revision: i64 },
    QueuePublishNow { post_id: Uuid, expected_revision: i64 },
    QueueCancel { post_id: Uuid, expected_revision: i64 },
}

impl CallbackData {
    pub fn encode(self) -> String {
        match self {
            Self::DuplicateUse { request_id, media_id } => {
                format!("v1:duplicate_use:{}:{}", encode_uuid(request_id), encode_uuid(media_id))
            }
            Self::DuplicateForceSave { request_id } => {
                format!("v1:duplicate_force_save:{}", encode_uuid(request_id))
            }
            Self::QueueCount { count } => format!("v1:q:n:{count}"),
            Self::QueueEarlier { post_id, expected_revision } => {
                format!("v1:q:e:{}:{expected_revision}", encode_uuid(post_id))
            }
            Self::QueueLater { post_id, expected_revision } => {
                format!("v1:q:l:{}:{expected_revision}", encode_uuid(post_id))
            }
            Self::QueueSetSlot { post_id, expected_revision } => {
                format!("v1:q:s:{}:{expected_revision}", encode_uuid(post_id))
            }
            Self::QueueEditCaption { post_id, expected_revision } => {
                format!("v1:q:c:{}:{expected_revision}", encode_uuid(post_id))
            }
            Self::QueueClearCaption { post_id, expected_revision } => {
                format!("v1:q:z:{}:{expected_revision}", encode_uuid(post_id))
            }
            Self::QueuePublishNow { post_id, expected_revision } => {
                format!("v1:q:p:{}:{expected_revision}", encode_uuid(post_id))
            }
            Self::QueueCancel { post_id, expected_revision } => {
                format!("v1:q:x:{}:{expected_revision}", encode_uuid(post_id))
            }
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        let mut parts = value.split(':');
        if parts.next()? != "v1" {
            return None;
        }
        match parts.next()? {
            "duplicate_use" => {
                let request_id = decode_uuid(parts.next()?)?;
                let media_id = decode_uuid(parts.next()?)?;
                if parts.next().is_some() {
                    return None;
                }
                Some(Self::DuplicateUse { request_id, media_id })
            }
            "duplicate_force_save" => {
                let request_id = decode_uuid(parts.next()?)?;
                if parts.next().is_some() {
                    return None;
                }
                Some(Self::DuplicateForceSave { request_id })
            }
            "q" => {
                let action = parts.next()?;
                match action {
                    "n" => {
                        let count = parts.next()?.parse().ok()?;
                        (parts.next().is_none() && (1..=QUEUE_MAX_POSTS).contains(&count))
                            .then_some(Self::QueueCount { count })
                    }
                    action @ ("e" | "l" | "s" | "c" | "z" | "p" | "x") => {
                        let post_id = decode_uuid(parts.next()?)?;
                        let expected_revision = parts.next()?.parse().ok()?;
                        if parts.next().is_some() {
                            return None;
                        }
                        Some(match action {
                            "e" => Self::QueueEarlier { post_id, expected_revision },
                            "l" => Self::QueueLater { post_id, expected_revision },
                            "s" => Self::QueueSetSlot { post_id, expected_revision },
                            "c" => Self::QueueEditCaption { post_id, expected_revision },
                            "z" => Self::QueueClearCaption { post_id, expected_revision },
                            "p" => Self::QueuePublishNow { post_id, expected_revision },
                            "x" => Self::QueueCancel { post_id, expected_revision },
                            _ => unreachable!("queue action is restricted above"),
                        })
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }
}

fn encode_uuid(value: Uuid) -> String {
    URL_SAFE_NO_PAD.encode(value.as_bytes())
}

fn decode_uuid(value: &str) -> Option<Uuid> {
    let bytes = URL_SAFE_NO_PAD.decode(value).ok()?;
    let bytes: [u8; 16] = bytes.try_into().ok()?;
    Some(Uuid::from_bytes(bytes))
}

fn render_duplicate_cards(cards: &[DuplicatePendingCard]) -> (String, Vec<Vec<InlineButton>>) {
    if cards.is_empty() {
        return ("No pending duplicate decisions.".to_owned(), Vec::new());
    }

    let mut text = String::from("Pending duplicate decisions:\n");
    let mut keyboard = Vec::new();
    for card in cards {
        text.push_str(&format!(
            "\nIngest {}\nSource: {}\n",
            card.request_id,
            truncate_for_telegram(&card.source_url, 320)
        ));
        for (index, candidate) in card.candidates.iter().enumerate() {
            let (state_label, can_use, open_url) = match &candidate.storage {
                DuplicateCandidateStorage::Ready { open_url } => ("ready", true, open_url.clone()),
                DuplicateCandidateStorage::PendingStorage => ("storing", true, None),
                DuplicateCandidateStorage::Unavailable { state } => (state.as_str(), false, None),
                DuplicateCandidateStorage::Missing => ("missing", false, None),
            };
            text.push_str(&format!(
                "Candidate {}: {} (score {} bps) — {}\n",
                index + 1,
                candidate.classification,
                candidate.score_bps,
                state_label
            ));
            let mut row = Vec::new();
            if let Some(open_url) = open_url {
                row.push(InlineButton::Url { text: "Open media".to_owned(), url: open_url });
            }
            if can_use {
                row.push(InlineButton::Callback {
                    text: "Use this".to_owned(),
                    data: CallbackData::DuplicateUse {
                        request_id: card.request_id,
                        media_id: candidate.media_id,
                    }
                    .encode(),
                });
            }
            if !row.is_empty() {
                keyboard.push(row);
            }
        }
        keyboard.push(vec![InlineButton::Callback {
            text: "Save anyway".to_owned(),
            data: CallbackData::DuplicateForceSave { request_id: card.request_id }.encode(),
        }]);
    }
    (text, keyboard)
}

fn truncate_for_telegram(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    let mut end = max_bytes.saturating_sub(3);
    while !value.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    format!("{}...", &value[..end])
}

fn format_duplicate_decision_response(prefix: &str, result: &DuplicateDecisionResult) -> String {
    format!("{prefix}\nID: {}\nStatus: {}", result.request_id, result.status)
}

fn is_queue_callback(data: &CallbackData) -> bool {
    matches!(
        data,
        CallbackData::QueueCount { .. }
            | CallbackData::QueueEarlier { .. }
            | CallbackData::QueueLater { .. }
            | CallbackData::QueueSetSlot { .. }
            | CallbackData::QueueEditCaption { .. }
            | CallbackData::QueueClearCaption { .. }
            | CallbackData::QueuePublishNow { .. }
            | CallbackData::QueueCancel { .. }
    )
}

fn render_queue_count(total: usize) -> (String, Vec<Vec<InlineButton>>) {
    let selectable_total = total.min(QUEUE_MAX_POSTS);
    let mut counts = Vec::new();
    let mut scale = 1_usize;
    while scale <= selectable_total {
        for base in [1_usize, 2, 5] {
            let value = base.saturating_mul(scale);
            if value <= selectable_total {
                counts.push(value);
            }
        }
        scale = scale.saturating_mul(10);
    }
    if total <= QUEUE_MAX_POSTS && !counts.contains(&total) {
        counts.push(total);
    }
    counts.sort_unstable();
    counts.dedup();
    let keyboard = counts
        .into_iter()
        .map(|count| {
            vec![InlineButton::Callback {
                text: if count == total { format!("All {count}") } else { count.to_string() },
                data: CallbackData::QueueCount { count }.encode(),
            }]
        })
        .collect();
    (format!("Publication queue: {total} post(s). How many should I show?"), keyboard)
}

fn render_queue_post(index: usize, post: &QueuePost) -> (String, Vec<Vec<InlineButton>>) {
    let slot = post
        .cadence_slot_at
        .map_or_else(|| "unscheduled".to_owned(), |slot| format_queue_slot(slot, &post.time_zone));
    let title = post.title.as_deref().unwrap_or("—");
    let description = post.description.as_deref().unwrap_or("—");
    let tags = if post.tags.is_empty() { "—".to_owned() } else { post.tags.join(", ") };
    let source_url = post.source_url.as_deref().unwrap_or("—");
    let post_text = post.caption.as_deref().unwrap_or("—");
    let text = format!(
        "Post {index}\nState: {}\nScheduled: {slot}\nMedia: {}\nTitle: {}\nDescription: {}\nTags: {}\nSource: {}\nPost text: {}",
        post.state.as_str(),
        truncate_for_telegram(&post.media_kind, 80),
        truncate_for_telegram(title, 240),
        truncate_for_telegram(description, 480),
        truncate_for_telegram(&tags, 240),
        truncate_for_telegram(source_url, 320),
        truncate_for_telegram(post_text, 1_024),
    );
    let mut keyboard = Vec::new();
    if let Some(url) = post
        .storage_chat_id
        .zip(post.storage_message_id)
        .and_then(|(chat_id, message_id)| storage_message_url(chat_id, message_id))
    {
        keyboard.push(vec![InlineButton::Url { text: "Open media".to_owned(), url }]);
    }
    if post.state == PostState::Queued && post.cadence_slot_at.is_some() {
        keyboard.push(vec![
            InlineButton::Callback {
                text: "Earlier".to_owned(),
                data: CallbackData::QueueEarlier {
                    post_id: post.id,
                    expected_revision: post.revision,
                }
                .encode(),
            },
            InlineButton::Callback {
                text: "Later".to_owned(),
                data: CallbackData::QueueLater {
                    post_id: post.id,
                    expected_revision: post.revision,
                }
                .encode(),
            },
        ]);
    }
    if post.state == PostState::Queued {
        keyboard.push(vec![InlineButton::Callback {
            text: "Set slot".to_owned(),
            data: CallbackData::QueueSetSlot { post_id: post.id, expected_revision: post.revision }
                .encode(),
        }]);
    }
    let mut caption_actions = vec![InlineButton::Callback {
        text: "Edit post text".to_owned(),
        data: CallbackData::QueueEditCaption { post_id: post.id, expected_revision: post.revision }
            .encode(),
    }];
    if post.caption.is_some() {
        caption_actions.push(InlineButton::Callback {
            text: "Clear text".to_owned(),
            data: CallbackData::QueueClearCaption {
                post_id: post.id,
                expected_revision: post.revision,
            }
            .encode(),
        });
    }
    keyboard.push(caption_actions);
    keyboard.push(vec![
        InlineButton::Callback {
            text: "Post now".to_owned(),
            data: CallbackData::QueuePublishNow {
                post_id: post.id,
                expected_revision: post.revision,
            }
            .encode(),
        },
        InlineButton::Callback {
            text: "Remove from queue".to_owned(),
            data: CallbackData::QueueCancel { post_id: post.id, expected_revision: post.revision }
                .encode(),
        },
    ]);
    (text, keyboard)
}

fn format_queue_slot(slot: OffsetDateTime, time_zone: &str) -> String {
    let Some(utc) = DateTime::<Utc>::from_timestamp(slot.unix_timestamp(), slot.nanosecond())
    else {
        return format!("{slot} ({time_zone})");
    };
    let Ok(time_zone) = time_zone.parse::<Tz>() else {
        return format!("{slot} ({time_zone})");
    };
    format!("{} ({})", utc.with_timezone(&time_zone).format("%Y-%m-%d %H:%M %Z"), time_zone)
}

fn storage_message_url(chat_id: i64, message_id: i64) -> Option<String> {
    if chat_id >= 0 || message_id <= 0 {
        return None;
    }
    let raw_id = chat_id.to_string();
    let internal_id = raw_id.strip_prefix("-100").unwrap_or_else(|| raw_id.trim_start_matches('-'));
    (!internal_id.is_empty()).then(|| format!("https://t.me/c/{internal_id}/{message_id}"))
}

#[derive(Clone)]
pub struct TeloxideApi {
    control_bot: Bot,
    media_bot: Bot,
    upload_bot: Bot,
    cloud_download_limit_bytes: Option<u64>,
    cloud_upload_limit_bytes: Option<u64>,
    source_download_max_bytes: u64,
    queue_card_next_send: Arc<Mutex<HashMap<i64, Instant>>>,
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
            queue_card_next_send: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn with_source_download_max_bytes(mut self, max_bytes: u64) -> Self {
        self.source_download_max_bytes = max_bytes;
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
            | TelegramApiError::Api(_)
            | TelegramApiError::Download(_)
            | TelegramApiError::Io(_) => false,
        }
    }

    fn retry_after(error: &Self::Error) -> Option<Duration> {
        match error {
            TelegramApiError::Api(teloxide::RequestError::RetryAfter(seconds)) => {
                Some(seconds.duration())
            }
            _ => None,
        }
    }

    async fn wait_for_queue_card(&self, chat_id: i64) -> Result<(), Self::Error> {
        let now = Instant::now();
        let target = {
            let mut next_send =
                self.queue_card_next_send.lock().expect("queue pacing lock is not poisoned");
            let target = next_send.get(&chat_id).copied().unwrap_or(now).max(now);
            next_send.insert(chat_id, target + QUEUE_CARD_INTERVAL);
            target
        };
        tokio::time::sleep(target.saturating_duration_since(now)).await;
        Ok(())
    }

    async fn send_text(&self, chat_id: i64, text: &str) -> Result<(), Self::Error> {
        self.bot()
            .send_message(ChatId(chat_id), text.to_owned())
            .await
            .map(|_| ())
            .map_err(TelegramApiError::Api)
    }

    async fn send_inline_keyboard(
        &self,
        chat_id: i64,
        text: &str,
        keyboard: Vec<Vec<InlineButton>>,
    ) -> Result<(), Self::Error> {
        let keyboard = keyboard
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|button| match button {
                        InlineButton::Callback { text, data } => {
                            InlineKeyboardButton::callback(text, data)
                        }
                        InlineButton::Url { text, url } => {
                            let url = Url::parse(&url).expect("rendered Telegram URL is valid");
                            InlineKeyboardButton::url(text, url)
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        self.bot()
            .send_message(ChatId(chat_id), text.to_owned())
            .reply_markup(InlineKeyboardMarkup::new(keyboard))
            .await
            .map(|_| ())
            .map_err(TelegramApiError::Api)
    }

    async fn send_inline_keyboard_with_id(
        &self,
        chat_id: i64,
        text: &str,
        keyboard: Vec<Vec<InlineButton>>,
    ) -> Result<i64, Self::Error> {
        let keyboard = keyboard
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|button| match button {
                        InlineButton::Callback { text, data } => {
                            InlineKeyboardButton::callback(text, data)
                        }
                        InlineButton::Url { text, url } => {
                            let url = Url::parse(&url).expect("rendered Telegram URL is valid");
                            InlineKeyboardButton::url(text, url)
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        self.bot()
            .send_message(ChatId(chat_id), text.to_owned())
            .reply_markup(InlineKeyboardMarkup::new(keyboard))
            .await
            .map(|message| i64::from(message.id.0))
            .map_err(TelegramApiError::Api)
    }

    async fn send_force_reply(&self, chat_id: i64, text: &str) -> Result<i64, Self::Error> {
        self.bot()
            .send_message(ChatId(chat_id), text.to_owned())
            .reply_markup(ReplyMarkup::force_reply())
            .await
            .map(|message| i64::from(message.id.0))
            .map_err(TelegramApiError::Api)
    }

    async fn delete_message(&self, chat_id: i64, message_id: i64) -> Result<(), Self::Error> {
        let message_id = i32::try_from(message_id)
            .map_err(|_| TelegramApiError::InvalidMessageId(message_id))?;
        self.bot()
            .delete_message(ChatId(chat_id), MessageId(message_id))
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
        let limit =
            self.cloud_download_limit_bytes.map_or(self.source_download_max_bytes, |cloud_limit| {
                cloud_limit.min(self.source_download_max_bytes)
            });
        if size > limit {
            return Err(TelegramApiError::DownloadLimit { size, limit });
        }
        let temporary =
            destination.with_file_name(format!(".sooqa-download-{}.tmp", Uuid::new_v4()));
        let mut output = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .await
            .map_err(TelegramApiError::Io)?;
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
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(TelegramApiError::DownloadLimit { size: written.saturating_add(1), limit });
        }
        if let Err(error) = download_result {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(TelegramApiError::Download(error));
        }
        if let Err(error) = tokio::io::AsyncWriteExt::flush(&mut output).await {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(TelegramApiError::Io(error));
        }
        drop(output);
        let actual_size = match tokio::fs::metadata(&temporary).await {
            Ok(metadata) => metadata.len(),
            Err(error) => {
                let _ = tokio::fs::remove_file(&temporary).await;
                return Err(TelegramApiError::Io(error));
            }
        };
        if actual_size > limit {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(TelegramApiError::DownloadLimit { size: actual_size, limit });
        }
        if let Err(error) = tokio::fs::rename(&temporary, destination).await {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(TelegramApiError::Io(error));
        }
        Ok(())
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

pub struct TelegramRuntime<S, I, P = ()> {
    api: TeloxideApi,
    service: TelegramService<TeloxideApi, S, I, P>,
    poll_timeout: Duration,
    storage_chat_id: Option<i64>,
}

impl<S, I> TelegramRuntime<S, I, ()>
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
}

impl<S, I, P> TelegramRuntime<S, I, P>
where
    S: UpdateStore,
    I: IngestService,
    P: PublisherService,
{
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

impl<S, I> TelegramRuntime<S, I, ()>
where
    S: UpdateStore,
    I: IngestService,
{
    pub fn with_publisher<P>(self, publisher: P) -> TelegramRuntime<S, I, P>
    where
        P: PublisherService,
    {
        let TelegramRuntime { api, service, poll_timeout, storage_chat_id } = self;
        TelegramRuntime {
            api,
            service: service.with_publisher(publisher),
            poll_timeout,
            storage_chat_id,
        }
    }
}

async fn handle_update_with_retries<A, S, I, P>(
    service: &TelegramService<A, S, I, P>,
    update: Update,
) -> Result<i32, TelegramError>
where
    A: TelegramApi,
    S: UpdateStore,
    I: IngestService,
    P: PublisherService,
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
        io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
        net::{TcpListener, TcpStream},
    };

    use super::*;

    type MockKeyboard = (i64, String, Vec<Vec<InlineButton>>);

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum QueueMutationCall {
        Move { post_id: Uuid, direction: QueueDirection, expected_revision: i64 },
        Slot { post_id: Uuid, slot: OffsetDateTime, expected_revision: i64 },
        Caption { post_id: Uuid, caption: Option<String>, expected_revision: i64 },
        Publish { post_id: Uuid, expected_revision: i64 },
        Cancel { post_id: Uuid, expected_revision: i64 },
    }

    #[derive(Clone, Default)]
    struct MockApi {
        messages: Arc<Mutex<Vec<(i64, String)>>>,
        keyboards: Arc<Mutex<Vec<MockKeyboard>>>,
        force_replies: Arc<Mutex<Vec<(i64, String)>>>,
        deleted_messages: Arc<Mutex<Vec<(i64, i64)>>>,
        next_message_id: Arc<Mutex<i64>>,
        callback_answers: Arc<Mutex<Vec<String>>>,
        downloads: Arc<Mutex<Vec<String>>>,
        fail: Arc<Mutex<bool>>,
        failures_remaining: Arc<Mutex<u8>>,
        queue_send_calls: Arc<Mutex<usize>>,
        queue_failures_remaining: Arc<Mutex<u8>>,
        queue_fail_from_call: Arc<Mutex<Option<usize>>>,
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

        async fn send_inline_keyboard(
            &self,
            chat_id: i64,
            text: &str,
            keyboard: Vec<Vec<InlineButton>>,
        ) -> Result<(), Self::Error> {
            let mut queue_send_calls =
                self.queue_send_calls.lock().expect("mock mutex should not be poisoned");
            *queue_send_calls += 1;
            let call_number = *queue_send_calls;
            let mut queue_failures =
                self.queue_failures_remaining.lock().expect("mock mutex should not be poisoned");
            let fail_from_call =
                *self.queue_fail_from_call.lock().expect("mock mutex should not be poisoned");
            if *queue_failures > 0 || fail_from_call.is_some_and(|start| call_number >= start) {
                if *queue_failures > 0 {
                    *queue_failures -= 1;
                }
                return Err(MockError);
            }
            self.keyboards.lock().expect("mock mutex should not be poisoned").push((
                chat_id,
                text.to_owned(),
                keyboard,
            ));
            Ok(())
        }

        async fn send_inline_keyboard_with_id(
            &self,
            chat_id: i64,
            text: &str,
            keyboard: Vec<Vec<InlineButton>>,
        ) -> Result<i64, Self::Error> {
            self.send_inline_keyboard(chat_id, text, keyboard).await?;
            let mut next_message_id =
                self.next_message_id.lock().expect("mock mutex should not be poisoned");
            let message_id = *next_message_id;
            *next_message_id += 1;
            Ok(message_id)
        }

        async fn send_force_reply(&self, chat_id: i64, text: &str) -> Result<i64, Self::Error> {
            self.force_replies
                .lock()
                .expect("mock mutex should not be poisoned")
                .push((chat_id, text.to_owned()));
            let mut next_message_id =
                self.next_message_id.lock().expect("mock mutex should not be poisoned");
            let message_id = *next_message_id;
            *next_message_id += 1;
            Ok(message_id)
        }

        async fn delete_message(&self, chat_id: i64, message_id: i64) -> Result<(), Self::Error> {
            self.deleted_messages
                .lock()
                .expect("mock mutex should not be poisoned")
                .push((chat_id, message_id));
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

        async fn wait_for_queue_card(&self, _chat_id: i64) -> Result<(), Self::Error> {
            Ok(())
        }

        fn retry_after(_error: &Self::Error) -> Option<Duration> {
            Some(Duration::ZERO)
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

        async fn send_inline_keyboard(
            &self,
            _chat_id: i64,
            _text: &str,
            _keyboard: Vec<Vec<InlineButton>>,
        ) -> Result<(), Self::Error> {
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
    struct MockStore {
        claimed: Arc<Mutex<BTreeSet<i64>>>,
        completed: Arc<Mutex<BTreeSet<i64>>>,
    }

    #[derive(Clone, Default)]
    struct MockIngestService {
        commands: Arc<Mutex<Vec<UrlIngestCommand>>>,
        media_commands: Arc<Mutex<Vec<MediaIngestCommand>>>,
        duplicate_cards: Arc<Mutex<Vec<DuplicatePendingCard>>>,
        duplicate_accepts: Arc<Mutex<Vec<(Uuid, Uuid)>>>,
        force_saves: Arc<Mutex<Vec<Uuid>>>,
        fail: Arc<Mutex<bool>>,
    }

    #[derive(Debug, ThisError)]
    enum MockPublisherError {
        #[error("mock publisher failure")]
        Failure,
        #[error("mock publisher post is missing")]
        Missing,
        #[error("mock publisher conflict")]
        Conflict,
    }

    #[derive(Clone, Default)]
    struct MockPublisherService {
        posts: Arc<Mutex<Vec<QueuePost>>>,
        conflict: Arc<Mutex<bool>>,
        operations: Arc<Mutex<Vec<QueueMutationCall>>>,
    }

    #[async_trait]
    impl PublisherService for MockPublisherService {
        type Error = MockPublisherError;

        async fn queue_count(&self) -> Result<usize, Self::Error> {
            Ok(self.posts.lock().expect("mock mutex should not be poisoned").len())
        }

        async fn list_queue(&self, limit: usize) -> Result<Vec<QueuePost>, Self::Error> {
            Ok(self
                .posts
                .lock()
                .expect("mock mutex should not be poisoned")
                .iter()
                .take(limit)
                .cloned()
                .collect())
        }

        async fn get_queue_post(&self, post_id: Uuid) -> Result<QueuePost, Self::Error> {
            self.posts
                .lock()
                .expect("mock mutex should not be poisoned")
                .iter()
                .find(|post| post.id == post_id)
                .cloned()
                .ok_or(MockPublisherError::Missing)
        }

        async fn move_queue_post(
            &self,
            post_id: Uuid,
            direction: QueueDirection,
            expected_revision: i64,
        ) -> Result<QueuePost, Self::Error> {
            self.operations
                .lock()
                .expect("mock mutex should not be poisoned")
                .push(QueueMutationCall::Move { post_id, direction, expected_revision });
            self.mutate(post_id, expected_revision, |post| post.clone())
        }

        async fn set_queue_post_slot(
            &self,
            post_id: Uuid,
            slot: OffsetDateTime,
            expected_revision: i64,
        ) -> Result<QueuePost, Self::Error> {
            self.operations
                .lock()
                .expect("mock mutex should not be poisoned")
                .push(QueueMutationCall::Slot { post_id, slot, expected_revision });
            self.mutate(post_id, expected_revision, |post| {
                post.cadence_slot_at = Some(slot);
                post.clone()
            })
        }

        async fn update_queue_caption(
            &self,
            post_id: Uuid,
            caption: Option<String>,
            expected_revision: i64,
        ) -> Result<QueuePost, Self::Error> {
            self.operations.lock().expect("mock mutex should not be poisoned").push(
                QueueMutationCall::Caption { post_id, caption: caption.clone(), expected_revision },
            );
            self.mutate(post_id, expected_revision, |post| {
                post.caption = caption;
                post.clone()
            })
        }

        async fn publish_queue_post(
            &self,
            post_id: Uuid,
            expected_revision: i64,
        ) -> Result<QueuePost, Self::Error> {
            self.operations
                .lock()
                .expect("mock mutex should not be poisoned")
                .push(QueueMutationCall::Publish { post_id, expected_revision });
            self.mutate(post_id, expected_revision, |post| post.clone())
        }

        async fn cancel_queue_post(
            &self,
            post_id: Uuid,
            expected_revision: i64,
        ) -> Result<QueuePost, Self::Error> {
            self.operations
                .lock()
                .expect("mock mutex should not be poisoned")
                .push(QueueMutationCall::Cancel { post_id, expected_revision });
            self.mutate(post_id, expected_revision, |post| post.clone())
        }

        fn is_conflict(error: &Self::Error) -> bool {
            matches!(error, MockPublisherError::Conflict | MockPublisherError::Missing)
        }
    }

    impl MockPublisherService {
        fn mutate(
            &self,
            post_id: Uuid,
            expected_revision: i64,
            update: impl FnOnce(&mut QueuePost) -> QueuePost,
        ) -> Result<QueuePost, MockPublisherError> {
            if *self.conflict.lock().expect("mock mutex should not be poisoned") {
                return Err(MockPublisherError::Conflict);
            }
            let mut posts = self.posts.lock().expect("mock mutex should not be poisoned");
            let post = posts
                .iter_mut()
                .find(|post| post.id == post_id)
                .ok_or(MockPublisherError::Failure)?;
            if post.revision != expected_revision {
                return Err(MockPublisherError::Conflict);
            }
            post.revision += 1;
            Ok(update(post))
        }
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

        async fn list_duplicate_pending(
            &self,
            _limit: usize,
        ) -> Result<Vec<DuplicatePendingCard>, Self::Error> {
            Ok(self.duplicate_cards.lock().unwrap().clone())
        }

        async fn accept_duplicate(
            &self,
            request_id: Uuid,
            media_id: Uuid,
        ) -> Result<DuplicateDecisionResult, Self::Error> {
            self.duplicate_accepts.lock().unwrap().push((request_id, media_id));
            Ok(DuplicateDecisionResult {
                request_id,
                status: "completed".to_owned(),
                media_id: Some(media_id),
            })
        }

        async fn force_save(
            &self,
            request_id: Uuid,
        ) -> Result<DuplicateDecisionResult, Self::Error> {
            self.force_saves.lock().unwrap().push(request_id);
            Ok(DuplicateDecisionResult { request_id, status: "queued".to_owned(), media_id: None })
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
            reply_to_message_id: None,
            user_id,
            chat_id: 42,
            is_private: true,
            text: Some(text.to_owned()),
            caption: None,
            media: None,
        }
    }

    fn admin_message(update_id: i64, text: &str) -> IncomingMessage {
        let mut message = message(update_id, Some(123), text);
        message.chat_id = 123;
        message
    }

    fn queue_post(id: u128, revision: i64) -> QueuePost {
        QueuePost {
            id: Uuid::from_u128(id),
            revision,
            state: PostState::Queued,
            scheduled_at: OffsetDateTime::now_utc(),
            cadence_slot_at: Some(OffsetDateTime::now_utc()),
            time_zone: "Europe/Moscow".to_owned(),
            caption: Some("post text".to_owned()),
            media_kind: "video".to_owned(),
            title: Some("title".to_owned()),
            description: Some("description".to_owned()),
            tags: vec!["tag".to_owned()],
            source_url: Some("https://example.test/source".to_owned()),
            storage_chat_id: Some(-100123),
            storage_message_id: Some(456),
        }
    }

    fn callback(update_id: i64, data: CallbackData) -> IncomingCallback {
        IncomingCallback {
            update_id,
            callback_id: format!("callback-{update_id}"),
            user_id: 123,
            chat_id: Some(123),
            is_private: true,
            data: Some(data.encode()),
        }
    }

    #[test]
    fn queue_count_choices_match_the_bounded_scale() {
        for (total, expected) in [
            (1, vec!["All 1"]),
            (3, vec!["1", "2", "All 3"]),
            (10, vec!["1", "2", "5", "All 10"]),
            (37, vec!["1", "2", "5", "10", "20", "All 37"]),
            (125, vec!["1", "2", "5", "10", "20", "50", "100", "All 125"]),
        ] {
            let (_, keyboard) = render_queue_count(total);
            let labels = keyboard
                .iter()
                .flat_map(|row| row.iter())
                .map(|button| match button {
                    InlineButton::Callback { text, .. } | InlineButton::Url { text, .. } => {
                        text.as_str()
                    }
                })
                .collect::<Vec<_>>();
            assert_eq!(labels, expected);
        }
    }

    #[test]
    fn queue_card_actions_match_the_projected_post_state() {
        for state in [PostState::Draft, PostState::Queued, PostState::Failed] {
            let mut post = queue_post(40, 0);
            post.state = state;
            let (_, keyboard) = render_queue_post(1, &post);
            let callbacks = keyboard
                .iter()
                .flat_map(|row| row.iter())
                .filter_map(|button| match button {
                    InlineButton::Callback { data, .. } => CallbackData::parse(data),
                    InlineButton::Url { .. } => None,
                })
                .collect::<Vec<_>>();
            assert!(
                callbacks
                    .iter()
                    .any(|callback| { matches!(callback, CallbackData::QueueEditCaption { .. }) })
            );
            assert!(
                callbacks
                    .iter()
                    .any(|callback| { matches!(callback, CallbackData::QueuePublishNow { .. }) })
            );
            assert!(
                callbacks
                    .iter()
                    .any(|callback| { matches!(callback, CallbackData::QueueCancel { .. }) })
            );
            if state == PostState::Queued {
                assert!(
                    callbacks
                        .iter()
                        .any(|callback| { matches!(callback, CallbackData::QueueSetSlot { .. }) })
                );
                assert!(
                    callbacks
                        .iter()
                        .any(|callback| { matches!(callback, CallbackData::QueueEarlier { .. }) })
                );
                assert!(
                    callbacks
                        .iter()
                        .any(|callback| { matches!(callback, CallbackData::QueueLater { .. }) })
                );
            } else {
                assert!(
                    !callbacks
                        .iter()
                        .any(|callback| { matches!(callback, CallbackData::QueueSetSlot { .. }) })
                );
                assert!(
                    !callbacks
                        .iter()
                        .any(|callback| { matches!(callback, CallbackData::QueueEarlier { .. }) })
                );
                assert!(
                    !callbacks
                        .iter()
                        .any(|callback| { matches!(callback, CallbackData::QueueLater { .. }) })
                );
            }
        }
    }

    #[tokio::test]
    async fn queue_callbacks_delegate_each_direct_mutation() {
        let post_id = Uuid::from_u128(41);
        let expected_revision = 9;
        let cases = vec![
            (
                CallbackData::QueueEarlier { post_id, expected_revision },
                QueueMutationCall::Move {
                    post_id,
                    direction: QueueDirection::Earlier,
                    expected_revision,
                },
            ),
            (
                CallbackData::QueueLater { post_id, expected_revision },
                QueueMutationCall::Move {
                    post_id,
                    direction: QueueDirection::Later,
                    expected_revision,
                },
            ),
            (
                CallbackData::QueueClearCaption { post_id, expected_revision },
                QueueMutationCall::Caption { post_id, caption: None, expected_revision },
            ),
            (
                CallbackData::QueuePublishNow { post_id, expected_revision },
                QueueMutationCall::Publish { post_id, expected_revision },
            ),
            (
                CallbackData::QueueCancel { post_id, expected_revision },
                QueueMutationCall::Cancel { post_id, expected_revision },
            ),
        ];

        for (index, (data, expected_call)) in cases.into_iter().enumerate() {
            let api = MockApi::default();
            let publisher = MockPublisherService::default();
            publisher.posts.lock().unwrap().push(queue_post(41, expected_revision));
            let service = TelegramService::with_ingest(
                api.clone(),
                MockStore::default(),
                [123],
                MockIngestService::default(),
            )
            .with_publisher(publisher.clone());

            assert_eq!(
                service.handle_callback(callback(200 + index as i64, data)).await.unwrap(),
                HandleOutcome::CallbackHandled
            );
            assert_eq!(publisher.operations.lock().unwrap().as_slice(), &[expected_call]);
            assert_eq!(api.callback_answers.lock().unwrap().len(), 1);
        }
    }

    #[tokio::test]
    async fn valid_queue_slot_reply_delegates_the_exact_slot_and_revision() {
        let api = MockApi::default();
        *api.next_message_id.lock().unwrap() = 1;
        let publisher = MockPublisherService::default();
        let post = queue_post(42, 12);
        publisher.posts.lock().unwrap().push(post.clone());
        let service = TelegramService::with_ingest(
            api.clone(),
            MockStore::default(),
            [123],
            MockIngestService::default(),
        )
        .with_publisher(publisher.clone());

        assert_eq!(
            service
                .handle_callback(callback(
                    210,
                    CallbackData::QueueSetSlot {
                        post_id: post.id,
                        expected_revision: post.revision,
                    },
                ))
                .await
                .unwrap(),
            HandleOutcome::CallbackHandled
        );
        let slot_text = "2026-08-12T18:30:00Z";
        let slot = OffsetDateTime::parse(slot_text, &time::format_description::well_known::Rfc3339)
            .unwrap();
        let mut reply = admin_message(211, slot_text);
        reply.reply_to_message_id = Some(1);
        assert_eq!(
            service.handle_message(reply).await.unwrap(),
            HandleOutcome::Responded(Command::Queue)
        );
        assert_eq!(
            publisher.operations.lock().unwrap().as_slice(),
            &[QueueMutationCall::Slot { post_id: post.id, slot, expected_revision: post.revision }]
        );
        assert!(api.deleted_messages.lock().unwrap().contains(&(123, 1)));
    }

    #[tokio::test]
    async fn queue_command_renders_count_and_bounded_post_cards() {
        let api = MockApi::default();
        *api.next_message_id.lock().unwrap() = 1;
        let publisher = MockPublisherService::default();
        publisher.posts.lock().unwrap().extend([queue_post(1, 0), queue_post(2, 4)]);
        let service = TelegramService::with_ingest(
            api.clone(),
            MockStore::default(),
            [123],
            MockIngestService::default(),
        )
        .with_publisher(publisher);

        assert_eq!(
            service.handle_message(admin_message(100, "/queue")).await.unwrap(),
            HandleOutcome::Responded(Command::Queue)
        );
        assert_eq!(api.keyboards.lock().unwrap().len(), 1);
        assert!(api.keyboards.lock().unwrap()[0].2.iter().flatten().any(|button| {
            matches!(button, InlineButton::Callback { data, .. } if CallbackData::parse(data) == Some(CallbackData::QueueCount { count: 2 }))
        }));

        assert_eq!(
            service
                .handle_callback(callback(101, CallbackData::QueueCount { count: 2 }))
                .await
                .unwrap(),
            HandleOutcome::CallbackHandled
        );
        let keyboards = api.keyboards.lock().unwrap();
        assert_eq!(keyboards.len(), 3);
        assert!(keyboards[1].1.contains("Post text: post text"));
        assert!(keyboards[1].2.iter().flatten().any(|button| {
            matches!(button, InlineButton::Url { text, url } if text == "Open media" && url == "https://t.me/c/123/456")
        }));
        assert!(api.deleted_messages.lock().unwrap().contains(&(123, 1)));
    }

    #[tokio::test]
    async fn queue_caption_prompt_is_bound_to_revision_and_clears_old_view() {
        let api = MockApi::default();
        *api.next_message_id.lock().unwrap() = 1;
        let publisher = MockPublisherService::default();
        let post = queue_post(3, 7);
        publisher.posts.lock().unwrap().push(post.clone());
        let service = TelegramService::with_ingest(
            api.clone(),
            MockStore::default(),
            [123],
            MockIngestService::default(),
        )
        .with_publisher(publisher.clone());

        service.handle_message(admin_message(102, "/queue")).await.unwrap();
        service
            .handle_callback(callback(
                103,
                CallbackData::QueueEditCaption {
                    post_id: post.id,
                    expected_revision: post.revision,
                },
            ))
            .await
            .unwrap();
        assert_eq!(
            api.force_replies.lock().unwrap().as_slice(),
            &[(123, "Send new post text, /clear to remove it, or /cancel.".to_owned())]
        );
        let mut reply = admin_message(104, "updated text");
        reply.reply_to_message_id = Some(2);
        service.handle_message(reply).await.unwrap();
        assert_eq!(
            publisher.operations.lock().unwrap().as_slice(),
            &[QueueMutationCall::Caption {
                post_id: post.id,
                caption: Some("updated text".to_owned()),
                expected_revision: post.revision,
            }]
        );
        assert!(api.deleted_messages.lock().unwrap().contains(&(123, 1)));
        assert!(
            api.messages
                .lock()
                .unwrap()
                .iter()
                .any(|(_, text)| { text == "✅ Post text updated." })
        );
    }

    #[tokio::test]
    async fn queue_prompt_only_consumes_a_reply_to_its_prompt_and_commands_keep_priority() {
        let api = MockApi::default();
        *api.next_message_id.lock().unwrap() = 1;
        let publisher = MockPublisherService::default();
        let post = queue_post(30, 7);
        publisher.posts.lock().unwrap().push(post.clone());
        let service = TelegramService::with_ingest(
            api.clone(),
            MockStore::default(),
            [123],
            MockIngestService::default(),
        )
        .with_publisher(publisher.clone());

        service
            .handle_callback(callback(
                130,
                CallbackData::QueueEditCaption {
                    post_id: post.id,
                    expected_revision: post.revision,
                },
            ))
            .await
            .unwrap();

        let mut unrelated = admin_message(131, "ordinary text");
        unrelated.reply_to_message_id = None;
        assert_eq!(
            service.handle_message(unrelated).await.unwrap(),
            HandleOutcome::UnrecognizedIgnored
        );
        assert!(publisher.operations.lock().unwrap().is_empty());

        let mut queue_command = admin_message(134, "/queue");
        queue_command.reply_to_message_id = Some(1);
        assert_eq!(
            service.handle_message(queue_command).await.unwrap(),
            HandleOutcome::Responded(Command::Queue)
        );
        assert!(api.deleted_messages.lock().unwrap().contains(&(123, 1)));
        assert!(publisher.operations.lock().unwrap().is_empty());

        service
            .handle_callback(callback(
                135,
                CallbackData::QueueEditCaption {
                    post_id: post.id,
                    expected_revision: post.revision,
                },
            ))
            .await
            .unwrap();
        assert_eq!(
            service.handle_message(admin_message(136, "/cancel")).await.unwrap(),
            HandleOutcome::Responded(Command::Queue)
        );
        assert_eq!(api.force_replies.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn queue_prompt_expiry_cleans_the_prompt_without_mutating_the_post() {
        let api = MockApi::default();
        *api.next_message_id.lock().unwrap() = 1;
        let publisher = MockPublisherService::default();
        let post = queue_post(31, 2);
        publisher.posts.lock().unwrap().push(post.clone());
        let service = TelegramService::with_ingest(
            api.clone(),
            MockStore::default(),
            [123],
            MockIngestService::default(),
        )
        .with_publisher(publisher.clone());

        service
            .handle_callback(callback(
                135,
                CallbackData::QueueEditCaption {
                    post_id: post.id,
                    expected_revision: post.revision,
                },
            ))
            .await
            .unwrap();
        service.views.lock().unwrap().get_mut(&123).unwrap().prompt_expires_at =
            Some(Instant::now() - Duration::from_secs(1));

        assert_eq!(
            service.handle_message(admin_message(136, "expired text")).await.unwrap(),
            HandleOutcome::UnrecognizedIgnored
        );
        assert!(publisher.operations.lock().unwrap().is_empty());
        assert!(api.deleted_messages.lock().unwrap().contains(&(123, 1)));
    }

    #[tokio::test]
    async fn stale_queue_prompt_is_rejected_before_sending_force_reply() {
        let api = MockApi::default();
        let publisher = MockPublisherService::default();
        let post = queue_post(32, 8);
        publisher.posts.lock().unwrap().push(post.clone());
        let service = TelegramService::with_ingest(
            api.clone(),
            MockStore::default(),
            [123],
            MockIngestService::default(),
        )
        .with_publisher(publisher);

        assert_eq!(
            service
                .handle_callback(callback(
                    137,
                    CallbackData::QueueEditCaption {
                        post_id: post.id,
                        expected_revision: post.revision - 1,
                    },
                ))
                .await
                .unwrap(),
            HandleOutcome::CallbackHandled
        );
        assert!(api.force_replies.lock().unwrap().is_empty());
        assert!(
            api.messages
                .lock()
                .unwrap()
                .iter()
                .any(|(_, text)| { text == "Queue changed; run /queue again." })
        );
    }

    #[tokio::test]
    async fn stale_queue_slot_prompt_is_rejected_before_mutating_the_post() {
        let api = MockApi::default();
        let publisher = MockPublisherService::default();
        let post = queue_post(35, 8);
        publisher.posts.lock().unwrap().push(post.clone());
        let service = TelegramService::with_ingest(
            api.clone(),
            MockStore::default(),
            [123],
            MockIngestService::default(),
        )
        .with_publisher(publisher.clone());

        assert_eq!(
            service
                .handle_callback(callback(
                    139,
                    CallbackData::QueueSetSlot {
                        post_id: post.id,
                        expected_revision: post.revision - 1,
                    },
                ))
                .await
                .unwrap(),
            HandleOutcome::CallbackHandled
        );
        assert!(api.force_replies.lock().unwrap().is_empty());
        assert!(publisher.operations.lock().unwrap().is_empty());
        assert!(
            api.messages
                .lock()
                .unwrap()
                .iter()
                .any(|(_, text)| { text == "Queue changed; run /queue again." })
        );
    }

    #[tokio::test]
    async fn missing_queue_prompt_post_returns_stable_conflict_response() {
        let api = MockApi::default();
        let service = TelegramService::with_ingest(
            api.clone(),
            MockStore::default(),
            [123],
            MockIngestService::default(),
        )
        .with_publisher(MockPublisherService::default());

        assert_eq!(
            service
                .handle_callback(callback(
                    140,
                    CallbackData::QueueEditCaption {
                        post_id: Uuid::from_u128(36),
                        expected_revision: 0,
                    },
                ))
                .await
                .unwrap(),
            HandleOutcome::CallbackHandled
        );
        assert!(api.force_replies.lock().unwrap().is_empty());
        assert!(
            api.messages
                .lock()
                .unwrap()
                .iter()
                .any(|(_, text)| { text == "Queue changed; run /queue again." })
        );
    }

    #[tokio::test]
    async fn queue_cards_retry_retry_after_without_sleep_and_cleanup_partial_view() {
        let api = MockApi::default();
        *api.next_message_id.lock().unwrap() = 1;
        let publisher = MockPublisherService::default();
        let first = queue_post(33, 0);
        let second = queue_post(34, 0);
        publisher.posts.lock().unwrap().extend([first, second]);
        let service = TelegramService::with_ingest(
            api.clone(),
            MockStore::default(),
            [123],
            MockIngestService::default(),
        )
        .with_publisher(publisher);
        *api.queue_failures_remaining.lock().unwrap() = 1;

        let result = tokio::time::timeout(
            Duration::from_millis(250),
            service.handle_callback(callback(138, CallbackData::QueueCount { count: 2 })),
        )
        .await
        .expect("mock retry-after must not sleep")
        .unwrap();
        assert_eq!(result, HandleOutcome::CallbackHandled);
        assert_eq!(api.keyboards.lock().unwrap().len(), 2);
        assert_eq!(*api.queue_send_calls.lock().unwrap(), 3);

        *api.queue_fail_from_call.lock().unwrap() = Some(5);
        assert_eq!(
            service
                .handle_callback(callback(139, CallbackData::QueueCount { count: 2 }))
                .await
                .unwrap(),
            HandleOutcome::CallbackHandled
        );
        assert!(
            api.messages
                .lock()
                .unwrap()
                .iter()
                .any(|(_, text)| { text == "⚠️ Queue view could not be rendered completely." })
        );
        assert!(api.deleted_messages.lock().unwrap().contains(&(123, 3)));
        assert_eq!(
            service
                .handle_callback(callback(139, CallbackData::QueueCount { count: 2 }))
                .await
                .unwrap(),
            HandleOutcome::DuplicateIgnored
        );
    }

    #[tokio::test]
    async fn stale_queue_callback_returns_stable_conflict_response() {
        let api = MockApi::default();
        *api.next_message_id.lock().unwrap() = 1;
        let publisher = MockPublisherService::default();
        let post = queue_post(4, 1);
        publisher.posts.lock().unwrap().push(post.clone());
        *publisher.conflict.lock().unwrap() = true;
        let service = TelegramService::with_ingest(
            api.clone(),
            MockStore::default(),
            [123],
            MockIngestService::default(),
        )
        .with_publisher(publisher);

        service.handle_message(admin_message(105, "/queue")).await.unwrap();
        service
            .handle_callback(callback(
                106,
                CallbackData::QueueClearCaption {
                    post_id: post.id,
                    expected_revision: post.revision,
                },
            ))
            .await
            .unwrap();
        assert!(
            api.messages
                .lock()
                .unwrap()
                .iter()
                .any(|(_, text)| { text == "Queue changed; run /queue again." })
        );
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
    async fn duplicate_command_renders_bounded_candidates_and_actions() {
        let api = MockApi::default();
        let ingest = MockIngestService::default();
        let request_id = Uuid::from_u128(10);
        let ready_media_id = Uuid::from_u128(11);
        let pending_media_id = Uuid::from_u128(12);
        ingest.duplicate_cards.lock().unwrap().push(DuplicatePendingCard {
            request_id,
            source_url: "https://example.test/incoming".to_owned(),
            candidates: vec![
                DuplicateCandidateCard {
                    media_id: ready_media_id,
                    classification: "strong_duplicate".to_owned(),
                    score_bps: 9_500,
                    storage: DuplicateCandidateStorage::Ready {
                        open_url: Some("https://t.me/c/123/456".to_owned()),
                    },
                },
                DuplicateCandidateCard {
                    media_id: pending_media_id,
                    classification: "partial_match".to_owned(),
                    score_bps: 7_100,
                    storage: DuplicateCandidateStorage::PendingStorage,
                },
            ],
        });
        let service =
            TelegramService::with_ingest(api.clone(), MockStore::default(), [123], ingest);

        assert_eq!(
            service.handle_message(message(20, Some(123), "/duplicates")).await.unwrap(),
            HandleOutcome::Responded(Command::Duplicates)
        );
        let keyboards = api.keyboards.lock().unwrap();
        assert_eq!(keyboards.len(), 1);
        assert!(keyboards[0].1.contains("Candidate 1: strong_duplicate (score 9500 bps) — ready"));
        assert!(keyboards[0].1.contains("Candidate 2: partial_match (score 7100 bps) — storing"));
        assert!(keyboards[0].2.iter().flatten().any(|button| {
            matches!(button, InlineButton::Url { text, url } if text == "Open media" && url == "https://t.me/c/123/456")
        }));
        assert_eq!(
            keyboards[0]
                .2
                .iter()
                .flatten()
                .filter(|button| {
                    matches!(button, InlineButton::Callback { text, .. } if text == "Use this")
                })
                .count(),
            2
        );
        assert!(keyboards[0].2.iter().flatten().any(|button| {
            matches!(button, InlineButton::Callback { text, .. } if text == "Save anyway")
        }));
    }

    #[tokio::test]
    async fn duplicate_callback_is_reauthorized_and_acknowledged_before_decision() {
        let api = MockApi::default();
        let ingest = MockIngestService::default();
        let request_id = Uuid::from_u128(30);
        let media_id = Uuid::from_u128(31);
        let service =
            TelegramService::with_ingest(api.clone(), MockStore::default(), [123], ingest.clone());

        assert_eq!(
            service
                .handle_callback(IncomingCallback {
                    update_id: 21,
                    callback_id: "callback-1".to_owned(),
                    user_id: 123,
                    chat_id: Some(123),
                    is_private: true,
                    data: Some(CallbackData::DuplicateUse { request_id, media_id }.encode()),
                })
                .await
                .unwrap(),
            HandleOutcome::CallbackHandled
        );
        assert_eq!(api.callback_answers.lock().unwrap().as_slice(), &["callback-1"]);
        assert_eq!(ingest.duplicate_accepts.lock().unwrap().as_slice(), &[(request_id, media_id)]);
        assert!(api.messages.lock().unwrap()[0].1.contains("Duplicate accepted"));

        assert_eq!(
            service
                .handle_callback(IncomingCallback {
                    update_id: 22,
                    callback_id: "callback-2".to_owned(),
                    user_id: 456,
                    chat_id: Some(456),
                    is_private: true,
                    data: Some(CallbackData::DuplicateForceSave { request_id }.encode()),
                })
                .await
                .unwrap(),
            HandleOutcome::CallbackHandled
        );
        assert_eq!(api.callback_answers.lock().unwrap().as_slice(), &["callback-1", "callback-2"]);
        assert!(ingest.force_saves.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn unauthorized_malformed_callback_is_acknowledged_without_dispatch() {
        let api = MockApi::default();
        let ingest = MockIngestService::default();
        let service =
            TelegramService::with_ingest(api.clone(), MockStore::default(), [123], ingest.clone());

        assert_eq!(
            service
                .handle_callback(IncomingCallback {
                    update_id: 23,
                    callback_id: "malformed-unauthorized".to_owned(),
                    user_id: 456,
                    chat_id: Some(456),
                    is_private: true,
                    data: Some("not-a-callback".to_owned()),
                })
                .await
                .unwrap(),
            HandleOutcome::CallbackHandled
        );
        assert_eq!(api.callback_answers.lock().unwrap().as_slice(), &["malformed-unauthorized"]);
        assert!(ingest.duplicate_accepts.lock().unwrap().is_empty());
        assert!(ingest.force_saves.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn authorized_media_is_queued_without_downloading_bytes() {
        let api = MockApi::default();
        let ingest = MockIngestService::default();
        let service =
            TelegramService::with_ingest(api.clone(), MockStore::default(), [123], ingest.clone());
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
        let service =
            TelegramService::with_ingest(api, MockStore::default(), [123], ingest.clone());
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
        let service =
            TelegramService::with_ingest(api.clone(), MockStore::default(), [123], ingest.clone())
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
        let service =
            TelegramService::with_ingest(api.clone(), MockStore::default(), [123], ingest.clone());
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
        let service =
            TelegramService::with_ingest(api.clone(), MockStore::default(), [123], ingest.clone());
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
    async fn replayed_media_update_reuses_the_queued_ingest_without_downloading() {
        let api = MockApi::default();
        let ingest = MockIngestService::default();
        let service =
            TelegramService::with_ingest(api.clone(), MockStore::default(), [123], ingest.clone());
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
        assert_eq!(
            service
                .handle_message(IncomingMessage {
                    update_id: 13,
                    message_id: 101,
                    reply_to_message_id: None,
                    user_id: Some(123),
                    chat_id: 42,
                    is_private: true,
                    text: None,
                    caption: None,
                    media: None,
                })
                .await
                .unwrap(),
            HandleOutcome::DuplicateIgnored
        );
        assert_eq!(ingest.media_commands.lock().unwrap().len(), 1);
        assert!(api.downloads.lock().unwrap().is_empty());
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
        let (stream, _) = listener.accept().await.expect("fake API should accept getFile");
        let mut reader = BufReader::new(stream);
        let request = read_http_request(&mut reader).await.expect("getFile should be readable");
        let response = format!(
            r#"{{"ok":true,"result":{{"file_id":"file-id","file_unique_id":"file-unique-id","file_size":{},"file_path":"/var/lib/telegram-bot-api/files/payload.bin"}}}}"#,
            file_size
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
            r#"{{"ok":true,"result":{{"file_id":"file-id","file_unique_id":"file-unique-id","file_size":{},"file_path":"/var/lib/telegram-bot-api/files/payload.bin"}}}}"#,
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
    async fn local_bot_api_downloads_absolute_file_path_above_cloud_ceiling() {
        const PAYLOAD: &[u8] = b"local transfer payload";
        const SOURCE_LIMIT: u64 = 64;
        const REDUCED_CLOUD_CEILING: u64 = 4;
        assert!(PAYLOAD.len() as u64 > REDUCED_CLOUD_CEILING);

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("fake API should bind");
        let address = listener.local_addr().expect("fake API address should be available");
        let server = tokio::spawn(serve_download(listener, PAYLOAD.to_vec(), Duration::ZERO));
        let directory = std::env::temp_dir().join(format!("sooqa-telegram-{}", Uuid::new_v4()));
        tokio::fs::create_dir(&directory).await.expect("test directory should be created");
        let destination = directory.join("download.webm");
        let api =
            TeloxideApi::new("test-token", &format!("http://{address}"), Duration::from_secs(1))
                .expect("fake API URL should be accepted")
                .with_source_download_max_bytes(SOURCE_LIMIT);

        api.download_file("file-id", &destination)
            .await
            .expect("local Bot API download should succeed");
        assert_eq!(
            tokio::fs::read(&destination).await.expect("download should be readable"),
            PAYLOAD
        );
        let mut entries =
            tokio::fs::read_dir(&directory).await.expect("directory should be readable");
        while let Some(entry) =
            entries.next_entry().await.expect("directory entry should be readable")
        {
            let name = entry.file_name();
            assert!(!name.to_string_lossy().starts_with(".sooqa-download-"));
        }
        let (get_file_target, download_target) = server.await.expect("fake API task should finish");
        assert!(get_file_target.contains("/bottest-token/GetFile"));
        assert!(download_target.contains("/file/bottest-token/"));
        tokio::fs::remove_dir_all(directory).await.expect("test directory should be removed");
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
        assert_eq!(parse_command("/duplicates"), Some(Command::Duplicates));
        assert_eq!(parse_command("/queue"), Some(Command::Queue));
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

    #[test]
    fn callback_data_round_trips_and_rejects_unknown_shapes() {
        let request_id = Uuid::from_u128(2);
        let legacy_ingest_status = "v1:ingest_status:00000000-0000-0000-0000-000000000002";
        assert_eq!(CallbackData::parse(legacy_ingest_status), None);
        assert_eq!(CallbackData::parse("v2:duplicate_force_save:AAAAAAAAAAAAAAAAAAAAAA"), None);
        assert_eq!(CallbackData::parse("v1:ingest_status:legacy:extra"), None);

        let media_id = Uuid::from_u128(3);
        let duplicate = CallbackData::DuplicateUse { request_id, media_id };
        let encoded_duplicate = duplicate.encode();
        assert!(encoded_duplicate.len() <= 64);
        assert_eq!(CallbackData::parse(&encoded_duplicate), Some(duplicate));
        let force_save = CallbackData::DuplicateForceSave { request_id };
        assert_eq!(CallbackData::parse(&force_save.encode()), Some(force_save));

        let queue = CallbackData::QueuePublishNow { post_id: request_id, expected_revision: 17 };
        assert_eq!(CallbackData::parse(&queue.encode()), Some(queue));
        assert!(queue.encode().len() <= 64);
        assert_eq!(
            CallbackData::parse("v1:q:n:0"),
            None,
            "zero-count queue callbacks must be rejected"
        );
    }

    #[tokio::test]
    async fn memory_update_store_fences_duplicate_delivery() {
        let store = MemoryUpdateStore::default();
        let claim = match store.claim_update(42).await.expect("claim should succeed") {
            UpdateClaimResult::Claimed(claim) => claim,
            other => panic!("unexpected claim result: {other:?}"),
        };
        assert!(matches!(
            store.claim_update(42).await.expect("duplicate claim should succeed"),
            UpdateClaimResult::InProgress
        ));
        store.complete_update(claim).await.expect("completion should succeed");
        assert!(matches!(
            store.claim_update(42).await.expect("completed claim should succeed"),
            UpdateClaimResult::Completed
        ));
    }
}
