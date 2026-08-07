//! Telegram adapter and editorial interaction boundaries for sooqa.

use std::{
    collections::{BTreeSet, HashMap},
    error::Error,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use teloxide::{
    Bot,
    dispatching::Dispatcher,
    dptree,
    error_handlers::LoggingErrorHandler,
    prelude::{Request, Requester},
    types::{Update, UpdateKind},
    update_listeners::Polling,
};
use thiserror::Error as ThisError;
use tracing::warn;
use url::Url;
use uuid::Uuid;

pub const START_RESPONSE: &str = "sooqa is ready. You are authorized.";
pub const HELP_RESPONSE: &str = "Available commands:\n/start — show authorization\n/help — show this help\n/status — show service status";
pub const STATUS_RESPONSE: &str = "sooqa is online.";
pub const UNAUTHORIZED_RESPONSE: &str = "This bot is restricted to its configured administrator.";
const RESPONSE_RATE_LIMIT: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IncomingMessage {
    pub update_id: i64,
    pub user_id: Option<i64>,
    pub chat_id: i64,
    pub is_private: bool,
    pub text: Option<String>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum HandleOutcome {
    DuplicateIgnored,
    NonMessageIgnored,
    NonPrivateIgnored,
    RateLimited,
    Unauthorized,
    UnrecognizedIgnored,
    Responded(Command),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Command {
    Start,
    Help,
    Status,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct UpdateClaim {
    pub update_id: i64,
    pub claim_token: Uuid,
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
}

#[async_trait]
pub trait TelegramApi: Clone + Send + Sync + 'static {
    type Error: Error + Send + Sync + 'static;

    async fn send_text(&self, chat_id: i64, text: &str) -> Result<(), Self::Error>;
}

#[async_trait]
pub trait UpdateStore: Clone + Send + Sync + 'static {
    type Error: Error + Send + Sync + 'static;

    async fn claim_update(&self, update_id: i64) -> Result<Option<UpdateClaim>, Self::Error>;

    async fn complete_update(&self, claim: UpdateClaim) -> Result<(), Self::Error>;

    async fn release_update(&self, claim: UpdateClaim) -> Result<(), Self::Error>;
}

#[derive(Clone)]
pub struct TelegramService<A, S> {
    api: A,
    update_store: S,
    admin_user_ids: Arc<BTreeSet<i64>>,
    response_limiter: Arc<Mutex<HashMap<RateLimitKey, Instant>>>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
struct RateLimitKey {
    user_id: Option<i64>,
    chat_id: i64,
}

impl<A, S> TelegramService<A, S>
where
    A: TelegramApi,
    S: UpdateStore,
{
    pub fn new(api: A, update_store: S, admin_user_ids: impl IntoIterator<Item = i64>) -> Self {
        Self {
            api,
            update_store,
            admin_user_ids: Arc::new(admin_user_ids.into_iter().collect()),
            response_limiter: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn handle_message(
        &self,
        message: IncomingMessage,
    ) -> Result<HandleOutcome, TelegramError> {
        let Some(claim) = self.claim(message.update_id).await? else {
            return Ok(HandleOutcome::DuplicateIgnored);
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
        let Some(command) = message.text.as_deref().and_then(parse_command) else {
            self.complete(claim).await?;
            return Ok(HandleOutcome::UnrecognizedIgnored);
        };
        if !self.allow_response(message.user_id, message.chat_id) {
            self.complete(claim).await?;
            return Ok(HandleOutcome::RateLimited);
        }
        let response = match command {
            Command::Start => START_RESPONSE,
            Command::Help => HELP_RESPONSE,
            Command::Status => STATUS_RESPONSE,
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

    pub async fn handle_update(&self, update: Update) -> Result<HandleOutcome, TelegramError> {
        let update_id = i64::from(update.id.0);
        let UpdateKind::Message(message) = update.kind else {
            let Some(claim) = self.claim(update_id).await? else {
                return Ok(HandleOutcome::DuplicateIgnored);
            };
            self.complete(claim).await?;
            return Ok(HandleOutcome::NonMessageIgnored);
        };
        self.handle_message(IncomingMessage {
            update_id,
            user_id: message.from.as_ref().and_then(|user| i64::try_from(user.id.0).ok()),
            chat_id: message.chat.id.0,
            is_private: message.chat.is_private(),
            text: message.text().map(str::to_owned),
        })
        .await
    }

    async fn claim(&self, update_id: i64) -> Result<Option<UpdateClaim>, TelegramError> {
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

fn parse_command(text: &str) -> Option<Command> {
    let command = text.split_whitespace().next()?.strip_prefix('/')?;
    let command = command.split('@').next().unwrap_or(command);
    match command.to_ascii_lowercase().as_str() {
        "start" => Some(Command::Start),
        "help" => Some(Command::Help),
        "status" => Some(Command::Status),
        _ => None,
    }
}

#[derive(Clone)]
pub struct TeloxideApi {
    bot: Bot,
}

impl TeloxideApi {
    pub fn new(
        token: impl Into<String>,
        api_base_url: &str,
        poll_timeout: Duration,
    ) -> Result<Self, TelegramError> {
        if poll_timeout.is_zero() {
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
        Ok(Self { bot: Bot::with_client(token, client).set_api_url(api_base_url) })
    }

    fn bot(&self) -> Bot {
        self.bot.clone()
    }
}

#[async_trait]
impl TelegramApi for TeloxideApi {
    type Error = teloxide::RequestError;

    async fn send_text(&self, chat_id: i64, text: &str) -> Result<(), Self::Error> {
        self.bot.send_message(teloxide::types::ChatId(chat_id), text.to_owned()).await.map(|_| ())
    }
}

pub struct TelegramRuntime<S> {
    api: TeloxideApi,
    service: TelegramService<TeloxideApi, S>,
    poll_timeout: Duration,
}

impl<S> TelegramRuntime<S>
where
    S: UpdateStore,
{
    pub fn new(
        token: impl Into<String>,
        api_base_url: &str,
        poll_timeout: Duration,
        update_store: S,
        admin_user_ids: impl IntoIterator<Item = i64>,
    ) -> Result<Self, TelegramError> {
        let api = TeloxideApi::new(token, api_base_url, poll_timeout)?;
        let service = TelegramService::new(api.clone(), update_store, admin_user_ids);
        Ok(Self { api, service, poll_timeout })
    }

    pub async fn run(self) -> Result<(), TelegramError> {
        self.api
            .bot()
            .delete_webhook()
            .send()
            .await
            .map_err(|error| TelegramError::Api(Box::new(error)))?;
        let listener = Polling::builder(self.api.bot()).timeout(self.poll_timeout).build();
        let service = self.service;
        let handler = dptree::entry().endpoint(move |update: Update| {
            let service = service.clone();
            async move { service.handle_update(update).await.map(|_| ()) }
        });
        Dispatcher::builder(self.api.bot(), handler)
            .error_handler(LoggingErrorHandler::with_custom_text("Telegram dispatcher error"))
            .enable_ctrlc_handler()
            .build()
            .try_dispatch_with_listener(
                listener,
                LoggingErrorHandler::with_custom_text("Telegram polling error"),
            )
            .await
            .map_err(|error| TelegramError::Api(Box::new(error)))
    }
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
    }

    #[derive(Debug, ThisError)]
    #[error("mock failure")]
    struct MockError;

    #[async_trait]
    impl TelegramApi for MockApi {
        type Error = MockError;

        async fn send_text(&self, chat_id: i64, text: &str) -> Result<(), Self::Error> {
            if *self.fail.lock().expect("mock mutex should not be poisoned") {
                return Err(MockError);
            }
            self.messages
                .lock()
                .expect("mock mutex should not be poisoned")
                .push((chat_id, text.to_owned()));
            Ok(())
        }
    }

    #[derive(Clone, Default)]
    struct MockStore {
        claimed: Arc<Mutex<BTreeSet<i64>>>,
        completed: Arc<Mutex<BTreeSet<i64>>>,
    }

    #[async_trait]
    impl UpdateStore for MockStore {
        type Error = MockError;

        async fn claim_update(&self, update_id: i64) -> Result<Option<UpdateClaim>, Self::Error> {
            if self
                .completed
                .lock()
                .expect("mock mutex should not be poisoned")
                .contains(&update_id)
                || self
                    .claimed
                    .lock()
                    .expect("mock mutex should not be poisoned")
                    .contains(&update_id)
            {
                return Ok(None);
            }
            self.claimed.lock().expect("mock mutex should not be poisoned").insert(update_id);
            Ok(Some(UpdateClaim { update_id, claim_token: Uuid::new_v4() }))
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
            user_id,
            chat_id: 42,
            is_private: true,
            text: Some(text.to_owned()),
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
    fn command_parser_accepts_bot_suffix_and_rejects_other_text() {
        assert_eq!(parse_command("/start@sooqa_bot"), Some(Command::Start));
        assert_eq!(parse_command("/HELP extra"), Some(Command::Help));
        assert_eq!(parse_command("hello /status"), None);
        assert_eq!(parse_command("/add"), None);
    }
}
