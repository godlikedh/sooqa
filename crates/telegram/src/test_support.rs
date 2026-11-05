use std::{
    collections::BTreeSet,
    path::Path,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use teloxide::types::{Update, UpdateKind};
use thiserror::Error as ThisError;
use uuid::Uuid;

use super::{
    IncomingMessage, IngestAccepted, IngestService, MediaIngestCommand, TelegramApi,
    UrlIngestCommand,
};

#[derive(Clone, Default)]
pub(crate) struct MockApi {
    pub(crate) messages: Arc<Mutex<Vec<(i64, String)>>>,
    pub(crate) callback_answers: Arc<Mutex<Vec<String>>>,
    pub(crate) downloads: Arc<Mutex<Vec<String>>>,
    pub(crate) fail: Arc<Mutex<bool>>,
    pub(crate) failures_remaining: Arc<Mutex<u8>>,
}

#[derive(Debug, ThisError)]
#[error("mock failure")]
pub(crate) struct MockError;

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

    async fn download_file(&self, file_id: &str, destination: &Path) -> Result<(), Self::Error> {
        self.downloads.lock().expect("mock mutex should not be poisoned").push(file_id.to_owned());
        tokio::fs::write(destination, b"mock media").await.expect("mock media should be written");
        Ok(())
    }

    fn is_retryable_error(_error: &Self::Error) -> bool {
        false
    }
}

#[derive(Clone, Default)]
pub(crate) struct BlockingDownloadApi;

#[async_trait]
impl TelegramApi for BlockingDownloadApi {
    type Error = MockError;

    async fn send_text(&self, _chat_id: i64, _text: &str) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn answer_callback_query(&self, _callback_id: &str) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn download_file(&self, _file_id: &str, _destination: &Path) -> Result<(), Self::Error> {
        std::future::pending().await
    }

    fn is_retryable_error(_error: &Self::Error) -> bool {
        true
    }
}

#[derive(Clone, Default)]
pub(crate) struct MockIngestService {
    pub(crate) commands: Arc<Mutex<Vec<UrlIngestCommand>>>,
    pub(crate) media_commands: Arc<Mutex<Vec<MediaIngestCommand>>>,
    pub(crate) accepted_keys: Arc<Mutex<BTreeSet<String>>>,
    pub(crate) fail: Arc<Mutex<bool>>,
    pub(crate) failures_remaining: Arc<Mutex<usize>>,
    pub(crate) attempts: Arc<Mutex<usize>>,
}

impl MockIngestService {
    fn should_fail(&self) -> bool {
        *self.attempts.lock().expect("mock mutex should not be poisoned") += 1;
        let mut failures_remaining =
            self.failures_remaining.lock().expect("mock mutex should not be poisoned");
        if *failures_remaining > 0 {
            *failures_remaining -= 1;
            return true;
        }
        *self.fail.lock().expect("mock mutex should not be poisoned")
    }
}

#[async_trait]
impl IngestService for MockIngestService {
    type Error = MockError;

    async fn create_url(&self, command: UrlIngestCommand) -> Result<IngestAccepted, Self::Error> {
        if self.should_fail() {
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
        if self.should_fail() {
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

pub(crate) fn message(update_id: i64, user_id: Option<i64>, text: &str) -> IncomingMessage {
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

pub(crate) fn url_update(update_id: u32) -> Update {
    let message: teloxide::types::Message = serde_json::from_value(serde_json::json!({
        "message_id": update_id,
        "from": {"id": 123, "is_bot": false, "first_name": "Admin"},
        "chat": {"id": 42, "type": "private", "first_name": "Admin"},
        "date": 0,
        "text": "https://example.test/video.webm"
    }))
    .expect("Telegram URL message fixture should deserialize");
    Update { id: teloxide::types::UpdateId(update_id), kind: UpdateKind::Message(message) }
}
