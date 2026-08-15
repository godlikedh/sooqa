use std::{path::PathBuf, time::Duration};

use async_trait::async_trait;
use sooqa_library::{
    MediaKind, StorageCaptionMetadata, StorageReceipt, StorageUploadReservation,
    StorageUploadReservationRequest, StorageUploadStore,
};
use sooqa_media::sha256_file;
use teloxide::{
    payloads::{SendAnimationSetters, SendAudioSetters, SendPhotoSetters, SendVideoSetters},
    prelude::{Request, Requester},
    types::{ChatFullInfoKind, ChatFullInfoPublicKind, ChatId, ChatMemberKind, InputFile},
};
use thiserror::Error;
use uuid::Uuid;

use crate::TeloxideApi;

pub const TELEGRAM_STORAGE_PROVIDER: &str = "telegram";

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StorageUploadInput {
    pub media_id: Uuid,
    pub generation: i32,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StorageUploadRequest {
    pub storage_chat_id: i64,
    pub media_kind: MediaKind,
    pub local_work_path: PathBuf,
    pub caption: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StorageUploadResult {
    pub storage_message_id: i64,
    pub telegram_file_id: String,
    pub telegram_file_unique_id: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum StorageUploadOutcome {
    Uploaded(StorageReceipt),
    Reused(StorageReceipt),
}

#[async_trait]
pub trait TelegramStorageApi: Clone + Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn upload_media(
        &self,
        request: StorageUploadRequest,
    ) -> Result<StorageUploadResult, Self::Error>;

    async fn verify_storage_chat(&self, chat_id: i64) -> Result<(), Self::Error>;

    fn is_ambiguous_error(error: &Self::Error) -> bool;

    fn max_upload_bytes(&self) -> Option<u64> {
        None
    }
}

#[derive(Debug, Error)]
pub enum StorageUploadApiError {
    #[error("Telegram storage API request failed: {0}")]
    Api(#[source] teloxide::RequestError),
    #[error("Telegram returned no {media_kind:?} file reference")]
    MissingFileReference { media_kind: MediaKind },
    #[error("Telegram storage chat must be a private channel")]
    StorageChatNotPrivateChannel,
    #[error("Telegram bot is not an administrator of the storage channel")]
    StorageBotNotAdministrator,
    #[error("Telegram bot administrator cannot post messages to the storage channel")]
    StorageBotCannotPost,
}

#[derive(Debug, Error)]
pub enum StorageUploadError {
    #[error("Telegram storage chat ID must be negative")]
    InvalidStorageChatId,
    #[error("media {0} was not found")]
    MediaMissing(Uuid),
    #[error("media {0} has no recorded SHA-256")]
    MediaMissingSha256(Uuid),
    #[error("media {0} workspace was reclaimed; reconstruction is required before storage upload")]
    WorkspaceReclaimed(Uuid),
    #[error("media SHA-256 must contain 32 bytes, got {actual}")]
    InvalidSha256Length { actual: usize },
    #[error("media file is not available at {path}")]
    LocalFileUnavailable { path: PathBuf },
    #[error("media file {path} is outside the configured work root {root}")]
    LocalFileOutsideWorkRoot { path: PathBuf, root: PathBuf },
    #[error(
        "canonical media file is {size} bytes, above the configured storage ceiling of {limit} bytes"
    )]
    StorageOutputLimitExceeded { size: u64, limit: u64 },
    #[error(
        "canonical media file is {size} bytes, above the Telegram upload limit of {limit} bytes"
    )]
    TelegramUploadLimitExceeded { size: u64, limit: u64 },
    #[error("could not hash canonical asset: {0}")]
    Hash(#[source] sooqa_media::HashError),
    #[error("media hash mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: String, actual: String },
    #[error("storage upload is already in progress until {retry_at:?}")]
    InProgress { retry_at: Option<time::OffsetDateTime> },
    #[error("storage upload for media {0} requires explicit reconciliation")]
    ReconciliationRequired(Uuid),
    #[error(
        "storage upload generation {requested_generation} is stale; current generation is {current_generation}"
    )]
    StaleGeneration { requested_generation: i32, current_generation: i32 },
    #[error("storage persistence failed: {0}")]
    Persistence(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("Telegram storage API failed: {0}")]
    Api(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("Telegram storage API result is ambiguous; storage state requires reconciliation: {0}")]
    AmbiguousApi(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("storage state became ambiguous after a successful or uncertain Telegram effect: {0}")]
    AmbiguousPersistence(#[source] Box<dyn std::error::Error + Send + Sync>),
}

impl StorageUploadError {
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::InProgress { .. } | Self::Persistence(_) | Self::Api(_))
    }
}

#[derive(Clone)]
pub struct StorageUploadProvider<A, S> {
    api: A,
    store: S,
    storage_chat_id: i64,
    max_storage_bytes: Option<u64>,
    work_root: Option<PathBuf>,
}

const STORAGE_UPLOAD_LEASE_DURATION: Duration = Duration::from_secs(600);
const STORAGE_UPLOAD_RENEW_INTERVAL: Duration = Duration::from_secs(60);

impl<A, S> StorageUploadProvider<A, S>
where
    A: TelegramStorageApi,
    S: StorageUploadStore,
{
    pub fn new(api: A, store: S, storage_chat_id: i64) -> Result<Self, StorageUploadError> {
        if storage_chat_id >= 0 {
            return Err(StorageUploadError::InvalidStorageChatId);
        }
        Ok(Self { api, store, storage_chat_id, max_storage_bytes: None, work_root: None })
    }

    pub fn with_max_storage_bytes(mut self, max_bytes: u64) -> Self {
        self.max_storage_bytes = Some(max_bytes);
        self
    }

    pub fn with_work_root(mut self, work_root: impl Into<PathBuf>) -> Self {
        self.work_root = Some(work_root.into());
        self
    }

    pub async fn verify_storage_chat(&self) -> Result<(), StorageUploadError> {
        self.api
            .verify_storage_chat(self.storage_chat_id)
            .await
            .map_err(|error| StorageUploadError::Api(Box::new(error)))
    }

    pub async fn upload(
        &self,
        input: StorageUploadInput,
    ) -> Result<StorageUploadOutcome, StorageUploadError> {
        let media = self
            .store
            .find_media(input.media_id)
            .await
            .map_err(|error| StorageUploadError::Persistence(Box::new(error)))?
            .ok_or(StorageUploadError::MediaMissing(input.media_id))?;

        if let Some(receipt) = self
            .store
            .find_storage_receipt(input.media_id)
            .await
            .map_err(|error| StorageUploadError::Persistence(Box::new(error)))?
        {
            return Ok(StorageUploadOutcome::Reused(receipt));
        }

        let caption_metadata = self
            .store
            .find_storage_caption_metadata(input.media_id)
            .await
            .map_err(|error| StorageUploadError::Persistence(Box::new(error)))?;

        let expected_sha256_bytes =
            media.sha256.clone().ok_or(StorageUploadError::MediaMissingSha256(input.media_id))?;
        if expected_sha256_bytes.len() != 32 {
            return Err(StorageUploadError::InvalidSha256Length {
                actual: expected_sha256_bytes.len(),
            });
        }
        let local_work_path = media
            .local_work_path
            .clone()
            .map(PathBuf::from)
            .ok_or(StorageUploadError::WorkspaceReclaimed(input.media_id))?;

        let symlink_metadata =
            tokio::fs::symlink_metadata(&local_work_path).await.map_err(|_| {
                StorageUploadError::LocalFileUnavailable { path: local_work_path.clone() }
            })?;
        if !symlink_metadata.is_file() || symlink_metadata.file_type().is_symlink() {
            return Err(StorageUploadError::LocalFileUnavailable { path: local_work_path.clone() });
        }
        if let Some(work_root) = &self.work_root {
            let canonical_root = tokio::fs::canonicalize(work_root).await.map_err(|_| {
                StorageUploadError::LocalFileUnavailable { path: work_root.clone() }
            })?;
            let canonical_file = tokio::fs::canonicalize(&local_work_path).await.map_err(|_| {
                StorageUploadError::LocalFileUnavailable { path: local_work_path.clone() }
            })?;
            if !canonical_file.starts_with(&canonical_root) {
                return Err(StorageUploadError::LocalFileOutsideWorkRoot {
                    path: local_work_path.clone(),
                    root: canonical_root,
                });
            }
        }
        let metadata = tokio::fs::metadata(&local_work_path).await.map_err(|_| {
            StorageUploadError::LocalFileUnavailable { path: local_work_path.clone() }
        })?;
        if let Some(limit) = self.max_storage_bytes
            && metadata.len() > limit
        {
            return Err(StorageUploadError::StorageOutputLimitExceeded {
                size: metadata.len(),
                limit,
            });
        }
        if let Some(limit) = self.api.max_upload_bytes()
            && metadata.len() > limit
        {
            return Err(StorageUploadError::TelegramUploadLimitExceeded {
                size: metadata.len(),
                limit,
            });
        }

        let expected_sha256 = hex_encode(&expected_sha256_bytes);
        let digest = sha256_file(&local_work_path).await.map_err(StorageUploadError::Hash)?;
        if digest.sha256 != expected_sha256 {
            return Err(StorageUploadError::HashMismatch {
                expected: expected_sha256,
                actual: digest.sha256,
            });
        }

        let reservation = self
            .store
            .reserve_storage_upload(StorageUploadReservationRequest {
                media_id: input.media_id,
                generation: input.generation,
            })
            .await
            .map_err(|error| StorageUploadError::Persistence(Box::new(error)))?;
        let (media_id, owner_token) = match reservation {
            StorageUploadReservation::Reserved { media_id, owner_token } => (media_id, owner_token),
            StorageUploadReservation::Reused(object) => {
                return Ok(StorageUploadOutcome::Reused(object));
            }
            StorageUploadReservation::InProgress { retry_at } => {
                return Err(StorageUploadError::InProgress { retry_at });
            }
            StorageUploadReservation::ReconciliationRequired => {
                return Err(StorageUploadError::ReconciliationRequired(input.media_id));
            }
            StorageUploadReservation::StaleGeneration { current_generation } => {
                return Err(StorageUploadError::StaleGeneration {
                    requested_generation: input.generation,
                    current_generation,
                });
            }
        };

        let request = StorageUploadRequest {
            storage_chat_id: self.storage_chat_id,
            media_kind: media.kind,
            local_work_path,
            caption: storage_caption(&caption_metadata),
        };
        let mut upload = Box::pin(self.api.upload_media(request));
        let mut renewal = tokio::time::interval(STORAGE_UPLOAD_RENEW_INTERVAL);
        renewal.tick().await;
        let uploaded = loop {
            tokio::select! {
                result = &mut upload => break result,
                _ = renewal.tick() => {
                    if let Err(error) = self
                        .store
                        .renew_storage_upload(
                            media_id,
                            owner_token,
                            STORAGE_UPLOAD_LEASE_DURATION,
                        )
                        .await
                    {
                        if let Err(mark_error) =
                            self.store.mark_storage_upload_unknown(media_id, owner_token).await
                        {
                            return Err(StorageUploadError::AmbiguousPersistence(Box::new(
                                mark_error,
                            )));
                        }
                        return Err(StorageUploadError::AmbiguousPersistence(Box::new(error)));
                    }
                }
            }
        };
        let uploaded = match uploaded {
            Ok(uploaded) => uploaded,
            Err(error) => {
                if A::is_ambiguous_error(&error) {
                    if let Err(mark_error) =
                        self.store.mark_storage_upload_unknown(media_id, owner_token).await
                    {
                        return Err(StorageUploadError::AmbiguousPersistence(Box::new(mark_error)));
                    }
                    return Err(StorageUploadError::AmbiguousApi(Box::new(error)));
                }
                self.store.release_storage_upload(media_id, owner_token).await.map_err(
                    |release_error| StorageUploadError::Persistence(Box::new(release_error)),
                )?;
                return Err(StorageUploadError::Api(Box::new(error)));
            }
        };

        let object = match self
            .store
            .complete_storage_upload(
                input.media_id,
                owner_token,
                sooqa_library::StorageUploadAttachment {
                    storage_chat_id: self.storage_chat_id,
                    storage_message_id: uploaded.storage_message_id,
                    telegram_file_id: Some(uploaded.telegram_file_id),
                    telegram_file_unique_id: Some(uploaded.telegram_file_unique_id),
                },
            )
            .await
        {
            Ok(object) => object,
            Err(error) => {
                if let Err(mark_error) =
                    self.store.mark_storage_upload_unknown(media_id, owner_token).await
                {
                    return Err(StorageUploadError::AmbiguousPersistence(Box::new(mark_error)));
                }
                return Err(StorageUploadError::AmbiguousPersistence(Box::new(error)));
            }
        };
        Ok(StorageUploadOutcome::Uploaded(object))
    }
}

const MAX_STORAGE_CAPTION_CHARS: usize = 1_024;
const MAX_STORAGE_CAPTION_TAGS: usize = 32;

fn storage_caption(metadata: &StorageCaptionMetadata) -> String {
    let mut lines = Vec::new();
    if let Some(description) = &metadata.description {
        let description = clean_caption_value(description, 512);
        if !description.is_empty() {
            lines.push(format!("description: {description}"));
        }
    }
    if !metadata.tags.is_empty() {
        let tags = metadata
            .tags
            .iter()
            .take(MAX_STORAGE_CAPTION_TAGS)
            .map(|tag| clean_caption_value(tag, 64))
            .filter(|tag| !tag.is_empty())
            .collect::<Vec<_>>()
            .join(", ");
        if !tags.is_empty() {
            lines.push(format!("tags: {tags}"));
        }
    }
    if let Some(source_url) = &metadata.source_url {
        let source_url = clean_caption_value(source_url, 512);
        if !source_url.is_empty() {
            lines.push(format!("source: {source_url}"));
        }
    }
    let caption = lines.join("\n");
    if caption.is_empty() {
        "sooqa".to_owned()
    } else {
        truncate_chars(&caption, MAX_STORAGE_CAPTION_CHARS)
    }
}

fn clean_caption_value(value: &str, max_chars: usize) -> String {
    truncate_chars(
        &value
            .chars()
            .map(|character| if character.is_control() { ' ' } else { character })
            .collect::<String>(),
        max_chars,
    )
    .trim()
    .to_owned()
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(HEX[(byte >> 4) as usize] as char);
        value.push(HEX[(byte & 0x0f) as usize] as char);
    }
    value
}

#[async_trait]
impl TelegramStorageApi for TeloxideApi {
    type Error = StorageUploadApiError;

    fn is_ambiguous_error(error: &Self::Error) -> bool {
        match error {
            StorageUploadApiError::Api(teloxide::RequestError::Api(
                teloxide::errors::ApiError::Unknown(_),
            ))
            | StorageUploadApiError::Api(teloxide::RequestError::Network(_))
            | StorageUploadApiError::Api(teloxide::RequestError::InvalidJson { .. })
            | StorageUploadApiError::Api(teloxide::RequestError::Io(_)) => true,
            StorageUploadApiError::Api(_) => false,
            StorageUploadApiError::MissingFileReference { .. } => true,
            StorageUploadApiError::StorageChatNotPrivateChannel
            | StorageUploadApiError::StorageBotNotAdministrator
            | StorageUploadApiError::StorageBotCannotPost => false,
        }
    }

    fn max_upload_bytes(&self) -> Option<u64> {
        self.cloud_upload_limit_bytes
    }

    async fn upload_media(
        &self,
        request: StorageUploadRequest,
    ) -> Result<StorageUploadResult, Self::Error> {
        let chat_id = ChatId(request.storage_chat_id);
        let message = match request.media_kind {
            MediaKind::Video => self
                .upload_bot()
                .send_video(chat_id, InputFile::file(request.local_work_path))
                .caption(request.caption)
                .supports_streaming(true)
                .send()
                .await
                .map_err(StorageUploadApiError::Api)?,
            MediaKind::Image => self
                .upload_bot()
                .send_photo(chat_id, InputFile::file(request.local_work_path))
                .caption(request.caption)
                .send()
                .await
                .map_err(StorageUploadApiError::Api)?,
            MediaKind::Audio => self
                .upload_bot()
                .send_audio(chat_id, InputFile::file(request.local_work_path))
                .caption(request.caption)
                .send()
                .await
                .map_err(StorageUploadApiError::Api)?,
            MediaKind::Animation => self
                .upload_bot()
                .send_animation(chat_id, InputFile::file(request.local_work_path))
                .caption(request.caption)
                .send()
                .await
                .map_err(StorageUploadApiError::Api)?,
        };

        let reference = match request.media_kind {
            MediaKind::Video => {
                message.video().map(|video| (&video.file.id, &video.file.unique_id))
            }
            MediaKind::Image => message
                .photo()
                .and_then(|photos| photos.last())
                .map(|photo| (&photo.file.id, &photo.file.unique_id)),
            MediaKind::Audio => {
                message.audio().map(|audio| (&audio.file.id, &audio.file.unique_id))
            }
            MediaKind::Animation => {
                message.animation().map(|animation| (&animation.file.id, &animation.file.unique_id))
            }
        }
        .ok_or(StorageUploadApiError::MissingFileReference { media_kind: request.media_kind })?;

        Ok(StorageUploadResult {
            storage_message_id: i64::from(message.id.0),
            telegram_file_id: reference.0.to_string(),
            telegram_file_unique_id: reference.1.to_string(),
        })
    }

    async fn verify_storage_chat(&self, chat_id: i64) -> Result<(), Self::Error> {
        let chat = self
            .bot()
            .get_chat(ChatId(chat_id))
            .send()
            .await
            .map_err(StorageUploadApiError::Api)?;
        let is_private_channel = matches!(
            chat.kind,
            ChatFullInfoKind::Public(public)
                if matches!(&public.kind, ChatFullInfoPublicKind::Channel(channel) if channel.username.is_none())
        );
        if !is_private_channel {
            return Err(StorageUploadApiError::StorageChatNotPrivateChannel);
        }

        let bot_user = self.bot().get_me().send().await.map_err(StorageUploadApiError::Api)?;
        let member = self
            .bot()
            .get_chat_member(ChatId(chat_id), bot_user.id)
            .send()
            .await
            .map_err(StorageUploadApiError::Api)?;
        match member.kind {
            ChatMemberKind::Owner(_) => Ok(()),
            ChatMemberKind::Administrator(admin) if admin.can_post_messages => Ok(()),
            ChatMemberKind::Administrator(_) => Err(StorageUploadApiError::StorageBotCannotPost),
            _ => Err(StorageUploadApiError::StorageBotNotAdministrator),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use sooqa_library::{Media, MediaStorageState, StorageReceipt};
    use time::OffsetDateTime;

    use super::*;

    #[derive(Clone, Default)]
    struct MockApi {
        requests: Arc<Mutex<Vec<StorageUploadRequest>>>,
        fail: Arc<Mutex<bool>>,
        ambiguous: Arc<Mutex<bool>>,
        upload_limit: Arc<Mutex<Option<u64>>>,
    }

    #[derive(Debug, Error)]
    #[error("mock storage error")]
    struct MockError {
        ambiguous: bool,
    }

    #[async_trait]
    impl TelegramStorageApi for MockApi {
        type Error = MockError;

        fn is_ambiguous_error(error: &Self::Error) -> bool {
            error.ambiguous
        }

        async fn upload_media(
            &self,
            request: StorageUploadRequest,
        ) -> Result<StorageUploadResult, Self::Error> {
            if *self.fail.lock().expect("mock mutex should not be poisoned") {
                return Err(MockError {
                    ambiguous: *self.ambiguous.lock().expect("mock mutex should not be poisoned"),
                });
            }
            self.requests.lock().expect("mock mutex should not be poisoned").push(request);
            Ok(StorageUploadResult {
                storage_message_id: 42,
                telegram_file_id: "file-id".to_owned(),
                telegram_file_unique_id: "unique-id".to_owned(),
            })
        }

        async fn verify_storage_chat(&self, _chat_id: i64) -> Result<(), Self::Error> {
            Ok(())
        }

        fn max_upload_bytes(&self) -> Option<u64> {
            *self.upload_limit.lock().expect("mock mutex should not be poisoned")
        }
    }

    #[derive(Clone, Default)]
    struct MockStore {
        canonical: Arc<Mutex<Option<Media>>>,
        active: Arc<Mutex<Option<StorageReceipt>>>,
        reserved: Arc<Mutex<bool>>,
        released: Arc<Mutex<bool>>,
        unknown: Arc<Mutex<bool>>,
        current_generation: Arc<Mutex<i32>>,
        complete_fail: Arc<Mutex<bool>>,
    }

    #[async_trait]
    impl StorageUploadStore for MockStore {
        type Error = MockError;

        async fn find_media(&self, _media_id: Uuid) -> Result<Option<Media>, Self::Error> {
            Ok(self.canonical.lock().expect("mock mutex should not be poisoned").clone())
        }

        async fn find_storage_caption_metadata(
            &self,
            _media_id: Uuid,
        ) -> Result<StorageCaptionMetadata, Self::Error> {
            Ok(StorageCaptionMetadata {
                description: Some("internal note".to_owned()),
                tags: vec!["cats".to_owned()],
                source_url: Some("https://example.test/clip.webm".to_owned()),
            })
        }

        async fn find_storage_receipt(
            &self,
            _media_id: Uuid,
        ) -> Result<Option<StorageReceipt>, Self::Error> {
            Ok(self.active.lock().expect("mock mutex should not be poisoned").clone())
        }

        async fn reserve_storage_upload(
            &self,
            request: StorageUploadReservationRequest,
        ) -> Result<StorageUploadReservation, Self::Error> {
            let current_generation =
                *self.current_generation.lock().expect("mock mutex should not be poisoned");
            if request.generation != current_generation {
                return Ok(StorageUploadReservation::StaleGeneration { current_generation });
            }
            if *self.unknown.lock().expect("mock mutex should not be poisoned") {
                return Ok(StorageUploadReservation::ReconciliationRequired);
            }
            let mut reserved = self.reserved.lock().expect("mock mutex should not be poisoned");
            if *reserved {
                return Ok(StorageUploadReservation::InProgress { retry_at: None });
            }
            *reserved = true;
            Ok(StorageUploadReservation::Reserved {
                media_id: Uuid::from_u128(3),
                owner_token: Uuid::from_u128(5),
            })
        }

        async fn complete_storage_upload(
            &self,
            _media_id: Uuid,
            _owner_token: Uuid,
            attachment: sooqa_library::StorageUploadAttachment,
        ) -> Result<StorageReceipt, Self::Error> {
            if *self.complete_fail.lock().expect("mock mutex should not be poisoned") {
                return Err(MockError { ambiguous: false });
            }
            let object = StorageReceipt {
                media_id: Uuid::from_u128(1),
                storage_chat_id: attachment.storage_chat_id,
                storage_message_id: attachment.storage_message_id,
                telegram_file_id: attachment.telegram_file_id,
                telegram_file_unique_id: attachment.telegram_file_unique_id,
                media_kind: MediaKind::Video,
                stored_at: OffsetDateTime::now_utc(),
            };
            *self.active.lock().expect("mock mutex should not be poisoned") = Some(object.clone());
            Ok(object)
        }

        async fn release_storage_upload(
            &self,
            _media_id: Uuid,
            _owner_token: Uuid,
        ) -> Result<(), Self::Error> {
            *self.released.lock().expect("mock mutex should not be poisoned") = true;
            Ok(())
        }

        async fn mark_storage_upload_unknown(
            &self,
            _media_id: Uuid,
            _owner_token: Uuid,
        ) -> Result<(), Self::Error> {
            *self.unknown.lock().expect("mock mutex should not be poisoned") = true;
            Ok(())
        }

        async fn renew_storage_upload(
            &self,
            _media_id: Uuid,
            _owner_token: Uuid,
            _lease_duration: std::time::Duration,
        ) -> Result<time::OffsetDateTime, Self::Error> {
            Ok(time::OffsetDateTime::now_utc())
        }
    }

    fn input() -> StorageUploadInput {
        StorageUploadInput { media_id: Uuid::from_u128(1), generation: 0 }
    }

    fn canonical_asset(path: &std::path::Path, sha256: Vec<u8>) -> Media {
        Media {
            id: Uuid::from_u128(1),
            kind: MediaKind::Video,
            title: None,
            description: None,
            tags: Vec::new(),
            mime_type: Some("video/mp4".to_owned()),
            container: Some("mp4".to_owned()),
            video_codec: None,
            audio_codec: None,
            width: None,
            height: None,
            duration_ms: None,
            bit_rate: None,
            file_size_bytes: None,
            sha256: Some(sha256),
            local_work_path: Some(path.to_string_lossy().into_owned()),
            storage_state: MediaStorageState::Pending,
            created_at: OffsetDateTime::now_utc(),
            updated_at: OffsetDateTime::now_utc(),
        }
    }

    #[tokio::test]
    async fn uploads_asset_with_bounded_metadata_caption_and_reuses_object() {
        let path = std::env::temp_dir().join(format!("sooqa-storage-{}.mp4", Uuid::new_v4()));
        tokio::fs::write(&path, b"canonical asset").await.expect("fixture should be written");
        let digest = sha256_file(&path).await.expect("fixture should hash");
        let api = MockApi::default();
        let store = MockStore::default();
        *store.canonical.lock().unwrap() =
            Some(canonical_asset(&path, hex_to_bytes(&digest.sha256)));
        let provider = StorageUploadProvider::new(api.clone(), store.clone(), -100123)
            .expect("storage chat ID should be valid");

        let outcome = provider.upload(input()).await.expect("upload should succeed");
        assert!(matches!(outcome, StorageUploadOutcome::Uploaded(_)));
        let request = api.requests.lock().unwrap().pop().expect("one upload should be sent");
        assert!(request.caption.contains("description: internal note"));
        assert!(request.caption.contains("tags: cats"));
        assert!(request.caption.contains("source: https://example.test/clip.webm"));
        assert!(!request.caption.contains("sha256"));
        assert!(!request.caption.contains("00000000-0000-0000-0000-000000000001"));
        assert!(request.caption.chars().count() <= MAX_STORAGE_CAPTION_CHARS);

        let outcome = provider.upload(input()).await.expect("stored object should be reused");
        assert!(matches!(outcome, StorageUploadOutcome::Reused(_)));
        assert!(api.requests.lock().unwrap().is_empty());
        tokio::fs::remove_file(path).await.expect("fixture should be removed");
    }

    #[tokio::test]
    async fn storage_ceiling_rejects_canonical_asset_before_reservation_or_upload() {
        let path = std::env::temp_dir().join(format!("sooqa-storage-{}.mp4", Uuid::new_v4()));
        tokio::fs::write(&path, b"canonical asset").await.expect("fixture should be written");
        let digest = sha256_file(&path).await.expect("fixture should hash");
        let api = MockApi::default();
        let store = MockStore::default();
        *store.canonical.lock().unwrap() =
            Some(canonical_asset(&path, hex_to_bytes(&digest.sha256)));
        let provider = StorageUploadProvider::new(api.clone(), store.clone(), -100123)
            .expect("storage chat ID should be valid")
            .with_max_storage_bytes(4);

        assert!(matches!(
            provider.upload(input()).await,
            Err(StorageUploadError::StorageOutputLimitExceeded { size: 15, limit: 4 })
        ));
        assert!(!*store.reserved.lock().unwrap());
        assert!(api.requests.lock().unwrap().is_empty());
        tokio::fs::remove_file(path).await.expect("fixture should be removed");
    }

    #[tokio::test]
    async fn cloud_upload_limit_rejects_canonical_asset_before_reservation_or_upload() {
        let path = std::env::temp_dir().join(format!("sooqa-storage-{}.mp4", Uuid::new_v4()));
        tokio::fs::write(&path, b"canonical asset").await.expect("fixture should be written");
        let digest = sha256_file(&path).await.expect("fixture should hash");
        let api = MockApi::default();
        *api.upload_limit.lock().unwrap() = Some(4);
        let store = MockStore::default();
        *store.canonical.lock().unwrap() =
            Some(canonical_asset(&path, hex_to_bytes(&digest.sha256)));
        let provider = StorageUploadProvider::new(api.clone(), store.clone(), -100123)
            .expect("storage chat ID should be valid");

        assert!(matches!(
            provider.upload(input()).await,
            Err(StorageUploadError::TelegramUploadLimitExceeded { size: 15, limit: 4 })
        ));
        assert!(!*store.reserved.lock().unwrap());
        assert!(api.requests.lock().unwrap().is_empty());
        tokio::fs::remove_file(path).await.expect("fixture should be removed");
    }

    #[tokio::test]
    async fn cloud_endpoint_upload_limit_rejects_before_reservation_or_network() {
        let path = std::env::temp_dir().join(format!("sooqa-storage-{}.mp4", Uuid::new_v4()));
        tokio::fs::write(&path, b"canonical asset").await.expect("fixture should be written");
        let digest = sha256_file(&path).await.expect("fixture should hash");
        let api =
            TeloxideApi::new("test-token", "https://api.telegram.org", Duration::from_secs(1))
                .expect("cloud Bot API URL should be accepted")
                .with_test_cloud_limits(4);
        let store = MockStore::default();
        *store.canonical.lock().unwrap() =
            Some(canonical_asset(&path, hex_to_bytes(&digest.sha256)));
        let provider = StorageUploadProvider::new(api, store.clone(), -100123)
            .expect("storage chat ID should be valid");

        assert!(matches!(
            provider.upload(input()).await,
            Err(StorageUploadError::TelegramUploadLimitExceeded { size: 15, limit: 4 })
        ));
        assert!(!*store.reserved.lock().unwrap());
        tokio::fs::remove_file(path).await.expect("fixture should be removed");
    }

    #[tokio::test]
    async fn upload_failure_releases_reservation() {
        let path = std::env::temp_dir().join(format!("sooqa-storage-{}.mp4", Uuid::new_v4()));
        tokio::fs::write(&path, b"canonical asset").await.expect("fixture should be written");
        let digest = sha256_file(&path).await.expect("fixture should hash");
        let api = MockApi::default();
        *api.fail.lock().unwrap() = true;
        let store = MockStore::default();
        *store.canonical.lock().unwrap() =
            Some(canonical_asset(&path, hex_to_bytes(&digest.sha256)));
        let provider = StorageUploadProvider::new(api.clone(), store.clone(), -100123)
            .expect("storage chat ID should be valid");

        assert!(provider.upload(input()).await.is_err());
        assert!(*store.released.lock().unwrap());
        tokio::fs::remove_file(path).await.expect("fixture should be removed");
    }

    #[tokio::test]
    async fn ambiguous_upload_keeps_storage_unknown_for_reconciliation() {
        let path = std::env::temp_dir().join(format!("sooqa-storage-{}.mp4", Uuid::new_v4()));
        tokio::fs::write(&path, b"canonical asset").await.expect("fixture should be written");
        let digest = sha256_file(&path).await.expect("fixture should hash");
        let api = MockApi::default();
        *api.fail.lock().unwrap() = true;
        *api.ambiguous.lock().unwrap() = true;
        let store = MockStore::default();
        *store.canonical.lock().unwrap() =
            Some(canonical_asset(&path, hex_to_bytes(&digest.sha256)));
        let provider = StorageUploadProvider::new(api.clone(), store.clone(), -100123)
            .expect("storage chat ID should be valid");

        assert!(matches!(provider.upload(input()).await, Err(StorageUploadError::AmbiguousApi(_))));
        assert!(*store.unknown.lock().unwrap());
        assert!(!*store.released.lock().unwrap());
        *api.fail.lock().unwrap() = false;
        assert!(matches!(
            provider.upload(input()).await,
            Err(StorageUploadError::ReconciliationRequired(_))
        ));
        assert!(api.requests.lock().unwrap().is_empty());
        tokio::fs::remove_file(path).await.expect("fixture should be removed");
    }

    #[tokio::test]
    async fn stale_generation_cannot_reserve_or_call_telegram() {
        let path = std::env::temp_dir().join(format!("sooqa-storage-{}.mp4", Uuid::new_v4()));
        tokio::fs::write(&path, b"canonical asset").await.expect("fixture should be written");
        let digest = sha256_file(&path).await.expect("fixture should hash");
        let api = MockApi::default();
        let store = MockStore::default();
        *store.canonical.lock().unwrap() =
            Some(canonical_asset(&path, hex_to_bytes(&digest.sha256)));
        *store.current_generation.lock().unwrap() = 1;
        let provider = StorageUploadProvider::new(api.clone(), store, -100123)
            .expect("storage chat ID should be valid");

        assert!(matches!(
            provider.upload(input()).await,
            Err(StorageUploadError::StaleGeneration {
                requested_generation: 0,
                current_generation: 1
            })
        ));
        assert!(api.requests.lock().unwrap().is_empty());
        tokio::fs::remove_file(path).await.expect("fixture should be removed");
    }

    #[tokio::test]
    async fn reset_after_ready_attach_rejects_reclaimed_workspace_upload() {
        let api = MockApi::default();
        let store = MockStore::default();
        let path = std::env::temp_dir().join(format!("sooqa-reclaimed-{}.mp4", Uuid::new_v4()));
        let digest = vec![7_u8; 32];
        let mut media = canonical_asset(&path, digest);
        media.storage_state = MediaStorageState::Ready;
        *store.canonical.lock().unwrap() = Some(media);
        *store.active.lock().unwrap() = Some(StorageReceipt {
            media_id: Uuid::from_u128(1),
            storage_chat_id: -100123,
            storage_message_id: 42,
            telegram_file_id: Some("file-id".to_owned()),
            telegram_file_unique_id: Some("unique-id".to_owned()),
            media_kind: MediaKind::Video,
            stored_at: OffsetDateTime::now_utc(),
        });
        let provider = StorageUploadProvider::new(api.clone(), store.clone(), -100123)
            .expect("storage chat ID should be valid");

        assert!(matches!(provider.upload(input()).await, Ok(StorageUploadOutcome::Reused(_))));

        // A reset clears the Telegram attachment and the reclaimed local
        // path before it can enqueue a new generation. The provider must
        // surface reconstruction as a terminal state, not retry an upload
        // that has no bytes to send.
        *store.active.lock().unwrap() = None;
        let mut reclaimed = canonical_asset(&path, vec![7_u8; 32]);
        reclaimed.local_work_path = None;
        reclaimed.storage_state = MediaStorageState::Pending;
        *store.canonical.lock().unwrap() = Some(reclaimed);
        assert!(matches!(
            provider.upload(input()).await,
            Err(StorageUploadError::WorkspaceReclaimed(media_id)) if media_id == Uuid::from_u128(1)
        ));
        assert!(!*store.reserved.lock().unwrap());
        assert!(api.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn hash_mismatch_is_rejected_before_reserving_a_storage_upload() {
        let path = std::env::temp_dir().join(format!("sooqa-storage-{}.mp4", Uuid::new_v4()));
        tokio::fs::write(&path, b"canonical asset").await.expect("fixture should be written");
        let api = MockApi::default();
        let store = MockStore::default();
        *store.canonical.lock().unwrap() = Some(canonical_asset(&path, vec![0; 32]));
        let provider = StorageUploadProvider::new(api, store.clone(), -100123)
            .expect("storage chat ID should be valid");

        assert!(matches!(
            provider.upload(input()).await,
            Err(StorageUploadError::HashMismatch { .. })
        ));
        assert!(!*store.reserved.lock().unwrap());
        tokio::fs::remove_file(path).await.expect("fixture should be removed");
    }

    #[tokio::test]
    async fn persistence_failure_after_upload_keeps_storage_unknown() {
        let path = std::env::temp_dir().join(format!("sooqa-storage-{}.mp4", Uuid::new_v4()));
        tokio::fs::write(&path, b"canonical asset").await.expect("fixture should be written");
        let digest = sha256_file(&path).await.expect("fixture should hash");
        let api = MockApi::default();
        let store = MockStore::default();
        *store.canonical.lock().unwrap() =
            Some(canonical_asset(&path, hex_to_bytes(&digest.sha256)));
        *store.complete_fail.lock().unwrap() = true;
        let provider = StorageUploadProvider::new(api, store.clone(), -100123)
            .expect("storage chat ID should be valid");

        assert!(matches!(
            provider.upload(input()).await,
            Err(StorageUploadError::AmbiguousPersistence(_))
        ));
        assert!(*store.unknown.lock().unwrap());
        assert!(!*store.released.lock().unwrap());
        tokio::fs::remove_file(path).await.expect("fixture should be removed");
    }

    #[test]
    fn unknown_telegram_errors_are_ambiguous_but_known_rejections_are_not() {
        let unknown = StorageUploadApiError::Api(teloxide::RequestError::Api(
            teloxide::errors::ApiError::Unknown("Internal Server Error".to_owned()),
        ));
        let rejected = StorageUploadApiError::Api(teloxide::RequestError::Api(
            teloxide::errors::ApiError::BotBlocked,
        ));

        assert!(<TeloxideApi as TelegramStorageApi>::is_ambiguous_error(&unknown));
        assert!(!<TeloxideApi as TelegramStorageApi>::is_ambiguous_error(&rejected));
    }

    #[test]
    fn rejects_public_storage_chat_ids() {
        assert!(matches!(
            StorageUploadProvider::<MockApi, MockStore>::new(
                MockApi::default(),
                MockStore::default(),
                123
            ),
            Err(StorageUploadError::InvalidStorageChatId)
        ));
    }

    fn hex_to_bytes(value: &str) -> Vec<u8> {
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                u8::from_str_radix(std::str::from_utf8(pair).expect("hex is UTF-8"), 16)
                    .expect("hex is valid")
            })
            .collect()
    }
}
