use std::path::PathBuf;

use async_trait::async_trait;
use sooqa_library::{
    MediaKind, NewStorageObject, StorageObject, StorageUploadReservation, StorageUploadStore,
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
    pub asset_id: Uuid,
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
    Uploaded(StorageObject),
    Reused(StorageObject),
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
    #[error("canonical asset {0} was not found")]
    CanonicalAssetMissing(Uuid),
    #[error("canonical asset {0} has no recorded SHA-256")]
    CanonicalAssetMissingSha256(Uuid),
    #[error("canonical asset {0} has no local work path")]
    CanonicalAssetMissingWorkPath(Uuid),
    #[error("canonical asset SHA-256 must contain 32 bytes, got {actual}")]
    InvalidSha256Length { actual: usize },
    #[error("canonical asset file is not available at {path}")]
    LocalFileUnavailable { path: PathBuf },
    #[error("could not hash canonical asset: {0}")]
    Hash(#[source] sooqa_media::HashError),
    #[error("canonical asset hash mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: String, actual: String },
    #[error("storage upload is already in progress or requires reconciliation for this asset")]
    InProgress,
    #[error("storage persistence failed: {0}")]
    Persistence(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("Telegram storage API failed: {0}")]
    Api(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("Telegram storage API result is ambiguous; upload intent remains pending: {0}")]
    AmbiguousApi(#[source] Box<dyn std::error::Error + Send + Sync>),
}

impl StorageUploadError {
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::InProgress | Self::Persistence(_) | Self::Api(_) | Self::AmbiguousApi(_)
        )
    }
}

#[derive(Clone)]
pub struct StorageUploadProvider<A, S> {
    api: A,
    store: S,
    storage_chat_id: i64,
}

impl<A, S> StorageUploadProvider<A, S>
where
    A: TelegramStorageApi,
    S: StorageUploadStore,
{
    pub fn new(api: A, store: S, storage_chat_id: i64) -> Result<Self, StorageUploadError> {
        if storage_chat_id >= 0 {
            return Err(StorageUploadError::InvalidStorageChatId);
        }
        Ok(Self { api, store, storage_chat_id })
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
        let asset = self
            .store
            .find_canonical_asset(input.asset_id)
            .await
            .map_err(|error| StorageUploadError::Persistence(Box::new(error)))?
            .ok_or(StorageUploadError::CanonicalAssetMissing(input.asset_id))?;
        let expected_sha256_bytes = asset
            .sha256
            .clone()
            .ok_or(StorageUploadError::CanonicalAssetMissingSha256(input.asset_id))?;
        if expected_sha256_bytes.len() != 32 {
            return Err(StorageUploadError::InvalidSha256Length {
                actual: expected_sha256_bytes.len(),
            });
        }
        let local_work_path = asset
            .local_work_path
            .clone()
            .map(PathBuf::from)
            .ok_or(StorageUploadError::CanonicalAssetMissingWorkPath(input.asset_id))?;

        let metadata = tokio::fs::metadata(&local_work_path).await.map_err(|_| {
            StorageUploadError::LocalFileUnavailable { path: local_work_path.clone() }
        })?;
        if !metadata.is_file() {
            return Err(StorageUploadError::LocalFileUnavailable { path: local_work_path.clone() });
        }

        let expected_sha256 = hex_encode(&expected_sha256_bytes);
        let digest = sha256_file(&local_work_path).await.map_err(StorageUploadError::Hash)?;
        if digest.sha256 != expected_sha256 {
            return Err(StorageUploadError::HashMismatch {
                expected: expected_sha256,
                actual: digest.sha256,
            });
        }

        if let Some(object) = self
            .store
            .find_active_storage_object(input.asset_id, TELEGRAM_STORAGE_PROVIDER)
            .await
            .map_err(|error| StorageUploadError::Persistence(Box::new(error)))?
        {
            return Ok(StorageUploadOutcome::Reused(object));
        }

        let idempotency_key = format!("telegram:storage:{}:v1", input.asset_id);
        let reservation = self
            .store
            .reserve_storage_upload(
                input.asset_id,
                TELEGRAM_STORAGE_PROVIDER,
                &idempotency_key,
                &expected_sha256_bytes,
            )
            .await
            .map_err(|error| StorageUploadError::Persistence(Box::new(error)))?;
        let intent_id = match reservation {
            StorageUploadReservation::Reserved { intent_id } => intent_id,
            StorageUploadReservation::Reused(object) => {
                return Ok(StorageUploadOutcome::Reused(object));
            }
            StorageUploadReservation::InProgress => return Err(StorageUploadError::InProgress),
        };

        let request = StorageUploadRequest {
            storage_chat_id: self.storage_chat_id,
            media_kind: asset.media_kind,
            local_work_path,
            caption: storage_caption(input.asset_id, asset.content_item_id, &expected_sha256),
        };
        let uploaded = match self.api.upload_media(request).await {
            Ok(uploaded) => uploaded,
            Err(error) => {
                if A::is_ambiguous_error(&error) {
                    self.store.mark_storage_upload_unknown(intent_id).await.map_err(
                        |mark_error| StorageUploadError::Persistence(Box::new(mark_error)),
                    )?;
                    return Err(StorageUploadError::AmbiguousApi(Box::new(error)));
                }
                self.store.release_storage_upload(intent_id).await.map_err(|release_error| {
                    StorageUploadError::Persistence(Box::new(release_error))
                })?;
                return Err(StorageUploadError::Api(Box::new(error)));
            }
        };

        let object = match self
            .store
            .complete_storage_upload(
                intent_id,
                NewStorageObject {
                    asset_id: input.asset_id,
                    provider: TELEGRAM_STORAGE_PROVIDER.to_owned(),
                    storage_chat_id: self.storage_chat_id,
                    storage_message_id: uploaded.storage_message_id,
                    telegram_file_id: Some(uploaded.telegram_file_id),
                    telegram_file_unique_id: Some(uploaded.telegram_file_unique_id),
                    media_kind: asset.media_kind,
                },
            )
            .await
        {
            Ok(object) => object,
            Err(error) => {
                self.store
                    .mark_storage_upload_unknown(intent_id)
                    .await
                    .map_err(|mark_error| StorageUploadError::Persistence(Box::new(mark_error)))?;
                return Err(StorageUploadError::Persistence(Box::new(error)));
            }
        };
        Ok(StorageUploadOutcome::Uploaded(object))
    }
}

fn storage_caption(asset_id: Uuid, content_item_id: Uuid, sha256: &str) -> String {
    format!(
        "asset: {asset_id}\ncontent: {content_item_id}\nsha256: {}",
        &sha256[..sha256.len().min(12)]
    )
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
            StorageUploadApiError::Api(error) => !matches!(
                error,
                teloxide::RequestError::Api(_)
                    | teloxide::RequestError::MigrateToChatId(_)
                    | teloxide::RequestError::RetryAfter(_)
            ),
            StorageUploadApiError::MissingFileReference { .. } => true,
            StorageUploadApiError::StorageChatNotPrivateChannel
            | StorageUploadApiError::StorageBotNotAdministrator
            | StorageUploadApiError::StorageBotCannotPost => false,
        }
    }

    async fn upload_media(
        &self,
        request: StorageUploadRequest,
    ) -> Result<StorageUploadResult, Self::Error> {
        let chat_id = ChatId(request.storage_chat_id);
        let message = match request.media_kind {
            MediaKind::Video => self
                .bot()
                .send_video(chat_id, InputFile::file(request.local_work_path))
                .caption(request.caption)
                .send()
                .await
                .map_err(StorageUploadApiError::Api)?,
            MediaKind::Image => self
                .bot()
                .send_photo(chat_id, InputFile::file(request.local_work_path))
                .caption(request.caption)
                .send()
                .await
                .map_err(StorageUploadApiError::Api)?,
            MediaKind::Audio => self
                .bot()
                .send_audio(chat_id, InputFile::file(request.local_work_path))
                .caption(request.caption)
                .send()
                .await
                .map_err(StorageUploadApiError::Api)?,
            MediaKind::Animation => self
                .bot()
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

    use sooqa_library::{AssetRole, MediaAsset, StorageObjectStatus, StorageState};
    use time::OffsetDateTime;

    use super::*;

    #[derive(Clone, Default)]
    struct MockApi {
        requests: Arc<Mutex<Vec<StorageUploadRequest>>>,
        fail: Arc<Mutex<bool>>,
        ambiguous: Arc<Mutex<bool>>,
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
    }

    #[derive(Clone, Default)]
    struct MockStore {
        canonical: Arc<Mutex<Option<MediaAsset>>>,
        active: Arc<Mutex<Option<StorageObject>>>,
        reserved: Arc<Mutex<bool>>,
        released: Arc<Mutex<bool>>,
        unknown: Arc<Mutex<bool>>,
    }

    #[async_trait]
    impl StorageUploadStore for MockStore {
        type Error = MockError;

        async fn find_canonical_asset(
            &self,
            _asset_id: Uuid,
        ) -> Result<Option<MediaAsset>, Self::Error> {
            Ok(self.canonical.lock().expect("mock mutex should not be poisoned").clone())
        }

        async fn find_active_storage_object(
            &self,
            _asset_id: Uuid,
            _provider: &str,
        ) -> Result<Option<StorageObject>, Self::Error> {
            Ok(self.active.lock().expect("mock mutex should not be poisoned").clone())
        }

        async fn reserve_storage_upload(
            &self,
            _asset_id: Uuid,
            _provider: &str,
            _idempotency_key: &str,
            _request_hash: &[u8],
        ) -> Result<StorageUploadReservation, Self::Error> {
            let mut reserved = self.reserved.lock().expect("mock mutex should not be poisoned");
            if *reserved {
                return Ok(StorageUploadReservation::InProgress);
            }
            *reserved = true;
            Ok(StorageUploadReservation::Reserved { intent_id: Uuid::from_u128(3) })
        }

        async fn complete_storage_upload(
            &self,
            _intent_id: Uuid,
            object: NewStorageObject,
        ) -> Result<StorageObject, Self::Error> {
            let object = StorageObject {
                id: Uuid::from_u128(4),
                asset_id: object.asset_id,
                provider: object.provider,
                storage_chat_id: object.storage_chat_id,
                storage_message_id: object.storage_message_id,
                telegram_file_id: object.telegram_file_id,
                telegram_file_unique_id: object.telegram_file_unique_id,
                media_kind: object.media_kind,
                stored_at: OffsetDateTime::now_utc(),
                verified_at: None,
                status: StorageObjectStatus::Active,
            };
            *self.active.lock().expect("mock mutex should not be poisoned") = Some(object.clone());
            Ok(object)
        }

        async fn release_storage_upload(&self, _intent_id: Uuid) -> Result<(), Self::Error> {
            *self.released.lock().expect("mock mutex should not be poisoned") = true;
            Ok(())
        }

        async fn mark_storage_upload_unknown(&self, _intent_id: Uuid) -> Result<(), Self::Error> {
            *self.unknown.lock().expect("mock mutex should not be poisoned") = true;
            Ok(())
        }
    }

    fn input() -> StorageUploadInput {
        StorageUploadInput { asset_id: Uuid::from_u128(1) }
    }

    fn canonical_asset(path: &std::path::Path, sha256: Vec<u8>) -> MediaAsset {
        MediaAsset {
            id: Uuid::from_u128(1),
            content_item_id: Uuid::from_u128(2),
            role: AssetRole::Canonical,
            media_kind: MediaKind::Video,
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
            storage_state: StorageState::Local,
            created_at: OffsetDateTime::now_utc(),
        }
    }

    #[tokio::test]
    async fn uploads_hashed_asset_with_diagnostic_caption_and_reuses_object() {
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
        assert!(request.caption.contains("asset: 00000000-0000-0000-0000-000000000001"));
        assert!(request.caption.contains("content: 00000000-0000-0000-0000-000000000002"));

        let outcome = provider.upload(input()).await.expect("stored object should be reused");
        assert!(matches!(outcome, StorageUploadOutcome::Reused(_)));
        assert!(api.requests.lock().unwrap().is_empty());
        tokio::fs::remove_file(path).await.expect("fixture should be removed");
    }

    #[tokio::test]
    async fn upload_failure_releases_intent() {
        let path = std::env::temp_dir().join(format!("sooqa-storage-{}.mp4", Uuid::new_v4()));
        tokio::fs::write(&path, b"canonical asset").await.expect("fixture should be written");
        let digest = sha256_file(&path).await.expect("fixture should hash");
        let api = MockApi::default();
        *api.fail.lock().unwrap() = true;
        let store = MockStore::default();
        *store.canonical.lock().unwrap() =
            Some(canonical_asset(&path, hex_to_bytes(&digest.sha256)));
        let provider = StorageUploadProvider::new(api, store.clone(), -100123)
            .expect("storage chat ID should be valid");

        assert!(provider.upload(input()).await.is_err());
        assert!(*store.released.lock().unwrap());
        tokio::fs::remove_file(path).await.expect("fixture should be removed");
    }

    #[tokio::test]
    async fn ambiguous_upload_keeps_intent_for_reconciliation() {
        let path = std::env::temp_dir().join(format!("sooqa-storage-{}.mp4", Uuid::new_v4()));
        tokio::fs::write(&path, b"canonical asset").await.expect("fixture should be written");
        let digest = sha256_file(&path).await.expect("fixture should hash");
        let api = MockApi::default();
        *api.fail.lock().unwrap() = true;
        *api.ambiguous.lock().unwrap() = true;
        let store = MockStore::default();
        *store.canonical.lock().unwrap() =
            Some(canonical_asset(&path, hex_to_bytes(&digest.sha256)));
        let provider = StorageUploadProvider::new(api, store.clone(), -100123)
            .expect("storage chat ID should be valid");

        assert!(matches!(provider.upload(input()).await, Err(StorageUploadError::AmbiguousApi(_))));
        assert!(*store.unknown.lock().unwrap());
        assert!(!*store.released.lock().unwrap());
        tokio::fs::remove_file(path).await.expect("fixture should be removed");
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
