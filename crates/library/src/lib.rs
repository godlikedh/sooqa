//! Content catalogue and duplicate-management boundaries for sooqa.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentKind {
    Video,
    Image,
    Animation,
    Audio,
}

impl ContentKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Video => "video",
            Self::Image => "image",
            Self::Animation => "animation",
            Self::Audio => "audio",
        }
    }
}

impl TryFrom<&str> for ContentKind {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "video" => Ok(Self::Video),
            "image" => Ok(Self::Image),
            "animation" => Ok(Self::Animation),
            "audio" => Ok(Self::Audio),
            unknown => Err(unknown.to_owned()),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentStatus {
    Active,
    Archived,
    Deleted,
}

impl ContentStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Archived => "archived",
            Self::Deleted => "deleted",
        }
    }
}

impl TryFrom<&str> for ContentStatus {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "active" => Ok(Self::Active),
            "archived" => Ok(Self::Archived),
            "deleted" => Ok(Self::Deleted),
            unknown => Err(unknown.to_owned()),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssetRole {
    Original,
    Canonical,
    Preview,
    Thumbnail,
}

impl AssetRole {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Original => "original",
            Self::Canonical => "canonical",
            Self::Preview => "preview",
            Self::Thumbnail => "thumbnail",
        }
    }
}

impl TryFrom<&str> for AssetRole {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "original" => Ok(Self::Original),
            "canonical" => Ok(Self::Canonical),
            "preview" => Ok(Self::Preview),
            "thumbnail" => Ok(Self::Thumbnail),
            unknown => Err(unknown.to_owned()),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaKind {
    Video,
    Image,
    Audio,
    Animation,
}

impl MediaKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Video => "video",
            Self::Image => "image",
            Self::Audio => "audio",
            Self::Animation => "animation",
        }
    }
}

impl TryFrom<&str> for MediaKind {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "video" => Ok(Self::Video),
            "image" => Ok(Self::Image),
            "audio" => Ok(Self::Audio),
            "animation" => Ok(Self::Animation),
            unknown => Err(unknown.to_owned()),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceType {
    Webpage,
    DirectUrl,
    Youtube,
    Telegram,
    Upload,
}

impl SourceType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Webpage => "webpage",
            Self::DirectUrl => "direct_url",
            Self::Youtube => "youtube",
            Self::Telegram => "telegram",
            Self::Upload => "upload",
        }
    }
}

impl TryFrom<&str> for SourceType {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "webpage" => Ok(Self::Webpage),
            "direct_url" => Ok(Self::DirectUrl),
            "youtube" => Ok(Self::Youtube),
            "telegram" => Ok(Self::Telegram),
            "upload" => Ok(Self::Upload),
            unknown => Err(unknown.to_owned()),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageState {
    Local,
    Uploaded,
    Missing,
}

impl StorageState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Uploaded => "uploaded",
            Self::Missing => "missing",
        }
    }
}

impl TryFrom<&str> for StorageState {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "local" => Ok(Self::Local),
            "uploaded" => Ok(Self::Uploaded),
            "missing" => Ok(Self::Missing),
            unknown => Err(unknown.to_owned()),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageObjectStatus {
    Active,
    Missing,
    Inaccessible,
    Deleted,
}

impl StorageObjectStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Missing => "missing",
            Self::Inaccessible => "inaccessible",
            Self::Deleted => "deleted",
        }
    }
}

impl TryFrom<&str> for StorageObjectStatus {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "active" => Ok(Self::Active),
            "missing" => Ok(Self::Missing),
            "inaccessible" => Ok(Self::Inaccessible),
            "deleted" => Ok(Self::Deleted),
            unknown => Err(unknown.to_owned()),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ContentItem {
    pub id: Uuid,
    pub kind: ContentKind,
    pub status: ContentStatus,
    pub canonical_asset_id: Option<Uuid>,
    pub preferred_title: Option<String>,
    pub editorial_description: Option<String>,
    pub notes: Option<String>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub archived_at: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NewContentItem {
    pub kind: ContentKind,
    pub preferred_title: Option<String>,
    pub editorial_description: Option<String>,
    pub notes: Option<String>,
}

impl NewContentItem {
    pub fn new(kind: ContentKind) -> Self {
        Self { kind, preferred_title: None, editorial_description: None, notes: None }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct MediaAsset {
    pub id: Uuid,
    pub content_item_id: Uuid,
    pub role: AssetRole,
    pub media_kind: MediaKind,
    pub mime_type: Option<String>,
    pub container: Option<String>,
    pub video_codec: Option<String>,
    pub audio_codec: Option<String>,
    pub width: Option<i32>,
    pub height: Option<i32>,
    pub duration_ms: Option<u64>,
    pub bit_rate: Option<u64>,
    pub file_size_bytes: Option<u64>,
    pub sha256: Option<Vec<u8>>,
    pub local_work_path: Option<String>,
    pub storage_state: StorageState,
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NewMediaAsset {
    pub content_item_id: Uuid,
    pub role: AssetRole,
    pub media_kind: MediaKind,
    pub mime_type: Option<String>,
    pub container: Option<String>,
    pub video_codec: Option<String>,
    pub audio_codec: Option<String>,
    pub width: Option<i32>,
    pub height: Option<i32>,
    pub duration_ms: Option<u64>,
    pub bit_rate: Option<u64>,
    pub file_size_bytes: Option<u64>,
    pub sha256: Option<Vec<u8>>,
    pub local_work_path: Option<String>,
    pub storage_state: StorageState,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NewMediaAssetDraft {
    pub role: AssetRole,
    pub media_kind: MediaKind,
    pub mime_type: Option<String>,
    pub container: Option<String>,
    pub video_codec: Option<String>,
    pub audio_codec: Option<String>,
    pub width: Option<i32>,
    pub height: Option<i32>,
    pub duration_ms: Option<u64>,
    pub bit_rate: Option<u64>,
    pub file_size_bytes: Option<u64>,
    pub sha256: Option<Vec<u8>>,
    pub local_work_path: Option<String>,
    pub storage_state: StorageState,
}

impl NewMediaAssetDraft {
    pub fn for_content_item(self, content_item_id: Uuid) -> NewMediaAsset {
        NewMediaAsset {
            content_item_id,
            role: self.role,
            media_kind: self.media_kind,
            mime_type: self.mime_type,
            container: self.container,
            video_codec: self.video_codec,
            audio_codec: self.audio_codec,
            width: self.width,
            height: self.height,
            duration_ms: self.duration_ms,
            bit_rate: self.bit_rate,
            file_size_bytes: self.file_size_bytes,
            sha256: self.sha256,
            local_work_path: self.local_work_path,
            storage_state: self.storage_state,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceRecord {
    pub id: Uuid,
    pub content_item_id: Uuid,
    pub ingest_request_id: Option<Uuid>,
    pub source_type: SourceType,
    pub original_url: Option<String>,
    pub normalized_url: Option<String>,
    pub platform: Option<String>,
    pub platform_content_id: Option<String>,
    pub author_name: Option<String>,
    pub source_title: Option<String>,
    pub source_description: Option<String>,
    pub source_published_at: Option<OffsetDateTime>,
    pub retrieved_at: OffsetDateTime,
    pub metadata_json: Value,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NewSourceRecord {
    pub content_item_id: Uuid,
    pub ingest_request_id: Option<Uuid>,
    pub source_type: SourceType,
    pub original_url: Option<String>,
    pub normalized_url: Option<String>,
    pub platform: Option<String>,
    pub platform_content_id: Option<String>,
    pub author_name: Option<String>,
    pub source_title: Option<String>,
    pub source_description: Option<String>,
    pub source_published_at: Option<OffsetDateTime>,
    pub metadata_json: Value,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NewSourceRecordDraft {
    pub ingest_request_id: Option<Uuid>,
    pub source_type: SourceType,
    pub original_url: Option<String>,
    pub normalized_url: Option<String>,
    pub platform: Option<String>,
    pub platform_content_id: Option<String>,
    pub author_name: Option<String>,
    pub source_title: Option<String>,
    pub source_description: Option<String>,
    pub source_published_at: Option<OffsetDateTime>,
    pub metadata_json: Value,
}

impl NewSourceRecordDraft {
    pub fn for_content_item(self, content_item_id: Uuid) -> NewSourceRecord {
        NewSourceRecord {
            content_item_id,
            ingest_request_id: self.ingest_request_id,
            source_type: self.source_type,
            original_url: self.original_url,
            normalized_url: self.normalized_url,
            platform: self.platform,
            platform_content_id: self.platform_content_id,
            author_name: self.author_name,
            source_title: self.source_title,
            source_description: self.source_description,
            source_published_at: self.source_published_at,
            metadata_json: self.metadata_json,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExactDuplicateRequest {
    pub content_item: NewContentItem,
    pub asset: NewMediaAssetDraft,
    pub source: NewSourceRecordDraft,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExactDuplicateResolution {
    pub content_item: ContentItem,
    pub canonical_asset: MediaAsset,
    pub source_record: SourceRecord,
    pub content_created: bool,
    pub source_created: bool,
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DuplicateCandidateStatus {
    Pending,
    ConfirmedVariant,
    KeptSeparate,
    Dismissed,
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DuplicateCandidateAction {
    ConfirmVariant,
    KeepSeparate,
    Dismiss,
}

impl DuplicateCandidateAction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ConfirmVariant => "confirm_variant",
            Self::KeepSeparate => "keep_separate",
            Self::Dismiss => "dismiss",
        }
    }

    pub const fn resulting_status(self) -> DuplicateCandidateStatus {
        match self {
            Self::ConfirmVariant => DuplicateCandidateStatus::ConfirmedVariant,
            Self::KeepSeparate => DuplicateCandidateStatus::KeptSeparate,
            Self::Dismiss => DuplicateCandidateStatus::Dismissed,
        }
    }
}

impl DuplicateCandidateStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::ConfirmedVariant => "confirmed_variant",
            Self::KeptSeparate => "kept_separate",
            Self::Dismissed => "dismissed",
        }
    }
}

impl TryFrom<&str> for DuplicateCandidateStatus {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "pending" => Ok(Self::Pending),
            "confirmed_variant" => Ok(Self::ConfirmedVariant),
            "kept_separate" => Ok(Self::KeptSeparate),
            "dismissed" => Ok(Self::Dismissed),
            unknown => Err(unknown.to_owned()),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct DuplicateCandidate {
    pub id: Uuid,
    pub left_content_item_id: Uuid,
    pub right_content_item_id: Uuid,
    pub algorithm_version: String,
    pub score_basis_points: u16,
    pub evidence_json: Value,
    pub status: DuplicateCandidateStatus,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub resolved_at: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct DuplicateCandidateEvent {
    pub id: Uuid,
    pub candidate_id: Uuid,
    pub action: DuplicateCandidateAction,
    pub actor_device_token_id: Option<Uuid>,
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NewDuplicateCandidate {
    pub left_content_item_id: Uuid,
    pub right_content_item_id: Uuid,
    pub algorithm_version: String,
    pub score_basis_points: u16,
    pub evidence_json: Value,
}

impl NewDuplicateCandidate {
    pub fn try_new(
        left_content_item_id: Uuid,
        right_content_item_id: Uuid,
        algorithm_version: impl Into<String>,
        score_basis_points: u16,
        evidence_json: Value,
    ) -> Result<Self, DuplicateCandidateValidationError> {
        if left_content_item_id == right_content_item_id {
            return Err(DuplicateCandidateValidationError::SameContentItem);
        }
        if score_basis_points > 10_000 {
            return Err(DuplicateCandidateValidationError::InvalidScore(score_basis_points));
        }
        let algorithm_version = algorithm_version.into();
        if algorithm_version.trim().is_empty() {
            return Err(DuplicateCandidateValidationError::EmptyAlgorithmVersion);
        }
        let (left_content_item_id, right_content_item_id) =
            if left_content_item_id < right_content_item_id {
                (left_content_item_id, right_content_item_id)
            } else {
                (right_content_item_id, left_content_item_id)
            };
        Ok(Self {
            left_content_item_id,
            right_content_item_id,
            algorithm_version,
            score_basis_points,
            evidence_json,
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Error)]
pub enum DuplicateCandidateValidationError {
    #[error("duplicate candidate must reference two different content items")]
    SameContentItem,
    #[error("duplicate candidate score must be between 0 and 10000 basis points, got {0}")]
    InvalidScore(u16),
    #[error("duplicate candidate algorithm version must not be empty")]
    EmptyAlgorithmVersion,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LibraryItemDetail {
    pub content_item: ContentItem,
    pub canonical_asset: Option<MediaAsset>,
    pub tags: Vec<Tag>,
    pub sources: Vec<SourceRecord>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LibraryItemSummary {
    pub content_item: ContentItem,
    pub canonical_asset: Option<MediaAsset>,
    pub tags: Vec<Tag>,
    pub source_count: u64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LibraryCursor {
    pub updated_at: OffsetDateTime,
    pub id: Uuid,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LibrarySearchQuery {
    pub text: Option<String>,
    pub tags: Vec<String>,
    pub kind: Option<ContentKind>,
    pub status: Option<ContentStatus>,
    pub limit: u32,
    pub cursor: Option<LibraryCursor>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LibrarySearchPage {
    pub items: Vec<LibraryItemSummary>,
    pub next_cursor: Option<LibraryCursor>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ContentItemUpdate {
    pub preferred_title: Option<Option<String>>,
    pub editorial_description: Option<Option<String>>,
    pub notes: Option<Option<String>>,
    pub expected_updated_at: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Tag {
    pub id: Uuid,
    pub normalized_name: String,
    pub display_name: String,
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NewTag {
    pub normalized_name: String,
    pub display_name: String,
}

impl NewTag {
    pub fn try_new(display_name: impl Into<String>) -> Result<Self, TagValidationError> {
        let display_name = display_name.into();
        let display_name = display_name.trim().to_owned();
        if display_name.is_empty() {
            return Err(TagValidationError::Empty);
        }
        if display_name.chars().count() > MAX_TAG_LENGTH {
            return Err(TagValidationError::TooLong { max: MAX_TAG_LENGTH });
        }
        Ok(Self { normalized_name: display_name.to_lowercase(), display_name })
    }
}

const MAX_TAG_LENGTH: usize = 100;

#[derive(Debug, Clone, Eq, PartialEq, Error)]
pub enum TagValidationError {
    #[error("tag must not be empty")]
    Empty,
    #[error("tag must be at most {max} characters")]
    TooLong { max: usize },
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct StorageObject {
    pub id: Uuid,
    pub asset_id: Uuid,
    pub provider: String,
    pub storage_chat_id: i64,
    pub storage_message_id: i64,
    pub telegram_file_id: Option<String>,
    pub telegram_file_unique_id: Option<String>,
    pub media_kind: MediaKind,
    pub stored_at: OffsetDateTime,
    pub verified_at: Option<OffsetDateTime>,
    pub status: StorageObjectStatus,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NewStorageObject {
    pub asset_id: Uuid,
    pub provider: String,
    pub storage_chat_id: i64,
    pub storage_message_id: i64,
    pub telegram_file_id: Option<String>,
    pub telegram_file_unique_id: Option<String>,
    pub media_kind: MediaKind,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum StorageUploadReservation {
    Reserved { intent_id: Uuid, owner_token: Uuid },
    Reused(StorageObject),
    InProgress,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct StorageUploadIntent {
    pub id: Uuid,
    pub idempotency_key: String,
    pub state: String,
    pub resource_id: Option<Uuid>,
    pub created_at: OffsetDateTime,
    pub reservation_expires_at: Option<OffsetDateTime>,
}

#[async_trait]
pub trait StorageUploadStore: Clone + Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn find_canonical_asset(&self, asset_id: Uuid)
    -> Result<Option<MediaAsset>, Self::Error>;

    async fn find_active_storage_object(
        &self,
        asset_id: Uuid,
        provider: &str,
    ) -> Result<Option<StorageObject>, Self::Error>;

    async fn reserve_storage_upload(
        &self,
        asset_id: Uuid,
        provider: &str,
        idempotency_key: &str,
        request_hash: &[u8],
    ) -> Result<StorageUploadReservation, Self::Error>;

    async fn complete_storage_upload(
        &self,
        intent_id: Uuid,
        owner_token: Uuid,
        object: NewStorageObject,
    ) -> Result<StorageObject, Self::Error>;

    async fn release_storage_upload(
        &self,
        intent_id: Uuid,
        owner_token: Uuid,
    ) -> Result<(), Self::Error>;

    /// Preserve an intent after an external request whose outcome is unknown.
    /// A later reconciliation can complete the same intent with the returned
    /// Telegram message reference instead of sending another message.
    async fn mark_storage_upload_unknown(
        &self,
        intent_id: Uuid,
        owner_token: Uuid,
    ) -> Result<(), Self::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_kinds_round_trip_to_database_values() {
        for kind in
            [ContentKind::Video, ContentKind::Image, ContentKind::Animation, ContentKind::Audio]
        {
            assert_eq!(ContentKind::try_from(kind.as_str()), Ok(kind));
        }
    }

    #[test]
    fn tags_trim_and_normalize_without_merging_display_text() {
        let tag = NewTag::try_new("  Rust 🦀  ").expect("tag should be valid");

        assert_eq!(tag.display_name, "Rust 🦀");
        assert_eq!(tag.normalized_name, "rust 🦀");
    }

    #[test]
    fn empty_tags_are_rejected() {
        assert_eq!(
            NewTag::try_new("   ").expect_err("empty tag must fail"),
            TagValidationError::Empty
        );
    }

    #[test]
    fn duplicate_candidate_ids_are_canonicalized_and_validated() {
        let left = Uuid::from_u128(2);
        let right = Uuid::from_u128(1);
        let candidate = NewDuplicateCandidate::try_new(
            left,
            right,
            "frame_dhash_v1",
            9_000,
            serde_json::json!({"final_score": 0.9}),
        )
        .expect("candidate should be valid");
        assert_eq!(candidate.left_content_item_id, right);
        assert_eq!(candidate.right_content_item_id, left);
        assert_eq!(DuplicateCandidateStatus::Pending.as_str(), "pending");
    }
}
