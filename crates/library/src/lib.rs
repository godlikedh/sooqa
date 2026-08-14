//! Media catalogue and storage boundaries for sooqa.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaKind {
    Video,
    Image,
    Animation,
    Audio,
}

impl MediaKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Video => "video",
            Self::Image => "image",
            Self::Animation => "animation",
            Self::Audio => "audio",
        }
    }
}

impl TryFrom<&str> for MediaKind {
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
pub enum MediaStatus {
    Active,
    Archived,
    Deleted,
}

impl MediaStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Archived => "archived",
            Self::Deleted => "deleted",
        }
    }
}

impl TryFrom<&str> for MediaStatus {
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
pub enum MediaStorageState {
    Pending,
    Ready,
    Unknown,
    Missing,
}

impl MediaStorageState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending_storage",
            Self::Ready => "ready",
            Self::Unknown => "storage_unknown",
            Self::Missing => "missing",
        }
    }
}

impl TryFrom<&str> for MediaStorageState {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "pending_storage" => Ok(Self::Pending),
            "ready" => Ok(Self::Ready),
            "storage_unknown" => Ok(Self::Unknown),
            "missing" => Ok(Self::Missing),
            unknown => Err(unknown.to_owned()),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    Webpage,
    DirectUrl,
    Youtube,
    Telegram,
    Upload,
}

impl SourceKind {
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

impl TryFrom<&str> for SourceKind {
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

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Media {
    pub id: Uuid,
    pub kind: MediaKind,
    pub status: MediaStatus,
    pub title: Option<String>,
    pub description: Option<String>,
    pub notes: Option<String>,
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
    pub storage_state: MediaStorageState,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub archived_at: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NewMedia {
    pub kind: MediaKind,
    pub title: Option<String>,
    pub description: Option<String>,
    pub notes: Option<String>,
}

impl NewMedia {
    pub fn new(kind: MediaKind) -> Self {
        Self { kind, title: None, description: None, notes: None }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MediaMetadata {
    pub kind: MediaKind,
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
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MediaSourceInput {
    pub ingest_id: Option<Uuid>,
    pub kind: SourceKind,
    pub original_url: Option<String>,
    pub normalized_url: Option<String>,
    pub platform: Option<String>,
    pub platform_content_id: Option<String>,
    pub author_name: Option<String>,
    pub title: Option<String>,
    pub description: Option<String>,
    pub published_at: Option<OffsetDateTime>,
    pub metadata: Value,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MediaIngest {
    pub media: NewMedia,
    pub metadata: MediaMetadata,
    pub source: MediaSourceInput,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct MediaSource {
    pub ingest_id: Option<Uuid>,
    pub kind: SourceKind,
    pub original_url: Option<String>,
    pub normalized_url: Option<String>,
    pub platform: Option<String>,
    pub platform_content_id: Option<String>,
    pub author_name: Option<String>,
    pub title: Option<String>,
    pub description: Option<String>,
    pub published_at: Option<OffsetDateTime>,
    pub retrieved_at: OffsetDateTime,
    pub metadata: Value,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MediaResolution {
    pub media: Media,
    pub source: MediaSource,
    pub media_created: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MediaDetails {
    pub media: Media,
    pub tags: Vec<Tag>,
    pub source: Option<MediaSource>,
    pub storage_url: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MediaSummary {
    pub media: Media,
    pub tags: Vec<Tag>,
    pub source_count: u64,
    pub source_url: Option<String>,
    pub source_original_url: Option<String>,
    pub source_metadata: Option<Value>,
    pub storage_url: Option<String>,
}

/// A bounded read model for media whose storage-caption synchronization needs attention.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CaptionSyncFailure {
    pub media_id: Uuid,
    pub error_message: Option<String>,
}

/// Exact lookup forms supported by the bounded admin catalogue API.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MediaLookup {
    Identifier(Uuid),
    MediaId(Uuid),
    IngestId(Uuid),
    SourceUrls(Vec<String>),
    StorageMessage { chat_id: i64, message_id: i64 },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MediaCursor {
    pub updated_at: OffsetDateTime,
    pub id: Uuid,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MediaSearchQuery {
    pub text: Option<String>,
    pub tags: Vec<String>,
    pub kind: Option<MediaKind>,
    pub status: Option<MediaStatus>,
    pub limit: u32,
    pub cursor: Option<MediaCursor>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MediaPage {
    pub items: Vec<MediaSummary>,
    pub next_cursor: Option<MediaCursor>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MediaUpdate {
    pub title: Option<Option<String>>,
    pub description: Option<Option<String>>,
    pub notes: Option<Option<String>>,
    pub expected_updated_at: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Tag {
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
        let display_name = display_name.into().trim().to_owned();
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

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct VideoFingerprintCandidate {
    pub media_id: Uuid,
    pub width: Option<i32>,
    pub height: Option<i32>,
    pub audio_codec: Option<String>,
    pub fingerprint_version: String,
    pub fingerprint_data: Vec<u8>,
    pub search_tokens: Vec<i64>,
    pub shared_token_count: i64,
    pub overlap_bps: i64,
}

/// Bounded evidence retained when the video identity gate needs an explicit
/// human decision. It deliberately contains scalar alignment results only;
/// authoritative fingerprint bytes remain on `media`.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct VideoDuplicateEvidence {
    pub algorithm_version: String,
    pub matches: Vec<VideoDuplicateMatch>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct VideoDuplicateMatch {
    pub media_id: Uuid,
    pub fingerprint_version: String,
    pub classification: VideoDuplicateClassification,
    pub aligned_offset_ms: i64,
    pub informative_matched_samples: u16,
    pub incoming_coverage_bps: u16,
    pub candidate_coverage_bps: u16,
    pub median_distance_bps: u16,
    pub high_percentile_distance_bps: u16,
    pub longest_temporally_consistent_run: u16,
    pub unmatched_incoming_prefix: u16,
    pub unmatched_incoming_suffix: u16,
    pub unmatched_candidate_prefix: u16,
    pub unmatched_candidate_suffix: u16,
    pub gap_count: u16,
    pub score_bps: u16,
    pub shared_token_count: i64,
    pub token_overlap_bps: i64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VideoDuplicateClassification {
    StrongDuplicate,
    PartialMatch,
}

pub const MAX_VIDEO_DUPLICATE_MATCHES: usize = 3;
pub const MAX_VIDEO_DUPLICATE_EVIDENCE_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum VideoIdentityOutcome {
    ExactDuplicate { media_id: Uuid },
    NewMedia { media_id: Uuid },
    DuplicatePending { evidence: VideoDuplicateEvidence },
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct StorageReceipt {
    pub media_id: Uuid,
    pub storage_chat_id: i64,
    pub storage_message_id: i64,
    pub telegram_file_id: Option<String>,
    pub telegram_file_unique_id: Option<String>,
    pub media_kind: MediaKind,
    pub stored_at: OffsetDateTime,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct StorageUploadInfo {
    pub media_id: Uuid,
    pub state: String,
    pub generation: i32,
    pub storage_chat_id: Option<i64>,
    pub storage_message_id: Option<i64>,
    pub file_id: Option<String>,
    pub file_unique_id: Option<String>,
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StorageCaptionMetadata {
    pub description: Option<String>,
    pub tags: Vec<String>,
    pub source_url: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StorageUploadAttachment {
    pub storage_chat_id: i64,
    pub storage_message_id: i64,
    pub telegram_file_id: Option<String>,
    pub telegram_file_unique_id: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StorageUploadReservationRequest {
    pub media_id: Uuid,
    pub generation: i32,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum StorageUploadReservation {
    Reserved { media_id: Uuid, owner_token: Uuid },
    Reused(StorageReceipt),
    InProgress { retry_at: Option<OffsetDateTime> },
    ReconciliationRequired,
    StaleGeneration { current_generation: i32 },
}

#[async_trait]
pub trait StorageUploadStore: Clone + Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn find_media(&self, media_id: Uuid) -> Result<Option<Media>, Self::Error>;

    async fn find_storage_caption_metadata(
        &self,
        media_id: Uuid,
    ) -> Result<StorageCaptionMetadata, Self::Error>;

    async fn find_storage_receipt(
        &self,
        media_id: Uuid,
    ) -> Result<Option<StorageReceipt>, Self::Error>;

    async fn reserve_storage_upload(
        &self,
        request: StorageUploadReservationRequest,
    ) -> Result<StorageUploadReservation, Self::Error>;

    async fn renew_storage_upload(
        &self,
        media_id: Uuid,
        owner_token: Uuid,
        lease_duration: Duration,
    ) -> Result<OffsetDateTime, Self::Error>;

    async fn complete_storage_upload(
        &self,
        media_id: Uuid,
        owner_token: Uuid,
        attachment: StorageUploadAttachment,
    ) -> Result<StorageReceipt, Self::Error>;

    async fn release_storage_upload(
        &self,
        media_id: Uuid,
        owner_token: Uuid,
    ) -> Result<(), Self::Error>;

    async fn mark_storage_upload_unknown(
        &self,
        media_id: Uuid,
        owner_token: Uuid,
    ) -> Result<(), Self::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn media_kinds_round_trip_to_database_values() {
        for kind in [MediaKind::Video, MediaKind::Image, MediaKind::Animation, MediaKind::Audio] {
            assert_eq!(MediaKind::try_from(kind.as_str()), Ok(kind));
        }
    }

    #[test]
    fn storage_states_preserve_ambiguous_uploads() {
        assert_eq!(MediaStorageState::try_from("storage_unknown"), Ok(MediaStorageState::Unknown));
        assert_eq!(MediaStorageState::Unknown.as_str(), "storage_unknown");
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
}
