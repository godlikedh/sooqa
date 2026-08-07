//! Content catalogue and duplicate-management boundaries for sooqa.

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
}

impl ContentKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Video => "video",
            Self::Image => "image",
            Self::Animation => "animation",
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

#[cfg(test)]
mod tests {
    use super::*;

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
