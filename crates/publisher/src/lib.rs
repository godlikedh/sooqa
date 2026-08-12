//! Channel and post aggregates for the Publisher boundary.

use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::{OffsetDateTime, Time};
use uuid::Uuid;

const MAX_CHANNEL_NAME_LENGTH: usize = 128;
const MAX_REQUEST_KEY_LENGTH: usize = 255;
pub const MAX_CAPTION_LENGTH: usize = 1_024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Channel {
    pub id: Uuid,
    pub name: String,
    pub telegram_chat_id: i64,
    pub is_enabled: bool,
    pub time_zone: String,
    pub window_start: Time,
    pub window_end: Time,
    pub interval_minutes: i32,
    pub default_parse_mode: Option<String>,
    pub default_disable_notification: bool,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewChannel {
    pub name: String,
    pub telegram_chat_id: i64,
    pub time_zone: String,
    pub window_start: Time,
    pub window_end: Time,
    pub interval_minutes: i32,
    pub default_parse_mode: Option<String>,
    pub default_disable_notification: bool,
}

impl NewChannel {
    pub fn try_new(
        name: impl Into<String>,
        telegram_chat_id: i64,
    ) -> Result<Self, ChannelValidationError> {
        let name = name.into().trim().to_owned();
        if name.is_empty() {
            return Err(ChannelValidationError::EmptyName);
        }
        if name.chars().count() > MAX_CHANNEL_NAME_LENGTH {
            return Err(ChannelValidationError::NameTooLong { max: MAX_CHANNEL_NAME_LENGTH });
        }
        if telegram_chat_id >= 0 {
            return Err(ChannelValidationError::InvalidTelegramChatId(telegram_chat_id));
        }
        Ok(Self {
            name,
            telegram_chat_id,
            time_zone: "UTC".to_owned(),
            window_start: Time::from_hms(8, 0, 0).expect("valid default window"),
            window_end: Time::from_hms(22, 0, 0).expect("valid default window"),
            interval_minutes: 30,
            default_parse_mode: None,
            default_disable_notification: false,
        })
    }

    pub fn validate(&self) -> Result<(), ChannelValidationError> {
        if self.time_zone.trim().is_empty() {
            return Err(ChannelValidationError::EmptyTimeZone);
        }
        if self.time_zone.parse::<chrono_tz::Tz>().is_err() {
            return Err(ChannelValidationError::InvalidTimeZone);
        }
        if self.window_start >= self.window_end {
            return Err(ChannelValidationError::InvalidWindow);
        }
        if self.interval_minutes <= 0 {
            return Err(ChannelValidationError::InvalidInterval);
        }
        if self
            .default_parse_mode
            .as_deref()
            .is_some_and(|mode| !matches!(mode, "HTML" | "MarkdownV2"))
        {
            return Err(ChannelValidationError::InvalidParseMode);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Error)]
pub enum ChannelValidationError {
    #[error("channel name must not be empty")]
    EmptyName,
    #[error("channel name must be at most {max} characters")]
    NameTooLong { max: usize },
    #[error("Telegram channel chat ID must be negative, got {0}")]
    InvalidTelegramChatId(i64),
    #[error("channel time zone must not be empty")]
    EmptyTimeZone,
    #[error("channel time zone is invalid")]
    InvalidTimeZone,
    #[error("channel publication window is invalid")]
    InvalidWindow,
    #[error("channel interval must be greater than zero")]
    InvalidInterval,
    #[error("channel default parse mode is invalid")]
    InvalidParseMode,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostState {
    Draft,
    Queued,
    Sending,
    Published,
    Unknown,
    Failed,
    Cancelled,
}

impl PostState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Queued => "queued",
            Self::Sending => "sending",
            Self::Published => "published",
            Self::Unknown => "unknown",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub const fn is_live_or_fenced(self) -> bool {
        matches!(self, Self::Sending | Self::Published | Self::Unknown)
    }

    pub const fn is_queue_mutable(self) -> bool {
        matches!(self, Self::Draft | Self::Queued | Self::Failed)
    }
}

impl TryFrom<&str> for PostState {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "draft" => Ok(Self::Draft),
            "queued" => Ok(Self::Queued),
            "sending" => Ok(Self::Sending),
            "published" => Ok(Self::Published),
            "unknown" => Ok(Self::Unknown),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            unknown => Err(unknown.to_owned()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Post {
    pub id: Uuid,
    pub media_id: Uuid,
    pub channel_id: Uuid,
    pub caption: Option<String>,
    pub parse_mode: Option<String>,
    pub disable_notification: bool,
    pub state: PostState,
    pub scheduled_at: OffsetDateTime,
    pub cadence_slot_at: Option<OffsetDateTime>,
    pub send_generation: i32,
    pub send_token: Option<Uuid>,
    pub send_started_at: Option<OffsetDateTime>,
    pub telegram_message_id: Option<i64>,
    pub published_at: Option<OffsetDateTime>,
    pub error_class: Option<String>,
    pub error_message: Option<String>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub revision: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewPost {
    pub media_id: Uuid,
    pub channel_id: Uuid,
    pub caption: Option<String>,
    pub parse_mode: Option<String>,
    pub disable_notification: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostUpdate {
    pub caption: Option<Option<String>>,
    pub parse_mode: Option<Option<String>>,
    pub disable_notification: Option<bool>,
    pub expected_updated_at: Option<OffsetDateTime>,
    pub expected_revision: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostSchedule {
    pub post_id: Uuid,
    pub requested_at: OffsetDateTime,
    pub request_key: String,
    pub expected_revision: Option<i64>,
}

impl PostSchedule {
    pub fn try_new(
        post_id: Uuid,
        requested_at: OffsetDateTime,
        request_key: impl Into<String>,
    ) -> Result<Self, PublisherValidationError> {
        let request_key = normalize_request_key(request_key.into())?;
        Ok(Self { post_id, requested_at, request_key, expected_revision: None })
    }

    pub const fn with_expected_revision(mut self, expected_revision: i64) -> Self {
        self.expected_revision = Some(expected_revision);
        self
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum QueueDirection {
    Earlier,
    Later,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishClaim {
    pub post: Post,
    pub channel_chat_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedMessage {
    pub post: Post,
    pub channel_chat_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuePost {
    pub id: Uuid,
    pub revision: i64,
    pub scheduled_at: OffsetDateTime,
    pub cadence_slot_at: Option<OffsetDateTime>,
    pub time_zone: String,
    pub caption: Option<String>,
    pub media_kind: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub tags: Vec<String>,
    pub source_url: Option<String>,
    pub storage_chat_id: Option<i64>,
    pub storage_message_id: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PublisherValidationError {
    #[error("publication request key must not be empty")]
    EmptyRequestKey,
    #[error("publication request key must be at most {max} characters")]
    RequestKeyTooLong { max: usize },
    #[error("caption must be at most {max} characters")]
    CaptionTooLong { max: usize },
    #[error("caption contains a disallowed control character")]
    CaptionControlCharacter,
    #[error("invalid parse mode")]
    InvalidParseMode,
}

pub fn normalize_request_key(value: String) -> Result<String, PublisherValidationError> {
    let value = value.trim().to_owned();
    if value.is_empty() {
        return Err(PublisherValidationError::EmptyRequestKey);
    }
    if value.chars().count() > MAX_REQUEST_KEY_LENGTH {
        return Err(PublisherValidationError::RequestKeyTooLong { max: MAX_REQUEST_KEY_LENGTH });
    }
    Ok(value)
}

pub fn validate_caption(caption: &str) -> Result<(), PublisherValidationError> {
    if caption.chars().count() > MAX_CAPTION_LENGTH {
        return Err(PublisherValidationError::CaptionTooLong { max: MAX_CAPTION_LENGTH });
    }
    if caption
        .chars()
        .any(|character| character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
    {
        return Err(PublisherValidationError::CaptionControlCharacter);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_defaults_include_cadence_policy() {
        let channel = NewChannel::try_new(" Main ", -100123).expect("channel should be valid");
        assert_eq!(channel.time_zone, "UTC");
        assert_eq!(channel.interval_minutes, 30);
        assert!(channel.window_start < channel.window_end);
    }

    #[test]
    fn queue_mutable_states_are_editable() {
        assert!(PostState::Draft.is_queue_mutable());
        assert!(PostState::Queued.is_queue_mutable());
        assert!(!PostState::Sending.is_queue_mutable());
        assert!(!PostState::Unknown.is_queue_mutable());
    }

    #[test]
    fn schedule_keys_are_normalized_and_bounded() {
        let schedule = PostSchedule::try_new(Uuid::now_v7(), OffsetDateTime::now_utc(), " key ")
            .expect("key should be valid");
        assert_eq!(schedule.request_key, "key");
        assert!(matches!(
            PostSchedule::try_new(Uuid::now_v7(), OffsetDateTime::now_utc(), " "),
            Err(PublisherValidationError::EmptyRequestKey)
        ));
    }
}
