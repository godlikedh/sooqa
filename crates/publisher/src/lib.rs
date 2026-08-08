//! Draft, schedule, policy, and publication boundaries for sooqa.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

const MAX_CHANNEL_NAME_LENGTH: usize = 128;
const MAX_IDEMPOTENCY_KEY_LENGTH: usize = 255;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TargetChannel {
    pub id: Uuid,
    pub name: String,
    pub telegram_chat_id: i64,
    pub is_enabled: bool,
    pub default_parse_mode: Option<String>,
    pub default_disable_notification: bool,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTargetChannel {
    pub name: String,
    pub telegram_chat_id: i64,
    pub default_parse_mode: Option<String>,
    pub default_disable_notification: bool,
}

impl NewTargetChannel {
    pub fn try_new(
        name: impl Into<String>,
        telegram_chat_id: i64,
    ) -> Result<Self, TargetChannelValidationError> {
        let name = name.into().trim().to_owned();
        if name.is_empty() {
            return Err(TargetChannelValidationError::EmptyName);
        }
        if name.chars().count() > MAX_CHANNEL_NAME_LENGTH {
            return Err(TargetChannelValidationError::NameTooLong { max: MAX_CHANNEL_NAME_LENGTH });
        }
        if telegram_chat_id >= 0 {
            return Err(TargetChannelValidationError::InvalidTelegramChatId(telegram_chat_id));
        }
        Ok(Self {
            name,
            telegram_chat_id,
            default_parse_mode: None,
            default_disable_notification: false,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TargetChannelValidationError {
    #[error("target channel name must not be empty")]
    EmptyName,
    #[error("target channel name must be at most {max} characters")]
    NameTooLong { max: usize },
    #[error("Telegram target chat ID must be negative, got {0}")]
    InvalidTelegramChatId(i64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CooldownViolation {
    Warn,
    Block,
    Allow,
}

impl CooldownViolation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Warn => "warn",
            Self::Block => "block",
            Self::Allow => "allow",
        }
    }
}

impl TryFrom<&str> for CooldownViolation {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "warn" => Ok(Self::Warn),
            "block" => Ok(Self::Block),
            "allow" => Ok(Self::Allow),
            unknown => Err(unknown.to_owned()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChannelPolicy {
    pub target_channel_id: Uuid,
    pub minimum_post_interval_seconds: u64,
    pub same_content_cooldown_seconds: u64,
    pub similar_content_cooldown_seconds: u64,
    pub similarity_threshold: f64,
    pub on_cooldown_violation: CooldownViolation,
    pub allowed_windows_json: Value,
    pub max_posts_per_day: Option<u32>,
    pub jitter_seconds: u64,
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewChannelPolicy {
    pub target_channel_id: Uuid,
    pub minimum_post_interval_seconds: u64,
    pub same_content_cooldown_seconds: u64,
    pub similar_content_cooldown_seconds: u64,
    pub similarity_threshold: f64,
    pub on_cooldown_violation: CooldownViolation,
    pub allowed_windows_json: Value,
    pub max_posts_per_day: Option<u32>,
    pub jitter_seconds: u64,
}

impl NewChannelPolicy {
    pub fn default_for(target_channel_id: Uuid) -> Self {
        Self {
            target_channel_id,
            minimum_post_interval_seconds: 0,
            same_content_cooldown_seconds: 0,
            similar_content_cooldown_seconds: 0,
            similarity_threshold: 0.75,
            on_cooldown_violation: CooldownViolation::Warn,
            allowed_windows_json: Value::Array(Vec::new()),
            max_posts_per_day: None,
            jitter_seconds: 0,
        }
    }

    pub fn validate(&self) -> Result<(), PublisherValidationError> {
        if !self.similarity_threshold.is_finite()
            || !(0.0..=1.0).contains(&self.similarity_threshold)
        {
            return Err(PublisherValidationError::InvalidSimilarityThreshold(
                self.similarity_threshold,
            ));
        }
        if self.max_posts_per_day == Some(0) {
            return Err(PublisherValidationError::InvalidDailyLimit);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostDraftStatus {
    Editing,
    Ready,
    Scheduled,
    Published,
    Cancelled,
}

impl PostDraftStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Editing => "editing",
            Self::Ready => "ready",
            Self::Scheduled => "scheduled",
            Self::Published => "published",
            Self::Cancelled => "cancelled",
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Published | Self::Cancelled)
    }

    pub fn can_transition_to(self, target: Self) -> bool {
        if self == target {
            return true;
        }
        matches!(
            (self, target),
            (Self::Editing, Self::Ready | Self::Cancelled)
                | (Self::Ready, Self::Editing | Self::Scheduled | Self::Cancelled)
                | (Self::Scheduled, Self::Published | Self::Cancelled)
        )
    }
}

impl TryFrom<&str> for PostDraftStatus {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "editing" => Ok(Self::Editing),
            "ready" => Ok(Self::Ready),
            "scheduled" => Ok(Self::Scheduled),
            "published" => Ok(Self::Published),
            "cancelled" => Ok(Self::Cancelled),
            unknown => Err(unknown.to_owned()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PostDraft {
    pub id: Uuid,
    pub content_item_id: Uuid,
    pub asset_id: Uuid,
    pub target_channel_id: Uuid,
    pub caption: Option<String>,
    pub parse_mode: Option<String>,
    pub status: PostDraftStatus,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewPostDraft {
    pub content_item_id: Uuid,
    pub asset_id: Uuid,
    pub target_channel_id: Uuid,
    pub caption: Option<String>,
    pub parse_mode: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostDraftUpdate {
    pub caption: Option<Option<String>>,
    pub parse_mode: Option<Option<String>>,
    pub status: Option<PostDraftStatus>,
    pub expected_updated_at: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationScheduleStatus {
    Pending,
    Queued,
    Publishing,
    Published,
    Failed,
    Unknown,
    Cancelled,
}

impl PublicationScheduleStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Queued => "queued",
            Self::Publishing => "publishing",
            Self::Published => "published",
            Self::Failed => "failed",
            Self::Unknown => "unknown",
            Self::Cancelled => "cancelled",
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Published | Self::Cancelled)
    }

    pub fn can_transition_to(self, target: Self) -> bool {
        if self == target {
            return true;
        }
        matches!(
            (self, target),
            (Self::Pending, Self::Queued | Self::Publishing | Self::Cancelled)
                | (Self::Queued, Self::Publishing | Self::Cancelled)
                | (Self::Publishing, Self::Published | Self::Failed | Self::Unknown)
                | (Self::Failed, Self::Queued | Self::Cancelled)
                | (Self::Unknown, Self::Queued | Self::Cancelled)
        )
    }
}

impl TryFrom<&str> for PublicationScheduleStatus {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "pending" => Ok(Self::Pending),
            "queued" => Ok(Self::Queued),
            "publishing" => Ok(Self::Publishing),
            "published" => Ok(Self::Published),
            "failed" => Ok(Self::Failed),
            "unknown" => Ok(Self::Unknown),
            "cancelled" => Ok(Self::Cancelled),
            unknown => Err(unknown.to_owned()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicationSchedule {
    pub id: Uuid,
    pub post_draft_id: Uuid,
    pub status: PublicationScheduleStatus,
    pub publish_at: OffsetDateTime,
    pub not_before: Option<OffsetDateTime>,
    pub not_after: Option<OffsetDateTime>,
    pub priority: i32,
    pub cooldown_override: Option<bool>,
    pub idempotency_key: String,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationScheduleScope {
    Schedule,
    PublishNow,
}

impl PublicationScheduleScope {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Schedule => "schedule",
            Self::PublishNow => "publish_now",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewPublicationSchedule {
    pub post_draft_id: Uuid,
    pub publish_at: OffsetDateTime,
    pub not_before: Option<OffsetDateTime>,
    pub not_after: Option<OffsetDateTime>,
    pub priority: i32,
    pub cooldown_override: Option<bool>,
    pub idempotency_key: String,
}

impl NewPublicationSchedule {
    pub fn try_new(
        post_draft_id: Uuid,
        publish_at: OffsetDateTime,
        idempotency_key: impl Into<String>,
    ) -> Result<Self, PublisherValidationError> {
        let idempotency_key = idempotency_key.into().trim().to_owned();
        if idempotency_key.is_empty() {
            return Err(PublisherValidationError::EmptyIdempotencyKey);
        }
        if idempotency_key.chars().count() > MAX_IDEMPOTENCY_KEY_LENGTH {
            return Err(PublisherValidationError::IdempotencyKeyTooLong {
                max: MAX_IDEMPOTENCY_KEY_LENGTH,
            });
        }
        Ok(Self {
            post_draft_id,
            publish_at,
            not_before: None,
            not_after: None,
            priority: 0,
            cooldown_override: None,
            idempotency_key,
        })
    }

    pub fn validate(&self) -> Result<(), PublisherValidationError> {
        if self.idempotency_key.trim().is_empty() {
            return Err(PublisherValidationError::EmptyIdempotencyKey);
        }
        if self.idempotency_key.chars().count() > MAX_IDEMPOTENCY_KEY_LENGTH {
            return Err(PublisherValidationError::IdempotencyKeyTooLong {
                max: MAX_IDEMPOTENCY_KEY_LENGTH,
            });
        }
        if let (Some(not_before), Some(not_after)) = (self.not_before, self.not_after)
            && not_before > not_after
        {
            return Err(PublisherValidationError::InvalidScheduleWindow);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationAttemptStatus {
    Running,
    Succeeded,
    Failed,
    Unknown,
}

impl PublicationAttemptStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Unknown => "unknown",
        }
    }
}

impl TryFrom<&str> for PublicationAttemptStatus {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "unknown" => Ok(Self::Unknown),
            unknown => Err(unknown.to_owned()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicationAttempt {
    pub id: Uuid,
    pub publication_schedule_id: Uuid,
    pub attempt_number: i32,
    pub status: PublicationAttemptStatus,
    pub started_at: OffsetDateTime,
    pub finished_at: Option<OffsetDateTime>,
    pub telegram_request_key: Option<String>,
    pub error_class: Option<String>,
    pub error_message: Option<String>,
    pub response_json: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublishedPost {
    pub id: Uuid,
    pub publication_schedule_id: Uuid,
    pub content_item_id: Uuid,
    pub asset_id: Uuid,
    pub target_channel_id: Uuid,
    pub telegram_chat_id: i64,
    pub telegram_message_id: i64,
    pub caption_snapshot: Option<String>,
    pub published_at: OffsetDateTime,
    pub status: PublishedPostStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicationCompletion {
    pub attempt: PublicationAttempt,
    pub published_post: PublishedPost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublishedPostStatus {
    Active,
    Edited,
    Deleted,
    Unknown,
}

impl PublishedPostStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Edited => "edited",
            Self::Deleted => "deleted",
            Self::Unknown => "unknown",
        }
    }
}

impl TryFrom<&str> for PublishedPostStatus {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "active" => Ok(Self::Active),
            "edited" => Ok(Self::Edited),
            "deleted" => Ok(Self::Deleted),
            "unknown" => Ok(Self::Unknown),
            unknown => Err(unknown.to_owned()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Error)]
pub enum PublisherValidationError {
    #[error("publication idempotency key must not be empty")]
    EmptyIdempotencyKey,
    #[error("publication idempotency key must be at most {max} characters")]
    IdempotencyKeyTooLong { max: usize },
    #[error("publication schedule window is invalid")]
    InvalidScheduleWindow,
    #[error("daily publication limit must be greater than zero")]
    InvalidDailyLimit,
    #[error("similarity threshold must be between 0 and 1, got {0}")]
    InvalidSimilarityThreshold(f64),
    #[error("invalid {entity} status transition from {from} to {to}")]
    InvalidStatusTransition { entity: &'static str, from: String, to: String },
}

pub fn transition_post_draft_status(
    current: PostDraftStatus,
    target: PostDraftStatus,
) -> Result<PostDraftStatus, PublisherValidationError> {
    if current.can_transition_to(target) {
        Ok(target)
    } else {
        Err(PublisherValidationError::InvalidStatusTransition {
            entity: "post draft",
            from: current.as_str().to_owned(),
            to: target.as_str().to_owned(),
        })
    }
}

pub fn transition_publication_schedule_status(
    current: PublicationScheduleStatus,
    target: PublicationScheduleStatus,
) -> Result<PublicationScheduleStatus, PublisherValidationError> {
    if current.can_transition_to(target) {
        Ok(target)
    } else {
        Err(PublisherValidationError::InvalidStatusTransition {
            entity: "publication schedule",
            from: current.as_str().to_owned(),
            to: target.as_str().to_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_channel_normalizes_name_and_rejects_public_chat_ids() {
        let channel = NewTargetChannel::try_new("  Main channel ", -100123).expect("valid channel");
        assert_eq!(channel.name, "Main channel");
        assert!(matches!(
            NewTargetChannel::try_new("main", 123),
            Err(TargetChannelValidationError::InvalidTelegramChatId(123))
        ));
    }

    #[test]
    fn draft_and_schedule_state_machines_allow_only_intended_transitions() {
        assert_eq!(
            transition_post_draft_status(PostDraftStatus::Ready, PostDraftStatus::Scheduled)
                .expect("ready draft should schedule"),
            PostDraftStatus::Scheduled
        );
        assert!(
            transition_post_draft_status(PostDraftStatus::Published, PostDraftStatus::Editing)
                .is_err()
        );
        assert_eq!(
            transition_publication_schedule_status(
                PublicationScheduleStatus::Failed,
                PublicationScheduleStatus::Queued,
            )
            .expect("failed publication should be retryable"),
            PublicationScheduleStatus::Queued
        );
        assert!(
            transition_publication_schedule_status(
                PublicationScheduleStatus::Published,
                PublicationScheduleStatus::Queued,
            )
            .is_err()
        );
        assert_eq!(
            transition_publication_schedule_status(
                PublicationScheduleStatus::Publishing,
                PublicationScheduleStatus::Unknown,
            )
            .expect("ambiguous publication should be preserved"),
            PublicationScheduleStatus::Unknown
        );
        assert!(
            transition_publication_schedule_status(
                PublicationScheduleStatus::Publishing,
                PublicationScheduleStatus::Cancelled,
            )
            .is_err()
        );
    }

    #[test]
    fn schedule_validation_rejects_bad_windows_and_keys() {
        let now = OffsetDateTime::now_utc();
        let mut schedule = NewPublicationSchedule::try_new(Uuid::now_v7(), now, " schedule-key ")
            .expect("key should be normalized");
        assert_eq!(schedule.idempotency_key, "schedule-key");
        schedule.not_before = Some(now + time::Duration::seconds(10));
        schedule.not_after = Some(now);
        assert_eq!(schedule.validate(), Err(PublisherValidationError::InvalidScheduleWindow));
        schedule.not_before = None;
        schedule.not_after = None;
        schedule.idempotency_key = " ".to_owned();
        assert_eq!(schedule.validate(), Err(PublisherValidationError::EmptyIdempotencyKey));
        assert!(matches!(
            NewPublicationSchedule::try_new(Uuid::now_v7(), now, " "),
            Err(PublisherValidationError::EmptyIdempotencyKey)
        ));
    }

    #[test]
    fn policy_validation_rejects_invalid_threshold_and_daily_limit() {
        let mut policy = NewChannelPolicy::default_for(Uuid::now_v7());
        policy.similarity_threshold = 1.1;
        assert!(matches!(
            policy.validate(),
            Err(PublisherValidationError::InvalidSimilarityThreshold(_))
        ));
        policy.similarity_threshold = 0.75;
        policy.max_posts_per_day = Some(0);
        assert_eq!(policy.validate(), Err(PublisherValidationError::InvalidDailyLimit));
    }
}
