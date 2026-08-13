//! Ingest request and source submission boundaries for sooqa.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;
use url::Url;
use uuid::Uuid;

const MAX_IDEMPOTENCY_KEY_LENGTH: usize = 255;

/// Durable source metadata produced by an inspection adapter.
///
/// This schema belongs to the Inbox/job boundary. Media adapters produce it,
/// but durable payloads should not depend on the infrastructure-heavy media
/// crate for their serialized value types.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceMediaKind {
    Video,
    Image,
    Audio,
    Animation,
    Unknown,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceInspection {
    pub adapter: String,
    pub source_url: String,
    pub resolved_url: Option<String>,
    pub media_kind: SourceMediaKind,
    pub mime_type: Option<String>,
    pub content_length_bytes: Option<u64>,
    pub title: Option<String>,
    pub metadata: Value,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceDownload {
    pub bytes: u64,
    pub mime_type: Option<String>,
    pub media_kind: SourceMediaKind,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AssetNormalization {
    pub local_work_path: String,
    pub file_size_bytes: u64,
    pub sha256: String,
    pub media_kind: SourceMediaKind,
    pub mime_type: Option<String>,
    pub container: Option<String>,
    pub video_codec: Option<String>,
    pub audio_codec: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub duration_ms: Option<u64>,
    pub bit_rate: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thumbnail: Option<AssetThumbnailNormalization>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AssetThumbnailNormalization {
    pub local_work_path: String,
    pub file_size_bytes: u64,
    pub sha256: String,
    pub mime_type: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct IngestFinalization {
    pub media_id: Uuid,
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IngestKind {
    Url,
    TelegramMessage,
    Upload,
}

impl IngestKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Url => "url",
            Self::TelegramMessage => "telegram_message",
            Self::Upload => "upload",
        }
    }
}

impl TryFrom<&str> for IngestKind {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "url" => Ok(Self::Url),
            "telegram_message" => Ok(Self::TelegramMessage),
            "upload" => Ok(Self::Upload),
            unknown => Err(unknown.to_owned()),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SubmittedVia {
    Api,
    Companion,
    TelegramBot,
}

/// The one follow-up operation requested alongside a media ingest.
///
/// This is intentionally only a durable request at the Inbox boundary. It
/// does not create a post or decide how publication policy will be applied.
#[derive(Debug, Clone, Copy, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestedAction {
    #[default]
    Save,
    Queue,
    PostNow,
}

impl RequestedAction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Save => "save",
            Self::Queue => "queue",
            Self::PostNow => "post_now",
        }
    }
}

impl TryFrom<&str> for RequestedAction {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "save" => Ok(Self::Save),
            "queue" => Ok(Self::Queue),
            "post_now" => Ok(Self::PostNow),
            unknown => Err(unknown.to_owned()),
        }
    }
}

impl SubmittedVia {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Api => "api",
            Self::Companion => "companion",
            Self::TelegramBot => "telegram_bot",
        }
    }
}

impl TryFrom<&str> for SubmittedVia {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "api" => Ok(Self::Api),
            "companion" => Ok(Self::Companion),
            "telegram_bot" => Ok(Self::TelegramBot),
            unknown => Err(unknown.to_owned()),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IngestStatus {
    Received,
    Queued,
    Downloading,
    Probing,
    ExactDedupCheck,
    Normalizing,
    Fingerprinting,
    DuplicatePending,
    Storing,
    Completed,
    FailedRetryable,
    FailedTerminal,
    Cancelled,
}

impl IngestStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Received => "received",
            Self::Queued => "queued",
            Self::Downloading => "downloading",
            Self::Probing => "probing",
            Self::ExactDedupCheck => "exact_dedup_check",
            Self::Normalizing => "normalizing",
            Self::Fingerprinting => "fingerprinting",
            Self::DuplicatePending => "duplicate_pending",
            Self::Storing => "storing",
            Self::Completed => "completed",
            Self::FailedRetryable => "failed_retryable",
            Self::FailedTerminal => "failed_terminal",
            Self::Cancelled => "cancelled",
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::FailedTerminal | Self::Cancelled)
    }

    fn can_transition_to(self, target: Self) -> bool {
        if self == target {
            return true;
        }

        if self.is_terminal() {
            return false;
        }

        if matches!(target, Self::FailedRetryable | Self::FailedTerminal | Self::Cancelled) {
            return true;
        }

        if self == Self::FailedRetryable && target == Self::Queued {
            return true;
        }

        matches!(
            (self, target),
            (Self::Received, Self::Queued)
                | (Self::Queued, Self::Downloading)
                | (Self::Downloading, Self::Probing)
                | (Self::Probing, Self::Normalizing)
                | (Self::Probing, Self::ExactDedupCheck)
                | (Self::ExactDedupCheck, Self::Normalizing)
                | (Self::Normalizing, Self::Fingerprinting)
                | (Self::Normalizing, Self::Storing)
                | (Self::Fingerprinting, Self::Storing)
                | (Self::Fingerprinting, Self::DuplicatePending)
                | (Self::Fingerprinting, Self::Completed)
                | (Self::DuplicatePending, Self::Queued)
                | (Self::DuplicatePending, Self::Storing)
                | (Self::FailedRetryable, Self::Storing)
                | (Self::FailedRetryable, Self::Fingerprinting)
                | (Self::Storing, Self::Fingerprinting)
                | (Self::Storing, Self::Completed)
        )
    }
}

impl TryFrom<&str> for IngestStatus {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "received" => Ok(Self::Received),
            "queued" => Ok(Self::Queued),
            "downloading" => Ok(Self::Downloading),
            "probing" => Ok(Self::Probing),
            "exact_dedup_check" => Ok(Self::ExactDedupCheck),
            "normalizing" => Ok(Self::Normalizing),
            "fingerprinting" => Ok(Self::Fingerprinting),
            "duplicate_pending" => Ok(Self::DuplicatePending),
            "storing" => Ok(Self::Storing),
            "completed" => Ok(Self::Completed),
            "failed_retryable" => Ok(Self::FailedRetryable),
            "failed_terminal" => Ok(Self::FailedTerminal),
            "cancelled" => Ok(Self::Cancelled),
            unknown => Err(unknown.to_owned()),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IngestSubmissionInput {
    pub source_url: String,
    pub submitted_via: SubmittedVia,
    pub submitted_by_admin_id: Option<Uuid>,
    pub page_url: Option<String>,
    pub page_title: Option<String>,
    pub supplied_caption: Option<String>,
    pub supplied_description: Option<String>,
    pub supplied_tags: Vec<String>,
    pub requested_action: RequestedAction,
    pub requested_publish_at: Option<time::OffsetDateTime>,
    pub requested_post_caption: Option<String>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TelegramSubmissionInput {
    pub source_reference: String,
    pub submitted_via: SubmittedVia,
    pub submitted_by_admin_id: Option<Uuid>,
    pub original_input: Value,
    pub supplied_caption: Option<String>,
    pub idempotency_key: Option<String>,
}

impl IngestSubmissionInput {
    pub fn new(source_url: impl Into<String>, submitted_via: SubmittedVia) -> Self {
        Self {
            source_url: source_url.into(),
            submitted_via,
            submitted_by_admin_id: None,
            page_url: None,
            page_title: None,
            supplied_caption: None,
            supplied_description: None,
            supplied_tags: Vec::new(),
            requested_action: RequestedAction::Save,
            requested_publish_at: None,
            requested_post_caption: None,
            idempotency_key: None,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct IngestSubmission {
    pub kind: IngestKind,
    pub submitted_via: SubmittedVia,
    pub submitted_by_admin_id: Option<Uuid>,
    pub original_url: String,
    pub normalized_url: String,
    pub original_input: Value,
    pub page_url: Option<String>,
    pub page_title: Option<String>,
    pub supplied_caption: Option<String>,
    pub supplied_description: Option<String>,
    pub supplied_tags: Vec<String>,
    pub requested_action: RequestedAction,
    pub requested_publish_at: Option<time::OffsetDateTime>,
    pub requested_post_caption: Option<String>,
    pub idempotency_key: Option<String>,
}

impl IngestSubmission {
    pub fn try_new(input: IngestSubmissionInput) -> Result<Self, IngestValidationError> {
        Self::try_new_inner(input, true)
    }

    /// Build a submission before the persistence layer can distinguish a new
    /// request from an idempotent replay. Temporal freshness is checked by
    /// persistence after that distinction; otherwise a replay of an exact
    /// queue request could fail merely because its requested time elapsed.
    pub fn try_new_for_idempotency_lookup(
        input: IngestSubmissionInput,
    ) -> Result<Self, IngestValidationError> {
        Self::try_new_inner(input, false)
    }

    fn try_new_inner(
        input: IngestSubmissionInput,
        enforce_publish_time_freshness: bool,
    ) -> Result<Self, IngestValidationError> {
        let original_url = input.source_url.trim().to_owned();
        let normalized_url = normalize_url(&original_url, "source URL")?;
        let page_url =
            input.page_url.as_deref().map(|value| normalize_url(value, "page URL")).transpose()?;
        let idempotency_key = normalize_idempotency_key(input.idempotency_key)?;

        let mut supplied_tags = Vec::with_capacity(input.supplied_tags.len());
        for tag in input.supplied_tags {
            let tag = tag.trim().to_ascii_lowercase();
            if tag.is_empty() {
                return Err(IngestValidationError::EmptyTag);
            }
            if !supplied_tags.iter().any(|existing| existing == &tag) {
                supplied_tags.push(tag);
            }
        }

        let page_title = normalize_optional_text(input.page_title);
        let supplied_caption = normalize_optional_text(input.supplied_caption);
        let supplied_description = normalize_optional_text(input.supplied_description);
        let requested_post_caption = normalize_optional_text(input.requested_post_caption);
        validate_requested_intent(
            input.requested_action,
            input.requested_publish_at,
            requested_post_caption.as_deref(),
            enforce_publish_time_freshness,
        )?;
        let original_input = json!({
            "url": &original_url,
            "page_url": &page_url,
            "page_title": &page_title,
            "selected_text": &supplied_caption,
            "description": &supplied_description,
            "tags": &supplied_tags,
            "requested_action": input.requested_action.as_str(),
            "requested_publish_at": format_requested_publish_at(input.requested_publish_at),
            "requested_post_caption": &requested_post_caption,
        });

        Ok(Self {
            kind: IngestKind::Url,
            submitted_via: input.submitted_via,
            submitted_by_admin_id: input.submitted_by_admin_id,
            original_url,
            normalized_url,
            original_input,
            page_url,
            page_title,
            supplied_caption,
            supplied_description,
            supplied_tags,
            requested_action: input.requested_action,
            requested_publish_at: input.requested_publish_at,
            requested_post_caption,
            idempotency_key,
        })
    }

    pub fn try_new_telegram(input: TelegramSubmissionInput) -> Result<Self, IngestValidationError> {
        let source_reference = input.source_reference.trim().to_owned();
        if source_reference.is_empty() {
            return Err(IngestValidationError::EmptyUrl("Telegram source reference"));
        }
        let idempotency_key = normalize_idempotency_key(input.idempotency_key)?;
        Ok(Self {
            kind: IngestKind::TelegramMessage,
            submitted_via: input.submitted_via,
            submitted_by_admin_id: input.submitted_by_admin_id,
            original_url: source_reference.clone(),
            normalized_url: source_reference,
            original_input: input.original_input,
            page_url: None,
            page_title: None,
            supplied_caption: normalize_optional_text(input.supplied_caption),
            supplied_description: None,
            supplied_tags: Vec::new(),
            requested_action: RequestedAction::Save,
            requested_publish_at: None,
            requested_post_caption: None,
            idempotency_key,
        })
    }

    pub fn original_input(&self) -> Value {
        self.original_input.clone()
    }

    pub fn request_hash(&self) -> [u8; 32] {
        let serialized = serde_json::to_vec(self).expect("ingest submission must be serializable");
        Sha256::digest(serialized).into()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Ingest {
    pub id: Uuid,
    /// The filesystem workspace for this processing generation. A force-save
    /// receives a new ID so delayed cleanup cannot remove its replacement.
    pub workspace_id: Uuid,
    pub kind: IngestKind,
    pub status: IngestStatus,
    pub submitted_via: SubmittedVia,
    pub submitted_by_admin_id: Option<Uuid>,
    pub original_input: Value,
    pub source_url: String,
    pub page_url: Option<String>,
    pub page_title: Option<String>,
    pub supplied_caption: Option<String>,
    pub supplied_description: Option<String>,
    pub supplied_tags: Vec<String>,
    pub requested_action: RequestedAction,
    pub requested_publish_at: Option<time::OffsetDateTime>,
    pub requested_post_caption: Option<String>,
    pub idempotency_key: Option<String>,
    pub media_id: Option<Uuid>,
    pub force_save: bool,
    pub duplicate_evidence: Option<Value>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub created_at: time::OffsetDateTime,
    pub updated_at: time::OffsetDateTime,
    pub completed_at: Option<time::OffsetDateTime>,
}

impl Ingest {
    pub fn from_submission(id: Uuid, submission: &IngestSubmission) -> Self {
        let now = time::OffsetDateTime::now_utc();
        Self {
            id,
            workspace_id: id,
            kind: submission.kind,
            status: IngestStatus::Received,
            submitted_via: submission.submitted_via,
            submitted_by_admin_id: submission.submitted_by_admin_id,
            original_input: submission.original_input(),
            source_url: submission.normalized_url.clone(),
            page_url: submission.page_url.clone(),
            page_title: submission.page_title.clone(),
            supplied_caption: submission.supplied_caption.clone(),
            supplied_description: submission.supplied_description.clone(),
            supplied_tags: submission.supplied_tags.clone(),
            requested_action: submission.requested_action,
            requested_publish_at: submission.requested_publish_at,
            requested_post_caption: submission.requested_post_caption.clone(),
            idempotency_key: submission.idempotency_key.clone(),
            media_id: None,
            force_save: false,
            duplicate_evidence: None,
            error_code: None,
            error_message: None,
            created_at: now,
            updated_at: now,
            completed_at: None,
        }
    }

    pub fn transition_to(&mut self, target: IngestStatus) -> Result<(), IngestStateError> {
        if !self.status.can_transition_to(target) {
            return Err(IngestStateError { from: self.status, to: target });
        }
        self.status = target;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Error)]
#[error("invalid ingest state transition from {from:?} to {to:?}")]
pub struct IngestStateError {
    pub from: IngestStatus,
    pub to: IngestStatus,
}

#[derive(Debug, Clone, Eq, PartialEq, Error)]
pub enum IngestValidationError {
    #[error("{0} must not be empty")]
    EmptyUrl(&'static str),
    #[error("{0} is invalid")]
    InvalidUrl(&'static str),
    #[error("{0} must use http or https")]
    UnsupportedScheme(&'static str),
    #[error("{0} must include a host")]
    MissingHost(&'static str),
    #[error("{0} must not include credentials")]
    CredentialsNotAllowed(&'static str),
    #[error("idempotency key must not be empty")]
    EmptyIdempotencyKey,
    #[error("idempotency key must be at most {MAX_IDEMPOTENCY_KEY_LENGTH} characters")]
    IdempotencyKeyTooLong,
    #[error("supplied tags must not contain empty values")]
    EmptyTag,
    #[error("requested action is invalid")]
    InvalidRequestedAction,
    #[error("requested publish time is invalid")]
    InvalidRequestedPublishAt,
    #[error("save requests must not include a requested publish time")]
    RequestedPublishAtForbidden,
    #[error("post-now requests must not include a requested publish time")]
    RequestedPublishAtForbiddenForPostNow,
    #[error("requested publish time must be in the future")]
    RequestedPublishAtNotFuture,
    #[error("save requests must not include public post text")]
    RequestedPostCaptionForbidden,
}

fn normalize_url(input: &str, field: &'static str) -> Result<String, IngestValidationError> {
    let input = input.trim();
    if input.is_empty() {
        return Err(IngestValidationError::EmptyUrl(field));
    }

    let mut url = Url::parse(input).map_err(|_| IngestValidationError::InvalidUrl(field))?;
    let scheme = url.scheme().to_ascii_lowercase();
    if !matches!(scheme.as_str(), "http" | "https") {
        return Err(IngestValidationError::UnsupportedScheme(field));
    }
    if url.host_str().is_none() {
        return Err(IngestValidationError::MissingHost(field));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(IngestValidationError::CredentialsNotAllowed(field));
    }

    if url.scheme() != scheme {
        url.set_scheme(&scheme).map_err(|_| IngestValidationError::InvalidUrl(field))?;
    }
    if let Some(host) = url.host_str() {
        let lowercase_host = host.to_ascii_lowercase();
        if host != lowercase_host {
            url.set_host(Some(&lowercase_host))
                .map_err(|_| IngestValidationError::InvalidUrl(field))?;
        }
    }
    if (scheme == "http" && url.port() == Some(80))
        || (scheme == "https" && url.port() == Some(443))
    {
        url.set_port(None).map_err(|_| IngestValidationError::InvalidUrl(field))?;
    }
    url.set_fragment(None);

    let query_pairs: Vec<(String, String)> =
        url.query_pairs().map(|(name, value)| (name.into_owned(), value.into_owned())).collect();
    let filtered_pairs: Vec<(String, String)> =
        query_pairs.iter().filter(|(name, _)| !is_tracking_parameter(name)).cloned().collect();
    if filtered_pairs.len() != query_pairs.len() {
        if filtered_pairs.is_empty() {
            url.set_query(None);
        } else {
            let mut query = url.query_pairs_mut();
            query.clear();
            query.extend_pairs(filtered_pairs.iter().map(|(name, value)| (&**name, &**value)));
        }
    }

    Ok(url.to_string())
}

fn is_tracking_parameter(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name.starts_with("utm_")
        || matches!(name.as_str(), "fbclid" | "gclid" | "dclid" | "msclkid" | "mc_cid" | "mc_eid")
}

fn normalize_idempotency_key(
    value: Option<String>,
) -> Result<Option<String>, IngestValidationError> {
    value
        .map(|value| {
            let value = value.trim().to_owned();
            if value.is_empty() {
                return Err(IngestValidationError::EmptyIdempotencyKey);
            }
            if value.chars().count() > MAX_IDEMPOTENCY_KEY_LENGTH {
                return Err(IngestValidationError::IdempotencyKeyTooLong);
            }
            Ok(value)
        })
        .transpose()
}

fn normalize_optional_text(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let value = value.trim().to_owned();
        (!value.is_empty()).then_some(value)
    })
}

fn format_requested_publish_at(value: Option<time::OffsetDateTime>) -> Option<String> {
    value.map(|value| {
        value
            .format(&time::format_description::well_known::Rfc3339)
            .expect("RFC3339 formatting should be available")
    })
}

fn validate_requested_intent(
    action: RequestedAction,
    requested_publish_at: Option<time::OffsetDateTime>,
    requested_post_caption: Option<&str>,
    enforce_publish_time_freshness: bool,
) -> Result<(), IngestValidationError> {
    match action {
        RequestedAction::Save => {
            if requested_publish_at.is_some() {
                return Err(IngestValidationError::RequestedPublishAtForbidden);
            }
            if requested_post_caption.is_some() {
                return Err(IngestValidationError::RequestedPostCaptionForbidden);
            }
        }
        RequestedAction::Queue => {
            if enforce_publish_time_freshness
                && let Some(requested_publish_at) = requested_publish_at
                && requested_publish_at <= time::OffsetDateTime::now_utc()
            {
                return Err(IngestValidationError::RequestedPublishAtNotFuture);
            }
        }
        RequestedAction::PostNow => {
            if requested_publish_at.is_some() {
                return Err(IngestValidationError::RequestedPublishAtForbiddenForPostNow);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn submission(url: &str) -> IngestSubmission {
        IngestSubmission::try_new(IngestSubmissionInput::new(url, SubmittedVia::Api))
            .expect("submission should be valid")
    }

    #[test]
    fn normalizes_safe_url_parts_and_allowlisted_tracking_parameters() {
        let value = submission(
            "HTTPS://Example.COM:443/video?id=123&utm_source=newsletter&fbclid=abc#comments",
        );

        assert_eq!(value.normalized_url, "https://example.com/video?id=123");
    }

    #[test]
    fn url_normalization_is_idempotent() {
        let first = submission("https://example.com/video?utm_campaign=x&id=123");
        let second = submission(&first.normalized_url);

        assert_eq!(first.normalized_url, second.normalized_url);
    }

    #[test]
    fn rejects_unsafe_url_forms() {
        assert!(matches!(
            IngestSubmission::try_new(IngestSubmissionInput::new(
                "ftp://example.com/a",
                SubmittedVia::Api
            )),
            Err(IngestValidationError::UnsupportedScheme("source URL"))
        ));
        assert!(matches!(
            IngestSubmission::try_new(IngestSubmissionInput::new(
                "https://user:pass@example.com/a",
                SubmittedVia::Api
            )),
            Err(IngestValidationError::CredentialsNotAllowed("source URL"))
        ));
    }

    #[test]
    fn normalizes_tags_and_optional_text() {
        let mut input = IngestSubmissionInput::new("https://example.com", SubmittedVia::Companion);
        input.supplied_tags = vec![" Cats ".to_owned(), "cats".to_owned(), "FUN".to_owned()];
        input.page_title = Some(" Title ".to_owned());
        input.supplied_caption = Some(" Caption ".to_owned());
        input.supplied_description = Some(" Internal description ".to_owned());

        let value = IngestSubmission::try_new(input).expect("submission should be valid");

        assert_eq!(value.supplied_tags, ["cats", "fun"]);
        assert_eq!(value.page_title.as_deref(), Some("Title"));
        assert_eq!(value.supplied_caption.as_deref(), Some("Caption"));
        assert_eq!(value.supplied_description.as_deref(), Some("Internal description"));
    }

    #[test]
    fn requested_intent_is_typed_and_keeps_public_text_separate() {
        let mut input = IngestSubmissionInput::new("https://example.com/video", SubmittedVia::Api);
        input.supplied_caption = Some("selected text".to_owned());
        input.supplied_description = Some("internal description".to_owned());
        input.supplied_tags = vec!["cats".to_owned()];
        input.requested_action = RequestedAction::Queue;
        input.requested_publish_at =
            Some(time::OffsetDateTime::now_utc() + time::Duration::minutes(5));
        input.requested_post_caption = Some("public post text".to_owned());

        let value = IngestSubmission::try_new(input).expect("requested intent should be valid");

        assert_eq!(value.requested_action, RequestedAction::Queue);
        assert!(value.requested_publish_at.is_some());
        assert_eq!(value.requested_post_caption.as_deref(), Some("public post text"));
        assert_eq!(value.supplied_caption.as_deref(), Some("selected text"));
        assert_eq!(value.supplied_description.as_deref(), Some("internal description"));
        assert_eq!(value.original_input["requested_action"], "queue");
        assert_eq!(value.original_input["requested_post_caption"], "public post text");
        assert_eq!(value.original_input["selected_text"], "selected text");
        assert_eq!(value.original_input["description"], "internal description");
    }

    #[test]
    fn requested_intent_defaults_to_save_for_legacy_submissions() {
        let value = submission("https://example.com/video");

        assert_eq!(value.requested_action, RequestedAction::Save);
        assert!(value.requested_publish_at.is_none());
        assert!(value.requested_post_caption.is_none());
    }

    #[test]
    fn requested_intent_allows_cadence_queue_and_post_now() {
        let mut queue = IngestSubmissionInput::new("https://example.com/queue", SubmittedVia::Api);
        queue.requested_action = RequestedAction::Queue;
        let queue = IngestSubmission::try_new(queue).expect("normal queue should be valid");
        assert_eq!(queue.requested_action, RequestedAction::Queue);
        assert!(queue.requested_publish_at.is_none());

        let mut post_now =
            IngestSubmissionInput::new("https://example.com/post-now", SubmittedVia::Api);
        post_now.requested_action = RequestedAction::PostNow;
        post_now.requested_post_caption = Some("publish this".to_owned());
        let post_now = IngestSubmission::try_new(post_now).expect("post-now should be valid");
        assert_eq!(post_now.requested_action, RequestedAction::PostNow);
        assert_eq!(post_now.requested_post_caption.as_deref(), Some("publish this"));
    }

    #[test]
    fn requested_intent_rejects_invalid_action_combinations_and_past_times() {
        let mut save_with_caption =
            IngestSubmissionInput::new("https://example.com", SubmittedVia::Api);
        save_with_caption.requested_post_caption = Some("public".to_owned());
        assert!(matches!(
            IngestSubmission::try_new(save_with_caption),
            Err(IngestValidationError::RequestedPostCaptionForbidden)
        ));

        let mut save_with_time =
            IngestSubmissionInput::new("https://example.com", SubmittedVia::Api);
        save_with_time.requested_publish_at =
            Some(time::OffsetDateTime::now_utc() + time::Duration::minutes(5));
        assert!(matches!(
            IngestSubmission::try_new(save_with_time),
            Err(IngestValidationError::RequestedPublishAtForbidden)
        ));

        let mut post_now_with_time =
            IngestSubmissionInput::new("https://example.com", SubmittedVia::Api);
        post_now_with_time.requested_action = RequestedAction::PostNow;
        post_now_with_time.requested_publish_at =
            Some(time::OffsetDateTime::now_utc() + time::Duration::minutes(5));
        assert!(matches!(
            IngestSubmission::try_new(post_now_with_time),
            Err(IngestValidationError::RequestedPublishAtForbiddenForPostNow)
        ));

        let mut queue_in_the_past =
            IngestSubmissionInput::new("https://example.com", SubmittedVia::Api);
        queue_in_the_past.requested_action = RequestedAction::Queue;
        queue_in_the_past.requested_publish_at =
            Some(time::OffsetDateTime::now_utc() - time::Duration::minutes(1));
        assert!(matches!(
            IngestSubmission::try_new(queue_in_the_past),
            Err(IngestValidationError::RequestedPublishAtNotFuture)
        ));
    }

    #[test]
    fn telegram_submission_preserves_media_metadata_and_kind() {
        let submission = IngestSubmission::try_new_telegram(TelegramSubmissionInput {
            source_reference: " telegram://42/99 ".to_owned(),
            submitted_via: SubmittedVia::TelegramBot,
            submitted_by_admin_id: None,
            original_input: json!({
                "telegram_message_id": 99,
                "telegram_file_unique_id": "unique-file",
                "media_kind": "video",
            }),
            supplied_caption: Some(" caption ".to_owned()),
            idempotency_key: Some("telegram:update:11:v1".to_owned()),
        })
        .expect("Telegram submission should be valid");

        assert_eq!(submission.kind, IngestKind::TelegramMessage);
        assert_eq!(submission.original_url, "telegram://42/99");
        assert_eq!(submission.normalized_url, "telegram://42/99");
        assert_eq!(submission.supplied_caption.as_deref(), Some("caption"));
        assert_eq!(submission.original_input["telegram_file_unique_id"], "unique-file");
    }

    #[test]
    fn state_machine_allows_pipeline_and_retry_transitions() {
        let mut request =
            Ingest::from_submission(Uuid::now_v7(), &submission("https://example.com"));

        for status in [
            IngestStatus::Queued,
            IngestStatus::Downloading,
            IngestStatus::Probing,
            IngestStatus::ExactDedupCheck,
            IngestStatus::Normalizing,
            IngestStatus::Fingerprinting,
            IngestStatus::Storing,
            IngestStatus::Completed,
        ] {
            request.transition_to(status).expect("pipeline transition should be valid");
        }

        let mut storing =
            Ingest::from_submission(Uuid::now_v7(), &submission("https://example.com"));
        for status in [
            IngestStatus::Queued,
            IngestStatus::Downloading,
            IngestStatus::Probing,
            IngestStatus::Normalizing,
            IngestStatus::Storing,
        ] {
            storing.transition_to(status).expect("normalization handoff should be valid");
        }
        storing
            .transition_to(IngestStatus::Fingerprinting)
            .expect("stored content should enter fingerprinting");
        storing
            .transition_to(IngestStatus::Storing)
            .expect("fingerprinted content should wait for storage");
        storing.transition_to(IngestStatus::Completed).expect("stored content should complete");

        let mut duplicate =
            Ingest::from_submission(Uuid::now_v7(), &submission("https://example.com"));
        for status in [
            IngestStatus::Queued,
            IngestStatus::Downloading,
            IngestStatus::Probing,
            IngestStatus::Normalizing,
            IngestStatus::Fingerprinting,
            IngestStatus::DuplicatePending,
        ] {
            duplicate.transition_to(status).expect("duplicate review transition should be valid");
        }
        duplicate
            .transition_to(IngestStatus::Storing)
            .expect("accepted duplicate should join storage");

        assert!(request.transition_to(IngestStatus::Queued).is_err());

        let mut direct_normalizing =
            Ingest::from_submission(Uuid::now_v7(), &submission("https://example.com"));
        for status in [
            IngestStatus::Queued,
            IngestStatus::Downloading,
            IngestStatus::Probing,
            IngestStatus::Normalizing,
        ] {
            direct_normalizing
                .transition_to(status)
                .expect("direct probe-to-normalize transition should be valid");
        }

        let mut retrying =
            Ingest::from_submission(Uuid::now_v7(), &submission("https://example.com"));
        retrying.transition_to(IngestStatus::Queued).expect("request should queue");
        retrying
            .transition_to(IngestStatus::FailedRetryable)
            .expect("request should enter retryable failure");
        retrying
            .transition_to(IngestStatus::Queued)
            .expect("retryable failure should return to queue");
    }

    #[test]
    fn terminal_states_cannot_transition() {
        let mut request =
            Ingest::from_submission(Uuid::now_v7(), &submission("https://example.com"));
        request.transition_to(IngestStatus::Cancelled).expect("request should be cancellable");

        assert!(request.transition_to(IngestStatus::Queued).is_err());
    }
}
