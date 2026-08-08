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
    SimilarityCheck,
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
            Self::SimilarityCheck => "similarity_check",
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
                | (Self::Fingerprinting, Self::SimilarityCheck)
                | (Self::SimilarityCheck, Self::Storing)
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
            "similarity_check" => Ok(Self::SimilarityCheck),
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
    pub supplied_tags: Vec<String>,
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
            supplied_tags: Vec::new(),
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
    pub supplied_tags: Vec<String>,
    pub idempotency_key: Option<String>,
}

impl IngestSubmission {
    pub fn try_new(input: IngestSubmissionInput) -> Result<Self, IngestValidationError> {
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
        let original_input = json!({
            "url": &original_url,
            "page_url": &page_url,
            "page_title": &page_title,
            "selected_text": &supplied_caption,
            "tags": &supplied_tags,
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
            supplied_tags,
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
            supplied_tags: Vec::new(),
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
pub struct IngestRequest {
    pub id: Uuid,
    pub kind: IngestKind,
    pub status: IngestStatus,
    pub submitted_via: SubmittedVia,
    pub submitted_by_admin_id: Option<Uuid>,
    pub original_input: Value,
    pub source_url: String,
    pub page_url: Option<String>,
    pub page_title: Option<String>,
    pub supplied_caption: Option<String>,
    pub supplied_tags: Vec<String>,
    pub idempotency_key: Option<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub created_at: time::OffsetDateTime,
    pub updated_at: time::OffsetDateTime,
    pub completed_at: Option<time::OffsetDateTime>,
}

impl IngestRequest {
    pub fn from_submission(id: Uuid, submission: &IngestSubmission) -> Self {
        let now = time::OffsetDateTime::now_utc();
        Self {
            id,
            kind: submission.kind,
            status: IngestStatus::Received,
            submitted_via: submission.submitted_via,
            submitted_by_admin_id: submission.submitted_by_admin_id,
            original_input: submission.original_input(),
            source_url: submission.normalized_url.clone(),
            page_url: submission.page_url.clone(),
            page_title: submission.page_title.clone(),
            supplied_caption: submission.supplied_caption.clone(),
            supplied_tags: submission.supplied_tags.clone(),
            idempotency_key: submission.idempotency_key.clone(),
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

        let value = IngestSubmission::try_new(input).expect("submission should be valid");

        assert_eq!(value.supplied_tags, ["cats", "fun"]);
        assert_eq!(value.page_title.as_deref(), Some("Title"));
        assert_eq!(value.supplied_caption.as_deref(), Some("Caption"));
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
            IngestRequest::from_submission(Uuid::now_v7(), &submission("https://example.com"));

        for status in [
            IngestStatus::Queued,
            IngestStatus::Downloading,
            IngestStatus::Probing,
            IngestStatus::ExactDedupCheck,
            IngestStatus::Normalizing,
            IngestStatus::Fingerprinting,
            IngestStatus::SimilarityCheck,
            IngestStatus::Storing,
            IngestStatus::Completed,
        ] {
            request.transition_to(status).expect("pipeline transition should be valid");
        }

        assert!(request.transition_to(IngestStatus::Queued).is_err());

        let mut direct_normalizing =
            IngestRequest::from_submission(Uuid::now_v7(), &submission("https://example.com"));
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
            IngestRequest::from_submission(Uuid::now_v7(), &submission("https://example.com"));
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
            IngestRequest::from_submission(Uuid::now_v7(), &submission("https://example.com"));
        request.transition_to(IngestStatus::Cancelled).expect("request should be cancellable");

        assert!(request.transition_to(IngestStatus::Queued).is_err());
    }
}
