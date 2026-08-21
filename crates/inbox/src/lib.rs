//! Ingest request and source submission boundaries for sooqa.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;
use url::Url;
use uuid::Uuid;

const MAX_IDEMPOTENCY_KEY_LENGTH: usize = 255;
/// Must remain aligned with the Publisher's Telegram caption contract.
pub const MAX_REQUESTED_POST_CAPTION_LENGTH: usize = 1_024;

/// The only serialized ingest-data format written by the current code.
///
/// `input_json` is deliberately kept as one column, but its shape is owned by
/// this module rather than by individual persistence and worker call sites.
pub const INGEST_DATA_VERSION: u16 = 1;
const MAX_INGEST_DATA_BYTES: usize = 256 * 1024;
const DUPLICATE_DECISION_MARKER_KEY: &str = "_sooqa_duplicate_decision_v1";

fn is_envelope_key(key: &str) -> bool {
    matches!(
        key,
        "version"
            | "source"
            | "inspection"
            | "download"
            | "probe"
            | "probed_media_kind"
            | "normalization"
            | "finalization"
            | DUPLICATE_DECISION_MARKER_KEY
    )
}

fn validate_extensions(
    extensions: &BTreeMap<String, OpaqueIngestValue>,
    scope: &str,
) -> Result<(), IngestDataError> {
    if let Some(key) = extensions.keys().find(|key| {
        if scope == "source" {
            matches!(
                key.as_str(),
                "url"
                    | "page_url"
                    | "page_title"
                    | "selected_text"
                    | "description"
                    | "tags"
                    | "requested_action"
                    | "requested_publish_at"
                    | "requested_post_caption"
                    | "source_type"
                    | "telegram_update_id"
                    | "telegram_chat_id"
                    | "telegram_message_id"
                    | "telegram_user_id"
                    | "telegram_file_id"
                    | "telegram_file_unique_id"
                    | "telegram_workspace_id"
                    | "file_size"
                    | "mime_type"
                    | "file_name"
                    | "media_kind"
            )
        } else {
            is_envelope_key(key)
        }
    }) {
        return Err(IngestDataError::Malformed(format!(
            "{scope} extension uses reserved key `{key}`"
        )));
    }
    Ok(())
}

/// A bounded opaque value retained only for adapter metadata and unknown
/// fields from a newer envelope. Pipeline state itself is represented by the
/// typed fields below.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OpaqueIngestValue(Value);

impl OpaqueIngestValue {
    pub fn new(value: Value) -> Self {
        Self(value)
    }

    pub fn as_value(&self) -> &Value {
        &self.0
    }

    pub fn into_value(self) -> Value {
        self.0
    }
}

/// Typed source-side fields captured at ingest creation. The database
/// columns remain authoritative for request identity and publication intent;
/// these fields preserve the original adapter input for provenance and
/// reconstruction.
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct IngestSourceData {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_action: Option<RequestedAction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_publish_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_post_caption: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub telegram_update_id: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub telegram_chat_id: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub telegram_message_id: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub telegram_user_id: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub telegram_file_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub telegram_file_unique_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub telegram_workspace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_size: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_kind: Option<SourceMediaKind>,
    /// Adapter-specific source fields not needed by the core pipeline.
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: BTreeMap<String, OpaqueIngestValue>,
}

/// Probe JSON is produced by the media boundary, so the inbox crate keeps it
/// opaque at the serialization boundary while exposing typed decoding to the
/// worker. This prevents persistence code from depending on ffprobe's crate.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IngestProbe(Value);

impl IngestProbe {
    pub fn from_value(value: Value) -> Self {
        Self(value)
    }

    pub fn as_value(&self) -> &Value {
        &self.0
    }

    pub fn decode<T: for<'de> Deserialize<'de>>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_value(self.0.clone())
    }

    pub fn media_kind(&self) -> Option<SourceMediaKind> {
        let container = self.0.get("container_format").and_then(Value::as_str);
        let codecs = self
            .0
            .get("streams")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|stream| stream.get("kind").and_then(Value::as_str) == Some("video"))
            .filter_map(|stream| stream.get("codec").and_then(Value::as_str))
            .collect::<Vec<_>>();
        let container = container.map(str::to_ascii_lowercase);
        let is_gif = container.as_deref().is_some_and(|value| value.contains("gif"))
            || codecs.iter().any(|value| value.to_ascii_lowercase().contains("gif"));
        if is_gif {
            return Some(SourceMediaKind::Animation);
        }
        let is_image_container = container.as_deref().is_some_and(|value| {
            ["image2", "png", "jpeg", "jpg", "webp", "avif", "mjpeg"]
                .iter()
                .any(|format| value.contains(format))
        });
        let is_image_codec = codecs
            .iter()
            .any(|value| ["png", "webp"].iter().any(|format| value.eq_ignore_ascii_case(format)))
            || (container.is_none()
                && codecs.iter().any(|value| value.eq_ignore_ascii_case("mjpeg")));
        if is_image_container || is_image_codec {
            return Some(SourceMediaKind::Image);
        }
        let streams = self.0.get("streams").and_then(Value::as_array);
        if streams.is_some_and(|streams| {
            streams.iter().any(|stream| stream.get("kind").and_then(Value::as_str) == Some("video"))
        }) {
            return Some(SourceMediaKind::Video);
        }
        if streams.is_some_and(|streams| {
            streams.iter().any(|stream| stream.get("kind").and_then(Value::as_str) == Some("audio"))
        }) {
            return Some(SourceMediaKind::Audio);
        }
        None
    }

    pub fn image_format(&self) -> Option<&str> {
        self.0.get("container_format").and_then(Value::as_str).or_else(|| {
            self.0.get("streams").and_then(Value::as_array).and_then(|streams| {
                streams.iter().find_map(|stream| {
                    (stream.get("kind").and_then(Value::as_str) == Some("video"))
                        .then(|| stream.get("codec").and_then(Value::as_str))
                        .flatten()
                })
            })
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct DuplicateDecisionData {
    pub version: u8,
    pub kind: String,
    pub media_id: Uuid,
}

/// Versioned durable ingest protocol. The shape is intentionally a plain
/// struct: no generic event framework or speculative stage registry is
/// needed for the five-table MVP.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct IngestData {
    pub version: u16,
    pub source: IngestSourceData,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inspection: Option<SourceInspection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub download: Option<SourceDownload>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe: Option<IngestProbe>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probed_media_kind: Option<SourceMediaKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub normalization: Option<AssetNormalization>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finalization: Option<IngestFinalization>,
    #[serde(
        rename = "_sooqa_duplicate_decision_v1",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub duplicate_decision: Option<DuplicateDecisionData>,
    /// Forward-compatible envelope fields are retained but never interpreted
    /// by the current worker.
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: BTreeMap<String, OpaqueIngestValue>,
}

#[derive(Debug, Error)]
pub enum IngestDataError {
    #[error("ingest data must be a JSON object")]
    NotAnObject,
    #[error("ingest data has an invalid version")]
    InvalidVersion,
    #[error("ingest data version {0} is not supported")]
    UnsupportedVersion(u64),
    #[error("ingest data is malformed: {0}")]
    Malformed(String),
    #[error("ingest data exceeds the {0}-byte limit")]
    TooLarge(usize),
}

impl IngestData {
    pub fn new(source: IngestSourceData) -> Self {
        Self {
            version: INGEST_DATA_VERSION,
            source,
            inspection: None,
            download: None,
            probe: None,
            probed_media_kind: None,
            normalization: None,
            finalization: None,
            duplicate_decision: None,
            extensions: BTreeMap::new(),
        }
    }

    /// Decode either the current canonical envelope or the pre-envelope
    /// object written by the five-table schema. Legacy data is upgraded in
    /// memory; callers persist the canonical serialization on their next
    /// state transition.
    pub fn decode(value: &Value) -> Result<Self, IngestDataError> {
        let encoded = serde_json::to_vec(value)
            .map_err(|error| IngestDataError::Malformed(error.to_string()))?;
        if encoded.len() > MAX_INGEST_DATA_BYTES {
            return Err(IngestDataError::TooLarge(MAX_INGEST_DATA_BYTES));
        }
        let object = value.as_object().ok_or(IngestDataError::NotAnObject)?;
        if let Some(version) = object.get("version") {
            let version = version.as_u64().ok_or(IngestDataError::InvalidVersion)?;
            if version != u64::from(INGEST_DATA_VERSION) {
                return Err(IngestDataError::UnsupportedVersion(version));
            }
            let data = serde_json::from_value::<Self>(value.clone()).map_err(|error| {
                let field = if object.contains_key("inspection") { " inspection" } else { "" };
                IngestDataError::Malformed(format!("versioned envelope{field}: {error}"))
            })?;
            return data.validate();
        }

        Self::decode_legacy(object)
    }

    fn decode_legacy(object: &serde_json::Map<String, Value>) -> Result<Self, IngestDataError> {
        let (source_value, stage_values, wrapped_source) = if let Some(source) =
            object.get("source")
        {
            let mut stages = object.clone();
            stages.remove("source");
            match source {
                Value::Object(source) => (source.clone(), stages, true),
                // A few early five-table fixtures used `source` as the URL
                // field. Keep that bounded shape readable as the canonical
                // typed URL source.
                Value::String(source) => (
                    serde_json::Map::from_iter([("url".to_owned(), Value::String(source.clone()))]),
                    stages,
                    true,
                ),
                _ => {
                    return Err(IngestDataError::Malformed(
                        "legacy source field must be an object or URL string".to_owned(),
                    ));
                }
            }
        } else {
            let mut source = object.clone();
            for key in [
                "inspection",
                "download",
                "probe",
                "probed_media_kind",
                "normalization",
                "finalization",
                DUPLICATE_DECISION_MARKER_KEY,
            ] {
                source.remove(key);
            }
            (source, object.clone(), false)
        };
        let source = serde_json::from_value::<IngestSourceData>(Value::Object(source_value))
            .map_err(|error| IngestDataError::Malformed(format!("legacy source: {error}")))?;
        let mut data = Self::new(source);
        data.inspection = stage_values
            .get("inspection")
            .map(|value| decode_legacy_inspection(value, &data.source))
            .transpose()?;
        data.download = decode_optional_stage(&stage_values, "download")?;
        data.probe = decode_optional_stage(&stage_values, "probe")?;
        data.probed_media_kind = decode_optional_stage(&stage_values, "probed_media_kind")?;
        data.normalization = decode_optional_stage(&stage_values, "normalization")?;
        data.finalization = decode_optional_stage(&stage_values, "finalization")?;
        data.duplicate_decision =
            decode_optional_stage(&stage_values, DUPLICATE_DECISION_MARKER_KEY)?;
        if wrapped_source {
            for (key, value) in &stage_values {
                if !is_envelope_key(key) {
                    data.extensions.insert(key.clone(), OpaqueIngestValue::new(value.clone()));
                }
            }
        }
        data.validate()
    }

    fn validate(self) -> Result<Self, IngestDataError> {
        self.validate_ref()?;
        Ok(self)
    }

    fn validate_ref(&self) -> Result<(), IngestDataError> {
        if self.version != INGEST_DATA_VERSION {
            return Err(IngestDataError::UnsupportedVersion(u64::from(self.version)));
        }
        validate_extensions(&self.source.extensions, "source")?;
        validate_extensions(&self.extensions, "envelope")?;
        if self.probe.as_ref().is_some_and(|probe| !probe.as_value().is_object()) {
            return Err(IngestDataError::Malformed("probe must be a JSON object".to_owned()));
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Value, IngestDataError> {
        self.validate_ref()?;
        let value = serde_json::to_value(self)
            .map_err(|error| IngestDataError::Malformed(error.to_string()))?;
        let size = serde_json::to_vec(&value)
            .map_err(|error| IngestDataError::Malformed(error.to_string()))?
            .len();
        if size > MAX_INGEST_DATA_BYTES {
            return Err(IngestDataError::TooLarge(MAX_INGEST_DATA_BYTES));
        }
        Ok(value)
    }

    pub fn source_url(&self) -> Option<&str> {
        self.source.url.as_deref()
    }

    pub fn media_kind(&self) -> Option<SourceMediaKind> {
        self.probed_media_kind.or_else(|| {
            self.download.as_ref().map(|download| download.media_kind).or(self.source.media_kind)
        })
    }

    pub fn mime_type(&self) -> Option<&str> {
        self.download
            .as_ref()
            .and_then(|download| download.mime_type.as_deref())
            .or(self.source.mime_type.as_deref())
    }
}

fn decode_optional_stage<T: for<'de> Deserialize<'de>>(
    object: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<Option<T>, IngestDataError> {
    object
        .get(key)
        .map(|value| {
            serde_json::from_value(value.clone())
                .map_err(|error| IngestDataError::Malformed(format!("{key}: {error}")))
        })
        .transpose()
}

fn decode_legacy_inspection(
    value: &Value,
    source: &IngestSourceData,
) -> Result<SourceInspection, IngestDataError> {
    match serde_json::from_value(value.clone()) {
        Ok(inspection) => Ok(inspection),
        Err(error) if value.is_object() && error.to_string().contains("missing field") => {
            let Some(object) = value.as_object() else {
                return Err(IngestDataError::Malformed(
                    "legacy inspection must be an object".to_owned(),
                ));
            };
            Ok(SourceInspection {
                adapter: object
                    .get("adapter")
                    .and_then(Value::as_str)
                    .unwrap_or("legacy")
                    .to_owned(),
                source_url: object
                    .get("source_url")
                    .and_then(Value::as_str)
                    .or(source.url.as_deref())
                    .unwrap_or_default()
                    .to_owned(),
                resolved_url: object.get("resolved_url").and_then(Value::as_str).map(str::to_owned),
                media_kind: object
                    .get("media_kind")
                    .cloned()
                    .and_then(|value| serde_json::from_value(value).ok())
                    .unwrap_or(SourceMediaKind::Unknown),
                mime_type: object.get("mime_type").and_then(Value::as_str).map(str::to_owned),
                content_length_bytes: object.get("content_length_bytes").and_then(Value::as_u64),
                title: object.get("title").and_then(Value::as_str).map(str::to_owned),
                metadata: object.get("metadata").cloned().unwrap_or_else(|| json!({})),
            })
        }
        Err(error) => Err(IngestDataError::Malformed(format!("inspection: {error}"))),
    }
}

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_format: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AssetNormalization {
    pub local_work_path: String,
    pub file_size_bytes: u64,
    pub sha256: String,
    pub media_kind: SourceMediaKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_version: Option<String>,
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
    pub original_input: Value,
    pub supplied_caption: Option<String>,
    pub idempotency_key: Option<String>,
}

impl IngestSubmissionInput {
    pub fn new(source_url: impl Into<String>, submitted_via: SubmittedVia) -> Self {
        Self {
            source_url: source_url.into(),
            submitted_via,
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

/// The request-hash wire shape used before the unused admin identity was
/// removed from the domain model. Existing ingests store hashes of this
/// shape, so new requests must keep hashing the explicit `null` field while
/// the field itself stays out of the submission API and persisted ingest
/// state.
#[derive(Serialize)]
struct LegacyIngestSubmission<'a> {
    kind: IngestKind,
    submitted_via: SubmittedVia,
    submitted_by_admin_id: Option<Uuid>,
    original_url: &'a str,
    normalized_url: &'a str,
    original_input: &'a Value,
    page_url: &'a Option<String>,
    page_title: &'a Option<String>,
    supplied_caption: &'a Option<String>,
    supplied_description: &'a Option<String>,
    supplied_tags: &'a Vec<String>,
    requested_action: RequestedAction,
    requested_publish_at: &'a Option<time::OffsetDateTime>,
    requested_post_caption: &'a Option<String>,
    idempotency_key: &'a Option<String>,
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
        IngestData::decode(&input.original_input)
            .map_err(|error| IngestValidationError::InvalidInputEnvelope(error.to_string()))?;
        Ok(Self {
            kind: IngestKind::TelegramMessage,
            submitted_via: input.submitted_via,
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
        let serialized = serde_json::to_vec(&LegacyIngestSubmission {
            kind: self.kind,
            submitted_via: self.submitted_via,
            submitted_by_admin_id: None,
            original_url: &self.original_url,
            normalized_url: &self.normalized_url,
            original_input: &self.original_input,
            page_url: &self.page_url,
            page_title: &self.page_title,
            supplied_caption: &self.supplied_caption,
            supplied_description: &self.supplied_description,
            supplied_tags: &self.supplied_tags,
            requested_action: self.requested_action,
            requested_publish_at: &self.requested_publish_at,
            requested_post_caption: &self.requested_post_caption,
            idempotency_key: &self.idempotency_key,
        })
        .expect("ingest submission must be serializable");
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
    /// Server-selected publication target captured when the request is created.
    /// This is deliberately absent from `IngestSubmission` and its request hash.
    pub requested_channel_id: Option<Uuid>,
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

/// Stable cursor for the newest-first operational ingest view. The UUID
/// breaks ties when concurrent inserts share a database timestamp.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct IngestCursor {
    pub created_at: time::OffsetDateTime,
    pub id: Uuid,
}

/// Bounded fields used by the read-only admin ingest list. Pipeline input and
/// job payloads stay out of this contract.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IngestListItem {
    pub id: Uuid,
    pub source_url: Option<String>,
    pub page_url: Option<String>,
    pub requested_action: RequestedAction,
    pub status: IngestStatus,
    pub created_at: time::OffsetDateTime,
    pub updated_at: time::OffsetDateTime,
    pub completed_at: Option<time::OffsetDateTime>,
    pub media_id: Option<Uuid>,
    pub storage_url: Option<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IngestPage {
    pub items: Vec<IngestListItem>,
    pub next_cursor: Option<IngestCursor>,
}

impl Ingest {
    pub fn from_submission(
        id: Uuid,
        submission: &IngestSubmission,
    ) -> Result<Self, IngestDataError> {
        let now = time::OffsetDateTime::now_utc();
        let original_input =
            IngestData::decode(&submission.original_input).and_then(|data| data.encode())?;
        Ok(Self {
            id,
            workspace_id: id,
            kind: submission.kind,
            status: IngestStatus::Received,
            submitted_via: submission.submitted_via,
            original_input,
            source_url: submission.normalized_url.clone(),
            page_url: submission.page_url.clone(),
            page_title: submission.page_title.clone(),
            supplied_caption: submission.supplied_caption.clone(),
            supplied_description: submission.supplied_description.clone(),
            supplied_tags: submission.supplied_tags.clone(),
            requested_action: submission.requested_action,
            requested_publish_at: submission.requested_publish_at,
            requested_post_caption: submission.requested_post_caption.clone(),
            requested_channel_id: None,
            idempotency_key: submission.idempotency_key.clone(),
            media_id: None,
            force_save: false,
            duplicate_evidence: None,
            error_code: None,
            error_message: None,
            created_at: now,
            updated_at: now,
            completed_at: None,
        })
    }

    pub fn transition_to(&mut self, target: IngestStatus) -> Result<(), IngestStateError> {
        if !self.status.can_transition_to(target) {
            return Err(IngestStateError { from: self.status, to: target });
        }
        self.status = target;
        Ok(())
    }

    /// Decode the durable stage data through the single envelope boundary.
    /// Legacy rows are upgraded in memory and become canonical on the next
    /// state mutation.
    pub fn input_data(&self) -> Result<IngestData, IngestDataError> {
        IngestData::decode(&self.original_input)
    }

    pub fn set_input_data(&mut self, data: IngestData) -> Result<(), IngestDataError> {
        self.original_input = data.encode()?;
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
    #[error("invalid ingest input envelope: {0}")]
    InvalidInputEnvelope(String),
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
    #[error("requested post caption must be at most {max} characters")]
    RequestedPostCaptionTooLong { max: usize },
    #[error("requested post caption contains a disallowed control character")]
    RequestedPostCaptionControlCharacter,
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
    if let Some(caption) = requested_post_caption {
        if caption.chars().count() > MAX_REQUESTED_POST_CAPTION_LENGTH {
            return Err(IngestValidationError::RequestedPostCaptionTooLong {
                max: MAX_REQUESTED_POST_CAPTION_LENGTH,
            });
        }
        if caption
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
        {
            return Err(IngestValidationError::RequestedPostCaptionControlCharacter);
        }
    }

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
    fn requested_post_caption_matches_the_publisher_text_contract() {
        let mut multiline =
            IngestSubmissionInput::new("https://example.com/multiline-caption", SubmittedVia::Api);
        multiline.requested_action = RequestedAction::PostNow;
        multiline.requested_post_caption = Some("line one\nline two\tready".to_owned());
        let submission = IngestSubmission::try_new(multiline).expect("multiline caption is valid");
        assert_eq!(submission.requested_post_caption.as_deref(), Some("line one\nline two\tready"));

        let mut too_long =
            IngestSubmissionInput::new("https://example.com/long-caption", SubmittedVia::Api);
        too_long.requested_action = RequestedAction::PostNow;
        too_long.requested_post_caption = Some("x".repeat(MAX_REQUESTED_POST_CAPTION_LENGTH + 1));
        assert!(matches!(
            IngestSubmission::try_new(too_long),
            Err(IngestValidationError::RequestedPostCaptionTooLong { .. })
        ));

        let mut control =
            IngestSubmissionInput::new("https://example.com/control-caption", SubmittedVia::Api);
        control.requested_action = RequestedAction::PostNow;
        control.requested_post_caption = Some("bad\u{0000}caption".to_owned());
        assert!(matches!(
            IngestSubmission::try_new(control),
            Err(IngestValidationError::RequestedPostCaptionControlCharacter)
        ));

        let mut blank =
            IngestSubmissionInput::new("https://example.com/blank-caption", SubmittedVia::Api);
        blank.requested_action = RequestedAction::PostNow;
        blank.requested_post_caption = Some(" \n\t ".to_owned());
        let blank = IngestSubmission::try_new(blank).expect("blank optional caption is omitted");
        assert!(blank.requested_post_caption.is_none());
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
            Ingest::from_submission(Uuid::now_v7(), &submission("https://example.com"))
                .expect("valid submission");

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
            Ingest::from_submission(Uuid::now_v7(), &submission("https://example.com"))
                .expect("valid submission");
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
            Ingest::from_submission(Uuid::now_v7(), &submission("https://example.com"))
                .expect("valid submission");
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
            Ingest::from_submission(Uuid::now_v7(), &submission("https://example.com"))
                .expect("valid submission");
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
            Ingest::from_submission(Uuid::now_v7(), &submission("https://example.com"))
                .expect("valid submission");
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
            Ingest::from_submission(Uuid::now_v7(), &submission("https://example.com"))
                .expect("valid submission");
        request.transition_to(IngestStatus::Cancelled).expect("request should be cancellable");

        assert!(request.transition_to(IngestStatus::Queued).is_err());
    }

    #[test]
    fn new_submissions_write_the_versioned_envelope_without_changing_request_hash() {
        let value = submission("https://example.com/clip.mp4");
        let legacy_input = value.original_input.clone();
        let hash = value.request_hash();
        let request = Ingest::from_submission(Uuid::now_v7(), &value).expect("valid submission");

        assert_eq!(request.original_input["version"], INGEST_DATA_VERSION);
        assert_eq!(request.original_input["source"]["url"], "https://example.com/clip.mp4");
        assert_eq!(
            request.input_data().unwrap().source_url(),
            Some("https://example.com/clip.mp4")
        );
        assert_eq!(value.original_input, legacy_input);
        assert_eq!(value.request_hash(), hash);
    }

    #[test]
    fn current_legacy_stage_rows_upgrade_to_typed_data() {
        let legacy = json!({
            "url": "https://example.com/clip.mp4",
            "page_title": "legacy title",
            "inspection": {
                "adapter": "direct_http",
                "source_url": "https://example.com/clip.mp4",
                "resolved_url": null,
                "media_kind": "video",
                "mime_type": "video/mp4",
                "content_length_bytes": 12,
                "title": null,
                "metadata": {}
            },
            "download": {
                "bytes": 12,
                "mime_type": "video/mp4",
                "media_kind": "video"
            },
            "probed_media_kind": "video"
        });
        let data = IngestData::decode(&legacy).expect("legacy row should decode");
        assert_eq!(data.version, INGEST_DATA_VERSION);
        assert_eq!(data.source_url(), Some("https://example.com/clip.mp4"));
        assert_eq!(data.inspection.as_ref().unwrap().adapter, "direct_http");
        assert_eq!(data.download.as_ref().unwrap().bytes, 12);
        assert_eq!(data.media_kind(), Some(SourceMediaKind::Video));

        let canonical = data.encode().unwrap();
        assert_eq!(canonical["version"], INGEST_DATA_VERSION);
        assert!(canonical.get("inspection").is_some());
        assert!(canonical.get("download").is_some());
        assert!(canonical.get("url").is_none());
        assert!(canonical["source"].get("inspection").is_none());
        assert_eq!(IngestData::decode(&canonical).unwrap().inspection, data.inspection);
    }

    #[test]
    fn telegram_and_decision_rows_are_forward_readable() {
        let media_id = Uuid::now_v7();
        let legacy = json!({
            "source_type": "telegram",
            "telegram_workspace_id": Uuid::now_v7(),
            "telegram_file_id": "file-id",
            "telegram_file_unique_id": "unique-id",
            "file_size": 42,
            "mime_type": "video/mp4",
            "file_name": "clip.mp4",
            "media_kind": "video",
            "_sooqa_duplicate_decision_v1": {
                "version": 1,
                "kind": "accepted",
                "media_id": media_id
            }
        });
        let data = IngestData::decode(&legacy).expect("Telegram row should decode");
        assert_eq!(data.source.telegram_file_id.as_deref(), Some("file-id"));
        assert_eq!(data.source.file_size, Some(42));
        assert_eq!(data.duplicate_decision.unwrap().media_id, media_id);
    }

    const LEGACY_STAGE_FIXTURE: &str = r#"
    {
      "url": "https://example.com/fixture.mp4",
      "inspection": {
        "adapter": "direct_http",
        "source_url": "https://example.com/fixture.mp4",
        "resolved_url": "https://example.com/fixture.mp4",
        "media_kind": "video",
        "mime_type": "video/mp4",
        "content_length_bytes": 12,
        "title": "fixture",
        "metadata": {"fixture": true}
      },
      "download": {"bytes": 12, "mime_type": "video/mp4", "media_kind": "video", "selected_format": "best"},
      "probe": {"container_format": "mp4", "streams": [{"kind": "video", "codec": "h264"}]},
      "probed_media_kind": "video",
      "normalization": {
        "local_work_path": "normalized.mp4",
        "file_size_bytes": 12,
        "sha256": "fixture-sha256",
        "media_kind": "video",
        "profile_version": "video-v1",
        "mime_type": "video/mp4",
        "container": "mp4",
        "video_codec": "h264",
        "audio_codec": null,
        "width": 320,
        "height": 240,
        "duration_ms": 1000,
        "bit_rate": 96,
        "thumbnail": null
      },
      "finalization": {"media_id": "00000000-0000-0000-0000-000000000001"},
      "_sooqa_duplicate_decision_v1": {
        "version": 1,
        "kind": "pending",
        "media_id": "00000000-0000-0000-0000-000000000002"
      }
    }
    "#;

    const LEGACY_TELEGRAM_FIXTURE: &str = r#"
    {
      "source_type": "telegram",
      "telegram_workspace_id": "00000000-0000-0000-0000-000000000003",
      "telegram_chat_id": 42,
      "telegram_message_id": 99,
      "telegram_file_id": "file-id",
      "telegram_file_unique_id": "unique-id",
      "file_size": 12,
      "mime_type": "video/mp4",
      "file_name": "fixture.mp4",
      "media_kind": "video",
      "probe": {"container_format": "mp4", "streams": [{"kind": "video", "codec": "h264"}]}
    }
    "#;

    const LEGACY_UPLOAD_FIXTURE: &str = r#"
    {
      "source_type": "upload",
      "file_size": 12,
      "mime_type": "image/png",
      "file_name": "fixture.png",
      "media_kind": "image",
      "probe": {"container_format": "png", "streams": [{"kind": "video", "codec": "png"}]},
      "probed_media_kind": "image"
    }
    "#;

    #[test]
    fn serialized_legacy_stage_fixtures_upgrade_and_round_trip_for_each_kind() {
        for (fixture, expected_kind, expected_source) in [
            (LEGACY_STAGE_FIXTURE, IngestKind::Url, Some("https://example.com/fixture.mp4")),
            (LEGACY_TELEGRAM_FIXTURE, IngestKind::TelegramMessage, None),
            (LEGACY_UPLOAD_FIXTURE, IngestKind::Upload, None),
        ] {
            let legacy: Value = serde_json::from_str(fixture).expect("fixture JSON should parse");
            let data = IngestData::decode(&legacy).expect("pre-envelope fixture should decode");
            assert_eq!(data.version, INGEST_DATA_VERSION);
            assert!(data.probe.is_some(), "probe stage must survive upgrade");
            if expected_kind == IngestKind::Url {
                assert!(data.inspection.is_some());
                assert!(data.download.is_some());
                assert!(data.normalization.is_some());
                assert!(data.finalization.is_some());
                assert!(data.duplicate_decision.is_some());
            }
            assert_eq!(data.source_url(), expected_source);
            let canonical = data.encode().expect("fixture should encode canonically");
            assert_eq!(canonical["version"], INGEST_DATA_VERSION);
            assert_eq!(IngestData::decode(&canonical).unwrap(), data);
        }
    }

    #[test]
    fn corrupt_and_unsupported_envelopes_fail_boundedly() {
        assert!(matches!(
            IngestData::decode(&json!({"version": 2, "source": {}})),
            Err(IngestDataError::UnsupportedVersion(2))
        ));
        assert!(matches!(
            IngestData::decode(&json!({"version": 1, "source": "not-an-object"})),
            Err(IngestDataError::Malformed(_))
        ));
        assert!(matches!(
            IngestData::decode(&json!({"version": 1, "source": {}, "probe": "broken"})),
            Err(IngestDataError::Malformed(_))
        ));
        assert!(matches!(IngestData::decode(&json!(null)), Err(IngestDataError::NotAnObject)));
    }

    #[test]
    fn malformed_versioned_inspection_is_not_treated_as_legacy() {
        let malformed = json!({
            "version": 1,
            "source": {"url": "https://example.com/clip.mp4"},
            "inspection": {"adapter": "direct_http"}
        });
        assert!(matches!(
            IngestData::decode(&malformed),
            Err(IngestDataError::Malformed(message)) if message.contains("inspection")
        ));
    }

    #[test]
    fn legacy_wrapper_retains_unknown_adapter_metadata() {
        let legacy = json!({
            "source": {"url": "https://example.com/clip.mp4"},
            "adapter_state": {"provider": "example", "attempt": 2},
            "inspection": {
                "adapter": "direct_http",
                "source_url": "https://example.com/clip.mp4",
                "resolved_url": null,
                "media_kind": "video",
                "mime_type": "video/mp4",
                "content_length_bytes": null,
                "title": null,
                "metadata": {}
            }
        });
        let data = IngestData::decode(&legacy).expect("legacy wrapper should decode");
        assert_eq!(
            data.extensions["adapter_state"].as_value(),
            &json!({"provider": "example", "attempt": 2})
        );
        let canonical = data.encode().expect("extension should encode");
        assert_eq!(canonical["adapter_state"]["attempt"], 2);
        assert_eq!(IngestData::decode(&canonical).unwrap().extensions, data.extensions);
    }

    #[test]
    fn public_extension_maps_cannot_shadow_canonical_fields() {
        let mut data = IngestData::new(IngestSourceData::default());
        data.extensions.insert("source".to_owned(), OpaqueIngestValue::new(json!("shadow")));
        assert!(matches!(
            data.encode(),
            Err(IngestDataError::Malformed(message)) if message.contains("reserved key")
        ));
    }

    #[test]
    fn mutated_submission_fails_without_panicking() {
        let mut submission = submission("https://example.com/clip.mp4");
        submission.original_input = json!({
            "version": 1,
            "source": {"url": "https://example.com/clip.mp4"},
            "inspection": {"adapter": "missing-required-fields"}
        });
        assert!(matches!(
            Ingest::from_submission(Uuid::now_v7(), &submission),
            Err(IngestDataError::Malformed(_))
        ));
    }
}
