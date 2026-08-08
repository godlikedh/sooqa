//! Durable job envelopes and typed command boundaries for sooqa.

use std::fmt;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use sooqa_inbox::SourceInspection;
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

pub type JobId = Uuid;

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub enum JobType {
    InspectSource,
    DownloadSource,
    ProbeAsset,
    CheckExactDuplicate,
    NormalizeAsset,
    ComputeFingerprint,
    CheckSimilarity,
    UploadStorageAsset,
    FinalizeIngest,
    PublishPost,
    VerifyStorageObject,
    CleanupWorkspace,
    RecoverStaleJobs,
}

impl JobType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InspectSource => "inspect_source",
            Self::DownloadSource => "download_source",
            Self::ProbeAsset => "probe_asset",
            Self::CheckExactDuplicate => "check_exact_duplicate",
            Self::NormalizeAsset => "normalize_asset",
            Self::ComputeFingerprint => "compute_fingerprint",
            Self::CheckSimilarity => "check_similarity",
            Self::UploadStorageAsset => "upload_storage_asset",
            Self::FinalizeIngest => "finalize_ingest",
            Self::PublishPost => "publish_post",
            Self::VerifyStorageObject => "verify_storage_object",
            Self::CleanupWorkspace => "cleanup_workspace",
            Self::RecoverStaleJobs => "recover_stale_jobs",
        }
    }
}

impl fmt::Display for JobType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl TryFrom<&str> for JobType {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "inspect_source" => Ok(Self::InspectSource),
            "download_source" => Ok(Self::DownloadSource),
            "probe_asset" => Ok(Self::ProbeAsset),
            "check_exact_duplicate" => Ok(Self::CheckExactDuplicate),
            "normalize_asset" => Ok(Self::NormalizeAsset),
            "compute_fingerprint" => Ok(Self::ComputeFingerprint),
            "check_similarity" => Ok(Self::CheckSimilarity),
            "upload_storage_asset" => Ok(Self::UploadStorageAsset),
            "finalize_ingest" => Ok(Self::FinalizeIngest),
            "publish_post" => Ok(Self::PublishPost),
            "verify_storage_object" => Ok(Self::VerifyStorageObject),
            "cleanup_workspace" => Ok(Self::CleanupWorkspace),
            "recover_stale_jobs" => Ok(Self::RecoverStaleJobs),
            unknown => Err(unknown.to_owned()),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum JobStatus {
    Queued,
    Running,
    Succeeded,
    RetryWait,
    Failed,
    Cancelled,
}

impl JobStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::RetryWait => "retry_wait",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

impl fmt::Display for JobStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl TryFrom<&str> for JobStatus {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "retry_wait" => Ok(Self::RetryWait),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            unknown => Err(unknown.to_owned()),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InspectSourcePayload {
    pub ingest_request_id: Uuid,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DownloadSourcePayload {
    pub ingest_request_id: Uuid,
    pub inspection: SourceInspection,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngestJobPayload {
    pub ingest_request_id: Uuid,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishPostPayload {
    pub post_id: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssetJobPayload {
    pub asset_id: Uuid,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageUploadAssetPayload {
    pub asset_id: Uuid,
    pub generation: i32,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmptyJobPayload {}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum JobCommand {
    InspectSource(InspectSourcePayload),
    DownloadSource(DownloadSourcePayload),
    ProbeAsset(IngestJobPayload),
    CheckExactDuplicate(IngestJobPayload),
    NormalizeAsset(IngestJobPayload),
    ComputeFingerprint(IngestJobPayload),
    CheckSimilarity(IngestJobPayload),
    UploadStorageAsset(StorageUploadAssetPayload),
    FinalizeIngest(IngestJobPayload),
    PublishPost(PublishPostPayload),
    VerifyStorageObject(AssetJobPayload),
    CleanupWorkspace(EmptyJobPayload),
    RecoverStaleJobs(EmptyJobPayload),
}

impl JobCommand {
    pub const fn job_type(&self) -> JobType {
        match self {
            Self::InspectSource(_) => JobType::InspectSource,
            Self::DownloadSource(_) => JobType::DownloadSource,
            Self::ProbeAsset(_) => JobType::ProbeAsset,
            Self::CheckExactDuplicate(_) => JobType::CheckExactDuplicate,
            Self::NormalizeAsset(_) => JobType::NormalizeAsset,
            Self::ComputeFingerprint(_) => JobType::ComputeFingerprint,
            Self::CheckSimilarity(_) => JobType::CheckSimilarity,
            Self::UploadStorageAsset(_) => JobType::UploadStorageAsset,
            Self::FinalizeIngest(_) => JobType::FinalizeIngest,
            Self::PublishPost(_) => JobType::PublishPost,
            Self::VerifyStorageObject(_) => JobType::VerifyStorageObject,
            Self::CleanupWorkspace(_) => JobType::CleanupWorkspace,
            Self::RecoverStaleJobs(_) => JobType::RecoverStaleJobs,
        }
    }

    pub fn from_payload(job_type: JobType, payload: Value) -> Result<Self, JobPayloadError> {
        macro_rules! decode {
            ($payload_type:ty, $variant:ident) => {
                decode_payload::<$payload_type>(job_type, payload).map(Self::$variant)
            };
        }

        match job_type {
            JobType::InspectSource => decode!(InspectSourcePayload, InspectSource),
            JobType::DownloadSource => decode!(DownloadSourcePayload, DownloadSource),
            JobType::ProbeAsset => decode!(IngestJobPayload, ProbeAsset),
            JobType::CheckExactDuplicate => decode!(IngestJobPayload, CheckExactDuplicate),
            JobType::NormalizeAsset => decode!(IngestJobPayload, NormalizeAsset),
            JobType::ComputeFingerprint => decode!(IngestJobPayload, ComputeFingerprint),
            JobType::CheckSimilarity => decode!(IngestJobPayload, CheckSimilarity),
            JobType::UploadStorageAsset => {
                decode!(StorageUploadAssetPayload, UploadStorageAsset)
            }
            JobType::FinalizeIngest => decode!(IngestJobPayload, FinalizeIngest),
            JobType::PublishPost => decode!(PublishPostPayload, PublishPost),
            JobType::VerifyStorageObject => decode!(AssetJobPayload, VerifyStorageObject),
            JobType::CleanupWorkspace => decode!(EmptyJobPayload, CleanupWorkspace),
            JobType::RecoverStaleJobs => decode!(EmptyJobPayload, RecoverStaleJobs),
        }
    }

    pub fn payload_json(&self) -> Value {
        match self {
            Self::InspectSource(payload) => serde_json::to_value(payload),
            Self::DownloadSource(payload) => serde_json::to_value(payload),
            Self::ProbeAsset(payload) => serde_json::to_value(payload),
            Self::CheckExactDuplicate(payload) => serde_json::to_value(payload),
            Self::NormalizeAsset(payload) => serde_json::to_value(payload),
            Self::ComputeFingerprint(payload) => serde_json::to_value(payload),
            Self::CheckSimilarity(payload) => serde_json::to_value(payload),
            Self::UploadStorageAsset(payload) => serde_json::to_value(payload),
            Self::FinalizeIngest(payload) => serde_json::to_value(payload),
            Self::PublishPost(payload) => serde_json::to_value(payload),
            Self::VerifyStorageObject(payload) => serde_json::to_value(payload),
            Self::CleanupWorkspace(payload) => serde_json::to_value(payload),
            Self::RecoverStaleJobs(payload) => serde_json::to_value(payload),
        }
        .expect("job payloads must be JSON serializable")
    }
}

fn decode_payload<T: DeserializeOwned>(
    job_type: JobType,
    payload: Value,
) -> Result<T, JobPayloadError> {
    serde_json::from_value(payload)
        .map_err(|error| JobPayloadError::Invalid { job_type, message: error.to_string() })
}

#[derive(Debug, Clone, Eq, PartialEq, Error)]
pub enum JobPayloadError {
    #[error("invalid payload for {job_type}: {message}")]
    Invalid { job_type: JobType, message: String },
}

#[derive(Debug, Clone)]
pub struct NewJob {
    command: JobCommand,
    priority: i32,
    available_at: Option<OffsetDateTime>,
    max_attempts: i32,
    idempotency_key: Option<String>,
}

impl NewJob {
    pub fn new(command: JobCommand) -> Self {
        Self { command, priority: 0, available_at: None, max_attempts: 5, idempotency_key: None }
    }

    pub fn inspect_source(ingest_request_id: Uuid) -> Self {
        Self::new(JobCommand::InspectSource(InspectSourcePayload { ingest_request_id }))
    }

    pub fn download_source(ingest_request_id: Uuid, inspection: SourceInspection) -> Self {
        Self::new(JobCommand::DownloadSource(DownloadSourcePayload {
            ingest_request_id,
            inspection,
        }))
    }

    pub fn publish_post(post_id: impl Into<String>) -> Self {
        Self::new(JobCommand::PublishPost(PublishPostPayload { post_id: post_id.into() }))
    }

    pub fn upload_storage_asset(asset_id: Uuid) -> Self {
        Self::upload_storage_asset_generation(asset_id, 0)
    }

    pub fn upload_storage_asset_generation(asset_id: Uuid, generation: i32) -> Self {
        Self::new(JobCommand::UploadStorageAsset(StorageUploadAssetPayload {
            asset_id,
            generation,
        }))
    }

    pub fn probe_asset(ingest_request_id: Uuid) -> Self {
        Self::new(JobCommand::ProbeAsset(IngestJobPayload { ingest_request_id }))
    }

    pub fn normalize_asset(ingest_request_id: Uuid) -> Self {
        Self::new(JobCommand::NormalizeAsset(IngestJobPayload { ingest_request_id }))
    }

    pub fn finalize_ingest(ingest_request_id: Uuid) -> Self {
        Self::new(JobCommand::FinalizeIngest(IngestJobPayload { ingest_request_id }))
    }

    pub fn cleanup_workspace() -> Self {
        Self::new(JobCommand::CleanupWorkspace(EmptyJobPayload {}))
    }

    pub fn command(&self) -> &JobCommand {
        &self.command
    }

    pub fn job_type(&self) -> JobType {
        self.command.job_type()
    }

    pub fn payload_json(&self) -> Value {
        self.command.payload_json()
    }

    pub fn priority(&self) -> i32 {
        self.priority
    }

    pub fn available_at_value(&self) -> Option<OffsetDateTime> {
        self.available_at
    }

    pub fn max_attempts_value(&self) -> i32 {
        self.max_attempts
    }

    pub fn idempotency_key_value(&self) -> Option<&str> {
        self.idempotency_key.as_deref()
    }

    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    pub fn available_at(mut self, available_at: OffsetDateTime) -> Self {
        self.available_at = Some(available_at);
        self
    }

    pub fn max_attempts(mut self, max_attempts: i32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    pub fn idempotency_key(mut self, idempotency_key: impl Into<String>) -> Self {
        self.idempotency_key = Some(idempotency_key.into());
        self
    }
}

#[derive(Debug, Clone)]
pub struct Job {
    pub id: JobId,
    pub command: JobCommand,
    pub status: JobStatus,
    pub priority: i32,
    pub available_at: OffsetDateTime,
    pub attempt_count: i32,
    pub max_attempts: i32,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<OffsetDateTime>,
    pub last_heartbeat_at: Option<OffsetDateTime>,
    pub last_error_class: Option<String>,
    pub last_error_message: Option<String>,
    pub idempotency_key: Option<String>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub completed_at: Option<OffsetDateTime>,
}

impl Job {
    pub fn job_type(&self) -> JobType {
        self.command.job_type()
    }

    pub fn attempt(&self) -> Option<JobAttempt> {
        if self.status != JobStatus::Running || self.attempt_count <= 0 {
            return None;
        }
        self.lease_owner.clone().map(|lease_owner| JobAttempt {
            job_id: self.id,
            attempt_number: self.attempt_count,
            lease_owner,
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct JobAttempt {
    pub job_id: JobId,
    pub attempt_number: i32,
    pub lease_owner: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_values_round_trip_to_database_strings() {
        assert_eq!(JobType::PublishPost.as_str(), "publish_post");
        assert_eq!(JobType::try_from("publish_post"), Ok(JobType::PublishPost));
        assert_eq!(JobStatus::RetryWait.as_str(), "retry_wait");
        assert_eq!(JobStatus::try_from("retry_wait"), Ok(JobStatus::RetryWait));
    }

    #[test]
    fn typed_job_payload_round_trips_with_its_discriminator() {
        let id = Uuid::new_v4();
        let new_job = NewJob::inspect_source(id);
        let command = JobCommand::from_payload(new_job.job_type(), new_job.payload_json())
            .expect("typed payload should decode");

        assert_eq!(command.job_type(), JobType::InspectSource);
        assert_eq!(
            command,
            JobCommand::InspectSource(InspectSourcePayload { ingest_request_id: id })
        );
    }

    #[test]
    fn mismatched_payload_is_rejected() {
        let error = JobCommand::from_payload(
            JobType::InspectSource,
            serde_json::json!({"post_id": "wrong-command"}),
        )
        .expect_err("payload should not decode as inspect_source");

        assert!(error.to_string().contains("invalid payload for inspect_source"));
    }

    #[test]
    fn new_job_has_safe_defaults() {
        let job = NewJob::inspect_source(Uuid::new_v4());

        assert_eq!(job.priority(), 0);
        assert_eq!(job.max_attempts_value(), 5);
        assert!(job.available_at_value().is_none());
        assert!(job.idempotency_key_value().is_none());
    }
}
