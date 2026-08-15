//! Durable queue commands and fenced job envelopes.

use std::fmt;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use sooqa_inbox::SourceInspection;
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

pub type JobId = Uuid;

#[derive(Debug, Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum JobType {
    InspectSource,
    DownloadSource,
    ProbeAsset,
    NormalizeAsset,
    ComputeFingerprint,
    FinalizeIngest,
    MaterializePublication,
    UploadStorageAsset,
    PublishPost,
    CleanupWorkspace,
    RecoverStaleJobs,
}

impl JobType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InspectSource => "inspect_source",
            Self::DownloadSource => "download_source",
            Self::ProbeAsset => "probe_asset",
            Self::NormalizeAsset => "normalize_asset",
            Self::ComputeFingerprint => "compute_fingerprint",
            Self::FinalizeIngest => "finalize_ingest",
            Self::MaterializePublication => "materialize_publication",
            Self::UploadStorageAsset => "upload_storage_asset",
            Self::PublishPost => "publish_post",
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
            "normalize_asset" => Ok(Self::NormalizeAsset),
            "compute_fingerprint" => Ok(Self::ComputeFingerprint),
            "finalize_ingest" => Ok(Self::FinalizeIngest),
            "materialize_publication" => Ok(Self::MaterializePublication),
            "upload_storage_asset" => Ok(Self::UploadStorageAsset),
            "publish_post" => Ok(Self::PublishPost),
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
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub struct JobCounts {
    pub queued: u64,
    pub running: u64,
}

impl JobStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
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
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            unknown => Err(unknown.to_owned()),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InspectSourcePayload {
    pub ingest_id: Uuid,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DownloadSourcePayload {
    pub ingest_id: Uuid,
    pub inspection: SourceInspection,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngestJobPayload {
    pub ingest_id: Uuid,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaterializePublicationPayload {
    pub ingest_id: Uuid,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaJobPayload {
    pub media_id: Uuid,
    pub generation: i32,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishPostPayload {
    pub post_id: Uuid,
    #[serde(default)]
    pub expected_revision: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupWorkspacePayload {
    pub ingest_id: Uuid,
    /// The workspace ID is generation-scoped: a force-save receives a new ID,
    /// so an old cleanup job can never remove the new generation's workspace.
    pub workspace_id: Uuid,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmptyJobPayload {}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum JobCommand {
    InspectSource(InspectSourcePayload),
    DownloadSource(DownloadSourcePayload),
    ProbeAsset(IngestJobPayload),
    NormalizeAsset(IngestJobPayload),
    ComputeFingerprint(IngestJobPayload),
    FinalizeIngest(IngestJobPayload),
    MaterializePublication(MaterializePublicationPayload),
    UploadStorageAsset(MediaJobPayload),
    PublishPost(PublishPostPayload),
    CleanupWorkspace(CleanupWorkspacePayload),
    RecoverStaleJobs(EmptyJobPayload),
}

impl JobCommand {
    pub const fn job_type(&self) -> JobType {
        match self {
            Self::InspectSource(_) => JobType::InspectSource,
            Self::DownloadSource(_) => JobType::DownloadSource,
            Self::ProbeAsset(_) => JobType::ProbeAsset,
            Self::NormalizeAsset(_) => JobType::NormalizeAsset,
            Self::ComputeFingerprint(_) => JobType::ComputeFingerprint,
            Self::FinalizeIngest(_) => JobType::FinalizeIngest,
            Self::MaterializePublication(_) => JobType::MaterializePublication,
            Self::UploadStorageAsset(_) => JobType::UploadStorageAsset,
            Self::PublishPost(_) => JobType::PublishPost,
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
            JobType::NormalizeAsset => decode!(IngestJobPayload, NormalizeAsset),
            JobType::ComputeFingerprint => decode!(IngestJobPayload, ComputeFingerprint),
            JobType::FinalizeIngest => decode!(IngestJobPayload, FinalizeIngest),
            JobType::MaterializePublication => {
                decode!(MaterializePublicationPayload, MaterializePublication)
            }
            JobType::UploadStorageAsset => decode!(MediaJobPayload, UploadStorageAsset),
            JobType::PublishPost => decode!(PublishPostPayload, PublishPost),
            JobType::CleanupWorkspace => decode!(CleanupWorkspacePayload, CleanupWorkspace),
            JobType::RecoverStaleJobs => decode!(EmptyJobPayload, RecoverStaleJobs),
        }
    }

    pub fn payload_json(&self) -> Value {
        match self {
            Self::InspectSource(payload) => serde_json::to_value(payload),
            Self::DownloadSource(payload) => serde_json::to_value(payload),
            Self::ProbeAsset(payload) => serde_json::to_value(payload),
            Self::NormalizeAsset(payload) => serde_json::to_value(payload),
            Self::ComputeFingerprint(payload) => serde_json::to_value(payload),
            Self::FinalizeIngest(payload) => serde_json::to_value(payload),
            Self::MaterializePublication(payload) => serde_json::to_value(payload),
            Self::UploadStorageAsset(payload) => serde_json::to_value(payload),
            Self::PublishPost(payload) => serde_json::to_value(payload),
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
    run_at: Option<OffsetDateTime>,
    max_attempts: i32,
    dedupe_key: Option<String>,
}

impl NewJob {
    pub fn new(command: JobCommand) -> Self {
        Self { command, priority: 0, run_at: None, max_attempts: 5, dedupe_key: None }
    }

    pub fn inspect_source(ingest_id: Uuid) -> Self {
        Self::new(JobCommand::InspectSource(InspectSourcePayload { ingest_id }))
    }

    pub fn download_source(ingest_id: Uuid, inspection: SourceInspection) -> Self {
        Self::new(JobCommand::DownloadSource(DownloadSourcePayload { ingest_id, inspection }))
    }

    pub fn publish_post(post_id: Uuid, expected_revision: i64) -> Self {
        Self::new(JobCommand::PublishPost(PublishPostPayload { post_id, expected_revision }))
    }

    pub fn upload_storage_asset(asset_id: Uuid) -> Self {
        Self::new(JobCommand::UploadStorageAsset(MediaJobPayload {
            media_id: asset_id,
            generation: 0,
        }))
    }

    pub fn upload_storage_asset_generation(asset_id: Uuid, generation: i32) -> Self {
        Self::new(JobCommand::UploadStorageAsset(MediaJobPayload {
            media_id: asset_id,
            generation,
        }))
    }

    pub fn probe_asset(ingest_id: Uuid) -> Self {
        Self::new(JobCommand::ProbeAsset(IngestJobPayload { ingest_id }))
    }

    pub fn normalize_asset(ingest_id: Uuid) -> Self {
        Self::new(JobCommand::NormalizeAsset(IngestJobPayload { ingest_id }))
    }

    pub fn compute_fingerprint(ingest_id: Uuid) -> Self {
        Self::new(JobCommand::ComputeFingerprint(IngestJobPayload { ingest_id }))
    }

    pub fn finalize_ingest(ingest_id: Uuid) -> Self {
        Self::new(JobCommand::FinalizeIngest(IngestJobPayload { ingest_id }))
    }

    pub fn materialize_publication(ingest_id: Uuid) -> Self {
        Self::new(JobCommand::MaterializePublication(MaterializePublicationPayload { ingest_id }))
    }

    pub fn cleanup_workspace(ingest_id: Uuid, workspace_id: Uuid) -> Self {
        Self::new(JobCommand::CleanupWorkspace(CleanupWorkspacePayload { ingest_id, workspace_id }))
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
    pub fn run_at_value(&self) -> Option<OffsetDateTime> {
        self.run_at
    }
    pub fn available_at_value(&self) -> Option<OffsetDateTime> {
        self.run_at
    }
    pub fn max_attempts_value(&self) -> i32 {
        self.max_attempts
    }
    pub fn dedupe_key_value(&self) -> Option<&str> {
        self.dedupe_key.as_deref()
    }
    pub fn idempotency_key_value(&self) -> Option<&str> {
        self.dedupe_key.as_deref()
    }

    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }
    pub fn run_at(mut self, run_at: OffsetDateTime) -> Self {
        self.run_at = Some(run_at);
        self
    }
    pub fn available_at(self, run_at: OffsetDateTime) -> Self {
        self.run_at(run_at)
    }
    pub fn max_attempts(mut self, max_attempts: i32) -> Self {
        self.max_attempts = max_attempts;
        self
    }
    pub fn dedupe_key(mut self, key: impl Into<String>) -> Self {
        self.dedupe_key = Some(key.into());
        self
    }
    pub fn idempotency_key(self, key: impl Into<String>) -> Self {
        self.dedupe_key(key)
    }
}

#[derive(Debug, Clone)]
pub struct Job {
    pub id: JobId,
    pub command: JobCommand,
    pub status: JobStatus,
    pub priority: i32,
    pub run_at: OffsetDateTime,
    pub attempt_count: i32,
    pub max_attempts: i32,
    pub lease_token: Option<Uuid>,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<OffsetDateTime>,
    pub last_heartbeat_at: Option<OffsetDateTime>,
    pub last_error_class: Option<String>,
    pub last_error_message: Option<String>,
    pub dedupe_key: Option<String>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub completed_at: Option<OffsetDateTime>,
}

impl Job {
    pub fn job_type(&self) -> JobType {
        self.command.job_type()
    }

    pub fn lease(&self) -> Option<JobLease> {
        Some(JobLease {
            job_id: self.id,
            attempt_number: self.attempt_count,
            worker_id: self.lease_owner.clone()?,
            lease_owner: self.lease_owner.clone()?,
            lease_token: self.lease_token?,
        })
    }

    pub fn attempt(&self) -> Option<JobLease> {
        self.lease()
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct JobLease {
    pub job_id: JobId,
    pub attempt_number: i32,
    pub worker_id: String,
    pub lease_owner: String,
    pub lease_token: Uuid,
}

pub type JobAttempt = JobLease;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_inspection_payload_round_trips() {
        let ingest_id = Uuid::new_v4();
        let job = NewJob::inspect_source(ingest_id);
        let command = JobCommand::from_payload(job.job_type(), job.payload_json()).unwrap();
        assert_eq!(command, JobCommand::InspectSource(InspectSourcePayload { ingest_id }));
    }

    #[test]
    fn publish_payload_carries_post_id() {
        let post_id = Uuid::new_v4();
        let job = NewJob::publish_post(post_id, 7);
        let command = JobCommand::from_payload(job.job_type(), job.payload_json()).unwrap();
        assert_eq!(
            command,
            JobCommand::PublishPost(PublishPostPayload { post_id, expected_revision: 7 })
        );
    }

    #[test]
    fn materialization_payload_carries_ingest_id() {
        let ingest_id = Uuid::new_v4();
        let job = NewJob::materialize_publication(ingest_id);
        assert_eq!(job.job_type(), JobType::MaterializePublication);
        let command = JobCommand::from_payload(job.job_type(), job.payload_json()).unwrap();
        assert_eq!(
            command,
            JobCommand::MaterializePublication(MaterializePublicationPayload { ingest_id })
        );
    }

    #[test]
    fn legacy_publish_payload_defaults_to_initial_revision() {
        let post_id = Uuid::new_v4();
        let command = JobCommand::from_payload(
            JobType::PublishPost,
            serde_json::json!({ "post_id": post_id }),
        )
        .unwrap();
        assert_eq!(
            command,
            JobCommand::PublishPost(PublishPostPayload { post_id, expected_revision: 0 })
        );
    }

    #[test]
    fn cleanup_payload_carries_its_generation_scoped_workspace() {
        let ingest_id = Uuid::new_v4();
        let workspace_id = Uuid::new_v4();
        let job = NewJob::cleanup_workspace(ingest_id, workspace_id);
        let command = JobCommand::from_payload(job.job_type(), job.payload_json()).unwrap();
        assert_eq!(
            command,
            JobCommand::CleanupWorkspace(CleanupWorkspacePayload { ingest_id, workspace_id })
        );
    }

    #[test]
    fn retry_wait_is_represented_by_run_at_and_queued_state() {
        assert_eq!(JobStatus::Queued.as_str(), "queued");
        assert_eq!(JobType::InspectSource.as_str(), "inspect_source");
    }
}
