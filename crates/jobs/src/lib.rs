//! Durable job values and execution boundaries for sooqa.

use std::fmt;

use serde_json::Value;
use time::OffsetDateTime;
use uuid::Uuid;

pub type JobId = Uuid;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
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

#[derive(Debug, Clone)]
pub struct NewJob {
    pub job_type: JobType,
    pub payload: Value,
    pub priority: i32,
    pub available_at: Option<OffsetDateTime>,
    pub max_attempts: i32,
    pub idempotency_key: Option<String>,
}

impl NewJob {
    pub fn new(job_type: JobType, payload: Value) -> Self {
        Self {
            job_type,
            payload,
            priority: 0,
            available_at: None,
            max_attempts: 5,
            idempotency_key: None,
        }
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
    pub job_type: JobType,
    pub payload: Value,
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
    fn new_job_has_safe_defaults() {
        let job =
            NewJob::new(JobType::InspectSource, serde_json::json!({"url": "https://example.com"}));

        assert_eq!(job.priority, 0);
        assert_eq!(job.max_attempts, 5);
        assert!(job.available_at.is_none());
        assert!(job.idempotency_key.is_none());
    }
}
