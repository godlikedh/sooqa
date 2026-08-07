//! Download, inspection, normalization, and fingerprinting boundaries for sooqa.

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

mod direct_http;
mod hashing;
mod workspace;

pub use direct_http::{DirectHttpDownloader, HostResolver, ResolvedAddress};
pub use hashing::{FileDigest, HashError, sha256_file};
pub use workspace::{
    ManifestEntry, MediaWorkspace, WorkspaceArea, WorkspaceError, WorkspaceManifest,
};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceInput {
    pub ingest_request_id: Uuid,
    pub source_url: String,
    pub page_url: Option<String>,
}

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

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DownloadLimits {
    pub max_bytes: u64,
    pub max_redirects: u32,
    pub timeout: Duration,
}

impl Default for DownloadLimits {
    fn default() -> Self {
        Self {
            max_bytes: 2 * 1024 * 1024 * 1024,
            max_redirects: 5,
            timeout: Duration::from_secs(300),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DownloadedSource {
    pub path: PathBuf,
    pub bytes: u64,
    pub mime_type: Option<String>,
}

#[async_trait]
pub trait SourceDownloader: Send + Sync {
    async fn inspect(&self, source: &SourceInput) -> Result<SourceInspection, DownloadError>;

    async fn download(
        &self,
        _inspection: &SourceInspection,
        _destination: &Path,
        _limits: &DownloadLimits,
    ) -> Result<DownloadedSource, DownloadError> {
        Err(DownloadError::NotImplemented)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Error)]
pub enum DownloadError {
    #[error("{message}")]
    Retryable { class: String, message: String },
    #[error("{message}")]
    Terminal { class: String, message: String },
    #[error("source downloader is not implemented")]
    NotImplemented,
}

impl DownloadError {
    pub fn retryable(class: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Retryable { class: class.into(), message: message.into() }
    }

    pub fn terminal(class: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Terminal { class: class.into(), message: message.into() }
    }

    pub fn class(&self) -> &str {
        match self {
            Self::Retryable { class, .. } | Self::Terminal { class, .. } => class,
            Self::NotImplemented => "downloader_not_implemented",
        }
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_inspection_round_trips_as_job_payload() {
        let inspection = SourceInspection {
            adapter: "fake".to_owned(),
            source_url: "https://example.com/video".to_owned(),
            resolved_url: Some("https://cdn.example.com/video.mp4".to_owned()),
            media_kind: SourceMediaKind::Video,
            mime_type: Some("video/mp4".to_owned()),
            content_length_bytes: Some(42),
            title: Some("Example".to_owned()),
            metadata: serde_json::json!({"duration_seconds": 2}),
        };

        let value = serde_json::to_value(&inspection).expect("inspection should serialize");
        let decoded: SourceInspection =
            serde_json::from_value(value).expect("inspection should deserialize");
        assert_eq!(decoded, inspection);
    }
}
