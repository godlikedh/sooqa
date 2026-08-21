use std::{
    collections::HashMap,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use sooqa_inbox::{IngestKind, SourceMediaKind};
use sooqa_jobs::{Job, JobType};
use sooqa_media::{
    DiskAdmissionError, FfmpegExecutor, FrameExtractor, WorkspaceError, check_disk_space,
};
use sooqa_persistence::LibraryRepositoryError;
use sooqa_persistence::{InboxRepository, InboxRepositoryError};
use sooqa_telegram::StorageUploadCancellation;
use time::OffsetDateTime;
use tracing::warn;
use uuid::Uuid;

pub(crate) const DISK_ADMISSION_RETRY_DELAY: time::Duration = time::Duration::minutes(1);

pub type HandlerFuture = Pin<Box<dyn Future<Output = Result<(), HandlerFailure>> + Send + 'static>>;
pub type HandlerFn = Arc<dyn Fn(Job) -> HandlerFuture + Send + Sync>;
pub type CancellableHandlerFn =
    Arc<dyn Fn(Job, HandlerCancellation) -> HandlerFuture + Send + Sync>;

#[derive(Clone, Debug)]
pub struct HandlerCancellation {
    storage_upload: StorageUploadCancellation,
}

impl HandlerCancellation {
    pub(crate) fn new() -> Self {
        Self { storage_upload: StorageUploadCancellation::new() }
    }

    pub(crate) fn cancel(&self) {
        self.storage_upload.cancel();
    }

    pub fn storage_upload(&self) -> StorageUploadCancellation {
        self.storage_upload.clone()
    }
}

/// Admission policy for operations that may create large workspace artifacts.
/// The policy is deliberately per operation: the filesystem check observes
/// bytes already consumed by other workers, while the operation amount is
/// expanded to the bounded two-worker worst-case before it is checked.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct WorkspaceAdmission {
    reserve_bytes: u64,
    enabled: bool,
}

impl WorkspaceAdmission {
    pub const fn disabled() -> Self {
        Self { reserve_bytes: 0, enabled: false }
    }

    pub const fn new(reserve_bytes: u64) -> Self {
        Self { reserve_bytes, enabled: true }
    }

    pub const fn reserve_bytes(self) -> u64 {
        self.reserve_bytes
    }

    pub(crate) fn admit(self, work_root: &Path, required_bytes: u64) -> Result<(), HandlerFailure> {
        if !self.enabled {
            return Ok(());
        }
        let concurrent_budget = sooqa_media::concurrent_operation_budget(
            required_bytes,
            sooqa_media::MAX_CONCURRENT_WORKSPACE_OPERATIONS,
        );
        match check_disk_space(work_root, self.reserve_bytes, concurrent_budget) {
            Ok(_) => Ok(()),
            Err(error) => Self::defer_for_space_error(work_root, required_bytes, error),
        }
    }

    pub(crate) fn defer_for_space_error(
        work_root: &Path,
        operation_bytes: u64,
        error: DiskAdmissionError,
    ) -> Result<(), HandlerFailure> {
        let message = error.to_string();
        match &error {
            DiskAdmissionError::Insufficient {
                available_bytes,
                reserve_bytes,
                required_bytes,
                ..
            } => warn!(
                work_root = %work_root.display(),
                available_bytes,
                reserve_bytes,
                required_bytes,
                operation_bytes,
                concurrent_budget_bytes = required_bytes,
                "work volume is below the configured two-worker admission reserve; deferring job"
            ),
            DiskAdmissionError::Stat { .. } => warn!(
                work_root = %work_root.display(),
                error = %message,
                "work volume free-space check failed; deferring job"
            ),
        }
        Err(HandlerFailure::defer_without_consuming_attempt(
            "work_disk_low",
            message,
            OffsetDateTime::now_utc() + DISK_ADMISSION_RETRY_DELAY,
        ))
    }
}

pub fn media_processing_components(
    ffmpeg_executable: impl Into<PathBuf>,
    ffprobe_executable: impl Into<PathBuf>,
    timeout: Duration,
) -> (FfmpegExecutor, FrameExtractor) {
    (
        FfmpegExecutor::new(
            Arc::new(sooqa_media::ProcessCommandRunner),
            ffprobe_executable,
            timeout,
        ),
        FrameExtractor::new(ffmpeg_executable, timeout),
    )
}

#[derive(Debug, Clone)]
pub struct HandlerFailure {
    pub retryable: bool,
    pub class: String,
    pub message: String,
    pub defer_until: Option<OffsetDateTime>,
    pub defer_without_consuming_attempt: bool,
    pub retry_without_consuming_attempt: bool,
    pub requires_storage_reconciliation: bool,
}

impl HandlerFailure {
    pub fn retryable(class: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            retryable: true,
            class: class.into(),
            message: message.into(),
            defer_until: None,
            defer_without_consuming_attempt: false,
            retry_without_consuming_attempt: false,
            requires_storage_reconciliation: false,
        }
    }

    pub fn retryable_without_consuming_attempt(
        class: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            retryable: true,
            class: class.into(),
            message: message.into(),
            defer_until: None,
            defer_without_consuming_attempt: false,
            retry_without_consuming_attempt: true,
            requires_storage_reconciliation: false,
        }
    }

    pub fn permanent(class: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            retryable: false,
            class: class.into(),
            message: message.into(),
            defer_until: None,
            defer_without_consuming_attempt: false,
            retry_without_consuming_attempt: false,
            requires_storage_reconciliation: false,
        }
    }

    pub fn defer(
        class: impl Into<String>,
        message: impl Into<String>,
        defer_until: OffsetDateTime,
    ) -> Self {
        Self {
            retryable: true,
            class: class.into(),
            message: message.into(),
            defer_until: Some(defer_until),
            defer_without_consuming_attempt: false,
            retry_without_consuming_attempt: false,
            requires_storage_reconciliation: false,
        }
    }

    pub fn defer_without_consuming_attempt(
        class: impl Into<String>,
        message: impl Into<String>,
        defer_until: OffsetDateTime,
    ) -> Self {
        Self {
            retryable: true,
            class: class.into(),
            message: message.into(),
            defer_until: Some(defer_until),
            defer_without_consuming_attempt: true,
            retry_without_consuming_attempt: false,
            requires_storage_reconciliation: false,
        }
    }

    pub fn storage_reconciliation_required(message: impl Into<String>) -> Self {
        Self {
            retryable: false,
            class: "storage_upload_unknown".to_owned(),
            message: message.into(),
            defer_until: None,
            defer_without_consuming_attempt: false,
            retry_without_consuming_attempt: false,
            requires_storage_reconciliation: true,
        }
    }
}

#[derive(Clone, Default)]
pub struct HandlerRegistry {
    handlers: HashMap<JobType, HandlerEntry>,
}

#[derive(Clone)]
pub(crate) struct HandlerEntry {
    pub(crate) handler: Option<HandlerFn>,
    pub(crate) cancellable_handler: Option<CancellableHandlerFn>,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<F>(&mut self, job_type: JobType, handler: F)
    where
        F: Fn(Job) -> HandlerFuture + Send + Sync + 'static,
    {
        self.handlers.insert(
            job_type,
            HandlerEntry { handler: Some(Arc::new(handler)), cancellable_handler: None },
        );
    }

    pub fn register_cancellable<F>(&mut self, job_type: JobType, handler: F)
    where
        F: Fn(Job, HandlerCancellation) -> HandlerFuture + Send + Sync + 'static,
    {
        self.handlers.insert(
            job_type,
            HandlerEntry { handler: None, cancellable_handler: Some(Arc::new(handler)) },
        );
    }

    pub fn contains(&self, job_type: JobType) -> bool {
        self.handlers.contains_key(&job_type)
    }

    pub fn job_types(&self) -> Vec<JobType> {
        let mut job_types = self.handlers.keys().copied().collect::<Vec<_>>();
        job_types.sort_by_key(|job_type| job_type.as_str());
        job_types
    }

    pub(crate) fn handler(&self, job_type: JobType) -> Option<HandlerEntry> {
        self.handlers.get(&job_type).cloned()
    }
}

pub(crate) fn map_workspace_error(error: WorkspaceError) -> HandlerFailure {
    HandlerFailure::permanent("workspace_error", error.to_string())
}

pub(crate) async fn load_ingest_for_admission(
    inbox: &InboxRepository,
    ingest_request_id: Uuid,
) -> Result<sooqa_inbox::Ingest, HandlerFailure> {
    inbox
        .find(ingest_request_id)
        .await
        .map_err(map_inbox_error)?
        .ok_or_else(|| HandlerFailure::permanent("ingest_missing", "ingest request was not found"))
}

pub(crate) async fn source_artifact_exists(path: &Path) -> Result<bool, HandlerFailure> {
    let metadata = match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(HandlerFailure::permanent(
                "source_reconstruction",
                format!("could not inspect source artifact: {error}"),
            ));
        }
    };
    if metadata.file_type().is_symlink() {
        return Err(HandlerFailure::permanent(
            "source_reconstruction",
            "source artifact is a symlink",
        ));
    }
    Ok(metadata.is_file() && metadata.len() > 0)
}

pub(crate) fn workspace_input(
    request: &sooqa_inbox::Ingest,
) -> Result<(Uuid, &'static str), HandlerFailure> {
    if request.kind == IngestKind::Url {
        return Ok((request.workspace_id, "source.bin"));
    }
    Ok((request.workspace_id, "telegram-input.bin"))
}

pub(crate) fn request_media_kind(request: &sooqa_inbox::Ingest) -> Option<SourceMediaKind> {
    request.input_data().ok()?.media_kind()
}

pub(crate) fn map_library_error(error: LibraryRepositoryError) -> HandlerFailure {
    let message = error.to_string();
    match error {
        LibraryRepositoryError::Database(_) => HandlerFailure::retryable("database_error", message),
        _ => HandlerFailure::permanent("library_error", message),
    }
}

pub(crate) fn map_inbox_error(error: InboxRepositoryError) -> HandlerFailure {
    let message = error.to_string();
    match error {
        InboxRepositoryError::Library(error) => map_library_error(error),
        InboxRepositoryError::InputEnvelope(_) => {
            HandlerFailure::permanent("invalid_ingest_state", message)
        }
        InboxRepositoryError::ResourceMissing(_)
        | InboxRepositoryError::MissingSourceUrl(_)
        | InboxRepositoryError::InvalidFailureStatus(_)
        | InboxRepositoryError::InvalidStateTransition(_)
        | InboxRepositoryError::UnknownIngestKind(_)
        | InboxRepositoryError::UnknownIngestStatus(_)
        | InboxRepositoryError::UnknownSubmittedVia(_)
        | InboxRepositoryError::VideoFinalizationNotAllowed
        | InboxRepositoryError::DuplicateEvidenceTooManyMatches { .. }
        | InboxRepositoryError::DuplicateEvidenceTooLarge { .. } => {
            HandlerFailure::permanent("invalid_ingest_state", message)
        }
        _ => HandlerFailure::retryable("database_error", message),
    }
}
