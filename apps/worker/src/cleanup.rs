//! Workspace cleanup jobs.

use crate::common::*;

pub fn cleanup_workspace_handler(
    inbox: InboxRepository,
    work_root: impl Into<PathBuf>,
) -> HandlerFn {
    let work_root = work_root.into();
    Arc::new(move |job| {
        let inbox = inbox.clone();
        let work_root = work_root.clone();
        Box::pin(async move { cleanup_workspace(&inbox, &work_root, job).await })
    })
}

async fn cleanup_workspace(
    inbox: &InboxRepository,
    work_root: &Path,
    job: Job,
) -> Result<(), HandlerFailure> {
    let (ingest_id, workspace_id) = match &job.command {
        JobCommand::CleanupWorkspace(payload) => (payload.ingest_id, payload.workspace_id),
        _ => {
            return Err(HandlerFailure::permanent(
                "invalid_payload",
                "cleanup_workspace handler received a different job command",
            ));
        }
    };
    let job_attempt = job.lease().ok_or_else(|| {
        HandlerFailure::permanent(
            "invalid_job_state",
            "cleanup_workspace handler requires a running job lease",
        )
    })?;
    let start = inbox
        .begin_workspace_cleanup(&job_attempt, ingest_id, workspace_id)
        .await
        .map_err(map_inbox_error)?;
    match start {
        WorkspaceCleanupStart::Deferred => {
            return Err(HandlerFailure::defer(
                "workspace_protected",
                "workspace is still protected by durable ingest or storage state",
                OffsetDateTime::now_utc() + TimeDuration::minutes(1),
            ));
        }
        WorkspaceCleanupStart::AlreadyAdvanced => return Ok(()),
        WorkspaceCleanupStart::Ready => {}
    }

    if let Err(error) = MediaWorkspace::cleanup_existing(work_root, workspace_id).await {
        let message = error.to_string();
        return Err(if matches!(error, WorkspaceError::Io { .. }) {
            HandlerFailure::retryable("workspace_cleanup", message)
        } else {
            HandlerFailure::permanent("workspace_cleanup", message)
        });
    }
    Ok(())
}
