//! Telegram storage upload and caption synchronization jobs.

use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use sooqa_inbox::IngestStatus;
use sooqa_jobs::{Job, JobCommand};
use sooqa_library::StorageUploadStore;
use sooqa_persistence::{InboxRepository, LibraryRepository};
use sooqa_telegram::{
    StorageCaptionEditRequest, StorageUploadCancellation, StorageUploadError, StorageUploadInput,
    StorageUploadProvider, TelegramStorageApi, TelegramStorageCaptionApi, storage_caption,
};
use tracing::{error, info, warn};

use crate::common::{
    CancellableHandlerFn, HandlerFailure, HandlerFn, map_inbox_error, map_library_error,
};

/// A best-effort startup probe for optional Telegram storage.  It is
/// intentionally separate from [`Worker::run`]: a remote outage must not
/// prevent non-storage jobs from being claimed, and storage jobs continue to
/// use their durable handler retry policy.
#[async_trait]
pub trait StoragePreflight: Send + 'static {
    type Error: Display + Send + Sync + 'static;

    async fn verify_storage_chat(&self) -> Result<(), Self::Error>;

    fn is_terminal_configuration(error: &Self::Error) -> bool;
}

#[async_trait]
impl<A, S> StoragePreflight for StorageUploadProvider<A, S>
where
    A: sooqa_telegram::TelegramStorageApi,
    S: StorageUploadStore,
{
    type Error = StorageUploadError;

    async fn verify_storage_chat(&self) -> Result<(), Self::Error> {
        StorageUploadProvider::verify_storage_chat(self).await
    }

    fn is_terminal_configuration(error: &Self::Error) -> bool {
        error.is_terminal_configuration()
    }
}

pub fn spawn_storage_preflight<P>(preflight: P, storage_chat_id: i64) -> tokio::task::JoinHandle<()>
where
    P: StoragePreflight,
{
    tokio::spawn(async move {
        match preflight.verify_storage_chat().await {
            Ok(()) => info!(
                target: "sooqa.telegram",
                status = "ready",
                phase = "worker_storage_preflight",
                storage_chat_id,
                "Telegram storage chat preflight passed"
            ),
            Err(error) if P::is_terminal_configuration(&error) => error!(
                target: "sooqa.telegram",
                status = "terminally_misconfigured",
                phase = "worker_storage_preflight",
                storage_chat_id,
                error = %error,
                "Telegram storage preflight found invalid remote permissions or authentication; storage jobs remain enabled"
            ),
            Err(error) => warn!(
                target: "sooqa.telegram",
                status = "degraded",
                phase = "worker_storage_preflight",
                storage_chat_id,
                error = %error,
                "Telegram storage preflight unavailable; storage jobs will use normal retry policy"
            ),
        }
    })
}

pub fn upload_storage_asset_handler<A, S>(
    inbox: InboxRepository,
    provider: StorageUploadProvider<A, S>,
) -> HandlerFn
where
    A: TelegramStorageApi,
    S: StorageUploadStore,
{
    Arc::new(move |job| {
        let inbox = inbox.clone();
        let provider = provider.clone();
        Box::pin(async move {
            upload_storage_asset(&inbox, &provider, job, StorageUploadCancellation::new()).await
        })
    })
}

pub fn upload_storage_asset_cancellable_handler<A, S>(
    inbox: InboxRepository,
    provider: StorageUploadProvider<A, S>,
) -> CancellableHandlerFn
where
    A: TelegramStorageApi,
    S: StorageUploadStore,
{
    Arc::new(move |job, cancellation| {
        let inbox = inbox.clone();
        let provider = provider.clone();
        Box::pin(async move {
            upload_storage_asset(&inbox, &provider, job, cancellation.storage_upload()).await
        })
    })
}

pub fn sync_storage_caption_handler<A>(library: LibraryRepository, api: A) -> HandlerFn
where
    A: TelegramStorageCaptionApi,
{
    Arc::new(move |job| {
        let library = library.clone();
        let api = api.clone();
        Box::pin(async move { sync_storage_caption(&library, &api, job).await })
    })
}

async fn sync_storage_caption<A>(
    library: &LibraryRepository,
    api: &A,
    job: Job,
) -> Result<(), HandlerFailure>
where
    A: TelegramStorageCaptionApi,
{
    let (media_id, generation) = match &job.command {
        JobCommand::SyncStorageCaption(payload) => (payload.media_id, payload.generation),
        _ => {
            return Err(HandlerFailure::permanent(
                "invalid_payload",
                "sync_storage_caption handler received a different job command",
            ));
        }
    };
    let job_attempt = job.lease().ok_or_else(|| {
        HandlerFailure::permanent(
            "invalid_job_state",
            "sync_storage_caption handler requires a running job lease",
        )
    })?;
    let Some(claim) = library
        .begin_caption_sync(media_id, generation, job_attempt.lease_token)
        .await
        .map_err(map_library_error)?
    else {
        return Ok(());
    };
    let caption = storage_caption(&claim.metadata);
    let request = StorageCaptionEditRequest {
        storage_chat_id: claim.storage_chat_id,
        storage_message_id: claim.storage_message_id,
        caption,
    };
    match api.edit_storage_caption(request).await {
        Ok(()) => {
            library
                .complete_caption_sync(
                    media_id,
                    generation,
                    job_attempt.lease_token,
                    true,
                    false,
                    None,
                )
                .await
                .map_err(map_library_error)?;
            Ok(())
        }
        Err(error) => {
            let message = error.to_string();
            let retryable = A::is_retryable_error(&error) && job.attempt_count < job.max_attempts;
            library
                .complete_caption_sync(
                    media_id,
                    generation,
                    job_attempt.lease_token,
                    false,
                    retryable,
                    Some(&message),
                )
                .await
                .map_err(map_library_error)?;
            Err(if retryable {
                HandlerFailure::retryable("caption_sync", message)
            } else {
                HandlerFailure::permanent("caption_sync", message)
            })
        }
    }
}

async fn upload_storage_asset<A, S>(
    inbox: &InboxRepository,
    provider: &StorageUploadProvider<A, S>,
    job: Job,
    cancellation: StorageUploadCancellation,
) -> Result<(), HandlerFailure>
where
    A: TelegramStorageApi,
    S: StorageUploadStore,
{
    let (media_id, generation) = match &job.command {
        JobCommand::UploadStorageAsset(payload) => (payload.media_id, payload.generation),
        _ => {
            return Err(HandlerFailure::permanent(
                "invalid_payload",
                "upload_storage_asset handler received a different job command",
            ));
        }
    };

    match provider
        .upload_with_cancellation(StorageUploadInput { media_id, generation }, cancellation)
        .await
    {
        Ok(_) => {
            inbox.complete_storage_for_media(media_id).await.map_err(map_inbox_error)?;
            Ok(())
        }
        Err(error) => {
            let message = error.to_string();
            if matches!(&error, StorageUploadError::StaleGeneration { .. }) {
                return Ok(());
            }
            if matches!(&error, StorageUploadError::CancelledBeforeDispatch) {
                return Err(HandlerFailure::retryable_without_consuming_attempt(
                    "storage_upload_cancelled",
                    message,
                ));
            }
            if matches!(&error, StorageUploadError::AmbiguousPersistence(_)) {
                return Err(HandlerFailure::storage_reconciliation_required(message));
            }
            if error.is_ambiguous() {
                return Err(HandlerFailure::permanent("storage_upload_unknown", message));
            }
            if let StorageUploadError::InProgress { retry_at: Some(retry_at) } = &error
                && job.attempt_count < job.max_attempts
            {
                return Err(HandlerFailure::defer(
                    "storage_upload_in_progress",
                    message,
                    *retry_at,
                ));
            }
            let terminal = !error.is_retryable() || job.attempt_count >= job.max_attempts;
            let status =
                if terminal { IngestStatus::FailedTerminal } else { IngestStatus::FailedRetryable };
            inbox
                .fail_storage_for_media(media_id, status, "storage_upload", &message)
                .await
                .map_err(map_inbox_error)?;
            if terminal {
                Err(HandlerFailure::permanent("storage_upload", message))
            } else {
                Err(HandlerFailure::retryable("storage_upload", message))
            }
        }
    }
}
