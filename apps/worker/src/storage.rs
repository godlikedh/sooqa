//! Telegram storage upload and caption synchronization jobs.

use crate::common::*;

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
