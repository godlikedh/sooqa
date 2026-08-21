//! Durable publication materialization and send jobs.

use crate::common::*;

pub fn publish_post_handler<A>(
    publisher: sooqa_persistence::PublisherRepository,
    library: LibraryRepository,
    telegram: A,
) -> HandlerFn
where
    A: TelegramPublicationApi,
{
    Arc::new(move |job| {
        let publisher = publisher.clone();
        let library = library.clone();
        let telegram = telegram.clone();
        Box::pin(async move { publish_post(&publisher, &library, &telegram, job).await })
    })
}

pub fn materialize_publication_handler(
    publisher: sooqa_persistence::PublisherRepository,
) -> HandlerFn {
    Arc::new(move |job| {
        let publisher = publisher.clone();
        Box::pin(async move { materialize_publication(&publisher, job).await })
    })
}

async fn publish_post<A>(
    publisher: &sooqa_persistence::PublisherRepository,
    library: &LibraryRepository,
    telegram: &A,
    job: Job,
) -> Result<(), HandlerFailure>
where
    A: TelegramPublicationApi,
{
    let (post_id, expected_revision) = match &job.command {
        JobCommand::PublishPost(payload) => (payload.post_id, payload.expected_revision),
        _ => {
            return Err(HandlerFailure::permanent(
                "invalid_payload",
                "publish_post handler received a different job command",
            ));
        }
    };
    let attempt = job.lease().ok_or_else(|| {
        HandlerFailure::permanent(
            "invalid_job_state",
            "publish_post handler requires a running job lease",
        )
    })?;
    let claim = match publisher.claim_publish(post_id, expected_revision, &attempt).await {
        Ok(claim) => claim,
        Err(sooqa_persistence::PublisherRepositoryError::PostNotClaimable {
            state: sooqa_publisher::PostState::Sending,
            ..
        }) => match publisher.reconcile_interrupted_publish(post_id, &attempt).await {
            Ok(_) | Err(sooqa_persistence::PublisherRepositoryError::PublishLeaseLost(_)) => {
                return Ok(());
            }
            Err(error) => return Err(map_publisher_error(error)),
        },
        Err(
            sooqa_persistence::PublisherRepositoryError::PostNotClaimable { .. }
            | sooqa_persistence::PublisherRepositoryError::StalePublicationJob { .. }
            | sooqa_persistence::PublisherRepositoryError::PublishLeaseLost(_),
        ) => return Ok(()),
        Err(error) => return Err(map_publisher_error(error)),
    };
    let token = claim.post.send_token.ok_or_else(|| {
        HandlerFailure::permanent(
            "invalid_publication_state",
            "publication claim has no send token",
        )
    })?;
    let lease = sooqa_persistence::PublishLease {
        generation: claim.post.send_generation,
        token,
        attempt: attempt.clone(),
    };
    let receipt = match library.find_storage_receipt(claim.post.media_id).await {
        Ok(Some(receipt)) => receipt,
        Ok(None) => {
            return record_publication_failure(
                publisher,
                &claim,
                &lease,
                sooqa_publisher::PostState::Failed,
                "storage_receipt_missing",
                "media has no ready Telegram storage receipt",
            )
            .await;
        }
        Err(error) if matches!(error, LibraryRepositoryError::Database(_)) => {
            return settle_pre_send_retryable_error(
                publisher,
                &claim,
                &lease,
                "storage_receipt_lookup",
                &error.to_string(),
            )
            .await;
        }
        Err(error) => {
            return record_publication_failure(
                publisher,
                &claim,
                &lease,
                sooqa_publisher::PostState::Failed,
                "storage_receipt_invalid",
                &error.to_string(),
            )
            .await;
        }
    };
    let request = TelegramPublicationRequest {
        target_chat_id: claim.channel_chat_id,
        storage_chat_id: receipt.storage_chat_id,
        storage_message_id: receipt.storage_message_id,
        telegram_file_id: receipt.telegram_file_id,
        media_kind: receipt.media_kind,
        caption: claim.post.caption.clone(),
        parse_mode: claim.post.parse_mode.clone(),
        disable_notification: claim.post.disable_notification,
    };
    let message_id = match telegram.copy_from_storage(&request).await {
        Ok(message_id) => message_id,
        Err(error) if A::is_copy_unavailable(&error) => {
            if request.telegram_file_id.is_none() {
                return record_publication_failure(
                    publisher,
                    &claim,
                    &lease,
                    sooqa_publisher::PostState::Failed,
                    "storage_file_reference_missing",
                    "copyMessage was unavailable and the storage receipt has no file ID",
                )
                .await;
            }
            match telegram.send_storage_file(&request).await {
                Ok(message_id) => message_id,
                Err(error) => {
                    return settle_publication_transport_error::<A>(
                        publisher, &claim, &lease, error,
                    )
                    .await;
                }
            }
        }
        Err(error) => {
            return settle_publication_transport_error::<A>(publisher, &claim, &lease, error).await;
        }
    };

    match publisher.complete_publish(post_id, &lease, message_id).await {
        Ok(_) | Err(sooqa_persistence::PublisherRepositoryError::PublishConflict(_)) => Ok(()),
        Err(sooqa_persistence::PublisherRepositoryError::PublishLeaseLost(_)) => Ok(()),
        Err(error) => {
            let message = error.to_string();
            match publisher
                .fail_publish(
                    post_id,
                    &lease,
                    sooqa_publisher::PostState::Unknown,
                    "publication_commit",
                    &message,
                )
                .await
            {
                Ok(_) => Err(HandlerFailure::permanent("publication_commit", message)),
                Err(sooqa_persistence::PublisherRepositoryError::PublishLeaseLost(_)) => Ok(()),
                Err(failure) => Err(HandlerFailure::permanent(
                    "publication_commit",
                    format!("{message}; could not fence the publication as unknown: {failure}"),
                )),
            }
        }
    }
}

async fn materialize_publication(
    publisher: &sooqa_persistence::PublisherRepository,
    job: Job,
) -> Result<(), HandlerFailure> {
    let ingest_id = match &job.command {
        JobCommand::MaterializePublication(payload) => payload.ingest_id,
        _ => {
            return Err(HandlerFailure::permanent(
                "invalid_payload",
                "materialize_publication handler received a different job command",
            ));
        }
    };
    publisher.materialize_ingest(ingest_id).await.map(|_| ()).map_err(map_publisher_error)
}

async fn settle_publication_transport_error<A>(
    publisher: &sooqa_persistence::PublisherRepository,
    claim: &sooqa_publisher::PublishClaim,
    lease: &sooqa_persistence::PublishLease,
    error: A::Error,
) -> Result<(), HandlerFailure>
where
    A: TelegramPublicationApi,
{
    let message = error.to_string();
    if A::is_retryable_no_effect(&error) {
        return match publisher
            .retry_publish(claim.post.id, lease, "telegram_retryable", &message)
            .await
        {
            Ok(result) if result.terminal => {
                Err(HandlerFailure::permanent("telegram_retryable", message))
            }
            Ok(_) => Err(HandlerFailure::retryable("telegram_retryable", message)),
            Err(sooqa_persistence::PublisherRepositoryError::PublishLeaseLost(_)) => Ok(()),
            Err(error) => Err(HandlerFailure::permanent(
                "publication_state",
                format!("could not requeue a safe publication retry: {error}"),
            )),
        };
    }
    let (state, class) = if A::is_known_caption_error(&error) {
        (sooqa_publisher::PostState::Failed, "caption_rejected")
    } else if A::is_ambiguous_error(&error) {
        (sooqa_publisher::PostState::Unknown, "publication_unknown")
    } else {
        (sooqa_publisher::PostState::Failed, "publication_rejected")
    };
    record_publication_failure(publisher, claim, lease, state, class, &message).await
}

async fn settle_pre_send_retryable_error(
    publisher: &sooqa_persistence::PublisherRepository,
    claim: &sooqa_publisher::PublishClaim,
    lease: &sooqa_persistence::PublishLease,
    error_class: &str,
    error_message: &str,
) -> Result<(), HandlerFailure> {
    match publisher.retry_publish(claim.post.id, lease, error_class, error_message).await {
        Ok(result) if result.terminal => Err(HandlerFailure::permanent(error_class, error_message)),
        Ok(_) => Err(HandlerFailure::retryable(error_class, error_message)),
        Err(sooqa_persistence::PublisherRepositoryError::PublishLeaseLost(_)) => Ok(()),
        Err(error) => Err(HandlerFailure::permanent(
            "publication_state",
            format!("could not requeue a safe publication retry: {error}"),
        )),
    }
}

async fn record_publication_failure(
    publisher: &sooqa_persistence::PublisherRepository,
    claim: &sooqa_publisher::PublishClaim,
    lease: &sooqa_persistence::PublishLease,
    state: sooqa_publisher::PostState,
    error_class: &str,
    error_message: &str,
) -> Result<(), HandlerFailure> {
    match publisher.fail_publish(claim.post.id, lease, state, error_class, error_message).await {
        Ok(_) => Err(HandlerFailure::permanent(error_class, error_message)),
        Err(sooqa_persistence::PublisherRepositoryError::PublishLeaseLost(_)) => Ok(()),
        Err(error) => Err(HandlerFailure::permanent(
            "publication_state",
            format!("could not record publication outcome: {error}"),
        )),
    }
}

fn map_publisher_error(error: sooqa_persistence::PublisherRepositoryError) -> HandlerFailure {
    let message = error.to_string();
    match error {
        sooqa_persistence::PublisherRepositoryError::Database(_) => {
            HandlerFailure::retryable("database_error", message)
        }
        sooqa_persistence::PublisherRepositoryError::ChannelDisabled(_)
        | sooqa_persistence::PublisherRepositoryError::MediaNotReady { .. }
        | sooqa_persistence::PublisherRepositoryError::MaterializationNotReady { .. } => {
            HandlerFailure::retryable("publication_dependency", message)
        }
        _ => HandlerFailure::permanent("publication_state", message),
    }
}
