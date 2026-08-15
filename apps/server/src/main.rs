//! HTTP server entry point for sooqa.

use std::error::Error;

use axum::Router;
use sooqa_config::{AppConfig, AppRole, CliCommand, CliOptions, ConfigError, StorageCommand};
use sooqa_inbox::{
    IngestSubmission, IngestSubmissionInput, IngestValidationError, SubmittedVia,
    TelegramSubmissionInput,
};
use sooqa_library::StorageUploadAttachment;
use sooqa_persistence::{DuplicateCandidate, InboxRepository};
use sooqa_telegram::{
    DuplicateCandidateCard, DuplicateCandidateStorage, DuplicateDecisionResult,
    DuplicatePendingCard, IngestAccepted, IngestService, MediaIngestCommand, MemoryUpdateStore,
    TelegramRuntime, UrlIngestCommand,
};
use tokio::net::TcpListener;
use uuid::Uuid;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("sooqa-server: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let options = CliOptions::parse(std::env::args().skip(1))?;
    let config = AppConfig::load(AppRole::Server, options.config_path.as_deref())?;

    if options.check_config {
        println!("{}", config.summary());
        return Ok(());
    }

    if let Some(command) = options.command {
        let database_url = config
            .secrets
            .database_url
            .as_ref()
            .ok_or(ConfigError::MissingSecret("database URL"))?;
        let database = sooqa_persistence::Database::connect_secret(
            database_url,
            config.database.max_connections,
        )
        .await?;
        match command {
            CliCommand::Migrate => {
                database.migrate().await?;
                println!("sooqa-server: database migrations applied");
            }
            CliCommand::Storage(command) => {
                run_storage_command(&database, command).await?;
            }
        }
        return Ok(());
    }

    sooqa_runtime::init_tracing(&config.observability)?;
    let database_url =
        config.secrets.database_url.as_ref().ok_or(ConfigError::MissingSecret("database URL"))?;
    let database =
        sooqa_persistence::Database::connect_secret(database_url, config.database.max_connections)
            .await?;
    let api_token = config
        .secrets
        .api_token
        .as_ref()
        .filter(|token| token.is_configured())
        .ok_or(ConfigError::MissingSecret("API token"))?;
    let listener = TcpListener::bind(&config.server.listen_address).await?;
    let api_settings = sooqa_api::ApiSettings {
        request_body_limit_bytes: config.server.request_body_limit_bytes,
        request_timeout_seconds: config.server.request_timeout_seconds,
    };
    let app: Router = sooqa_api::router(
        api_settings,
        sooqa_api::ApiState::new(
            database.inbox(),
            api_token.expose_secret(),
            database.library(),
            database.publisher(),
        ),
    )
    .merge(sooqa_server::admin_router());

    tracing::info!(role = %config.role, "sooqa server started");
    let server = sooqa_server::serve(listener, app, sooqa_runtime::shutdown_signal());
    if let Some(token) =
        config.secrets.telegram_bot_token.as_ref().filter(|token| token.is_configured())
    {
        let telegram = TelegramRuntime::new(
            token.expose_secret(),
            &config.telegram.api_base_url,
            std::time::Duration::from_secs(config.telegram.poll_timeout_seconds),
            MemoryUpdateStore::default(),
            config.telegram.admin_user_ids.clone(),
            config.telegram.storage_chat_id,
            DatabaseIngestService { repository: database.inbox() },
        )?
        .with_upload_timeout(std::time::Duration::from_secs(
            config.telegram.upload_timeout_seconds,
        ))?
        .with_source_download_max_bytes(config.telegram.source_download_max_bytes);
        tracing::info!(api_base_url = %config.telegram.api_base_url, "Telegram bot polling enabled");
        tokio::select! {
            result = server => result?,
            result = telegram.run() => result?,
        }
    } else {
        server.await?;
    }
    tracing::info!(role = %config.role, "sooqa server stopped");

    Ok(())
}

async fn run_storage_command(
    database: &sooqa_persistence::Database,
    command: StorageCommand,
) -> Result<(), Box<dyn Error>> {
    match command {
        StorageCommand::List => {
            println!(
                "media_id\tstate\tgeneration\tstorage_chat_id\tstorage_message_id\tfile_id\tfile_unique_id\tupdated_at"
            );
            for upload in database.library().list_storage_uploads().await? {
                println!(
                    "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                    upload.media_id,
                    upload.state,
                    upload.generation,
                    upload.storage_chat_id.map_or_else(|| "-".to_owned(), |id| id.to_string()),
                    upload.storage_message_id.map_or_else(|| "-".to_owned(), |id| id.to_string()),
                    upload.file_id.as_deref().unwrap_or("-"),
                    upload.file_unique_id.as_deref().unwrap_or("-"),
                    upload.updated_at,
                );
            }
        }
        StorageCommand::MarkUnknown { media_id, force } => {
            let media_id = parse_uuid("media-id", &media_id)?;
            database.library().mark_storage_upload_unknown(media_id, force).await?;
            println!("sooqa-server: storage upload for media {media_id} marked unknown");
        }
        StorageCommand::Reset { media_id } => {
            let media_id = parse_uuid("media-id", &media_id)?;
            database.library().reset_storage_upload(media_id).await?;
            println!("sooqa-server: storage upload for media {media_id} reset");
        }
        StorageCommand::Attach {
            media_id,
            generation,
            storage_chat_id,
            storage_message_id,
            telegram_file_id,
            telegram_file_unique_id,
        } => {
            let media_id = parse_uuid("media-id", &media_id)?;
            let generation = parse_i32("generation", &generation)?;
            let storage_chat_id = parse_i64("storage-chat-id", &storage_chat_id)?;
            let storage_message_id = parse_i64("storage-message-id", &storage_message_id)?;
            let object = database
                .library()
                .attach_storage_upload(
                    media_id,
                    generation,
                    StorageUploadAttachment {
                        storage_chat_id,
                        storage_message_id,
                        telegram_file_id: Some(telegram_file_id),
                        telegram_file_unique_id: Some(telegram_file_unique_id),
                        caption_metadata: None,
                    },
                )
                .await?;
            println!(
                "sooqa-server: storage upload for media {media_id} attached at message {}",
                object.storage_message_id
            );
        }
    }
    Ok(())
}

fn parse_uuid(name: &str, value: &str) -> Result<Uuid, Box<dyn Error>> {
    value.parse().map_err(|error| format!("{name} is not a UUID: {error}").into())
}

fn parse_i64(name: &str, value: &str) -> Result<i64, Box<dyn Error>> {
    value.parse().map_err(|error| format!("{name} is not an integer: {error}").into())
}

fn parse_i32(name: &str, value: &str) -> Result<i32, Box<dyn Error>> {
    value.parse().map_err(|error| format!("{name} is not an integer: {error}").into())
}

#[derive(Clone)]
struct DatabaseIngestService {
    repository: InboxRepository,
}

#[derive(Debug, thiserror::Error)]
enum TelegramIngestError {
    #[error("Telegram URL validation failed: {0}")]
    Validation(#[from] IngestValidationError),
    #[error("Telegram ingest repository failed: {0}")]
    Repository(#[from] sooqa_persistence::InboxRepositoryError),
}

#[async_trait::async_trait]
impl IngestService for DatabaseIngestService {
    type Error = TelegramIngestError;

    async fn create_url(&self, command: UrlIngestCommand) -> Result<IngestAccepted, Self::Error> {
        let mut input = IngestSubmissionInput::new(command.source_url, SubmittedVia::TelegramBot);
        input.idempotency_key = Some(command.idempotency_key);
        let submission = IngestSubmission::try_new(input)?;
        let result = self.repository.create_ingest(submission).await?;
        Ok(IngestAccepted {
            request_id: result.ingest.id,
            status: result.ingest.status.as_str().to_owned(),
        })
    }

    async fn create_media(
        &self,
        command: MediaIngestCommand,
    ) -> Result<IngestAccepted, Self::Error> {
        let source_reference = format!("telegram://{}/{}", command.chat_id, command.message_id);
        let original_input = serde_json::json!({
            "source_type": "telegram",
            "telegram_update_id": command.update_id,
            "telegram_chat_id": command.chat_id,
            "telegram_message_id": command.message_id,
            "telegram_user_id": command.submitted_by_user_id,
            "telegram_file_id": command.file_id,
            "telegram_file_unique_id": command.file_unique_id,
            "file_size": command.file_size,
            "mime_type": command.mime_type,
            "file_name": command.file_name,
            "media_kind": command.media_kind.map(sooqa_library::MediaKind::as_str),
        });
        let submission = IngestSubmission::try_new_telegram(TelegramSubmissionInput {
            source_reference,
            submitted_via: SubmittedVia::TelegramBot,
            original_input,
            supplied_caption: command.caption,
            idempotency_key: Some(command.idempotency_key),
        })?;
        let result = self.repository.create_ingest(submission).await?;
        Ok(IngestAccepted {
            request_id: result.ingest.id,
            status: result.ingest.status.as_str().to_owned(),
        })
    }

    async fn list_duplicate_pending(
        &self,
        limit: usize,
    ) -> Result<Vec<DuplicatePendingCard>, Self::Error> {
        let pending = self.repository.list_duplicate_pending(limit as u32).await?;
        Ok(pending
            .into_iter()
            .map(|pending| DuplicatePendingCard {
                request_id: pending.ingest.id,
                source_url: pending.ingest.source_url,
                candidates: pending.candidates.into_iter().map(duplicate_candidate_card).collect(),
            })
            .collect())
    }

    async fn accept_duplicate(
        &self,
        request_id: Uuid,
        media_id: Uuid,
    ) -> Result<DuplicateDecisionResult, Self::Error> {
        let result = self.repository.accept_duplicate(request_id, media_id).await?;
        Ok(DuplicateDecisionResult {
            request_id: result.ingest.id,
            status: result.ingest.status.as_str().to_owned(),
            media_id: result.ingest.media_id,
        })
    }

    async fn force_save(&self, request_id: Uuid) -> Result<DuplicateDecisionResult, Self::Error> {
        let result = self.repository.force_save(request_id).await?;
        Ok(DuplicateDecisionResult {
            request_id: result.ingest.id,
            status: result.ingest.status.as_str().to_owned(),
            media_id: result.ingest.media_id,
        })
    }
}

fn duplicate_candidate_card(candidate: DuplicateCandidate) -> DuplicateCandidateCard {
    let storage = match candidate.storage_state.as_str() {
        "ready" => DuplicateCandidateStorage::Ready {
            open_url: candidate
                .storage_chat_id
                .zip(candidate.storage_message_id)
                .and_then(|(chat_id, message_id)| storage_message_url(chat_id, message_id)),
        },
        "pending_storage" => DuplicateCandidateStorage::PendingStorage,
        "missing" => DuplicateCandidateStorage::Missing,
        state => DuplicateCandidateStorage::Unavailable { state: state.to_owned() },
    };
    DuplicateCandidateCard {
        media_id: candidate.media_id,
        classification: match candidate.classification {
            sooqa_library::VideoDuplicateClassification::StrongDuplicate => {
                "strong_duplicate".to_owned()
            }
            sooqa_library::VideoDuplicateClassification::PartialMatch => "partial_match".to_owned(),
        },
        score_bps: candidate.score_bps,
        storage,
    }
}

fn storage_message_url(chat_id: i64, message_id: i64) -> Option<String> {
    if chat_id >= 0 || message_id <= 0 {
        return None;
    }
    let raw_id = chat_id.to_string();
    let internal_id = raw_id.strip_prefix("-100").unwrap_or_else(|| raw_id.trim_start_matches('-'));
    (!internal_id.is_empty()).then(|| format!("https://t.me/c/{internal_id}/{message_id}"))
}
