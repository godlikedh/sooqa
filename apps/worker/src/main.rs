//! Durable job worker entry point for sooqa.

use std::{error::Error, sync::Arc, time::Duration};

use sooqa_config::{AppConfig, AppRole, CliOptions, ConfigError};
use sooqa_jobs::JobType;
use sooqa_media::{BinaryCheck, ProcessCommandRunner, diagnose_binaries};
use sooqa_persistence::Database;
use uuid::Uuid;

use sooqa_telegram::{StorageUploadProvider, TeloxideApi};
use sooqa_worker::{HandlerRegistry, Worker, upload_storage_asset_handler};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("sooqa-worker: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let options = CliOptions::parse(std::env::args().skip(1))?;
    let config = AppConfig::load(AppRole::Worker, options.config_path.as_deref())?;

    if options.check_config {
        println!("{}", config.summary());
        return Ok(());
    }

    sooqa_runtime::init_tracing(&config.observability)?;
    let binary_diagnostics = diagnose_binaries(
        Arc::new(ProcessCommandRunner),
        &[
            BinaryCheck::new("ffmpeg", config.media.ffmpeg_path.clone(), ["-version"]),
            BinaryCheck::new("ffprobe", config.media.ffprobe_path.clone(), ["-version"]),
            BinaryCheck::new("yt-dlp", config.media.ytdlp_path.clone(), ["--version"]),
        ],
        Duration::from_secs(5),
    )
    .await;
    for diagnostic in &binary_diagnostics {
        match (&diagnostic.version, &diagnostic.error) {
            (Some(version), None) => tracing::info!(
                binary = %diagnostic.name,
                executable = %diagnostic.executable.display(),
                version = %version,
                "external binary detected"
            ),
            (_, Some(error)) => tracing::error!(
                binary = %diagnostic.name,
                executable = %diagnostic.executable.display(),
                error = %error,
                "required external binary is unavailable"
            ),
            _ => tracing::error!(
                binary = %diagnostic.name,
                executable = %diagnostic.executable.display(),
                "required external binary returned no version"
            ),
        }
    }
    let missing_binaries = binary_diagnostics
        .iter()
        .filter(|diagnostic| !diagnostic.available())
        .map(|diagnostic| diagnostic.name.as_str())
        .collect::<Vec<_>>();
    if !missing_binaries.is_empty() {
        return Err(format!(
            "required worker binaries are unavailable: {}",
            missing_binaries.join(", ")
        )
        .into());
    }

    let database_url =
        config.secrets.database_url.as_ref().ok_or(ConfigError::MissingSecret("database URL"))?;
    let database = Database::connect_secret(database_url, config.database.max_connections).await?;
    let mut handlers = HandlerRegistry::new();
    match (
        config.secrets.telegram_bot_token.as_ref().filter(|token| token.is_configured()),
        config.telegram.storage_chat_id,
    ) {
        (Some(token), Some(storage_chat_id)) => {
            let api = TeloxideApi::new(
                token.expose_secret(),
                &config.telegram.api_base_url,
                Duration::from_secs(config.telegram.poll_timeout_seconds),
            )?;
            let provider = StorageUploadProvider::new(api, database.library(), storage_chat_id)?;
            provider.verify_storage_chat().await?;
            let storage_handler = upload_storage_asset_handler(provider);
            handlers.register(JobType::UploadStorageAsset, move |job| storage_handler(job));
            tracing::info!(storage_chat_id, "Telegram storage upload job handler enabled");
        }
        (None, Some(_)) => {
            return Err(ConfigError::MissingSecret("Telegram bot token").into());
        }
        _ => {
            tracing::warn!("Telegram storage upload job handler is disabled");
        }
    }
    let worker_id = format!("worker-{}", Uuid::new_v4());
    let worker = Worker::new(
        database.jobs(),
        handlers,
        worker_id,
        Duration::from_secs(config.worker.poll_interval_seconds),
        Duration::from_secs(config.worker.lease_duration_seconds),
    )?;

    tracing::info!(role = %config.role, worker_id = %worker.worker_id(), "sooqa worker starting");
    worker.run(sooqa_runtime::shutdown_signal()).await?;
    tracing::info!(role = %config.role, "sooqa worker stopped");

    Ok(())
}
