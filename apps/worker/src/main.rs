//! Durable job worker entry point for sooqa.

use std::{error::Error, sync::Arc, time::Duration};

use sooqa_config::{AppConfig, AppRole, CliOptions, ConfigError};
use sooqa_jobs::JobType;
use sooqa_media::{
    BinaryCheck, FfprobeAdapter, MediaWorkspace, ProcessCommandRunner, diagnose_binaries,
};
use sooqa_persistence::Database;
use uuid::Uuid;

use sooqa_telegram::{StorageUploadProvider, TeloxideApi};
use sooqa_worker::{HandlerRegistry, Worker, probe_asset_handler, upload_storage_asset_handler};

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
    let database_url =
        config.secrets.database_url.as_ref().ok_or(ConfigError::MissingSecret("database URL"))?;
    let database = Database::connect_secret(database_url, config.database.max_connections).await?;
    let mut handlers = HandlerRegistry::new();
    let probe_handler = probe_asset_handler(
        database.inbox(),
        config.media.work_root.clone(),
        FfprobeAdapter::new(config.media.ffprobe_path.clone(), Duration::from_secs(30)),
    );
    handlers.register(JobType::ProbeAsset, move |job| probe_handler(job));
    tracing::info!("Telegram and upload ingest probe job handler enabled");
    match (
        config.secrets.telegram_bot_token.as_ref().filter(|token| token.is_configured()),
        config.telegram.storage_chat_id,
    ) {
        (Some(token), Some(storage_chat_id)) => {
            let api = TeloxideApi::new(
                token.expose_secret(),
                &config.telegram.api_base_url,
                Duration::from_secs(config.telegram.poll_timeout_seconds),
            )?
            .with_max_download_bytes(config.telegram.max_download_bytes);
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
    let capabilities = handlers.job_types();
    ensure_work_root(&config.media.work_root).await?;
    let live_job_ids = database.jobs().live_job_ids().await?;
    let stale_artifact_age =
        Duration::from_secs(config.worker.lease_duration_seconds.saturating_add(5 * 60));
    let removed_artifacts = MediaWorkspace::scavenge_stale_artifacts(
        &config.media.work_root,
        stale_artifact_age,
        &live_job_ids,
    )
    .await?;
    if removed_artifacts > 0 {
        tracing::info!(removed_artifacts, "removed stale media workspace artifacts");
    }
    let mut binary_checks = Vec::new();
    if capabilities.contains(&JobType::ProbeAsset) {
        binary_checks.push(BinaryCheck::new(
            "ffprobe",
            config.media.ffprobe_path.clone(),
            ["-version"],
        ));
    }
    let binary_diagnostics =
        diagnose_binaries(Arc::new(ProcessCommandRunner), &binary_checks, Duration::from_secs(5))
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
            "required worker binaries for enabled handlers are unavailable: {}",
            missing_binaries.join(", ")
        )
        .into());
    }
    tracing::info!(
        capabilities = ?capabilities.iter().map(|job_type| job_type.as_str()).collect::<Vec<_>>(),
        "worker handler capabilities enabled"
    );
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

async fn ensure_work_root(path: &std::path::Path) -> Result<(), Box<dyn Error>> {
    tokio::fs::create_dir_all(path).await?;
    let check_path = path.join(format!(".sooqa-worker-check-{}", Uuid::new_v4()));
    let file = tokio::fs::OpenOptions::new().write(true).create_new(true).open(&check_path).await?;
    drop(file);
    tokio::fs::remove_file(check_path).await?;
    Ok(())
}
