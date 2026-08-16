//! Durable job worker entry point for sooqa.

use std::{error::Error, path::PathBuf, sync::Arc, time::Duration};

use sooqa_config::{AppConfig, AppRole, CliOptions, ConfigError};
use sooqa_jobs::JobType;
use sooqa_media::{
    BinaryCheck, CanonicalImageProfile, CanonicalVideoProfile, DirectHttpDownloader,
    DownloadLimits, FfprobeAdapter, ImageNormalizer, MediaWorkspace, NormalizationPlanner,
    ProcessCommandRunner, SourceDownloader, SourceDownloaderRouter, TwoChMirrorDownloader,
    YtDlpConfig, YtDlpDownloader, diagnose_binaries, is_supported_deno_version,
    ytdlp_allowed_hosts_include_youtube,
};
use sooqa_persistence::{Database, JobRepository, WORKSPACE_CLEANUP_RETENTION};
use uuid::Uuid;

use sooqa_telegram::{StorageUploadProvider, TeloxideApi};
use sooqa_worker::{
    HandlerRegistry, TelegramSourceDownloader, Worker, cleanup_workspace_handler,
    compute_fingerprint_handler, download_source_handler, finalize_ingest_handler,
    inspect_source_handler, materialize_publication_handler, media_processing_components,
    normalize_asset_handler, probe_asset_handler_with_telegram_source, publish_post_handler,
    sync_storage_caption_handler, upload_storage_asset_handler,
};

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
    let download_limits = DownloadLimits {
        max_bytes: config.media.source_download_max_bytes,
        ..DownloadLimits::default()
    };
    let mut binary_checks = vec![
        BinaryCheck::new("ffprobe", config.media.ffprobe_path.clone(), ["-version"]),
        BinaryCheck::new("ffmpeg", config.media.ffmpeg_path.clone(), ["-version"]),
    ];
    if !config.media.ytdlp_allowed_hosts.is_empty() {
        binary_checks.push(
            BinaryCheck::new("yt-dlp", config.media.ytdlp_path.clone(), ["--version"])
                .with_cleared_environment(),
        );
        binary_checks.push(
            BinaryCheck::new("yt-dlp capabilities", config.media.ytdlp_path.clone(), ["--help"])
                .with_cleared_environment()
                .requiring_output(["--js-runtimes", "--no-remote-components"]),
        );
        binary_checks
            .push(BinaryCheck::new("deno", "deno", ["--version"]).with_cleared_environment());
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
    if !config.media.ytdlp_allowed_hosts.is_empty() {
        let deno_version = binary_diagnostics
            .iter()
            .find(|diagnostic| diagnostic.name == "deno")
            .and_then(|diagnostic| diagnostic.version.as_deref())
            .unwrap_or_default();
        if !is_supported_deno_version(deno_version) {
            return Err(format!(
                "yt-dlp is enabled but Deno must be at least 2.3.0; detected {deno_version:?}"
            )
            .into());
        }
    }

    let direct_http =
        Arc::new(TwoChMirrorDownloader::new(DirectHttpDownloader::new(download_limits)));
    let source_downloader: Arc<dyn SourceDownloader> = if config
        .media
        .ytdlp_allowed_hosts
        .is_empty()
    {
        tracing::info!(
            "source inspection handler enabled (direct HTTP with 2ch mirrors; yt-dlp disabled because the host allowlist is empty)"
        );
        Arc::new(SourceDownloaderRouter::direct_only(direct_http))
    } else {
        let ytdlp_config =
            YtDlpConfig::new(config.media.ytdlp_path.clone(), config.media.ytdlp_format.clone())?
                .with_pot_provider_url(config.media.ytdlp_pot_provider_url.clone());
        let ytdlp = YtDlpDownloader::with_limits(ytdlp_config, download_limits);
        if let Err(error) = ytdlp.verify_runtime(Duration::from_secs(5)).await {
            return Err(std::io::Error::other(format!(
                "yt-dlp is enabled but its pinned plugin/EJS/Deno runtime check failed: {error}"
            ))
            .into());
        }
        if ytdlp_allowed_hosts_include_youtube(&config.media.ytdlp_allowed_hosts)
            && let Err(error) = ytdlp.verify_pot_provider(Duration::from_secs(5)).await
        {
            return Err(std::io::Error::other(format!(
                "YouTube yt-dlp support is enabled but its PO-token provider preflight failed: {error}"
            ))
            .into());
        }
        tracing::info!(
            allowed_hosts = ?config.media.ytdlp_allowed_hosts,
            "source inspection handler enabled (direct HTTP with allowlisted yt-dlp fallback)"
        );
        Arc::new(SourceDownloaderRouter::new(
            direct_http,
            Arc::new(ytdlp),
            config.media.ytdlp_allowed_hosts.clone(),
        ))
    };
    let telegram_api =
        match config.secrets.telegram_bot_token.as_ref().filter(|token| token.is_configured()) {
            Some(token) => {
                let mut api = TeloxideApi::new_with_upload_timeout(
                    token.expose_secret(),
                    &config.telegram.api_base_url,
                    Duration::from_secs(config.telegram.poll_timeout_seconds),
                    Duration::from_secs(config.telegram.upload_timeout_seconds),
                )?
                .with_source_download_max_bytes(config.telegram.source_download_max_bytes);
                if let Some(root) = config.telegram.local_file_root.as_ref() {
                    api = api.with_local_file_root(root.clone());
                }
                Some(api)
            }
            None => None,
        };
    let telegram_source =
        telegram_api.clone().map(|api| Arc::new(api) as Arc<dyn TelegramSourceDownloader>);
    let inspect_handler = inspect_source_handler(database.inbox(), Arc::clone(&source_downloader));
    handlers.register(JobType::InspectSource, move |job| inspect_handler(job));
    let download_handler = download_source_handler(
        database.inbox(),
        config.media.work_root.clone(),
        source_downloader,
        download_limits,
    );
    handlers.register(JobType::DownloadSource, move |job| download_handler(job));
    tracing::info!("source download handler enabled");
    let probe_handler = probe_asset_handler_with_telegram_source(
        database.inbox(),
        config.media.work_root.clone(),
        FfprobeAdapter::new(config.media.ffprobe_path.clone(), Duration::from_secs(30)),
        telegram_source,
    );
    handlers.register(JobType::ProbeAsset, move |job| probe_handler(job));
    tracing::info!("Telegram and upload ingest probe job handler enabled");
    let processing_timeout = Duration::from_secs(config.media.processing_timeout_seconds);
    let normalization_planner = NormalizationPlanner::new(
        config.media.ffmpeg_path.clone(),
        CanonicalVideoProfile::default(),
    )?;
    let (normalization_executor, frame_extractor) = media_processing_components(
        config.media.ffmpeg_path.clone(),
        config.media.ffprobe_path.clone(),
        processing_timeout,
    );
    let normalize_handler = normalize_asset_handler(
        database.inbox(),
        config.media.work_root.clone(),
        normalization_planner,
        normalization_executor,
        ImageNormalizer::new(CanonicalImageProfile::default())?,
        config.media.normalized_storage_max_bytes,
    );
    handlers.register(JobType::NormalizeAsset, move |job| normalize_handler(job));
    tracing::info!("asset normalization handler enabled");
    let fingerprint_handler = compute_fingerprint_handler(
        database.inbox(),
        database.library(),
        config.media.work_root.clone(),
        frame_extractor,
    );
    handlers.register(JobType::ComputeFingerprint, move |job| fingerprint_handler(job));
    tracing::info!("video fingerprint handler enabled");
    let finalize_handler = finalize_ingest_handler(
        database.inbox(),
        database.library(),
        config.media.work_root.clone(),
    );
    handlers.register(JobType::FinalizeIngest, move |job| finalize_handler(job));
    tracing::info!("ingest finalization handler enabled");
    let materialize_handler = materialize_publication_handler(database.publisher());
    handlers.register(JobType::MaterializePublication, move |job| materialize_handler(job));
    tracing::info!("publication materialization handler enabled");
    let cleanup_handler =
        cleanup_workspace_handler(database.inbox(), config.media.work_root.clone());
    handlers.register(JobType::CleanupWorkspace, move |job| cleanup_handler(job));
    tracing::info!("workspace cleanup handler enabled");
    if let Some(api) = telegram_api.clone() {
        let publication_handler =
            publish_post_handler(database.publisher(), database.library(), api);
        handlers.register(JobType::PublishPost, move |job| publication_handler(job));
        tracing::info!("fenced Telegram publication job handler enabled");
    }
    match (telegram_api, config.telegram.storage_chat_id) {
        (Some(api), Some(storage_chat_id)) => {
            let caption_api = api.clone();
            let provider = StorageUploadProvider::new(api, database.library(), storage_chat_id)?
                .with_max_storage_bytes(config.media.normalized_storage_max_bytes)
                .with_work_root(config.media.work_root.clone());
            provider.verify_storage_chat().await?;
            let storage_handler = upload_storage_asset_handler(database.inbox(), provider);
            handlers.register(JobType::UploadStorageAsset, move |job| storage_handler(job));
            let caption_handler = sync_storage_caption_handler(database.library(), caption_api);
            handlers.register(JobType::SyncStorageCaption, move |job| caption_handler(job));
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
    let removed_workspaces = reconcile_workspaces(
        database.jobs(),
        &config.media.work_root,
        WORKSPACE_CLEANUP_RETENTION,
        128,
    )
    .await?;
    if removed_workspaces > 0 {
        tracing::info!(removed_workspaces, "removed stale media workspaces");
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
    let (stop_reconciliation, reconciliation_signal) = tokio::sync::watch::channel(false);
    let reconciliation_repository = database.jobs();
    let reconciliation_work_root = config.media.work_root.clone();
    let reconciliation_task = tokio::spawn(async move {
        reconcile_workspaces_periodically(
            reconciliation_repository,
            reconciliation_work_root,
            WORKSPACE_CLEANUP_RETENTION,
            reconciliation_signal,
        )
        .await;
    });
    let worker_result = worker.run(sooqa_runtime::shutdown_signal()).await;
    let _ = stop_reconciliation.send(true);
    if let Err(error) = reconciliation_task.await {
        tracing::warn!(?error, "workspace reconciliation task stopped unexpectedly");
    }
    worker_result?;
    tracing::info!(role = %config.role, "sooqa worker stopped");

    Ok(())
}

async fn reconcile_workspaces(
    jobs: JobRepository,
    work_root: &std::path::Path,
    max_age: time::Duration,
    limit: usize,
) -> Result<u64, Box<dyn Error>> {
    let protected = jobs.protected_workspace_ids().await?;
    Ok(MediaWorkspace::scavenge_completed_workspaces(
        work_root,
        max_age.unsigned_abs(),
        &protected,
        limit,
    )
    .await?)
}

async fn reconcile_workspaces_periodically(
    jobs: JobRepository,
    work_root: PathBuf,
    max_age: time::Duration,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let interval = Duration::from_secs(5 * 60);
    loop {
        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    return;
                }
            }
            _ = tokio::time::sleep(interval) => {
                match reconcile_workspaces(jobs.clone(), &work_root, max_age, 128).await {
                    Ok(removed) if removed > 0 => {
                        tracing::info!(removed, "periodic workspace reconciliation removed stale workspaces");
                    }
                    Ok(_) => {}
                    Err(error) => tracing::warn!(?error, "periodic workspace reconciliation failed"),
                }
            }
        }
    }
}

async fn ensure_work_root(path: &std::path::Path) -> Result<(), Box<dyn Error>> {
    tokio::fs::create_dir_all(path).await?;
    let check_path = path.join(format!(".sooqa-worker-check-{}", Uuid::new_v4()));
    let file = tokio::fs::OpenOptions::new().write(true).create_new(true).open(&check_path).await?;
    drop(file);
    tokio::fs::remove_file(check_path).await?;
    Ok(())
}
