use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use image::{DynamicImage, ImageBuffer, Rgb};
use serde_json::json;
use sooqa_inbox::{
    AssetNormalization, IngestStatus, IngestSubmission, IngestSubmissionInput, SourceInspection,
    SourceMediaKind, SubmittedVia, TelegramSubmissionInput,
};
use sooqa_jobs::{JobType, NewJob};
use sooqa_library::{
    MediaIngest, MediaKind, MediaMetadata, MediaSourceInput, NewMedia, SourceKind,
};
use sooqa_media::{
    CommandError, DownloadError, DownloadLimits, DownloadedSource, ExternalCommand,
    ExternalCommandOutput, ExternalCommandRunner, FfprobeAdapter, FrameExtractor, MediaWorkspace,
    SourceDownloader, SourceInput, WorkspaceArea, sha256_file,
};
use sooqa_persistence::Database;
use sooqa_telegram::{
    StorageUploadProvider, StorageUploadRequest, StorageUploadResult, TelegramStorageApi,
};
use sooqa_worker::{
    TelegramSourceDownloader, cleanup_workspace_handler, compute_fingerprint_handler,
    download_source_handler, inspect_source_handler, probe_asset_handler_with_telegram_source,
    upload_storage_asset_handler,
};
use thiserror::Error;
use tokio::fs;
use uuid::Uuid;

#[derive(Clone)]
struct ReconstructingDirectSource {
    inspection_calls: Arc<AtomicUsize>,
    download_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl SourceDownloader for ReconstructingDirectSource {
    async fn inspect(&self, source: &SourceInput) -> Result<SourceInspection, DownloadError> {
        self.inspection_calls.fetch_add(1, Ordering::Relaxed);
        Ok(SourceInspection {
            adapter: "direct_http".to_owned(),
            source_url: source.source_url.clone(),
            resolved_url: None,
            media_kind: SourceMediaKind::Video,
            mime_type: Some("video/mp4".to_owned()),
            content_length_bytes: Some(13),
            title: Some("reconstructed".to_owned()),
            metadata: json!({"test": true}),
        })
    }

    async fn download(
        &self,
        _inspection: &SourceInspection,
        destination: &Path,
        _limits: &DownloadLimits,
    ) -> Result<DownloadedSource, DownloadError> {
        self.download_calls.fetch_add(1, Ordering::Relaxed);
        let bytes = b"reconstructed";
        fs::write(destination, bytes).await.map_err(|error| {
            DownloadError::terminal("test_download", format!("could not write fixture: {error}"))
        })?;
        Ok(DownloadedSource {
            path: destination.to_owned(),
            bytes: bytes.len() as u64,
            mime_type: Some("video/mp4".to_owned()),
        })
    }
}

#[derive(Clone)]
struct ReconstructingTelegramSource {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl TelegramSourceDownloader for ReconstructingTelegramSource {
    async fn download_file(
        &self,
        file_id: &str,
        destination: &Path,
    ) -> Result<(), sooqa_worker::HandlerFailure> {
        assert_eq!(file_id, "durable-file-id");
        self.calls.fetch_add(1, Ordering::Relaxed);
        fs::write(destination, b"telegram-reconstructed").await.map_err(|error| {
            sooqa_worker::HandlerFailure::permanent("test_download", error.to_string())
        })
    }
}

#[derive(Clone, Copy)]
struct StaticProbeRunner;

#[async_trait]
impl ExternalCommandRunner for StaticProbeRunner {
    async fn run(&self, _command: ExternalCommand) -> Result<ExternalCommandOutput, CommandError> {
        Ok(ExternalCommandOutput {
            success: true,
            exit_code: Some(0),
            stdout: br#"{
                "format": {"format_name":"mp4", "duration":"4.0", "size":"22", "bit_rate":"1000"},
                "streams": [{"index":0, "codec_type":"video", "codec_name":"h264", "width":320, "height":240, "duration":"4.0", "bit_rate":"1000"}]
            }"#
            .to_vec(),
            stderr: Vec::new(),
            stdout_truncated: false,
            stderr_truncated: false,
        })
    }
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn force_save_reconstructs_url_after_workspace_cleanup(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let work_root = std::env::temp_dir().join(format!("sooqa-worker-url-{}", Uuid::new_v4()));
    let ingest = database
        .inbox()
        .create_ingest(
            IngestSubmission::try_new(IngestSubmissionInput::new(
                format!("https://example.test/reconstruct-{}", Uuid::new_v4()),
                SubmittedVia::Api,
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    let workspace = MediaWorkspace::create(&work_root, ingest.ingest.id).await.unwrap();
    let source_path = workspace.path(WorkspaceArea::Source, "source.bin").unwrap();
    fs::write(&source_path, b"old-workspace-source").await.unwrap();
    workspace.cleanup().await.unwrap();
    mark_duplicate_pending(&database, ingest.ingest.id).await;

    let resumed = database.inbox().force_save(ingest.ingest.id).await.unwrap();
    assert!(resumed.resumed);
    let replay = database.inbox().force_save(ingest.ingest.id).await.unwrap();
    assert!(!replay.resumed);
    assert_eq!(replay.ingest.status, IngestStatus::Queued);
    assert_eq!(count_ingest_jobs(&database, ingest.ingest.id, "inspect_source").await, 1);

    let source = ReconstructingDirectSource {
        inspection_calls: Arc::new(AtomicUsize::new(0)),
        download_calls: Arc::new(AtomicUsize::new(0)),
    };
    let inspect_handler = inspect_source_handler(database.inbox(), Arc::new(source.clone()));
    let inspect_job = database
        .jobs()
        .claim_next("url-inspector", Duration::from_secs(30), &[JobType::InspectSource])
        .await
        .unwrap()
        .expect("force-save inspect job should be claimable");
    inspect_handler(inspect_job).await.unwrap();

    let download_handler = download_source_handler(
        database.inbox(),
        work_root.clone(),
        Arc::new(source.clone()),
        DownloadLimits::default(),
    );
    let download_job = database
        .jobs()
        .claim_next("url-downloader", Duration::from_secs(30), &[JobType::DownloadSource])
        .await
        .unwrap()
        .expect("force-save download job should be claimable");
    download_handler(download_job).await.unwrap();

    let workspace = MediaWorkspace::create(&work_root, ingest.ingest.id).await.unwrap();
    let source_path = workspace.path(WorkspaceArea::Source, "source.bin").unwrap();
    assert_eq!(fs::read(&source_path).await.unwrap(), b"reconstructed");
    assert_eq!(source.inspection_calls.load(Ordering::Relaxed), 1);
    assert_eq!(source.download_calls.load(Ordering::Relaxed), 1);
    assert_eq!(count_ingest_jobs(&database, ingest.ingest.id, "probe_asset").await, 1);

    let _ = fs::remove_dir_all(work_root).await;
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn cleanup_handler_removes_only_the_claimed_workspace(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let work_root = std::env::temp_dir().join(format!("sooqa-worker-cleanup-{}", Uuid::new_v4()));
    let ingest = database
        .inbox()
        .create_ingest(
            IngestSubmission::try_new(IngestSubmissionInput::new(
                format!("https://example.test/cleanup-{}", Uuid::new_v4()),
                SubmittedVia::Api,
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    let workspace = MediaWorkspace::create(&work_root, ingest.ingest.workspace_id).await.unwrap();
    let source_path = workspace.path(WorkspaceArea::Source, "source.bin").unwrap();
    fs::write(&source_path, b"cleanup-me").await.unwrap();
    sqlx::query("UPDATE ingests SET state = 'completed', completed_at = now() WHERE id = $1")
        .bind(ingest.ingest.id)
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query(
        "UPDATE queue.jobs SET state = 'succeeded', completed_at = now() WHERE payload->>'ingest_id' = $1",
    )
    .bind(ingest.ingest.id.to_string())
    .execute(database.pool())
    .await
    .unwrap();

    let cleanup_job = database
        .jobs()
        .enqueue(
            NewJob::cleanup_workspace(ingest.ingest.id, ingest.ingest.workspace_id)
                .dedupe_key(format!("test:cleanup-handler:{}", ingest.ingest.id)),
        )
        .await
        .unwrap();
    let claimed = database
        .jobs()
        .claim_next("cleanup-handler", Duration::from_secs(30), &[JobType::CleanupWorkspace])
        .await
        .unwrap()
        .expect("cleanup job should be claimable");
    assert_eq!(claimed.id, cleanup_job.id);
    let handler = cleanup_workspace_handler(database.inbox(), work_root.clone());
    handler(claimed.clone()).await.unwrap();
    database.jobs().complete_lease(&claimed.lease().unwrap()).await.unwrap();
    assert!(!workspace.root().exists());

    let _ = fs::remove_dir_all(work_root).await;
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn force_save_reconstructs_telegram_source_after_workspace_cleanup(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let work_root = std::env::temp_dir().join(format!("sooqa-worker-telegram-{}", Uuid::new_v4()));
    let ingest = database
        .inbox()
        .create_ingest(
            IngestSubmission::try_new_telegram(TelegramSubmissionInput {
                source_reference: "telegram://-100/42".to_owned(),
                submitted_via: SubmittedVia::TelegramBot,
                submitted_by_admin_id: None,
                original_input: json!({
                    "source_type": "telegram",
                    "telegram_file_id": "durable-file-id",
                    "telegram_file_unique_id": "durable-unique-id",
                    "media_kind": "video"
                }),
                supplied_caption: None,
                idempotency_key: Some(format!("test:telegram:{}", Uuid::new_v4())),
            })
            .unwrap(),
        )
        .await
        .unwrap();
    let workspace = MediaWorkspace::create(&work_root, ingest.ingest.workspace_id).await.unwrap();
    let source_path = workspace.path(WorkspaceArea::Source, "telegram-input.bin").unwrap();
    fs::write(&source_path, b"old-telegram-source").await.unwrap();
    workspace.cleanup().await.unwrap();
    mark_duplicate_pending(&database, ingest.ingest.id).await;

    let resumed = database.inbox().force_save(ingest.ingest.id).await.unwrap();
    assert_ne!(resumed.ingest.workspace_id, ingest.ingest.workspace_id);
    assert_eq!(count_ingest_jobs(&database, ingest.ingest.id, "probe_asset").await, 1);
    let source = ReconstructingTelegramSource { calls: Arc::new(AtomicUsize::new(0)) };
    let probe_handler = probe_asset_handler_with_telegram_source(
        database.inbox(),
        work_root.clone(),
        FfprobeAdapter::with_runner(
            "ffprobe",
            Duration::from_secs(5),
            64 * 1024,
            Arc::new(StaticProbeRunner),
        ),
        Some(Arc::new(source.clone())),
    );
    let probe_job = database
        .jobs()
        .claim_next("telegram-prober", Duration::from_secs(30), &[JobType::ProbeAsset])
        .await
        .unwrap()
        .expect("force-save probe job should be claimable");
    probe_handler(probe_job).await.unwrap();

    let request = database.inbox().find(ingest.ingest.id).await.unwrap().unwrap();
    assert_eq!(request.status, IngestStatus::Normalizing);
    assert_eq!(source.calls.load(Ordering::Relaxed), 1);
    let workspace = MediaWorkspace::create(&work_root, request.workspace_id).await.unwrap();
    let source_path = workspace.path(WorkspaceArea::Source, "telegram-input.bin").unwrap();
    assert_eq!(fs::read(&source_path).await.unwrap(), b"telegram-reconstructed");
    assert_eq!(count_ingest_jobs(&database, ingest.ingest.id, "probe_asset").await, 1);
    assert_eq!(count_ingest_jobs(&database, ingest.ingest.id, "normalize_asset").await, 1);

    let _ = fs::remove_dir_all(work_root).await;
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn non_video_normalization_queues_finalization_without_fingerprint_job(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let ingest = database
        .inbox()
        .create_ingest(
            IngestSubmission::try_new(IngestSubmissionInput::new(
                format!("https://example.test/image-{}", Uuid::new_v4()),
                SubmittedVia::Api,
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    sqlx::query("DELETE FROM queue.jobs WHERE payload->>'ingest_id' = $1")
        .bind(ingest.ingest.id.to_string())
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE ingests SET state = 'normalizing' WHERE id = $1")
        .bind(ingest.ingest.id)
        .execute(database.pool())
        .await
        .unwrap();
    database
        .jobs()
        .enqueue(
            sooqa_jobs::NewJob::normalize_asset(ingest.ingest.id)
                .dedupe_key(format!("test:normalize:{}", ingest.ingest.id)),
        )
        .await
        .unwrap();
    let job = database
        .jobs()
        .claim_next("image-normalizer", Duration::from_secs(30), &[JobType::NormalizeAsset])
        .await
        .unwrap()
        .expect("normalization job should be claimable");
    let attempt = job.lease().unwrap();
    let completed = database
        .inbox()
        .complete_asset_normalization(
            ingest.ingest.id,
            &attempt,
            AssetNormalization {
                local_work_path: "/tmp/sooqa-image.png".to_owned(),
                file_size_bytes: 10,
                sha256: "11".repeat(32),
                media_kind: SourceMediaKind::Image,
                mime_type: Some("image/png".to_owned()),
                container: Some("png".to_owned()),
                video_codec: None,
                audio_codec: None,
                width: Some(10),
                height: Some(10),
                duration_ms: None,
                bit_rate: None,
                thumbnail: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(completed.status, IngestStatus::Storing);
    assert_eq!(count_ingest_jobs(&database, ingest.ingest.id, "compute_fingerprint").await, 0);
    assert_eq!(count_ingest_jobs(&database, ingest.ingest.id, "finalize_ingest").await, 1);
}

#[derive(Clone)]
struct IdentityFrameRunner {
    variant: u8,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl ExternalCommandRunner for IdentityFrameRunner {
    async fn run(&self, command: ExternalCommand) -> Result<ExternalCommandOutput, CommandError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let output_pattern = command.args().last().expect("frame output argument");
        let output_pattern = output_pattern.to_string_lossy();
        let frame_count = command
            .args()
            .windows(2)
            .find_map(|args| (args[0] == "-frames:v").then(|| args[1].to_string_lossy()))
            .expect("frame count argument")
            .parse::<usize>()
            .expect("frame count should be numeric");
        let variant = self.variant;
        let image = ImageBuffer::from_fn(64, 64, |x, y| {
            let (x, y) = if variant == 0 { (x, y) } else { (y, x) };
            Rgb([
                (x.saturating_mul(3) % 256) as u8,
                (y.saturating_mul(2) % 256) as u8,
                ((x + y).saturating_mul(2) % 256) as u8,
            ])
        });
        for index in 0..frame_count {
            let path = PathBuf::from(output_pattern.replace("%04d", &format!("{index:04}")));
            DynamicImage::ImageRgb8(image.clone())
                .save_with_format(&path, image::ImageFormat::Png)
                .expect("fake extractor should write a frame");
        }
        Ok(ExternalCommandOutput {
            success: true,
            exit_code: Some(0),
            stdout: Vec::new(),
            stderr: Vec::new(),
            stdout_truncated: false,
            stderr_truncated: false,
        })
    }
}

#[derive(Debug, Error)]
#[error("storage test API error")]
struct StorageTestError;

#[derive(Clone)]
struct CountingStorageApi {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl TelegramStorageApi for CountingStorageApi {
    type Error = StorageTestError;

    async fn upload_media(
        &self,
        _request: StorageUploadRequest,
    ) -> Result<StorageUploadResult, Self::Error> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(StorageUploadResult {
            storage_message_id: 42,
            telegram_file_id: "stored-file-id".to_owned(),
            telegram_file_unique_id: "stored-unique-id".to_owned(),
        })
    }

    async fn verify_storage_chat(&self, _chat_id: i64) -> Result<(), Self::Error> {
        Ok(())
    }

    fn is_ambiguous_error(_error: &Self::Error) -> bool {
        false
    }
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn composed_identity_worker_has_bounded_storage_effects(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let work_root = std::env::temp_dir().join(format!("sooqa-worker-identity-{}", Uuid::new_v4()));
    let calls = Arc::new(AtomicUsize::new(0));
    let storage_api = CountingStorageApi { calls: Arc::clone(&calls) };

    let exact = prepare_fingerprint_request(&database, &work_root, "exact", b"exact-bytes").await;
    let exact_sha = hex_bytes(&exact.normalization.sha256);
    let exact_media = database
        .library()
        .resolve_media(video_media_ingest(exact_sha, "https://example.test/exact-candidate"))
        .await
        .unwrap();
    mark_media_ready(&database, exact_media.media.id).await;
    let exact_runner = IdentityFrameRunner { variant: 0, calls: Arc::new(AtomicUsize::new(0)) };
    run_fingerprint(
        &database,
        &work_root,
        exact,
        FrameExtractor::with_runner(
            "fake-ffmpeg",
            Duration::from_secs(5),
            64 * 1024,
            Arc::new(exact_runner.clone()),
        ),
    )
    .await;
    assert_eq!(exact_runner.calls.load(Ordering::Relaxed), 0);
    assert_eq!(calls.load(Ordering::Relaxed), 0);

    let strong_runner = IdentityFrameRunner { variant: 0, calls: Arc::new(AtomicUsize::new(0)) };
    let candidate_workspace = MediaWorkspace::create(&work_root, Uuid::new_v4()).await.unwrap();
    let candidate_path =
        candidate_workspace.path(WorkspaceArea::Normalized, "canonical.mp4").unwrap();
    fs::write(&candidate_path, b"candidate").await.unwrap();
    let candidate_fingerprint = FrameExtractor::with_runner(
        "fake-ffmpeg",
        Duration::from_secs(5),
        64 * 1024,
        Arc::new(strong_runner.clone()),
    )
    .extract_video_sequence_from_area(
        &candidate_workspace,
        WorkspaceArea::Normalized,
        "canonical.mp4",
        4_000,
    )
    .await
    .unwrap();
    let strong_candidate = database
        .library()
        .resolve_video_identity(
            video_media_ingest(vec![0x31; 32], "https://example.test/strong-candidate"),
            &candidate_fingerprint,
            sooqa_media::SequenceAlignmentConfig::default(),
            false,
        )
        .await
        .unwrap();
    let strong_candidate_id = match strong_candidate {
        sooqa_library::VideoIdentityOutcome::NewMedia { media_id } => media_id,
        other => panic!("expected candidate media, got {other:?}"),
    };
    mark_media_ready(&database, strong_candidate_id).await;

    let strong =
        prepare_fingerprint_request(&database, &work_root, "strong", b"strong-bytes").await;
    let strong_id = strong.ingest_id;
    run_fingerprint(
        &database,
        &work_root,
        strong,
        FrameExtractor::with_runner(
            "fake-ffmpeg",
            Duration::from_secs(5),
            64 * 1024,
            Arc::new(strong_runner.clone()),
        ),
    )
    .await;
    assert!(strong_runner.calls.load(Ordering::Relaxed) > 0);
    let strong_request = database.inbox().find(strong_id).await.unwrap().unwrap();
    assert_eq!(strong_request.status, IngestStatus::DuplicatePending);
    assert!(strong_request.media_id.is_none());
    assert_eq!(calls.load(Ordering::Relaxed), 0);

    let no_match_runner = IdentityFrameRunner { variant: 1, calls: Arc::new(AtomicUsize::new(0)) };
    let no_match =
        prepare_fingerprint_request(&database, &work_root, "no-match", b"no-match-bytes").await;
    let no_match_id = no_match.ingest_id;
    run_fingerprint(
        &database,
        &work_root,
        no_match,
        FrameExtractor::with_runner(
            "fake-ffmpeg",
            Duration::from_secs(5),
            64 * 1024,
            Arc::new(no_match_runner.clone()),
        ),
    )
    .await;
    let no_match_request = database.inbox().find(no_match_id).await.unwrap().unwrap();
    assert_eq!(no_match_request.status, IngestStatus::Storing);
    let media_id = no_match_request.media_id.expect("no-match should reserve media");
    let no_match_sha = hex_bytes_from_media(&database, media_id).await;
    assert_eq!(count_media_with_sha(&database, no_match_sha).await, 1);

    let provider =
        StorageUploadProvider::new(storage_api.clone(), database.library(), -100123).unwrap();
    let upload_handler = upload_storage_asset_handler(database.inbox(), provider.clone());
    let upload_job = database
        .jobs()
        .claim_next("storage-worker", Duration::from_secs(30), &[JobType::UploadStorageAsset])
        .await
        .unwrap()
        .expect("no-match should enqueue one storage job");
    upload_handler(upload_job).await.unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    provider.upload(sooqa_telegram::StorageUploadInput { media_id, generation: 0 }).await.unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(
        database.inbox().find(no_match_id).await.unwrap().unwrap().status,
        IngestStatus::Completed
    );

    fs::remove_dir_all(&work_root).await.unwrap();
}

async fn prepare_fingerprint_request(
    database: &Database,
    work_root: &Path,
    name: &str,
    bytes: &[u8],
) -> PreparedFingerprintRequest {
    let ingest = database
        .inbox()
        .create_ingest(
            IngestSubmission::try_new(IngestSubmissionInput::new(
                format!("https://example.test/{name}"),
                SubmittedVia::Api,
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    sqlx::query("DELETE FROM queue.jobs WHERE payload->>'ingest_id' = $1")
        .bind(ingest.ingest.id.to_string())
        .execute(database.pool())
        .await
        .unwrap();
    let workspace = MediaWorkspace::create(work_root, ingest.ingest.id).await.unwrap();
    let path = workspace.path(WorkspaceArea::Normalized, "canonical.mp4").unwrap();
    fs::write(&path, bytes).await.unwrap();
    let digest = sha256_file(&path).await.unwrap();
    let normalization = AssetNormalization {
        local_work_path: path.to_string_lossy().into_owned(),
        file_size_bytes: digest.bytes,
        sha256: digest.sha256,
        media_kind: SourceMediaKind::Video,
        mime_type: Some("video/mp4".to_owned()),
        container: Some("mp4".to_owned()),
        video_codec: Some("h264".to_owned()),
        audio_codec: None,
        width: Some(320),
        height: Some(240),
        duration_ms: Some(4_000),
        bit_rate: Some(100_000),
        thumbnail: None,
    };
    sqlx::query(
        "UPDATE ingests SET state = 'fingerprinting', input_json = input_json || jsonb_build_object('normalization', $2::jsonb) WHERE id = $1",
    )
    .bind(ingest.ingest.id)
    .bind(serde_json::to_value(&normalization).unwrap())
    .execute(database.pool())
    .await
    .unwrap();
    database
        .jobs()
        .enqueue(
            sooqa_jobs::NewJob::compute_fingerprint(ingest.ingest.id)
                .dedupe_key(format!("test:compute:{}", ingest.ingest.id)),
        )
        .await
        .unwrap();
    PreparedFingerprintRequest { ingest_id: ingest.ingest.id, normalization }
}

struct PreparedFingerprintRequest {
    ingest_id: Uuid,
    normalization: AssetNormalization,
}

async fn run_fingerprint(
    database: &Database,
    work_root: &Path,
    request: PreparedFingerprintRequest,
    extractor: FrameExtractor,
) {
    let handler = compute_fingerprint_handler(
        database.inbox(),
        database.library(),
        work_root.to_owned(),
        extractor,
    );
    let job = database
        .jobs()
        .claim_next("fingerprint-worker", Duration::from_secs(30), &[JobType::ComputeFingerprint])
        .await
        .unwrap()
        .expect("fingerprint job should be claimable");
    let command_ingest_id = match &job.command {
        sooqa_jobs::JobCommand::ComputeFingerprint(payload) => payload.ingest_id,
        command => panic!("expected compute fingerprint command, got {command:?}"),
    };
    assert_eq!(command_ingest_id, request.ingest_id);
    handler(job).await.unwrap();
}

fn video_media_ingest(sha256: Vec<u8>, source: &str) -> MediaIngest {
    MediaIngest {
        media: NewMedia {
            kind: MediaKind::Video,
            title: Some("worker test".to_owned()),
            description: None,
        },
        metadata: MediaMetadata {
            kind: MediaKind::Video,
            mime_type: Some("video/mp4".to_owned()),
            container: Some("mp4".to_owned()),
            video_codec: Some("h264".to_owned()),
            audio_codec: None,
            width: Some(320),
            height: Some(240),
            duration_ms: Some(4_000),
            bit_rate: Some(100_000),
            file_size_bytes: Some(100),
            sha256: Some(sha256),
            local_work_path: Some("/tmp/sooqa-worker-test.mp4".to_owned()),
        },
        source: MediaSourceInput {
            ingest_id: None,
            kind: SourceKind::DirectUrl,
            original_url: Some(source.to_owned()),
            normalized_url: Some(source.to_owned()),
            platform: None,
            platform_content_id: None,
            author_name: None,
            title: None,
            description: None,
            published_at: None,
            metadata: json!({"test": true}),
        },
        tags: Vec::new(),
    }
}

async fn mark_duplicate_pending(database: &Database, ingest_id: Uuid) {
    sqlx::query(
        "UPDATE ingests SET state = 'duplicate_pending', duplicate_evidence = $2 WHERE id = $1",
    )
    .bind(ingest_id)
    .bind(json!({"algorithm_version":"video_sequence_v1", "matches": []}))
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query("DELETE FROM queue.jobs WHERE payload->>'ingest_id' = $1")
        .bind(ingest_id.to_string())
        .execute(database.pool())
        .await
        .unwrap();
}

async fn count_ingest_jobs(database: &Database, ingest_id: Uuid, kind: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM queue.jobs WHERE kind = $2 AND payload->>'ingest_id' = $1",
    )
    .bind(ingest_id.to_string())
    .bind(kind)
    .fetch_one(database.pool())
    .await
    .unwrap()
}

async fn mark_media_ready(database: &Database, media_id: Uuid) {
    sqlx::query(
        "UPDATE media SET storage_state = 'ready', telegram_storage_chat_id = -100123, telegram_storage_message_id = 42, telegram_file_id = 'ready-file' WHERE id = $1",
    )
    .bind(media_id)
    .execute(database.pool())
    .await
    .unwrap();
}

fn hex_bytes(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

async fn hex_bytes_from_media(database: &Database, media_id: Uuid) -> Vec<u8> {
    sqlx::query_scalar::<_, Vec<u8>>("SELECT canonical_sha256 FROM media WHERE id = $1")
        .bind(media_id)
        .fetch_one(database.pool())
        .await
        .unwrap()
}

async fn count_media_with_sha(database: &Database, sha256: Vec<u8>) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM media WHERE canonical_sha256 = $1")
        .bind(sha256)
        .fetch_one(database.pool())
        .await
        .unwrap()
}
