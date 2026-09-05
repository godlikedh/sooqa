//! Canonical media normalization jobs.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use sooqa_inbox::{AssetNormalization, AssetThumbnailNormalization, IngestStatus, SourceMediaKind};
use sooqa_jobs::{Job, JobCommand};
use sooqa_media::{
    ArtifactPublicationError, CANONICAL_VIDEO_PROFILE_VERSION, FfmpegExecutor, ImageNormalizer,
    MediaProbe, MediaStreamKind, MediaWorkspace, NormalizationExecutionError, NormalizationPlanner,
    WorkspaceArea, decode_first_preview_frame, encode_bounded_preview, publish_artifact,
    sha256_file,
};
use sooqa_persistence::{AssetNormalizationStart, InboxRepository};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::common::{
    HandlerFailure, HandlerFn, WorkspaceAdmission, load_ingest_for_admission, map_inbox_error,
    map_workspace_error, request_media_kind, source_artifact_exists, workspace_input,
};

#[derive(Debug, Clone, Copy)]
struct NormalizationLimits {
    max_normalized_storage_bytes: u64,
    admission: WorkspaceAdmission,
}

pub(crate) fn normalization_stage_may_run(status: IngestStatus) -> bool {
    matches!(status, IngestStatus::Normalizing | IngestStatus::FailedRetryable)
}

pub fn normalize_asset_handler(
    inbox: InboxRepository,
    work_root: impl Into<std::path::PathBuf>,
    planner: NormalizationPlanner,
    executor: FfmpegExecutor,
    image_normalizer: ImageNormalizer,
    max_normalized_storage_bytes: u64,
) -> HandlerFn {
    normalize_asset_handler_with_admission(
        inbox,
        work_root,
        planner,
        executor,
        image_normalizer,
        max_normalized_storage_bytes,
        WorkspaceAdmission::disabled(),
    )
}

pub fn normalize_asset_handler_with_admission(
    inbox: InboxRepository,
    work_root: impl Into<PathBuf>,
    planner: NormalizationPlanner,
    executor: FfmpegExecutor,
    image_normalizer: ImageNormalizer,
    max_normalized_storage_bytes: u64,
    admission: WorkspaceAdmission,
) -> HandlerFn {
    let work_root = work_root.into();
    Arc::new(move |job| {
        let inbox = inbox.clone();
        let work_root = work_root.clone();
        let planner = planner.clone();
        let executor = executor.clone();
        let limits = NormalizationLimits { max_normalized_storage_bytes, admission };
        Box::pin(async move {
            normalize_asset(&inbox, &work_root, &planner, &executor, image_normalizer, limits, job)
                .await
        })
    })
}

async fn normalize_asset(
    inbox: &InboxRepository,
    work_root: &std::path::Path,
    planner: &NormalizationPlanner,
    executor: &FfmpegExecutor,
    image_normalizer: ImageNormalizer,
    limits: NormalizationLimits,
    job: Job,
) -> Result<(), HandlerFailure> {
    let NormalizationLimits { max_normalized_storage_bytes, admission } = limits;
    let ingest_request_id = match &job.command {
        JobCommand::NormalizeAsset(payload) => payload.ingest_id,
        _ => {
            return Err(HandlerFailure::permanent(
                "invalid_payload",
                "normalize_asset handler received a different job command",
            ));
        }
    };
    let job_attempt = job.lease().ok_or_else(|| {
        HandlerFailure::permanent(
            "invalid_job_state",
            "normalize_asset handler requires a running job lease",
        )
    })?;
    // Parse the already persisted probe and reserve space before the durable
    // normalization stage transition. A low-space refusal therefore leaves
    // the ingest in its current state for a later retry.
    let current_request = load_ingest_for_admission(inbox, ingest_request_id).await?;
    let preflight_failure = preflight_normalization(
        &current_request,
        work_root,
        admission,
        max_normalized_storage_bytes,
    )?;

    let request = match inbox.begin_asset_normalization(ingest_request_id, &job_attempt).await {
        Ok(AssetNormalizationStart::Ready(request)) => request,
        Ok(AssetNormalizationStart::AlreadyAdvanced(_)) => return Ok(()),
        Err(error) => return Err(map_inbox_error(error)),
    };
    if let Some(failure) = preflight_failure {
        return settle_normalization(inbox, ingest_request_id, &job_attempt, Err(failure)).await;
    }

    let (probe, media_kind) = match stored_probe_and_kind(&request) {
        Ok(value) => value,
        Err(failure) => {
            return settle_normalization(inbox, ingest_request_id, &job_attempt, Err(failure))
                .await;
        }
    };
    let artifact = match media_kind {
        SourceMediaKind::Image => {
            normalize_image_asset(
                work_root,
                image_normalizer,
                &request,
                max_normalized_storage_bytes,
            )
            .await
        }
        SourceMediaKind::Animation | SourceMediaKind::Audio => {
            normalize_exact_asset(
                work_root,
                &request,
                ExactNormalizationSpec { media_kind, probe: &probe, max_normalized_storage_bytes },
            )
            .await
        }
        SourceMediaKind::Video => {
            normalize_video_asset(
                work_root,
                planner,
                executor,
                &request,
                &probe,
                &job,
                max_normalized_storage_bytes,
            )
            .await
        }
        _ => Err(HandlerFailure::permanent(
            "unsupported_media_kind",
            format!("asset media kind {media_kind:?} is not supported by the video normalizer"),
        )),
    };
    settle_normalization(inbox, ingest_request_id, &job_attempt, artifact).await
}

/// Performs checks that must happen before the durable stage transition.
///
/// Admission failures are returned immediately so no reservation is made, while
/// malformed persisted metadata is returned as a deferred failure: the caller
/// settles it only after `begin_asset_normalization` has recorded ownership of
/// the running stage.
fn preflight_normalization(
    request: &sooqa_inbox::Ingest,
    work_root: &Path,
    admission: WorkspaceAdmission,
    max_normalized_storage_bytes: u64,
) -> Result<Option<HandlerFailure>, HandlerFailure> {
    if !normalization_stage_may_run(request.status) {
        return Ok(None);
    }
    let input_data = match request.input_data() {
        Ok(input_data) => input_data,
        Err(error) => {
            return Ok(Some(HandlerFailure::permanent("invalid_ingest_state", error.to_string())));
        }
    };
    if input_data.normalization.is_some() && !request.force_save {
        return Ok(None);
    }
    let (_, media_kind) = match stored_probe_and_kind(request) {
        Ok(value) => value,
        Err(failure) => return Ok(Some(failure)),
    };
    match media_kind {
        SourceMediaKind::Video => {
            admission.admit(work_root, max_normalized_storage_bytes.saturating_mul(2))?;
        }
        _ => admission.admit(work_root, max_normalized_storage_bytes)?,
    }
    Ok(None)
}

fn stored_probe_and_kind(
    request: &sooqa_inbox::Ingest,
) -> Result<(MediaProbe, SourceMediaKind), HandlerFailure> {
    let input_data = request
        .input_data()
        .map_err(|error| HandlerFailure::permanent("invalid_ingest_state", error.to_string()))?;
    let probe = input_data
        .probe
        .as_ref()
        .ok_or_else(|| {
            HandlerFailure::permanent(
                "invalid_ingest_state",
                "ingest request has no stored media probe",
            )
        })?
        .decode::<MediaProbe>()
        .map_err(|error| {
            HandlerFailure::permanent(
                "invalid_ingest_state",
                format!("stored media probe could not be decoded: {error}"),
            )
        })?;
    let media_kind =
        probe_media_kind(&probe).or_else(|| request_media_kind(request)).ok_or_else(|| {
            HandlerFailure::permanent(
                "invalid_ingest_state",
                "ingest request has no stored source media kind",
            )
        })?;
    Ok((probe, media_kind))
}

async fn normalize_video_asset(
    work_root: &Path,
    planner: &NormalizationPlanner,
    executor: &FfmpegExecutor,
    request: &sooqa_inbox::Ingest,
    probe: &MediaProbe,
    job: &Job,
    max_normalized_storage_bytes: u64,
) -> Result<AssetNormalization, HandlerFailure> {
    let (workspace, input_name) = prepare_workspace(work_root, request).await?;
    let input_path =
        workspace.path(WorkspaceArea::Source, input_name).map_err(map_workspace_error)?;
    let output_path =
        workspace.path(WorkspaceArea::Normalized, "canonical.mp4").map_err(map_workspace_error)?;
    let plan = planner
        .plan(&input_path, &output_path, probe)
        .map_err(|error| HandlerFailure::permanent("normalize_plan", error.to_string()))?;
    let result = execute_video_normalization(
        planner,
        executor,
        &input_path,
        &output_path,
        probe,
        max_normalized_storage_bytes,
        plan,
    )
    .await
    .map_err(|error| map_video_normalization_error(error, job))?;
    Ok(normalization_metadata(result))
}

fn map_video_normalization_error(error: NormalizationExecutionError, job: &Job) -> HandlerFailure {
    let retryable = normalization_error_is_retryable(&error);
    let message = error.to_string();
    if retryable && job.attempt_count < job.max_attempts {
        HandlerFailure::retryable("normalize_timeout", message)
    } else {
        HandlerFailure::permanent("normalize", message)
    }
}

async fn prepare_workspace(
    work_root: &Path,
    request: &sooqa_inbox::Ingest,
) -> Result<(MediaWorkspace, &'static str), HandlerFailure> {
    let (workspace_id, input_name) = workspace_input(request)?;
    let workspace =
        MediaWorkspace::create(work_root, workspace_id).await.map_err(map_workspace_error)?;
    workspace.validate().map_err(map_workspace_error)?;
    Ok((workspace, input_name))
}

struct VideoCandidateCleanup {
    paths: Vec<PathBuf>,
}

impl VideoCandidateCleanup {
    fn push(&mut self, path: PathBuf) {
        self.paths.push(path);
    }

    fn paths(&self) -> &[PathBuf] {
        &self.paths
    }

    fn forget(&mut self, path: &Path) {
        self.paths.retain(|candidate| candidate != path);
    }
}

impl Drop for VideoCandidateCleanup {
    fn drop(&mut self) {
        // Async cleanup is attempted on normal paths below. This synchronous
        // guard covers task cancellation or worker shutdown between awaits.
        for path in &self.paths {
            let _ = fs::remove_file(path);
        }
    }
}

#[derive(Clone, Copy)]
enum FallbackPolicy {
    KeepFirst,
    KeepSmallest,
}

/// Tracks all adaptation outputs until one candidate is published.
///
/// A candidate can be retained as the best quality-preserving fallback, but
/// every other output remains owned by the cleanup guard until it is removed.
struct VideoCandidateSet {
    cleanup: VideoCandidateCleanup,
    fallback: Option<sooqa_media::NormalizationResult>,
    largest_oversized_candidate: Option<u64>,
    attempts: usize,
    target_max_bytes: u64,
    storage_limit: u64,
}

impl VideoCandidateSet {
    fn new(target_max_bytes: u64, storage_limit: u64) -> Self {
        Self {
            cleanup: VideoCandidateCleanup { paths: Vec::new() },
            fallback: None,
            largest_oversized_candidate: None,
            attempts: 0,
            target_max_bytes,
            storage_limit,
        }
    }

    fn next_path(&mut self, output_path: &Path) -> PathBuf {
        self.attempts += 1;
        let path = video_candidate_path(output_path);
        self.cleanup.push(path.clone());
        path
    }

    fn record_attempt(&mut self) {
        self.attempts += 1;
    }

    async fn consider(
        &mut self,
        result: sooqa_media::NormalizationResult,
        fallback_policy: FallbackPolicy,
    ) -> Option<sooqa_media::NormalizationResult> {
        let fits_storage = result.digest.bytes <= self.storage_limit;
        let selects_candidate = fits_storage
            && match fallback_policy {
                FallbackPolicy::KeepFirst => result.digest.bytes <= self.target_max_bytes,
                FallbackPolicy::KeepSmallest => self
                    .fallback
                    .as_ref()
                    .is_none_or(|previous| result.digest.bytes < previous.digest.bytes),
            };
        if selects_candidate {
            if let Some(previous) = self.fallback.take() {
                remove_video_candidate(&previous.output_path, &mut self.cleanup).await;
            }
            return Some(result);
        }

        if result.digest.bytes > self.storage_limit {
            self.largest_oversized_candidate =
                Some(self.largest_oversized_candidate.unwrap_or(0).max(result.digest.bytes));
        }

        let should_keep_as_fallback = fits_storage
            && self.fallback.is_none()
            && matches!(fallback_policy, FallbackPolicy::KeepFirst);
        if should_keep_as_fallback {
            self.fallback = Some(result);
        } else {
            remove_video_candidate(&result.output_path, &mut self.cleanup).await;
        }
        None
    }

    fn take_fallback(
        &mut self,
    ) -> Result<sooqa_media::NormalizationResult, NormalizationExecutionError> {
        self.fallback.take().ok_or(NormalizationExecutionError::OutputExceedsStorageLimit {
            bytes: self.largest_oversized_candidate.unwrap_or(self.storage_limit),
            limit: self.storage_limit,
        })
    }
}

struct VideoPasslogCleanup {
    directory: Option<PathBuf>,
}

impl VideoPasslogCleanup {
    async fn reserve(output_path: &Path) -> Result<Self, NormalizationExecutionError> {
        let directory = output_path.with_file_name(format!(".sooqa-two-pass-{}", Uuid::new_v4()));
        tokio::fs::create_dir(&directory).await.map_err(|source| {
            NormalizationExecutionError::TemporaryOutput { path: directory.clone(), source }
        })?;
        Ok(Self { directory: Some(directory) })
    }

    fn prefix(&self) -> PathBuf {
        self.directory.as_deref().expect("two-pass directory guard must be armed").join("stats")
    }

    async fn remove(&mut self) {
        let Some(directory) = self.directory.as_ref() else {
            return;
        };
        if tokio::fs::remove_dir_all(directory).await.is_ok() {
            self.directory = None;
        }
    }
}

impl Drop for VideoPasslogCleanup {
    fn drop(&mut self) {
        if let Some(directory) = self.directory.take() {
            let _ = fs::remove_dir_all(directory);
        }
    }
}

async fn execute_video_normalization(
    planner: &NormalizationPlanner,
    executor: &FfmpegExecutor,
    input_path: &Path,
    output_path: &Path,
    probe: &MediaProbe,
    max_normalized_storage_bytes: u64,
    initial_plan: sooqa_media::NormalizationPlan,
) -> Result<sooqa_media::NormalizationResult, NormalizationExecutionError> {
    let mut candidates =
        VideoCandidateSet::new(planner.profile().target_max_bytes, max_normalized_storage_bytes);

    if initial_plan.mode() == sooqa_media::NormalizationMode::Remux {
        // Never run a decision-making remux directly against canonical.mp4.
        // Its actual bytes may cross the target, and a lease-expired worker
        // must not delete or replace a newer canonical artifact.
        let candidate_path = candidates.next_path(output_path);
        let remux_plan = initial_plan.with_output(&candidate_path);
        let result = executor.execute(&remux_plan, std::future::pending()).await?;
        if let Some(selected) = candidates.consider(result, FallbackPolicy::KeepFirst).await {
            return publish_video_candidate(
                selected,
                output_path,
                &mut candidates.cleanup,
                candidates.attempts,
                planner.profile().target_max_bytes,
            )
            .await;
        }
    }

    let video = probe
        .streams
        .iter()
        .find(|stream| stream.kind == MediaStreamKind::Video)
        .ok_or(NormalizationExecutionError::OutputHasNoVideo)?;
    let ladder = planner.resolution_ladder(video);
    let Some(quality_dimensions) = ladder.first().copied() else {
        return Err(NormalizationExecutionError::InvalidOutputProfile {
            message: "video dimensions are missing or invalid",
        });
    };

    // One constant-quality encode lets x264 exploit low-complexity inputs
    // without padding them toward the preferred byte target. It also gives an
    // incompatible source a bounded quality-preserving fallback.
    let quality_path = candidates.next_path(output_path);
    let quality_plan = if initial_plan.mode() == sooqa_media::NormalizationMode::Transcode {
        initial_plan.with_output(&quality_path)
    } else {
        planner
            .plan_quality_candidate(input_path, &quality_path, probe, quality_dimensions)
            .map_err(map_candidate_plan_error)?
    };
    let quality =
        execute_quality_candidate(executor, &quality_plan, quality_dimensions, video, planner)
            .await?;
    if let Some(selected) = candidates.consider(quality, FallbackPolicy::KeepFirst).await {
        return publish_video_candidate(
            selected,
            output_path,
            &mut candidates.cleanup,
            candidates.attempts,
            planner.profile().target_max_bytes,
        )
        .await;
    }

    if let Some((dimensions, video_bitrate_kbps)) = planner.two_pass_candidate(probe) {
        let candidate_path = candidates.next_path(output_path);
        let mut passlogs = VideoPasslogCleanup::reserve(output_path).await?;
        let passlog_prefix = passlogs.prefix();
        let plan = planner
            .plan_two_pass(
                input_path,
                &candidate_path,
                probe,
                dimensions,
                video_bitrate_kbps,
                &passlog_prefix,
            )
            .map_err(map_candidate_plan_error)?;
        candidates.record_attempt();
        let result =
            execute_two_pass_candidate(executor, &plan, dimensions, video, planner).await?;
        passlogs.remove().await;
        if let Some(selected) = candidates.consider(result, FallbackPolicy::KeepSmallest).await {
            return publish_video_candidate(
                selected,
                output_path,
                &mut candidates.cleanup,
                candidates.attempts,
                planner.profile().target_max_bytes,
            )
            .await;
        }
    } else {
        // The bitrate heuristic is deliberately conservative and can reject a
        // fixed-size encode for low-complexity material that CRF can represent
        // efficiently. Try each remaining quality-preserving resolution once;
        // if even the floor misses, retain the original-quality fallback.
        for dimensions in ladder.into_iter().skip(1) {
            let candidate_path = candidates.next_path(output_path);
            let plan = planner
                .plan_quality_candidate(input_path, &candidate_path, probe, dimensions)
                .map_err(map_candidate_plan_error)?;
            let result =
                execute_quality_candidate(executor, &plan, dimensions, video, planner).await?;
            if let Some(selected) = candidates.consider(result, FallbackPolicy::KeepFirst).await {
                return publish_video_candidate(
                    selected,
                    output_path,
                    &mut candidates.cleanup,
                    candidates.attempts,
                    planner.profile().target_max_bytes,
                )
                .await;
            }
        }
    }

    let selected = candidates.take_fallback()?;
    publish_video_candidate(
        selected,
        output_path,
        &mut candidates.cleanup,
        candidates.attempts,
        planner.profile().target_max_bytes,
    )
    .await
}

async fn execute_quality_candidate(
    executor: &FfmpegExecutor,
    plan: &sooqa_media::NormalizationPlan,
    dimensions: sooqa_media::VideoDimensions,
    source: &sooqa_media::MediaStream,
    planner: &NormalizationPlanner,
) -> Result<sooqa_media::NormalizationResult, NormalizationExecutionError> {
    let result = executor.execute(plan, std::future::pending()).await?;
    validate_adapted_dimensions(&result.probe, dimensions, source, planner)?;
    Ok(result)
}

async fn execute_two_pass_candidate(
    executor: &FfmpegExecutor,
    plan: &sooqa_media::TwoPassNormalizationPlan,
    dimensions: sooqa_media::VideoDimensions,
    source: &sooqa_media::MediaStream,
    planner: &NormalizationPlanner,
) -> Result<sooqa_media::NormalizationResult, NormalizationExecutionError> {
    executor.execute_analysis(plan.first_pass(), std::future::pending()).await?;
    let result = executor.execute(plan.second_pass(), std::future::pending()).await?;
    validate_adapted_dimensions(&result.probe, dimensions, source, planner)?;
    Ok(result)
}

fn map_candidate_plan_error(error: sooqa_media::NormalizationError) -> NormalizationExecutionError {
    NormalizationExecutionError::InvalidOutputProfile {
        message: match error {
            sooqa_media::NormalizationError::InvalidCandidateDimensions { .. } => {
                "candidate dimensions are outside the canonical profile"
            }
            _ => "candidate normalization plan is invalid",
        },
    }
}

fn video_candidate_path(output_path: &Path) -> PathBuf {
    output_path.with_file_name(format!(".sooqa-inline-candidate-{}.mp4", Uuid::new_v4()))
}

async fn remove_video_candidate(path: &Path, candidates: &mut VideoCandidateCleanup) {
    match tokio::fs::remove_file(path).await {
        Ok(()) => candidates.forget(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => candidates.forget(path),
        Err(_) => {
            // Keep the guard armed when async cleanup fails. A synchronous
            // retry still runs before the task can be cancelled or dropped.
            if fs::remove_file(path).is_ok() || !path.exists() {
                candidates.forget(path);
            }
        }
    }
}

async fn publish_video_candidate(
    mut selected: sooqa_media::NormalizationResult,
    output_path: &Path,
    candidates: &mut VideoCandidateCleanup,
    attempts: usize,
    target_max_bytes: u64,
) -> Result<sooqa_media::NormalizationResult, NormalizationExecutionError> {
    let selected_path = selected.output_path.clone();
    if let Err(error) = publish_artifact(&selected_path, output_path).await {
        return Err(NormalizationExecutionError::OutputPublish {
            path: output_path.to_owned(),
            message: error.to_string(),
        });
    }
    selected.output_path = output_path.to_owned();
    let remaining = candidates.paths().to_vec();
    for candidate in remaining {
        remove_video_candidate(&candidate, candidates).await;
    }
    debug!(
        attempts,
        selected_bytes = selected.digest.bytes,
        target_bytes = target_max_bytes,
        "selected bounded video adaptation candidate"
    );
    Ok(selected)
}

fn validate_adapted_dimensions(
    probe: &MediaProbe,
    requested: sooqa_media::VideoDimensions,
    source: &sooqa_media::MediaStream,
    planner: &NormalizationPlanner,
) -> Result<(), NormalizationExecutionError> {
    let profile = planner.profile();
    let video = probe
        .streams
        .iter()
        .find(|stream| stream.kind == MediaStreamKind::Video)
        .ok_or(NormalizationExecutionError::OutputHasNoVideo)?;
    let Some((width, height)) = video.width.zip(video.height) else {
        return Err(NormalizationExecutionError::InvalidOutputProfile {
            message: "adapted video dimensions are missing",
        });
    };
    if width == 0
        || height == 0
        || !width.is_multiple_of(2)
        || !height.is_multiple_of(2)
        || width > requested.width
        || height > requested.height
        || width > profile.max_width
        || height > profile.max_height
        || planner
            .effective_minimum_short_edge(source)
            .is_some_and(|minimum| width.min(height) < minimum)
    {
        return Err(NormalizationExecutionError::InvalidOutputProfile {
            message: "adapted video dimensions exceed the bounded ladder",
        });
    }
    Ok(())
}

async fn normalize_image_asset(
    work_root: &Path,
    image_normalizer: ImageNormalizer,
    request: &sooqa_inbox::Ingest,
    max_normalized_storage_bytes: u64,
) -> Result<AssetNormalization, HandlerFailure> {
    let (workspace, input_name) = prepare_workspace(work_root, request).await?;
    let plan = match image_normalizer.plan(&workspace, input_name, "canonical", "thumbnail") {
        Ok(plan) => plan,
        Err(error) => {
            return Err(HandlerFailure::permanent("normalize_plan", error.to_string()));
        }
    };
    let result = match image_normalizer.execute(&plan).await {
        Ok(result) => result,
        Err(error) => return Err(HandlerFailure::permanent("normalize_image", error.to_string())),
    };
    if let Some(failure) = normalized_storage_limit_failure(
        result.canonical_digest.bytes,
        max_normalized_storage_bytes,
    ) {
        return Err(failure);
    }
    Ok(image_normalization_metadata(result))
}

struct ExactNormalizationSpec<'a> {
    media_kind: SourceMediaKind,
    probe: &'a MediaProbe,
    max_normalized_storage_bytes: u64,
}

async fn normalize_exact_asset(
    work_root: &Path,
    request: &sooqa_inbox::Ingest,
    spec: ExactNormalizationSpec<'_>,
) -> Result<AssetNormalization, HandlerFailure> {
    let ExactNormalizationSpec { media_kind, probe, max_normalized_storage_bytes } = spec;
    let (workspace, input_name) = prepare_workspace(work_root, request).await?;
    let input_path =
        workspace.path(WorkspaceArea::Source, input_name).map_err(map_workspace_error)?;
    let canonical_name = match media_kind {
        SourceMediaKind::Animation => "canonical.animation",
        SourceMediaKind::Audio => "canonical.audio",
        SourceMediaKind::Video | SourceMediaKind::Image | SourceMediaKind::Unknown => {
            "canonical.media"
        }
    };
    let canonical_path =
        workspace.path(WorkspaceArea::Normalized, canonical_name).map_err(map_workspace_error)?;
    match source_artifact_exists(&canonical_path).await {
        Ok(true) => {}
        Ok(false) => match publish_artifact(&input_path, &canonical_path).await {
            Ok(()) | Err(ArtifactPublicationError::DestinationConflict) => {}
            Err(error) => {
                return Err(HandlerFailure::permanent("normalize_exact", error.to_string()));
            }
        },
        Err(failure) => return Err(failure),
    }
    let digest = match sha256_file(&canonical_path).await {
        Ok(digest) => digest,
        Err(error) => return Err(HandlerFailure::permanent("normalize_exact", error.to_string())),
    };
    if let Some(failure) =
        normalized_storage_limit_failure(digest.bytes, max_normalized_storage_bytes)
    {
        return Err(failure);
    }
    let thumbnail = if media_kind == SourceMediaKind::Animation {
        match decode_first_preview_frame(&canonical_path).await {
            Ok(frame) => match encode_bounded_preview(&frame) {
                Ok(preview) => {
                    let thumbnail_path = workspace
                        .path(WorkspaceArea::Previews, "animation-preview.jpg")
                        .map_err(map_workspace_error)?;
                    let temporary_path = workspace
                        .path(
                            WorkspaceArea::Previews,
                            &format!(".animation-preview-{}.tmp", Uuid::new_v4()),
                        )
                        .map_err(map_workspace_error)?;
                    tokio::fs::write(&temporary_path, &preview.bytes).await.map_err(|error| {
                        HandlerFailure::permanent(
                            "normalize_animation_preview",
                            format!("animation preview could not be staged: {error}"),
                        )
                    })?;
                    let published = publish_artifact(&temporary_path, &thumbnail_path).await;
                    let _ = tokio::fs::remove_file(&temporary_path).await;
                    match published {
                        Ok(()) | Err(ArtifactPublicationError::DestinationConflict) => {
                            Some(AssetThumbnailNormalization {
                                local_work_path: thumbnail_path.to_string_lossy().into_owned(),
                                file_size_bytes: preview.digest.bytes,
                                sha256: preview.digest.sha256,
                                mime_type: Some("image/jpeg".to_owned()),
                                width: Some(preview.width),
                                height: Some(preview.height),
                            })
                        }
                        Err(error) => {
                            warn!(error = %error, "animation preview publication was skipped");
                            None
                        }
                    }
                }
                Err(error) => {
                    warn!(error = %error, "animation preview encoding was skipped");
                    None
                }
            },
            Err(error) => {
                debug!(error = %error, "animation decoder could not produce a safe preview frame");
                None
            }
        }
    } else {
        None
    };
    let video = probe.streams.iter().find(|stream| stream.kind == MediaStreamKind::Video);
    let audio = probe.streams.iter().find(|stream| stream.kind == MediaStreamKind::Audio);
    let normalization = AssetNormalization {
        local_work_path: canonical_path.to_string_lossy().into_owned(),
        file_size_bytes: digest.bytes,
        sha256: digest.sha256,
        media_kind,
        profile_version: None,
        mime_type: source_mime_type(request),
        container: probe.container_format.clone(),
        video_codec: video.and_then(|stream| stream.codec.clone()),
        audio_codec: audio.and_then(|stream| stream.codec.clone()),
        width: video.and_then(|stream| stream.width),
        height: video.and_then(|stream| stream.height),
        duration_ms: probe.duration_ms,
        bit_rate: probe.bit_rate,
        thumbnail,
    };
    Ok(normalization)
}

fn source_mime_type(request: &sooqa_inbox::Ingest) -> Option<String> {
    request.input_data().ok()?.mime_type().map(ToOwned::to_owned)
}

fn normalization_error_is_retryable(error: &NormalizationExecutionError) -> bool {
    match error {
        NormalizationExecutionError::Command(error) => error.is_timeout(),
        NormalizationExecutionError::Probe(error) => error.is_retryable(),
        _ => false,
    }
}

fn probe_media_kind(probe: &MediaProbe) -> Option<SourceMediaKind> {
    let container = probe.container_format.as_deref().map(str::to_ascii_lowercase);
    let video_streams = probe
        .streams
        .iter()
        .filter(|stream| matches!(&stream.kind, MediaStreamKind::Video) && !stream.attached_picture)
        .collect::<Vec<_>>();
    let codecs =
        video_streams.iter().filter_map(|stream| stream.codec.as_deref()).collect::<Vec<_>>();
    let is_gif = container.as_deref().is_some_and(|value| value.contains("gif"))
        || codecs.iter().any(|value| value.to_ascii_lowercase().contains("gif"));
    if is_gif {
        return Some(SourceMediaKind::Animation);
    }

    let is_image_container = container.as_deref().is_some_and(|value| {
        ["image2", "png", "jpeg", "jpg", "webp", "avif", "mjpeg"]
            .iter()
            .any(|format| value.contains(format))
    });
    let is_image_codec = codecs
        .iter()
        .any(|value| ["png", "webp"].iter().any(|format| value.eq_ignore_ascii_case(format)))
        || (container.is_none() && codecs.iter().any(|value| value.eq_ignore_ascii_case("mjpeg")));
    if is_image_container || is_image_codec {
        return Some(SourceMediaKind::Image);
    }
    if !video_streams.is_empty() {
        return Some(SourceMediaKind::Video);
    }
    if probe.streams.iter().any(|stream| matches!(&stream.kind, MediaStreamKind::Audio)) {
        return Some(SourceMediaKind::Audio);
    }
    None
}

fn normalization_metadata(result: sooqa_media::NormalizationResult) -> AssetNormalization {
    let video =
        result.probe.streams.iter().find(|stream| matches!(&stream.kind, MediaStreamKind::Video));
    let audio =
        result.probe.streams.iter().find(|stream| matches!(&stream.kind, MediaStreamKind::Audio));
    AssetNormalization {
        local_work_path: result.output_path.to_string_lossy().into_owned(),
        file_size_bytes: result.digest.bytes,
        sha256: result.digest.sha256,
        media_kind: SourceMediaKind::Video,
        profile_version: Some(CANONICAL_VIDEO_PROFILE_VERSION.to_owned()),
        mime_type: Some("video/mp4".to_owned()),
        container: result.probe.container_format,
        video_codec: video.and_then(|stream| stream.codec.clone()),
        audio_codec: audio.and_then(|stream| stream.codec.clone()),
        width: video.and_then(|stream| stream.width),
        height: video.and_then(|stream| stream.height),
        duration_ms: result.probe.duration_ms,
        bit_rate: result.probe.bit_rate,
        thumbnail: None,
    }
}

fn image_normalization_metadata(
    result: sooqa_media::ImageNormalizationResult,
) -> AssetNormalization {
    AssetNormalization {
        local_work_path: result.canonical_path.to_string_lossy().into_owned(),
        file_size_bytes: result.canonical_digest.bytes,
        sha256: result.canonical_digest.sha256,
        media_kind: SourceMediaKind::Image,
        profile_version: None,
        mime_type: Some(result.format.mime_type().to_owned()),
        container: Some(result.format.extension().to_owned()),
        video_codec: None,
        audio_codec: None,
        width: Some(result.width),
        height: Some(result.height),
        duration_ms: None,
        bit_rate: None,
        thumbnail: Some(AssetThumbnailNormalization {
            local_work_path: result.thumbnail_path.to_string_lossy().into_owned(),
            file_size_bytes: result.thumbnail_digest.bytes,
            sha256: result.thumbnail_digest.sha256,
            mime_type: Some(result.thumbnail_format.mime_type().to_owned()),
            width: Some(result.thumbnail_width),
            height: Some(result.thumbnail_height),
        }),
    }
}

fn normalized_storage_limit_failure(bytes: u64, limit: u64) -> Option<HandlerFailure> {
    (bytes > limit).then(|| {
        HandlerFailure::permanent(
            "normalized_storage_too_large",
            format!(
                "canonical normalized media is {bytes} bytes, above the configured storage ceiling of {limit} bytes"
            ),
        )
    })
}

async fn settle_normalization(
    inbox: &InboxRepository,
    ingest_request_id: Uuid,
    job_attempt: &sooqa_jobs::JobLease,
    artifact: Result<AssetNormalization, HandlerFailure>,
) -> Result<(), HandlerFailure> {
    match artifact {
        Ok(normalization) => inbox
            .complete_asset_normalization(ingest_request_id, job_attempt, normalization)
            .await
            .map(|_| ())
            .map_err(map_inbox_error),
        Err(failure) => fail_normalization(inbox, ingest_request_id, job_attempt, failure).await,
    }
}

async fn fail_normalization(
    inbox: &InboxRepository,
    ingest_request_id: uuid::Uuid,
    job_attempt: &sooqa_jobs::JobLease,
    failure: HandlerFailure,
) -> Result<(), HandlerFailure> {
    let status = if failure.retryable {
        IngestStatus::FailedRetryable
    } else {
        IngestStatus::FailedTerminal
    };
    inbox
        .fail_asset_normalization(
            ingest_request_id,
            job_attempt,
            status,
            &failure.class,
            &failure.message,
        )
        .await
        .map_err(map_inbox_error)?;
    Err(failure)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        path::Path,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use async_trait::async_trait;
    use tokio::sync::Notify;

    use sooqa_media::{
        CanonicalVideoProfile, CommandError, ExternalCommand, ExternalCommandOutput,
        ExternalCommandRunner, FfmpegExecutor, FfprobeAdapter, FrameRate, MediaProbe, MediaStream,
        MediaStreamKind, NormalizationMode,
    };

    use super::*;

    #[test]
    fn normalized_storage_limit_failure_is_terminal_and_descriptive() {
        let failure = normalized_storage_limit_failure(101, 100)
            .expect("an oversized canonical artifact should fail");
        assert!(!failure.retryable);
        assert_eq!(failure.class, "normalized_storage_too_large");
        assert!(failure.message.contains("101 bytes"));
        assert!(failure.message.contains("100 bytes"));
        assert!(normalized_storage_limit_failure(100, 100).is_none());
    }

    #[test]
    fn attached_png_does_not_override_typed_mp4_video_kind() {
        let mut probe = adaptation_probe(1920, 1080, 35_923_336);
        probe.container_format = Some("mov,mp4,m4a,3gp,3g2,mj2".to_owned());
        probe.duration_ms = Some(109_274);
        probe.streams.push(MediaStream {
            index: 2,
            kind: MediaStreamKind::Video,
            codec: Some("png".to_owned()),
            attached_picture: true,
            codec_tag: None,
            codec_mime: None,
            level: None,
            profile: None,
            pixel_format: Some("rgb24".to_owned()),
            width: Some(1280),
            height: Some(720),
            display_aspect_ratio: None,
            frame_rate: None,
            rotation_degrees: None,
            sample_rate_hz: None,
            channels: None,
            bit_rate: None,
        });

        assert_eq!(probe_media_kind(&probe), Some(SourceMediaKind::Video));
    }

    #[tokio::test]
    #[ignore = "requires ffprobe and SOOQA_TEST_ATTACHED_PICTURE_MEDIA"]
    async fn classifies_external_attached_picture_fixture_as_video() {
        let path = std::env::var_os("SOOQA_TEST_ATTACHED_PICTURE_MEDIA")
            .expect("SOOQA_TEST_ATTACHED_PICTURE_MEDIA must name the external fixture");
        let probe = FfprobeAdapter::new("ffprobe", Duration::from_secs(30))
            .probe(path)
            .await
            .expect("external fixture should be probeable");

        assert!(probe.streams.iter().any(|stream| stream.attached_picture));
        assert_eq!(probe_media_kind(&probe), Some(SourceMediaKind::Video));
    }

    #[derive(Clone)]
    struct VideoAdaptationRunner {
        sizes: Arc<Mutex<VecDeque<usize>>>,
        commands: Arc<Mutex<Vec<ExternalCommand>>>,
        source_dimensions: (u32, u32),
        last_dimensions: Arc<Mutex<(u32, u32)>>,
        fail_at_ffmpeg: Option<usize>,
        block_at_ffmpeg: Option<usize>,
        blocked: Option<Arc<Notify>>,
    }

    impl VideoAdaptationRunner {
        fn new(sizes: impl IntoIterator<Item = usize>, source_dimensions: (u32, u32)) -> Self {
            Self {
                sizes: Arc::new(Mutex::new(sizes.into_iter().collect())),
                commands: Arc::new(Mutex::new(Vec::new())),
                source_dimensions,
                last_dimensions: Arc::new(Mutex::new(source_dimensions)),
                fail_at_ffmpeg: None,
                block_at_ffmpeg: None,
                blocked: None,
            }
        }

        fn failing_at(mut self, attempt: usize) -> Self {
            self.fail_at_ffmpeg = Some(attempt);
            self
        }

        fn blocking_at(mut self, attempt: usize, blocked: Arc<Notify>) -> Self {
            self.block_at_ffmpeg = Some(attempt);
            self.blocked = Some(blocked);
            self
        }

        fn ffmpeg_commands(&self) -> Vec<ExternalCommand> {
            self.commands
                .lock()
                .expect("runner command mutex should not be poisoned")
                .iter()
                .filter(|command| command.program() == Path::new("ffmpeg"))
                .cloned()
                .collect()
        }
    }

    #[async_trait]
    impl ExternalCommandRunner for VideoAdaptationRunner {
        async fn run(
            &self,
            command: ExternalCommand,
        ) -> Result<ExternalCommandOutput, CommandError> {
            let is_ffmpeg = command.program() == Path::new("ffmpeg");
            self.commands
                .lock()
                .expect("runner command mutex should not be poisoned")
                .push(command.clone());
            if is_ffmpeg {
                let attempt = self
                    .commands
                    .lock()
                    .expect("runner command mutex should not be poisoned")
                    .iter()
                    .filter(|command| command.program() == Path::new("ffmpeg"))
                    .count();
                if self.block_at_ffmpeg == Some(attempt) {
                    if let Some(blocked) = &self.blocked {
                        blocked.notify_waiters();
                    }
                    return std::future::pending().await;
                }
                if self.fail_at_ffmpeg == Some(attempt) {
                    return Ok(ExternalCommandOutput {
                        success: false,
                        exit_code: Some(1),
                        stdout: Vec::new(),
                        stderr: b"synthetic ffmpeg failure".to_vec(),
                        stdout_truncated: false,
                        stderr_truncated: false,
                    });
                }
                let pass = command
                    .args()
                    .windows(2)
                    .find(|pair| pair[0] == "-pass")
                    .and_then(|pair| pair[1].to_str());
                if pass == Some("1") {
                    let prefix = command
                        .args()
                        .windows(2)
                        .find(|pair| pair[0] == "-passlogfile")
                        .map(|pair| pair[1].to_owned())
                        .expect("first pass should carry a passlog prefix");
                    for suffix in ["-0.log", "-0.log.mbtree"] {
                        let mut path = prefix.clone();
                        path.push(suffix);
                        tokio::fs::write(path, b"synthetic two-pass stats")
                            .await
                            .expect("synthetic passlog should be writable");
                    }
                    return Ok(successful_command_output());
                }
                let size = self
                    .sizes
                    .lock()
                    .expect("runner size mutex should not be poisoned")
                    .pop_front()
                    .expect("video test runner needs an output size for every ffmpeg call");
                let output = command.args().last().expect("ffmpeg output path should be present");
                tokio::fs::write(output, vec![b'x'; size])
                    .await
                    .expect("synthetic ffmpeg output should be writable");
                let dimensions = command
                    .args()
                    .windows(2)
                    .find(|pair| pair[0] == "-vf")
                    .and_then(|pair| parse_scale_dimensions(&pair[1].to_string_lossy()))
                    .unwrap_or(self.source_dimensions);
                *self
                    .last_dimensions
                    .lock()
                    .expect("runner dimensions mutex should not be poisoned") = dimensions;
                return Ok(successful_command_output());
            }

            let dimensions = *self
                .last_dimensions
                .lock()
                .expect("runner dimensions mutex should not be poisoned");
            Ok(successful_probe_output(dimensions))
        }
    }

    fn parse_scale_dimensions(filter: &str) -> Option<(u32, u32)> {
        let mut quoted = filter.split('\'');
        quoted.next()?;
        let width = quoted.next()?.parse().ok()?;
        quoted.next()?;
        let height = quoted.next()?.parse().ok()?;
        Some((width, height))
    }

    fn successful_command_output() -> ExternalCommandOutput {
        ExternalCommandOutput {
            success: true,
            exit_code: Some(0),
            stdout: b"frame=1\nout_time_ms=1000\nprogress=end\n".to_vec(),
            stderr: Vec::new(),
            stdout_truncated: false,
            stderr_truncated: false,
        }
    }

    fn successful_probe_output(dimensions: (u32, u32)) -> ExternalCommandOutput {
        let (width, height) = dimensions;
        ExternalCommandOutput {
            success: true,
            exit_code: Some(0),
            stdout: format!(
                r#"{{"streams":[{{"index":0,"codec_type":"video","codec_name":"h264","pix_fmt":"yuv420p","width":{width},"height":{height},"avg_frame_rate":"30/1"}}],"format":{{"format_name":"mp4","duration":"1.0","size":"1"}}}}"#
            )
            .into_bytes(),
            stderr: Vec::new(),
            stdout_truncated: false,
            stderr_truncated: false,
        }
    }

    fn candidate_result(path: impl AsRef<Path>, bytes: u64) -> sooqa_media::NormalizationResult {
        sooqa_media::NormalizationResult {
            output_path: path.as_ref().to_owned(),
            progress: sooqa_media::FfmpegProgress {
                frame: None,
                out_time_ms: None,
                state: sooqa_media::FfmpegProgressState::End,
            },
            probe: adaptation_probe(320, 240, bytes),
            digest: sooqa_media::FileDigest { bytes, sha256: format!("{bytes:064x}") },
        }
    }

    fn adaptation_probe(width: u32, height: u32, size_bytes: u64) -> MediaProbe {
        MediaProbe {
            container_format: Some("mp4".to_owned()),
            duration_ms: Some(1_000),
            size_bytes,
            bit_rate: Some(100_000),
            streams: vec![MediaStream {
                index: 0,
                kind: MediaStreamKind::Video,
                codec: Some("h264".to_owned()),
                attached_picture: false,
                codec_tag: Some("avc1".to_owned()),
                codec_mime: Some("avc1.640028".to_owned()),
                level: Some(40),
                profile: Some("High".to_owned()),
                pixel_format: Some("yuv420p".to_owned()),
                width: Some(width),
                height: Some(height),
                display_aspect_ratio: None,
                frame_rate: Some(FrameRate { numerator: 30, denominator: 1 }),
                rotation_degrees: Some(0),
                sample_rate_hz: None,
                channels: None,
                bit_rate: Some(100_000),
            }],
        }
    }

    fn adaptation_executor(runner: Arc<VideoAdaptationRunner>) -> FfmpegExecutor {
        let ffprobe = FfprobeAdapter::with_runner(
            "ffprobe",
            Duration::from_secs(10),
            sooqa_media::DEFAULT_MAX_OUTPUT_BYTES,
            runner.clone(),
        );
        FfmpegExecutor::with_runner(
            runner,
            ffprobe,
            Duration::from_secs(10),
            sooqa_media::DEFAULT_MAX_OUTPUT_BYTES,
        )
    }

    async fn assert_no_video_attempt_files(root: &Path) {
        let mut entries = tokio::fs::read_dir(root).await.expect("test root should be readable");
        while let Some(entry) = entries.next_entry().await.expect("directory should be readable") {
            let name = entry.file_name().to_string_lossy().into_owned();
            assert!(
                !name.starts_with(".sooqa-inline-candidate-")
                    && !name.starts_with(".sooqa-normalize-")
                    && !name.starts_with(".sooqa-two-pass-"),
                "video attempt file was left behind: {name}"
            );
        }
    }

    async fn candidate_fixture(
        candidates: &mut VideoCandidateSet,
        root: &Path,
        name: &str,
        bytes: u64,
    ) -> sooqa_media::NormalizationResult {
        let path = root.join(name);
        tokio::fs::write(&path, b"candidate").await.expect("candidate should be writable");
        candidates.cleanup.push(path.clone());
        candidate_result(path, bytes)
    }

    #[tokio::test]
    async fn two_pass_candidate_above_target_can_replace_larger_quality_fallback() {
        let root = std::env::temp_dir().join(format!("sooqa-candidate-set-{}", Uuid::new_v4()));
        tokio::fs::create_dir(&root).await.expect("test root should be created");
        let mut candidates = VideoCandidateSet::new(100, 1_000);
        let quality = candidate_fixture(&mut candidates, &root, "quality.mp4", 120).await;
        let quality_path = quality.output_path.clone();
        let two_pass = candidate_fixture(&mut candidates, &root, "two-pass.mp4", 110).await;
        let two_pass_path = two_pass.output_path.clone();

        assert!(candidates.consider(quality, FallbackPolicy::KeepFirst).await.is_none());
        let selected = candidates
            .consider(two_pass, FallbackPolicy::KeepSmallest)
            .await
            .expect("a smaller two-pass candidate should be accepted");
        assert_eq!(selected.digest.bytes, 110);
        assert!(!quality_path.exists(), "the replaced fallback should be removed");
        assert!(two_pass_path.exists(), "the selected fallback should remain owned");
        drop(candidates);
        clean_test_root(&root).await;
    }

    #[tokio::test]
    async fn equal_or_larger_quality_candidate_keeps_the_remux_fallback() {
        for quality_bytes in [110, 111] {
            let root = std::env::temp_dir().join(format!("sooqa-candidate-set-{}", Uuid::new_v4()));
            tokio::fs::create_dir(&root).await.expect("test root should be created");
            let mut candidates = VideoCandidateSet::new(100, 1_000);
            let remux = candidate_fixture(&mut candidates, &root, "remux.mp4", 110).await;
            let remux_path = remux.output_path.clone();
            let quality =
                candidate_fixture(&mut candidates, &root, "quality.mp4", quality_bytes).await;
            let quality_path = quality.output_path.clone();

            assert!(candidates.consider(remux, FallbackPolicy::KeepFirst).await.is_none());
            assert!(candidates.consider(quality, FallbackPolicy::KeepSmallest).await.is_none());
            assert_eq!(
                candidates
                    .fallback
                    .as_ref()
                    .expect("the remux should remain as the quality fallback")
                    .output_path,
                remux_path
            );
            assert!(remux_path.exists(), "the retained remux should remain owned");
            assert!(!quality_path.exists(), "an equal or larger candidate should be removed");
            drop(candidates);
            clean_test_root(&root).await;
        }
    }

    async fn clean_test_root(root: &Path) {
        tokio::fs::remove_dir_all(root).await.expect("test root should be removable");
    }

    #[tokio::test]
    async fn remux_actual_bytes_can_fit_even_when_source_probe_is_over_target() {
        let root = std::env::temp_dir().join(format!("sooqa-video-adaptation-{}", Uuid::new_v4()));
        tokio::fs::create_dir(&root).await.expect("test root should be created");
        let output = root.join("canonical.mp4");
        let probe = adaptation_probe(320, 240, 20);
        let planner = NormalizationPlanner::new(
            "ffmpeg",
            CanonicalVideoProfile { target_max_bytes: 10, ..Default::default() },
        )
        .expect("test profile should be valid");
        let initial_plan = planner.plan("input.mp4", &output, &probe).expect("plan should build");
        assert_eq!(initial_plan.mode(), NormalizationMode::Remux);
        let runner = Arc::new(VideoAdaptationRunner::new([8], (320, 240)));
        let executor = adaptation_executor(runner.clone());

        let result = execute_video_normalization(
            &planner,
            &executor,
            Path::new("input.mp4"),
            &output,
            &probe,
            100,
            initial_plan,
        )
        .await
        .expect("fitting remux should be selected");
        assert_eq!(result.digest.bytes, 8);
        assert_eq!(runner.ffmpeg_commands().len(), 1);
        assert_no_video_attempt_files(&root).await;
        clean_test_root(&root).await;
    }

    #[tokio::test]
    async fn content_aware_quality_candidate_wins_and_losing_candidates_are_removed() {
        let root = std::env::temp_dir().join(format!("sooqa-video-adaptation-{}", Uuid::new_v4()));
        tokio::fs::create_dir(&root).await.expect("test root should be created");
        let output = root.join("canonical.mp4");
        let probe = adaptation_probe(320, 240, 20);
        let planner = NormalizationPlanner::new(
            "ffmpeg",
            CanonicalVideoProfile { target_max_bytes: 10, ..Default::default() },
        )
        .expect("test profile should be valid");
        let initial_plan = planner.plan("input.mp4", &output, &probe).expect("plan should build");
        let runner = Arc::new(VideoAdaptationRunner::new([20, 9], (320, 240)));
        let executor = adaptation_executor(runner.clone());

        let result = execute_video_normalization(
            &planner,
            &executor,
            Path::new("input.mp4"),
            &output,
            &probe,
            100,
            initial_plan,
        )
        .await
        .expect("content-aware quality transcode should be selected");
        assert_eq!(result.digest.bytes, 9);
        assert_eq!(tokio::fs::metadata(&output).await.unwrap().len(), 9);
        assert_no_video_attempt_files(&root).await;
        clean_test_root(&root).await;
    }

    #[tokio::test]
    async fn oversized_quality_candidate_uses_one_two_pass_encode_and_cleans_stats() {
        let root = std::env::temp_dir().join(format!("sooqa-video-adaptation-{}", Uuid::new_v4()));
        tokio::fs::create_dir(&root).await.expect("test root should be created");
        let output = root.join("canonical.mp4");
        let probe = adaptation_probe(320, 240, 30_000);
        let planner = NormalizationPlanner::new(
            "ffmpeg",
            CanonicalVideoProfile { target_max_bytes: 20_000, ..Default::default() },
        )
        .expect("test profile should be valid");
        let initial_plan = planner.plan("input.mp4", &output, &probe).expect("plan should build");
        let runner = Arc::new(VideoAdaptationRunner::new([30_000, 25_000, 19_000], (320, 240)));
        let executor = adaptation_executor(runner.clone());

        let result = execute_video_normalization(
            &planner,
            &executor,
            Path::new("input.mp4"),
            &output,
            &probe,
            100_000,
            initial_plan,
        )
        .await
        .expect("two-pass target candidate should be selected");

        assert_eq!(result.digest.bytes, 19_000);
        let commands = runner.ffmpeg_commands();
        assert_eq!(commands.len(), 4, "remux, quality, and two target-bitrate passes");
        assert!(commands[2].args().windows(2).any(|pair| pair == ["-pass", "1"]));
        assert!(commands[3].args().windows(2).any(|pair| pair == ["-pass", "2"]));
        assert_no_video_attempt_files(&root).await;
        clean_test_root(&root).await;
    }

    #[tokio::test]
    async fn oversized_two_pass_result_cannot_displace_a_smaller_quality_fallback() {
        let root = std::env::temp_dir().join(format!("sooqa-video-adaptation-{}", Uuid::new_v4()));
        tokio::fs::create_dir(&root).await.expect("test root should be created");
        let output = root.join("canonical.mp4");
        let mut probe = adaptation_probe(320, 240, 30_000);
        probe.container_format = Some("matroska,webm".to_owned());
        probe.streams[0].codec = Some("vp9".to_owned());
        let planner = NormalizationPlanner::new(
            "ffmpeg",
            CanonicalVideoProfile { target_max_bytes: 20_000, ..Default::default() },
        )
        .expect("test profile should be valid");
        let initial_plan = planner.plan("input.webm", &output, &probe).expect("plan should build");
        assert_eq!(initial_plan.mode(), NormalizationMode::Transcode);
        let runner = Arc::new(VideoAdaptationRunner::new([25_000, 26_000], (320, 240)));
        let executor = adaptation_executor(runner);

        let result = execute_video_normalization(
            &planner,
            &executor,
            Path::new("input.webm"),
            &output,
            &probe,
            100_000,
            initial_plan,
        )
        .await
        .expect("smaller quality candidate should remain selected");

        assert_eq!(result.digest.bytes, 25_000);
        assert_no_video_attempt_files(&root).await;
        clean_test_root(&root).await;
    }

    #[tokio::test]
    async fn oversized_remux_cannot_displace_storable_quality_candidate() {
        let root = std::env::temp_dir().join(format!("sooqa-video-adaptation-{}", Uuid::new_v4()));
        tokio::fs::create_dir(&root).await.expect("test root should be created");
        let output = root.join("canonical.mp4");
        let probe = adaptation_probe(320, 240, 20);
        let planner = NormalizationPlanner::new(
            "ffmpeg",
            CanonicalVideoProfile { target_max_bytes: 10, ..Default::default() },
        )
        .expect("test profile should be valid");
        let initial_plan = planner.plan("input.mp4", &output, &probe).expect("plan should build");
        let runner = Arc::new(VideoAdaptationRunner::new([2_500, 100], (320, 240)));
        let executor = adaptation_executor(runner.clone());

        let result = execute_video_normalization(
            &planner,
            &executor,
            Path::new("input.mp4"),
            &output,
            &probe,
            200,
            initial_plan,
        )
        .await
        .expect("storable CRF fallback should be selected");
        assert_eq!(result.digest.bytes, 100);
        assert_eq!(runner.ffmpeg_commands().len(), 2);
        assert_no_video_attempt_files(&root).await;
        clean_test_root(&root).await;
    }

    #[tokio::test]
    async fn normalization_errors_when_every_video_candidate_exceeds_storage_ceiling() {
        let root = std::env::temp_dir().join(format!("sooqa-video-adaptation-{}", Uuid::new_v4()));
        tokio::fs::create_dir(&root).await.expect("test root should be created");
        let output = root.join("canonical.mp4");
        let probe = adaptation_probe(320, 240, 20);
        let planner = NormalizationPlanner::new(
            "ffmpeg",
            CanonicalVideoProfile { target_max_bytes: 1, ..Default::default() },
        )
        .expect("test profile should be valid");
        let initial_plan = planner.plan("input.mp4", &output, &probe).expect("plan should build");
        let runner = Arc::new(VideoAdaptationRunner::new([20, 20], (320, 240)));
        let executor = adaptation_executor(runner);

        let error = execute_video_normalization(
            &planner,
            &executor,
            Path::new("input.mp4"),
            &output,
            &probe,
            19,
            initial_plan,
        )
        .await
        .expect_err("an unstorable canonical candidate should fail");
        assert!(matches!(
            error,
            NormalizationExecutionError::OutputExceedsStorageLimit { bytes: 20, limit: 19 }
        ));
        assert_no_video_attempt_files(&root).await;
        clean_test_root(&root).await;
    }

    #[tokio::test]
    async fn no_two_pass_fit_keeps_the_no_loss_remux_fallback() {
        let root = std::env::temp_dir().join(format!("sooqa-video-adaptation-{}", Uuid::new_v4()));
        tokio::fs::create_dir(&root).await.expect("test root should be created");
        let output = root.join("canonical.mp4");
        let probe = adaptation_probe(1920, 1080, 20);
        let planner = NormalizationPlanner::new(
            "ffmpeg",
            CanonicalVideoProfile { target_max_bytes: 1, ..Default::default() },
        )
        .expect("test profile should be valid");
        let initial_plan = planner.plan("input.mp4", &output, &probe).expect("plan should build");
        let runner = Arc::new(VideoAdaptationRunner::new([20; 5], (1920, 1080)));
        let executor = adaptation_executor(runner.clone());

        let result = execute_video_normalization(
            &planner,
            &executor,
            Path::new("input.mp4"),
            &output,
            &probe,
            20,
            initial_plan,
        )
        .await
        .expect("quality-floor fallback should be selected");
        assert_eq!(result.digest.bytes, 20);
        let commands = runner.ffmpeg_commands();
        assert_eq!(commands.len(), 5, "one remux plus one CRF encode per resolution rung");
        for command in commands.iter().skip(1) {
            assert!(command.args().windows(2).any(|pair| pair == ["-crf", "27"]));
        }
        assert!(
            !commands
                .iter()
                .any(|command| { command.args().windows(2).any(|pair| pair == ["-pass", "1"]) })
        );
        assert_no_video_attempt_files(&root).await;
        clean_test_root(&root).await;
    }

    #[tokio::test]
    async fn conservative_bitrate_rejection_still_allows_a_simple_crf_rung_to_fit() {
        let root = std::env::temp_dir().join(format!("sooqa-video-adaptation-{}", Uuid::new_v4()));
        tokio::fs::create_dir(&root).await.expect("test root should be created");
        let output = root.join("canonical.mp4");
        let probe = adaptation_probe(1920, 1080, 20);
        let planner = NormalizationPlanner::new(
            "ffmpeg",
            CanonicalVideoProfile { target_max_bytes: 10, ..Default::default() },
        )
        .expect("test profile should be valid");
        assert_eq!(planner.two_pass_candidate(&probe), None);
        let initial_plan = planner.plan("input.mp4", &output, &probe).expect("plan should build");
        let runner = Arc::new(VideoAdaptationRunner::new([20, 20, 15, 9], (1920, 1080)));
        let executor = adaptation_executor(runner.clone());

        let result = execute_video_normalization(
            &planner,
            &executor,
            Path::new("input.mp4"),
            &output,
            &probe,
            100,
            initial_plan,
        )
        .await
        .expect("content-aware lower rung should satisfy the preferred target");

        assert_eq!(result.digest.bytes, 9);
        let commands = runner.ffmpeg_commands();
        assert_eq!(commands.len(), 4, "remux, 1080p, 810p, and 608p CRF attempts");
        let filter = commands[3]
            .args()
            .windows(2)
            .find(|pair| pair[0] == "-vf")
            .and_then(|pair| pair[1].to_str())
            .expect("selected CRF rung should carry a scale filter");
        assert_eq!(parse_scale_dimensions(filter), Some((1080, 608)));
        assert_no_video_attempt_files(&root).await;
        clean_test_root(&root).await;
    }

    #[tokio::test]
    async fn two_pass_error_cleans_fallback_candidate_and_stats_directory() {
        let root = std::env::temp_dir().join(format!("sooqa-video-adaptation-{}", Uuid::new_v4()));
        tokio::fs::create_dir(&root).await.expect("test root should be created");
        let output = root.join("canonical.mp4");
        let probe = adaptation_probe(320, 240, 30_000);
        let planner = NormalizationPlanner::new(
            "ffmpeg",
            CanonicalVideoProfile { target_max_bytes: 20_000, ..Default::default() },
        )
        .expect("test profile should be valid");
        let initial_plan = planner.plan("input.mp4", &output, &probe).expect("plan should build");
        let runner =
            Arc::new(VideoAdaptationRunner::new([30_000, 25_000], (320, 240)).failing_at(3));
        let executor = adaptation_executor(runner);

        let error = execute_video_normalization(
            &planner,
            &executor,
            Path::new("input.mp4"),
            &output,
            &probe,
            100_000,
            initial_plan,
        )
        .await
        .expect_err("synthetic transcode failure should be returned");
        assert!(matches!(error, NormalizationExecutionError::ProcessFailed { .. }));
        assert_no_video_attempt_files(&root).await;
        clean_test_root(&root).await;
    }

    #[tokio::test]
    async fn two_pass_cancellation_cleans_attempt_files_and_stats_directory() {
        let root = std::env::temp_dir().join(format!("sooqa-video-adaptation-{}", Uuid::new_v4()));
        tokio::fs::create_dir(&root).await.expect("test root should be created");
        let output = root.join("canonical.mp4");
        let probe = adaptation_probe(320, 240, 30_000);
        let planner = NormalizationPlanner::new(
            "ffmpeg",
            CanonicalVideoProfile { target_max_bytes: 20_000, ..Default::default() },
        )
        .expect("test profile should be valid");
        let initial_plan = planner.plan("input.mp4", &output, &probe).expect("plan should build");
        let blocked = Arc::new(Notify::new());
        let runner = Arc::new(
            VideoAdaptationRunner::new([30_000, 25_000], (320, 240))
                .blocking_at(3, blocked.clone()),
        );
        let executor = adaptation_executor(runner);
        let task = tokio::spawn(async move {
            execute_video_normalization(
                &planner,
                &executor,
                Path::new("input.mp4"),
                &output,
                &probe,
                100_000,
                initial_plan,
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(1), blocked.notified())
            .await
            .expect("adaptation should reach the blocking runner");
        task.abort();
        let _ = task.await;
        assert_no_video_attempt_files(&root).await;
        clean_test_root(&root).await;
    }

    #[tokio::test]
    async fn existing_canonical_conflict_is_not_overwritten() {
        let root = std::env::temp_dir().join(format!("sooqa-video-adaptation-{}", Uuid::new_v4()));
        tokio::fs::create_dir(&root).await.expect("test root should be created");
        let output = root.join("canonical.mp4");
        tokio::fs::write(&output, b"newer canonical").await.expect("canonical should be written");
        let probe = adaptation_probe(320, 240, 20);
        let planner = NormalizationPlanner::new(
            "ffmpeg",
            CanonicalVideoProfile { target_max_bytes: 10, ..Default::default() },
        )
        .expect("test profile should be valid");
        let initial_plan = planner.plan("input.mp4", &output, &probe).expect("plan should build");
        let runner = Arc::new(VideoAdaptationRunner::new([8], (320, 240)));
        let executor = adaptation_executor(runner);

        let error = execute_video_normalization(
            &planner,
            &executor,
            Path::new("input.mp4"),
            &output,
            &probe,
            100,
            initial_plan,
        )
        .await
        .expect_err("different canonical content must remain a conflict");
        assert!(matches!(error, NormalizationExecutionError::OutputPublish { .. }));
        assert_eq!(tokio::fs::read(&output).await.unwrap(), b"newer canonical");
        assert_no_video_attempt_files(&root).await;
        clean_test_root(&root).await;
    }
}
