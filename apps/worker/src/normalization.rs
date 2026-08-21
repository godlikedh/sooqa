//! Canonical media normalization jobs.

use crate::common::*;

#[derive(Debug, Clone, Copy)]
struct NormalizationLimits {
    max_normalized_storage_bytes: u64,
    admission: WorkspaceAdmission,
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
    let mut preflight_failure = None;
    if normalization_stage_may_run(current_request.status) {
        let input_data = match current_request.input_data() {
            Ok(input_data) => Some(input_data),
            Err(error) => {
                preflight_failure =
                    Some(HandlerFailure::permanent("invalid_ingest_state", error.to_string()));
                None
            }
        };
        if let Some(input_data) = input_data
            && !(input_data.normalization.is_some() && !current_request.force_save)
        {
            let probe = match input_data.probe {
                Some(probe) => match probe.decode::<MediaProbe>() {
                    Ok(probe) => Some(probe),
                    Err(error) => {
                        preflight_failure = Some(HandlerFailure::permanent(
                            "invalid_ingest_state",
                            format!("stored media probe could not be decoded: {error}"),
                        ));
                        None
                    }
                },
                None => {
                    preflight_failure = Some(HandlerFailure::permanent(
                        "invalid_ingest_state",
                        "ingest request has no stored media probe",
                    ));
                    None
                }
            };
            if let Some(probe) = probe {
                let media_kind =
                    probe_media_kind(&probe).or_else(|| request_media_kind(&current_request));
                match media_kind {
                    Some(SourceMediaKind::Video) => admission
                        .admit(work_root, max_normalized_storage_bytes.saturating_mul(2))?,
                    Some(_) => admission.admit(work_root, max_normalized_storage_bytes)?,
                    None => {
                        preflight_failure = Some(HandlerFailure::permanent(
                            "invalid_ingest_state",
                            "ingest request has no stored source media kind",
                        ));
                    }
                }
            }
        }
    }

    let request = match inbox.begin_asset_normalization(ingest_request_id, &job_attempt).await {
        Ok(AssetNormalizationStart::Ready(request)) => request,
        Ok(AssetNormalizationStart::AlreadyAdvanced(_)) => return Ok(()),
        Err(error) => return Err(map_inbox_error(error)),
    };
    if let Some(failure) = preflight_failure {
        return fail_normalization(inbox, ingest_request_id, &job_attempt, failure).await;
    }
    let input_data = match request.input_data() {
        Ok(input_data) => input_data,
        Err(error) => {
            return fail_normalization(
                inbox,
                ingest_request_id,
                &job_attempt,
                HandlerFailure::permanent("invalid_ingest_state", error.to_string()),
            )
            .await;
        }
    };
    let probe = match input_data.probe {
        Some(probe) => match probe.decode::<MediaProbe>() {
            Ok(probe) => probe,
            Err(error) => {
                return fail_normalization(
                    inbox,
                    ingest_request_id,
                    &job_attempt,
                    HandlerFailure::permanent(
                        "invalid_ingest_state",
                        format!("stored media probe could not be decoded: {error}"),
                    ),
                )
                .await;
            }
        },
        None => {
            return fail_normalization(
                inbox,
                ingest_request_id,
                &job_attempt,
                HandlerFailure::permanent(
                    "invalid_ingest_state",
                    "ingest request has no stored media probe",
                ),
            )
            .await;
        }
    };
    let media_kind = match probe_media_kind(&probe).or_else(|| request_media_kind(&request)) {
        Some(media_kind) => media_kind,
        None => {
            return fail_normalization(
                inbox,
                ingest_request_id,
                &job_attempt,
                HandlerFailure::permanent(
                    "invalid_ingest_state",
                    "ingest request has no stored source media kind",
                ),
            )
            .await;
        }
    };
    if media_kind == SourceMediaKind::Image {
        return normalize_image_asset(
            inbox,
            work_root,
            image_normalizer,
            &request,
            ingest_request_id,
            &job_attempt,
            max_normalized_storage_bytes,
        )
        .await;
    }
    if matches!(media_kind, SourceMediaKind::Animation | SourceMediaKind::Audio) {
        return normalize_exact_asset(
            inbox,
            work_root,
            &request,
            ingest_request_id,
            &job_attempt,
            ExactNormalizationSpec { media_kind, probe: &probe, max_normalized_storage_bytes },
        )
        .await;
    }
    if media_kind != SourceMediaKind::Video {
        return fail_normalization(
            inbox,
            ingest_request_id,
            &job_attempt,
            HandlerFailure::permanent(
                "unsupported_media_kind",
                format!("asset media kind {media_kind:?} is not supported by the video normalizer"),
            ),
        )
        .await;
    }
    let (workspace_id, input_name) = match workspace_input(&request) {
        Ok(value) => value,
        Err(failure) => {
            return fail_normalization(inbox, ingest_request_id, &job_attempt, failure).await;
        }
    };
    let workspace = match MediaWorkspace::create(work_root, workspace_id).await {
        Ok(workspace) => workspace,
        Err(error) => {
            return fail_normalization(
                inbox,
                ingest_request_id,
                &job_attempt,
                map_workspace_error(error),
            )
            .await;
        }
    };
    if let Err(error) = workspace.validate() {
        return fail_normalization(
            inbox,
            ingest_request_id,
            &job_attempt,
            map_workspace_error(error),
        )
        .await;
    }
    let input_path = match workspace.path(WorkspaceArea::Source, input_name) {
        Ok(path) => path,
        Err(error) => {
            return fail_normalization(
                inbox,
                ingest_request_id,
                &job_attempt,
                map_workspace_error(error),
            )
            .await;
        }
    };
    let output_path = match workspace.path(WorkspaceArea::Normalized, "canonical.mp4") {
        Ok(path) => path,
        Err(error) => {
            return fail_normalization(
                inbox,
                ingest_request_id,
                &job_attempt,
                map_workspace_error(error),
            )
            .await;
        }
    };
    let plan = match planner.plan(&input_path, &output_path, &probe) {
        Ok(plan) => plan,
        Err(error) => {
            return fail_normalization(
                inbox,
                ingest_request_id,
                &job_attempt,
                HandlerFailure::permanent("normalize_plan", error.to_string()),
            )
            .await;
        }
    };
    let result = match execute_video_normalization(
        planner,
        executor,
        &input_path,
        &output_path,
        &probe,
        max_normalized_storage_bytes,
        plan,
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            let retryable = normalization_error_is_retryable(&error);
            let terminal = !retryable || job.attempt_count >= job.max_attempts;
            let failure = if terminal {
                HandlerFailure::permanent("normalize", error.to_string())
            } else {
                HandlerFailure::retryable("normalize_timeout", error.to_string())
            };
            return fail_normalization(inbox, ingest_request_id, &job_attempt, failure).await;
        }
    };
    let normalization = normalization_metadata(result);
    inbox
        .complete_asset_normalization(ingest_request_id, &job_attempt, normalization)
        .await
        .map_err(map_inbox_error)?;
    Ok(())
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

async fn execute_video_normalization(
    planner: &NormalizationPlanner,
    executor: &FfmpegExecutor,
    input_path: &Path,
    output_path: &Path,
    probe: &MediaProbe,
    max_normalized_storage_bytes: u64,
    initial_plan: sooqa_media::NormalizationPlan,
) -> Result<sooqa_media::NormalizationResult, NormalizationExecutionError> {
    let mut candidate_paths = VideoCandidateCleanup { paths: Vec::new() };
    let mut fallback = None;
    let mut largest_oversized_candidate = None;
    let mut attempts = 0;

    if initial_plan.mode() == sooqa_media::NormalizationMode::Remux {
        // Never run a decision-making remux directly against canonical.mp4.
        // Its actual bytes may cross the target, and a lease-expired worker
        // must not delete or replace a newer canonical artifact.
        let candidate_path = video_candidate_path(output_path);
        candidate_paths.push(candidate_path.clone());
        attempts += 1;
        let remux_plan = initial_plan.with_output(&candidate_path);
        let result = executor.execute(&remux_plan, std::future::pending()).await?;
        if result.digest.bytes <= planner.profile().target_max_bytes
            && result.digest.bytes <= max_normalized_storage_bytes
        {
            return publish_video_candidate(
                result,
                output_path,
                &mut candidate_paths,
                attempts,
                planner.profile().target_max_bytes,
            )
            .await;
        }
        // Remux is the highest-quality candidate even when incidental
        // container overhead keeps it above the eligibility target. Retain it
        // as a fallback only when it can still be stored under the hard
        // ceiling; an oversized remux must not displace a later CRF result.
        if result.digest.bytes <= max_normalized_storage_bytes {
            fallback = Some(result);
        } else {
            largest_oversized_candidate =
                Some(largest_oversized_candidate.unwrap_or(0).max(result.digest.bytes));
            remove_video_candidate(&candidate_path, &mut candidate_paths).await;
        }
    }

    let video = probe
        .streams
        .iter()
        .find(|stream| stream.kind == MediaStreamKind::Video)
        .ok_or(NormalizationExecutionError::OutputHasNoVideo)?;
    let ladder = planner.resolution_ladder(video);
    if ladder.is_empty() {
        return Err(NormalizationExecutionError::InvalidOutputProfile {
            message: "video dimensions are missing or invalid",
        });
    }

    for dimensions in ladder {
        for crf in planner.profile().preferred_crf..=planner.profile().maximum_crf {
            attempts += 1;
            let candidate_path = video_candidate_path(output_path);
            candidate_paths.push(candidate_path.clone());
            let plan =
                match planner.plan_candidate(input_path, &candidate_path, probe, dimensions, crf) {
                    Ok(plan) => plan,
                    Err(error) => {
                        return Err(NormalizationExecutionError::InvalidOutputProfile {
                            message: match error {
                                sooqa_media::NormalizationError::InvalidCandidateDimensions {
                                    ..
                                } => "candidate dimensions are outside the canonical profile",
                                _ => "candidate normalization plan is invalid",
                            },
                        });
                    }
                };
            let result = executor.execute(&plan, std::future::pending()).await?;
            validate_adapted_dimensions(&result.probe, dimensions, video, planner)?;
            let fits_target = result.digest.bytes <= planner.profile().target_max_bytes
                && result.digest.bytes <= max_normalized_storage_bytes;
            let fits_storage = result.digest.bytes <= max_normalized_storage_bytes;
            if fits_target {
                if let Some(previous) = fallback.take() {
                    remove_video_candidate(&previous.output_path, &mut candidate_paths).await;
                }
                return publish_video_candidate(
                    result,
                    output_path,
                    &mut candidate_paths,
                    attempts,
                    planner.profile().target_max_bytes,
                )
                .await;
            }
            if fallback.is_none() && fits_storage {
                fallback = Some(result);
            } else {
                if !fits_storage {
                    largest_oversized_candidate =
                        Some(largest_oversized_candidate.unwrap_or(0).max(result.digest.bytes));
                }
                // Losing candidates are not useful for selection or retry and
                // can be full-sized videos. Keep disk use to one fallback plus
                // the candidate currently under inspection.
                remove_video_candidate(&candidate_path, &mut candidate_paths).await;
            }
        }
    }
    let selected = fallback.ok_or(NormalizationExecutionError::OutputExceedsStorageLimit {
        bytes: largest_oversized_candidate.unwrap_or(max_normalized_storage_bytes),
        limit: max_normalized_storage_bytes,
    })?;
    publish_video_candidate(
        selected,
        output_path,
        &mut candidate_paths,
        attempts,
        planner.profile().target_max_bytes,
    )
    .await
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
    inbox: &InboxRepository,
    work_root: &std::path::Path,
    image_normalizer: ImageNormalizer,
    request: &sooqa_inbox::Ingest,
    ingest_request_id: Uuid,
    job_attempt: &sooqa_jobs::JobLease,
    max_normalized_storage_bytes: u64,
) -> Result<(), HandlerFailure> {
    let (workspace_id, input_name) = match workspace_input(request) {
        Ok(value) => value,
        Err(failure) => {
            return fail_normalization(inbox, ingest_request_id, job_attempt, failure).await;
        }
    };
    let workspace = match MediaWorkspace::create(work_root, workspace_id).await {
        Ok(workspace) => workspace,
        Err(error) => {
            return fail_normalization(
                inbox,
                ingest_request_id,
                job_attempt,
                map_workspace_error(error),
            )
            .await;
        }
    };
    if let Err(error) = workspace.validate() {
        return fail_normalization(
            inbox,
            ingest_request_id,
            job_attempt,
            map_workspace_error(error),
        )
        .await;
    }
    let plan = match image_normalizer.plan(&workspace, input_name, "canonical", "thumbnail") {
        Ok(plan) => plan,
        Err(error) => {
            return fail_normalization(
                inbox,
                ingest_request_id,
                job_attempt,
                HandlerFailure::permanent("normalize_plan", error.to_string()),
            )
            .await;
        }
    };
    let result = match image_normalizer.execute(&plan).await {
        Ok(result) => result,
        Err(error) => {
            return fail_normalization(
                inbox,
                ingest_request_id,
                job_attempt,
                HandlerFailure::permanent("normalize_image", error.to_string()),
            )
            .await;
        }
    };
    if let Some(failure) = normalized_storage_limit_failure(
        result.canonical_digest.bytes,
        max_normalized_storage_bytes,
    ) {
        return fail_normalization(inbox, ingest_request_id, job_attempt, failure).await;
    }
    inbox
        .complete_asset_normalization(
            ingest_request_id,
            job_attempt,
            image_normalization_metadata(result),
        )
        .await
        .map_err(map_inbox_error)?;
    Ok(())
}

struct ExactNormalizationSpec<'a> {
    media_kind: SourceMediaKind,
    probe: &'a MediaProbe,
    max_normalized_storage_bytes: u64,
}

async fn normalize_exact_asset(
    inbox: &InboxRepository,
    work_root: &Path,
    request: &sooqa_inbox::Ingest,
    ingest_request_id: Uuid,
    job_attempt: &sooqa_jobs::JobLease,
    spec: ExactNormalizationSpec<'_>,
) -> Result<(), HandlerFailure> {
    let ExactNormalizationSpec { media_kind, probe, max_normalized_storage_bytes } = spec;
    let (workspace_id, input_name) = match workspace_input(request) {
        Ok(value) => value,
        Err(failure) => {
            return fail_normalization(inbox, ingest_request_id, job_attempt, failure).await;
        }
    };
    let workspace = match MediaWorkspace::create(work_root, workspace_id).await {
        Ok(workspace) => workspace,
        Err(error) => {
            return fail_normalization(
                inbox,
                ingest_request_id,
                job_attempt,
                map_workspace_error(error),
            )
            .await;
        }
    };
    if let Err(error) = workspace.validate() {
        return fail_normalization(
            inbox,
            ingest_request_id,
            job_attempt,
            map_workspace_error(error),
        )
        .await;
    }
    let input_path = match workspace.path(WorkspaceArea::Source, input_name) {
        Ok(path) => path,
        Err(error) => {
            return fail_normalization(
                inbox,
                ingest_request_id,
                job_attempt,
                map_workspace_error(error),
            )
            .await;
        }
    };
    let canonical_name = match media_kind {
        SourceMediaKind::Animation => "canonical.animation",
        SourceMediaKind::Audio => "canonical.audio",
        SourceMediaKind::Video | SourceMediaKind::Image | SourceMediaKind::Unknown => {
            "canonical.media"
        }
    };
    let canonical_path = match workspace.path(WorkspaceArea::Normalized, canonical_name) {
        Ok(path) => path,
        Err(error) => {
            return fail_normalization(
                inbox,
                ingest_request_id,
                job_attempt,
                map_workspace_error(error),
            )
            .await;
        }
    };
    match source_artifact_exists(&canonical_path).await {
        Ok(true) => {}
        Ok(false) => match publish_artifact(&input_path, &canonical_path).await {
            Ok(()) | Err(ArtifactPublicationError::DestinationConflict) => {}
            Err(error) => {
                return fail_normalization(
                    inbox,
                    ingest_request_id,
                    job_attempt,
                    HandlerFailure::permanent("normalize_exact", error.to_string()),
                )
                .await;
            }
        },
        Err(failure) => {
            return fail_normalization(inbox, ingest_request_id, job_attempt, failure).await;
        }
    }
    let digest = match sha256_file(&canonical_path).await {
        Ok(digest) => digest,
        Err(error) => {
            return fail_normalization(
                inbox,
                ingest_request_id,
                job_attempt,
                HandlerFailure::permanent("normalize_exact", error.to_string()),
            )
            .await;
        }
    };
    if let Some(failure) =
        normalized_storage_limit_failure(digest.bytes, max_normalized_storage_bytes)
    {
        return fail_normalization(inbox, ingest_request_id, job_attempt, failure).await;
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
    inbox
        .complete_asset_normalization(ingest_request_id, job_attempt, normalization)
        .await
        .map_err(map_inbox_error)?;
    Ok(())
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
        .filter(|stream| matches!(&stream.kind, MediaStreamKind::Video))
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
    };

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
                    && !name.starts_with(".sooqa-normalize-"),
                "video attempt file was left behind: {name}"
            );
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
    async fn first_fitting_transcode_wins_and_losing_candidates_are_removed() {
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
        .expect("first fitting transcode should be selected");
        assert_eq!(result.digest.bytes, 9);
        assert_eq!(tokio::fs::metadata(&output).await.unwrap().len(), 9);
        assert_no_video_attempt_files(&root).await;
        clean_test_root(&root).await;
    }

    #[tokio::test]
    async fn oversized_remux_cannot_displace_storable_preferred_crf_candidate() {
        let root = std::env::temp_dir().join(format!("sooqa-video-adaptation-{}", Uuid::new_v4()));
        tokio::fs::create_dir(&root).await.expect("test root should be created");
        let output = root.join("canonical.mp4");
        let probe = adaptation_probe(320, 240, 20);
        let planner = NormalizationPlanner::new(
            "ffmpeg",
            CanonicalVideoProfile {
                target_max_bytes: 10,
                preferred_crf: 23,
                maximum_crf: 23,
                ..Default::default()
            },
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
            CanonicalVideoProfile {
                target_max_bytes: 1,
                preferred_crf: 23,
                maximum_crf: 23,
                ..Default::default()
            },
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
    async fn no_fit_keeps_the_no_loss_remux_fallback_and_bounds_crf_resolution_attempts() {
        let root = std::env::temp_dir().join(format!("sooqa-video-adaptation-{}", Uuid::new_v4()));
        tokio::fs::create_dir(&root).await.expect("test root should be created");
        let output = root.join("canonical.mp4");
        let probe = adaptation_probe(1920, 1080, 20);
        let planner = NormalizationPlanner::new(
            "ffmpeg",
            CanonicalVideoProfile {
                target_max_bytes: 1,
                preferred_crf: 23,
                maximum_crf: 24,
                ..Default::default()
            },
        )
        .expect("test profile should be valid");
        let initial_plan = planner.plan("input.mp4", &output, &probe).expect("plan should build");
        let runner = Arc::new(VideoAdaptationRunner::new([20; 9], (1920, 1080)));
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
        assert_eq!(commands.len(), 9, "one remux plus two CRFs across four resolutions");
        for command in commands.iter().skip(1) {
            let crf = command
                .args()
                .windows(2)
                .find(|pair| pair[0] == "-crf")
                .and_then(|pair| pair[1].to_str())
                .and_then(|value| value.parse::<u8>().ok())
                .expect("transcode candidate should carry CRF");
            assert!((23..=24).contains(&crf));
            let filter = command
                .args()
                .windows(2)
                .find(|pair| pair[0] == "-vf")
                .and_then(|pair| pair[1].to_str())
                .expect("transcode candidate should carry a scale filter");
            let (width, height) = parse_scale_dimensions(filter).expect("scale dimensions parse");
            assert!(width <= 1920 && height <= 1080 && width.min(height) >= 480);
        }
        assert_no_video_attempt_files(&root).await;
        clean_test_root(&root).await;
    }

    #[tokio::test]
    async fn adaptation_error_cleans_fallback_and_current_candidate() {
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
        let runner = Arc::new(VideoAdaptationRunner::new([20], (320, 240)).failing_at(2));
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
        .expect_err("synthetic transcode failure should be returned");
        assert!(matches!(error, NormalizationExecutionError::ProcessFailed { .. }));
        assert_no_video_attempt_files(&root).await;
        clean_test_root(&root).await;
    }

    #[tokio::test]
    async fn adaptation_cancellation_cleans_attempt_files() {
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
        let blocked = Arc::new(Notify::new());
        let runner =
            Arc::new(VideoAdaptationRunner::new([20], (320, 240)).blocking_at(2, blocked.clone()));
        let executor = adaptation_executor(runner);
        let task = tokio::spawn(async move {
            execute_video_normalization(
                &planner,
                &executor,
                Path::new("input.mp4"),
                &output,
                &probe,
                100,
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
