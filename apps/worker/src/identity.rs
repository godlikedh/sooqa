//! Media identity finalization and fingerprint jobs.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use sooqa_inbox::{
    AssetNormalization, AssetThumbnailNormalization, IngestFinalization, IngestKind, IngestStatus,
    SourceMediaKind,
};
use sooqa_jobs::{Job, JobCommand};
use sooqa_library::{
    MAX_MEDIA_PREVIEW_BYTES, MediaIngest, MediaKind, MediaMetadata, MediaPreviewInput,
    MediaSourceInput, NewMedia, SourceKind, VideoDuplicateClassification, VideoDuplicateEvidence,
    VideoDuplicateMatch, VideoFingerprintCandidate, VideoFingerprintInput, VideoIdentityDecision,
};
use sooqa_media::{
    FrameExtractionError, FrameExtractor, MediaWorkspace, SequenceAlignmentConfig,
    SequenceClassification, VideoSequenceFingerprint, WorkspaceArea, align_video_sequences,
    encode_bounded_preview, sha256_file, validate_bounded_preview_for_mime,
};
use sooqa_persistence::{
    InboxRepository, IngestFinalizationStart, IngestFingerprintStart, IngestVideoIdentityStart,
    LibraryRepository,
};
use uuid::Uuid;

use crate::common::{
    HandlerFailure, HandlerFn, WorkspaceAdmission, load_ingest_for_admission, map_inbox_error,
    map_library_error, map_workspace_error, request_media_kind, workspace_input,
};

pub type IdentityAlignmentHook = Arc<dyn Fn() + Send + Sync>;

pub(crate) fn fingerprint_stage_may_run(status: IngestStatus) -> bool {
    matches!(status, IngestStatus::Fingerprinting | IngestStatus::FailedRetryable)
}

pub fn finalize_ingest_handler(
    inbox: InboxRepository,
    library: LibraryRepository,
    work_root: impl Into<PathBuf>,
) -> HandlerFn {
    let work_root = work_root.into();
    Arc::new(move |job| {
        let inbox = inbox.clone();
        let library = library.clone();
        let work_root = work_root.clone();
        Box::pin(async move { finalize_ingest(&inbox, &library, &work_root, job).await })
    })
}

pub fn compute_fingerprint_handler(
    inbox: InboxRepository,
    library: LibraryRepository,
    work_root: impl Into<PathBuf>,
    extractor: FrameExtractor,
) -> HandlerFn {
    compute_fingerprint_handler_with_options(
        inbox,
        library,
        work_root,
        extractor,
        WorkspaceAdmission::disabled(),
        None,
    )
}

/// Build the fingerprint handler with an optional synchronous test probe that
/// runs inside the blocking alignment closure.  Production callers should use
/// [`compute_fingerprint_handler`]; the probe keeps transaction-boundary and
/// thread-placement integration tests deterministic without changing the
/// identity algorithm.
pub fn compute_fingerprint_handler_with_alignment_hook(
    inbox: InboxRepository,
    library: LibraryRepository,
    work_root: impl Into<PathBuf>,
    extractor: FrameExtractor,
    alignment_hook: Option<IdentityAlignmentHook>,
) -> HandlerFn {
    let work_root = work_root.into();
    Arc::new(move |job| {
        let inbox = inbox.clone();
        let library = library.clone();
        let work_root = work_root.clone();
        let extractor = extractor.clone();
        let alignment_hook = alignment_hook.clone();
        Box::pin(async move {
            compute_fingerprint(
                &inbox,
                &library,
                &work_root,
                &extractor,
                WorkspaceAdmission::disabled(),
                alignment_hook.as_ref(),
                job,
            )
            .await
        })
    })
}

pub fn compute_fingerprint_handler_with_admission(
    inbox: InboxRepository,
    library: LibraryRepository,
    work_root: impl Into<PathBuf>,
    extractor: FrameExtractor,
    admission: WorkspaceAdmission,
) -> HandlerFn {
    compute_fingerprint_handler_with_options(inbox, library, work_root, extractor, admission, None)
}

fn compute_fingerprint_handler_with_options(
    inbox: InboxRepository,
    library: LibraryRepository,
    work_root: impl Into<PathBuf>,
    extractor: FrameExtractor,
    admission: WorkspaceAdmission,
    alignment_hook: Option<IdentityAlignmentHook>,
) -> HandlerFn {
    let work_root = work_root.into();
    Arc::new(move |job| {
        let inbox = inbox.clone();
        let library = library.clone();
        let work_root = work_root.clone();
        let extractor = extractor.clone();
        let alignment_hook = alignment_hook.clone();
        Box::pin(async move {
            compute_fingerprint(
                &inbox,
                &library,
                &work_root,
                &extractor,
                admission,
                alignment_hook.as_ref(),
                job,
            )
            .await
        })
    })
}

async fn finalize_ingest(
    inbox: &InboxRepository,
    library: &LibraryRepository,
    work_root: &Path,
    job: Job,
) -> Result<(), HandlerFailure> {
    let ingest_request_id = match &job.command {
        JobCommand::FinalizeIngest(payload) => payload.ingest_id,
        _ => {
            return Err(HandlerFailure::permanent(
                "invalid_payload",
                "finalize_ingest handler received a different job command",
            ));
        }
    };
    let job_attempt = job.lease().ok_or_else(|| {
        HandlerFailure::permanent(
            "invalid_job_state",
            "finalize_ingest handler requires a running job lease",
        )
    })?;
    let request = match inbox.begin_ingest_finalization(ingest_request_id, &job_attempt).await {
        Ok(IngestFinalizationStart::Ready(request)) => request,
        Ok(IngestFinalizationStart::AlreadyAdvanced(_)) => return Ok(()),
        Err(error) => return Err(map_inbox_error(error)),
    };
    let input_data = match request.input_data() {
        Ok(input_data) => input_data,
        Err(error) => {
            return fail_finalization(
                inbox,
                ingest_request_id,
                &job_attempt,
                HandlerFailure::permanent("invalid_ingest_state", error.to_string()),
            )
            .await;
        }
    };
    let normalization = match input_data.normalization {
        Some(normalization) => normalization,
        None => {
            return fail_finalization(
                inbox,
                ingest_request_id,
                &job_attempt,
                HandlerFailure::permanent(
                    "invalid_ingest_state",
                    "ingest request has no stored normalization metadata",
                ),
            )
            .await;
        }
    };
    let mut metadata = match normalization_to_media_metadata(&normalization) {
        Ok(metadata) => metadata,
        Err(failure) => {
            return fail_finalization(inbox, ingest_request_id, &job_attempt, failure).await;
        }
    };
    metadata.preview = match load_thumbnail_preview(
        work_root,
        request.workspace_id,
        normalization.thumbnail.as_ref(),
    )
    .await
    {
        Ok(preview) => preview,
        Err(failure) => {
            return fail_finalization(inbox, ingest_request_id, &job_attempt, failure).await;
        }
    };
    if normalization.media_kind == SourceMediaKind::Video {
        return fail_finalization(
            inbox,
            ingest_request_id,
            &job_attempt,
            HandlerFailure::permanent(
                "invalid_ingest_state",
                "video finalization is handled by the pre-storage identity gate",
            ),
        )
        .await;
    }
    let source = source_record_for_request(&request);
    let resolution = match library
        .resolve_media(MediaIngest {
            media: NewMedia {
                kind: metadata.kind,
                title: request.page_title.clone(),
                description: request.supplied_description.clone(),
            },
            metadata,
            source,
            tags: request.supplied_tags.clone(),
        })
        .await
    {
        Ok(resolution) => resolution,
        Err(error) => {
            return fail_finalization(
                inbox,
                ingest_request_id,
                &job_attempt,
                map_library_error(error),
            )
            .await;
        }
    };
    inbox
        .complete_ingest_finalization(
            ingest_request_id,
            &job_attempt,
            IngestFinalization { media_id: resolution.media.id },
        )
        .await
        .map_err(map_inbox_error)?;
    Ok(())
}

async fn compute_fingerprint(
    inbox: &InboxRepository,
    library: &LibraryRepository,
    work_root: &Path,
    extractor: &FrameExtractor,
    admission: WorkspaceAdmission,
    alignment_hook: Option<&IdentityAlignmentHook>,
    job: Job,
) -> Result<(), HandlerFailure> {
    let ingest_request_id = match &job.command {
        JobCommand::ComputeFingerprint(payload) => payload.ingest_id,
        _ => {
            return Err(HandlerFailure::permanent(
                "invalid_payload",
                "compute_fingerprint handler received a different job command",
            ));
        }
    };
    let job_attempt = job.lease().ok_or_else(|| {
        HandlerFailure::permanent(
            "invalid_job_state",
            "compute_fingerprint handler requires a running job lease",
        )
    })?;
    let current_request = load_ingest_for_admission(inbox, ingest_request_id).await?;
    let mut preflight_failure = None;
    let mut preflight_exact_media_exists = None;
    if fingerprint_stage_may_run(current_request.status) {
        match current_request.input_data() {
            Ok(input_data) => match input_data.normalization {
                Some(normalization) if normalization.media_kind == SourceMediaKind::Video => {
                    match normalization_to_media_metadata(&normalization) {
                        Ok(metadata) => {
                            let media_ingest = MediaIngest {
                                media: NewMedia {
                                    kind: MediaKind::Video,
                                    title: current_request.page_title.clone(),
                                    description: current_request.supplied_description.clone(),
                                },
                                metadata,
                                source: source_record_for_request(&current_request),
                                tags: current_request.supplied_tags.clone(),
                            };
                            match library.resolve_exact_sha(&media_ingest).await {
                                Ok(media_id) => {
                                    let exists = media_id.is_some();
                                    if !exists {
                                        if !matches!(normalization.duration_ms, Some(value) if value > 0)
                                        {
                                            preflight_failure = Some(HandlerFailure::permanent(
                                                "invalid_ingest_state",
                                                "video normalization has no valid canonical duration",
                                            ));
                                        } else {
                                            admission.admit(
                                                work_root,
                                                sooqa_media::DEFAULT_MAX_FRAME_SEQUENCE_BYTES,
                                            )?;
                                        }
                                    }
                                    preflight_exact_media_exists = Some(exists);
                                }
                                Err(error) => {
                                    preflight_failure = Some(map_library_error(error));
                                }
                            }
                        }
                        Err(failure) => preflight_failure = Some(failure),
                    }
                }
                Some(_) => {}
                None => {
                    preflight_failure = Some(HandlerFailure::permanent(
                        "invalid_ingest_state",
                        "ingest request has no stored normalization metadata",
                    ));
                }
            },
            Err(error) => {
                preflight_failure =
                    Some(HandlerFailure::permanent("invalid_ingest_state", error.to_string()));
            }
        }
    }

    let request = match inbox.begin_ingest_fingerprinting(ingest_request_id, &job_attempt).await {
        Ok(IngestFingerprintStart::Ready(request)) => request,
        Ok(IngestFingerprintStart::AlreadyAdvanced(_)) => return Ok(()),
        Err(error) => return Err(map_inbox_error(error)),
    };
    if let Some(failure) = preflight_failure {
        return fail_fingerprint(inbox, ingest_request_id, &job_attempt, failure).await;
    }
    let input_data = match request.input_data() {
        Ok(input_data) => input_data,
        Err(error) => {
            return fail_fingerprint(
                inbox,
                ingest_request_id,
                &job_attempt,
                HandlerFailure::permanent("invalid_ingest_state", error.to_string()),
            )
            .await;
        }
    };
    let normalization = match input_data.normalization {
        Some(normalization) => normalization,
        None => {
            return fail_fingerprint(
                inbox,
                ingest_request_id,
                &job_attempt,
                HandlerFailure::permanent(
                    "invalid_ingest_state",
                    "ingest request has no stored normalization metadata",
                ),
            )
            .await;
        }
    };

    if normalization.media_kind != SourceMediaKind::Video {
        return fail_fingerprint(
            inbox,
            ingest_request_id,
            &job_attempt,
            HandlerFailure::permanent(
                "invalid_ingest_state",
                "video fingerprinting was queued for a non-video normalization",
            ),
        )
        .await;
    }

    let metadata = match normalization_to_media_metadata(&normalization) {
        Ok(metadata) => metadata,
        Err(failure) => {
            return fail_fingerprint(inbox, ingest_request_id, &job_attempt, failure).await;
        }
    };
    let mut media_ingest = MediaIngest {
        media: NewMedia {
            kind: MediaKind::Video,
            title: request.page_title.clone(),
            description: request.supplied_description.clone(),
        },
        metadata,
        source: source_record_for_request(&request),
        tags: request.supplied_tags.clone(),
    };
    let exact_media_exists = match preflight_exact_media_exists {
        Some(exists) => exists,
        None => {
            return fail_fingerprint(
                inbox,
                ingest_request_id,
                &job_attempt,
                HandlerFailure::permanent(
                    "invalid_ingest_state",
                    "fingerprint preflight did not resolve the exact media identity",
                ),
            )
            .await;
        }
    };

    let fingerprint = if exact_media_exists {
        None
    } else {
        let duration_ms = match normalization.duration_ms {
            Some(duration_ms) if duration_ms > 0 => duration_ms,
            _ => {
                return fail_fingerprint(
                    inbox,
                    ingest_request_id,
                    &job_attempt,
                    HandlerFailure::permanent(
                        "invalid_ingest_state",
                        "video normalization has no valid canonical duration",
                    ),
                )
                .await;
            }
        };
        let workspace_id = match workspace_input(&request) {
            Ok((workspace_id, _)) => workspace_id,
            Err(failure) => {
                return fail_fingerprint(inbox, ingest_request_id, &job_attempt, failure).await;
            }
        };
        let workspace = match MediaWorkspace::create(work_root, workspace_id).await {
            Ok(workspace) => workspace,
            Err(error) => {
                return fail_fingerprint(
                    inbox,
                    ingest_request_id,
                    &job_attempt,
                    map_workspace_error(error),
                )
                .await;
            }
        };
        if let Err(error) = workspace.validate() {
            return fail_fingerprint(
                inbox,
                ingest_request_id,
                &job_attempt,
                map_workspace_error(error),
            )
            .await;
        }
        let extraction = match extractor
            .extract_video_sequence_with_best_frame_from_area(
                &workspace,
                WorkspaceArea::Normalized,
                "canonical.mp4",
                duration_ms,
            )
            .await
        {
            Ok(result) => result,
            Err(error) => {
                return fail_fingerprint(
                    inbox,
                    ingest_request_id,
                    &job_attempt,
                    map_fingerprint_error(&job, error),
                )
                .await;
            }
        };
        if let Some(best_frame) = extraction.best_frame.as_ref() {
            let preview = match encode_bounded_preview(best_frame) {
                Ok(preview) => preview,
                Err(error) => {
                    return fail_fingerprint(
                        inbox,
                        ingest_request_id,
                        &job_attempt,
                        HandlerFailure::permanent("preview_encode", error.to_string()),
                    )
                    .await;
                }
            };
            media_ingest.metadata.preview = Some(MediaPreviewInput {
                bytes: preview.bytes,
                mime_type: "image/jpeg".to_owned(),
                width: preview.width,
                height: preview.height,
                sha256: decode_sha256(&preview.digest.sha256)?,
            });
        }
        Some(extraction.fingerprint)
    };

    let fingerprint_input = match fingerprint.as_ref() {
        Some(fingerprint) => Some(
            VideoFingerprintInput::try_new(
                fingerprint.version.as_str(),
                fingerprint.encode().map_err(|error| {
                    HandlerFailure::permanent("fingerprint_failed", error.to_string())
                })?,
                fingerprint.search_tokens(),
            )
            .map_err(|error| HandlerFailure::permanent("fingerprint_failed", error.to_string()))?,
        ),
        None => None,
    };
    let start = match inbox
        .begin_video_identity(
            ingest_request_id,
            &job_attempt,
            &media_ingest,
            fingerprint_input.as_ref(),
        )
        .await
    {
        Ok(start) => start,
        Err(error) => return Err(map_inbox_error(error)),
    };
    let IngestVideoIdentityStart::Ready { ingest, preparation, session } = start else {
        return Ok(());
    };

    let decision = if preparation.exact_media_id.is_some() || ingest.force_save {
        VideoIdentityDecision::NoMatch
    } else {
        let Some(fingerprint) = fingerprint else {
            inbox.abort_video_identity(session).await.map_err(map_inbox_error)?;
            return Err(HandlerFailure::permanent(
                "invalid_ingest_state",
                "video identity preparation requires a sequence fingerprint",
            ));
        };
        let candidates = preparation.candidates;
        let alignment_hook = alignment_hook.cloned();
        let alignment = match tokio::task::spawn_blocking(move || {
            if let Some(hook) = alignment_hook {
                hook();
            }
            align_video_identity(&fingerprint, &candidates, SequenceAlignmentConfig::default())
        })
        .await
        {
            Ok(alignment) => alignment,
            Err(error) => {
                inbox.abort_video_identity(session).await.map_err(map_inbox_error)?;
                return Err(HandlerFailure::permanent(
                    "identity_alignment_failed",
                    format!("video identity alignment task failed: {error}"),
                ));
            }
        };
        match alignment {
            Ok(decision) => decision,
            Err(message) => {
                inbox.abort_video_identity(session).await.map_err(map_inbox_error)?;
                return Err(HandlerFailure::permanent("identity_alignment_failed", message));
            }
        }
    };

    inbox
        .complete_video_identity(
            ingest_request_id,
            &job_attempt,
            session,
            media_ingest,
            fingerprint_input.as_ref(),
            &decision,
        )
        .await
        .map_err(map_inbox_error)?;
    Ok(())
}

fn align_video_identity(
    incoming: &VideoSequenceFingerprint,
    candidates: &[VideoFingerprintCandidate],
    config: SequenceAlignmentConfig,
) -> Result<VideoIdentityDecision, String> {
    let mut matches = candidates
        .iter()
        .filter_map(|candidate| {
            let stored =
                VideoSequenceFingerprint::decode(&candidate.fingerprint_data).map_err(|error| {
                    format!(
                        "media {} has an invalid stored fingerprint: {error}",
                        candidate.media_id
                    )
                });
            let stored = match stored {
                Ok(stored) => stored,
                Err(error) => return Some(Err(error)),
            };
            let alignment =
                align_video_sequences(incoming, &stored, config).map_err(|error| error.to_string());
            match alignment {
                Ok(alignment) => duplicate_match(candidate, alignment).map(Ok),
                Err(error) => Some(Err(error)),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    matches.sort_by(|left, right| {
        classification_rank(right.classification)
            .cmp(&classification_rank(left.classification))
            .then_with(|| right.score_bps.cmp(&left.score_bps))
            .then_with(|| right.shared_token_count.cmp(&left.shared_token_count))
            .then_with(|| left.media_id.cmp(&right.media_id))
    });
    if matches
        .iter()
        .any(|item| item.classification == VideoDuplicateClassification::StrongDuplicate)
    {
        matches.truncate(sooqa_library::MAX_VIDEO_DUPLICATE_MATCHES);
        return Ok(VideoIdentityDecision::DuplicatePending {
            evidence: VideoDuplicateEvidence {
                algorithm_version: incoming.version.as_str().to_owned(),
                matches,
            },
        });
    }
    Ok(VideoIdentityDecision::NoMatch)
}

fn duplicate_match(
    candidate: &VideoFingerprintCandidate,
    alignment: sooqa_media::SequenceAlignment,
) -> Option<VideoDuplicateMatch> {
    let classification = match alignment.classification {
        SequenceClassification::StrongDuplicate => VideoDuplicateClassification::StrongDuplicate,
        SequenceClassification::PartialMatch => VideoDuplicateClassification::PartialMatch,
        SequenceClassification::NotDuplicate => return None,
    };
    let evidence = alignment.evidence;
    Some(VideoDuplicateMatch {
        media_id: candidate.media_id,
        fingerprint_version: candidate.fingerprint_version.clone(),
        classification,
        aligned_offset_ms: evidence.aligned_offset_ms,
        informative_matched_samples: evidence.informative_matched_samples,
        incoming_coverage_bps: evidence.incoming_coverage_bps,
        candidate_coverage_bps: evidence.candidate_coverage_bps,
        median_distance_bps: evidence.median_distance_bps,
        high_percentile_distance_bps: evidence.high_percentile_distance_bps,
        longest_temporally_consistent_run: evidence.longest_temporally_consistent_run,
        unmatched_incoming_prefix: evidence.unmatched_incoming_prefix,
        unmatched_incoming_suffix: evidence.unmatched_incoming_suffix,
        unmatched_candidate_prefix: evidence.unmatched_candidate_prefix,
        unmatched_candidate_suffix: evidence.unmatched_candidate_suffix,
        gap_count: evidence.gap_count,
        score_bps: evidence.score_bps,
        shared_token_count: candidate.shared_token_count,
        token_overlap_bps: candidate.overlap_bps,
    })
}

fn classification_rank(classification: VideoDuplicateClassification) -> u8 {
    match classification {
        VideoDuplicateClassification::StrongDuplicate => 2,
        VideoDuplicateClassification::PartialMatch => 1,
    }
}

fn map_fingerprint_error(job: &Job, error: FrameExtractionError) -> HandlerFailure {
    let message = error.to_string();
    let retryable = matches!(&error, FrameExtractionError::Command(error) if error.is_timeout())
        && job.attempt_count < job.max_attempts;
    if retryable {
        HandlerFailure::retryable("fingerprint_timeout", message)
    } else {
        HandlerFailure::permanent("fingerprint_failed", message)
    }
}

async fn fail_fingerprint(
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
        .fail_ingest_fingerprint(
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

async fn load_thumbnail_preview(
    work_root: &Path,
    workspace_id: Uuid,
    thumbnail: Option<&AssetThumbnailNormalization>,
) -> Result<Option<MediaPreviewInput>, HandlerFailure> {
    let Some(thumbnail) = thumbnail else {
        return Ok(None);
    };
    let mime_type = thumbnail.mime_type.as_deref().ok_or_else(|| {
        HandlerFailure::permanent("invalid_preview", "preview MIME type is missing")
    })?;
    if !matches!(mime_type, "image/jpeg" | "image/png") {
        return Err(HandlerFailure::permanent(
            "invalid_preview",
            format!("preview MIME type {mime_type:?} is not supported"),
        ));
    }
    let workspace =
        MediaWorkspace::create(work_root, workspace_id).await.map_err(map_workspace_error)?;
    workspace.validate().map_err(map_workspace_error)?;
    let stored_path = PathBuf::from(&thumbnail.local_work_path);
    let file_name = stored_path.file_name().and_then(|value| value.to_str()).ok_or_else(|| {
        HandlerFailure::permanent("invalid_preview", "preview path has no safe file name")
    })?;
    let expected_path =
        workspace.path(WorkspaceArea::Previews, file_name).map_err(map_workspace_error)?;
    if stored_path != expected_path {
        return Err(HandlerFailure::permanent(
            "invalid_preview",
            "preview path is outside the workspace preview area",
        ));
    }
    let metadata = tokio::fs::symlink_metadata(&expected_path).await.map_err(|error| {
        HandlerFailure::permanent(
            "invalid_preview",
            format!("preview artifact metadata could not be read: {error}"),
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(HandlerFailure::permanent(
            "invalid_preview",
            "preview artifact is not a regular file",
        ));
    }
    let size = usize::try_from(metadata.len()).map_err(|_| {
        HandlerFailure::permanent("invalid_preview", "preview artifact size does not fit memory")
    })?;
    if size == 0 || size > MAX_MEDIA_PREVIEW_BYTES || metadata.len() != thumbnail.file_size_bytes {
        return Err(HandlerFailure::permanent(
            "invalid_preview",
            "preview artifact exceeds its persisted bounds",
        ));
    }
    let bytes = tokio::fs::read(&expected_path).await.map_err(|error| {
        HandlerFailure::permanent(
            "invalid_preview",
            format!("preview artifact could not be read: {error}"),
        )
    })?;
    let digest = sha256_file(&expected_path).await.map_err(|error| {
        HandlerFailure::permanent(
            "invalid_preview",
            format!("preview artifact could not be hashed: {error}"),
        )
    })?;
    if digest.sha256 != thumbnail.sha256 {
        return Err(HandlerFailure::permanent(
            "invalid_preview",
            "preview artifact SHA-256 does not match normalization metadata",
        ));
    }
    let (width, height) = validate_bounded_preview_for_mime(&bytes, Some(mime_type))
        .map_err(|error| HandlerFailure::permanent("invalid_preview", error.to_string()))?;
    if thumbnail.width != Some(width) || thumbnail.height != Some(height) {
        return Err(HandlerFailure::permanent(
            "invalid_preview",
            "preview artifact dimensions do not match normalization metadata",
        ));
    }
    Ok(Some(MediaPreviewInput {
        bytes,
        mime_type: mime_type.to_owned(),
        width,
        height,
        sha256: decode_sha256(&thumbnail.sha256)?,
    }))
}

fn normalization_to_media_metadata(
    normalization: &AssetNormalization,
) -> Result<MediaMetadata, HandlerFailure> {
    Ok(MediaMetadata {
        kind: media_kind_for_normalization(normalization.media_kind)?,
        mime_type: normalization.mime_type.clone(),
        container: normalization.container.clone(),
        video_codec: normalization.video_codec.clone(),
        audio_codec: normalization.audio_codec.clone(),
        width: to_database_dimension(normalization.width, "width")?,
        height: to_database_dimension(normalization.height, "height")?,
        duration_ms: normalization.duration_ms,
        bit_rate: normalization.bit_rate,
        file_size_bytes: Some(normalization.file_size_bytes),
        sha256: Some(decode_sha256(&normalization.sha256)?),
        local_work_path: Some(normalization.local_work_path.clone()),
        preview: None,
    })
}

fn media_kind_for_normalization(media_kind: SourceMediaKind) -> Result<MediaKind, HandlerFailure> {
    match media_kind {
        SourceMediaKind::Video => Ok(MediaKind::Video),
        SourceMediaKind::Image => Ok(MediaKind::Image),
        SourceMediaKind::Audio => Ok(MediaKind::Audio),
        SourceMediaKind::Animation => Ok(MediaKind::Animation),
        SourceMediaKind::Unknown => Err(HandlerFailure::permanent(
            "invalid_normalization",
            "normalized media kind is unknown",
        )),
    }
}

fn to_database_dimension(
    value: Option<u32>,
    field: &'static str,
) -> Result<Option<i32>, HandlerFailure> {
    value
        .map(|value| {
            i32::try_from(value).map_err(|_| {
                HandlerFailure::permanent(
                    "invalid_normalization",
                    format!("normalized {field} does not fit the library schema"),
                )
            })
        })
        .transpose()
}

fn decode_sha256(value: &str) -> Result<Vec<u8>, HandlerFailure> {
    let bytes = value.as_bytes();
    if bytes.len() != 64 || !bytes.is_ascii() {
        return Err(HandlerFailure::permanent(
            "invalid_normalization",
            "normalized SHA-256 digest must contain 64 hexadecimal characters",
        ));
    }
    let mut digest = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let high = decode_hex_digit(pair[0]).ok_or_else(|| {
            HandlerFailure::permanent(
                "invalid_normalization",
                "normalized SHA-256 digest is not hexadecimal",
            )
        })?;
        let low = decode_hex_digit(pair[1]).ok_or_else(|| {
            HandlerFailure::permanent(
                "invalid_normalization",
                "normalized SHA-256 digest is not hexadecimal",
            )
        })?;
        digest.push((high << 4) | low);
    }
    Ok(digest)
}

fn decode_hex_digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn source_record_for_request(request: &sooqa_inbox::Ingest) -> MediaSourceInput {
    let ytdlp = ytdlp_provenance_for_request(request);
    let (kind, normalized_url, platform, platform_content_id) = match request.kind {
        IngestKind::Url => (
            SourceKind::DirectUrl,
            ytdlp
                .as_ref()
                .and_then(|metadata| metadata.canonical_url.clone())
                .or_else(|| Some(request.source_url.clone())),
            ytdlp.as_ref().and_then(|metadata| metadata.platform.clone()),
            ytdlp.as_ref().and_then(|metadata| metadata.content_id.clone()),
        ),
        IngestKind::TelegramMessage => (
            SourceKind::Telegram,
            None,
            Some("telegram".to_owned()),
            Some(request.source_url.clone()),
        ),
        IngestKind::Upload => (
            SourceKind::Upload,
            None,
            Some("sooqa_ingest".to_owned()),
            Some(request.id.to_string()),
        ),
    };
    MediaSourceInput {
        ingest_id: Some(request.id),
        kind,
        original_url: Some(original_source_url(request)),
        normalized_url,
        platform,
        platform_content_id,
        author_name: ytdlp.as_ref().and_then(|metadata| metadata.uploader.clone()),
        title: request
            .page_title
            .clone()
            .or_else(|| ytdlp.as_ref().and_then(|metadata| metadata.title.clone())),
        description: request.supplied_description.clone().or_else(|| {
            if request.kind == IngestKind::TelegramMessage {
                request.supplied_caption.clone()
            } else {
                None
            }
        }),
        published_at: None,
        metadata: source_provenance_for_request(request),
    }
}

#[derive(Debug, Default, Clone)]
struct YtDlpProvenance {
    platform: Option<String>,
    content_id: Option<String>,
    extractor: Option<String>,
    uploader: Option<String>,
    title: Option<String>,
    canonical_url: Option<String>,
}

fn ytdlp_provenance_for_request(request: &sooqa_inbox::Ingest) -> Option<YtDlpProvenance> {
    if request.kind != IngestKind::Url {
        return None;
    }
    let input_data = request.input_data().ok()?;
    let inspection = input_data.inspection.as_ref()?;
    if inspection.adapter != "yt_dlp" {
        return None;
    }
    let metadata = &inspection.metadata;
    let canonical_url = inspection
        .resolved_url
        .as_deref()
        .or_else(|| metadata.get("webpage_url").and_then(serde_json::Value::as_str));
    Some(YtDlpProvenance {
        platform: bounded_provenance_string(metadata.get("platform"), 64),
        content_id: bounded_provenance_string(metadata.get("id"), 256),
        extractor: bounded_provenance_string(metadata.get("extractor"), 128),
        uploader: bounded_provenance_string(metadata.get("uploader"), 4 * 1024),
        title: bounded_provenance_string(metadata.get("title"), 4 * 1024),
        canonical_url: bounded_provenance_text(canonical_url, 2 * 1024),
    })
}

fn bounded_provenance_string(
    value: Option<&serde_json::Value>,
    max_bytes: usize,
) -> Option<String> {
    let value = value?.as_str()?;
    if value.len() > max_bytes || value.chars().any(char::is_control) {
        return None;
    }
    Some(value.to_owned())
}

fn bounded_provenance_text(value: Option<&str>, max_bytes: usize) -> Option<String> {
    let value = value?;
    if value.len() > max_bytes || value.chars().any(char::is_control) {
        return None;
    }
    Some(value.to_owned())
}

fn original_source_url(request: &sooqa_inbox::Ingest) -> String {
    request
        .input_data()
        .ok()
        .and_then(|data| data.source_url().map(ToOwned::to_owned))
        .unwrap_or_else(|| request.source_url.clone())
}

#[derive(Debug, serde::Serialize)]
struct SourceProvenance {
    #[serde(skip_serializing_if = "Option::is_none")]
    page_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    media_kind: Option<SourceMediaKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    telegram_update_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    telegram_chat_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    telegram_message_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    telegram_file_unique_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    two_ch_mirror: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    platform: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    platform_content_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    extractor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    uploader: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    canonical_url: Option<String>,
}

fn source_provenance_for_request(request: &sooqa_inbox::Ingest) -> serde_json::Value {
    let input_data = request.input_data().ok();
    let download = input_data.as_ref().and_then(|data| data.download.as_ref());
    let media_kind = request_media_kind(request);
    let mime_type = download
        .and_then(|download| download.mime_type.clone())
        .or_else(|| input_data.as_ref().and_then(|data| data.source.mime_type.clone()));
    let source_size_bytes = download
        .map(|download| download.bytes)
        .or_else(|| input_data.as_ref().and_then(|data| data.source.file_size));
    let two_ch_mirror = input_data
        .as_ref()
        .and_then(|data| data.inspection.as_ref())
        .and_then(|inspection| inspection.metadata.get("two_ch_mirror"))
        .cloned();
    let ytdlp = ytdlp_provenance_for_request(request);
    let source = input_data.as_ref().map(|data| &data.source);
    let provenance = SourceProvenance {
        page_url: request.page_url.clone(),
        media_kind,
        mime_type,
        source_size_bytes,
        telegram_update_id: source.and_then(|source| source.telegram_update_id),
        telegram_chat_id: source.and_then(|source| source.telegram_chat_id),
        telegram_message_id: source.and_then(|source| source.telegram_message_id),
        telegram_file_unique_id: source.and_then(|source| source.telegram_file_unique_id.clone()),
        two_ch_mirror,
        platform: ytdlp.as_ref().and_then(|metadata| metadata.platform.clone()),
        platform_content_id: ytdlp.as_ref().and_then(|metadata| metadata.content_id.clone()),
        extractor: ytdlp.as_ref().and_then(|metadata| metadata.extractor.clone()),
        uploader: ytdlp.as_ref().and_then(|metadata| metadata.uploader.clone()),
        canonical_url: ytdlp.as_ref().and_then(|metadata| metadata.canonical_url.clone()),
    };
    serde_json::to_value(provenance).expect("source provenance is serializable")
}

async fn fail_finalization(
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
        .fail_ingest_finalization(
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
    use std::{path::PathBuf, time::Duration};

    use sooqa_inbox::{Ingest, IngestSubmission, IngestSubmissionInput, SubmittedVia};
    use sooqa_jobs::JobStatus;
    use sooqa_media::{CommandError, FrameExtractionError};
    use time::OffsetDateTime;

    use super::*;

    fn running_compute_job(attempt_count: i32, max_attempts: i32) -> Job {
        let now = OffsetDateTime::now_utc();
        Job {
            id: Uuid::new_v4(),
            command: JobCommand::ComputeFingerprint(sooqa_jobs::IngestJobPayload {
                ingest_id: Uuid::new_v4(),
            }),
            status: JobStatus::Running,
            priority: 0,
            run_at: now,
            attempt_count,
            max_attempts,
            lease_token: Some(Uuid::new_v4()),
            lease_owner: Some("test-worker".to_owned()),
            lease_expires_at: None,
            last_heartbeat_at: None,
            last_error_class: None,
            last_error_message: None,
            dedupe_key: None,
            created_at: now,
            updated_at: now,
            completed_at: None,
        }
    }

    #[test]
    fn malformed_stored_fingerprint_is_rejected_before_alignment() {
        let incoming = VideoSequenceFingerprint::new(
            500,
            500,
            vec![sooqa_media::VideoSequenceSample {
                phash: 1,
                dhash: 2,
                mean_luma: 100,
                mean_chroma_u: 0,
                mean_chroma_v: 0,
                information_bps: 8_000,
                transition_bps: 0,
            }],
        )
        .unwrap();
        let candidate = VideoFingerprintCandidate {
            media_id: Uuid::new_v4(),
            width: None,
            height: None,
            audio_codec: None,
            fingerprint_version: "video_sequence_v1".to_owned(),
            fingerprint_data: vec![0; 1],
            search_tokens: Vec::new(),
            shared_token_count: 8,
            overlap_bps: 1_000,
        };
        let error =
            align_video_identity(&incoming, &[candidate], SequenceAlignmentConfig::default())
                .expect_err("malformed candidate bytes must fail closed");
        assert!(error.contains("invalid stored fingerprint"));
    }

    #[test]
    fn sha256_decoder_accepts_hex_and_rejects_malformed_utf8_without_panicking() {
        let digest = decode_sha256(&"ab".repeat(32)).expect("hex digest should decode");
        assert_eq!(digest, vec![0xab; 32]);

        let malformed = "é".repeat(32);
        let error = decode_sha256(&malformed).expect_err("non-ASCII digest should be rejected");
        assert_eq!(error.class, "invalid_normalization");

        let error = decode_sha256(&format!("{}g", "0".repeat(63)))
            .expect_err("non-hex digest should be rejected");
        assert_eq!(error.class, "invalid_normalization");
    }

    #[test]
    fn fingerprint_timeout_becomes_terminal_on_the_last_attempt() {
        let timeout_error = || {
            FrameExtractionError::Command(CommandError::TimedOut {
                program: PathBuf::from("ffmpeg"),
                timeout: Duration::from_secs(1),
            })
        };
        let retry = map_fingerprint_error(&running_compute_job(1, 5), timeout_error());
        assert!(retry.retryable);
        assert_eq!(retry.class, "fingerprint_timeout");

        let exhausted = map_fingerprint_error(&running_compute_job(5, 5), timeout_error());
        assert!(!exhausted.retryable);
        assert_eq!(exhausted.class, "fingerprint_failed");
    }

    #[test]
    fn source_provenance_keeps_page_context_and_selected_2ch_mirror() {
        let mut input =
            IngestSubmissionInput::new("https://2ch.life/b/src/clip.webm", SubmittedVia::Companion);
        input.page_url = Some("https://2ch.life/b/res/123".to_owned());
        let submission = IngestSubmission::try_new(input).expect("submission should validate");
        let mut request =
            Ingest::from_submission(Uuid::new_v4(), &submission).expect("valid submission");
        let mut input_data = request.input_data().expect("envelope should decode");
        input_data.inspection = Some(sooqa_inbox::SourceInspection {
            adapter: "two_ch".to_owned(),
            source_url: "https://2ch.life/b/src/clip.webm".to_owned(),
            resolved_url: Some("https://2ch.org/b/src/clip.webm".to_owned()),
            media_kind: SourceMediaKind::Video,
            mime_type: Some("video/webm".to_owned()),
            content_length_bytes: None,
            title: None,
            metadata: serde_json::json!({
                "two_ch_mirror": {
                    "submitted_host": "2ch.life",
                    "selected_host": "2ch.org",
                    "selected_url": "https://2ch.org/b/src/clip.webm"
                }
            }),
        });
        request.set_input_data(input_data).expect("envelope should encode");

        let metadata = source_provenance_for_request(&request);
        assert_eq!(metadata["page_url"], "https://2ch.life/b/res/123");
        assert_eq!(metadata["two_ch_mirror"]["submitted_host"], "2ch.life");
        assert_eq!(metadata["two_ch_mirror"]["selected_host"], "2ch.org");
    }

    #[test]
    fn source_record_preserves_the_submitted_url_separately_from_normalized_url() {
        let submission = IngestSubmission::try_new(IngestSubmissionInput::new(
            "HTTPS://Example.COM:443/clip.webm?utm_source=feed&id=7#frame",
            SubmittedVia::Companion,
        ))
        .expect("submission should validate");
        let request =
            Ingest::from_submission(Uuid::new_v4(), &submission).expect("valid submission");

        let source = source_record_for_request(&request);
        assert_eq!(
            source.original_url.as_deref(),
            Some("HTTPS://Example.COM:443/clip.webm?utm_source=feed&id=7#frame")
        );
        assert_eq!(source.normalized_url.as_deref(), Some("https://example.com/clip.webm?id=7"));
    }

    #[test]
    fn source_record_keeps_bounded_ytdlp_provider_provenance() {
        let submission = IngestSubmission::try_new(IngestSubmissionInput::new(
            "https://vm.tiktok.com/ZMshare/",
            SubmittedVia::Companion,
        ))
        .expect("submission should validate");
        let mut request =
            Ingest::from_submission(Uuid::new_v4(), &submission).expect("valid submission");
        let mut input_data = request.input_data().expect("envelope should decode");
        input_data.inspection = Some(sooqa_inbox::SourceInspection {
            adapter: "yt_dlp".to_owned(),
            source_url: "https://vm.tiktok.com/ZMshare/".to_owned(),
            resolved_url: Some("https://www.tiktok.com/@creator/video/123456".to_owned()),
            media_kind: SourceMediaKind::Video,
            mime_type: Some("video/mp4".to_owned()),
            content_length_bytes: None,
            title: None,
            metadata: serde_json::json!({
                "platform": "tiktok",
                "id": "123456",
                "extractor": "TikTok",
                "uploader": "creator",
                "title": "A public clip",
                "webpage_url": "https://www.tiktok.com/@creator/video/123456"
            }),
        });
        request.set_input_data(input_data).expect("envelope should encode");

        let source = source_record_for_request(&request);
        assert_eq!(source.original_url.as_deref(), Some("https://vm.tiktok.com/ZMshare/"));
        assert_eq!(source.platform.as_deref(), Some("tiktok"));
        assert_eq!(source.platform_content_id.as_deref(), Some("123456"));
        assert_eq!(source.author_name.as_deref(), Some("creator"));
        assert_eq!(source.title.as_deref(), Some("A public clip"));
        assert_eq!(
            source.normalized_url.as_deref(),
            Some("https://www.tiktok.com/@creator/video/123456")
        );
        assert_eq!(source.metadata["extractor"], "TikTok");
        assert_eq!(
            source.metadata["canonical_url"],
            "https://www.tiktok.com/@creator/video/123456"
        );
    }
}
