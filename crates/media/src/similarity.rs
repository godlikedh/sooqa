use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{FingerprintVersion, VideoFingerprint};

const FRAME_HASH_BITS: f64 = 64.0;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct SimilarityThresholds {
    pub likely_duplicate: f64,
    pub possible_duplicate: f64,
}

impl Default for SimilarityThresholds {
    fn default() -> Self {
        Self { likely_duplicate: 0.90, possible_duplicate: 0.75 }
    }
}

impl SimilarityThresholds {
    fn validate(self) -> Result<(), SimilarityError> {
        if !self.likely_duplicate.is_finite()
            || !self.possible_duplicate.is_finite()
            || !(0.0..=1.0).contains(&self.possible_duplicate)
            || !(0.0..=1.0).contains(&self.likely_duplicate)
            || self.possible_duplicate > self.likely_duplicate
        {
            return Err(SimilarityError::InvalidThresholds);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct SimilarityConfig {
    pub min_duration_score: f64,
    pub max_aspect_ratio_delta: f64,
    pub thresholds: SimilarityThresholds,
}

impl Default for SimilarityConfig {
    fn default() -> Self {
        Self {
            min_duration_score: 0.50,
            max_aspect_ratio_delta: 0.25,
            thresholds: SimilarityThresholds::default(),
        }
    }
}

impl SimilarityConfig {
    pub fn validate(self) -> Result<(), SimilarityError> {
        if !self.min_duration_score.is_finite()
            || !(0.0..=1.0).contains(&self.min_duration_score)
            || !self.max_aspect_ratio_delta.is_finite()
            || !(0.0..=1.0).contains(&self.max_aspect_ratio_delta)
        {
            return Err(SimilarityError::InvalidConfig);
        }
        self.thresholds.validate()
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VideoSimilarityInput<'a> {
    pub fingerprint: &'a VideoFingerprint,
    pub aspect_ratio: Option<f64>,
    pub has_audio: Option<bool>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SimilarityClassification {
    LikelyDuplicate,
    PossibleDuplicate,
    NoWarning,
    PrefilterRejected,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrefilterRejection {
    DurationMismatch,
    AspectMismatch,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct FrameDistance {
    pub left_ratio_bps: u16,
    pub right_ratio_bps: u16,
    pub hamming_distance: u8,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SimilarityEvidence {
    pub algorithm_version: FingerprintVersion,
    pub duration_delta_ms: u64,
    pub duration_score: f64,
    pub aspect_score: Option<f64>,
    pub structure_score: f64,
    pub visual_score: Option<f64>,
    pub median_hamming_distance: Option<f64>,
    pub final_score: f64,
    pub prefilter_rejection: Option<PrefilterRejection>,
    pub frame_distances: Vec<FrameDistance>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SimilarityResult {
    pub classification: SimilarityClassification,
    pub evidence: SimilarityEvidence,
}

impl SimilarityResult {
    pub fn score_basis_points(&self) -> u16 {
        (self.evidence.final_score * 10_000.0).round().clamp(0.0, 10_000.0) as u16
    }
}

pub fn compare_videos(
    left: VideoSimilarityInput<'_>,
    right: VideoSimilarityInput<'_>,
    config: SimilarityConfig,
) -> Result<SimilarityResult, SimilarityError> {
    config.validate()?;
    validate_input(&left)?;
    validate_input(&right)?;
    if left.fingerprint.version != right.fingerprint.version {
        return Err(SimilarityError::FingerprintVersionMismatch {
            left: left.fingerprint.version,
            right: right.fingerprint.version,
        });
    }
    if left.fingerprint.frames.is_empty() || right.fingerprint.frames.is_empty() {
        return Err(SimilarityError::EmptyFingerprint);
    }

    let duration_delta_ms = left.fingerprint.duration_ms.abs_diff(right.fingerprint.duration_ms);
    let max_duration_ms = left.fingerprint.duration_ms.max(right.fingerprint.duration_ms);
    let duration_score = 1.0 - (duration_delta_ms as f64 / max_duration_ms as f64);
    let aspect_score = aspect_score(left.aspect_ratio, right.aspect_ratio);
    let rejection = if duration_score < config.min_duration_score {
        Some(PrefilterRejection::DurationMismatch)
    } else if aspect_score.is_some_and(|score| 1.0 - score > config.max_aspect_ratio_delta) {
        Some(PrefilterRejection::AspectMismatch)
    } else {
        None
    };

    if let Some(prefilter_rejection) = rejection {
        let structure_score = structure_score(aspect_score, left.has_audio, right.has_audio);
        return Ok(SimilarityResult {
            classification: SimilarityClassification::PrefilterRejected,
            evidence: SimilarityEvidence {
                algorithm_version: left.fingerprint.version,
                duration_delta_ms,
                duration_score,
                aspect_score,
                structure_score,
                visual_score: None,
                median_hamming_distance: None,
                final_score: 0.0,
                prefilter_rejection: Some(prefilter_rejection),
                frame_distances: Vec::new(),
            },
        });
    }

    let frame_distances = compare_frame_sequences(left.fingerprint, right.fingerprint);
    let median_hamming_distance = median(
        &frame_distances
            .iter()
            .map(|distance| f64::from(distance.hamming_distance))
            .collect::<Vec<_>>(),
    )
    .ok_or(SimilarityError::EmptyFingerprint)?;
    let visual_score = 1.0 - median_hamming_distance / FRAME_HASH_BITS;
    let structure_score = structure_score(aspect_score, left.has_audio, right.has_audio);
    let final_score =
        (0.75 * visual_score + 0.20 * duration_score + 0.05 * structure_score).clamp(0.0, 1.0);
    let classification = if final_score >= config.thresholds.likely_duplicate {
        SimilarityClassification::LikelyDuplicate
    } else if final_score >= config.thresholds.possible_duplicate {
        SimilarityClassification::PossibleDuplicate
    } else {
        SimilarityClassification::NoWarning
    };

    Ok(SimilarityResult {
        classification,
        evidence: SimilarityEvidence {
            algorithm_version: left.fingerprint.version,
            duration_delta_ms,
            duration_score,
            aspect_score,
            structure_score,
            visual_score: Some(visual_score),
            median_hamming_distance: Some(median_hamming_distance),
            final_score,
            prefilter_rejection: None,
            frame_distances,
        },
    })
}

fn validate_input(input: &VideoSimilarityInput<'_>) -> Result<(), SimilarityError> {
    if input.fingerprint.duration_ms == 0 {
        return Err(SimilarityError::InvalidDuration);
    }
    if let Some(aspect_ratio) = input.aspect_ratio
        && (!aspect_ratio.is_finite() || aspect_ratio <= 0.0)
    {
        return Err(SimilarityError::InvalidAspectRatio(aspect_ratio));
    }
    Ok(())
}

fn aspect_score(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(1.0 - (left - right).abs() / left.max(right)),
        _ => None,
    }
}

fn structure_score(
    aspect: Option<f64>,
    left_audio: Option<bool>,
    right_audio: Option<bool>,
) -> f64 {
    let aspect_score = aspect.unwrap_or(0.5);
    let audio_score = match (left_audio, right_audio) {
        (Some(left), Some(right)) if left == right => 1.0,
        (Some(_), Some(_)) => 0.5,
        _ => 0.5,
    };
    0.8 * aspect_score + 0.2 * audio_score
}

fn compare_frame_sequences(
    left: &VideoFingerprint,
    right: &VideoFingerprint,
) -> Vec<FrameDistance> {
    let mut distances = left
        .frames
        .iter()
        .map(|left_frame| {
            let right_frame = right
                .frames
                .iter()
                .min_by_key(|right_frame| left_frame.ratio_bps.abs_diff(right_frame.ratio_bps))
                .expect("right fingerprint is non-empty");
            FrameDistance {
                left_ratio_bps: left_frame.ratio_bps,
                right_ratio_bps: right_frame.ratio_bps,
                hamming_distance: (left_frame.hash ^ right_frame.hash).count_ones() as u8,
            }
        })
        .collect::<Vec<_>>();
    distances.extend(right.frames.iter().map(|right_frame| {
        let left_frame = left
            .frames
            .iter()
            .min_by_key(|left_frame| right_frame.ratio_bps.abs_diff(left_frame.ratio_bps))
            .expect("left fingerprint is non-empty");
        FrameDistance {
            left_ratio_bps: left_frame.ratio_bps,
            right_ratio_bps: right_frame.ratio_bps,
            hamming_distance: (left_frame.hash ^ right_frame.hash).count_ones() as u8,
        }
    }));
    distances
}

fn median(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut values = values.to_vec();
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    if values.len().is_multiple_of(2) {
        Some((values[middle - 1] + values[middle]) / 2.0)
    } else {
        Some(values[middle])
    }
}

#[derive(Debug, Error)]
pub enum SimilarityError {
    #[error("similarity thresholds are invalid")]
    InvalidThresholds,
    #[error("similarity configuration is invalid")]
    InvalidConfig,
    #[error("fingerprint versions do not match: {left:?} versus {right:?}")]
    FingerprintVersionMismatch { left: FingerprintVersion, right: FingerprintVersion },
    #[error("fingerprints must contain at least one frame")]
    EmptyFingerprint,
    #[error("fingerprint duration must be greater than zero")]
    InvalidDuration,
    #[error("aspect ratio must be finite and greater than zero, got {0}")]
    InvalidAspectRatio(f64),
}

#[cfg(test)]
mod tests {
    use crate::FrameFingerprint;

    use super::*;

    fn fingerprint(duration_ms: u64, hashes: &[u64]) -> VideoFingerprint {
        let ratios = [500, 1500, 3000, 5000, 7000, 8500, 9500];
        VideoFingerprint {
            version: FingerprintVersion::FrameDHashV1,
            duration_ms,
            frames: hashes
                .iter()
                .enumerate()
                .map(|(index, hash)| FrameFingerprint {
                    timestamp_ms: duration_ms * u64::from(ratios[index]) / 10_000,
                    ratio_bps: ratios[index],
                    hash: *hash,
                })
                .collect(),
        }
    }

    #[test]
    fn identical_fingerprints_are_likely_duplicates() {
        let fingerprint = fingerprint(10_000, &[0xAA; 7]);
        let result = compare_videos(
            VideoSimilarityInput {
                fingerprint: &fingerprint,
                aspect_ratio: Some(16.0 / 9.0),
                has_audio: Some(true),
            },
            VideoSimilarityInput {
                fingerprint: &fingerprint,
                aspect_ratio: Some(16.0 / 9.0),
                has_audio: Some(true),
            },
            SimilarityConfig::default(),
        )
        .expect("identical fingerprints should compare");
        assert_eq!(result.classification, SimilarityClassification::LikelyDuplicate);
        assert_eq!(result.evidence.final_score, 1.0);
        assert_eq!(result.score_basis_points(), 10_000);
        assert!(
            result.evidence.frame_distances.iter().all(|distance| distance.hamming_distance == 0)
        );
    }

    #[test]
    fn duration_and_aspect_prefilters_reject_unrelated_inputs() {
        let left = fingerprint(10_000, &[0; 7]);
        let right = fingerprint(30_000, &[0; 7]);
        let result = compare_videos(
            VideoSimilarityInput { fingerprint: &left, aspect_ratio: Some(1.0), has_audio: None },
            VideoSimilarityInput { fingerprint: &right, aspect_ratio: Some(2.0), has_audio: None },
            SimilarityConfig::default(),
        )
        .expect("prefilter should return evidence");
        assert_eq!(result.classification, SimilarityClassification::PrefilterRejected);
        assert_eq!(result.evidence.prefilter_rejection, Some(PrefilterRejection::DurationMismatch));
        assert!(result.evidence.frame_distances.is_empty());
    }

    #[test]
    fn calibration_fixture_produces_no_warning_for_opposite_hashes() {
        let left = fingerprint(10_000, &[0; 7]);
        let right = fingerprint(10_000, &[u64::MAX; 7]);
        let result = compare_videos(
            VideoSimilarityInput {
                fingerprint: &left,
                aspect_ratio: Some(1.0),
                has_audio: Some(false),
            },
            VideoSimilarityInput {
                fingerprint: &right,
                aspect_ratio: Some(1.0),
                has_audio: Some(false),
            },
            SimilarityConfig::default(),
        )
        .expect("fixture should compare");
        assert_eq!(result.classification, SimilarityClassification::NoWarning);
        assert_eq!(result.evidence.median_hamming_distance, Some(64.0));
        assert_eq!(result.score_basis_points(), 2_500);
    }

    #[test]
    fn invalid_configuration_and_versions_are_rejected() {
        let fingerprint = fingerprint(1_000, &[0]);
        assert!(matches!(
            SimilarityConfig {
                thresholds: SimilarityThresholds { likely_duplicate: 0.5, possible_duplicate: 0.8 },
                ..Default::default()
            }
            .validate(),
            Err(SimilarityError::InvalidThresholds)
        ));
        assert!(matches!(
            compare_videos(
                VideoSimilarityInput {
                    fingerprint: &fingerprint,
                    aspect_ratio: Some(0.0),
                    has_audio: None
                },
                VideoSimilarityInput {
                    fingerprint: &fingerprint,
                    aspect_ratio: None,
                    has_audio: None
                },
                SimilarityConfig::default(),
            ),
            Err(SimilarityError::InvalidAspectRatio(0.0))
        ));
    }
}
