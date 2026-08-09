use crate::{VideoSequenceFingerprint, VideoSequenceSample};
use thiserror::Error;

const MAX_DISTANCE_BPS: i32 = 10_000;
const INFORMATIVE_DISTANCE_LIMIT_BPS: u16 = 3_500;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SequenceAlignmentConfig {
    pub min_incoming_coverage_bps: u16,
    pub min_candidate_coverage_bps: u16,
    pub min_informative_matches: u16,
    pub max_median_distance_bps: u16,
    pub max_high_percentile_distance_bps: u16,
    pub min_longest_run: u16,
    pub max_gap_count: u16,
    pub min_score_bps: u16,
    pub gap_penalty_bps: u16,
    pub max_cells: usize,
}

impl Default for SequenceAlignmentConfig {
    fn default() -> Self {
        Self {
            min_incoming_coverage_bps: 7_000,
            min_candidate_coverage_bps: 7_000,
            min_informative_matches: 8,
            max_median_distance_bps: 2_200,
            max_high_percentile_distance_bps: 3_500,
            min_longest_run: 6,
            max_gap_count: 8,
            min_score_bps: 6_500,
            gap_penalty_bps: 1_200,
            // (2_048 + 1)^2, with a small amount of room for the DP border.
            max_cells: 4_200_000,
        }
    }
}

impl SequenceAlignmentConfig {
    pub fn validate(self) -> Result<(), SequenceAlignmentError> {
        if self.min_incoming_coverage_bps > 10_000
            || self.min_candidate_coverage_bps > 10_000
            || self.min_informative_matches == 0
            || self.max_median_distance_bps > 10_000
            || self.max_high_percentile_distance_bps > 10_000
            || self.min_longest_run == 0
            || self.min_score_bps > 10_000
            || self.gap_penalty_bps == 0
            || self.max_cells == 0
        {
            return Err(SequenceAlignmentError::InvalidConfig);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SequenceClassification {
    StrongDuplicate,
    PartialMatch,
    NotDuplicate,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct SequenceEvidence {
    pub algorithm_version: crate::FingerprintVersion,
    pub aligned_offset_ms: i64,
    pub informative_matched_samples: u16,
    pub incoming_coverage_bps: u16,
    pub candidate_coverage_bps: u16,
    pub median_distance_bps: u16,
    pub high_percentile_distance_bps: u16,
    pub longest_temporally_consistent_run: u16,
    pub unmatched_incoming_prefix: u16,
    pub unmatched_incoming_suffix: u16,
    pub unmatched_candidate_prefix: u16,
    pub unmatched_candidate_suffix: u16,
    pub gap_count: u16,
    pub score_bps: u16,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct SequenceAlignment {
    pub classification: SequenceClassification,
    pub evidence: SequenceEvidence,
}

pub fn align_video_sequences(
    incoming: &VideoSequenceFingerprint,
    candidate: &VideoSequenceFingerprint,
    config: SequenceAlignmentConfig,
) -> Result<SequenceAlignment, SequenceAlignmentError> {
    config.validate()?;
    if incoming.version != candidate.version {
        return Err(SequenceAlignmentError::FingerprintVersionMismatch);
    }
    if incoming.samples.is_empty() || candidate.samples.is_empty() {
        return Err(SequenceAlignmentError::EmptySequence);
    }
    let cells = (incoming.samples.len() + 1)
        .checked_mul(candidate.samples.len() + 1)
        .ok_or(SequenceAlignmentError::TooManyCells)?;
    if cells > config.max_cells {
        return Err(SequenceAlignmentError::TooManyCells);
    }

    let columns = candidate.samples.len() + 1;
    let mut scores = vec![0_i32; cells];
    let mut directions = vec![0_u8; cells];
    let mut best = (0_usize, 0_usize, 0_i32);
    let gap_penalty = i32::from(config.gap_penalty_bps);
    for incoming_index in 1..=incoming.samples.len() {
        for candidate_index in 1..=candidate.samples.len() {
            let index = incoming_index * columns + candidate_index;
            let diagonal_index = (incoming_index - 1) * columns + candidate_index - 1;
            let up_index = (incoming_index - 1) * columns + candidate_index;
            let left_index = incoming_index * columns + candidate_index - 1;
            let diagonal = scores[diagonal_index]
                + pair_score(
                    incoming.samples[incoming_index - 1],
                    candidate.samples[candidate_index - 1],
                );
            let up = scores[up_index] - gap_penalty;
            let left = scores[left_index] - gap_penalty;
            let (score, direction) = if diagonal > 0 && diagonal >= up && diagonal >= left {
                (diagonal, 1)
            } else if up > 0 && up >= left {
                (up, 2)
            } else if left > 0 {
                (left, 3)
            } else {
                (0, 0)
            };
            scores[index] = score;
            directions[index] = direction;
            if score > best.2 {
                best = (incoming_index, candidate_index, score);
            }
        }
    }

    let path = backtrack(&directions, &scores, columns, best.0, best.1, incoming, candidate);
    let evidence = evidence_from_path(incoming, candidate, path, best.2);
    let classification = classify(&evidence, config);
    Ok(SequenceAlignment { classification, evidence })
}

#[derive(Debug, Clone, Copy)]
enum PathStep {
    Match { incoming_index: usize, candidate_index: usize, distance_bps: u16 },
    Gap,
}

fn backtrack(
    directions: &[u8],
    scores: &[i32],
    columns: usize,
    mut incoming_index: usize,
    mut candidate_index: usize,
    incoming: &VideoSequenceFingerprint,
    candidate: &VideoSequenceFingerprint,
) -> Vec<PathStep> {
    let mut path = Vec::new();
    while incoming_index > 0 && candidate_index > 0 {
        let index = incoming_index * columns + candidate_index;
        if scores[index] == 0 {
            break;
        }
        match directions[index] {
            1 => {
                let left = incoming.samples[incoming_index - 1];
                let right = candidate.samples[candidate_index - 1];
                path.push(PathStep::Match {
                    incoming_index: incoming_index - 1,
                    candidate_index: candidate_index - 1,
                    distance_bps: pair_distance_bps(left, right),
                });
                incoming_index -= 1;
                candidate_index -= 1;
            }
            2 => {
                path.push(PathStep::Gap);
                incoming_index -= 1;
            }
            3 => {
                path.push(PathStep::Gap);
                candidate_index -= 1;
            }
            _ => break,
        }
    }
    path.reverse();
    path
}

fn evidence_from_path(
    incoming: &VideoSequenceFingerprint,
    candidate: &VideoSequenceFingerprint,
    path: Vec<PathStep>,
    alignment_score: i32,
) -> SequenceEvidence {
    let matches = path
        .iter()
        .filter_map(|step| match step {
            PathStep::Match { incoming_index, candidate_index, distance_bps } => {
                Some((*incoming_index, *candidate_index, *distance_bps))
            }
            PathStep::Gap => None,
        })
        .collect::<Vec<_>>();
    let informative = matches
        .iter()
        .filter(|(incoming_index, candidate_index, distance_bps)| {
            incoming.samples[*incoming_index].information_bps
                >= crate::video_sequence::VIDEO_SEQUENCE_INFO_THRESHOLD_BPS
                && candidate.samples[*candidate_index].information_bps
                    >= crate::video_sequence::VIDEO_SEQUENCE_INFO_THRESHOLD_BPS
                && *distance_bps <= 8_000
        })
        .collect::<Vec<_>>();
    let distances = informative.iter().map(|(_, _, distance)| *distance).collect::<Vec<_>>();
    let median_distance_bps = percentile(&distances, 50);
    let high_percentile_distance_bps = percentile(&distances, 95);
    let first = matches.first().copied();
    let last = matches.last().copied();
    let aligned_offset_ms = first
        .map(|(incoming_index, candidate_index, _)| {
            i64::try_from(candidate.sample_timestamp_ms(candidate_index).unwrap_or(0))
                .unwrap_or(i64::MAX)
                - i64::try_from(incoming.sample_timestamp_ms(incoming_index).unwrap_or(0))
                    .unwrap_or(i64::MAX)
        })
        .unwrap_or(0);
    let incoming_coverage_bps = basis_points(matches.len(), incoming.samples.len());
    let candidate_coverage_bps = basis_points(matches.len(), candidate.samples.len());
    let longest_run = longest_run(&matches);
    let gap_count = path
        .iter()
        .enumerate()
        .filter(|(index, step)| {
            matches!(step, PathStep::Gap)
                && (*index == 0 || !matches!(path[*index - 1], PathStep::Gap))
        })
        .count() as u16;
    let score_bps = alignment_score_bps(
        alignment_score,
        informative.len(),
        median_distance_bps,
        incoming_coverage_bps.min(candidate_coverage_bps),
    );
    SequenceEvidence {
        algorithm_version: incoming.version,
        aligned_offset_ms,
        informative_matched_samples: informative.len().min(u16::MAX as usize) as u16,
        incoming_coverage_bps,
        candidate_coverage_bps,
        median_distance_bps: median_distance_bps.unwrap_or(10_000),
        high_percentile_distance_bps: high_percentile_distance_bps.unwrap_or(10_000),
        longest_temporally_consistent_run: longest_run,
        unmatched_incoming_prefix: first
            .map(|(index, _, _)| index.min(u16::MAX as usize) as u16)
            .unwrap_or(incoming.samples.len().min(u16::MAX as usize) as u16),
        unmatched_incoming_suffix: last
            .map(|(index, _, _)| {
                incoming.samples.len().saturating_sub(index + 1).min(u16::MAX as usize) as u16
            })
            .unwrap_or(incoming.samples.len().min(u16::MAX as usize) as u16),
        unmatched_candidate_prefix: first
            .map(|(_, index, _)| index.min(u16::MAX as usize) as u16)
            .unwrap_or(candidate.samples.len().min(u16::MAX as usize) as u16),
        unmatched_candidate_suffix: last
            .map(|(_, index, _)| {
                candidate.samples.len().saturating_sub(index + 1).min(u16::MAX as usize) as u16
            })
            .unwrap_or(candidate.samples.len().min(u16::MAX as usize) as u16),
        gap_count,
        score_bps,
    }
}

fn classify(
    evidence: &SequenceEvidence,
    config: SequenceAlignmentConfig,
) -> SequenceClassification {
    let strong = evidence.informative_matched_samples >= config.min_informative_matches
        && evidence.incoming_coverage_bps >= config.min_incoming_coverage_bps
        && evidence.candidate_coverage_bps >= config.min_candidate_coverage_bps
        && evidence.median_distance_bps <= config.max_median_distance_bps
        && evidence.high_percentile_distance_bps <= config.max_high_percentile_distance_bps
        && evidence.longest_temporally_consistent_run >= config.min_longest_run
        && evidence.gap_count <= config.max_gap_count
        && evidence.score_bps >= config.min_score_bps;
    if strong {
        SequenceClassification::StrongDuplicate
    } else if evidence.informative_matched_samples > 0 {
        SequenceClassification::PartialMatch
    } else {
        SequenceClassification::NotDuplicate
    }
}

fn pair_score(left: VideoSequenceSample, right: VideoSequenceSample) -> i32 {
    let distance = pair_distance_bps(left, right);
    let information = u32::from(left.information_bps.min(right.information_bps));
    let weight_bps = 1_000_u32.saturating_add(information.saturating_mul(9_000) / 10_000);
    (u32::from(MAX_DISTANCE_BPS as u16 - distance).saturating_mul(weight_bps).saturating_div(10_000)
        as i32)
        - 4_500
}

fn pair_distance_bps(left: VideoSequenceSample, right: VideoSequenceSample) -> u16 {
    let phash = (left.phash ^ right.phash).count_ones() * 10_000 / 64;
    let dhash = (left.dhash ^ right.dhash).count_ones() * 10_000 / 64;
    let luma = u32::from(left.mean_luma.abs_diff(right.mean_luma)) * 10_000 / 255;
    let chroma_u =
        u32::from((left.mean_chroma_u as i16 - right.mean_chroma_u as i16).unsigned_abs()) * 10_000
            / 255;
    let chroma_v =
        u32::from((left.mean_chroma_v as i16 - right.mean_chroma_v as i16).unsigned_abs()) * 10_000
            / 255;
    (phash
        .saturating_mul(4)
        .saturating_add(dhash.saturating_mul(4))
        .saturating_add((luma + chroma_u + chroma_v) / 3 * 2)
        / 10)
        .min(10_000) as u16
}

fn basis_points(numerator: usize, denominator: usize) -> u16 {
    if denominator == 0 {
        return 0;
    }
    (numerator.min(denominator) as u64 * 10_000 / denominator as u64).min(10_000) as u16
}

fn percentile(values: &[u16], percentile: usize) -> Option<u16> {
    if values.is_empty() {
        return None;
    }
    let mut values = values.to_vec();
    values.sort_unstable();
    let index = ((values.len() - 1) * percentile).div_ceil(100);
    values.get(index).copied()
}

fn longest_run(matches: &[(usize, usize, u16)]) -> u16 {
    let mut longest = 0_usize;
    let mut current = 0_usize;
    for window in matches.windows(2) {
        let (left_incoming, left_candidate, left_distance) = window[0];
        let (right_incoming, right_candidate, right_distance) = window[1];
        if right_incoming == left_incoming + 1
            && right_candidate == left_candidate + 1
            && left_distance <= INFORMATIVE_DISTANCE_LIMIT_BPS
            && right_distance <= INFORMATIVE_DISTANCE_LIMIT_BPS
        {
            current += 1;
        } else {
            current = 0;
        }
        longest = longest.max(current);
    }
    if !matches.is_empty() {
        longest += 1;
    }
    longest.min(u16::MAX as usize) as u16
}

fn alignment_score_bps(
    alignment_score: i32,
    informative_count: usize,
    median_distance_bps: Option<u16>,
    coverage_bps: u16,
) -> u16 {
    if informative_count == 0 || alignment_score <= 0 {
        return 0;
    }
    let quality = 10_000_u32.saturating_sub(u32::from(median_distance_bps.unwrap_or(10_000)));
    let score = (u32::try_from(alignment_score).unwrap_or(0).min(10_000) * 4 / 10)
        .saturating_add(quality * 4 / 10)
        .saturating_add(u32::from(coverage_bps) * 2 / 10);
    score.min(10_000) as u16
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Error)]
pub enum SequenceAlignmentError {
    #[error("sequence alignment configuration is invalid")]
    InvalidConfig,
    #[error("video sequence fingerprint versions do not match")]
    FingerprintVersionMismatch,
    #[error("video sequences must not be empty")]
    EmptySequence,
    #[error("video sequence alignment exceeds the configured cell bound")]
    TooManyCells,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FingerprintVersion, VideoSequenceSample};

    fn sample(seed: u64) -> VideoSequenceSample {
        VideoSequenceSample {
            phash: seed,
            dhash: seed.rotate_left(17),
            mean_luma: 100,
            mean_chroma_u: 4,
            mean_chroma_v: -6,
            information_bps: 8_000,
            transition_bps: 0,
        }
    }

    fn sequence(seeds: &[u64], duration_ms: u64) -> VideoSequenceFingerprint {
        VideoSequenceFingerprint::new(duration_ms, 500, seeds.iter().copied().map(sample).collect())
            .expect("test sequence should be valid")
    }

    #[test]
    fn alignment_accepts_a_short_blank_prefix() {
        let incoming = sequence(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 5_000);
        let candidate = sequence(&[0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 6_000);
        let result =
            align_video_sequences(&incoming, &candidate, SequenceAlignmentConfig::default())
                .expect("sequences should align");
        assert_eq!(result.classification, SequenceClassification::StrongDuplicate);
        assert_eq!(result.evidence.aligned_offset_ms, 1_000);
        assert!(result.evidence.incoming_coverage_bps >= 9_000);
        assert!(result.evidence.candidate_coverage_bps >= 8_000);
    }

    #[test]
    fn contained_clip_is_not_a_full_duplicate() {
        let incoming = sequence(&[3, 4, 5, 6], 2_000);
        let candidate = sequence(&(0..20).map(|value| value as u64).collect::<Vec<_>>(), 10_000);
        let result =
            align_video_sequences(&incoming, &candidate, SequenceAlignmentConfig::default())
                .expect("sequences should align");
        assert_ne!(result.classification, SequenceClassification::StrongDuplicate);
        assert!(result.evidence.candidate_coverage_bps < 7_000);
    }

    #[test]
    fn low_information_frames_cannot_create_a_duplicate() {
        let mut left = vec![sample(1); 10];
        let mut right = vec![sample(1); 10];
        left.iter_mut().for_each(|sample| sample.information_bps = 0);
        right.iter_mut().for_each(|sample| sample.information_bps = 0);
        let left = VideoSequenceFingerprint::new(5_000, 500, left).unwrap();
        let right = VideoSequenceFingerprint::new(5_000, 500, right).unwrap();
        let result =
            align_video_sequences(&left, &right, SequenceAlignmentConfig::default()).unwrap();
        assert_eq!(result.classification, SequenceClassification::NotDuplicate);
        assert_eq!(result.evidence.informative_matched_samples, 0);
    }

    #[test]
    fn version_mismatch_is_rejected() {
        let left = sequence(&[1, 2, 3], 1_500);
        let mut right = sequence(&[1, 2, 3], 1_500);
        right.version = FingerprintVersion::FrameDHashV1;
        assert_eq!(
            align_video_sequences(&left, &right, SequenceAlignmentConfig::default()),
            Err(SequenceAlignmentError::FingerprintVersionMismatch)
        );
    }
}
