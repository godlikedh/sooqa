use crate::{VideoSequenceFingerprint, VideoSequenceSample};
use thiserror::Error;

const MAX_DISTANCE_BPS: i32 = 10_000;
const INFORMATIVE_DISTANCE_LIMIT_BPS: u16 = 3_500;
const MATCH_BASE_WEIGHT_BPS: u32 = 4_600;
const MATCH_INFORMATION_WEIGHT_BPS: u32 = 5_400;
const MATCH_INFORMATION_SPAN_BPS: u32 = 9_000;

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
    let weight_bps = if information
        < u32::from(crate::video_sequence::VIDEO_SEQUENCE_INFO_THRESHOLD_BPS)
    {
        0
    } else {
        MATCH_BASE_WEIGHT_BPS
            + information
                .saturating_sub(u32::from(crate::video_sequence::VIDEO_SEQUENCE_INFO_THRESHOLD_BPS))
                .saturating_mul(MATCH_INFORMATION_WEIGHT_BPS)
                / MATCH_INFORMATION_SPAN_BPS
    };
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
    use std::{path::Path, process::Command, time::Duration};

    use image::{DynamicImage, ImageBuffer, Rgb};
    use uuid::Uuid;

    use super::*;
    use crate::{FrameExtractor, MediaWorkspace, VideoSequenceSample, WorkspaceArea};

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

    fn feature_images(seeds: &[u8], transpose: bool) -> Vec<DynamicImage> {
        seeds
            .iter()
            .map(|seed| {
                DynamicImage::ImageRgb8(ImageBuffer::from_fn(64, 64, |x, y| {
                    let x = x as u16;
                    let y = y as u16;
                    let (horizontal, vertical) = if transpose { (y, x) } else { (x, y) };
                    let seed = u16::from(*seed);
                    Rgb([
                        (horizontal * 3 + seed * 5) as u8,
                        (vertical * 2 + seed * 3 + 40) as u8,
                        (80 + seed * 2) as u8,
                    ])
                }))
            })
            .collect()
    }

    fn feature_sequence(seeds: &[u8], transpose: bool) -> VideoSequenceFingerprint {
        let images = feature_images(seeds, transpose);
        VideoSequenceFingerprint::from_images(seeds.len() as u64 * 500, 500, &images)
            .expect("feature images should produce a sequence")
    }

    fn black_image() -> DynamicImage {
        DynamicImage::ImageRgb8(ImageBuffer::from_pixel(64, 64, Rgb([0, 0, 0])))
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
    fn extracted_features_align_with_themselves_and_a_blank_prefix() {
        let seeds = (0_u8..10).collect::<Vec<_>>();
        let incoming = feature_sequence(&seeds, false);
        assert!(incoming.samples.iter().all(|sample| {
            sample.information_bps >= crate::video_sequence::VIDEO_SEQUENCE_INFO_THRESHOLD_BPS
        }));

        let same = align_video_sequences(&incoming, &incoming, SequenceAlignmentConfig::default())
            .expect("a fingerprint should align with itself");
        assert_eq!(same.classification, SequenceClassification::StrongDuplicate);

        let mut prefixed_images = vec![black_image(), black_image()];
        prefixed_images.extend(feature_images(&seeds, false));
        let prefixed = VideoSequenceFingerprint::from_images(6_000, 500, &prefixed_images)
            .expect("prefixed feature images should produce a sequence");
        let result =
            align_video_sequences(&incoming, &prefixed, SequenceAlignmentConfig::default())
                .expect("a blank prefix should still align");
        assert_eq!(result.classification, SequenceClassification::StrongDuplicate);
        assert_eq!(result.evidence.aligned_offset_ms, 1_000);
    }

    #[test]
    fn extracted_unrelated_and_low_information_sequences_are_not_strong() {
        let seeds = (0_u8..10).collect::<Vec<_>>();
        let incoming = feature_sequence(&seeds, false);
        let unrelated = feature_sequence(&seeds, true);
        let unrelated_result =
            align_video_sequences(&incoming, &unrelated, SequenceAlignmentConfig::default())
                .expect("unrelated feature sequences should be comparable");
        assert_ne!(unrelated_result.classification, SequenceClassification::StrongDuplicate);

        let black_images = (0..10).map(|_| black_image()).collect::<Vec<_>>();
        let low_information = VideoSequenceFingerprint::from_images(5_000, 500, &black_images)
            .expect("black images should produce a sequence");
        let low_information_result = align_video_sequences(
            &low_information,
            &low_information,
            SequenceAlignmentConfig::default(),
        )
        .expect("low-information sequences should be comparable");
        assert_ne!(low_information_result.classification, SequenceClassification::StrongDuplicate);
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

    #[tokio::test]
    #[ignore = "requires ffmpeg; calibrates the active extractor against generated media"]
    async fn generated_media_identity_acceptance_matrix() {
        let root = std::env::temp_dir().join(format!("sooqa-media-acceptance-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&root).await.expect("fixture root should be creatable");

        let reference_workspace = MediaWorkspace::create(&root, Uuid::new_v4())
            .await
            .expect("reference workspace should be creatable");
        let reference_path = reference_workspace
            .path(WorkspaceArea::Normalized, "canonical.mp4")
            .expect("reference path should be safe");
        render_reference(&reference_path);

        let extractor = FrameExtractor::new("ffmpeg", Duration::from_secs(30));
        let reference = extractor
            .extract_video_sequence_from_area(
                &reference_workspace,
                WorkspaceArea::Normalized,
                "canonical.mp4",
                8_000,
            )
            .await
            .expect("reference should be fingerprintable");

        let cases = [
            ("ordinary_reencode_bitrate_resolution", 8_000, true),
            ("black_prefix", 9_000, true),
            ("short_prefix_suffix_trim", 7_000, true),
            ("unrelated_similar_shape", 8_000, false),
            ("black", 8_000, false),
            ("static", 8_000, false),
            ("repetitive", 8_000, false),
            ("contained_clip", 2_000, false),
            ("very_short", 750, false),
            ("no_audio", 8_000, true),
        ];

        for (name, duration_ms, expected_strong) in cases {
            let workspace = MediaWorkspace::create(&root, Uuid::new_v4())
                .await
                .expect("case workspace should be creatable");
            let path = workspace
                .path(WorkspaceArea::Normalized, "canonical.mp4")
                .expect("case path should be safe");
            render_case(name, &path, &reference_path);
            let fingerprint = extractor
                .extract_video_sequence_from_area(
                    &workspace,
                    WorkspaceArea::Normalized,
                    "canonical.mp4",
                    duration_ms,
                )
                .await
                .unwrap_or_else(|error| panic!("{name} should be fingerprintable: {error}"));
            let alignment =
                align_video_sequences(&reference, &fingerprint, SequenceAlignmentConfig::default())
                    .unwrap_or_else(|error| panic!("{name} should be comparable: {error}"));
            println!(
                "{name}: {:?}, incoming={}bps candidate={}bps informative={} run={} score={}",
                alignment.classification,
                alignment.evidence.incoming_coverage_bps,
                alignment.evidence.candidate_coverage_bps,
                alignment.evidence.informative_matched_samples,
                alignment.evidence.longest_temporally_consistent_run,
                alignment.evidence.score_bps,
            );
            if expected_strong {
                assert_eq!(
                    alignment.classification,
                    SequenceClassification::StrongDuplicate,
                    "{name} should be accepted as a strong duplicate",
                );
            } else {
                assert_ne!(
                    alignment.classification,
                    SequenceClassification::StrongDuplicate,
                    "{name} should not be accepted as a strong duplicate",
                );
            }
        }

        tokio::fs::remove_dir_all(root).await.expect("fixture root should be removable");
    }

    fn render_reference(path: &Path) {
        run_ffmpeg([
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=320x240:rate=24",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:sample_rate=48000",
            "-t",
            "8",
            "-map",
            "0:v:0",
            "-map",
            "1:a:0",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-crf",
            "18",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            "-shortest",
            path.to_str().expect("fixture path should be UTF-8"),
        ]);
    }

    fn render_case(name: &str, path: &Path, reference: &Path) {
        let output = path.to_str().expect("fixture path should be UTF-8");
        let reference = reference.to_str().expect("fixture path should be UTF-8");
        match name {
            "ordinary_reencode_bitrate_resolution" => run_ffmpeg([
                "-y",
                "-hide_banner",
                "-loglevel",
                "error",
                "-i",
                reference,
                "-vf",
                "scale=640:480",
                "-map",
                "0:v:0",
                "-an",
                "-c:v",
                "libx264",
                "-preset",
                "ultrafast",
                "-b:v",
                "350k",
                "-pix_fmt",
                "yuv420p",
                output,
            ]),
            "black_prefix" => run_ffmpeg([
                "-y",
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "color=c=black:s=320x240:r=24:d=1",
                "-i",
                reference,
                "-filter_complex",
                "[0:v:0][1:v:0]concat=n=2:v=1:a=0[v]",
                "-map",
                "[v]",
                "-an",
                "-c:v",
                "libx264",
                "-preset",
                "ultrafast",
                "-crf",
                "18",
                "-pix_fmt",
                "yuv420p",
                output,
            ]),
            "short_prefix_suffix_trim" => render_trimmed(reference, output, "0.5", "7"),
            "unrelated_similar_shape" => render_lavfi("testsrc=size=320x240:rate=24", output, "8"),
            "black" => render_lavfi("color=c=black:s=320x240:r=24", output, "8"),
            "static" => render_lavfi("color=c=blue:s=320x240:r=24", output, "8"),
            "repetitive" => render_lavfi("smptebars=size=320x240:rate=24", output, "8"),
            "contained_clip" => render_trimmed(reference, output, "3", "2"),
            "very_short" => render_trimmed(reference, output, "3", "0.75"),
            "no_audio" => run_ffmpeg([
                "-y",
                "-hide_banner",
                "-loglevel",
                "error",
                "-i",
                reference,
                "-map",
                "0:v:0",
                "-an",
                "-c:v",
                "libx264",
                "-preset",
                "ultrafast",
                "-crf",
                "18",
                "-pix_fmt",
                "yuv420p",
                output,
            ]),
            _ => panic!("unknown media fixture {name}"),
        }
    }

    fn render_lavfi(filter: &str, output: &str, duration: &str) {
        run_ffmpeg([
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            filter,
            "-t",
            duration,
            "-an",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-crf",
            "18",
            "-pix_fmt",
            "yuv420p",
            output,
        ]);
    }

    fn render_trimmed(reference: &str, output: &str, start: &str, duration: &str) {
        run_ffmpeg([
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-ss",
            start,
            "-i",
            reference,
            "-t",
            duration,
            "-map",
            "0:v:0",
            "-an",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-crf",
            "18",
            "-pix_fmt",
            "yuv420p",
            output,
        ]);
    }

    fn run_ffmpeg<const N: usize>(args: [&str; N]) {
        let output = Command::new("ffmpeg")
            .args(args)
            .output()
            .expect("ffmpeg should be installed for media acceptance tests");
        assert!(
            output.status.success(),
            "ffmpeg fixture command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
