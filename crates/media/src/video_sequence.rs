use std::sync::OnceLock;

use image::{DynamicImage, imageops::FilterType};
use thiserror::Error;

use crate::fingerprint::FingerprintVersion;

pub const VIDEO_SEQUENCE_MAGIC: [u8; 4] = *b"SQVS";
pub const VIDEO_SEQUENCE_CODEC_V1: u16 = 1;
pub const VIDEO_SEQUENCE_BASE_INTERVAL_MS: u64 = 500;
pub const VIDEO_SEQUENCE_MAX_SAMPLES: usize = 2_048;
pub const VIDEO_SEQUENCE_MAX_ANCHORS: usize = 128;
pub const VIDEO_SEQUENCE_MAX_TOKENS: usize = 1_024;
pub const VIDEO_SEQUENCE_INFO_THRESHOLD_BPS: u16 = 1_000;

const MAX_SCORE_BPS: u16 = 10_000;
const HEADER_BYTES: usize = 24;
const SAMPLE_BYTES: usize = 23;
const FEATURE_WIDTH: u32 = 32;
const FEATURE_HEIGHT: u32 = 32;
const PHASH_DCT_SIZE: usize = 32;
const PHASH_LOW_FREQUENCY_SIZE: usize = 8;
const PHASH_COEFFICIENT_COUNT: usize = PHASH_LOW_FREQUENCY_SIZE * PHASH_LOW_FREQUENCY_SIZE;
const MAX_ENCODED_BYTES: usize = HEADER_BYTES + VIDEO_SEQUENCE_MAX_SAMPLES * SAMPLE_BYTES;

static PHASH_BASIS: OnceLock<[[f64; PHASH_DCT_SIZE]; PHASH_LOW_FREQUENCY_SIZE]> = OnceLock::new();

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct VideoSequenceSample {
    pub phash: u64,
    pub dhash: u64,
    pub mean_luma: u8,
    pub mean_chroma_u: i8,
    pub mean_chroma_v: i8,
    pub information_bps: u16,
    pub transition_bps: u16,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct VideoSequenceFingerprint {
    pub version: FingerprintVersion,
    pub duration_ms: u64,
    pub interval_ms: u32,
    pub samples: Vec<VideoSequenceSample>,
}

impl VideoSequenceFingerprint {
    pub fn new(
        duration_ms: u64,
        interval_ms: u32,
        samples: Vec<VideoSequenceSample>,
    ) -> Result<Self, VideoSequenceError> {
        validate_grid(duration_ms, interval_ms, samples.len())?;
        validate_samples(&samples)?;
        Ok(Self { version: FingerprintVersion::VideoSequenceV1, duration_ms, interval_ms, samples })
    }

    pub fn from_images(
        duration_ms: u64,
        interval_ms: u32,
        images: &[DynamicImage],
    ) -> Result<Self, VideoSequenceError> {
        let mut samples = Vec::with_capacity(images.len());
        let mut previous_luma = None;
        for image in images {
            let normalized = normalize_image(image);
            let sample = sample_features(&normalized, previous_luma.as_deref());
            previous_luma = Some(normalized.luma);
            samples.push(sample);
        }
        Self::new(duration_ms, interval_ms, samples)
    }

    pub fn encode(&self) -> Result<Vec<u8>, VideoSequenceError> {
        if self.version != FingerprintVersion::VideoSequenceV1 {
            return Err(VideoSequenceError::UnsupportedAlgorithm(self.version));
        }
        validate_grid(self.duration_ms, self.interval_ms, self.samples.len())?;
        validate_samples(&self.samples)?;
        let mut encoded = Vec::with_capacity(HEADER_BYTES + self.samples.len() * SAMPLE_BYTES);
        encoded.extend_from_slice(&VIDEO_SEQUENCE_MAGIC);
        encoded.extend_from_slice(&VIDEO_SEQUENCE_CODEC_V1.to_le_bytes());
        encoded.extend_from_slice(&(self.samples.len() as u16).to_le_bytes());
        encoded.extend_from_slice(&self.duration_ms.to_le_bytes());
        encoded.extend_from_slice(&self.interval_ms.to_le_bytes());
        encoded.extend_from_slice(&0_u32.to_le_bytes());
        for sample in &self.samples {
            encoded.extend_from_slice(&sample.phash.to_le_bytes());
            encoded.extend_from_slice(&sample.dhash.to_le_bytes());
            encoded.push(sample.mean_luma);
            encoded.push(sample.mean_chroma_u as u8);
            encoded.push(sample.mean_chroma_v as u8);
            encoded.extend_from_slice(&sample.information_bps.to_le_bytes());
            encoded.extend_from_slice(&sample.transition_bps.to_le_bytes());
        }
        debug_assert_eq!(encoded.len(), HEADER_BYTES + self.samples.len() * SAMPLE_BYTES);
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, VideoSequenceError> {
        if encoded.len() > MAX_ENCODED_BYTES {
            return Err(VideoSequenceError::EncodedTooLarge { max: MAX_ENCODED_BYTES });
        }
        if encoded.len() < HEADER_BYTES {
            return Err(VideoSequenceError::Truncated);
        }
        if encoded[..4] != VIDEO_SEQUENCE_MAGIC {
            return Err(VideoSequenceError::InvalidMagic);
        }
        let codec_version = u16::from_le_bytes([encoded[4], encoded[5]]);
        if codec_version != VIDEO_SEQUENCE_CODEC_V1 {
            return Err(VideoSequenceError::UnsupportedCodecVersion(codec_version));
        }
        let sample_count = usize::from(u16::from_le_bytes([encoded[6], encoded[7]]));
        if sample_count == 0 || sample_count > VIDEO_SEQUENCE_MAX_SAMPLES {
            return Err(VideoSequenceError::InvalidSampleCount(sample_count));
        }
        let mut duration_bytes = [0_u8; 8];
        duration_bytes.copy_from_slice(&encoded[8..16]);
        let duration_ms = u64::from_le_bytes(duration_bytes);
        let mut interval_bytes = [0_u8; 4];
        interval_bytes.copy_from_slice(&encoded[16..20]);
        let interval_ms = u32::from_le_bytes(interval_bytes);
        let reserved = u32::from_le_bytes([encoded[20], encoded[21], encoded[22], encoded[23]]);
        if reserved != 0 {
            return Err(VideoSequenceError::InvalidReservedHeader);
        }
        let expected_len = HEADER_BYTES
            .checked_add(
                sample_count
                    .checked_mul(SAMPLE_BYTES)
                    .ok_or(VideoSequenceError::EncodedTooLarge { max: MAX_ENCODED_BYTES })?,
            )
            .ok_or(VideoSequenceError::EncodedTooLarge { max: MAX_ENCODED_BYTES })?;
        if encoded.len() < expected_len {
            return Err(VideoSequenceError::Truncated);
        }
        if encoded.len() > expected_len {
            return Err(VideoSequenceError::TrailingBytes);
        }
        let mut samples = Vec::with_capacity(sample_count);
        let mut offset = HEADER_BYTES;
        for _ in 0..sample_count {
            let phash = read_u64(encoded, &mut offset)?;
            let dhash = read_u64(encoded, &mut offset)?;
            let mean_luma = read_u8(encoded, &mut offset)?;
            let mean_chroma_u = read_u8(encoded, &mut offset)? as i8;
            let mean_chroma_v = read_u8(encoded, &mut offset)? as i8;
            let information_bps = read_u16(encoded, &mut offset)?;
            let transition_bps = read_u16(encoded, &mut offset)?;
            samples.push(VideoSequenceSample {
                phash,
                dhash,
                mean_luma,
                mean_chroma_u,
                mean_chroma_v,
                information_bps,
                transition_bps,
            });
        }
        Self::new(duration_ms, interval_ms, samples)
    }

    pub fn search_tokens(&self) -> Vec<i64> {
        derive_search_tokens(self)
    }

    pub fn sample_timestamp_ms(&self, index: usize) -> Option<u64> {
        if index >= self.samples.len() {
            return None;
        }
        Some(u64::from(self.interval_ms) * index as u64)
    }
}

pub fn select_video_sequence_timestamps(duration_ms: u64) -> Vec<u64> {
    if duration_ms == 0 {
        return Vec::new();
    }
    let interval_ms = VIDEO_SEQUENCE_BASE_INTERVAL_MS
        .max(duration_ms.div_ceil(VIDEO_SEQUENCE_MAX_SAMPLES as u64));
    let mut timestamps = Vec::with_capacity(
        usize::try_from(duration_ms.div_ceil(interval_ms)).unwrap_or(VIDEO_SEQUENCE_MAX_SAMPLES),
    );
    let mut timestamp = 0_u64;
    while timestamp < duration_ms && timestamps.len() < VIDEO_SEQUENCE_MAX_SAMPLES {
        timestamps.push(timestamp);
        timestamp = match timestamp.checked_add(interval_ms) {
            Some(timestamp) => timestamp,
            None => break,
        };
    }
    timestamps
}

pub fn video_sequence_interval_ms(duration_ms: u64) -> Option<u32> {
    if duration_ms == 0 {
        return None;
    }
    let interval_ms = VIDEO_SEQUENCE_BASE_INTERVAL_MS
        .max(duration_ms.div_ceil(VIDEO_SEQUENCE_MAX_SAMPLES as u64));
    u32::try_from(interval_ms).ok()
}

pub fn derive_search_tokens(fingerprint: &VideoSequenceFingerprint) -> Vec<i64> {
    let mut anchors = fingerprint
        .samples
        .iter()
        .enumerate()
        .filter(|(_, sample)| sample.information_bps >= VIDEO_SEQUENCE_INFO_THRESHOLD_BPS)
        .map(|(index, sample)| {
            let rank = u32::from(sample.information_bps)
                .saturating_mul(3)
                .saturating_add(u32::from(sample.transition_bps));
            (index, rank)
        })
        .collect::<Vec<_>>();
    anchors.sort_by(|(left_index, left_rank), (right_index, right_rank)| {
        right_rank.cmp(left_rank).then_with(|| left_index.cmp(right_index))
    });
    anchors.truncate(VIDEO_SEQUENCE_MAX_ANCHORS);
    anchors.sort_unstable_by_key(|(index, _)| *index);

    let mut tokens = Vec::with_capacity(anchors.len().saturating_mul(8));
    for (index, _) in anchors {
        let sample = fingerprint.samples[index];
        append_hash_tokens(&mut tokens, 1, sample.phash);
        append_hash_tokens(&mut tokens, 2, sample.dhash);
    }
    tokens.sort_unstable();
    tokens.dedup();
    tokens.truncate(VIDEO_SEQUENCE_MAX_TOKENS);
    tokens
}

fn append_hash_tokens(tokens: &mut Vec<i64>, hash_kind: u8, hash: u64) {
    for band in 0..4_u8 {
        let value = ((hash >> (u32::from(band) * 16)) & 0xffff) as u16;
        let token = (1_i64 << 56)
            | (i64::from(hash_kind) << 48)
            | (i64::from(band) << 40)
            | i64::from(value);
        tokens.push(token);
    }
}

fn validate_grid(
    duration_ms: u64,
    interval_ms: u32,
    sample_count: usize,
) -> Result<(), VideoSequenceError> {
    if duration_ms == 0 {
        return Err(VideoSequenceError::InvalidDuration);
    }
    if interval_ms == 0 {
        return Err(VideoSequenceError::InvalidInterval);
    }
    let expected_interval_ms = VIDEO_SEQUENCE_BASE_INTERVAL_MS
        .max(duration_ms.div_ceil(VIDEO_SEQUENCE_MAX_SAMPLES as u64));
    if u64::from(interval_ms) != expected_interval_ms {
        return Err(VideoSequenceError::InvalidTimestampGrid);
    }
    if sample_count == 0 || sample_count > VIDEO_SEQUENCE_MAX_SAMPLES {
        return Err(VideoSequenceError::InvalidSampleCount(sample_count));
    }
    let expected_sample_count = usize::try_from(duration_ms.div_ceil(expected_interval_ms))
        .map_err(|_| VideoSequenceError::InvalidTimestampGrid)?;
    if sample_count != expected_sample_count {
        return Err(VideoSequenceError::InvalidTimestampGrid);
    }
    let last_timestamp = u64::from(interval_ms)
        .checked_mul((sample_count - 1) as u64)
        .ok_or(VideoSequenceError::InvalidTimestampGrid)?;
    if last_timestamp >= duration_ms {
        return Err(VideoSequenceError::InvalidTimestampGrid);
    }
    Ok(())
}

fn validate_samples(samples: &[VideoSequenceSample]) -> Result<(), VideoSequenceError> {
    if let Some(first) = samples.first()
        && first.transition_bps != 0
    {
        return Err(VideoSequenceError::InvalidFirstTransition(first.transition_bps));
    }
    for sample in samples {
        if sample.information_bps > MAX_SCORE_BPS {
            return Err(VideoSequenceError::InvalidInformationScore(sample.information_bps));
        }
        if sample.transition_bps > MAX_SCORE_BPS {
            return Err(VideoSequenceError::InvalidTransitionScore(sample.transition_bps));
        }
    }
    Ok(())
}

fn read_u8(encoded: &[u8], offset: &mut usize) -> Result<u8, VideoSequenceError> {
    let value = *encoded.get(*offset).ok_or(VideoSequenceError::Truncated)?;
    *offset += 1;
    Ok(value)
}

fn read_u16(encoded: &[u8], offset: &mut usize) -> Result<u16, VideoSequenceError> {
    let end = offset.checked_add(2).ok_or(VideoSequenceError::Truncated)?;
    let bytes = encoded.get(*offset..end).ok_or(VideoSequenceError::Truncated)?;
    *offset = end;
    Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
}

fn read_u64(encoded: &[u8], offset: &mut usize) -> Result<u64, VideoSequenceError> {
    let end = offset.checked_add(8).ok_or(VideoSequenceError::Truncated)?;
    let bytes = encoded.get(*offset..end).ok_or(VideoSequenceError::Truncated)?;
    *offset = end;
    Ok(u64::from_le_bytes(bytes.try_into().expect("slice is eight bytes")))
}

#[derive(Debug, Clone)]
struct NormalizedImage {
    luma: Vec<u8>,
    rgb: Vec<[u8; 3]>,
}

fn normalize_image(image: &DynamicImage) -> NormalizedImage {
    let image = image.resize_exact(FEATURE_WIDTH, FEATURE_HEIGHT, FilterType::Triangle).to_rgb8();
    let rgb = image.pixels().map(|pixel| pixel.0).collect::<Vec<_>>();
    let luma = rgb.iter().map(luma).collect::<Vec<_>>();
    NormalizedImage { luma, rgb }
}

fn sample_features(image: &NormalizedImage, previous_luma: Option<&[u8]>) -> VideoSequenceSample {
    let phash = phash_v1(&image.luma);
    let dhash = dhash_v1(&image.luma);
    let (mean_luma, mean_chroma_u, mean_chroma_v) = color_summary(&image.rgb, &image.luma);
    let information_bps = information_score(&image.luma);
    let transition_bps =
        previous_luma.map(|previous| transition_score(previous, &image.luma)).unwrap_or(0);
    VideoSequenceSample {
        phash,
        dhash,
        mean_luma,
        mean_chroma_u,
        mean_chroma_v,
        information_bps,
        transition_bps,
    }
}

fn luma(pixel: &[u8; 3]) -> u8 {
    (0.299 * f64::from(pixel[0]) + 0.587 * f64::from(pixel[1]) + 0.114 * f64::from(pixel[2]))
        .round()
        .clamp(0.0, 255.0) as u8
}

fn color_summary(rgb: &[[u8; 3]], luma_values: &[u8]) -> (u8, i8, i8) {
    let count = rgb.len() as f64;
    let mean_luma = (luma_values.iter().map(|value| f64::from(*value)).sum::<f64>() / count)
        .round()
        .clamp(0.0, 255.0) as u8;
    let (u, v) = rgb.iter().fold((0.0, 0.0), |(u, v), pixel| {
        let y =
            0.299 * f64::from(pixel[0]) + 0.587 * f64::from(pixel[1]) + 0.114 * f64::from(pixel[2]);
        (u + (f64::from(pixel[2]) - y) * 0.565, v + (f64::from(pixel[0]) - y) * 0.713)
    });
    let mean_u = (u / count).round().clamp(-128.0, 127.0) as i8;
    let mean_v = (v / count).round().clamp(-128.0, 127.0) as i8;
    (mean_luma, mean_u, mean_v)
}

fn information_score(luma_values: &[u8]) -> u16 {
    let sample_count = luma_values.len() as f64;
    let mean = luma_values.iter().map(|value| f64::from(*value)).sum::<f64>() / sample_count;
    let variance = luma_values.iter().map(|value| (f64::from(*value) - mean).powi(2)).sum::<f64>()
        / sample_count;
    let variance_bps = variance / (255.0 * 255.0) * 10_000.0;
    let mut histogram = [0_u32; 16];
    for value in luma_values {
        histogram[usize::from(*value) / 16] += 1;
    }
    let entropy = histogram
        .iter()
        .filter(|count| **count > 0)
        .map(|count| {
            let probability = f64::from(*count) / sample_count;
            -probability * probability.log2()
        })
        .sum::<f64>();
    let entropy_bps = entropy / 4.0 * 10_000.0;
    (0.7 * variance_bps + 0.3 * entropy_bps).round().clamp(0.0, 10_000.0) as u16
}

fn transition_score(previous: &[u8], current: &[u8]) -> u16 {
    if previous.len() != current.len() || previous.is_empty() {
        return 0;
    }
    let difference = previous
        .iter()
        .zip(current)
        .map(|(previous, current)| u32::from(previous.abs_diff(*current)))
        .sum::<u32>();
    let maximum = 255_u32 * previous.len() as u32;
    (difference * 10_000 / maximum).min(10_000) as u16
}

fn dhash_v1(luma_values: &[u8]) -> u64 {
    let mut hash = 0_u64;
    for y in 0..8 {
        let row = y * 31 / 7;
        for x in 0..8 {
            let left_x = x * 31 / 8;
            let right_x = (x + 1) * 31 / 8;
            let left = luma_values[row * 32 + left_x];
            let right = luma_values[row * 32 + right_x];
            if left < right {
                hash |= 1_u64 << (y * 8 + x);
            }
        }
    }
    hash
}

fn phash_v1(luma_values: &[u8]) -> u64 {
    let basis = PHASH_BASIS.get_or_init(|| {
        std::array::from_fn(|frequency| {
            std::array::from_fn(|position| {
                (std::f64::consts::PI * (2 * position + 1) as f64 * frequency as f64
                    / (2.0 * PHASH_DCT_SIZE as f64))
                    .cos()
            })
        })
    });
    let mut horizontal = [[0.0_f64; PHASH_DCT_SIZE]; PHASH_LOW_FREQUENCY_SIZE];
    for frequency in 0..PHASH_LOW_FREQUENCY_SIZE {
        for row in 0..PHASH_DCT_SIZE {
            for column in 0..PHASH_DCT_SIZE {
                horizontal[frequency][row] += f64::from(luma_values[row * PHASH_DCT_SIZE + column])
                    * basis[frequency][column];
            }
        }
    }
    let mut coefficients = [0.0_f64; PHASH_COEFFICIENT_COUNT];
    for (u, horizontal_row) in horizontal.iter().enumerate() {
        for (v, basis_row) in basis.iter().enumerate() {
            let value = horizontal_row
                .iter()
                .zip(basis_row)
                .map(|(horizontal, basis)| horizontal * basis)
                .sum::<f64>();
            let scale = if u == 0 { 1.0 / 2.0_f64.sqrt() } else { 1.0 };
            let scale_y = if v == 0 { 1.0 / 2.0_f64.sqrt() } else { 1.0 };
            coefficients[u * PHASH_LOW_FREQUENCY_SIZE + v] = value * scale * scale_y;
        }
    }
    let mut sorted = coefficients;
    sorted.sort_by(f64::total_cmp);
    let median = sorted[sorted.len() / 2];
    coefficients.iter().enumerate().fold(0_u64, |hash, (index, coefficient)| {
        if *coefficient > median { hash | (1_u64 << index) } else { hash }
    })
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Error)]
pub enum VideoSequenceError {
    #[error("video sequence duration must be greater than zero")]
    InvalidDuration,
    #[error("video sequence interval must be greater than zero")]
    InvalidInterval,
    #[error("video sequence sample count is invalid: {0}")]
    InvalidSampleCount(usize),
    #[error("video sequence timestamp grid is invalid")]
    InvalidTimestampGrid,
    #[error("video sequence uses an unsupported algorithm: {0:?}")]
    UnsupportedAlgorithm(FingerprintVersion),
    #[error("video sequence blob is too large; maximum is {max} bytes")]
    EncodedTooLarge { max: usize },
    #[error("video sequence blob is truncated")]
    Truncated,
    #[error("video sequence blob has invalid magic")]
    InvalidMagic,
    #[error("video sequence codec version {0} is unsupported")]
    UnsupportedCodecVersion(u16),
    #[error("video sequence blob has a non-zero reserved header")]
    InvalidReservedHeader,
    #[error("video sequence blob has trailing bytes")]
    TrailingBytes,
    #[error("video sequence information score is invalid: {0}")]
    InvalidInformationScore(u16),
    #[error("video sequence transition score is invalid: {0}")]
    InvalidTransitionScore(u16),
    #[error("video sequence first transition score must be zero, got {0}")]
    InvalidFirstTransition(u16),
}

#[cfg(test)]
mod tests {
    use image::{ImageBuffer, Rgb};

    use super::*;

    fn sample(seed: u64, information_bps: u16) -> VideoSequenceSample {
        VideoSequenceSample {
            phash: seed,
            dhash: !seed,
            mean_luma: 120,
            mean_chroma_u: -4,
            mean_chroma_v: 8,
            information_bps,
            transition_bps: 0,
        }
    }

    #[test]
    fn timestamps_use_half_seconds_and_expand_at_the_cap() {
        assert_eq!(select_video_sequence_timestamps(1_000), vec![0, 500]);
        let timestamps = select_video_sequence_timestamps(2_000_000);
        assert_eq!(timestamps.len(), VIDEO_SEQUENCE_MAX_SAMPLES);
        assert_eq!(timestamps[1] - timestamps[0], 977);
        assert!(timestamps.windows(2).all(|pair| pair[1] > pair[0]));
    }

    #[test]
    fn features_are_stable_and_transition_responds_to_pixels() {
        let first = DynamicImage::ImageRgb8(ImageBuffer::from_fn(64, 64, |x, y| {
            Rgb([(x as u8).wrapping_mul(3), (y as u8).wrapping_mul(2), 80])
        }));
        let second = DynamicImage::ImageRgb8(ImageBuffer::from_fn(64, 64, |x, y| {
            Rgb([255 - (x as u8).wrapping_mul(3), (y as u8).wrapping_mul(2), 20])
        }));
        let fingerprint = VideoSequenceFingerprint::from_images(1_000, 500, &[first, second])
            .expect("images should produce a sequence");
        assert_eq!(fingerprint.version, FingerprintVersion::VideoSequenceV1);
        assert_eq!(
            fingerprint.samples,
            vec![
                VideoSequenceSample {
                    phash: 17_931_784_059_648_393_233,
                    dhash: u64::MAX,
                    mean_luma: 74,
                    mean_chroma_u: 3,
                    mean_chroma_v: 15,
                    information_bps: 2_190,
                    transition_bps: 0,
                },
                VideoSequenceSample {
                    phash: 12_613_633_870_826_187_525,
                    dhash: 0,
                    mean_luma: 87,
                    mean_chroma_u: -38,
                    mean_chroma_v: 52,
                    information_bps: 2_193,
                    transition_bps: 1_181,
                },
            ]
        );
    }

    #[test]
    fn dhash_samples_the_lower_part_of_the_frame() {
        let left = DynamicImage::ImageRgb8(ImageBuffer::from_fn(32, 32, |x, y| {
            let value = if y < 8 { 128 } else { x * 8 } as u8;
            Rgb([value, value, value])
        }));
        let right = DynamicImage::ImageRgb8(ImageBuffer::from_fn(32, 32, |x, y| {
            let value = if y < 8 { 128 } else { 255 - x * 8 } as u8;
            Rgb([value, value, value])
        }));
        let left = VideoSequenceFingerprint::from_images(500, 500, &[left]).unwrap();
        let right = VideoSequenceFingerprint::from_images(500, 500, &[right]).unwrap();
        assert_ne!(left.samples[0].dhash, right.samples[0].dhash);
    }

    #[test]
    fn codec_round_trips_a_golden_vector() {
        let fingerprint =
            VideoSequenceFingerprint::new(500, 500, vec![sample(0x0102_0304_0506_0708, 8_000)])
                .expect("sample should be valid");
        let encoded = fingerprint.encode().expect("sequence should encode");
        let expected = vec![
            0x53, 0x51, 0x56, 0x53, 0x01, 0x00, 0x01, 0x00, 0xf4, 0x01, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0xf4, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x07, 0x06, 0x05,
            0x04, 0x03, 0x02, 0x01, 0xf7, 0xf8, 0xf9, 0xfa, 0xfb, 0xfc, 0xfd, 0xfe, 0x78, 0xfc,
            0x08, 0x40, 0x1f, 0x00, 0x00,
        ];
        assert_eq!(encoded, expected);
        assert_eq!(VideoSequenceFingerprint::decode(&encoded).unwrap(), fingerprint);
        assert_eq!(VideoSequenceFingerprint::decode(&encoded).unwrap().encode().unwrap(), encoded);
    }

    #[test]
    fn codec_rejects_trailing_and_oversized_data() {
        let fingerprint = VideoSequenceFingerprint::new(500, 500, vec![sample(1, 2_000)])
            .expect("sample should be valid");
        let mut encoded = fingerprint.encode().unwrap();
        encoded.push(0);
        assert_eq!(
            VideoSequenceFingerprint::decode(&encoded),
            Err(VideoSequenceError::TrailingBytes)
        );
        assert_eq!(
            VideoSequenceFingerprint::decode(&vec![
                0;
                HEADER_BYTES
                    + VIDEO_SEQUENCE_MAX_SAMPLES * SAMPLE_BYTES
                    + 1
            ]),
            Err(VideoSequenceError::EncodedTooLarge { max: MAX_ENCODED_BYTES })
        );
    }

    #[test]
    fn codec_rejects_bad_headers_and_incomplete_grids() {
        let fingerprint = VideoSequenceFingerprint::new(500, 500, vec![sample(1, 2_000)])
            .expect("sample should be valid");
        let encoded = fingerprint.encode().unwrap();

        let mut bad_magic = encoded.clone();
        bad_magic[0] = 0;
        assert_eq!(
            VideoSequenceFingerprint::decode(&bad_magic),
            Err(VideoSequenceError::InvalidMagic)
        );

        let mut bad_codec = encoded.clone();
        bad_codec[4] = 2;
        assert_eq!(
            VideoSequenceFingerprint::decode(&bad_codec),
            Err(VideoSequenceError::UnsupportedCodecVersion(2))
        );

        let mut bad_reserved = encoded.clone();
        bad_reserved[20] = 1;
        assert_eq!(
            VideoSequenceFingerprint::decode(&bad_reserved),
            Err(VideoSequenceError::InvalidReservedHeader)
        );

        assert_eq!(
            VideoSequenceFingerprint::decode(&encoded[..23]),
            Err(VideoSequenceError::Truncated)
        );
        assert_eq!(
            VideoSequenceFingerprint::new(1_000, 500, vec![sample(1, 2_000)]),
            Err(VideoSequenceError::InvalidTimestampGrid)
        );
    }

    #[test]
    fn codec_rejects_semantically_invalid_sample_scores() {
        let valid = VideoSequenceFingerprint::new(500, 500, vec![sample(1, MAX_SCORE_BPS)])
            .expect("the score boundary should be valid");
        assert!(valid.encode().is_ok());

        assert_eq!(
            VideoSequenceFingerprint::new(500, 500, vec![sample(1, MAX_SCORE_BPS + 1)]),
            Err(VideoSequenceError::InvalidInformationScore(MAX_SCORE_BPS + 1))
        );

        let mut invalid_first_transition = sample(1, 2_000);
        invalid_first_transition.transition_bps = 1;
        assert_eq!(
            VideoSequenceFingerprint::new(500, 500, vec![invalid_first_transition]),
            Err(VideoSequenceError::InvalidFirstTransition(1))
        );

        let mut invalid_transition = vec![sample(1, 2_000), sample(2, 2_000)];
        invalid_transition[1].transition_bps = MAX_SCORE_BPS + 1;
        assert_eq!(
            VideoSequenceFingerprint::new(1_000, 500, invalid_transition),
            Err(VideoSequenceError::InvalidTransitionScore(MAX_SCORE_BPS + 1))
        );

        let mut invalid_public_field = valid.clone();
        invalid_public_field.samples[0].information_bps = MAX_SCORE_BPS + 1;
        assert_eq!(
            invalid_public_field.encode(),
            Err(VideoSequenceError::InvalidInformationScore(MAX_SCORE_BPS + 1))
        );

        let mut invalid_encoded_score = valid.encode().unwrap();
        invalid_encoded_score[43..45].copy_from_slice(&(MAX_SCORE_BPS + 1).to_le_bytes());
        assert_eq!(
            VideoSequenceFingerprint::decode(&invalid_encoded_score),
            Err(VideoSequenceError::InvalidInformationScore(MAX_SCORE_BPS + 1))
        );

        let mut invalid_encoded_transition = valid.encode().unwrap();
        invalid_encoded_transition[45..47].copy_from_slice(&1_u16.to_le_bytes());
        assert_eq!(
            VideoSequenceFingerprint::decode(&invalid_encoded_transition),
            Err(VideoSequenceError::InvalidFirstTransition(1))
        );
    }

    #[test]
    fn search_tokens_are_sorted_deduplicated_and_ignore_black_frames() {
        let fingerprint = VideoSequenceFingerprint::new(
            2_000,
            500,
            vec![sample(0, 0), sample(0x1111, 8_000), sample(0x1111, 8_000), sample(0x2222, 8_000)],
        )
        .expect("samples should be valid");
        let tokens = fingerprint.search_tokens();
        assert!(!tokens.is_empty());
        assert!(tokens.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(tokens.iter().all(|token| *token > 0));
        assert!(tokens.len() <= VIDEO_SEQUENCE_MAX_TOKENS);
    }
}
