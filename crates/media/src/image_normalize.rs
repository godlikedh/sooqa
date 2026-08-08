use std::{
    fs::{self, OpenOptions},
    io::{self, BufReader, Cursor, Write},
    path::{Path, PathBuf},
};

use image::{
    DynamicImage, ImageDecoder, ImageFormat, ImageReader, Limits,
    codecs::{jpeg::JpegEncoder, png::PngDecoder},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    FileDigest, HashError, MediaWorkspace, WorkspaceArea, WorkspaceError, sha256_bytes,
    sha256_file, sha256_file_sync,
};

const DEFAULT_MAX_WIDTH: u32 = 1920;
const DEFAULT_MAX_HEIGHT: u32 = 1080;
const DEFAULT_THUMBNAIL_MAX_WIDTH: u32 = 320;
const DEFAULT_THUMBNAIL_MAX_HEIGHT: u32 = 320;
const DEFAULT_JPEG_QUALITY: u8 = 85;
const DEFAULT_MAX_INPUT_PIXELS: u64 = 100_000_000;
const DEFAULT_MAX_INPUT_BYTES: u64 = 100 * 1024 * 1024;
const DEFAULT_MAX_WORKING_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub struct CanonicalImageProfile {
    pub max_width: u32,
    pub max_height: u32,
    pub thumbnail_max_width: u32,
    pub thumbnail_max_height: u32,
    pub jpeg_quality: u8,
    pub max_input_pixels: u64,
    pub max_input_bytes: u64,
    pub max_working_bytes: u64,
}

impl Default for CanonicalImageProfile {
    fn default() -> Self {
        Self {
            max_width: DEFAULT_MAX_WIDTH,
            max_height: DEFAULT_MAX_HEIGHT,
            thumbnail_max_width: DEFAULT_THUMBNAIL_MAX_WIDTH,
            thumbnail_max_height: DEFAULT_THUMBNAIL_MAX_HEIGHT,
            jpeg_quality: DEFAULT_JPEG_QUALITY,
            max_input_pixels: DEFAULT_MAX_INPUT_PIXELS,
            max_input_bytes: DEFAULT_MAX_INPUT_BYTES,
            max_working_bytes: DEFAULT_MAX_WORKING_BYTES,
        }
    }
}

impl CanonicalImageProfile {
    pub fn validate(self) -> Result<(), ImageProfileError> {
        if self.max_width == 0 || self.max_height == 0 {
            return Err(ImageProfileError::InvalidDimensions {
                width: self.max_width,
                height: self.max_height,
            });
        }
        if self.thumbnail_max_width == 0 || self.thumbnail_max_height == 0 {
            return Err(ImageProfileError::InvalidThumbnailDimensions {
                width: self.thumbnail_max_width,
                height: self.thumbnail_max_height,
            });
        }
        if !(1..=100).contains(&self.jpeg_quality) {
            return Err(ImageProfileError::InvalidJpegQuality(self.jpeg_quality));
        }
        if self.max_input_pixels == 0 {
            return Err(ImageProfileError::InvalidInputPixelLimit);
        }
        if self.max_input_bytes == 0 {
            return Err(ImageProfileError::InvalidInputByteLimit);
        }
        if self.max_working_bytes == 0 {
            return Err(ImageProfileError::InvalidWorkingSetLimit);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ImageNormalizationPlan {
    workspace: MediaWorkspace,
    input_path: PathBuf,
    canonical_base_path: PathBuf,
    thumbnail_base_path: PathBuf,
}

impl ImageNormalizationPlan {
    pub fn input_path(&self) -> &Path {
        &self.input_path
    }

    pub fn canonical_base_path(&self) -> &Path {
        &self.canonical_base_path
    }

    pub fn thumbnail_base_path(&self) -> &Path {
        &self.thumbnail_base_path
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanonicalImageFormat {
    Jpeg,
    Png,
}

impl CanonicalImageFormat {
    pub const fn extension(self) -> &'static str {
        match self {
            Self::Jpeg => "jpg",
            Self::Png => "png",
        }
    }

    pub const fn mime_type(self) -> &'static str {
        match self {
            Self::Jpeg => "image/jpeg",
            Self::Png => "image/png",
        }
    }

    const fn image_format(self) -> ImageFormat {
        match self {
            Self::Jpeg => ImageFormat::Jpeg,
            Self::Png => ImageFormat::Png,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ImageNormalizationResult {
    pub canonical_path: PathBuf,
    pub thumbnail_path: PathBuf,
    pub format: CanonicalImageFormat,
    pub has_transparency: bool,
    pub width: u32,
    pub height: u32,
    pub thumbnail_width: u32,
    pub thumbnail_height: u32,
    pub canonical_digest: FileDigest,
    pub thumbnail_digest: FileDigest,
}

#[derive(Debug, Clone, Copy)]
pub struct ImageNormalizer {
    profile: CanonicalImageProfile,
}

impl ImageNormalizer {
    pub fn new(profile: CanonicalImageProfile) -> Result<Self, ImageProfileError> {
        profile.validate()?;
        Ok(Self { profile })
    }

    pub fn profile(self) -> CanonicalImageProfile {
        self.profile
    }

    pub fn plan(
        &self,
        workspace: &MediaWorkspace,
        input_name: &str,
        canonical_name: &str,
        thumbnail_name: &str,
    ) -> Result<ImageNormalizationPlan, ImageNormalizationError> {
        Ok(ImageNormalizationPlan {
            workspace: workspace.clone(),
            input_path: workspace.path(WorkspaceArea::Source, input_name)?,
            canonical_base_path: workspace.path(WorkspaceArea::Normalized, canonical_name)?,
            thumbnail_base_path: workspace.path(WorkspaceArea::Previews, thumbnail_name)?,
        })
    }

    pub async fn execute(
        &self,
        plan: &ImageNormalizationPlan,
    ) -> Result<ImageNormalizationResult, ImageNormalizationError> {
        let plan = plan.clone();
        let profile = self.profile;
        let encoded = tokio::task::spawn_blocking(move || encode_image(&plan, profile))
            .await
            .map_err(ImageNormalizationError::TaskJoin)??;
        let canonical_digest = match sha256_file(&encoded.canonical_path).await {
            Ok(digest) => digest,
            Err(error) => {
                cleanup_outputs(&encoded).await;
                return Err(error.into());
            }
        };
        let thumbnail_digest = match sha256_file(&encoded.thumbnail_path).await {
            Ok(digest) => digest,
            Err(error) => {
                cleanup_outputs(&encoded).await;
                return Err(error.into());
            }
        };
        Ok(ImageNormalizationResult {
            canonical_path: encoded.canonical_path,
            thumbnail_path: encoded.thumbnail_path,
            format: encoded.format,
            has_transparency: encoded.has_transparency,
            width: encoded.width,
            height: encoded.height,
            thumbnail_width: encoded.thumbnail_width,
            thumbnail_height: encoded.thumbnail_height,
            canonical_digest,
            thumbnail_digest,
        })
    }
}

async fn cleanup_outputs(encoded: &EncodedImage) {
    if encoded.canonical_created {
        let _ = tokio::fs::remove_file(&encoded.canonical_path).await;
    }
    if encoded.thumbnail_created {
        let _ = tokio::fs::remove_file(&encoded.thumbnail_path).await;
    }
}

#[derive(Debug, Error)]
pub enum ImageProfileError {
    #[error("canonical image dimensions must be greater than zero, got {width}x{height}")]
    InvalidDimensions { width: u32, height: u32 },
    #[error("thumbnail dimensions must be greater than zero, got {width}x{height}")]
    InvalidThumbnailDimensions { width: u32, height: u32 },
    #[error("JPEG quality must be between 1 and 100, got {0}")]
    InvalidJpegQuality(u8),
    #[error("maximum input pixels must be greater than zero")]
    InvalidInputPixelLimit,
    #[error("maximum input bytes must be greater than zero")]
    InvalidInputByteLimit,
    #[error("maximum working bytes must be greater than zero")]
    InvalidWorkingSetLimit,
}

#[derive(Debug, Error)]
pub enum ImageNormalizationError {
    #[error("could not read image input {path}: {source}")]
    InputFile { path: PathBuf, source: io::Error },
    #[error("unsupported image input format: {format}")]
    UnsupportedInputFormat { format: String },
    #[error("image input is not a regular file: {path}")]
    InputNotFile { path: PathBuf },
    #[error("image input is a symlink: {path}")]
    InputSymlink { path: PathBuf },
    #[error("image input has a symlinked parent: {path}")]
    InputParentSymlink { path: PathBuf },
    #[error("image input exceeds the {limit}-byte limit: {path}")]
    InputTooLarge { path: PathBuf, limit: u64 },
    #[error("image input exceeds the {limit}-pixel limit: {path}")]
    InputTooManyPixels { path: PathBuf, limit: u64 },
    #[error("image input estimated working set exceeds the {limit}-byte limit: {path}")]
    InputWorkingSetTooLarge { path: PathBuf, limit: u64 },
    #[error("animated PNG inputs are not supported by the static image path: {path}")]
    AnimatedPng { path: PathBuf },
    #[error("could not decode image input {path}: {source}")]
    Decode { path: PathBuf, source: image::ImageError },
    #[error("image output path is a symlink: {path}")]
    OutputSymlink { path: PathBuf },
    #[error("image output has a symlinked parent: {path}")]
    OutputParentSymlink { path: PathBuf },
    #[error("image output already exists: {path}")]
    OutputExists { path: PathBuf },
    #[error("existing image output is invalid at {path}: {reason}")]
    ExistingOutputInvalid { path: PathBuf, reason: String },
    #[error("existing image output conflicts with the normalized input at {path}")]
    ExistingOutputConflict { path: PathBuf },
    #[error("image output paths collide: {path}")]
    OutputPathCollision { path: PathBuf },
    #[error("could not write image output {path}: {source}")]
    Output { path: PathBuf, source: io::Error },
    #[error("could not encode image output {path}: {source}")]
    Encode { path: PathBuf, source: image::ImageError },
    #[error("image normalization task failed: {0}")]
    TaskJoin(#[source] tokio::task::JoinError),
    #[error("could not hash normalized image: {0}")]
    Hash(#[from] HashError),
    #[error("workspace path is invalid: {0}")]
    Workspace(#[from] WorkspaceError),
}

#[derive(Debug)]
struct EncodedImage {
    canonical_path: PathBuf,
    thumbnail_path: PathBuf,
    format: CanonicalImageFormat,
    has_transparency: bool,
    width: u32,
    height: u32,
    thumbnail_width: u32,
    thumbnail_height: u32,
    canonical_created: bool,
    thumbnail_created: bool,
}

#[derive(Debug, Clone, Copy)]
struct EnsuredOutput {
    width: u32,
    height: u32,
    created: bool,
}

fn encode_image(
    plan: &ImageNormalizationPlan,
    profile: CanonicalImageProfile,
) -> Result<EncodedImage, ImageNormalizationError> {
    plan.workspace.validate()?;
    let image = decode_image(&plan.input_path, profile)?;
    let rgba = image.to_rgba8();
    let has_transparency = rgba.pixels().any(|pixel| pixel.0[3] != u8::MAX);
    let format =
        if has_transparency { CanonicalImageFormat::Png } else { CanonicalImageFormat::Jpeg };
    let canonical = if has_transparency {
        fit_image(&DynamicImage::ImageRgba8(rgba), profile.max_width, profile.max_height)
    } else {
        fit_image(&DynamicImage::ImageRgb8(image.to_rgb8()), profile.max_width, profile.max_height)
    };
    let thumbnail =
        fit_image(&canonical, profile.thumbnail_max_width, profile.thumbnail_max_height);
    let canonical_path = with_extension(&plan.canonical_base_path, format.extension());
    let thumbnail_path = with_extension(&plan.thumbnail_base_path, format.extension());
    if canonical_path == plan.input_path {
        return Err(ImageNormalizationError::OutputPathCollision { path: canonical_path });
    }
    if thumbnail_path == plan.input_path || thumbnail_path == canonical_path {
        return Err(ImageNormalizationError::OutputPathCollision { path: thumbnail_path });
    }
    let canonical_output = ensure_output(
        &canonical_path,
        &canonical,
        format,
        profile,
        profile.max_width,
        profile.max_height,
    )?;
    let thumbnail_output = match ensure_output(
        &thumbnail_path,
        &thumbnail,
        format,
        profile,
        profile.thumbnail_max_width,
        profile.thumbnail_max_height,
    ) {
        Ok(output) => output,
        Err(error) => {
            if canonical_output.created {
                let _ = fs::remove_file(&canonical_path);
            }
            return Err(error);
        }
    };

    Ok(EncodedImage {
        canonical_path,
        thumbnail_path,
        format,
        has_transparency,
        width: canonical_output.width,
        height: canonical_output.height,
        thumbnail_width: thumbnail_output.width,
        thumbnail_height: thumbnail_output.height,
        canonical_created: canonical_output.created,
        thumbnail_created: thumbnail_output.created,
    })
}

fn ensure_output(
    path: &Path,
    image: &DynamicImage,
    format: CanonicalImageFormat,
    profile: CanonicalImageProfile,
    max_width: u32,
    max_height: u32,
) -> Result<EnsuredOutput, ImageNormalizationError> {
    let encoded = encode_image_bytes(image, format, profile.jpeg_quality, path)?;
    let expected_digest = sha256_bytes(&encoded);
    if let Some((width, height)) =
        existing_output_dimensions(path, format, profile, max_width, max_height, &expected_digest)?
    {
        return Ok(EnsuredOutput { width, height, created: false });
    }

    match write_image(path, &encoded) {
        Ok(()) => Ok(EnsuredOutput { width: image.width(), height: image.height(), created: true }),
        Err(error @ ImageNormalizationError::OutputExists { .. }) => {
            if let Some((width, height)) = existing_output_dimensions(
                path,
                format,
                profile,
                max_width,
                max_height,
                &expected_digest,
            )? {
                Ok(EnsuredOutput { width, height, created: false })
            } else {
                Err(error)
            }
        }
        Err(error) => Err(error),
    }
}

fn existing_output_dimensions(
    path: &Path,
    expected_format: CanonicalImageFormat,
    profile: CanonicalImageProfile,
    max_width: u32,
    max_height: u32,
    expected_digest: &FileDigest,
) -> Result<Option<(u32, u32)>, ImageNormalizationError> {
    if let Some(parent) = symlinked_parent(path)
        .map_err(|source| ImageNormalizationError::Output { path: path.to_owned(), source })?
    {
        return Err(ImageNormalizationError::OutputParentSymlink { path: parent });
    }
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(ImageNormalizationError::Output { path: path.to_owned(), source });
        }
    };
    if metadata.file_type().is_symlink() {
        return Err(ImageNormalizationError::OutputSymlink { path: path.to_owned() });
    }
    if !metadata.is_file() || metadata.len() == 0 {
        return Err(ImageNormalizationError::ExistingOutputInvalid {
            path: path.to_owned(),
            reason: "output is not a non-empty regular file".to_owned(),
        });
    }
    let reader = ImageReader::open(path)
        .map_err(|source| ImageNormalizationError::Output { path: path.to_owned(), source })?
        .with_guessed_format()
        .map_err(|source| ImageNormalizationError::Output { path: path.to_owned(), source })?;
    if reader.format() != Some(expected_format.image_format()) {
        return Err(ImageNormalizationError::ExistingOutputInvalid {
            path: path.to_owned(),
            reason: format!(
                "expected {:?}, found {:?}",
                expected_format.image_format(),
                reader.format()
            ),
        });
    }
    let image = decode_image(path, profile).map_err(|error| {
        ImageNormalizationError::ExistingOutputInvalid {
            path: path.to_owned(),
            reason: error.to_string(),
        }
    })?;
    if image.width() == 0
        || image.height() == 0
        || image.width() > max_width
        || image.height() > max_height
    {
        return Err(ImageNormalizationError::ExistingOutputInvalid {
            path: path.to_owned(),
            reason: format!(
                "dimensions {}x{} exceed {}x{}",
                image.width(),
                image.height(),
                max_width,
                max_height
            ),
        });
    }
    let actual_digest = sha256_file_sync(path)?;
    if actual_digest != *expected_digest {
        return Err(ImageNormalizationError::ExistingOutputConflict { path: path.to_owned() });
    }
    Ok(Some((image.width(), image.height())))
}

fn fit_image(image: &DynamicImage, max_width: u32, max_height: u32) -> DynamicImage {
    if image.width() <= max_width && image.height() <= max_height {
        image.clone()
    } else {
        image.thumbnail(max_width, max_height)
    }
}

fn decode_image(
    path: &Path,
    profile: CanonicalImageProfile,
) -> Result<DynamicImage, ImageNormalizationError> {
    validate_input_path(path, profile.max_input_bytes)?;
    let input_path = path.to_owned();
    let mut limits = Limits::default();
    limits.max_alloc = Some(
        profile
            .max_input_pixels
            .saturating_mul(4)
            .min(profile.max_working_bytes.saturating_div(4))
            .max(1),
    );
    let reader = ImageReader::open(&input_path)
        .map_err(|source| ImageNormalizationError::InputFile { path: input_path.clone(), source })?
        .with_guessed_format()
        .map_err(|source| ImageNormalizationError::InputFile {
            path: input_path.clone(),
            source,
        })?;
    let format = reader.format().ok_or_else(|| {
        ImageNormalizationError::UnsupportedInputFormat { format: "unknown".to_owned() }
    })?;
    if !matches!(format, ImageFormat::Jpeg | ImageFormat::Png) {
        return Err(ImageNormalizationError::UnsupportedInputFormat {
            format: format!("{format:?}"),
        });
    }
    if format == ImageFormat::Png {
        let png_file = fs::File::open(&input_path).map_err(|source| {
            ImageNormalizationError::InputFile { path: input_path.clone(), source }
        })?;
        let png_decoder =
            PngDecoder::with_limits(BufReader::new(png_file), limits.clone()).map_err(
                |source| ImageNormalizationError::Decode { path: input_path.clone(), source },
            )?;
        if png_decoder.is_apng().map_err(|source| ImageNormalizationError::Decode {
            path: input_path.clone(),
            source,
        })? {
            return Err(ImageNormalizationError::AnimatedPng { path: input_path.clone() });
        }
    }
    let mut reader = reader;
    reader.limits(limits);
    let mut decoder = reader
        .into_decoder()
        .map_err(|source| ImageNormalizationError::Decode { path: input_path.clone(), source })?;
    let (width, height) = decoder.dimensions();
    let pixels = u64::from(width).checked_mul(u64::from(height)).ok_or(
        ImageNormalizationError::InputTooManyPixels {
            path: input_path.clone(),
            limit: profile.max_input_pixels,
        },
    )?;
    if pixels > profile.max_input_pixels {
        return Err(ImageNormalizationError::InputTooManyPixels {
            path: input_path.clone(),
            limit: profile.max_input_pixels,
        });
    }
    let estimated_working_bytes = estimate_working_bytes(width, height, profile).ok_or(
        ImageNormalizationError::InputWorkingSetTooLarge {
            path: input_path.clone(),
            limit: profile.max_working_bytes,
        },
    )?;
    if estimated_working_bytes > profile.max_working_bytes {
        return Err(ImageNormalizationError::InputWorkingSetTooLarge {
            path: input_path.clone(),
            limit: profile.max_working_bytes,
        });
    }
    let orientation = decoder
        .orientation()
        .map_err(|source| ImageNormalizationError::Decode { path: input_path.clone(), source })?;
    let mut image = DynamicImage::from_decoder(decoder)
        .map_err(|source| ImageNormalizationError::Decode { path: input_path, source })?;
    image.apply_orientation(orientation);
    Ok(image)
}

/// Estimate the peak image working set before decoding. The image crate does
/// not expose a process-wide allocation budget, so the normalizer reserves for
/// the decoded/oriented source, RGBA inspection, color conversion, resize
/// scratch space, canonical output, and thumbnail output. This conservative
/// preflight keeps the transformation bounded even when several intermediate
/// buffers overlap briefly.
fn estimate_working_bytes(width: u32, height: u32, profile: CanonicalImageProfile) -> Option<u64> {
    let input_pixels = u64::from(width).checked_mul(u64::from(height))?;
    let canonical_width = width.min(profile.max_width);
    let canonical_height = height.min(profile.max_height);
    let thumbnail_width = canonical_width.min(profile.thumbnail_max_width);
    let thumbnail_height = canonical_height.min(profile.thumbnail_max_height);
    let canonical_pixels = u64::from(canonical_width).checked_mul(u64::from(canonical_height))?;
    let thumbnail_pixels = u64::from(thumbnail_width).checked_mul(u64::from(thumbnail_height))?;

    input_pixels
        .checked_mul(32)?
        .checked_add(canonical_pixels.checked_mul(16)?)?
        .checked_add(thumbnail_pixels.checked_mul(16)?)
}

fn with_extension(path: &Path, extension: &str) -> PathBuf {
    let mut output = path.to_owned();
    output.set_extension(extension);
    output
}

fn encode_image_bytes(
    image: &DynamicImage,
    format: CanonicalImageFormat,
    jpeg_quality: u8,
    path: &Path,
) -> Result<Vec<u8>, ImageNormalizationError> {
    let mut bytes = Cursor::new(Vec::new());
    match format {
        CanonicalImageFormat::Jpeg => JpegEncoder::new_with_quality(&mut bytes, jpeg_quality)
            .encode_image(image)
            .map_err(|source| ImageNormalizationError::Encode { path: path.to_owned(), source })?,
        CanonicalImageFormat::Png => image
            .write_to(&mut bytes, format.image_format())
            .map_err(|source| ImageNormalizationError::Encode { path: path.to_owned(), source })?,
    }
    Ok(bytes.into_inner())
}

fn write_image(path: &Path, encoded: &[u8]) -> Result<(), ImageNormalizationError> {
    validate_output_path(path)?;
    let file_name = path.file_name().ok_or_else(|| ImageNormalizationError::Output {
        path: path.to_owned(),
        source: io::Error::new(io::ErrorKind::InvalidInput, "output path has no file name"),
    })?;
    let temporary_path =
        path.with_file_name(format!(".{}.tmp-{}", file_name.to_string_lossy(), Uuid::new_v4()));
    let file = OpenOptions::new().create_new(true).write(true).open(&temporary_path).map_err(
        |source| ImageNormalizationError::Output { path: temporary_path.clone(), source },
    )?;
    let result = (|| {
        let mut file = file;
        file.write_all(encoded)
            .map_err(|source| ImageNormalizationError::Output { path: path.to_owned(), source })?;
        file.flush()
            .map_err(|source| ImageNormalizationError::Output { path: path.to_owned(), source })?;
        file.sync_all()
            .map_err(|source| ImageNormalizationError::Output { path: path.to_owned(), source })?;
        drop(file);
        match fs::hard_link(&temporary_path, path) {
            Ok(()) => {}
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
                return Err(ImageNormalizationError::OutputExists { path: path.to_owned() });
            }
            Err(source) => {
                return Err(ImageNormalizationError::Output { path: path.to_owned(), source });
            }
        }
        // The hard link is the publication point. If cleanup of the private
        // temporary name fails after publication, keep the valid destination
        // and let a later workspace cleanup remove the orphaned temp file.
        let _ = fs::remove_file(&temporary_path);
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary_path);
    }
    result
}

fn validate_input_path(path: &Path, max_input_bytes: u64) -> Result<(), ImageNormalizationError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|source| ImageNormalizationError::InputFile { path: path.to_owned(), source })?;
    if metadata.file_type().is_symlink() {
        return Err(ImageNormalizationError::InputSymlink { path: path.to_owned() });
    }
    if !metadata.is_file() {
        return Err(ImageNormalizationError::InputNotFile { path: path.to_owned() });
    }
    if metadata.len() > max_input_bytes {
        return Err(ImageNormalizationError::InputTooLarge {
            path: path.to_owned(),
            limit: max_input_bytes,
        });
    }
    if let Some(parent) = symlinked_parent(path)
        .map_err(|source| ImageNormalizationError::InputFile { path: path.to_owned(), source })?
    {
        return Err(ImageNormalizationError::InputParentSymlink { path: parent });
    }
    Ok(())
}

fn validate_output_path(path: &Path) -> Result<(), ImageNormalizationError> {
    if let Some(path) = symlinked_parent(path)
        .map_err(|source| ImageNormalizationError::Output { path: path.to_owned(), source })?
    {
        return Err(ImageNormalizationError::OutputParentSymlink { path });
    }
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(ImageNormalizationError::OutputSymlink { path: path.to_owned() })
        }
        Ok(_) => Err(ImageNormalizationError::OutputExists { path: path.to_owned() }),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(ImageNormalizationError::Output { path: path.to_owned(), source }),
    }
}

fn symlinked_parent(path: &Path) -> Result<Option<PathBuf>, io::Error> {
    let mut current = path.parent();
    while let Some(parent) = current {
        if parent.as_os_str().is_empty() {
            break;
        }
        match fs::symlink_metadata(parent) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Ok(Some(parent.to_owned()));
            }
            Ok(_) => {}
            Err(source) if source.kind() == io::ErrorKind::NotFound => {}
            Err(source) => return Err(source),
        }
        current = parent.parent();
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use image::{ImageBuffer, Rgb, Rgba};
    use uuid::Uuid;

    use super::*;

    fn temp_path(stem: &str) -> PathBuf {
        let temp_dir = fs::canonicalize(std::env::temp_dir()).expect("test temp directory exists");
        temp_dir.join(format!("sooqa-{stem}-{}.img", Uuid::new_v4()))
    }

    fn write_png(path: &Path, pixel: Rgba<u8>, width: u32, height: u32) {
        let image = ImageBuffer::from_pixel(width, height, pixel);
        DynamicImage::ImageRgba8(image)
            .save_with_format(path, ImageFormat::Png)
            .expect("test image should be written");
    }

    fn write_jpeg(path: &Path, pixel: Rgb<u8>, width: u32, height: u32) {
        let image = ImageBuffer::from_pixel(width, height, pixel);
        DynamicImage::ImageRgb8(image)
            .save_with_format(path, ImageFormat::Jpeg)
            .expect("test image should be written");
    }

    #[tokio::test]
    async fn opaque_images_become_aspect_preserving_jpeg_with_thumbnail() {
        let workspace = MediaWorkspace::create(temp_path("opaque-workspace"), Uuid::new_v4())
            .await
            .expect("workspace should be created");
        let input =
            workspace.path(WorkspaceArea::Source, "input.png").expect("input path should be valid");
        write_png(&input, Rgba([40, 80, 120, u8::MAX]), 400, 200);
        let normalizer = ImageNormalizer::new(CanonicalImageProfile {
            max_width: 100,
            max_height: 100,
            thumbnail_max_width: 32,
            thumbnail_max_height: 32,
            ..CanonicalImageProfile::default()
        })
        .expect("profile should be valid");

        let result = normalizer
            .execute(
                &normalizer
                    .plan(&workspace, "input.png", "canonical", "thumbnail")
                    .expect("normalization plan should be valid"),
            )
            .await
            .expect("image should normalize");

        assert_eq!(result.format, CanonicalImageFormat::Jpeg);
        assert!(!result.has_transparency);
        assert_eq!((result.width, result.height), (100, 50));
        assert_eq!((result.thumbnail_width, result.thumbnail_height), (32, 16));
        assert_eq!(result.canonical_path.extension().and_then(|value| value.to_str()), Some("jpg"));
        assert!(result.canonical_digest.bytes > 0);
        assert!(result.thumbnail_digest.bytes > 0);
        let decoded = ImageReader::open(&result.canonical_path)
            .expect("canonical image should open")
            .decode()
            .expect("canonical image should decode");
        assert_eq!((decoded.width(), decoded.height()), (100, 50));

        workspace.cleanup().await.expect("workspace should be removed");
    }

    #[tokio::test]
    async fn transparent_images_remain_png_and_keep_alpha() {
        let workspace = MediaWorkspace::create(temp_path("transparent-workspace"), Uuid::new_v4())
            .await
            .expect("workspace should be created");
        let input =
            workspace.path(WorkspaceArea::Source, "input.png").expect("input path should be valid");
        write_png(&input, Rgba([255, 0, 0, 100]), 40, 30);
        let normalizer = ImageNormalizer::new(CanonicalImageProfile {
            max_width: 100,
            max_height: 100,
            thumbnail_max_width: 20,
            thumbnail_max_height: 20,
            ..CanonicalImageProfile::default()
        })
        .expect("profile should be valid");

        let result = normalizer
            .execute(
                &normalizer
                    .plan(&workspace, "input.png", "canonical", "thumbnail")
                    .expect("normalization plan should be valid"),
            )
            .await
            .expect("image should normalize");

        assert_eq!(result.format, CanonicalImageFormat::Png);
        assert!(result.has_transparency);
        assert_eq!((result.width, result.height), (40, 30));
        assert_eq!((result.thumbnail_width, result.thumbnail_height), (20, 15));
        let decoded = ImageReader::open(&result.canonical_path)
            .expect("canonical image should open")
            .decode()
            .expect("canonical image should decode")
            .to_rgba8();
        assert_eq!(decoded.get_pixel(0, 0).0[3], 100);

        workspace.cleanup().await.expect("workspace should be removed");
    }

    #[tokio::test]
    async fn invalid_existing_output_is_not_overwritten() {
        let workspace =
            MediaWorkspace::create(temp_path("existing-output-workspace"), Uuid::new_v4())
                .await
                .expect("workspace should be created");
        let input =
            workspace.path(WorkspaceArea::Source, "input.png").expect("input path should be valid");
        let canonical = workspace
            .path(WorkspaceArea::Normalized, "canonical.jpg")
            .expect("canonical path should be valid");
        write_png(&input, Rgba([40, 80, 120, u8::MAX]), 20, 10);
        std::fs::write(&canonical, b"existing output").expect("existing output should be written");
        let normalizer = ImageNormalizer::new(CanonicalImageProfile::default())
            .expect("profile should be valid");
        let plan = normalizer
            .plan(&workspace, "input.png", "canonical", "thumbnail")
            .expect("normalization plan should be valid");

        let error =
            normalizer.execute(&plan).await.expect_err("existing output should not be overwritten");
        assert!(matches!(error, ImageNormalizationError::ExistingOutputInvalid { .. }));
        assert_eq!(
            std::fs::read(&canonical).expect("existing output should load"),
            b"existing output"
        );
        workspace.cleanup().await.expect("workspace should be removed");
    }

    #[tokio::test]
    async fn valid_but_different_canonical_output_is_not_reused() {
        let workspace =
            MediaWorkspace::create(temp_path("different-canonical-workspace"), Uuid::new_v4())
                .await
                .expect("workspace should be created");
        let input =
            workspace.path(WorkspaceArea::Source, "input.png").expect("input path should be valid");
        let canonical = workspace
            .path(WorkspaceArea::Normalized, "canonical.jpg")
            .expect("canonical path should be valid");
        write_png(&input, Rgba([40, 80, 120, u8::MAX]), 20, 10);
        write_jpeg(&canonical, Rgb([220, 20, 20]), 20, 10);
        let existing = std::fs::read(&canonical).expect("existing output should be readable");
        let normalizer = ImageNormalizer::new(CanonicalImageProfile::default())
            .expect("profile should be valid");
        let plan = normalizer
            .plan(&workspace, "input.png", "canonical", "thumbnail")
            .expect("normalization plan should be valid");

        let error = normalizer
            .execute(&plan)
            .await
            .expect_err("different existing output should not be reused");
        assert!(matches!(error, ImageNormalizationError::ExistingOutputConflict { .. }));
        assert_eq!(std::fs::read(&canonical).expect("existing output should remain"), existing);
        workspace.cleanup().await.expect("workspace should be removed");
    }

    #[tokio::test]
    async fn valid_but_different_thumbnail_output_is_not_reused() {
        let workspace =
            MediaWorkspace::create(temp_path("different-thumbnail-workspace"), Uuid::new_v4())
                .await
                .expect("workspace should be created");
        let input =
            workspace.path(WorkspaceArea::Source, "input.png").expect("input path should be valid");
        let canonical = workspace
            .path(WorkspaceArea::Normalized, "canonical.jpg")
            .expect("canonical path should be valid");
        let thumbnail = workspace
            .path(WorkspaceArea::Previews, "thumbnail.jpg")
            .expect("thumbnail path should be valid");
        write_png(&input, Rgba([40, 80, 120, u8::MAX]), 400, 200);
        write_jpeg(&thumbnail, Rgb([220, 20, 20]), 320, 160);
        let existing = std::fs::read(&thumbnail).expect("existing output should be readable");
        let normalizer = ImageNormalizer::new(CanonicalImageProfile::default())
            .expect("profile should be valid");
        let plan = normalizer
            .plan(&workspace, "input.png", "canonical", "thumbnail")
            .expect("normalization plan should be valid");

        let error = normalizer
            .execute(&plan)
            .await
            .expect_err("different existing output should not be reused");
        assert!(matches!(error, ImageNormalizationError::ExistingOutputConflict { .. }));
        assert!(!canonical.exists(), "new canonical output should be cleaned after conflict");
        assert_eq!(std::fs::read(&thumbnail).expect("existing output should remain"), existing);
        workspace.cleanup().await.expect("workspace should be removed");
    }

    #[tokio::test]
    async fn valid_existing_outputs_are_reused_on_replay() {
        let workspace = MediaWorkspace::create(temp_path("replay-workspace"), Uuid::new_v4())
            .await
            .expect("workspace should be created");
        let input =
            workspace.path(WorkspaceArea::Source, "input.png").expect("input path should be valid");
        write_png(&input, Rgba([40, 80, 120, u8::MAX]), 400, 200);
        let normalizer = ImageNormalizer::new(CanonicalImageProfile::default())
            .expect("profile should be valid");
        let plan = normalizer
            .plan(&workspace, "input.png", "canonical", "thumbnail")
            .expect("normalization plan should be valid");

        let first = normalizer.execute(&plan).await.expect("first normalization should succeed");
        let canonical_bytes =
            std::fs::read(&first.canonical_path).expect("canonical output should be readable");
        let thumbnail_bytes =
            std::fs::read(&first.thumbnail_path).expect("thumbnail output should be readable");
        let replay = normalizer.execute(&plan).await.expect("replay should reuse outputs");

        assert_eq!(replay, first);
        assert_eq!(
            std::fs::read(&first.canonical_path).expect("canonical output should remain readable"),
            canonical_bytes
        );
        assert_eq!(
            std::fs::read(&first.thumbnail_path).expect("thumbnail output should remain readable"),
            thumbnail_bytes
        );
        workspace.cleanup().await.expect("workspace should be removed");
    }

    #[test]
    fn invalid_profiles_and_non_images_are_rejected() {
        assert!(matches!(
            CanonicalImageProfile { jpeg_quality: 0, ..Default::default() }.validate(),
            Err(ImageProfileError::InvalidJpegQuality(0))
        ));
        let input = temp_path("not-an-image");
        std::fs::write(&input, b"not an image").expect("test input should be written");
        let normalizer = ImageNormalizer::new(CanonicalImageProfile::default())
            .expect("profile should be valid");
        let error = decode_image(&input, normalizer.profile())
            .expect_err("invalid image should be rejected");
        assert!(matches!(error, ImageNormalizationError::UnsupportedInputFormat { .. }));
        std::fs::remove_file(input).expect("test input should be removed");

        let large_input = temp_path("too-many-pixels");
        write_png(&large_input, Rgba([0, 0, 0, u8::MAX]), 10, 10);
        let error = decode_image(
            &large_input,
            CanonicalImageProfile { max_input_pixels: 50, ..Default::default() },
        )
        .expect_err("pixel limit should be enforced before decoding");
        assert!(matches!(error, ImageNormalizationError::InputTooManyPixels { limit: 50, .. }));
        let error = decode_image(
            &large_input,
            CanonicalImageProfile { max_working_bytes: 1_000, ..Default::default() },
        )
        .expect_err("working-set limit should be enforced before decoding");
        assert!(matches!(
            error,
            ImageNormalizationError::InputWorkingSetTooLarge { limit: 1_000, .. }
        ));
        let error = decode_image(
            &large_input,
            CanonicalImageProfile { max_input_bytes: 1, ..Default::default() },
        )
        .expect_err("byte limit should be enforced before decoding");
        assert!(matches!(error, ImageNormalizationError::InputTooLarge { limit: 1, .. }));
        std::fs::remove_file(large_input).expect("test image should be removed");
    }

    #[cfg(unix)]
    #[test]
    fn direct_inputs_with_symlinked_parents_are_rejected() {
        use std::os::unix::fs::symlink;

        let real_parent = temp_path("real-input-parent");
        std::fs::create_dir(&real_parent).expect("real parent should be created");
        let input = real_parent.join("input.png");
        write_png(&input, Rgba([0, 0, 0, u8::MAX]), 2, 2);
        let symlink_parent = temp_path("symlink-input-parent");
        symlink(&real_parent, &symlink_parent).expect("parent symlink should be created");

        let error =
            decode_image(&symlink_parent.join("input.png"), CanonicalImageProfile::default())
                .expect_err("symlinked input parent should be rejected");
        assert!(matches!(error, ImageNormalizationError::InputParentSymlink { .. }));

        std::fs::remove_file(input).expect("input should be removed");
        std::fs::remove_dir(&real_parent).expect("real parent should be removed");
        std::fs::remove_file(symlink_parent).expect("parent symlink should be removed");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn execution_revalidates_workspace_areas_before_opening_files() {
        use std::os::unix::fs::symlink;

        let workspace = MediaWorkspace::create(temp_path("revalidation-workspace"), Uuid::new_v4())
            .await
            .expect("workspace should be created");
        let input =
            workspace.path(WorkspaceArea::Source, "input.png").expect("input path should be valid");
        write_png(&input, Rgba([40, 80, 120, u8::MAX]), 20, 10);
        let normalizer = ImageNormalizer::new(CanonicalImageProfile::default())
            .expect("profile should be valid");
        let plan = normalizer
            .plan(&workspace, "input.png", "canonical", "thumbnail")
            .expect("normalization plan should be valid");

        let source = workspace.root().join("source");
        let moved_source = workspace.root().join("source-real");
        let outside_source = temp_path("outside-source");
        std::fs::create_dir(&outside_source).expect("outside source should be created");
        std::fs::rename(&source, &moved_source).expect("source should be moved");
        symlink(&outside_source, &source).expect("source symlink should be created");

        let error = normalizer
            .execute(&plan)
            .await
            .expect_err("execution should revalidate the workspace boundary");
        assert!(matches!(error, ImageNormalizationError::Workspace(WorkspaceError::Symlink(_))));

        std::fs::remove_file(&source).expect("source symlink should be removed");
        std::fs::rename(&moved_source, &source).expect("source should be restored");
        std::fs::remove_dir(outside_source).expect("outside source should be removed");
        workspace.cleanup().await.expect("workspace should be removed");
    }
}
