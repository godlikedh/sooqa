use std::{
    fs::{self, OpenOptions},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
};

use image::{DynamicImage, ImageFormat, ImageReader, Limits, codecs::jpeg::JpegEncoder};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{FileDigest, HashError, sha256_file};

const DEFAULT_MAX_WIDTH: u32 = 1920;
const DEFAULT_MAX_HEIGHT: u32 = 1080;
const DEFAULT_THUMBNAIL_MAX_WIDTH: u32 = 320;
const DEFAULT_THUMBNAIL_MAX_HEIGHT: u32 = 320;
const DEFAULT_JPEG_QUALITY: u8 = 85;
const DEFAULT_MAX_INPUT_PIXELS: u64 = 100_000_000;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub struct CanonicalImageProfile {
    pub max_width: u32,
    pub max_height: u32,
    pub thumbnail_max_width: u32,
    pub thumbnail_max_height: u32,
    pub jpeg_quality: u8,
    pub max_input_pixels: u64,
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
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ImageNormalizationPlan {
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
        input: impl AsRef<Path>,
        canonical_base: impl AsRef<Path>,
        thumbnail_base: impl AsRef<Path>,
    ) -> ImageNormalizationPlan {
        ImageNormalizationPlan {
            input_path: input.as_ref().to_owned(),
            canonical_base_path: canonical_base.as_ref().to_owned(),
            thumbnail_base_path: thumbnail_base.as_ref().to_owned(),
        }
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
        let canonical_digest = sha256_file(&encoded.canonical_path).await?;
        let thumbnail_digest = sha256_file(&encoded.thumbnail_path).await?;
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
}

#[derive(Debug, Error)]
pub enum ImageNormalizationError {
    #[error("could not read image input {path}: {source}")]
    InputFile { path: PathBuf, source: io::Error },
    #[error("unsupported image input format: {format}")]
    UnsupportedInputFormat { format: String },
    #[error("could not decode image input {path}: {source}")]
    Decode { path: PathBuf, source: image::ImageError },
    #[error("image output path is a symlink: {path}")]
    OutputSymlink { path: PathBuf },
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
}

fn encode_image(
    plan: &ImageNormalizationPlan,
    profile: CanonicalImageProfile,
) -> Result<EncodedImage, ImageNormalizationError> {
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
    write_image(&canonical_path, &canonical, format, profile.jpeg_quality)?;
    write_image(&thumbnail_path, &thumbnail, format, profile.jpeg_quality)?;

    Ok(EncodedImage {
        canonical_path,
        thumbnail_path,
        format,
        has_transparency,
        width: canonical.width(),
        height: canonical.height(),
        thumbnail_width: thumbnail.width(),
        thumbnail_height: thumbnail.height(),
    })
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
    let reader = ImageReader::open(path)
        .map_err(|source| ImageNormalizationError::InputFile { path: path.to_owned(), source })?
        .with_guessed_format()
        .map_err(|source| ImageNormalizationError::InputFile { path: path.to_owned(), source })?;
    let format = reader.format().ok_or_else(|| {
        ImageNormalizationError::UnsupportedInputFormat { format: "unknown".to_owned() }
    })?;
    if !matches!(format, ImageFormat::Jpeg | ImageFormat::Png) {
        return Err(ImageNormalizationError::UnsupportedInputFormat {
            format: format!("{format:?}"),
        });
    }
    let mut reader = reader;
    let mut limits = Limits::default();
    limits.max_alloc = Some(profile.max_input_pixels.saturating_mul(4));
    reader.limits(limits);
    reader
        .decode()
        .map_err(|source| ImageNormalizationError::Decode { path: path.to_owned(), source })
}

fn with_extension(path: &Path, extension: &str) -> PathBuf {
    let mut output = path.to_owned();
    output.set_extension(extension);
    output
}

fn write_image(
    path: &Path,
    image: &DynamicImage,
    format: CanonicalImageFormat,
    jpeg_quality: u8,
) -> Result<(), ImageNormalizationError> {
    if let Ok(metadata) = fs::symlink_metadata(path)
        && metadata.file_type().is_symlink()
    {
        return Err(ImageNormalizationError::OutputSymlink { path: path.to_owned() });
    }
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .map_err(|source| ImageNormalizationError::Output { path: path.to_owned(), source })?;
    let mut writer = BufWriter::new(file);
    match format {
        CanonicalImageFormat::Jpeg => JpegEncoder::new_with_quality(&mut writer, jpeg_quality)
            .encode_image(image)
            .map_err(|source| ImageNormalizationError::Encode { path: path.to_owned(), source })?,
        CanonicalImageFormat::Png => image
            .write_to(&mut writer, format.image_format())
            .map_err(|source| ImageNormalizationError::Encode { path: path.to_owned(), source })?,
    }
    writer
        .flush()
        .map_err(|source| ImageNormalizationError::Output { path: path.to_owned(), source })
}

#[cfg(test)]
mod tests {
    use image::{ImageBuffer, Rgba};
    use uuid::Uuid;

    use super::*;

    fn temp_path(stem: &str) -> PathBuf {
        std::env::temp_dir().join(format!("sooqa-{stem}-{}.img", Uuid::new_v4()))
    }

    fn write_png(path: &Path, pixel: Rgba<u8>, width: u32, height: u32) {
        let image = ImageBuffer::from_pixel(width, height, pixel);
        DynamicImage::ImageRgba8(image)
            .save_with_format(path, ImageFormat::Png)
            .expect("test image should be written");
    }

    #[tokio::test]
    async fn opaque_images_become_aspect_preserving_jpeg_with_thumbnail() {
        let input = temp_path("opaque-input");
        let canonical_base = temp_path("opaque-canonical");
        let thumbnail_base = temp_path("opaque-thumbnail");
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
            .execute(&normalizer.plan(&input, &canonical_base, &thumbnail_base))
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

        for path in [input, result.canonical_path, result.thumbnail_path] {
            std::fs::remove_file(path).expect("test image should be removed");
        }
    }

    #[tokio::test]
    async fn transparent_images_remain_png_and_keep_alpha() {
        let input = temp_path("transparent-input");
        let canonical_base = temp_path("transparent-canonical");
        let thumbnail_base = temp_path("transparent-thumbnail");
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
            .execute(&normalizer.plan(&input, &canonical_base, &thumbnail_base))
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

        for path in [input, result.canonical_path, result.thumbnail_path] {
            std::fs::remove_file(path).expect("test image should be removed");
        }
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
        let plan = normalizer.plan(&input, temp_path("canonical"), temp_path("thumbnail"));
        let error = decode_image(plan.input_path(), normalizer.profile())
            .expect_err("invalid image should be rejected");
        assert!(matches!(error, ImageNormalizationError::UnsupportedInputFormat { .. }));
        std::fs::remove_file(input).expect("test input should be removed");
    }
}
