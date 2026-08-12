use std::{
    fs, io,
    path::{Path, PathBuf},
};

use thiserror::Error;

use crate::{HashError, sha256_file};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum PublishOutcome {
    Published,
    Reused,
}

#[derive(Debug, Error)]
pub(crate) enum PublishError {
    #[error("temporary output and destination must be below real parent directories")]
    InvalidWorkspace,
    #[error("publication path is not a regular file: {0}")]
    NotRegular(PathBuf),
    #[error("destination already contains different content: {0}")]
    DestinationConflict(PathBuf),
    #[error("publication I/O failed for {path}: {source}")]
    Io { path: PathBuf, source: io::Error },
    #[error("publication hash failed: {0}")]
    Hash(#[from] HashError),
}

/// A best-effort synchronous cleanup guard for a job-owned temporary file.
/// The async operation may be dropped during shutdown, so cleanup cannot rely
/// only on code after an await point.
pub(crate) struct TempArtifact {
    path: Option<PathBuf>,
}

impl TempArtifact {
    pub(crate) async fn reserve(path: PathBuf) -> Result<Self, io::Error> {
        let Some(parent) = path.parent() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "temporary artifact path has no parent directory",
            ));
        };
        let metadata = fs::symlink_metadata(parent)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "temporary artifact parent is not a real directory",
            ));
        }
        tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
            .map(|_| Self { path: Some(path) })
    }

    pub(crate) fn path(&self) -> &Path {
        self.path.as_deref().expect("temporary artifact must be armed")
    }

    pub(crate) async fn remove(&mut self) {
        let Some(path) = self.path.as_ref() else { return };
        if tokio::fs::remove_file(path).await.is_ok() {
            self.path = None;
        }
    }
}

impl Drop for TempArtifact {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            let _ = fs::remove_file(path);
        }
    }
}

/// Owns a job-scoped directory containing external-process intermediates.
///
/// Cleanup is synchronous in `Drop` so cancellation, shutdown, and every
/// error path remove the complete attempt even when the async operation is
/// dropped between await points.
pub(crate) struct TempDirectory {
    path: Option<PathBuf>,
}

impl TempDirectory {
    pub(crate) async fn reserve(path: PathBuf) -> Result<Self, io::Error> {
        let Some(parent) = path.parent() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "temporary directory has no parent directory",
            ));
        };
        let metadata = fs::symlink_metadata(parent)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "temporary directory parent is not a real directory",
            ));
        }
        tokio::fs::create_dir(&path).await.map(|_| Self { path: Some(path) })
    }

    pub(crate) fn path(&self) -> &Path {
        self.path.as_deref().expect("temporary directory must be armed")
    }
}

impl Drop for TempDirectory {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            let _ = fs::remove_dir_all(path);
        }
    }
}

pub(crate) async fn publish_or_reuse(
    temporary: &Path,
    destination: &Path,
) -> Result<PublishOutcome, PublishError> {
    let Some(temporary_parent) = temporary.parent() else {
        return Err(PublishError::InvalidWorkspace);
    };
    let Some(destination_parent) = destination.parent() else {
        return Err(PublishError::InvalidWorkspace);
    };
    let temporary_parent_metadata = fs::symlink_metadata(temporary_parent)
        .map_err(|source| PublishError::Io { path: temporary_parent.to_owned(), source })?;
    let destination_parent_metadata = fs::symlink_metadata(destination_parent)
        .map_err(|source| PublishError::Io { path: destination_parent.to_owned(), source })?;
    if temporary_parent_metadata.file_type().is_symlink()
        || !temporary_parent_metadata.is_dir()
        || destination_parent_metadata.file_type().is_symlink()
        || !destination_parent_metadata.is_dir()
    {
        return Err(PublishError::InvalidWorkspace);
    }
    require_regular(temporary).await?;

    match tokio::fs::hard_link(temporary, destination).await {
        Ok(()) => Ok(PublishOutcome::Published),
        Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
            compare_or_conflict(temporary, destination).await
        }
        Err(source) => Err(PublishError::Io { path: destination.to_owned(), source }),
    }
}

async fn compare_or_conflict(
    temporary: &Path,
    destination: &Path,
) -> Result<PublishOutcome, PublishError> {
    require_regular(destination).await?;
    let temporary_digest = sha256_file(temporary).await?;
    let destination_digest = sha256_file(destination).await?;
    if temporary_digest == destination_digest {
        Ok(PublishOutcome::Reused)
    } else {
        Err(PublishError::DestinationConflict(destination.to_owned()))
    }
}

async fn require_regular(path: &Path) -> Result<(), PublishError> {
    let metadata = tokio::fs::symlink_metadata(path)
        .await
        .map_err(|source| PublishError::Io { path: path.to_owned(), source })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(PublishError::NotRegular(path.to_owned()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[tokio::test]
    async fn publication_reuses_only_identical_destination_content() {
        let root = std::env::temp_dir().join(format!("sooqa-publication-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&root).await.expect("test root should exist");
        let destination = root.join("final.bin");
        let temporary = root.join(".sooqa-http-test.tmp");
        tokio::fs::write(&temporary, b"same").await.expect("temporary should be written");
        tokio::fs::write(&destination, b"same").await.expect("destination should be written");
        assert_eq!(
            publish_or_reuse(&temporary, &destination).await.expect("reuse should succeed"),
            PublishOutcome::Reused
        );

        tokio::fs::write(&temporary, b"different").await.expect("temporary should be rewritten");
        assert!(matches!(
            publish_or_reuse(&temporary, &destination).await,
            Err(PublishError::DestinationConflict(_))
        ));
        tokio::fs::remove_dir_all(root).await.expect("test root should be removed");
    }

    #[tokio::test]
    async fn dropped_artifact_guard_removes_reserved_file() {
        let root = std::env::temp_dir().join(format!("sooqa-artifact-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&root).await.expect("test root should exist");
        let path = root.join(".sooqa-test.tmp");
        {
            let _artifact =
                TempArtifact::reserve(path.clone()).await.expect("artifact should reserve");
            assert!(path.exists());
        }
        assert!(!path.exists());
        tokio::fs::remove_dir_all(root).await.expect("test root should be removed");
    }

    #[tokio::test]
    async fn dropped_directory_guard_removes_all_attempt_sidecars() {
        let root = std::env::temp_dir().join(format!("sooqa-directory-guard-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&root).await.expect("test root should exist");
        let attempt = root.join(".sooqa-attempt");
        {
            let guard =
                TempDirectory::reserve(attempt.clone()).await.expect("directory should reserve");
            tokio::fs::write(guard.path().join("final.mp4"), b"final")
                .await
                .expect("final output should be written");
            tokio::fs::write(guard.path().join("audio.m4a"), b"sidecar")
                .await
                .expect("sidecar should be written");
        }
        assert!(!attempt.exists());
        tokio::fs::remove_dir_all(root).await.expect("test root should be removed");
    }

    #[tokio::test]
    async fn publication_can_hard_link_from_an_attempt_directory() {
        let root =
            std::env::temp_dir().join(format!("sooqa-publication-directory-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&root).await.expect("test root should exist");
        let attempt = root.join(".sooqa-attempt");
        tokio::fs::create_dir(&attempt).await.expect("attempt directory should exist");
        let temporary = attempt.join("final.mp4");
        let destination = root.join("final.mp4");
        tokio::fs::write(&temporary, b"same").await.expect("temporary should be written");

        assert_eq!(
            publish_or_reuse(&temporary, &destination).await.expect("publication should succeed"),
            PublishOutcome::Published
        );
        assert_eq!(
            tokio::fs::read(&destination).await.expect("destination should be readable"),
            b"same"
        );
        tokio::fs::remove_dir_all(root).await.expect("test root should be removed");
    }
}
