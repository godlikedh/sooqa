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
    #[error("temporary output and destination must share a real parent directory")]
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
    if temporary_parent != destination_parent {
        return Err(PublishError::InvalidWorkspace);
    }
    let parent_metadata = fs::symlink_metadata(temporary_parent)
        .map_err(|source| PublishError::Io { path: temporary_parent.to_owned(), source })?;
    if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
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
}
