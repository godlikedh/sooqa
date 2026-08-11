use std::{
    collections::HashSet,
    fs::{self, FileType},
    io,
    path::{Component, Path, PathBuf},
    time::{Duration, SystemTime},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::OffsetDateTime;
use tokio::fs as async_fs;
use uuid::Uuid;

const WORKSPACE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub enum WorkspaceArea {
    Source,
    Normalized,
    Frames,
    Previews,
    Logs,
}

impl WorkspaceArea {
    const fn directory_name(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Normalized => "normalized",
            Self::Frames => "frames",
            Self::Previews => "previews",
            Self::Logs => "logs",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkspaceManifest {
    pub schema_version: u32,
    pub job_id: Uuid,
    pub created_at: OffsetDateTime,
    pub entries: Vec<ManifestEntry>,
}

impl WorkspaceManifest {
    pub fn new(job_id: Uuid) -> Self {
        Self {
            schema_version: WORKSPACE_SCHEMA_VERSION,
            job_id,
            created_at: OffsetDateTime::now_utc(),
            entries: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub relative_path: String,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone)]
pub struct MediaWorkspace {
    jobs_root: PathBuf,
    root: PathBuf,
    job_id: Uuid,
}

impl MediaWorkspace {
    pub async fn create(work_root: impl AsRef<Path>, job_id: Uuid) -> Result<Self, WorkspaceError> {
        let work_root = work_root.as_ref().to_owned();
        ensure_directory(&work_root).await?;
        let work_root = async_fs::canonicalize(&work_root)
            .await
            .map_err(|source| WorkspaceError::Io { path: work_root.clone(), source })?;
        let jobs_root = work_root.join("jobs");
        ensure_directory(&jobs_root).await?;
        let jobs_root = async_fs::canonicalize(&jobs_root)
            .await
            .map_err(|source| WorkspaceError::Io { path: jobs_root.clone(), source })?;
        let root = jobs_root.join(job_id.to_string());
        ensure_directory(&root).await?;

        for area in [
            WorkspaceArea::Source,
            WorkspaceArea::Normalized,
            WorkspaceArea::Frames,
            WorkspaceArea::Previews,
            WorkspaceArea::Logs,
        ] {
            let area_path = root.join(area.directory_name());
            ensure_directory(&area_path).await?;
            set_restrictive_permissions(&area_path).await?;
        }
        set_restrictive_permissions(&root).await?;

        Ok(Self { jobs_root, root, job_id })
    }

    pub fn job_id(&self) -> Uuid {
        self.job_id
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Re-check the fixed workspace boundary immediately before a worker uses it.
    ///
    /// Plans may outlive the directory checks performed while they were built. A
    /// worker therefore validates the root and every fixed area again before it
    /// opens an input or publishes an output.
    pub fn validate(&self) -> Result<(), WorkspaceError> {
        ensure_real_directory_sync(&self.jobs_root)?;
        ensure_real_directory_sync(&self.root)?;
        for area in [
            WorkspaceArea::Source,
            WorkspaceArea::Normalized,
            WorkspaceArea::Frames,
            WorkspaceArea::Previews,
            WorkspaceArea::Logs,
        ] {
            ensure_real_directory_sync(&self.root.join(area.directory_name()))?;
        }
        Ok(())
    }

    pub fn path(&self, area: WorkspaceArea, file_name: &str) -> Result<PathBuf, WorkspaceError> {
        validate_file_name(file_name)?;
        ensure_real_directory_sync(&self.jobs_root)?;
        ensure_real_directory_sync(&self.root)?;
        let area_path = self.root.join(area.directory_name());
        ensure_real_directory_sync(&area_path)?;
        let path = area_path.join(file_name);
        reject_existing_symlink(&path)?;
        Ok(path)
    }

    pub fn manifest_path(&self) -> Result<PathBuf, WorkspaceError> {
        ensure_real_directory_sync(&self.jobs_root)?;
        ensure_real_directory_sync(&self.root)?;
        let path = self.root.join("manifest.json");
        reject_existing_symlink(&path)?;
        Ok(path)
    }

    pub async fn write_manifest(&self, manifest: &WorkspaceManifest) -> Result<(), WorkspaceError> {
        if manifest.job_id != self.job_id {
            return Err(WorkspaceError::ManifestJobMismatch {
                expected: self.job_id,
                actual: manifest.job_id,
            });
        }
        let manifest_path = self.manifest_path()?;
        let temporary_path = self.root.join(format!(".manifest-{}.tmp", Uuid::new_v4()));
        reject_existing_symlink(&temporary_path)?;
        let data = serde_json::to_vec_pretty(manifest)?;
        async_fs::write(&temporary_path, data)
            .await
            .map_err(|source| WorkspaceError::Io { path: temporary_path.clone(), source })?;
        set_restrictive_permissions(&temporary_path).await?;
        async_fs::rename(&temporary_path, &manifest_path)
            .await
            .map_err(|source| WorkspaceError::Io { path: manifest_path, source })?;
        Ok(())
    }

    pub async fn read_manifest(&self) -> Result<WorkspaceManifest, WorkspaceError> {
        let path = self.manifest_path()?;
        let data = async_fs::read(&path)
            .await
            .map_err(|source| WorkspaceError::Io { path: path.clone(), source })?;
        Ok(serde_json::from_slice(&data)?)
    }

    pub async fn cleanup(&self) -> Result<(), WorkspaceError> {
        if self.root.parent() != Some(self.jobs_root.as_path()) {
            return Err(WorkspaceError::OutsideConfiguredRoot(self.root.clone()));
        }
        let jobs_metadata = async_fs::symlink_metadata(&self.jobs_root)
            .await
            .map_err(|source| WorkspaceError::Io { path: self.jobs_root.clone(), source })?;
        if jobs_metadata.file_type().is_symlink() {
            return Err(WorkspaceError::Symlink(self.jobs_root.clone()));
        }
        if !jobs_metadata.is_dir() {
            return Err(WorkspaceError::NotDirectory(self.jobs_root.clone()));
        }
        let metadata = match async_fs::symlink_metadata(&self.root).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(source) => {
                return Err(WorkspaceError::Io { path: self.root.clone(), source });
            }
        };
        if metadata.file_type().is_symlink() {
            return Err(WorkspaceError::Symlink(self.root.clone()));
        }
        if !metadata.is_dir() {
            return Err(WorkspaceError::NotDirectory(self.root.clone()));
        }
        async_fs::remove_dir_all(&self.root)
            .await
            .map_err(|source| WorkspaceError::Io { path: self.root.clone(), source })?;
        Ok(())
    }

    /// Remove one validated workspace without creating it first. The ID is a
    /// UUID supplied by a typed durable job, never an arbitrary filesystem
    /// path.
    pub async fn cleanup_existing(
        work_root: impl AsRef<Path>,
        workspace_id: Uuid,
    ) -> Result<(), WorkspaceError> {
        let Some(jobs_root) = existing_jobs_root(work_root.as_ref()).await? else {
            return Ok(());
        };
        let workspace_root = jobs_root.join(workspace_id.to_string());
        remove_workspace_root(&jobs_root, &workspace_root).await
    }

    /// Reconcile old whole workspaces in a bounded batch. The caller supplies
    /// workspace IDs currently protected by durable ingest/media/job state;
    /// unprotected directories older than the retention age are safe orphan
    /// candidates after a worker crash.
    pub async fn scavenge_completed_workspaces(
        work_root: impl AsRef<Path>,
        max_age: Duration,
        protected_workspace_ids: &[Uuid],
        limit: usize,
    ) -> Result<u64, WorkspaceError> {
        if limit == 0 {
            return Ok(0);
        }
        let Some(jobs_root) = existing_jobs_root(work_root.as_ref()).await? else {
            return Ok(0);
        };
        let protected_workspace_ids =
            protected_workspace_ids.iter().copied().collect::<HashSet<_>>();
        let cutoff = SystemTime::now().checked_sub(max_age).unwrap_or(SystemTime::UNIX_EPOCH);
        let mut entries = async_fs::read_dir(&jobs_root)
            .await
            .map_err(|source| WorkspaceError::Io { path: jobs_root.clone(), source })?;
        let mut removed = 0;
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|source| WorkspaceError::Io { path: jobs_root.clone(), source })?
        {
            let Some(workspace_id) = entry.file_name().to_str().and_then(|name| name.parse().ok())
            else {
                continue;
            };
            if protected_workspace_ids.contains(&workspace_id) {
                continue;
            }
            let workspace_root = entry.path();
            let metadata = async_fs::symlink_metadata(&workspace_root)
                .await
                .map_err(|source| WorkspaceError::Io { path: workspace_root.clone(), source })?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                continue;
            }
            if metadata.modified().ok().is_none_or(|modified| modified > cutoff) {
                continue;
            }
            remove_workspace_root(&jobs_root, &workspace_root).await?;
            removed += 1;
            if removed >= limit as u64 {
                break;
            }
        }
        Ok(removed)
    }

    /// Remove only stale temporary artifacts from UUID-named job workspaces.
    /// Live jobs are supplied by the database caller and are never scanned.
    pub async fn scavenge_stale_artifacts(
        work_root: impl AsRef<Path>,
        max_age: Duration,
        live_job_ids: &[Uuid],
    ) -> Result<u64, WorkspaceError> {
        let work_root = work_root.as_ref();
        ensure_real_directory_sync(work_root)?;
        let jobs_root = work_root.join("jobs");
        match fs::symlink_metadata(&jobs_root) {
            Ok(_) => ensure_real_directory_sync(&jobs_root)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
            Err(source) => {
                return Err(WorkspaceError::Io { path: jobs_root.clone(), source });
            }
        }
        let mut jobs = match async_fs::read_dir(&jobs_root).await {
            Ok(jobs) => jobs,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
            Err(source) => {
                return Err(WorkspaceError::Io { path: jobs_root, source });
            }
        };
        let cutoff = SystemTime::now().checked_sub(max_age).unwrap_or(SystemTime::UNIX_EPOCH);
        let mut removed = 0;

        while let Some(entry) = jobs
            .next_entry()
            .await
            .map_err(|source| WorkspaceError::Io { path: jobs_root.clone(), source })?
        {
            let job_id = match entry.file_name().to_str().and_then(|name| name.parse().ok()) {
                Some(job_id) => job_id,
                None => continue,
            };
            if live_job_ids.contains(&job_id) {
                continue;
            }
            let job_root = entry.path();
            let metadata = async_fs::symlink_metadata(&job_root)
                .await
                .map_err(|source| WorkspaceError::Io { path: job_root.clone(), source })?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                continue;
            }

            for area in [
                WorkspaceArea::Source,
                WorkspaceArea::Normalized,
                WorkspaceArea::Frames,
                WorkspaceArea::Previews,
                WorkspaceArea::Logs,
            ] {
                let area_path = job_root.join(area.directory_name());
                match fs::symlink_metadata(&area_path) {
                    Ok(metadata) => ensure_directory_metadata(
                        &area_path,
                        metadata.file_type(),
                        metadata.is_dir(),
                    )?,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                    Err(source) => {
                        return Err(WorkspaceError::Io { path: area_path.clone(), source });
                    }
                }
                let mut files = match async_fs::read_dir(&area_path).await {
                    Ok(files) => files,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                    Err(source) => {
                        return Err(WorkspaceError::Io { path: area_path, source });
                    }
                };
                while let Some(file) = files
                    .next_entry()
                    .await
                    .map_err(|source| WorkspaceError::Io { path: area_path.clone(), source })?
                {
                    let name = file.file_name();
                    let Some(name) = name.to_str() else { continue };
                    if !is_temporary_artifact_name(name) {
                        continue;
                    }
                    let metadata = async_fs::symlink_metadata(file.path())
                        .await
                        .map_err(|source| WorkspaceError::Io { path: file.path(), source })?;
                    if metadata.file_type().is_symlink()
                        || !metadata.is_file()
                        || metadata.modified().ok().is_none_or(|modified| modified > cutoff)
                    {
                        continue;
                    }
                    async_fs::remove_file(file.path())
                        .await
                        .map_err(|source| WorkspaceError::Io { path: file.path(), source })?;
                    removed += 1;
                }
            }
        }

        Ok(removed)
    }
}

async fn existing_jobs_root(work_root: &Path) -> Result<Option<PathBuf>, WorkspaceError> {
    let work_root_metadata = match async_fs::symlink_metadata(work_root).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(WorkspaceError::Io { path: work_root.to_owned(), source });
        }
    };
    ensure_directory_metadata(
        work_root,
        work_root_metadata.file_type(),
        work_root_metadata.is_dir(),
    )?;
    let jobs_root = work_root.join("jobs");
    let jobs_metadata = match async_fs::symlink_metadata(&jobs_root).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(WorkspaceError::Io { path: jobs_root, source }),
    };
    ensure_directory_metadata(&jobs_root, jobs_metadata.file_type(), jobs_metadata.is_dir())?;
    Ok(Some(jobs_root))
}

async fn remove_workspace_root(
    jobs_root: &Path,
    workspace_root: &Path,
) -> Result<(), WorkspaceError> {
    if workspace_root.parent() != Some(jobs_root) {
        return Err(WorkspaceError::OutsideConfiguredRoot(workspace_root.to_owned()));
    }
    let metadata = match async_fs::symlink_metadata(workspace_root).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(WorkspaceError::Io { path: workspace_root.to_owned(), source });
        }
    };
    if metadata.file_type().is_symlink() {
        return Err(WorkspaceError::Symlink(workspace_root.to_owned()));
    }
    if !metadata.is_dir() {
        return Err(WorkspaceError::NotDirectory(workspace_root.to_owned()));
    }
    match async_fs::remove_dir_all(workspace_root).await {
        Ok(()) => Ok(()),
        // Another cleanup worker may win the race after our metadata check.
        // The desired postcondition is already true in that case.
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(WorkspaceError::Io { path: workspace_root.to_owned(), source }),
    }
}

fn is_temporary_artifact_name(name: &str) -> bool {
    name.starts_with(".sooqa-")
        || name.starts_with(".part-")
        || name.starts_with(".manifest-")
        || name.contains(".tmp-")
        || (name.starts_with('.') && name.ends_with(".png"))
}

#[derive(Debug, Error)]
pub enum WorkspaceError {
    #[error("invalid workspace filename: {0}")]
    InvalidFileName(String),
    #[error("workspace path is outside the configured root: {0}")]
    OutsideConfiguredRoot(PathBuf),
    #[error("workspace path is a symlink: {0}")]
    Symlink(PathBuf),
    #[error("workspace path is not a directory: {0}")]
    NotDirectory(PathBuf),
    #[error("manifest belongs to job {actual}, expected job {expected}")]
    ManifestJobMismatch { expected: Uuid, actual: Uuid },
    #[error("workspace manifest format error: {0}")]
    ManifestFormat(#[from] serde_json::Error),
    #[error("workspace I/O failed for {path}: {source}")]
    Io { path: PathBuf, source: io::Error },
}

async fn ensure_directory(path: &Path) -> Result<(), WorkspaceError> {
    match async_fs::symlink_metadata(path).await {
        Ok(metadata) => ensure_directory_metadata(path, metadata.file_type(), metadata.is_dir()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            async_fs::create_dir(path)
                .await
                .map_err(|source| WorkspaceError::Io { path: path.to_owned(), source })?;
            let metadata = async_fs::symlink_metadata(path)
                .await
                .map_err(|source| WorkspaceError::Io { path: path.to_owned(), source })?;
            ensure_directory_metadata(path, metadata.file_type(), metadata.is_dir())
        }
        Err(source) => Err(WorkspaceError::Io { path: path.to_owned(), source }),
    }
}

fn ensure_directory_metadata(
    path: &Path,
    file_type: FileType,
    is_directory: bool,
) -> Result<(), WorkspaceError> {
    if file_type.is_symlink() {
        return Err(WorkspaceError::Symlink(path.to_owned()));
    }
    if !is_directory {
        return Err(WorkspaceError::NotDirectory(path.to_owned()));
    }
    Ok(())
}

fn ensure_real_directory_sync(path: &Path) -> Result<(), WorkspaceError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|source| WorkspaceError::Io { path: path.to_owned(), source })?;
    ensure_directory_metadata(path, metadata.file_type(), metadata.is_dir())
}

fn reject_existing_symlink(path: &Path) -> Result<(), WorkspaceError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(WorkspaceError::Symlink(path.to_owned()))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(WorkspaceError::Io { path: path.to_owned(), source }),
    }
}

fn validate_file_name(file_name: &str) -> Result<(), WorkspaceError> {
    if file_name.is_empty()
        || file_name == "."
        || file_name == ".."
        || file_name.contains('/')
        || file_name.contains('\\')
        || Path::new(file_name).is_absolute()
        || Path::new(file_name).components().any(|component| {
            matches!(component, Component::ParentDir | Component::RootDir | Component::Prefix(_))
        })
    {
        return Err(WorkspaceError::InvalidFileName(file_name.to_owned()));
    }
    Ok(())
}

async fn set_restrictive_permissions(path: &Path) -> Result<(), WorkspaceError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        async_fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .await
            .map_err(|source| WorkspaceError::Io { path: path.to_owned(), source })?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FileDigest, sha256_file};
    use tokio::fs as async_fs;

    fn test_root() -> PathBuf {
        std::env::temp_dir().join(format!("sooqa-workspace-{}", Uuid::new_v4()))
    }

    #[tokio::test]
    async fn creates_manifest_and_cleans_only_its_workspace() {
        let work_root = test_root();
        let outside = work_root.join("outside.txt");
        async_fs::create_dir_all(&work_root).await.expect("test root should be created");
        async_fs::write(&outside, b"keep me").await.expect("outside file should be written");
        let job_id = Uuid::new_v4();
        let workspace =
            MediaWorkspace::create(&work_root, job_id).await.expect("workspace should be created");
        let source_path = workspace
            .path(WorkspaceArea::Source, "source.bin")
            .expect("source path should be safe");
        async_fs::write(&source_path, b"hello").await.expect("source should be written");
        let digest = sha256_file(&source_path).await.expect("source should be hashed");
        let mut manifest = WorkspaceManifest::new(job_id);
        manifest.entries.push(ManifestEntry {
            relative_path: "source/source.bin".to_owned(),
            bytes: digest.bytes,
            sha256: digest.sha256,
        });
        workspace.write_manifest(&manifest).await.expect("manifest should be written");
        assert_eq!(workspace.read_manifest().await.expect("manifest should be readable"), manifest);

        workspace.cleanup().await.expect("workspace should be cleaned");
        assert!(!workspace.root().exists());
        assert_eq!(async_fs::read(&outside).await.expect("outside file should remain"), b"keep me");
        async_fs::remove_dir_all(work_root).await.expect("test root should be removed");
    }

    #[tokio::test]
    async fn scavenger_removes_old_job_artifacts_but_keeps_live_jobs() {
        let work_root = test_root();
        let stale_job = Uuid::new_v4();
        let live_job = Uuid::new_v4();
        let stale_workspace = MediaWorkspace::create(&work_root, stale_job)
            .await
            .expect("stale workspace should be created");
        let live_workspace = MediaWorkspace::create(&work_root, live_job)
            .await
            .expect("live workspace should be created");
        let stale_path = stale_workspace
            .path(WorkspaceArea::Source, ".sooqa-http-old.tmp")
            .expect("stale artifact path should be safe");
        let live_path = live_workspace
            .path(WorkspaceArea::Source, ".sooqa-ytdlp-live.tmp")
            .expect("live artifact path should be safe");
        async_fs::write(&stale_path, b"stale").await.expect("stale artifact should be written");
        async_fs::write(&live_path, b"live").await.expect("live artifact should be written");
        std::fs::OpenOptions::new()
            .write(true)
            .open(&stale_path)
            .expect("stale artifact should open")
            .set_modified(SystemTime::UNIX_EPOCH)
            .expect("stale artifact timestamp should update");

        let removed =
            MediaWorkspace::scavenge_stale_artifacts(&work_root, Duration::ZERO, &[live_job])
                .await
                .expect("scavenger should succeed");
        assert_eq!(removed, 1);
        assert!(!stale_path.exists());
        assert!(live_path.exists());

        stale_workspace.cleanup().await.expect("stale workspace should clean up");
        live_workspace.cleanup().await.expect("live workspace should clean up");
        async_fs::remove_dir_all(work_root).await.expect("test root should be removed");
    }

    #[tokio::test]
    async fn whole_workspace_cleanup_is_idempotent_and_scavenger_is_bounded() {
        let work_root = test_root();
        let stale = MediaWorkspace::create(&work_root, Uuid::new_v4())
            .await
            .expect("stale workspace should be created");
        let protected = MediaWorkspace::create(&work_root, Uuid::new_v4())
            .await
            .expect("protected workspace should be created");
        async_fs::write(
            stale.path(WorkspaceArea::Source, "source.bin").expect("stale source path"),
            b"stale",
        )
        .await
        .expect("stale source should be written");
        async_fs::write(
            protected
                .path(WorkspaceArea::Normalized, "canonical.mp4")
                .expect("protected normalized path"),
            b"protected",
        )
        .await
        .expect("protected output should be written");

        let removed = MediaWorkspace::scavenge_completed_workspaces(
            &work_root,
            Duration::ZERO,
            &[protected.job_id()],
            1,
        )
        .await
        .expect("workspace reconciliation should succeed");
        assert_eq!(removed, 1);
        assert!(!stale.root().exists());
        assert!(protected.root().exists());

        let (first_cleanup, second_cleanup) = tokio::join!(
            MediaWorkspace::cleanup_existing(&work_root, protected.job_id()),
            MediaWorkspace::cleanup_existing(&work_root, protected.job_id()),
        );
        first_cleanup.expect("first cleanup should succeed");
        second_cleanup.expect("racing cleanup should be harmless");
        assert!(!protected.root().exists());
        async_fs::remove_dir_all(work_root).await.expect("test root should be removed");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn whole_workspace_scavenger_never_follows_a_symlinked_root() {
        use std::os::unix::fs::symlink;

        let work_root = test_root();
        let outside = test_root();
        async_fs::create_dir_all(&outside).await.expect("outside root should be created");
        async_fs::create_dir_all(work_root.join("jobs")).await.expect("jobs root should exist");
        let workspace_id = Uuid::new_v4();
        symlink(&outside, work_root.join("jobs").join(workspace_id.to_string()))
            .expect("workspace symlink should be created");

        let removed =
            MediaWorkspace::scavenge_completed_workspaces(&work_root, Duration::ZERO, &[], 10)
                .await
                .expect("symlink should be ignored");
        assert_eq!(removed, 0);
        assert!(outside.exists());

        async_fs::remove_dir_all(work_root).await.expect("test root should be removed");
        async_fs::remove_dir_all(outside).await.expect("outside root should be removed");
    }

    #[tokio::test]
    async fn rejects_traversal_and_nested_output_names() {
        let work_root = test_root();
        let workspace = MediaWorkspace::create(&work_root, Uuid::new_v4())
            .await
            .expect("workspace should be created");

        for name in ["../outside", "/tmp/output", "nested/output", r"..\outside", ""] {
            assert!(matches!(
                workspace.path(WorkspaceArea::Source, name),
                Err(WorkspaceError::InvalidFileName(_))
            ));
        }

        workspace.cleanup().await.expect("workspace should be cleaned");
        async_fs::remove_dir_all(work_root).await.expect("test root should be removed");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_symlinked_workspace_area() {
        use std::os::unix::fs::symlink;

        let work_root = test_root();
        let outside = test_root();
        async_fs::create_dir_all(&outside).await.expect("outside root should be created");
        let workspace = MediaWorkspace::create(&work_root, Uuid::new_v4())
            .await
            .expect("workspace should be created");
        let source_dir = workspace.root().join("source");
        async_fs::remove_dir(&source_dir).await.expect("source directory should be removable");
        symlink(&outside, &source_dir).expect("test symlink should be created");

        assert!(matches!(
            workspace.path(WorkspaceArea::Source, "source.bin"),
            Err(WorkspaceError::Symlink(_))
        ));
        workspace.cleanup().await.expect("workspace should be cleaned");
        assert!(outside.exists(), "cleanup must not follow the symlink");
        async_fs::remove_dir_all(outside).await.expect("outside root should be removed");
        async_fs::remove_dir_all(work_root).await.expect("test root should be removed");
    }

    #[test]
    fn digest_value_can_be_embedded_in_a_manifest_entry() {
        let digest = FileDigest { bytes: 5, sha256: "abc".to_owned() };
        let entry = ManifestEntry {
            relative_path: "source/file".to_owned(),
            bytes: digest.bytes,
            sha256: digest.sha256,
        };
        assert_eq!(entry.bytes, 5);
        assert_eq!(entry.sha256, "abc");
    }
}
