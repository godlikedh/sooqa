//! Filesystem admission checks for bounded media work.

use std::path::{Path, PathBuf};

use thiserror::Error;

/// The largest number of large workspace operations the home deployment
/// promises to admit concurrently. A caller that supports a different
/// concurrency envelope must pass its own bound to
/// [`concurrent_operation_budget`].
pub const MAX_CONCURRENT_WORKSPACE_OPERATIONS: u64 = 2;

/// Return the aggregate operation budget that every concurrent admission must
/// observe. Saturation turns an impossible-to-represent budget into a safe
/// rejection when it is combined with a reserve.
pub const fn concurrent_operation_budget(operation_bytes: u64, concurrency: u64) -> u64 {
    operation_bytes.saturating_mul(concurrency)
}

/// A synthetic or observed filesystem free-space measurement.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DiskSpace {
    pub available_bytes: u64,
}

impl DiskSpace {
    pub const fn new(available_bytes: u64) -> Self {
        Self { available_bytes }
    }

    pub const fn admits(self, reserve_bytes: u64, required_bytes: u64) -> bool {
        match reserve_bytes.checked_add(required_bytes) {
            Some(required) => self.available_bytes >= required,
            None => false,
        }
    }

    pub fn check(
        self,
        path: impl AsRef<Path>,
        reserve_bytes: u64,
        required_bytes: u64,
    ) -> Result<Self, DiskAdmissionError> {
        if !self.admits(reserve_bytes, required_bytes) {
            return Err(DiskAdmissionError::Insufficient {
                path: path.as_ref().to_owned(),
                available_bytes: self.available_bytes,
                reserve_bytes,
                required_bytes,
            });
        }
        Ok(self)
    }
}

/// Read the unprivileged free bytes for the filesystem containing `path` and
/// require both the configured reserve and the next bounded operation's
/// maximum workspace demand to remain available.
pub fn check_disk_space(
    path: impl AsRef<Path>,
    reserve_bytes: u64,
    required_bytes: u64,
) -> Result<DiskSpace, DiskAdmissionError> {
    let path = path.as_ref();
    let space = filesystem_space(path)?;
    space.check(path, reserve_bytes, required_bytes)
}

#[cfg(unix)]
fn filesystem_space(path: &Path) -> Result<DiskSpace, DiskAdmissionError> {
    let stats = nix::sys::statvfs::statvfs(path).map_err(|source| DiskAdmissionError::Stat {
        path: path.to_owned(),
        message: source.to_string(),
    })?;
    let available_blocks = u128::from(stats.blocks_available());
    let fragment_size = u128::from(stats.fragment_size());
    let available_bytes = available_blocks
        .checked_mul(fragment_size)
        .and_then(|value| u64::try_from(value).ok())
        .ok_or_else(|| DiskAdmissionError::Stat {
            path: path.to_owned(),
            message: "filesystem free-space value overflowed u64".to_owned(),
        })?;
    Ok(DiskSpace::new(available_bytes))
}

#[cfg(not(unix))]
fn filesystem_space(path: &Path) -> Result<DiskSpace, DiskAdmissionError> {
    Err(DiskAdmissionError::Stat {
        path: path.to_owned(),
        message: "filesystem free-space checks are unsupported on this platform".to_owned(),
    })
}

#[derive(Debug, Clone, Eq, PartialEq, Error)]
pub enum DiskAdmissionError {
    #[error("could not inspect free space for {path}: {message}")]
    Stat { path: PathBuf, message: String },
    #[error(
        "work volume {path} has {available_bytes} free bytes, below the {reserve_bytes}-byte reserve plus {required_bytes} bytes required for the supported concurrent operation budget"
    )]
    Insufficient { path: PathBuf, available_bytes: u64, reserve_bytes: u64, required_bytes: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synthetic_space_requires_reserve_and_operation_budget() {
        let space = DiskSpace::new(120);
        assert!(space.check("/synthetic", 20, 100).is_ok());
        assert!(matches!(
            space.check("/synthetic", 21, 100),
            Err(DiskAdmissionError::Insufficient { available_bytes: 120, .. })
        ));
        assert!(space.check("/synthetic", u64::MAX, 1).is_err());

        let recovered = DiskSpace::new(121);
        assert!(recovered.check("/synthetic", 21, 100).is_ok());
        assert_eq!(concurrent_operation_budget(u64::MAX, 2), u64::MAX);
    }

    #[test]
    fn synthetic_two_worker_budget_preserves_the_reserve() {
        let reserve = 100;
        let per_worker = 400;
        let aggregate =
            concurrent_operation_budget(per_worker, MAX_CONCURRENT_WORKSPACE_OPERATIONS);
        let before = DiskSpace::new(reserve + aggregate);

        // Both workers can observe `before` concurrently. Requiring the full
        // two-worker budget on each admission makes their combined peak end
        // exactly at the reserve rather than below it.
        assert!(before.check("/synthetic", reserve, aggregate).is_ok());
        assert!(before.check("/synthetic", reserve, aggregate).is_ok());
        let after_both = DiskSpace::new(before.available_bytes - (per_worker * 2));
        assert_eq!(after_both.available_bytes, reserve);
        assert!(after_both.check("/synthetic", reserve, aggregate).is_err());
        assert!(
            DiskSpace::new(before.available_bytes - 1)
                .check("/synthetic", reserve, aggregate)
                .is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn current_filesystem_space_is_nonzero_for_a_real_directory() {
        let space = check_disk_space(".", 0, 1).expect("the test filesystem should be writable");
        assert!(space.available_bytes > 0);
    }
}
