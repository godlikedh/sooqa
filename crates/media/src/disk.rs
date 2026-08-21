//! Filesystem admission checks for bounded media work.

use std::path::{Path, PathBuf};

use thiserror::Error;

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
        "work volume {path} has {available_bytes} free bytes, below the {reserve_bytes}-byte reserve plus {required_bytes} bytes required for the next bounded operation"
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
    }

    #[test]
    fn synthetic_two_worker_budget_preserves_the_reserve() {
        let reserve = 100;
        let per_worker = 400;
        let after_first = DiskSpace::new(900 - per_worker);
        assert!(after_first.check("/synthetic", reserve, per_worker).is_ok());
        let after_second = DiskSpace::new(after_first.available_bytes - per_worker);
        assert_eq!(after_second.available_bytes, reserve);
        assert!(after_second.check("/synthetic", reserve, 1).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn current_filesystem_space_is_nonzero_for_a_real_directory() {
        let space = check_disk_space(".", 0, 1).expect("the test filesystem should be writable");
        assert!(space.available_bytes > 0);
    }
}
