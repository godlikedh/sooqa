use std::io::Read;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{fs::File, io::AsyncReadExt};

const HASH_BUFFER_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct FileDigest {
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Error)]
pub enum HashError {
    #[error("could not hash {path}: {source}")]
    Io { path: PathBuf, source: std::io::Error },
}

pub async fn sha256_file(path: impl AsRef<Path>) -> Result<FileDigest, HashError> {
    let path = path.as_ref().to_owned();
    let mut file =
        File::open(&path).await.map_err(|source| HashError::Io { path: path.clone(), source })?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; HASH_BUFFER_BYTES];
    let mut bytes = 0_u64;

    loop {
        let read = file
            .read(&mut buffer)
            .await
            .map_err(|source| HashError::Io { path: path.clone(), source })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        bytes = bytes.checked_add(read as u64).ok_or_else(|| HashError::Io {
            path: path.clone(),
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "file size overflowed while hashing",
            ),
        })?;
    }

    Ok(FileDigest { bytes, sha256: hex_digest(&hasher.finalize()) })
}

pub(crate) fn sha256_bytes(bytes: &[u8]) -> FileDigest {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    FileDigest { bytes: bytes.len() as u64, sha256: hex_digest(&hasher.finalize()) }
}

pub(crate) fn sha256_file_sync(path: impl AsRef<Path>) -> Result<FileDigest, HashError> {
    let path = path.as_ref().to_owned();
    let mut file = std::fs::File::open(&path)
        .map_err(|source| HashError::Io { path: path.clone(), source })?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; HASH_BUFFER_BYTES];
    let mut bytes = 0_u64;

    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|source| HashError::Io { path: path.clone(), source })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        bytes = bytes.checked_add(read as u64).ok_or_else(|| HashError::Io {
            path: path.clone(),
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "file size overflowed while hashing",
            ),
        })?;
    }

    Ok(FileDigest { bytes, sha256: hex_digest(&hasher.finalize()) })
}

fn hex_digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(HEX[(byte >> 4) as usize] as char);
        value.push(HEX[(byte & 0x0f) as usize] as char);
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[tokio::test]
    async fn hashes_a_file_as_a_stream() {
        let path = std::env::temp_dir().join(format!("sooqa-hash-{}.bin", Uuid::new_v4()));
        tokio::fs::write(&path, b"hello").await.expect("test file should be written");

        let digest = sha256_file(&path).await.expect("hash should succeed");

        assert_eq!(digest.bytes, 5);
        assert_eq!(
            digest.sha256,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        tokio::fs::remove_file(path).await.expect("test file should be removed");
    }
}
