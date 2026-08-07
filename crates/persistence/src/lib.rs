//! Database connection and migration boundaries for sooqa.

mod device_tokens;
mod inbox;
mod jobs;
mod library;

pub use device_tokens::{
    DeviceToken, DeviceTokenRepository, DeviceTokenRepositoryError, hash_device_token,
};
pub use inbox::{CreateIngestResult, InboxRepository, InboxRepositoryError, SourceInspectionStart};
pub use jobs::{JobRepository, JobRepositoryError};
pub use library::{LibraryRepository, LibraryRepositoryError};

use sooqa_config::SecretString;
use sqlx::{
    migrate::MigrateError,
    postgres::{PgPool, PgPoolOptions},
};
use thiserror::Error;

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

#[derive(Clone)]
pub struct Database {
    pool: PgPool,
}

impl Database {
    pub async fn connect(database_url: &str, max_connections: u32) -> Result<Self, DatabaseError> {
        if database_url.is_empty() {
            return Err(DatabaseError::MissingUrl);
        }
        if max_connections == 0 {
            return Err(DatabaseError::InvalidMaxConnections);
        }

        let pool =
            PgPoolOptions::new().max_connections(max_connections).connect(database_url).await?;
        Ok(Self { pool })
    }

    pub async fn connect_secret(
        database_url: &SecretString,
        max_connections: u32,
    ) -> Result<Self, DatabaseError> {
        Self::connect(database_url.expose_secret(), max_connections).await
    }

    pub async fn migrate(&self) -> Result<(), DatabaseError> {
        MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    pub fn jobs(&self) -> JobRepository {
        JobRepository::new(self.pool.clone())
    }

    pub fn inbox(&self) -> InboxRepository {
        InboxRepository::new(self.pool.clone())
    }

    pub fn device_tokens(&self) -> DeviceTokenRepository {
        DeviceTokenRepository::new(self.pool.clone())
    }

    pub fn library(&self) -> LibraryRepository {
        LibraryRepository::new(self.pool.clone())
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error("database URL is not configured")]
    MissingUrl,
    #[error("database max_connections must be greater than zero")]
    InvalidMaxConnections,
    #[error("database connection failed: {0}")]
    Connect(#[from] sqlx::Error),
    #[error("database migration failed: {0}")]
    Migration(#[from] MigrateError),
}
