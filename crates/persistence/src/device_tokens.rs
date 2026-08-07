use sha2::{Digest, Sha256};
use sqlx::{FromRow, PgPool};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Clone)]
pub struct DeviceTokenRepository {
    pool: PgPool,
}

impl DeviceTokenRepository {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn authenticate(
        &self,
        token: &str,
    ) -> Result<Option<DeviceToken>, DeviceTokenRepositoryError> {
        if token.is_empty() {
            return Ok(None);
        }

        let token_hash = hash_device_token(token);
        let device = sqlx::query_as::<_, DeviceTokenRow>(
            r#"
            SELECT id, name, token_prefix, scopes, created_at, last_used_at, revoked_at
            FROM device_tokens
            WHERE token_hash = $1 AND revoked_at IS NULL
            "#,
        )
        .bind(token_hash)
        .fetch_optional(&self.pool)
        .await?;

        let Some(device) = device else {
            return Ok(None);
        };

        sqlx::query("UPDATE device_tokens SET last_used_at = now() WHERE id = $1")
            .bind(device.id)
            .execute(&self.pool)
            .await?;

        Ok(Some(device.into_device()))
    }
}

#[derive(Debug, Clone)]
pub struct DeviceToken {
    pub id: Uuid,
    pub name: String,
    pub token_prefix: String,
    pub scopes: Vec<String>,
    pub created_at: OffsetDateTime,
    pub last_used_at: Option<OffsetDateTime>,
    pub revoked_at: Option<OffsetDateTime>,
}

pub fn hash_device_token(token: &str) -> Vec<u8> {
    Sha256::digest(token.as_bytes()).to_vec()
}

#[derive(Debug, FromRow)]
struct DeviceTokenRow {
    id: Uuid,
    name: String,
    token_prefix: String,
    scopes: Vec<String>,
    created_at: OffsetDateTime,
    last_used_at: Option<OffsetDateTime>,
    revoked_at: Option<OffsetDateTime>,
}

impl DeviceTokenRow {
    fn into_device(self) -> DeviceToken {
        DeviceToken {
            id: self.id,
            name: self.name,
            token_prefix: self.token_prefix,
            scopes: self.scopes,
            created_at: self.created_at,
            last_used_at: self.last_used_at,
            revoked_at: self.revoked_at,
        }
    }
}

#[derive(Debug, Error)]
pub enum DeviceTokenRepositoryError {
    #[error("database operation failed: {0}")]
    Database(#[from] sqlx::Error),
}
