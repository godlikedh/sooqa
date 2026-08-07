use sqlx::{FromRow, postgres::PgPool};
use thiserror::Error;

#[derive(Clone)]
pub struct TelegramRepository {
    pool: PgPool,
}

impl TelegramRepository {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn claim_update(&self, update_id: i64) -> Result<bool, TelegramRepositoryError> {
        if update_id <= 0 {
            return Err(TelegramRepositoryError::InvalidUpdateId(update_id));
        }
        let result = sqlx::query(
            r#"
            INSERT INTO telegram_update_receipts (update_id)
            VALUES ($1)
            ON CONFLICT (update_id) DO NOTHING
            "#,
        )
        .bind(update_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn receipt(
        &self,
        update_id: i64,
    ) -> Result<Option<TelegramUpdateReceipt>, TelegramRepositoryError> {
        Ok(sqlx::query_as::<_, TelegramUpdateReceipt>(
            "SELECT update_id, received_at FROM telegram_update_receipts WHERE update_id = $1",
        )
        .bind(update_id)
        .fetch_optional(&self.pool)
        .await?)
    }
}

#[derive(Debug, FromRow)]
pub struct TelegramUpdateReceipt {
    pub update_id: i64,
    pub received_at: time::OffsetDateTime,
}

#[derive(Debug, Error)]
pub enum TelegramRepositoryError {
    #[error("Telegram update ID must be positive: {0}")]
    InvalidUpdateId(i64),
    #[error("Telegram update repository database operation failed: {0}")]
    Database(#[from] sqlx::Error),
}
