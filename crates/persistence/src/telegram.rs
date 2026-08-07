use sqlx::{FromRow, postgres::PgPool};
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone)]
pub struct TelegramRepository {
    pool: PgPool,
}

impl TelegramRepository {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn claim_update(
        &self,
        update_id: i64,
    ) -> Result<TelegramUpdateClaimResult, TelegramRepositoryError> {
        if update_id <= 0 {
            return Err(TelegramRepositoryError::InvalidUpdateId(update_id));
        }
        let claim_token = sqlx::query_scalar::<_, Uuid>(
            r#"
            INSERT INTO telegram_update_receipts (update_id, claim_token, claimed_at)
            VALUES ($1, gen_random_uuid(), now())
            ON CONFLICT (update_id) DO UPDATE
            SET claim_token = gen_random_uuid(), claimed_at = now()
            WHERE telegram_update_receipts.completed_at IS NULL
              AND (
                  telegram_update_receipts.claim_token IS NULL
                  OR telegram_update_receipts.claimed_at < now() - interval '5 minutes'
              )
            RETURNING claim_token
            "#,
        )
        .bind(update_id)
        .fetch_optional(&self.pool)
        .await?;
        if let Some(claim_token) = claim_token {
            return Ok(TelegramUpdateClaimResult::Claimed(TelegramUpdateClaim {
                update_id,
                claim_token,
            }));
        }

        let state = sqlx::query_as::<_, TelegramUpdateState>(
            "SELECT completed_at FROM telegram_update_receipts WHERE update_id = $1",
        )
        .bind(update_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(TelegramRepositoryError::ClaimLost(update_id))?;
        if state.completed_at.is_some() {
            Ok(TelegramUpdateClaimResult::Completed)
        } else {
            Ok(TelegramUpdateClaimResult::InProgress)
        }
    }

    pub async fn complete_update(
        &self,
        claim: TelegramUpdateClaim,
    ) -> Result<(), TelegramRepositoryError> {
        let result = sqlx::query(
            "UPDATE telegram_update_receipts SET claim_token = NULL, claimed_at = NULL, completed_at = now() WHERE update_id = $1 AND claim_token = $2 AND completed_at IS NULL",
        )
        .bind(claim.update_id)
        .bind(claim.claim_token)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() != 1 {
            return Err(TelegramRepositoryError::ClaimLost(claim.update_id));
        }
        Ok(())
    }

    pub async fn release_update(
        &self,
        claim: TelegramUpdateClaim,
    ) -> Result<(), TelegramRepositoryError> {
        let result = sqlx::query(
            "UPDATE telegram_update_receipts SET claim_token = NULL, claimed_at = NULL WHERE update_id = $1 AND claim_token = $2 AND completed_at IS NULL",
        )
        .bind(claim.update_id)
        .bind(claim.claim_token)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() != 1 {
            return Err(TelegramRepositoryError::ClaimLost(claim.update_id));
        }
        Ok(())
    }

    pub async fn receipt(
        &self,
        update_id: i64,
    ) -> Result<Option<TelegramUpdateReceipt>, TelegramRepositoryError> {
        Ok(sqlx::query_as::<_, TelegramUpdateReceipt>(
            "SELECT update_id, received_at, claim_token, claimed_at, completed_at FROM telegram_update_receipts WHERE update_id = $1",
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
    pub claim_token: Option<Uuid>,
    pub claimed_at: Option<time::OffsetDateTime>,
    pub completed_at: Option<time::OffsetDateTime>,
}

#[derive(Debug, FromRow)]
struct TelegramUpdateState {
    completed_at: Option<time::OffsetDateTime>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct TelegramUpdateClaim {
    pub update_id: i64,
    pub claim_token: Uuid,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum TelegramUpdateClaimResult {
    Claimed(TelegramUpdateClaim),
    Completed,
    InProgress,
}

#[derive(Debug, Error)]
pub enum TelegramRepositoryError {
    #[error("Telegram update ID must be positive: {0}")]
    InvalidUpdateId(i64),
    #[error("Telegram update repository database operation failed: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Telegram update claim was lost: {0}")]
    ClaimLost(i64),
}
