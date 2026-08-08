use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use thiserror::Error;
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

/// Telegram update receipts are deliberately process-local.  They are a
/// delivery optimization, not application state; the five-table reset keeps
/// retries and business effects in `ingests`, `media`, `posts`, and jobs.
#[derive(Clone, Default)]
pub struct TelegramRepository {
    receipts: Arc<Mutex<HashMap<i64, TelegramUpdateReceipt>>>,
}

impl TelegramRepository {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub async fn claim_update(
        &self,
        update_id: i64,
    ) -> Result<TelegramUpdateClaimResult, TelegramRepositoryError> {
        if update_id <= 0 {
            return Err(TelegramRepositoryError::InvalidUpdateId(update_id));
        }
        let mut receipts =
            self.receipts.lock().map_err(|_| TelegramRepositoryError::LockPoisoned)?;
        let now = OffsetDateTime::now_utc();
        if let Some(receipt) = receipts.get_mut(&update_id) {
            if receipt.completed_at.is_some() {
                return Ok(TelegramUpdateClaimResult::Completed);
            }
            if receipt.claimed_at.is_some_and(|claimed_at| claimed_at > now - Duration::minutes(5))
            {
                return Ok(TelegramUpdateClaimResult::InProgress);
            }
            receipt.claim_token = Some(Uuid::new_v4());
            receipt.claimed_at = Some(now);
            return Ok(TelegramUpdateClaimResult::Claimed(TelegramUpdateClaim {
                update_id,
                claim_token: receipt.claim_token.expect("claim token was set"),
            }));
        }
        let token = Uuid::new_v4();
        receipts.insert(
            update_id,
            TelegramUpdateReceipt {
                update_id,
                received_at: now,
                claim_token: Some(token),
                claimed_at: Some(now),
                completed_at: None,
            },
        );
        Ok(TelegramUpdateClaimResult::Claimed(TelegramUpdateClaim {
            update_id,
            claim_token: token,
        }))
    }

    pub async fn complete_update(
        &self,
        claim: TelegramUpdateClaim,
    ) -> Result<(), TelegramRepositoryError> {
        let mut receipts =
            self.receipts.lock().map_err(|_| TelegramRepositoryError::LockPoisoned)?;
        let receipt = receipts
            .get_mut(&claim.update_id)
            .ok_or(TelegramRepositoryError::ClaimLost(claim.update_id))?;
        if receipt.completed_at.is_some() || receipt.claim_token != Some(claim.claim_token) {
            return Err(TelegramRepositoryError::ClaimLost(claim.update_id));
        }
        receipt.claim_token = None;
        receipt.claimed_at = None;
        receipt.completed_at = Some(OffsetDateTime::now_utc());
        Ok(())
    }

    pub async fn release_update(
        &self,
        claim: TelegramUpdateClaim,
    ) -> Result<(), TelegramRepositoryError> {
        let mut receipts =
            self.receipts.lock().map_err(|_| TelegramRepositoryError::LockPoisoned)?;
        let receipt = receipts
            .get_mut(&claim.update_id)
            .ok_or(TelegramRepositoryError::ClaimLost(claim.update_id))?;
        if receipt.completed_at.is_some() || receipt.claim_token != Some(claim.claim_token) {
            return Err(TelegramRepositoryError::ClaimLost(claim.update_id));
        }
        receipt.claim_token = None;
        receipt.claimed_at = None;
        Ok(())
    }

    pub async fn receipt(
        &self,
        update_id: i64,
    ) -> Result<Option<TelegramUpdateReceipt>, TelegramRepositoryError> {
        let receipts = self.receipts.lock().map_err(|_| TelegramRepositoryError::LockPoisoned)?;
        Ok(receipts.get(&update_id).cloned())
    }
}

#[derive(Debug, Clone)]
pub struct TelegramUpdateReceipt {
    pub update_id: i64,
    pub received_at: OffsetDateTime,
    pub claim_token: Option<Uuid>,
    pub claimed_at: Option<OffsetDateTime>,
    pub completed_at: Option<OffsetDateTime>,
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
    #[error("Telegram update claim was lost: {0}")]
    ClaimLost(i64),
    #[error("Telegram update repository lock was poisoned")]
    LockPoisoned,
}
