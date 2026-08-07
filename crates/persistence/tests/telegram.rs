use std::env;

use sooqa_persistence::{Database, TelegramUpdateClaimResult};
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance"]
async fn telegram_update_claim_is_idempotent() {
    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must point to the integration database");
    let database = Database::connect(&database_url, 5).await.expect("database should be reachable");
    database.migrate().await.expect("migrations should succeed");

    let update_id = (Uuid::new_v4().as_u128() % 9_000_000_000_000_000_000 + 1) as i64;
    let repository = database.telegram();
    let claim = match repository.claim_update(update_id).await.expect("first claim should succeed")
    {
        TelegramUpdateClaimResult::Claimed(claim) => claim,
        other => panic!("expected an available first claim, got {other:?}"),
    };
    assert_eq!(
        repository.claim_update(update_id).await.expect("second claim should succeed"),
        TelegramUpdateClaimResult::InProgress
    );
    repository.release_update(claim).await.expect("claim should release");
    let retry =
        match repository.claim_update(update_id).await.expect("released claim should succeed") {
            TelegramUpdateClaimResult::Claimed(claim) => claim,
            other => panic!("expected a released claim to be available, got {other:?}"),
        };
    repository.complete_update(retry).await.expect("claim should complete");
    assert_eq!(
        repository.claim_update(update_id).await.expect("completed claim should succeed"),
        TelegramUpdateClaimResult::Completed
    );
    assert_eq!(
        repository
            .receipt(update_id)
            .await
            .expect("receipt lookup should succeed")
            .map(|receipt| (receipt.update_id, receipt.completed_at.is_some())),
        Some((update_id, true))
    );

    sqlx::query("DELETE FROM telegram_update_receipts WHERE update_id = $1")
        .bind(update_id)
        .execute(database.pool())
        .await
        .expect("test receipt should clean up");
}
