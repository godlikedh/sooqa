use std::env;

use sooqa_persistence::Database;
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
    assert!(repository.claim_update(update_id).await.expect("first claim should succeed"));
    assert!(!repository.claim_update(update_id).await.expect("second claim should succeed"));
    assert_eq!(
        repository
            .receipt(update_id)
            .await
            .expect("receipt lookup should succeed")
            .map(|receipt| receipt.update_id),
        Some(update_id)
    );

    sqlx::query("DELETE FROM telegram_update_receipts WHERE update_id = $1")
        .bind(update_id)
        .execute(database.pool())
        .await
        .expect("test receipt should clean up");
}
