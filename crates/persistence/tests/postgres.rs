use std::env;

use sooqa_persistence::Database;

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance"]
async fn migrations_are_idempotent_and_create_core_tables() {
    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must point to the integration database");
    let database = Database::connect(&database_url, 5).await.expect("database should be reachable");

    database.migrate().await.expect("first migration should succeed");
    database.migrate().await.expect("second migration should be idempotent");

    for table in [
        "admins",
        "jobs",
        "job_attempts",
        "idempotency_records",
        "ingest_requests",
        "device_tokens",
        "content_items",
        "media_assets",
        "source_records",
        "tags",
        "content_item_tags",
        "storage_objects",
        "duplicate_candidates",
    ] {
        let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(format!("public.{}", table))
            .fetch_one(database.pool())
            .await
            .expect("table existence query should succeed");
        assert!(exists, "expected table {} to exist", table);
    }

    for index in [
        "media_assets_sha256_idx",
        "media_assets_canonical_sha256_idx",
        "media_assets_content_canonical_idx",
        "duplicate_candidates_pending_idx",
    ] {
        let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(format!("public.{}", index))
            .fetch_one(database.pool())
            .await
            .expect("index existence query should succeed");
        assert!(exists, "expected index {} to exist", index);
    }
}
