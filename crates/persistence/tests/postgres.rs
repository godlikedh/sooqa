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

    for table in ["admins", "jobs", "job_attempts", "idempotency_records"] {
        let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(format!("public.{}", table))
            .fetch_one(database.pool())
            .await
            .expect("table existence query should succeed");
        assert!(exists, "expected table {} to exist", table);
    }
}
