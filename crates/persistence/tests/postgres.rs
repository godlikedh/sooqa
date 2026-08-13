use std::{collections::BTreeSet, env};

use serde_json::json;
use sooqa_persistence::Database;
use sqlx::postgres::PgPoolOptions;
use url::Url;
use uuid::Uuid;

async fn create_legacy_upgrade_database() -> (sqlx::PgPool, sqlx::PgPool, Url, String) {
    let base_url =
        Url::parse(&env::var("DATABASE_URL").expect("DATABASE_URL must point to PostgreSQL"))
            .expect("DATABASE_URL should be a PostgreSQL URL");
    let mut admin_url = base_url.clone();
    admin_url.set_path("/postgres");
    let admin_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(admin_url.as_str())
        .await
        .expect("admin database should connect");
    let database_name = format!("sooqa_upgrade_{}", Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE DATABASE \"{database_name}\""))
        .execute(&admin_pool)
        .await
        .expect("temporary upgrade database should be created");

    let mut target_url = base_url;
    target_url.set_path(&format!("/{database_name}"));
    let target_pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(target_url.as_str())
        .await
        .expect("temporary upgrade database should connect");
    (admin_pool, target_pool, target_url, database_name)
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn fresh_migration_contains_only_the_five_application_tables(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema IN ('public', 'queue') AND NOT (table_schema = 'public' AND table_name = '_sqlx_migrations') ORDER BY table_schema, table_name",
    )
    .fetch_all(database.pool())
    .await
    .expect("table inventory should load");
    let actual = rows.into_iter().collect::<BTreeSet<_>>();
    let expected = [
        ("public".to_owned(), "channels".to_owned()),
        ("public".to_owned(), "ingests".to_owned()),
        ("public".to_owned(), "media".to_owned()),
        ("public".to_owned(), "posts".to_owned()),
        ("queue".to_owned(), "jobs".to_owned()),
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    assert_eq!(actual, expected);

    let migration_count: i64 = sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations")
        .fetch_one(database.pool())
        .await
        .expect("migration table should exist");
    assert_eq!(migration_count, 4);
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn database_constraints_fence_running_jobs_and_bound_media_digests(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let invalid_digest =
        sqlx::query("INSERT INTO media (kind, canonical_sha256) VALUES ('video', $1)")
            .bind(vec![1_u8; 31])
            .execute(database.pool())
            .await;
    assert!(invalid_digest.is_err());

    let job_id: uuid::Uuid = sqlx::query_scalar(
        "INSERT INTO queue.jobs (kind) VALUES ('cleanup_workspace') RETURNING id",
    )
    .fetch_one(database.pool())
    .await
    .expect("job should insert");
    let running_without_lease =
        sqlx::query("UPDATE queue.jobs SET state = 'running' WHERE id = $1")
            .bind(job_id)
            .execute(database.pool())
            .await;
    assert!(running_without_lease.is_err());
}

#[tokio::test]
#[ignore = "requires PostgreSQL superuser permissions to create a temporary database"]
async fn workspace_migration_preserves_legacy_ids_and_reconciliation_protection() {
    let (admin_pool, target_pool, target_url, database_name) =
        create_legacy_upgrade_database().await;
    sqlx::raw_sql(include_str!("../../../migrations/0001_initial.sql"))
        .execute(&target_pool)
        .await
        .expect("0001 should apply to the legacy database");
    sqlx::raw_sql(include_str!("../../../migrations/0002_ingest_description.sql"))
        .execute(&target_pool)
        .await
        .expect("0002 should apply to the legacy database");

    let url_ingest_id = Uuid::new_v4();
    let telegram_ingest_id = Uuid::new_v4();
    let telegram_workspace_id = Uuid::new_v4();
    for (id, input_key, input_kind, input_json, source_url) in [
        (
            url_ingest_id,
            "legacy-url",
            "url",
            json!({"source": "https://example.test/legacy.mp4"}),
            Some("https://example.test/legacy.mp4"),
        ),
        (
            telegram_ingest_id,
            "legacy-telegram",
            "telegram_message",
            json!({"telegram_workspace_id": telegram_workspace_id}),
            None,
        ),
    ] {
        sqlx::query(
            "INSERT INTO ingests (id, input_key, request_hash, input_kind, state, submitted_via, input_json, source_url) VALUES ($1, $2, $3, $4, 'queued', $5, $6, $7)",
        )
        .bind(id)
        .bind(input_key)
        .bind(vec![1_u8; 32])
        .bind(input_kind)
        .bind(if input_kind == "url" { "api" } else { "telegram_bot" })
        .bind(input_json)
        .bind(source_url)
        .execute(&target_pool)
        .await
        .expect("legacy ingest should insert at the 0002 shape");
    }

    sqlx::raw_sql(include_str!("../../../migrations/0003_workspace_lifecycle.sql"))
        .execute(&target_pool)
        .await
        .expect("0003 should upgrade the legacy database");
    let workspace_ids: Vec<(Uuid, Uuid)> =
        sqlx::query_as("SELECT id, workspace_id FROM ingests ORDER BY input_key")
            .fetch_all(&target_pool)
            .await
            .expect("upgraded workspace IDs should be queryable");
    assert_eq!(
        workspace_ids,
        vec![(telegram_ingest_id, telegram_workspace_id), (url_ingest_id, url_ingest_id)]
    );

    let database = Database::connect(target_url.as_str(), 5)
        .await
        .expect("upgraded database should connect through the repository");
    let protected = database
        .jobs()
        .protected_workspace_ids()
        .await
        .expect("legacy workspaces should be visible to reconciliation");
    assert!(protected.contains(&url_ingest_id));
    assert!(protected.contains(&telegram_workspace_id));
    drop(database);
    target_pool.close().await;
    sqlx::query(&format!("DROP DATABASE \"{database_name}\""))
        .execute(&admin_pool)
        .await
        .expect("temporary upgrade database should be removed");
    admin_pool.close().await;
}
