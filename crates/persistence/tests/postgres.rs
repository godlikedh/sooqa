use std::{collections::BTreeSet, env};

use sooqa_persistence::Database;

async fn database() -> Database {
    let url = env::var("DATABASE_URL").expect("DATABASE_URL must point to PostgreSQL");
    let database = Database::connect(&url, 5).await.expect("database should connect");
    database.migrate().await.expect("migration should apply");
    database
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn fresh_migration_contains_only_the_five_application_tables() {
    let database = database().await;
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
    assert_eq!(migration_count, 2);
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn database_constraints_fence_running_jobs_and_bound_media_digests() {
    let database = database().await;
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
    sqlx::query("DELETE FROM queue.jobs WHERE id = $1")
        .bind(job_id)
        .execute(database.pool())
        .await
        .expect("fixture should clean");
}
