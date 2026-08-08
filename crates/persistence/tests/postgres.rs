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
        "duplicate_candidate_events",
        "telegram_update_receipts",
        "target_channels",
        "channel_policies",
        "post_drafts",
        "publication_schedules",
        "publication_attempts",
        "published_posts",
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
        "media_assets_id_content_item_idx",
        "duplicate_candidates_pending_idx",
        "duplicate_candidate_events_candidate_idx",
        "duplicate_candidate_events_idempotency_idx",
        "idempotency_records_storage_reservation_idx",
        "idempotency_records_storage_asset_idx",
        "publication_schedules_due_idx",
        "publication_attempts_schedule_idx",
        "publication_attempts_running_schedule_idx",
        "published_posts_content_channel_idx",
    ] {
        let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(format!("public.{}", index))
            .fetch_one(database.pool())
            .await
            .expect("index existence query should succeed");
        assert!(exists, "expected index {} to exist", index);
    }
}

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance"]
async fn database_constraints_protect_durable_invariants() {
    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must point to the integration database");
    let database = Database::connect(&database_url, 5).await.expect("database should be reachable");
    database.migrate().await.expect("migrations should succeed");

    let first_content = uuid::Uuid::new_v4();
    let second_content = uuid::Uuid::new_v4();
    let canonical_asset = uuid::Uuid::new_v4();
    let other_asset = uuid::Uuid::new_v4();
    let preview_asset = uuid::Uuid::new_v4();
    let job_id = uuid::Uuid::new_v4();

    sqlx::query("INSERT INTO content_items (id, kind) VALUES ($1, 'video'), ($2, 'video')")
        .bind(first_content)
        .bind(second_content)
        .execute(database.pool())
        .await
        .expect("content fixtures should insert");
    sqlx::query(
        r#"
        INSERT INTO media_assets (id, content_item_id, role, media_kind, sha256)
        VALUES
            ($1, $3, 'canonical', 'video', $4),
            ($2, $5, 'canonical', 'video', $6),
            ($7, $3, 'preview', 'video', NULL)
        "#,
    )
    .bind(canonical_asset)
    .bind(other_asset)
    .bind(first_content)
    .bind(vec![7_u8; 32])
    .bind(second_content)
    .bind(vec![8_u8; 32])
    .bind(preview_asset)
    .execute(database.pool())
    .await
    .expect("asset fixtures should insert");

    let invalid_digest = sqlx::query(
        "INSERT INTO media_assets (content_item_id, role, media_kind, sha256) VALUES ($1, 'original', 'video', $2)",
    )
    .bind(first_content)
    .bind(vec![1_u8; 31])
    .execute(database.pool())
    .await;
    assert!(invalid_digest.is_err(), "database should reject malformed SHA-256 values");

    sqlx::query("UPDATE content_items SET canonical_asset_id = $2 WHERE id = $1")
        .bind(first_content)
        .bind(canonical_asset)
        .execute(database.pool())
        .await
        .expect("same-item canonical pointer should be accepted");

    let wrong_owner = sqlx::query("UPDATE content_items SET canonical_asset_id = $2 WHERE id = $1")
        .bind(first_content)
        .bind(other_asset)
        .execute(database.pool())
        .await;
    assert!(wrong_owner.is_err(), "database should reject a canonical asset owned by another item");

    let wrong_role = sqlx::query("UPDATE content_items SET canonical_asset_id = $2 WHERE id = $1")
        .bind(first_content)
        .bind(preview_asset)
        .execute(database.pool())
        .await;
    assert!(wrong_role.is_err(), "database should reject a non-canonical asset pointer");

    let changed_role = sqlx::query("UPDATE media_assets SET role = 'preview' WHERE id = $1")
        .bind(canonical_asset)
        .execute(database.pool())
        .await;
    assert!(changed_role.is_err(), "database should protect a referenced canonical asset");

    let deleted_canonical = sqlx::query("DELETE FROM media_assets WHERE id = $1")
        .bind(canonical_asset)
        .execute(database.pool())
        .await;
    assert!(deleted_canonical.is_err(), "database should protect a referenced canonical asset");

    sqlx::query("INSERT INTO jobs (id, job_type) VALUES ($1, 'cleanup_workspace')")
        .bind(job_id)
        .execute(database.pool())
        .await
        .expect("job fixture should insert");
    let running_without_lease = sqlx::query("UPDATE jobs SET status = 'running' WHERE id = $1")
        .bind(job_id)
        .execute(database.pool())
        .await;
    assert!(running_without_lease.is_err(), "running jobs must have a lease");

    let invalid_attempt = sqlx::query(
        "INSERT INTO job_attempts (job_id, attempt_number, status) VALUES ($1, 1, 'bogus')",
    )
    .bind(job_id)
    .execute(database.pool())
    .await;
    assert!(invalid_attempt.is_err(), "job attempts must use known statuses");

    sqlx::query("UPDATE content_items SET canonical_asset_id = NULL WHERE id = $1")
        .bind(first_content)
        .execute(database.pool())
        .await
        .expect("canonical pointer should clear");
    sqlx::query("DELETE FROM jobs WHERE id = $1")
        .bind(job_id)
        .execute(database.pool())
        .await
        .expect("job fixture should clean up");
    sqlx::query("DELETE FROM media_assets WHERE content_item_id IN ($1, $2)")
        .bind(first_content)
        .bind(second_content)
        .execute(database.pool())
        .await
        .expect("asset fixtures should clean up");
    sqlx::query("DELETE FROM content_items WHERE id IN ($1, $2)")
        .bind(first_content)
        .bind(second_content)
        .execute(database.pool())
        .await
        .expect("content fixtures should clean up");
}
