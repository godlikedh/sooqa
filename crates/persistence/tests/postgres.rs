use std::{borrow::Cow, collections::BTreeSet, env};

use serde_json::json;
use sooqa_jobs::{JobCommand, JobType};
use sooqa_persistence::{Database, PublishLease};
use sooqa_publisher::{PostState, PublicationAction};
use sqlx::postgres::PgPoolOptions;
use url::Url;
use uuid::Uuid;

const PREVIOUS_SUPPORTED_MIGRATION: i64 = 6;
static HEAD_MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

fn migrator_through(version: i64) -> sqlx::migrate::Migrator {
    let migrations = HEAD_MIGRATOR
        .iter()
        .filter(|migration| migration.version <= version)
        .cloned()
        .collect::<Vec<_>>();
    assert!(!migrations.is_empty(), "migration boundary must include the baseline");
    assert_eq!(
        migrations.last().map(|migration| migration.version),
        Some(version),
        "migration boundary must be present in the repository"
    );
    sqlx::migrate::Migrator {
        migrations: Cow::Owned(migrations),
        ignore_missing: HEAD_MIGRATOR.ignore_missing,
        locking: HEAD_MIGRATOR.locking,
        no_tx: HEAD_MIGRATOR.no_tx,
    }
}

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

async fn drop_legacy_upgrade_database(
    admin_pool: &sqlx::PgPool,
    target_pool: &sqlx::PgPool,
    database_name: &str,
) -> Result<(), sqlx::Error> {
    target_pool.close().await;
    let result = sqlx::query(&format!("DROP DATABASE IF EXISTS \"{database_name}\""))
        .execute(admin_pool)
        .await
        .map(|_| ());
    admin_pool.close().await;
    result
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
    assert_eq!(migration_count, HEAD_MIGRATOR.iter().count() as i64);
    let status_columns: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.columns WHERE table_name = 'ingests' AND column_name IN ('telegram_status_chat_id', 'telegram_status_message_id')",
    )
    .fetch_one(database.pool())
    .await
    .expect("ingest column inventory should load");
    assert_eq!(status_columns, 0);
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
async fn populated_previous_schema_migrates_through_head_and_preserves_records() {
    let (admin_pool, target_pool, target_url, database_name) =
        create_legacy_upgrade_database().await;
    let target_pool_for_task = target_pool.clone();
    let upgrade = tokio::spawn(async move {
        run_populated_upgrade(target_pool_for_task, target_url).await;
    });
    let upgrade_result = upgrade.await;
    let cleanup_result =
        drop_legacy_upgrade_database(&admin_pool, &target_pool, &database_name).await;
    if let Err(error) = cleanup_result {
        panic!("temporary upgrade database should be removed: {error}");
    }
    if let Err(panic) = upgrade_result {
        std::panic::resume_unwind(panic.into_panic());
    }
}

async fn run_populated_upgrade(target_pool: sqlx::PgPool, target_url: Url) {
    let before_workspace_telegram = Uuid::new_v4();
    let url_ingest_id = Uuid::new_v4();
    let telegram_ingest_id = Uuid::new_v4();

    migrator_through(2)
        .run(&target_pool)
        .await
        .expect("the previous database should have migration 0002 applied");
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
            json!({"telegram_workspace_id": before_workspace_telegram}),
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

    migrator_through(PREVIOUS_SUPPORTED_MIGRATION)
        .run(&target_pool)
        .await
        .expect("the supported previous schema should apply through migration 0006");
    let workspace_ids: Vec<(Uuid, Uuid)> =
        sqlx::query_as("SELECT id, workspace_id FROM ingests ORDER BY input_key")
            .fetch_all(&target_pool)
            .await
            .expect("workspace IDs should be backfilled at the previous boundary");
    assert_eq!(
        workspace_ids,
        vec![(telegram_ingest_id, before_workspace_telegram), (url_ingest_id, url_ingest_id)]
    );

    let active_workspace_id = Uuid::new_v4();
    let completed_ingest_id = Uuid::new_v4();
    let failed_ingest_id = Uuid::new_v4();
    let ready_media_id = Uuid::new_v4();
    let pending_media_id = Uuid::new_v4();
    let unknown_media_id = Uuid::new_v4();
    let missing_media_id = Uuid::new_v4();
    let channel_id = Uuid::new_v4();
    let queued_post_id = Uuid::new_v4();
    let published_post_id = Uuid::new_v4();

    for (id, kind, storage_state, digest, storage_chat_id, storage_message_id, telegram_file_id) in [
        (
            ready_media_id,
            "video",
            "ready",
            Some(vec![2_u8; 32]),
            Some(-1001234567890_i64),
            Some(101_i64),
            Some("legacy-file"),
        ),
        (pending_media_id, "image", "pending_storage", Some(vec![3_u8; 32]), None, None, None),
        (unknown_media_id, "animation", "storage_unknown", Some(vec![4_u8; 32]), None, None, None),
        (missing_media_id, "audio", "missing", None, None, None, None),
    ] {
        sqlx::query(
            "INSERT INTO media (id, kind, storage_state, canonical_sha256, title, description, tags, source_url, mime_type, width, height, duration_ms, file_size_bytes, telegram_storage_chat_id, telegram_storage_message_id, telegram_file_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 640, 360, 1200, 2048, $10, $11, $12)",
        )
        .bind(id)
        .bind(kind)
        .bind(storage_state)
        .bind(digest)
        .bind(format!("legacy-{kind}"))
        .bind("preserved description")
        .bind(vec!["legacy".to_owned(), kind.to_owned()])
        .bind(format!("https://example.test/{kind}.bin"))
        .bind(if kind == "video" { Some("video/mp4") } else { Some("image/jpeg") })
        .bind(storage_chat_id)
        .bind(storage_message_id)
        .bind(telegram_file_id)
        .execute(&target_pool)
        .await
        .expect("media rows should be valid at migration 0006");
    }

    sqlx::query(
        "UPDATE ingests SET state = 'downloading', workspace_id = $2, source_url = $3, supplied_description = $4 WHERE id = $1",
    )
    .bind(url_ingest_id)
    .bind(active_workspace_id)
    .bind("https://example.test/active.mp4")
    .bind("active legacy description")
    .execute(&target_pool)
    .await
    .expect("active legacy ingest should be writable at migration 0006");
    sqlx::query(
        "INSERT INTO ingests (id, input_key, request_hash, input_kind, state, submitted_via, input_json, source_url, media_id, workspace_id, error_code, error_message, completed_at) VALUES ($1, $2, $3, 'url', 'completed', 'api', $4, $5, $6, $7, NULL, NULL, now())",
    )
    .bind(completed_ingest_id)
    .bind("legacy-completed")
    .bind(vec![5_u8; 32])
    .bind(json!({"source": "https://example.test/completed.mp4"}))
    .bind("https://example.test/completed.mp4")
    .bind(ready_media_id)
    .bind(Uuid::new_v4())
    .execute(&target_pool)
    .await
    .expect("completed legacy ingest should be valid at migration 0006");
    sqlx::query(
        "INSERT INTO ingests (id, input_key, request_hash, input_kind, state, submitted_via, input_json, source_url, media_id, workspace_id, error_code, error_message, completed_at) VALUES ($1, $2, $3, 'url', 'failed_terminal', 'api', $4, $5, $6, $7, 'legacy_failure', 'preserved failure', now())",
    )
    .bind(failed_ingest_id)
    .bind("legacy-failed")
    .bind(vec![6_u8; 32])
    .bind(json!({"source": "https://example.test/failed.mp4"}))
    .bind("https://example.test/failed.mp4")
    .bind(unknown_media_id)
    .bind(Uuid::new_v4())
    .execute(&target_pool)
    .await
    .expect("failed legacy ingest should be valid at migration 0006");

    sqlx::query(
        "INSERT INTO channels (id, telegram_chat_id, name, time_zone, window_start, window_end, interval_minutes) VALUES ($1, -1001234567891, 'legacy target', 'UTC', '08:00', '22:00', 30)",
    )
    .bind(channel_id)
    .execute(&target_pool)
    .await
    .expect("channel should be valid at migration 0006");
    for (id, request_key, media_id, state, caption, telegram_message_id) in [
        (
            queued_post_id,
            "legacy-queued-post",
            ready_media_id,
            "queued",
            Some("queued legacy caption"),
            None,
        ),
        (
            published_post_id,
            "legacy-published-post",
            ready_media_id,
            "published",
            Some("published legacy caption"),
            Some(202_i64),
        ),
    ] {
        sqlx::query(
            "INSERT INTO posts (id, request_key, request_hash, media_id, channel_id, state, caption, scheduled_at, cadence_slot_at, telegram_message_id, published_at) VALUES ($1, $2, $3, $4, $5, $6, $7, now(), CASE WHEN $6 = 'queued' THEN now() ELSE NULL END, $8, CASE WHEN $6 = 'published' THEN now() ELSE NULL END)",
        )
        .bind(id)
        .bind(request_key)
        .bind(vec![7_u8; 32])
        .bind(media_id)
        .bind(channel_id)
        .bind(state)
        .bind(caption)
        .bind(telegram_message_id)
        .execute(&target_pool)
        .await
        .expect("post should be valid at migration 0006");
    }

    let queued_inspect_job_id: Uuid = sqlx::query_scalar(
        "INSERT INTO queue.jobs (kind, payload, state, dedupe_key) VALUES ('inspect_source', $1, 'queued', 'legacy:inspect') RETURNING id",
    )
    .bind(json!({"ingest_id": url_ingest_id}))
    .fetch_one(&target_pool)
    .await
    .expect("queued legacy job should be valid at migration 0006");
    let stale_cleanup_job_id: Uuid = sqlx::query_scalar(
        "INSERT INTO queue.jobs (kind, payload, state, attempt_count, max_attempts, lease_token, lease_owner, lease_expires_at, last_heartbeat_at, dedupe_key) VALUES ('cleanup_workspace', $1, 'running', 1, 3, $2, 'legacy-worker', now() - interval '1 minute', now() - interval '1 minute', 'legacy:cleanup') RETURNING id",
    )
    .bind(json!({"ingest_id": url_ingest_id, "workspace_id": active_workspace_id}))
    .bind(Uuid::new_v4())
    .fetch_one(&target_pool)
    .await
    .expect("running legacy job should be valid at migration 0006");
    sqlx::query(
        "INSERT INTO queue.jobs (kind, payload, state, completed_at, dedupe_key) VALUES ('normalize_asset', $1, 'succeeded', now(), 'legacy:succeeded')",
    )
    .bind(json!({"ingest_id": completed_ingest_id}))
    .execute(&target_pool)
    .await
    .expect("succeeded legacy job should be valid at migration 0006");
    sqlx::query(
        "INSERT INTO queue.jobs (kind, payload, state, error_class, error_message, completed_at, dedupe_key) VALUES ('probe_asset', $1, 'failed', 'legacy_failure', 'preserved failure', now(), 'legacy:failed')",
    )
    .bind(json!({"ingest_id": failed_ingest_id}))
    .execute(&target_pool)
    .await
    .expect("failed legacy job should be valid at migration 0006");
    sqlx::query(
        "INSERT INTO queue.jobs (kind, payload, state, completed_at, dedupe_key) VALUES ('compute_fingerprint', $1, 'cancelled', now(), 'legacy:cancelled')",
    )
    .bind(json!({"ingest_id": failed_ingest_id}))
    .execute(&target_pool)
    .await
    .expect("cancelled legacy job should be valid at migration 0006");
    let queued_publish_job_id: Uuid = sqlx::query_scalar(
        "INSERT INTO queue.jobs (kind, payload, state, dedupe_key) VALUES ('publish_post', $1, 'queued', 'post:' || $2 || ':publish:v1') RETURNING id",
    )
    .bind(json!({"post_id": queued_post_id, "expected_revision": 0}))
    .bind(queued_post_id)
    .fetch_one(&target_pool)
    .await
    .expect("queued publication job should be valid at migration 0006");

    let database = Database::connect(target_url.as_str(), 5)
        .await
        .expect("upgraded database should connect through the current repository");
    database.migrate().await.expect("the repository migrator should apply current HEAD");
    let applied_versions: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM _sqlx_migrations ORDER BY version")
            .fetch_all(database.pool())
            .await
            .expect("migration bookkeeping should load");
    let expected_versions =
        HEAD_MIGRATOR.iter().map(|migration| migration.version).collect::<Vec<_>>();
    assert_eq!(applied_versions, expected_versions);
    assert!(applied_versions.contains(&7), "publication intent migration must be exercised");
    assert!(applied_versions.contains(&8), "preview and caption migration must be exercised");

    let job_state_counts: (i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE state = 'queued'), count(*) FILTER (WHERE state = 'running'), count(*) FILTER (WHERE state = 'succeeded'), count(*) FILTER (WHERE state = 'failed'), count(*) FILTER (WHERE state = 'cancelled') FROM queue.jobs",
    )
    .fetch_one(database.pool())
    .await
    .expect("upgraded queue state inventory should load");
    assert_eq!(job_state_counts, (2, 1, 1, 1, 1));

    let remaining_status_columns: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.columns WHERE table_name = 'ingests' AND column_name IN ('telegram_status_chat_id', 'telegram_status_message_id')",
    )
    .fetch_one(database.pool())
    .await
    .expect("upgraded ingest column inventory should load");
    assert_eq!(remaining_status_columns, 0);
    let protected = database
        .jobs()
        .protected_workspace_ids()
        .await
        .expect("all upgraded ingest workspaces should be visible to reconciliation");
    assert!(protected.contains(&active_workspace_id));
    assert!(protected.contains(&before_workspace_telegram));

    let active = database
        .inbox()
        .find(url_ingest_id)
        .await
        .expect("current inbox repository should load an upgraded active ingest")
        .expect("active upgraded ingest should still exist");
    assert_eq!(active.status, sooqa_inbox::IngestStatus::Downloading);
    assert_eq!(active.supplied_description.as_deref(), Some("active legacy description"));
    assert_eq!(active.workspace_id, active_workspace_id);
    let completed = database
        .inbox()
        .find(completed_ingest_id)
        .await
        .expect("current inbox repository should load an upgraded terminal ingest")
        .expect("completed upgraded ingest should still exist");
    assert_eq!(completed.status, sooqa_inbox::IngestStatus::Completed);
    assert_eq!(completed.media_id, Some(ready_media_id));

    let ready = database
        .library()
        .find_media(ready_media_id)
        .await
        .expect("current library repository should load an upgraded ready media row")
        .expect("ready media should still exist");
    assert_eq!(ready.storage_state, sooqa_library::MediaStorageState::Ready);
    assert_eq!(ready.caption_sync_generation, 0);
    assert_eq!(ready.caption_sync_state, sooqa_library::CaptionSyncState::NotRequired);
    assert!(ready.preview.is_none(), "migration 0008 must not backfill previews");
    assert_eq!(ready.description.as_deref(), Some("preserved description"));
    for (media_id, expected_state) in [
        (pending_media_id, sooqa_library::MediaStorageState::Pending),
        (unknown_media_id, sooqa_library::MediaStorageState::Unknown),
        (missing_media_id, sooqa_library::MediaStorageState::Missing),
    ] {
        let media = database
            .library()
            .find_media(media_id)
            .await
            .expect("current library repository should load every upgraded storage state")
            .expect("upgraded media state should still exist");
        assert_eq!(media.storage_state, expected_state);
    }

    let channel = database
        .publisher()
        .find_channel(channel_id)
        .await
        .expect("current publisher repository should load an upgraded channel")
        .expect("upgraded channel should still exist");
    assert_eq!(channel.name, "legacy target");
    assert_eq!(channel.interval_minutes, 30);
    let old_published = database
        .publisher()
        .find_post(published_post_id)
        .await
        .expect("current publisher repository should load an upgraded published post")
        .expect("published post should still exist");
    assert_eq!(old_published.state, PostState::Published);
    assert_eq!(old_published.requested_action, PublicationAction::Queue);
    assert_eq!(old_published.revision, 0);
    assert_eq!(old_published.telegram_message_id, Some(202));

    let recovered = database
        .jobs()
        .recover_stale_leases()
        .await
        .expect("current job repository should recover an upgraded running job");
    assert_eq!(recovered, 1);
    let cleanup_job = database
        .jobs()
        .claim_next(
            "upgrade-test-worker",
            std::time::Duration::from_secs(30),
            &[JobType::CleanupWorkspace],
        )
        .await
        .expect("current job repository should claim the recovered legacy job")
        .expect("recovered legacy job should be queued");
    assert_eq!(cleanup_job.id, stale_cleanup_job_id);
    assert!(matches!(cleanup_job.command, JobCommand::CleanupWorkspace(_)));
    let cleanup_attempt = cleanup_job.lease().expect("claimed cleanup job should have a lease");
    database
        .jobs()
        .complete_lease(&cleanup_attempt)
        .await
        .expect("current job repository should complete the recovered legacy job");

    let inspect_job = database
        .jobs()
        .claim_next(
            "upgrade-test-worker",
            std::time::Duration::from_secs(30),
            &[JobType::InspectSource],
        )
        .await
        .expect("current job repository should claim the upgraded inspect payload")
        .expect("legacy inspect job should be queued");
    assert_eq!(inspect_job.id, queued_inspect_job_id);
    assert!(matches!(
        &inspect_job.command,
        JobCommand::InspectSource(payload) if payload.ingest_id == url_ingest_id
    ));
    let inspect_attempt = inspect_job.lease().expect("claimed inspect job should have a lease");
    database
        .jobs()
        .complete_lease(&inspect_attempt)
        .await
        .expect("current job repository should complete the upgraded inspect job");

    let queued_post = database
        .publisher()
        .find_post(queued_post_id)
        .await
        .expect("current publisher repository should load an upgraded queued post")
        .expect("queued post should still exist");
    assert_eq!(queued_post.state, PostState::Queued);
    assert_eq!(queued_post.requested_action, PublicationAction::Queue);
    assert_eq!(queued_post.revision, 0);
    let publish_job = database
        .jobs()
        .claim_next(
            "upgrade-publication-worker",
            std::time::Duration::from_secs(30),
            &[JobType::PublishPost],
        )
        .await
        .expect("current job repository should claim the upgraded publication payload")
        .expect("legacy publication job should be queued");
    assert_eq!(publish_job.id, queued_publish_job_id);
    let publish_attempt = publish_job.lease().expect("claimed publication job should have a lease");
    let claim = database
        .publisher()
        .claim_publish(queued_post_id, queued_post.revision, &publish_attempt)
        .await
        .expect("current publisher repository should claim the upgraded queued post");
    let publish_lease = PublishLease {
        generation: claim.post.send_generation,
        token: claim.post.send_token.expect("publication claim should have a token"),
        attempt: publish_attempt.clone(),
    };
    let published = database
        .publisher()
        .complete_publish(queued_post_id, &publish_lease, 303)
        .await
        .expect("current publisher repository should advance the upgraded post");
    assert_eq!(published.post.state, PostState::Published);
    assert_eq!(published.post.telegram_message_id, Some(303));
    database
        .jobs()
        .complete_lease(&publish_attempt)
        .await
        .expect("current job repository should complete the upgraded publication job");

    let final_post = database
        .publisher()
        .find_post(queued_post_id)
        .await
        .expect("current publisher repository should reload the advanced post")
        .expect("advanced post should still exist");
    assert_eq!(final_post.state, PostState::Published);
    assert_eq!(final_post.telegram_message_id, Some(303));
}
