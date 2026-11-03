use sooqa_inbox::{IngestSubmission, IngestSubmissionInput, SubmittedVia};
use sooqa_library::{
    MediaIngest, MediaKind, MediaLookup, MediaMetadata, MediaSourceInput, NewMedia, SourceKind,
};
use sooqa_persistence::{Database, PublisherRepositoryError};
use sooqa_publisher::{ChannelUpdate, NewChannel, NewPost, PostExactSchedule};
use time::OffsetDateTime;
use uuid::Uuid;

async fn set_terminal_failure(database: &Database, ingest_id: Uuid, epoch_seconds: i64) {
    sqlx::query(
        "UPDATE ingests SET state = 'failed_terminal', error_code = 'test_failure', error_message = 'failed for test', updated_at = to_timestamp($2), completed_at = to_timestamp($2) WHERE id = $1",
    )
    .bind(ingest_id)
    .bind(epoch_seconds as f64)
    .execute(database.pool())
    .await
    .unwrap();
}

async fn wait_for_failure_reset_block(pool: &sqlx::PgPool, blocker_pid: i32) -> bool {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let blocked: bool = sqlx::query_scalar(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_stat_activity AS activity
                    WHERE activity.datname = current_database()
                      AND activity.wait_event_type = 'Lock'
                      AND activity.query LIKE '%WITH failures AS MATERIALIZED%'
                      AND $1::integer = ANY(pg_blocking_pids(activity.pid))
                )
                "#,
            )
            .bind(blocker_pid)
            .fetch_one(pool)
            .await
            .unwrap();
            if blocked {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok()
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn ingest_failure_summary_acknowledges_only_the_current_terminal_failure_identity(
    pool: sqlx::PgPool,
) {
    let database = Database::from_pool(pool);
    let terminal_id = ingest(&database, "failure-terminal").await;
    let retryable_id = ingest(&database, "failure-retryable").await;

    let empty = database.inbox().ingest_failure_summary().await.unwrap();
    assert_eq!(empty.count, 0);
    assert_eq!(empty.revision.len(), 32);
    assert!(empty.revision.bytes().all(|byte| byte.is_ascii_hexdigit()));

    set_terminal_failure(&database, terminal_id, 1_000).await;
    sqlx::query(
        "UPDATE ingests SET state = 'failed_retryable', error_code = 'retryable', updated_at = to_timestamp(1000), completed_at = to_timestamp(1000) WHERE id = $1",
    )
    .bind(retryable_id)
    .execute(database.pool())
    .await
    .unwrap();
    let summary = database.inbox().ingest_failure_summary().await.unwrap();
    assert_eq!(summary.count, 1, "retryable failures are not included");

    let row_before: serde_json::Value = sqlx::query_scalar(
        "SELECT to_jsonb(i) - 'acknowledged_failure_at' FROM ingests AS i WHERE id = $1",
    )
    .bind(terminal_id)
    .fetch_one(database.pool())
    .await
    .unwrap();
    let jobs_before: serde_json::Value = sqlx::query_scalar(
        "SELECT COALESCE(jsonb_agg(to_jsonb(j) ORDER BY j.id), '[]'::jsonb) FROM queue.jobs AS j",
    )
    .fetch_one(database.pool())
    .await
    .unwrap();

    assert!(database.inbox().acknowledge_ingest_failures(&summary.revision).await.unwrap());
    let row_after: serde_json::Value = sqlx::query_scalar(
        "SELECT to_jsonb(i) - 'acknowledged_failure_at' FROM ingests AS i WHERE id = $1",
    )
    .bind(terminal_id)
    .fetch_one(database.pool())
    .await
    .unwrap();
    let jobs_after: serde_json::Value = sqlx::query_scalar(
        "SELECT COALESCE(jsonb_agg(to_jsonb(j) ORDER BY j.id), '[]'::jsonb) FROM queue.jobs AS j",
    )
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(row_after, row_before, "acknowledgement must not edit ingest data");
    assert_eq!(jobs_after, jobs_before, "acknowledgement must not edit jobs");
    assert_eq!(database.inbox().ingest_failure_summary().await.unwrap().count, 0);
    assert!(
        database.inbox().acknowledge_ingest_failures(&summary.revision).await.unwrap(),
        "replaying an acknowledgement with no pending failures succeeds"
    );

    sqlx::query("UPDATE ingests SET updated_at = to_timestamp(1500) WHERE id = $1")
        .bind(terminal_id)
        .execute(database.pool())
        .await
        .unwrap();
    assert_eq!(
        database.inbox().ingest_failure_summary().await.unwrap().count,
        0,
        "an unrelated updated_at edit must not make an acknowledged failure new"
    );

    sqlx::query("UPDATE ingests SET state = 'failed_retryable' WHERE id = $1")
        .bind(terminal_id)
        .execute(database.pool())
        .await
        .unwrap();
    set_terminal_failure(&database, terminal_id, 2_000).await;
    assert_eq!(
        database.inbox().ingest_failure_summary().await.unwrap().count,
        1,
        "a later terminal failure on the same ingest has a new identity"
    );
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn stale_ingest_failure_acknowledgement_changes_no_rows(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let first_id = ingest(&database, "failure-stale-first").await;
    let second_id = ingest(&database, "failure-stale-second").await;
    set_terminal_failure(&database, first_id, 1_000).await;
    let stale_revision = database.inbox().ingest_failure_summary().await.unwrap().revision;
    set_terminal_failure(&database, second_id, 2_000).await;

    assert!(!database.inbox().acknowledge_ingest_failures(&stale_revision).await.unwrap());
    let acknowledged_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ingests WHERE id = ANY($1) AND acknowledged_failure_at IS NOT NULL",
    )
    .bind(vec![first_id, second_id])
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(acknowledged_count, 0, "a stale reset must not acknowledge any row");
    assert_eq!(database.inbox().ingest_failure_summary().await.unwrap().count, 2);
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn concurrent_ingest_failure_reset_rechecks_locked_failure_and_does_not_sweep_new_failures(
    pool: sqlx::PgPool,
) {
    let database = Database::from_pool(pool);
    let first_id = ingest(&database, "failure-concurrent-first").await;
    let second_id = ingest(&database, "failure-concurrent-second").await;
    set_terminal_failure(&database, first_id, 1_000).await;

    let first_revision = database.inbox().ingest_failure_summary().await.unwrap().revision;
    let mut blocker = database.pool().begin().await.unwrap();
    let _: Uuid = sqlx::query_scalar("SELECT id FROM ingests WHERE id = $1 FOR UPDATE")
        .bind(first_id)
        .fetch_one(&mut *blocker)
        .await
        .unwrap();
    let blocker_pid: i32 =
        sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut *blocker).await.unwrap();
    let inbox = Database::from_pool(database.pool().clone()).inbox();
    let reset =
        tokio::spawn(async move { inbox.acknowledge_ingest_failures(&first_revision).await });

    let reset_blocked = wait_for_failure_reset_block(database.pool(), blocker_pid).await;
    if reset_blocked {
        sqlx::query(
            "UPDATE ingests SET completed_at = to_timestamp(2000), updated_at = to_timestamp(2000) WHERE id = $1",
        )
        .bind(first_id)
        .execute(&mut *blocker)
        .await
        .unwrap();
    }
    blocker.commit().await.unwrap();
    let reset_result = tokio::time::timeout(std::time::Duration::from_secs(5), reset)
        .await
        .expect("the reset should finish after the blocking transaction commits")
        .unwrap()
        .unwrap();
    assert!(reset_blocked, "PostgreSQL should report the reset waiting on the row lock");
    assert!(!reset_result, "the failure identity changed while the reset waited");
    assert_eq!(database.inbox().ingest_failure_summary().await.unwrap().count, 1);
    let first_acknowledged: bool = sqlx::query_scalar(
        "SELECT acknowledged_failure_at IS NOT DISTINCT FROM COALESCE(completed_at, updated_at) FROM ingests WHERE id = $1",
    )
    .bind(first_id)
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert!(!first_acknowledged, "the new failure identity must remain unacknowledged");

    let current_revision = database.inbox().ingest_failure_summary().await.unwrap().revision;
    let mut blocker = database.pool().begin().await.unwrap();
    let _: Uuid = sqlx::query_scalar("SELECT id FROM ingests WHERE id = $1 FOR UPDATE")
        .bind(first_id)
        .fetch_one(&mut *blocker)
        .await
        .unwrap();
    let blocker_pid: i32 =
        sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut *blocker).await.unwrap();
    let inbox = Database::from_pool(database.pool().clone()).inbox();
    let reset =
        tokio::spawn(async move { inbox.acknowledge_ingest_failures(&current_revision).await });

    let reset_blocked = wait_for_failure_reset_block(database.pool(), blocker_pid).await;
    if reset_blocked {
        sqlx::query(
            "UPDATE ingests SET state = 'failed_terminal', error_code = 'test_failure', error_message = 'failed for test', completed_at = to_timestamp(3000), updated_at = to_timestamp(3000) WHERE id = $1",
        )
        .bind(second_id)
        .execute(&mut *blocker)
        .await
        .unwrap();
    }
    blocker.commit().await.unwrap();
    let reset_result = tokio::time::timeout(std::time::Duration::from_secs(5), reset)
        .await
        .expect("the reset should finish after the blocking transaction commits")
        .unwrap()
        .unwrap();
    assert!(reset_blocked, "PostgreSQL should report the reset waiting on the row lock");
    assert!(reset_result, "the snapshot still matches when only a later failure is added");

    let first_acknowledged: bool = sqlx::query_scalar(
        "SELECT acknowledged_failure_at IS NOT DISTINCT FROM COALESCE(completed_at, updated_at) FROM ingests WHERE id = $1",
    )
    .bind(first_id)
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert!(first_acknowledged, "the failure present in the snapshot should be acknowledged");
    let pending_ids: Vec<Uuid> = sqlx::query_scalar(
        "SELECT id FROM ingests WHERE state = 'failed_terminal' AND acknowledged_failure_at IS DISTINCT FROM COALESCE(completed_at, updated_at)",
    )
    .fetch_all(database.pool())
    .await
    .unwrap();
    assert_eq!(
        pending_ids,
        vec![second_id],
        "a failure committed after the snapshot stays pending"
    );
}

async fn ingest(database: &Database, suffix: &str) -> Uuid {
    database
        .inbox()
        .create_ingest(
            IngestSubmission::try_new(IngestSubmissionInput::new(
                format!("https://example.test/admin-{suffix}"),
                SubmittedVia::Api,
            ))
            .unwrap(),
        )
        .await
        .unwrap()
        .ingest
        .id
}

async fn stored_media(database: &Database, ingest_id: Option<Uuid>, source_url: &str) -> Uuid {
    let result = database
        .library()
        .resolve_media(MediaIngest {
            media: NewMedia::new(MediaKind::Video),
            metadata: MediaMetadata {
                kind: MediaKind::Video,
                mime_type: Some("video/webm".to_owned()),
                container: Some("webm".to_owned()),
                video_codec: None,
                audio_codec: None,
                width: Some(640),
                height: Some(360),
                duration_ms: Some(1_000),
                bit_rate: None,
                file_size_bytes: Some(100),
                sha256: Some(Uuid::new_v4().as_bytes().repeat(2)),
                local_work_path: None,
                preview: None,
            },
            source: MediaSourceInput {
                ingest_id,
                kind: SourceKind::DirectUrl,
                original_url: Some(source_url.to_owned()),
                normalized_url: Some(source_url.to_owned()),
                platform: None,
                platform_content_id: None,
                author_name: None,
                title: None,
                description: None,
                published_at: None,
                metadata: serde_json::json!({"provenance": "kept"}),
            },
            tags: vec!["admin".to_owned()],
        })
        .await
        .unwrap();
    sqlx::query(
        "UPDATE media SET storage_state = 'ready', telegram_storage_chat_id = -1003971341583, telegram_storage_message_id = 57, telegram_file_id = 'admin-file' WHERE id = $1",
    )
    .bind(result.media.id)
    .execute(database.pool())
    .await
    .unwrap();
    if let Some(ingest_id) = ingest_id {
        sqlx::query("UPDATE ingests SET media_id = $2, state = 'completed', completed_at = now() WHERE id = $1")
            .bind(ingest_id)
            .bind(result.media.id)
            .execute(database.pool())
            .await
            .unwrap();
    }
    result.media.id
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn ingest_admin_cursor_is_bounded_and_stable_under_new_inserts(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    for suffix in ["one", "two", "three"] {
        ingest(&database, suffix).await;
    }
    let first = database.inbox().list_admin(2, None).await.unwrap();
    assert_eq!(first.items.len(), 2);
    let cursor = first.next_cursor.expect("first page should have a cursor");
    let newest = ingest(&database, "inserted-after-page").await;
    let second = database.inbox().list_admin(2, Some(cursor)).await.unwrap();
    assert_eq!(second.items.len(), 1);
    assert!(!second.items.iter().any(|item| item.id == newest));
    assert!(second.items.iter().all(|item| item.error_message.is_none()));
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn media_lookup_resolves_ids_mirrors_and_private_storage_links(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let ingest_id = ingest(&database, "lookup").await;
    let media_id =
        stored_media(&database, Some(ingest_id), "https://2ch.su/b/src/admin.webm").await;

    let mirror_page = database
        .library()
        .lookup_media(
            MediaLookup::SourceUrls(vec![
                "https://2ch.org/b/src/admin.webm".to_owned(),
                "https://2ch.su/b/src/admin.webm".to_owned(),
                "https://2ch.life/b/src/admin.webm".to_owned(),
            ]),
            50,
            None,
        )
        .await
        .unwrap();
    assert_eq!(mirror_page.items.len(), 1);
    assert_eq!(mirror_page.items[0].media.id, media_id);
    assert_eq!(mirror_page.items[0].storage_url.as_deref(), Some("https://t.me/c/3971341583/57"));

    let ingest_page = database
        .library()
        .lookup_media(MediaLookup::Identifier(ingest_id), 50, None)
        .await
        .unwrap();
    assert_eq!(ingest_page.items[0].media.id, media_id);
    let storage_page = database
        .library()
        .lookup_media(
            MediaLookup::StorageMessage { chat_id: -1003971341583, message_id: 57 },
            50,
            None,
        )
        .await
        .unwrap();
    assert_eq!(storage_page.items[0].media.id, media_id);
    assert_eq!(
        database
            .library()
            .find_media_details(media_id)
            .await
            .unwrap()
            .unwrap()
            .source
            .unwrap()
            .original_url,
        Some("https://2ch.su/b/src/admin.webm".to_owned())
    );
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn schedule_page_is_bounded_and_channel_updates_are_revision_fenced(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    let mut channel = NewChannel::try_new("admin-channel", -1003971341590).unwrap();
    channel.window_start = time::Time::MIDNIGHT;
    channel.window_end = time::Time::from_hms(23, 59, 0).unwrap();
    let channel = database.publisher().create_channel(channel).await.unwrap();
    let media_id = stored_media(&database, None, "https://example.test/schedule.webm").await;
    sqlx::query("UPDATE media SET caption_sync_state = 'failed', caption_sync_error = 'caption sync failed' WHERE id = $1")
        .bind(media_id)
        .execute(database.pool())
        .await
        .unwrap();
    let caption_failures = database.library().list_caption_sync_failures(50).await.unwrap();
    assert_eq!(caption_failures.len(), 1);
    assert_eq!(caption_failures[0].media_id, media_id);
    assert_eq!(caption_failures[0].error_message.as_deref(), Some("caption sync failed"));
    let post = database
        .publisher()
        .create_post_idempotent(
            NewPost {
                media_id,
                channel_id: channel.id,
                caption: Some("schedule card".to_owned()),
                parse_mode: None,
                disable_notification: false,
            },
            "admin-schedule-post".to_owned(),
            b"admin-schedule-post",
        )
        .await
        .unwrap()
        .post;
    let exact =
        (OffsetDateTime::now_utc() + time::Duration::hours(2)).replace_nanosecond(0).unwrap();
    database
        .publisher()
        .schedule_post_exact(PostExactSchedule::try_new(post.id, exact, "admin-exact", 0).unwrap())
        .await
        .unwrap();
    let page = database.publisher().list_posts(50, None, false).await.unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].post.requested_publish_at, Some(exact));
    assert_eq!(page.items[0].storage_url.as_deref(), Some("https://t.me/c/3971341583/57"));
    assert_eq!(page.items[0].preview, None);

    let updated = database
        .publisher()
        .update_channel(
            channel.id,
            ChannelUpdate {
                name: Some("renamed".to_owned()),
                expected_updated_at: Some(channel.updated_at),
                ..ChannelUpdate::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.name, "renamed");
    let stale = database
        .publisher()
        .update_channel(
            channel.id,
            ChannelUpdate {
                name: Some("stale".to_owned()),
                expected_updated_at: Some(channel.updated_at),
                ..ChannelUpdate::default()
            },
        )
        .await;
    assert!(matches!(stale, Err(PublisherRepositoryError::ChannelOptimisticConflict(_))));
    let invalid = database
        .publisher()
        .update_channel(
            channel.id,
            ChannelUpdate {
                time_zone: Some("Not/AZone".to_owned()),
                expected_updated_at: Some(updated.updated_at),
                ..ChannelUpdate::default()
            },
        )
        .await;
    assert!(matches!(invalid, Err(PublisherRepositoryError::ChannelValidation(_))));
}
