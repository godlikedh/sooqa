use std::env;

use sha2::{Digest, Sha256};
use sooqa_library::{
    AssetRole, ContentKind, MediaKind, NewContentItem, NewMediaAsset, StorageState,
};
use sooqa_persistence::Database;
use sooqa_publisher::{
    NewChannelPolicy, NewPostDraft, NewPublicationSchedule, NewTargetChannel, PostDraftStatus,
    PostDraftUpdate, publication_job_idempotency_key,
};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance"]
async fn concurrent_schedulers_enqueue_one_job_and_defer_for_cadence() {
    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must point to the integration database");
    let database =
        Database::connect(&database_url, 10).await.expect("database should be reachable");
    database.migrate().await.expect("migrations should succeed");
    clean_up_old_fixtures(database.pool()).await;

    let key_prefix = format!("i3-scheduler-{}", Uuid::new_v4());
    let target = database
        .publisher()
        .create_target_channel(
            NewTargetChannel::try_new(
                format!("{key_prefix}-channel"),
                -1_000_000_000_000_i64
                    - i64::try_from(Uuid::new_v4().as_u128() % 1_000_000)
                        .expect("bounded UUID fragment should fit in i64"),
            )
            .expect("target channel should be valid"),
        )
        .await
        .expect("target channel should be created");
    database
        .publisher()
        .upsert_channel_policy(NewChannelPolicy::default_for(target.id))
        .await
        .expect("default channel policy should be created");

    let content = database
        .library()
        .create_content_item(NewContentItem::new(ContentKind::Video))
        .await
        .expect("content item should be created");
    let asset = database
        .library()
        .create_media_asset(NewMediaAsset {
            content_item_id: content.id,
            role: AssetRole::Canonical,
            media_kind: MediaKind::Video,
            mime_type: Some("video/mp4".to_owned()),
            container: Some("mp4".to_owned()),
            video_codec: Some("h264".to_owned()),
            audio_codec: Some("aac".to_owned()),
            width: Some(320),
            height: Some(240),
            duration_ms: Some(1_000),
            bit_rate: Some(100_000),
            file_size_bytes: Some(8),
            sha256: Some(Sha256::digest(key_prefix.as_bytes()).to_vec()),
            local_work_path: None,
            storage_state: StorageState::Uploaded,
        })
        .await
        .expect("canonical asset should be created");
    sqlx::query("UPDATE content_items SET canonical_asset_id = $2 WHERE id = $1")
        .bind(content.id)
        .bind(asset.id)
        .execute(database.pool())
        .await
        .expect("canonical asset pointer should be set");

    let draft = ready_draft(&database, content.id, asset.id, target.id, "scheduled").await;
    let now = OffsetDateTime::now_utc().replace_nanosecond(0).expect("timestamp should be valid");
    let schedule = database
        .publisher()
        .create_publication_schedule(
            NewPublicationSchedule::try_new(
                draft.id,
                now - Duration::seconds(1),
                format!("{key_prefix}-schedule"),
            )
            .expect("schedule should be valid"),
        )
        .await
        .expect("schedule should be created");

    let publisher_a = database.publisher();
    let publisher_b = database.publisher();
    let (left, right) = tokio::join!(
        publisher_a.enqueue_due_publication_jobs(now, 10),
        publisher_b.enqueue_due_publication_jobs(now, 10),
    );
    let left = left.expect("first scheduler should succeed");
    let right = right.expect("second scheduler should succeed");
    assert_eq!(left.len() + right.len(), 1);
    let queued = database
        .publisher()
        .find_publication_schedule(schedule.id)
        .await
        .expect("queued schedule should load")
        .expect("queued schedule should exist");
    assert_eq!(queued.status, sooqa_publisher::PublicationScheduleStatus::Queued);
    let job_key = publication_job_idempotency_key(schedule.id);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM jobs WHERE idempotency_key = $1")
            .bind(&job_key)
            .fetch_one(database.pool())
            .await
            .expect("publish job should be queryable"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT job_type FROM jobs WHERE idempotency_key = $1")
            .bind(&job_key)
            .fetch_one(database.pool())
            .await
            .expect("publish job type should be queryable"),
        "publish_post"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT payload_json ->> 'schedule_id' FROM jobs WHERE idempotency_key = $1",
        )
        .bind(&job_key)
        .fetch_one(database.pool())
        .await
        .expect("publish job schedule ID should be queryable"),
        schedule.id.to_string()
    );

    let future_draft = ready_draft(&database, content.id, asset.id, target.id, "future").await;
    let future_schedule = database
        .publisher()
        .create_publication_schedule(
            NewPublicationSchedule::try_new(
                future_draft.id,
                now + Duration::minutes(5),
                format!("{key_prefix}-future-schedule"),
            )
            .expect("future schedule should be valid"),
        )
        .await
        .expect("future schedule should be created");
    assert!(
        database
            .publisher()
            .enqueue_due_publication_jobs(now, 10)
            .await
            .expect("future scheduler tick should succeed")
            .is_empty()
    );
    assert_eq!(
        database
            .publisher()
            .find_publication_schedule(future_schedule.id)
            .await
            .expect("future schedule should load")
            .expect("future schedule should exist")
            .status,
        sooqa_publisher::PublicationScheduleStatus::Pending
    );

    let old_draft = ready_draft(&database, content.id, asset.id, target.id, "old").await;
    let old_publish_at = now - Duration::minutes(30);
    let old_schedule = database
        .publisher()
        .create_publication_schedule(
            NewPublicationSchedule::try_new(
                old_draft.id,
                old_publish_at,
                format!("{key_prefix}-old-schedule"),
            )
            .expect("old schedule should be valid"),
        )
        .await
        .expect("old schedule should be created");
    sqlx::query("UPDATE post_drafts SET status = 'published' WHERE id = $1")
        .bind(old_draft.id)
        .execute(database.pool())
        .await
        .expect("old draft should become a fixture publication");
    sqlx::query("UPDATE publication_schedules SET status = 'published' WHERE id = $1")
        .bind(old_schedule.id)
        .execute(database.pool())
        .await
        .expect("old schedule should become a fixture publication");
    sqlx::query(
        r#"
        INSERT INTO published_posts (
            publication_schedule_id, content_item_id, asset_id, target_channel_id,
            telegram_chat_id, telegram_message_id, caption_snapshot, published_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#,
    )
    .bind(old_schedule.id)
    .bind(content.id)
    .bind(asset.id)
    .bind(target.id)
    .bind(target.telegram_chat_id)
    .bind(1_i64)
    .bind(Some("old caption"))
    .bind(old_publish_at)
    .execute(database.pool())
    .await
    .expect("old publication should be inserted");

    database
        .publisher()
        .upsert_channel_policy(NewChannelPolicy {
            minimum_post_interval_seconds: 3_600,
            ..NewChannelPolicy::default_for(target.id)
        })
        .await
        .expect("cadence policy should be updated");
    let deferred_draft = ready_draft(&database, content.id, asset.id, target.id, "deferred").await;
    let deferred_schedule = database
        .publisher()
        .create_publication_schedule(
            NewPublicationSchedule::try_new(
                deferred_draft.id,
                now - Duration::seconds(1),
                format!("{key_prefix}-deferred-schedule"),
            )
            .expect("deferred schedule should be valid"),
        )
        .await
        .expect("deferred schedule should be created");
    assert!(
        database
            .publisher()
            .enqueue_due_publication_jobs(now, 10)
            .await
            .expect("cadence scheduler should succeed")
            .is_empty()
    );
    let deferred = database
        .publisher()
        .find_publication_schedule(deferred_schedule.id)
        .await
        .expect("deferred schedule should load")
        .expect("deferred schedule should exist");
    assert_eq!(deferred.status, sooqa_publisher::PublicationScheduleStatus::Pending);
    assert_eq!(deferred.publish_at, old_publish_at + Duration::hours(1));

    database
        .publisher()
        .upsert_channel_policy(NewChannelPolicy {
            max_posts_per_day: Some(1),
            ..NewChannelPolicy::default_for(target.id)
        })
        .await
        .expect("daily policy should be updated");
    let daily_draft = ready_draft(&database, content.id, asset.id, target.id, "daily").await;
    let daily_schedule = database
        .publisher()
        .create_publication_schedule(
            NewPublicationSchedule::try_new(
                daily_draft.id,
                now - Duration::seconds(1),
                format!("{key_prefix}-daily-schedule"),
            )
            .expect("daily schedule should be valid"),
        )
        .await
        .expect("daily schedule should be created");
    assert!(
        database
            .publisher()
            .enqueue_due_publication_jobs(now, 10)
            .await
            .expect("daily scheduler should succeed")
            .is_empty()
    );
    let daily = database
        .publisher()
        .find_publication_schedule(daily_schedule.id)
        .await
        .expect("daily schedule should load")
        .expect("daily schedule should exist");
    assert_eq!(daily.status, sooqa_publisher::PublicationScheduleStatus::Pending);
    assert_eq!(daily.publish_at, now.replace_time(time::Time::MIDNIGHT) + Duration::days(1));

    sqlx::query("DELETE FROM jobs WHERE idempotency_key = $1")
        .bind(&job_key)
        .execute(database.pool())
        .await
        .expect("publish job should clean up");
    sqlx::query("DELETE FROM published_posts WHERE target_channel_id = $1")
        .bind(target.id)
        .execute(database.pool())
        .await
        .expect("published fixture should clean up");
    sqlx::query(
        "DELETE FROM publication_schedules WHERE post_draft_id IN (SELECT id FROM post_drafts WHERE content_item_id = $1)",
    )
    .bind(content.id)
    .execute(database.pool())
    .await
    .expect("publication schedules should clean up");
    sqlx::query("DELETE FROM post_drafts WHERE content_item_id = $1")
        .bind(content.id)
        .execute(database.pool())
        .await
        .expect("post drafts should clean up");
    sqlx::query("DELETE FROM channel_policies WHERE target_channel_id = $1")
        .bind(target.id)
        .execute(database.pool())
        .await
        .expect("channel policy should clean up");
    sqlx::query("DELETE FROM target_channels WHERE id = $1")
        .bind(target.id)
        .execute(database.pool())
        .await
        .expect("target channel should clean up");
    sqlx::query("UPDATE content_items SET canonical_asset_id = NULL WHERE id = $1")
        .bind(content.id)
        .execute(database.pool())
        .await
        .expect("canonical pointer should clean up");
    sqlx::query("DELETE FROM media_assets WHERE id = $1")
        .bind(asset.id)
        .execute(database.pool())
        .await
        .expect("asset should clean up");
    sqlx::query("DELETE FROM content_items WHERE id = $1")
        .bind(content.id)
        .execute(database.pool())
        .await
        .expect("content item should clean up");
}

async fn clean_up_old_fixtures(pool: &sqlx::PgPool) {
    let pattern = "i3-scheduler-%";
    sqlx::query(
        "DELETE FROM jobs WHERE idempotency_key IN (SELECT 'publisher:publish:' || id::text FROM publication_schedules WHERE idempotency_key LIKE $1)",
    )
    .bind(pattern)
    .execute(pool)
    .await
    .expect("old scheduler jobs should clean up");
    sqlx::query(
        "DELETE FROM published_posts WHERE publication_schedule_id IN (SELECT id FROM publication_schedules WHERE idempotency_key LIKE $1)",
    )
    .bind(pattern)
    .execute(pool)
    .await
    .expect("old published fixtures should clean up");
    sqlx::query("DELETE FROM publication_schedules WHERE idempotency_key LIKE $1")
        .bind(pattern)
        .execute(pool)
        .await
        .expect("old scheduler schedules should clean up");
    sqlx::query(
        "DELETE FROM post_drafts WHERE target_channel_id IN (SELECT id FROM target_channels WHERE name LIKE $1)",
    )
    .bind(pattern)
    .execute(pool)
    .await
    .expect("old scheduler drafts should clean up");
    sqlx::query(
        "DELETE FROM channel_policies WHERE target_channel_id IN (SELECT id FROM target_channels WHERE name LIKE $1)",
    )
    .bind(pattern)
    .execute(pool)
    .await
    .expect("old scheduler policies should clean up");
    sqlx::query("DELETE FROM target_channels WHERE name LIKE $1")
        .bind(pattern)
        .execute(pool)
        .await
        .expect("old scheduler targets should clean up");
}

async fn ready_draft(
    database: &Database,
    content_item_id: Uuid,
    asset_id: Uuid,
    target_channel_id: Uuid,
    _label: &str,
) -> sooqa_publisher::PostDraft {
    let draft = database
        .publisher()
        .create_post_draft(NewPostDraft {
            content_item_id,
            asset_id,
            target_channel_id,
            caption: None,
            parse_mode: None,
        })
        .await
        .expect("post draft should be created");
    database
        .publisher()
        .update_post_draft(
            draft.id,
            PostDraftUpdate {
                caption: None,
                parse_mode: None,
                status: Some(PostDraftStatus::Ready),
                expected_updated_at: Some(draft.updated_at),
            },
        )
        .await
        .expect("post draft should become ready")
}
