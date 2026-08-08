use std::env;

use sooqa_library::{
    AssetRole, ContentKind, MediaKind, NewContentItem, NewMediaAsset, StorageState,
};
use sooqa_persistence::Database;
use sooqa_publisher::{
    CooldownViolation, NewChannelPolicy, NewPostDraft, NewPublicationSchedule, NewTargetChannel,
    PostDraftStatus, PostDraftUpdate, PublicationAttemptStatus,
};
use time::Duration;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance"]
async fn publisher_repositories_round_trip_schedule_attempt_and_history() {
    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must point to the integration database");
    let database =
        Database::connect(&database_url, 10).await.expect("database should be reachable");
    database.migrate().await.expect("migrations should succeed");

    let key_prefix = format!("i1-publisher-{}", Uuid::new_v4());
    let telegram_chat_id = -1_000_000_000_000_i64
        - i64::try_from(Uuid::new_v4().as_u128() % 1_000_000)
            .expect("bounded UUID fragment should fit in i64");
    let target = database
        .publisher()
        .create_target_channel(
            NewTargetChannel::try_new(format!("{key_prefix}-channel"), telegram_chat_id)
                .expect("target channel should be valid"),
        )
        .await
        .expect("target channel should be created");
    assert!(target.is_enabled);
    let policy = database
        .publisher()
        .upsert_channel_policy(NewChannelPolicy {
            on_cooldown_violation: CooldownViolation::Block,
            ..NewChannelPolicy::default_for(target.id)
        })
        .await
        .expect("channel policy should be created");
    assert_eq!(policy.on_cooldown_violation, CooldownViolation::Block);
    assert_eq!(
        database
            .publisher()
            .find_channel_policy(target.id)
            .await
            .expect("channel policy should load")
            .expect("channel policy should exist")
            .target_channel_id,
        target.id
    );

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
            sha256: Some(vec![41; 32]),
            local_work_path: Some(format!("/tmp/{key_prefix}.mp4")),
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

    let mismatched_content = database
        .library()
        .create_content_item(NewContentItem::new(ContentKind::Video))
        .await
        .expect("second content fixture should be created");
    let mismatched_draft = sqlx::query(
        "INSERT INTO post_drafts (content_item_id, asset_id, target_channel_id) VALUES ($1, $2, $3)",
    )
    .bind(mismatched_content.id)
    .bind(asset.id)
    .bind(target.id)
    .execute(database.pool())
    .await;
    assert!(
        mismatched_draft.is_err(),
        "database should reject a draft asset owned by another content item"
    );
    sqlx::query("DELETE FROM content_items WHERE id = $1")
        .bind(mismatched_content.id)
        .execute(database.pool())
        .await
        .expect("mismatched content fixture should clean up");

    let draft = database
        .publisher()
        .create_post_draft(NewPostDraft {
            content_item_id: content.id,
            asset_id: asset.id,
            target_channel_id: target.id,
            caption: Some("caption".to_owned()),
            parse_mode: Some("HTML".to_owned()),
        })
        .await
        .expect("post draft should be created");
    let draft = database
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
        .expect("draft should become ready");

    let publish_at = (time::OffsetDateTime::now_utc() - Duration::minutes(1))
        .replace_nanosecond(123_456_789)
        .expect("test timestamp should be valid");
    let schedule =
        NewPublicationSchedule::try_new(draft.id, publish_at, format!("{key_prefix}-schedule"))
            .expect("schedule should be valid");
    let publisher_a = database.publisher();
    let publisher_b = database.publisher();
    let (first, second) = tokio::join!(
        publisher_a.create_publication_schedule(schedule.clone()),
        publisher_b.create_publication_schedule(schedule.clone()),
    );
    let schedule = first.expect("publication schedule should be created");
    let replay = second.expect("concurrent schedule request should replay");
    assert_eq!(replay.id, schedule.id);
    assert_eq!(schedule.publish_at.nanosecond() % 1_000, 0);
    let replay = database
        .publisher()
        .create_publication_schedule(schedule_request(&schedule, &key_prefix))
        .await
        .expect("same schedule request should replay after commit");
    assert_eq!(replay.id, schedule.id);
    let due = database
        .publisher()
        .list_due_publication_schedules(time::OffsetDateTime::now_utc(), 10)
        .await
        .expect("due schedules should load");
    assert_eq!(due.iter().filter(|item| item.id == schedule.id).count(), 1);
    assert!(matches!(
        database
            .publisher()
            .transition_publication_schedule(
                schedule.id,
                sooqa_publisher::PublicationScheduleStatus::Publishing,
            )
            .await,
        Err(sooqa_persistence::PublisherRepositoryError::ManagedScheduleTransitionRequired {
            id,
            target: sooqa_publisher::PublicationScheduleStatus::Publishing,
        }) if id == schedule.id
    ));

    let cancelled_draft = database
        .publisher()
        .create_post_draft(NewPostDraft {
            content_item_id: content.id,
            asset_id: asset.id,
            target_channel_id: target.id,
            caption: None,
            parse_mode: None,
        })
        .await
        .expect("cancelled-draft fixture should be created");
    let cancelled_draft = database
        .publisher()
        .update_post_draft(
            cancelled_draft.id,
            PostDraftUpdate {
                caption: None,
                parse_mode: None,
                status: Some(PostDraftStatus::Ready),
                expected_updated_at: Some(cancelled_draft.updated_at),
            },
        )
        .await
        .expect("cancelled-draft fixture should become ready");
    let cancelled_schedule = database
        .publisher()
        .create_publication_schedule(
            NewPublicationSchedule::try_new(
                cancelled_draft.id,
                publish_at,
                format!("{key_prefix}-cancelled"),
            )
            .expect("cancelled schedule should be valid"),
        )
        .await
        .expect("cancelled schedule should be created");
    let cancelled_draft = database
        .publisher()
        .find_post_draft(cancelled_draft.id)
        .await
        .expect("cancelled draft should reload")
        .expect("cancelled draft should exist");
    let cancelled_draft = database
        .publisher()
        .update_post_draft(
            cancelled_draft.id,
            PostDraftUpdate {
                caption: None,
                parse_mode: None,
                status: Some(PostDraftStatus::Cancelled),
                expected_updated_at: Some(cancelled_draft.updated_at),
            },
        )
        .await
        .expect("cancelled-draft fixture should be cancelled");
    assert!(matches!(
        database
            .publisher()
            .start_publication_attempt(cancelled_schedule.id, None)
            .await,
        Err(sooqa_persistence::PublisherRepositoryError::DraftNotReady {
            id,
            status: PostDraftStatus::Cancelled,
        }) if id == cancelled_draft.id
    ));
    sqlx::query("DELETE FROM publication_schedules WHERE id = $1")
        .bind(cancelled_schedule.id)
        .execute(database.pool())
        .await
        .expect("cancelled schedule should clean up");
    sqlx::query("DELETE FROM post_drafts WHERE id = $1")
        .bind(cancelled_draft.id)
        .execute(database.pool())
        .await
        .expect("cancelled draft should clean up");

    let attempt = database
        .publisher()
        .start_publication_attempt(schedule.id, Some(format!("{key_prefix}-telegram")))
        .await
        .expect("publication attempt should start");
    assert_eq!(attempt.attempt_number, 1);
    assert!(matches!(
        database
            .publisher()
            .start_publication_attempt(schedule.id, Some(format!("{key_prefix}-duplicate")))
            .await,
        Err(sooqa_persistence::PublisherRepositoryError::AttemptAlreadyRunning {
            schedule_id,
            attempt_number: 1,
        }) if schedule_id == schedule.id
    ));
    assert!(matches!(
        database
            .publisher()
            .transition_publication_schedule(
                schedule.id,
                sooqa_publisher::PublicationScheduleStatus::Failed,
            )
            .await,
        Err(sooqa_persistence::PublisherRepositoryError::ManagedScheduleTransitionRequired {
            id,
            target: sooqa_publisher::PublicationScheduleStatus::Failed,
        }) if id == schedule.id
    ));
    let ambiguous = database
        .publisher()
        .finish_publication_attempt(
            schedule.id,
            attempt.attempt_number,
            PublicationAttemptStatus::Unknown,
            Some("telegram_timeout"),
            Some("request outcome is ambiguous"),
            None,
        )
        .await
        .expect("ambiguous publication should be preserved");
    assert_eq!(ambiguous.status, PublicationAttemptStatus::Unknown);
    assert_eq!(
        database
            .publisher()
            .list_due_publication_schedules(time::OffsetDateTime::now_utc(), 10)
            .await
            .expect("due schedules should load")
            .len(),
        0
    );
    database
        .publisher()
        .transition_publication_schedule(
            schedule.id,
            sooqa_publisher::PublicationScheduleStatus::Queued,
        )
        .await
        .expect("explicit reconciliation should requeue the schedule");
    assert!(matches!(
        database
            .publisher()
            .transition_publication_schedule(
                schedule.id,
                sooqa_publisher::PublicationScheduleStatus::Publishing,
            )
            .await,
        Err(sooqa_persistence::PublisherRepositoryError::ManagedScheduleTransitionRequired {
            id,
            target: sooqa_publisher::PublicationScheduleStatus::Publishing,
        }) if id == schedule.id
    ));
    let attempt = database
        .publisher()
        .start_publication_attempt(schedule.id, Some(format!("{key_prefix}-telegram-retry")))
        .await
        .expect("reconciled publication should start a new attempt");
    assert_eq!(attempt.attempt_number, 2);
    let completion = database
        .publisher()
        .complete_publication_attempt(
            schedule.id,
            attempt.attempt_number,
            77,
            Some("caption".to_owned()),
            Some(serde_json::json!({"message_id": 77})),
        )
        .await
        .expect("publication attempt should finish atomically");
    assert_eq!(completion.attempt.status, PublicationAttemptStatus::Succeeded);
    assert_eq!(completion.published_post.telegram_chat_id, target.telegram_chat_id);
    assert_eq!(completion.published_post.telegram_message_id, 77);
    assert_eq!(
        database
            .publisher()
            .complete_publication_attempt(
                schedule.id,
                attempt.attempt_number,
                77,
                Some("caption".to_owned()),
                Some(serde_json::json!({"message_id": 77})),
            )
            .await
            .expect("same publication completion should replay")
            .published_post
            .id,
        completion.published_post.id
    );
    assert!(matches!(
        database
            .publisher()
            .complete_publication_attempt(
                schedule.id,
                attempt.attempt_number,
                78,
                Some("caption".to_owned()),
                None,
            )
            .await,
        Err(sooqa_persistence::PublisherRepositoryError::PublishedPostConflict(id))
            if id == schedule.id
    ));
    assert_eq!(
        database
            .publisher()
            .find_publication_schedule(schedule.id)
            .await
            .expect("schedule should load")
            .expect("schedule should exist")
            .status
            .as_str(),
        "published"
    );
    assert_eq!(
        database
            .publisher()
            .find_post_draft(draft.id)
            .await
            .expect("draft should load")
            .expect("draft should exist")
            .status,
        PostDraftStatus::Published
    );

    sqlx::query("DELETE FROM published_posts WHERE publication_schedule_id = $1")
        .bind(schedule.id)
        .execute(database.pool())
        .await
        .expect("published post should clean up");
    sqlx::query("DELETE FROM publication_schedules WHERE id = $1")
        .bind(schedule.id)
        .execute(database.pool())
        .await
        .expect("schedule should clean up");
    sqlx::query("DELETE FROM post_drafts WHERE id = $1")
        .bind(draft.id)
        .execute(database.pool())
        .await
        .expect("draft should clean up");
    sqlx::query("UPDATE content_items SET canonical_asset_id = NULL WHERE id = $1")
        .bind(content.id)
        .execute(database.pool())
        .await
        .expect("canonical pointer should clear");
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
}

fn schedule_request(
    schedule: &sooqa_publisher::PublicationSchedule,
    key_prefix: &str,
) -> NewPublicationSchedule {
    NewPublicationSchedule {
        post_draft_id: schedule.post_draft_id,
        publish_at: schedule.publish_at,
        not_before: schedule.not_before,
        not_after: schedule.not_after,
        priority: schedule.priority,
        cooldown_override: schedule.cooldown_override,
        idempotency_key: format!("{key_prefix}-schedule"),
    }
}
