use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode},
};
use serde_json::{Value, json};
use sooqa_api::{ApiSettings, ApiState, router};
use sooqa_inbox::{IngestSubmission, IngestSubmissionInput, SubmittedVia};
use sooqa_library::{
    MediaIngest, MediaKind, MediaMetadata, MediaSourceInput, NewMedia, SourceKind,
};
use sooqa_persistence::Database;
use sooqa_publisher::{NewChannel, NewPost, PostExactSchedule};
use time::OffsetDateTime;
use tower::ServiceExt;
use uuid::Uuid;

fn app(pool: sqlx::PgPool) -> (Database, Router) {
    let database = Database::from_pool(pool);
    let app = router(
        ApiSettings::default(),
        ApiState::new(database.inbox(), "test-api-token", database.library(), database.publisher()),
    );
    (database, app)
}

async fn request(app: &Router, method: Method, uri: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("authorization", "Bearer test-api-token")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let body = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body).expect("API response should be JSON")
    };
    (status, body)
}

async fn stored_media(database: &Database, ingest_id: Uuid) -> Uuid {
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
                width: Some(1),
                height: Some(1),
                duration_ms: Some(1),
                bit_rate: None,
                file_size_bytes: Some(1),
                sha256: Some(Uuid::new_v4().as_bytes().repeat(2)),
                local_work_path: None,
            },
            source: MediaSourceInput {
                ingest_id: Some(ingest_id),
                kind: SourceKind::DirectUrl,
                original_url: Some("https://2ch.su/b/src/api-admin.webm".to_owned()),
                normalized_url: Some("https://2ch.su/b/src/api-admin.webm".to_owned()),
                platform: None,
                platform_content_id: None,
                author_name: None,
                title: None,
                description: None,
                published_at: None,
                metadata: json!({}),
            },
            tags: Vec::new(),
        })
        .await
        .unwrap();
    sqlx::query("UPDATE media SET storage_state = 'ready', telegram_storage_chat_id = -1003971341583, telegram_storage_message_id = 57, telegram_file_id = 'api-file' WHERE id = $1")
        .bind(result.media.id)
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query(
        "UPDATE ingests SET media_id = $2, state = 'completed', completed_at = now() WHERE id = $1",
    )
    .bind(ingest_id)
    .bind(result.media.id)
    .execute(database.pool())
    .await
    .unwrap();
    result.media.id
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn admin_api_lists_ingests_media_schedule_dashboard_and_channel_settings(pool: sqlx::PgPool) {
    let (database, app) = app(pool);
    let ingest = database
        .inbox()
        .create_ingest(
            IngestSubmission::try_new(IngestSubmissionInput::new(
                "https://example.test/api-admin",
                SubmittedVia::Api,
            ))
            .unwrap(),
        )
        .await
        .unwrap()
        .ingest
        .id;
    let media_id = stored_media(&database, ingest).await;
    sqlx::query("UPDATE media SET source_metadata = source_metadata || jsonb_build_object('caption_sync_state', 'failed', 'caption_sync_error', 'caption sync failed') WHERE id = $1")
        .bind(media_id)
        .execute(database.pool())
        .await
        .unwrap();
    let mut channel = NewChannel::try_new("api-admin-channel", -1003971341591).unwrap();
    channel.window_start = time::Time::MIDNIGHT;
    channel.window_end = time::Time::from_hms(23, 59, 0).unwrap();
    let channel = database.publisher().create_channel(channel).await.unwrap();
    let post = database
        .publisher()
        .create_post_idempotent(
            NewPost {
                media_id,
                channel_id: channel.id,
                caption: Some("api schedule".to_owned()),
                parse_mode: None,
                disable_notification: false,
            },
            "api-admin-post".to_owned(),
            b"api-admin-post",
        )
        .await
        .unwrap()
        .post;
    let exact =
        (OffsetDateTime::now_utc() + time::Duration::hours(1)).replace_nanosecond(0).unwrap();
    database
        .publisher()
        .schedule_post_exact(
            PostExactSchedule::try_new(post.id, exact, "api-admin-exact", 0).unwrap(),
        )
        .await
        .unwrap();

    let (status, ingests) =
        request(&app, Method::GET, "/api/v1/ingests?limit=50", Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ingests["items"][0]["id"], ingest.to_string());
    assert!(ingests["items"][0].get("error_message").is_some());

    let (status, media) = request(
        &app,
        Method::GET,
        "/api/v1/media?q=https%3A%2F%2F2ch.org%2Fb%2Fsrc%2Fapi-admin.webm",
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(media["items"][0]["id"], media_id.to_string());
    assert_eq!(media["items"][0]["storage_url"], "https://t.me/c/3971341583/57");
    assert_eq!(media["items"][0]["source_original_url"], "https://2ch.su/b/src/api-admin.webm");

    let (status, schedule) =
        request(&app, Method::GET, "/api/v1/posts?limit=50", Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(schedule["items"][0]["schedule_mode"], "explicit");
    assert_eq!(schedule["items"][0]["source_url"], "https://2ch.su/b/src/api-admin.webm");
    assert_eq!(schedule["items"][0]["storage_url"], "https://t.me/c/3971341583/57");

    let (status, dashboard) = request(&app, Method::GET, "/api/v1/dashboard", Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(dashboard["counts"]["ready_media"], 1);
    assert_eq!(dashboard["counts"]["active_ingests"], 0);
    assert_eq!(dashboard["counts"]["caption_sync_failures"], 1);
    assert_eq!(
        dashboard["attention"]["caption_sync_failures"][0]["media_id"],
        media_id.to_string()
    );
    assert_eq!(
        dashboard["attention"]["caption_sync_failures"][0]["error_message"],
        "caption sync failed"
    );

    let expected_updated_at =
        channel.updated_at.format(&time::format_description::well_known::Rfc3339).unwrap();
    let (status, settings) = request(
        &app,
        Method::PATCH,
        &format!("/api/v1/channels/{}", channel.id),
        json!({"name": "updated", "expected_updated_at": expected_updated_at}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(settings["name"], "updated");

    let (status, disabled) = request(
        &app,
        Method::PATCH,
        &format!("/api/v1/channels/{}", channel.id),
        json!({
            "is_enabled": false,
            "expected_updated_at": settings["updated_at"].as_str().unwrap()
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(disabled["is_enabled"], false);

    let (status, channels) = request(&app, Method::GET, "/api/v1/channels", Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        channels["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| { item["id"] == channel.id.to_string() && item["is_enabled"] == false })
    );

    let (status, _) = request(
        &app,
        Method::PATCH,
        &format!("/api/v1/channels/{}", channel.id),
        json!({
            "is_enabled": true,
            "expected_updated_at": disabled["updated_at"].as_str().unwrap()
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = request(
        &app,
        Method::POST,
        "/api/v1/channels",
        json!({"name": "second", "telegram_chat_id": -1003971341592_i64}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);

    let (status, _) = request(
        &app,
        Method::PATCH,
        &format!("/api/v1/channels/{}", channel.id),
        json!({"name": "missing-fence"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}
