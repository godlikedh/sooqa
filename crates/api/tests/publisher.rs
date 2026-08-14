use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode},
};
use serde_json::{Value, json};
use sooqa_library::{
    MediaIngest, MediaKind, MediaMetadata, MediaSourceInput, NewMedia, SourceKind,
};
use sooqa_persistence::Database;
use sooqa_publisher::{NewChannel, NewPost, PostSchedule};
use time::OffsetDateTime;
use tower::ServiceExt;
use uuid::Uuid;

fn app(pool: sqlx::PgPool) -> (Database, Router) {
    let database = Database::from_pool(pool);
    let app = sooqa_api::router(
        sooqa_api::ApiSettings::default(),
        sooqa_api::ApiState::new(
            database.inbox(),
            "test-api-token",
            database.library(),
            database.publisher(),
        ),
    );
    (database, app)
}

async fn send(app: &Router, method: Method, uri: &str, body: Value) -> StatusCode {
    app.clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("content-type", "application/json")
                .header("authorization", "Bearer test-api-token")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

async fn stored_media(database: &Database) -> Uuid {
    let media = database
        .library()
        .resolve_media(MediaIngest {
            media: NewMedia::new(MediaKind::Video),
            metadata: MediaMetadata {
                kind: MediaKind::Video,
                mime_type: Some("video/mp4".to_owned()),
                container: Some("mp4".to_owned()),
                video_codec: None,
                audio_codec: None,
                width: Some(1),
                height: Some(1),
                duration_ms: Some(1),
                bit_rate: None,
                file_size_bytes: Some(1),
                sha256: Some(vec![Uuid::new_v4().as_bytes()[0]; 32]),
                local_work_path: None,
            },
            source: MediaSourceInput {
                ingest_id: None,
                kind: SourceKind::DirectUrl,
                original_url: Some(format!("https://example.test/{}", Uuid::new_v4())),
                normalized_url: None,
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
    sqlx::query(
        "UPDATE media SET storage_state = 'ready', telegram_storage_chat_id = -100123, telegram_storage_message_id = 1, telegram_file_id = 'file' WHERE id = $1",
    )
    .bind(media.media.id)
    .execute(database.pool())
    .await
    .unwrap();
    media.media.id
}

async fn channel(database: &Database) -> sooqa_publisher::Channel {
    let mut new_channel =
        NewChannel::try_new(format!("test-{}", Uuid::new_v4()), -1000000000010).unwrap();
    new_channel.window_start = time::Time::from_hms(0, 0, 0).unwrap();
    new_channel.window_end = time::Time::from_hms(23, 59, 0).unwrap();
    new_channel.interval_minutes = 1;
    database.publisher().create_channel(new_channel).await.unwrap()
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn mutation_api_requires_nonnegative_expected_revision(pool: sqlx::PgPool) {
    let (_database, app) = app(pool);
    let id = Uuid::now_v7();
    let cases = [
        (
            Method::PATCH,
            format!("/api/v1/posts/{id}"),
            json!({"caption": "x"}),
            json!({"caption": "x", "expected_revision": -1}),
        ),
        (
            Method::POST,
            format!("/api/v1/posts/{id}/schedule"),
            json!({}),
            json!({"expected_revision": -1}),
        ),
        (
            Method::POST,
            format!("/api/v1/posts/{id}/publish"),
            json!({}),
            json!({"expected_revision": -1}),
        ),
        (
            Method::POST,
            format!("/api/v1/posts/{id}/cancel"),
            json!({}),
            json!({"expected_revision": -1}),
        ),
    ];
    for (method, uri, missing, negative) in cases {
        assert_eq!(
            send(&app, method.clone(), &uri, missing).await,
            StatusCode::BAD_REQUEST,
            "missing revision for {uri}"
        );
        assert_eq!(
            send(&app, method, &uri, negative).await,
            StatusCode::BAD_REQUEST,
            "negative revision for {uri}"
        );
    }
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn superseded_queue_mutation_routes_are_not_registered(pool: sqlx::PgPool) {
    let (_database, app) = app(pool);
    let id = Uuid::now_v7();
    for path in [
        format!("/api/v1/posts/{id}/earlier"),
        format!("/api/v1/posts/{id}/later"),
        format!("/api/v1/posts/{id}/slot"),
    ] {
        assert_eq!(send(&app, Method::POST, &path, json!({})).await, StatusCode::NOT_FOUND);
    }
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn stale_edit_is_rejected_by_the_http_revision_fence(pool: sqlx::PgPool) {
    let (database, app) = app(pool);
    let channel = channel(&database).await;
    let post = database
        .publisher()
        .create_post_idempotent(
            NewPost {
                media_id: stored_media(&database).await,
                channel_id: channel.id,
                caption: Some("original".to_owned()),
                parse_mode: None,
                disable_notification: false,
            },
            format!("post-{}", Uuid::new_v4()),
            b"api-stale",
        )
        .await
        .unwrap()
        .post;
    let queued = database
        .publisher()
        .schedule_post(
            PostSchedule::try_new(post.id, OffsetDateTime::now_utc(), "schedule", 0).unwrap(),
        )
        .await
        .unwrap();

    let status = send(
        &app,
        Method::PATCH,
        &format!("/api/v1/posts/{}", queued.id),
        json!({"caption": "stale", "expected_revision": 0}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        database.publisher().find_post(queued.id).await.unwrap().unwrap().caption.as_deref(),
        Some("original")
    );
}
