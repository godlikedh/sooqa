use std::env;

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use sooqa_api::{ApiSettings, ApiState, router};
use sooqa_library::{
    AssetRole, ContentKind, ExactDuplicateRequest, MediaKind, NewContentItem, NewMediaAssetDraft,
    NewSourceRecordDraft, SourceType, StorageState,
};
use sooqa_persistence::{Database, hash_device_token};
use sooqa_publisher::NewTargetChannel;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tower::util::ServiceExt;
use uuid::Uuid;

struct Fixture {
    database: Database,
    key_prefix: String,
    read_token: String,
    write_token: String,
    content_id: Uuid,
    target_channel_id: Uuid,
}

impl Fixture {
    async fn create() -> Self {
        let database_url =
            env::var("DATABASE_URL").expect("DATABASE_URL must point to the integration database");
        let database =
            Database::connect(&database_url, 10).await.expect("database should be reachable");
        database.migrate().await.expect("migrations should succeed");

        let key_prefix = format!("i2-api-{}-", Uuid::new_v4());
        let read_token = format!("{key_prefix}read-token-with-enough-entropy");
        let write_token = format!("{key_prefix}write-token-with-enough-entropy");
        for (name, token_suffix, token, scopes) in [
            (format!("{key_prefix}read"), "read", &read_token, vec!["publisher:read".to_owned()]),
            (
                format!("{key_prefix}write"),
                "write",
                &write_token,
                vec!["publisher:read".to_owned(), "publisher:write".to_owned()],
            ),
        ] {
            sqlx::query(
                r#"
                INSERT INTO device_tokens (name, token_prefix, token_hash, scopes)
                VALUES ($1, $2, $3, $4)
                "#,
            )
            .bind(name)
            .bind(format!("{key_prefix}{token_suffix}"))
            .bind(hash_device_token(token))
            .bind(scopes)
            .execute(database.pool())
            .await
            .expect("publisher API token should seed");
        }

        let mut new_target = NewTargetChannel::try_new(
            format!("{key_prefix}channel"),
            -1_000_000_000_000_i64
                - i64::try_from(Uuid::new_v4().as_u128() % 1_000_000)
                    .expect("bounded UUID fragment should fit in i64"),
        )
        .expect("target channel should be valid");
        new_target.default_parse_mode = Some("HTML".to_owned());
        let target = database
            .publisher()
            .create_target_channel(new_target)
            .await
            .expect("target channel should seed");

        let mut sha256 = Vec::from(Uuid::new_v4().as_bytes());
        sha256.extend_from_slice(Uuid::new_v4().as_bytes());

        let resolution = database
            .library()
            .resolve_exact_duplicate(ExactDuplicateRequest {
                content_item: NewContentItem {
                    kind: ContentKind::Video,
                    preferred_title: Some("I2 publisher API fixture".to_owned()),
                    editorial_description: None,
                    notes: None,
                },
                asset: NewMediaAssetDraft {
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
                    sha256: Some(sha256),
                    local_work_path: None,
                    storage_state: StorageState::Uploaded,
                },
                source: NewSourceRecordDraft {
                    ingest_request_id: None,
                    source_type: SourceType::DirectUrl,
                    original_url: Some(format!("https://i2-api.test/{key_prefix}")),
                    normalized_url: Some(format!("https://i2-api.test/{key_prefix}")),
                    platform: None,
                    platform_content_id: None,
                    author_name: None,
                    source_title: None,
                    source_description: None,
                    source_published_at: None,
                    metadata_json: json!({"fixture": "publisher-api"}),
                },
            })
            .await
            .expect("publisher API content should seed");

        Self {
            database,
            key_prefix,
            read_token,
            write_token,
            content_id: resolution.content_item.id,
            target_channel_id: target.id,
        }
    }

    async fn clean_up(&self) {
        sqlx::query(
            "DELETE FROM idempotency_records WHERE scope IN ('publisher:draft:create', 'publisher:draft:update') AND idempotency_key LIKE $1",
        )
        .bind(format!("{}%", self.key_prefix))
        .execute(self.database.pool())
        .await
        .expect("publisher idempotency records should clean up");
        sqlx::query("DELETE FROM publication_schedules WHERE post_draft_id IN (SELECT id FROM post_drafts WHERE content_item_id = $1)")
            .bind(self.content_id)
            .execute(self.database.pool())
            .await
            .expect("publisher schedules should clean up");
        sqlx::query("DELETE FROM post_drafts WHERE content_item_id = $1")
            .bind(self.content_id)
            .execute(self.database.pool())
            .await
            .expect("publisher drafts should clean up");
        sqlx::query("DELETE FROM target_channels WHERE id = $1")
            .bind(self.target_channel_id)
            .execute(self.database.pool())
            .await
            .expect("publisher target should clean up");
        sqlx::query("DELETE FROM source_records WHERE content_item_id = $1")
            .bind(self.content_id)
            .execute(self.database.pool())
            .await
            .expect("publisher sources should clean up");
        sqlx::query("UPDATE content_items SET canonical_asset_id = NULL WHERE id = $1")
            .bind(self.content_id)
            .execute(self.database.pool())
            .await
            .expect("publisher canonical pointer should clean up");
        sqlx::query("DELETE FROM media_assets WHERE content_item_id = $1")
            .bind(self.content_id)
            .execute(self.database.pool())
            .await
            .expect("publisher assets should clean up");
        sqlx::query("DELETE FROM content_items WHERE id = $1")
            .bind(self.content_id)
            .execute(self.database.pool())
            .await
            .expect("publisher content should clean up");
        sqlx::query("DELETE FROM device_tokens WHERE name LIKE $1")
            .bind(format!("{}%", self.key_prefix))
            .execute(self.database.pool())
            .await
            .expect("publisher tokens should clean up");
    }
}

fn app(fixture: &Fixture) -> axum::Router {
    router(
        ApiSettings::default(),
        ApiState::new(
            fixture.database.inbox(),
            fixture.database.device_tokens(),
            fixture.database.library(),
            fixture.database.publisher(),
        ),
    )
}

fn request(
    method: &str,
    uri: String,
    token: Option<&str>,
    idempotency_key: Option<&str>,
    body: Option<Value>,
) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    if let Some(idempotency_key) = idempotency_key {
        builder = builder.header("idempotency-key", idempotency_key);
    }
    if body.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    builder
        .body(body.map_or_else(Body::empty, |body| Body::from(body.to_string())))
        .expect("request should build")
}

async fn response_json(response: axum::response::Response) -> Value {
    let body = to_bytes(response.into_body(), 64 * 1024).await.expect("body should be readable");
    serde_json::from_slice(&body).expect("response should contain JSON")
}

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance"]
async fn authenticated_publisher_api_creates_edits_and_schedules_drafts() {
    let fixture = Fixture::create().await;
    let create_key = format!("{}create", fixture.key_prefix);
    let draft_body = json!({
        "content_item_id": fixture.content_id,
        "target_channel_id": fixture.target_channel_id,
        "caption": "Initial caption"
    });

    let forbidden = app(&fixture)
        .oneshot(request(
            "POST",
            "/api/v1/post-drafts".to_owned(),
            Some(&fixture.read_token),
            Some(&create_key),
            Some(draft_body.clone()),
        ))
        .await
        .expect("router should respond");
    assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);

    let created = app(&fixture)
        .oneshot(request(
            "POST",
            "/api/v1/post-drafts".to_owned(),
            Some(&fixture.write_token),
            Some(&create_key),
            Some(draft_body.clone()),
        ))
        .await
        .expect("router should respond");
    assert_eq!(created.status(), StatusCode::CREATED);
    let created_body = response_json(created).await;
    let draft_id = created_body["id"].as_str().expect("draft ID should exist").to_owned();
    assert_eq!(created_body["status"], "editing");
    assert_eq!(created_body["parse_mode"], "HTML");

    let replayed = app(&fixture)
        .oneshot(request(
            "POST",
            "/api/v1/post-drafts".to_owned(),
            Some(&fixture.write_token),
            Some(&create_key),
            Some(draft_body),
        ))
        .await
        .expect("router should respond");
    assert_eq!(replayed.status(), StatusCode::CREATED);
    assert_eq!(response_json(replayed).await["id"], draft_id);

    let conflict = app(&fixture)
        .oneshot(request(
            "POST",
            "/api/v1/post-drafts".to_owned(),
            Some(&fixture.write_token),
            Some(&create_key),
            Some(json!({
                "content_item_id": fixture.content_id,
                "target_channel_id": fixture.target_channel_id,
                "caption": "different"
            })),
        ))
        .await
        .expect("router should respond");
    assert_eq!(conflict.status(), StatusCode::CONFLICT);
    assert_eq!(response_json(conflict).await["error"]["code"], "idempotency_conflict");

    let read = app(&fixture)
        .oneshot(request(
            "GET",
            format!("/api/v1/post-drafts/{draft_id}"),
            Some(&fixture.read_token),
            None,
            None,
        ))
        .await
        .expect("router should respond");
    assert_eq!(read.status(), StatusCode::OK);

    let edit_key = format!("{}edit", fixture.key_prefix);
    let ready = app(&fixture)
        .oneshot(request(
            "PATCH",
            format!("/api/v1/post-drafts/{draft_id}"),
            Some(&fixture.write_token),
            Some(&edit_key),
            Some(json!({
                "caption": "Ready caption",
                "status": "ready",
                "expected_updated_at": created_body["updated_at"]
            })),
        ))
        .await
        .expect("router should respond");
    assert_eq!(ready.status(), StatusCode::OK);
    let ready_body = response_json(ready).await;
    assert_eq!(ready_body["status"], "ready");
    assert_eq!(ready_body["caption"], "Ready caption");

    let edit_replay = app(&fixture)
        .oneshot(request(
            "PATCH",
            format!("/api/v1/post-drafts/{draft_id}"),
            Some(&fixture.write_token),
            Some(&edit_key),
            Some(json!({
                "caption": "Ready caption",
                "status": "ready",
                "expected_updated_at": created_body["updated_at"]
            })),
        ))
        .await
        .expect("router should respond");
    assert_eq!(edit_replay.status(), StatusCode::OK);
    assert_eq!(response_json(edit_replay).await["status"], "ready");

    let publish_at =
        OffsetDateTime::now_utc().replace_nanosecond(0).expect("timestamp should be valid");
    let publish_at_wire = publish_at.format(&Rfc3339).expect("timestamp should format");
    let schedule_key = format!("{}schedule", fixture.key_prefix);
    let scheduled = app(&fixture)
        .oneshot(request(
            "POST",
            format!("/api/v1/post-drafts/{draft_id}/schedule"),
            Some(&fixture.write_token),
            Some(&schedule_key),
            Some(json!({"publish_at": publish_at_wire.clone()})),
        ))
        .await
        .expect("router should respond");
    assert_eq!(scheduled.status(), StatusCode::ACCEPTED);
    let scheduled_body = response_json(scheduled).await;
    let schedule_id = scheduled_body["id"].as_str().expect("schedule ID should exist");
    assert_eq!(scheduled_body["status"], "pending");

    let schedule_replay = app(&fixture)
        .oneshot(request(
            "POST",
            format!("/api/v1/post-drafts/{draft_id}/schedule"),
            Some(&fixture.write_token),
            Some(&schedule_key),
            Some(json!({"publish_at": publish_at_wire})),
        ))
        .await
        .expect("router should respond");
    assert_eq!(schedule_replay.status(), StatusCode::ACCEPTED);
    assert_eq!(response_json(schedule_replay).await["id"], schedule_id);

    let edit_after_schedule = app(&fixture)
        .oneshot(request(
            "PATCH",
            format!("/api/v1/post-drafts/{draft_id}"),
            Some(&fixture.write_token),
            Some(&edit_key),
            Some(json!({
                "caption": "Ready caption",
                "status": "ready",
                "expected_updated_at": created_body["updated_at"]
            })),
        ))
        .await
        .expect("router should respond");
    assert_eq!(edit_after_schedule.status(), StatusCode::OK);
    assert_eq!(response_json(edit_after_schedule).await["updated_at"], ready_body["updated_at"]);

    let now_create_key = format!("{}now-create", fixture.key_prefix);
    let now_created = app(&fixture)
        .oneshot(request(
            "POST",
            "/api/v1/post-drafts".to_owned(),
            Some(&fixture.write_token),
            Some(&now_create_key),
            Some(json!({
                "content_item_id": fixture.content_id,
                "target_channel_id": fixture.target_channel_id,
                "caption": "Immediate caption"
            })),
        ))
        .await
        .expect("router should respond");
    assert_eq!(now_created.status(), StatusCode::CREATED);
    let now_created_body = response_json(now_created).await;
    let now_draft_id = now_created_body["id"].as_str().expect("draft ID should exist");
    let now_edit_key = format!("{}now-edit", fixture.key_prefix);
    let now_ready = app(&fixture)
        .oneshot(request(
            "PATCH",
            format!("/api/v1/post-drafts/{now_draft_id}"),
            Some(&fixture.write_token),
            Some(&now_edit_key),
            Some(json!({
                "status": "ready",
                "expected_updated_at": now_created_body["updated_at"]
            })),
        ))
        .await
        .expect("router should respond");
    assert_eq!(now_ready.status(), StatusCode::OK);

    let now_key = schedule_key.clone();
    let published_now = app(&fixture)
        .oneshot(request(
            "POST",
            format!("/api/v1/post-drafts/{now_draft_id}/publish-now"),
            Some(&fixture.write_token),
            Some(&now_key),
            None,
        ))
        .await
        .expect("router should respond");
    assert_eq!(published_now.status(), StatusCode::ACCEPTED);
    let published_now_body = response_json(published_now).await;
    assert_eq!(published_now_body["status"], "pending");

    let publish_now_replay = app(&fixture)
        .oneshot(request(
            "POST",
            format!("/api/v1/post-drafts/{now_draft_id}/publish-now"),
            Some(&fixture.write_token),
            Some(&now_key),
            None,
        ))
        .await
        .expect("router should respond");
    assert_eq!(publish_now_replay.status(), StatusCode::ACCEPTED);
    assert_eq!(response_json(publish_now_replay).await["id"], published_now_body["id"]);

    fixture.clean_up().await;
}

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance"]
async fn publisher_api_validates_caption_and_parse_mode() {
    let fixture = Fixture::create().await;
    let oversized = app(&fixture)
        .oneshot(request(
            "POST",
            "/api/v1/post-drafts".to_owned(),
            Some(&fixture.write_token),
            Some(&format!("{}oversized", fixture.key_prefix)),
            Some(json!({
                "content_item_id": fixture.content_id,
                "target_channel_id": fixture.target_channel_id,
                "caption": "x".repeat(1025)
            })),
        ))
        .await
        .expect("router should respond");
    assert_eq!(oversized.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response_json(oversized).await["error"]["code"], "caption_too_long");

    let invalid_mode = app(&fixture)
        .oneshot(request(
            "POST",
            "/api/v1/post-drafts".to_owned(),
            Some(&fixture.write_token),
            Some(&format!("{}mode", fixture.key_prefix)),
            Some(json!({
                "content_item_id": fixture.content_id,
                "target_channel_id": fixture.target_channel_id,
                "parse_mode": "Markdown"
            })),
        ))
        .await
        .expect("router should respond");
    assert_eq!(invalid_mode.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response_json(invalid_mode).await["error"]["code"], "invalid_parse_mode");

    let invalid_markup = app(&fixture)
        .oneshot(request(
            "POST",
            "/api/v1/post-drafts".to_owned(),
            Some(&fixture.write_token),
            Some(&format!("{}markup", fixture.key_prefix)),
            Some(json!({
                "content_item_id": fixture.content_id,
                "target_channel_id": fixture.target_channel_id,
                "caption": "<b>unclosed",
                "parse_mode": "HTML"
            })),
        ))
        .await
        .expect("router should respond");
    assert_eq!(invalid_markup.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response_json(invalid_markup).await["error"]["code"], "invalid_caption_markup");

    fixture.clean_up().await;
}
