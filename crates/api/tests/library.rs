use std::env;

use axum::{
    body::{Body, to_bytes},
    http::{HeaderValue, Request, StatusCode},
};
use serde_json::{Value, json};
use sooqa_api::{ApiSettings, ApiState, router};
use sooqa_library::{
    AssetRole, ContentKind, ExactDuplicateRequest, MediaKind, NewContentItem,
    NewDuplicateCandidate, NewMediaAssetDraft, NewSourceRecordDraft, SourceType, StorageState,
};
use sooqa_persistence::{Database, hash_device_token};
use tower::util::ServiceExt;
use uuid::Uuid;

struct Fixture {
    database: Database,
    read_token: String,
    write_token: String,
    token_prefix: String,
    content_ids: Vec<Uuid>,
    tag_ids: Vec<Uuid>,
}

impl Fixture {
    async fn create() -> Self {
        let database_url =
            env::var("DATABASE_URL").expect("DATABASE_URL must point to the integration database");
        let database =
            Database::connect(&database_url, 10).await.expect("database should be reachable");
        database.migrate().await.expect("migrations should succeed");

        let token_prefix = format!("e3-{}", Uuid::new_v4());
        let read_token = format!("{token_prefix}-read-token-with-enough-entropy");
        let write_token = format!("{token_prefix}-write-token-with-enough-entropy");

        for (name, suffix, token, scopes) in [
            ("e3-read", "read", &read_token, vec!["library:read".to_owned()]),
            (
                "e3-write",
                "write",
                &write_token,
                vec!["library:read".to_owned(), "library:write".to_owned()],
            ),
        ] {
            sqlx::query(
                r#"
                INSERT INTO device_tokens (name, token_prefix, token_hash, scopes)
                VALUES ($1, $2, $3, $4)
                "#,
            )
            .bind(name)
            .bind(format!("{token_prefix}-{suffix}"))
            .bind(hash_device_token(token))
            .bind(scopes)
            .execute(database.pool())
            .await
            .expect("library API token should seed");
        }

        Self {
            database,
            read_token,
            write_token,
            token_prefix,
            content_ids: Vec::new(),
            tag_ids: Vec::new(),
        }
    }

    async fn seed_item(&mut self, title: &str, url_suffix: &str, sha_byte: u8) -> Uuid {
        let url = format!("https://library-api.test/{url_suffix}/{}", Uuid::new_v4());
        let resolution = self
            .database
            .library()
            .resolve_exact_duplicate(ExactDuplicateRequest {
                content_item: NewContentItem {
                    kind: ContentKind::Video,
                    preferred_title: Some(title.to_owned()),
                    editorial_description: Some("E3 API test item".to_owned()),
                    notes: Some("test notes".to_owned()),
                },
                asset: NewMediaAssetDraft {
                    role: AssetRole::Canonical,
                    media_kind: MediaKind::Video,
                    mime_type: Some("video/webm".to_owned()),
                    container: Some("webm".to_owned()),
                    video_codec: Some("vp9".to_owned()),
                    audio_codec: Some("opus".to_owned()),
                    width: Some(1280),
                    height: Some(720),
                    duration_ms: Some(3500),
                    bit_rate: Some(500_000),
                    file_size_bytes: Some(42),
                    sha256: Some(vec![sha_byte; 32]),
                    local_work_path: Some(format!("/tmp/sooqa-e3-{sha_byte}.webm")),
                    storage_state: StorageState::Local,
                },
                source: NewSourceRecordDraft {
                    ingest_request_id: None,
                    source_type: SourceType::DirectUrl,
                    original_url: Some(url.clone()),
                    normalized_url: Some(url),
                    platform: None,
                    platform_content_id: None,
                    author_name: Some("E3 test author".to_owned()),
                    source_title: Some("E3 source title".to_owned()),
                    source_description: Some("E3 source description".to_owned()),
                    source_published_at: None,
                    metadata_json: json!({"fixture": "library-api"}),
                },
            })
            .await
            .expect("library item should seed");
        self.content_ids.push(resolution.content_item.id);
        resolution.content_item.id
    }

    async fn clean_up(&self) {
        for content_id in &self.content_ids {
            sqlx::query("DELETE FROM source_records WHERE content_item_id = $1")
                .bind(content_id)
                .execute(self.database.pool())
                .await
                .expect("test sources should clean up");
            sqlx::query("DELETE FROM content_item_tags WHERE content_item_id = $1")
                .bind(content_id)
                .execute(self.database.pool())
                .await
                .expect("test tag attachments should clean up");
            sqlx::query("UPDATE content_items SET canonical_asset_id = NULL WHERE id = $1")
                .bind(content_id)
                .execute(self.database.pool())
                .await
                .expect("canonical asset references should clean up");
            sqlx::query("DELETE FROM media_assets WHERE content_item_id = $1")
                .bind(content_id)
                .execute(self.database.pool())
                .await
                .expect("test assets should clean up");
            sqlx::query("DELETE FROM content_items WHERE id = $1")
                .bind(content_id)
                .execute(self.database.pool())
                .await
                .expect("test content items should clean up");
        }
        for tag_id in &self.tag_ids {
            sqlx::query("DELETE FROM tags WHERE id = $1")
                .bind(tag_id)
                .execute(self.database.pool())
                .await
                .expect("test tags should clean up");
        }
        sqlx::query("DELETE FROM device_tokens WHERE token_prefix LIKE $1")
            .bind(format!("{}-%", self.token_prefix))
            .execute(self.database.pool())
            .await
            .expect("test device tokens should clean up");
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

fn request(method: &str, uri: String, token: &str, body: Option<Value>) -> Request<Body> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", format!("Bearer {token}"));
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
async fn authenticated_library_api_supports_search_edit_tags_and_archive() {
    let mut fixture = Fixture::create().await;
    let cat_id = fixture.seed_item("Cats reaction", "cats", 31).await;
    let dog_id = fixture.seed_item("Dogs reaction", "dogs", 32).await;

    let tag = fixture
        .database
        .library()
        .add_tag(cat_id, sooqa_library::NewTag::try_new("Reaction").expect("tag should be valid"))
        .await
        .expect("tag should attach");
    fixture.tag_ids.push(tag.id);

    let detail = app(&fixture)
        .oneshot(request(
            "GET",
            format!("/api/v1/library/items/{cat_id}"),
            &fixture.read_token,
            None,
        ))
        .await
        .expect("router should respond");
    assert_eq!(detail.status(), StatusCode::OK);
    let detail_body = response_json(detail).await;
    assert_eq!(detail_body["id"], cat_id.to_string());
    assert_eq!(detail_body["canonical_asset"]["container"], "webm");
    assert_eq!(detail_body["tags"][0]["normalized_name"], "reaction");
    assert_eq!(detail_body["sources"][0]["source_type"], "direct_url");

    let tagged = app(&fixture)
        .oneshot(request(
            "GET",
            "/api/v1/library/items?tags=REACTION".to_owned(),
            &fixture.read_token,
            None,
        ))
        .await
        .expect("router should respond");
    assert_eq!(tagged.status(), StatusCode::OK);
    let tagged_body = response_json(tagged).await;
    assert_eq!(tagged_body["items"].as_array().expect("items should be an array").len(), 1);
    assert_eq!(tagged_body["items"][0]["id"], cat_id.to_string());

    let first_page = app(&fixture)
        .oneshot(request(
            "GET",
            "/api/v1/library/items?q=Reaction&limit=1".to_owned(),
            &fixture.read_token,
            None,
        ))
        .await
        .expect("router should respond");
    assert_eq!(first_page.status(), StatusCode::OK);
    let first_page_body = response_json(first_page).await;
    assert_eq!(first_page_body["items"].as_array().expect("items should be an array").len(), 1);
    let cursor = first_page_body["next_cursor"].as_str().expect("next cursor should exist");

    let second_page = app(&fixture)
        .oneshot(request(
            "GET",
            format!("/api/v1/library/items?q=Reaction&limit=1&cursor={cursor}"),
            &fixture.read_token,
            None,
        ))
        .await
        .expect("router should respond");
    assert_eq!(second_page.status(), StatusCode::OK);
    let second_page_body = response_json(second_page).await;
    assert_eq!(second_page_body["items"].as_array().expect("items should be an array").len(), 1);
    assert_ne!(first_page_body["items"][0]["id"], second_page_body["items"][0]["id"]);
    assert!(
        [cat_id.to_string(), dog_id.to_string()]
            .contains(&second_page_body["items"][0]["id"].as_str().unwrap().to_owned())
    );

    let read_cannot_write = app(&fixture)
        .oneshot(request(
            "PATCH",
            format!("/api/v1/library/items/{cat_id}"),
            &fixture.read_token,
            Some(json!({"preferred_title": "Not allowed"})),
        ))
        .await
        .expect("router should respond");
    assert_eq!(read_cannot_write.status(), StatusCode::FORBIDDEN);

    let updated = app(&fixture)
        .oneshot(request(
            "PATCH",
            format!("/api/v1/library/items/{cat_id}"),
            &fixture.write_token,
            Some(json!({"preferred_title": "Updated cats reaction"})),
        ))
        .await
        .expect("router should respond");
    assert_eq!(updated.status(), StatusCode::OK);
    let updated_body = response_json(updated).await;
    assert_eq!(updated_body["preferred_title"], "Updated cats reaction");

    let stale_update = app(&fixture)
        .oneshot(request(
            "PATCH",
            format!("/api/v1/library/items/{cat_id}"),
            &fixture.write_token,
            Some(json!({
                "preferred_title": "Stale update",
                "expected_updated_at": detail_body["updated_at"].clone()
            })),
        ))
        .await
        .expect("router should respond");
    let stale_status = stale_update.status();
    let stale_body = response_json(stale_update).await;
    assert_eq!(stale_status, StatusCode::CONFLICT, "{stale_body}");
    assert_eq!(stale_body["error"]["code"], "library_item_changed", "{stale_body}");

    let added_tag = app(&fixture)
        .oneshot(request(
            "POST",
            format!("/api/v1/library/items/{cat_id}/tags"),
            &fixture.write_token,
            Some(json!({"tag": "Vertical"})),
        ))
        .await
        .expect("router should respond");
    assert_eq!(added_tag.status(), StatusCode::OK);
    let added_tag_body = response_json(added_tag).await;
    assert_eq!(added_tag_body["normalized_name"], "vertical");
    fixture.tag_ids.push(
        Uuid::parse_str(added_tag_body["id"].as_str().expect("tag ID should exist"))
            .expect("tag ID should be valid"),
    );

    let removed_tag = app(&fixture)
        .oneshot(request(
            "DELETE",
            format!("/api/v1/library/items/{cat_id}/tags/vertical"),
            &fixture.write_token,
            None,
        ))
        .await
        .expect("router should respond");
    assert_eq!(removed_tag.status(), StatusCode::NO_CONTENT);

    let archived = app(&fixture)
        .oneshot(request(
            "POST",
            format!("/api/v1/library/items/{cat_id}/archive"),
            &fixture.write_token,
            None,
        ))
        .await
        .expect("router should respond");
    assert_eq!(archived.status(), StatusCode::OK);
    let archived_body = response_json(archived).await;
    assert_eq!(archived_body["status"], "archived");
    assert!(archived_body["archived_at"].is_string(), "{archived_body}");

    let active_search = app(&fixture)
        .oneshot(request(
            "GET",
            "/api/v1/library/items?q=Updated%20cats".to_owned(),
            &fixture.read_token,
            None,
        ))
        .await
        .expect("router should respond");
    assert_eq!(active_search.status(), StatusCode::OK);
    assert!(
        response_json(active_search).await["items"]
            .as_array()
            .expect("items should be an array")
            .is_empty()
    );

    let archived_search = app(&fixture)
        .oneshot(request(
            "GET",
            "/api/v1/library/items?status=archived&q=Updated%20cats".to_owned(),
            &fixture.read_token,
            None,
        ))
        .await
        .expect("router should respond");
    assert_eq!(archived_search.status(), StatusCode::OK);
    assert_eq!(response_json(archived_search).await["items"][0]["id"], cat_id.to_string());

    fixture.clean_up().await;
}

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance"]
async fn authenticated_duplicate_candidate_api_supports_review_actions() {
    let mut fixture = Fixture::create().await;
    let left_id = fixture.seed_item("Candidate left", "candidate-left", 41).await;
    let right_id = fixture.seed_item("Candidate right", "candidate-right", 42).await;
    let candidate = fixture
        .database
        .library()
        .upsert_duplicate_candidate(
            NewDuplicateCandidate::try_new(
                right_id,
                left_id,
                "frame_dhash_v1",
                9_100,
                json!({"final_score": 0.91, "frame_distances": []}),
            )
            .expect("candidate should be valid"),
        )
        .await
        .expect("candidate should be stored");

    let listed = app(&fixture)
        .oneshot(request(
            "GET",
            "/api/v1/duplicate-candidates?status=pending".to_owned(),
            &fixture.read_token,
            None,
        ))
        .await
        .expect("router should respond");
    assert_eq!(listed.status(), StatusCode::OK);
    let listed_body = response_json(listed).await;
    assert!(
        listed_body["items"]
            .as_array()
            .expect("items should be an array")
            .iter()
            .any(|item| item["id"] == candidate.id.to_string())
    );

    let read_detail = app(&fixture)
        .oneshot(request(
            "GET",
            format!("/api/v1/duplicate-candidates/{}", candidate.id),
            &fixture.read_token,
            None,
        ))
        .await
        .expect("router should respond");
    assert_eq!(read_detail.status(), StatusCode::OK);
    let read_detail_body = response_json(read_detail).await;
    assert_eq!(read_detail_body["candidate"]["status"], "pending");
    assert!(read_detail_body["events"].as_array().expect("events should be an array").is_empty());

    let malformed_detail = app(&fixture)
        .oneshot(request(
            "GET",
            "/api/v1/duplicate-candidates/not-a-uuid".to_owned(),
            &fixture.read_token,
            None,
        ))
        .await
        .expect("router should respond");
    assert_eq!(malformed_detail.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response_json(malformed_detail).await["error"]["code"], "invalid_candidate_id");

    let read_cannot_decide = app(&fixture)
        .oneshot(request(
            "POST",
            format!("/api/v1/duplicate-candidates/{}/confirm-variant", candidate.id),
            &fixture.read_token,
            None,
        ))
        .await
        .expect("router should respond");
    assert_eq!(read_cannot_decide.status(), StatusCode::FORBIDDEN);

    let confirmed = app(&fixture)
        .oneshot(request_with_idempotency_key(
            "POST",
            format!("/api/v1/duplicate-candidates/{}/confirm-variant", candidate.id),
            &fixture.write_token,
            "candidate-confirm",
        ))
        .await
        .expect("router should respond");
    assert_eq!(confirmed.status(), StatusCode::OK);
    let confirmed_body = response_json(confirmed).await;
    assert_eq!(confirmed_body["candidate"]["status"], "confirmed_variant");
    assert_eq!(confirmed_body["events"][0]["action"], "confirm_variant");

    let replayed = app(&fixture)
        .oneshot(request_with_idempotency_key(
            "POST",
            format!("/api/v1/duplicate-candidates/{}/confirm-variant", candidate.id),
            &fixture.write_token,
            "candidate-confirm",
        ))
        .await
        .expect("router should respond");
    assert_eq!(replayed.status(), StatusCode::OK);
    let replayed_body = response_json(replayed).await;
    assert_eq!(replayed_body["candidate"]["status"], "confirmed_variant");
    assert_eq!(replayed_body["events"].as_array().expect("events should be an array").len(), 1);

    let second_decision = app(&fixture)
        .oneshot(request_with_idempotency_key(
            "POST",
            format!("/api/v1/duplicate-candidates/{}/dismiss", candidate.id),
            &fixture.write_token,
            "candidate-dismiss",
        ))
        .await
        .expect("router should respond");
    assert_eq!(second_decision.status(), StatusCode::CONFLICT);
    assert_eq!(response_json(second_decision).await["error"]["code"], "invalid_candidate_state");

    fixture.clean_up().await;
}

fn request_with_idempotency_key(
    method: &str,
    uri: String,
    token: &str,
    idempotency_key: &str,
) -> Request<Body> {
    let mut request = request(method, uri, token, None);
    request.headers_mut().insert(
        "idempotency-key",
        HeaderValue::from_str(idempotency_key).expect("idempotency key should be valid"),
    );
    request
}
