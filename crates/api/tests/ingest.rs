use std::env;

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use sooqa_api::{ApiSettings, ApiState, router};
use sooqa_persistence::{Database, hash_device_token};
use tower::util::ServiceExt;
use uuid::Uuid;

const CREATE_TOKEN: &str = "c2-create-token-with-enough-entropy";
const READ_TOKEN: &str = "c2-read-token-with-enough-entropy";

struct Fixture {
    database: Database,
    key_prefix: String,
    create_token: String,
    read_token: String,
}

impl Fixture {
    async fn create() -> Self {
        let database_url =
            env::var("DATABASE_URL").expect("DATABASE_URL must point to the integration database");
        let database =
            Database::connect(&database_url, 10).await.expect("database should be reachable");
        database.migrate().await.expect("migrations should succeed");

        let key_prefix = format!("c2-{}-", Uuid::new_v4());
        let name_prefix = format!("c2-test-{}", Uuid::new_v4());
        sqlx::query("DELETE FROM device_tokens WHERE token_prefix IN ('c2-cre', 'c2-rea')")
            .execute(database.pool())
            .await
            .expect("old device tokens should clean up");
        sqlx::query(
            r#"
            INSERT INTO device_tokens (name, token_prefix, token_hash, scopes)
            VALUES ($1, 'c2-cre', $2, $3)
            "#,
        )
        .bind(format!("{name_prefix}-create"))
        .bind(hash_device_token(CREATE_TOKEN))
        .bind(vec!["ingest:create".to_owned()])
        .execute(database.pool())
        .await
        .expect("create token should seed");
        sqlx::query(
            r#"
            INSERT INTO device_tokens (name, token_prefix, token_hash, scopes)
            VALUES ($1, 'c2-rea', $2, $3)
            "#,
        )
        .bind(format!("{name_prefix}-read"))
        .bind(hash_device_token(READ_TOKEN))
        .bind(vec!["ingest:read".to_owned()])
        .execute(database.pool())
        .await
        .expect("read token should seed");

        Self {
            database,
            key_prefix,
            create_token: CREATE_TOKEN.to_owned(),
            read_token: READ_TOKEN.to_owned(),
        }
    }

    async fn clean_up(&self) {
        sqlx::query(
            r#"
            DELETE FROM jobs
            WHERE payload_json->>'ingest_request_id' IN (
                SELECT id::text
                FROM ingest_requests
                WHERE idempotency_key LIKE $1
            )
            "#,
        )
        .bind(format!("{}%", self.key_prefix))
        .execute(self.database.pool())
        .await
        .expect("test jobs should clean up");
        sqlx::query(
            "DELETE FROM idempotency_records WHERE scope = 'ingest:create' AND idempotency_key LIKE $1",
        )
        .bind(format!("{}%", self.key_prefix))
        .execute(self.database.pool())
        .await
        .expect("test idempotency records should clean up");
        sqlx::query("DELETE FROM ingest_requests WHERE idempotency_key LIKE $1")
            .bind(format!("{}%", self.key_prefix))
            .execute(self.database.pool())
            .await
            .expect("test ingest requests should clean up");
        sqlx::query("DELETE FROM device_tokens WHERE token_prefix IN ('c2-cre', 'c2-rea')")
            .execute(self.database.pool())
            .await
            .expect("test device tokens should clean up");
    }
}

fn app_with_settings(fixture: &Fixture, settings: ApiSettings) -> axum::Router {
    router(
        settings,
        ApiState::new(
            fixture.database.inbox(),
            fixture.database.device_tokens(),
            fixture.database.library(),
        ),
    )
}

fn app(fixture: &Fixture) -> axum::Router {
    app_with_settings(fixture, ApiSettings::default())
}

fn post_request(token: Option<&str>, idempotency_key: Option<&str>, body: Value) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/api/v1/ingest-requests")
        .header("content-type", "application/json");
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    if let Some(idempotency_key) = idempotency_key {
        builder = builder.header("idempotency-key", idempotency_key);
    }
    builder.body(Body::from(body.to_string())).expect("request should build")
}

async fn response_json(response: axum::response::Response) -> Value {
    let body = to_bytes(response.into_body(), 64 * 1024).await.expect("body should be readable");
    serde_json::from_slice(&body).expect("response should contain JSON")
}

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance"]
async fn authenticated_ingest_api_creates_reads_and_replays_requests() {
    let fixture = Fixture::create().await;
    let key = format!("{}request", fixture.key_prefix);
    let body = json!({
        "url": "HTTPS://Example.COM:443/video?id=123&utm_source=test",
        "page_title": "A useful title",
        "selected_text": "Caption idea",
        "tags": ["Cats"]
    });

    let body_limit = app_with_settings(
        &fixture,
        ApiSettings { request_body_limit_bytes: 128, ..ApiSettings::default() },
    )
    .oneshot(post_request(
        Some(&fixture.create_token),
        Some(&format!("{}large", fixture.key_prefix)),
        json!({"url": "https://example.com", "selected_text": "x".repeat(512)}),
    ))
    .await
    .expect("router should respond");
    assert_eq!(body_limit.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let unauthorized = app(&fixture)
        .oneshot(post_request(None, Some(&key), body.clone()))
        .await
        .expect("router should respond");
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(response_json(unauthorized).await["error"]["code"], "authorization_required");

    let created = app(&fixture)
        .oneshot(post_request(Some(&fixture.create_token), Some(&key), body.clone()))
        .await
        .expect("router should respond");
    assert_eq!(created.status(), StatusCode::ACCEPTED);
    let created_body = response_json(created).await;
    let request_id = created_body["id"].as_str().expect("response should contain an ID").to_owned();
    assert_eq!(created_body["status"], "queued");
    assert_eq!(created_body["links"]["self"], format!("/api/v1/ingest-requests/{request_id}"));

    let insufficient_scope = app(&fixture)
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/api/v1/ingest-requests/{request_id}"))
                .header("authorization", format!("Bearer {}", fixture.create_token))
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");
    assert_eq!(insufficient_scope.status(), StatusCode::FORBIDDEN);
    assert_eq!(response_json(insufficient_scope).await["error"]["code"], "insufficient_scope");

    let fetched = app(&fixture)
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/api/v1/ingest-requests/{request_id}"))
                .header("authorization", format!("Bearer {}", fixture.read_token))
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");
    assert_eq!(fetched.status(), StatusCode::OK);
    let fetched_body = response_json(fetched).await;
    assert_eq!(fetched_body["id"], request_id);
    assert_eq!(fetched_body["source_url"], "https://example.com/video?id=123");
    assert_eq!(fetched_body["status"], "queued");

    let replayed = app(&fixture)
        .oneshot(post_request(Some(&fixture.create_token), Some(&key), body.clone()))
        .await
        .expect("router should respond");
    assert_eq!(replayed.status(), StatusCode::ACCEPTED);
    assert_eq!(response_json(replayed).await["id"], request_id);

    let conflict = app(&fixture)
        .oneshot(post_request(
            Some(&fixture.create_token),
            Some(&key),
            json!({"url": "https://example.com/different"}),
        ))
        .await
        .expect("router should respond");
    assert_eq!(conflict.status(), StatusCode::CONFLICT);
    assert_eq!(response_json(conflict).await["error"]["code"], "idempotency_conflict");

    let invalid_url = app(&fixture)
        .oneshot(post_request(
            Some(&fixture.create_token),
            Some(&format!("{}invalid", fixture.key_prefix)),
            json!({"url": "ftp://example.com/file"}),
        ))
        .await
        .expect("router should respond");
    assert_eq!(invalid_url.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response_json(invalid_url).await["error"]["code"], "unsupported_scheme");

    fixture.clean_up().await;
}
