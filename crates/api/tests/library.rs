use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use sooqa_api::{ApiSettings, ApiState, router};
use sooqa_persistence::Database;
use tower::ServiceExt;
use uuid::Uuid;

fn app(pool: sqlx::PgPool) -> (Database, axum::Router) {
    let database = Database::from_pool(pool);
    let app = router(
        ApiSettings::default(),
        ApiState::new(database.inbox(), "test-api-token", database.library(), database.publisher()),
    );
    (database, app)
}

async fn body(response: axum::response::Response) -> Value {
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn media_api_uses_complete_metadata_updates_and_bounded_search(pool: sqlx::PgPool) {
    let (database, app) = app(pool);
    let media_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO media (id, kind, description, tags, canonical_sha256) VALUES ($1, 'image', $2, $3, $4)",
    )
    .bind(media_id)
    .bind("before")
    .bind(vec!["old"])
    .bind(vec![11_u8; 32])
    .execute(database.pool())
    .await
    .unwrap();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/api/v1/media/{media_id}"))
                .header("authorization", "Bearer test-api-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let current = body(response).await;
    assert_eq!(current["tags"], json!(["old"]));
    assert!(current.get("status").is_none());
    assert!(current.get("notes").is_none());
    let expected_updated_at = current["updated_at"].as_str().unwrap().to_owned();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/api/v1/media/{media_id}"))
                .header("authorization", "Bearer test-api-token")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "description": "after",
                        "tags": [" Rust ", "reaction", "rust"],
                        "expected_updated_at": expected_updated_at,
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let updated = body(response).await;
    assert_eq!(updated["description"], "after");
    assert_eq!(updated["tags"], json!(["rust", "reaction"]));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/api/v1/media/{media_id}"))
                .header("authorization", "Bearer test-api-token")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "description": null,
                        "tags": [],
                        "expected_updated_at": expected_updated_at,
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/media?limit=20")
                .header("authorization", "Bearer test-api-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let page = body(response).await;
    assert_eq!(page["items"][0]["id"], media_id.to_string());

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/media?q=old")
                .header("authorization", "Bearer test-api-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
