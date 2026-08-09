use std::env;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::json;
use sooqa_api::{ApiSettings, ApiState, router};
use sooqa_persistence::Database;
use tower::ServiceExt;

async fn app() -> (Database, axum::Router) {
    let url = env::var("DATABASE_URL").expect("DATABASE_URL must point to PostgreSQL");
    let database = Database::connect(&url, 10).await.unwrap();
    database.migrate().await.unwrap();
    let app = router(
        ApiSettings::default(),
        ApiState::new(database.inbox(), "test-api-token", database.library(), database.publisher()),
    );
    (database, app)
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn api_authenticates_with_the_single_configured_bearer_secret() {
    let (_database, app) = app().await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/ingests")
                .header("content-type", "application/json")
                .body(Body::from(json!({"url": "https://example.test"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/ingests")
                .header("content-type", "application/json")
                .header("authorization", "Bearer test-api-token")
                .header("idempotency-key", "api-test-key")
                .body(Body::from(json!({"url": "https://example.test"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
}
