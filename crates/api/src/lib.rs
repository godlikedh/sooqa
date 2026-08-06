//! HTTP API boundary for sooqa.

use std::time::Duration;

use axum::{Json, Router, http::StatusCode, response::IntoResponse, routing::get};
use serde::Serialize;
use tower_http::{
    limit::RequestBodyLimitLayer,
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    timeout::TimeoutLayer,
    trace::TraceLayer,
};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ApiSettings {
    pub request_body_limit_bytes: usize,
    pub request_timeout_seconds: u64,
}

impl Default for ApiSettings {
    fn default() -> Self {
        Self { request_body_limit_bytes: 1_048_576, request_timeout_seconds: 30 }
    }
}

pub fn router(settings: ApiSettings) -> Router {
    Router::new()
        .route("/health/live", get(health_live))
        .layer(PropagateRequestIdLayer::x_request_id())
        .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
        .layer(TraceLayer::new_for_http())
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(settings.request_timeout_seconds),
        ))
        .layer(RequestBodyLimitLayer::new(settings.request_body_limit_bytes))
}

async fn health_live() -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(HealthResponse {
            status: "ok",
            service: "sooqa-server",
            build: BuildMetadata::current(),
        }),
    )
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    service: &'static str,
    build: BuildMetadata,
}

#[derive(Debug, Serialize)]
struct BuildMetadata {
    version: &'static str,
    git_sha: &'static str,
}

impl BuildMetadata {
    fn current() -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION"),
            git_sha: option_env!("SOOQA_BUILD_GIT_SHA").unwrap_or("unknown"),
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::util::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn liveness_returns_build_metadata_and_request_id() {
        let response = router(ApiSettings::default())
            .oneshot(
                Request::builder()
                    .uri("/health/live")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().contains_key("x-request-id"));

        let body =
            to_bytes(response.into_body(), 16 * 1024).await.expect("body should be readable");
        let body = String::from_utf8(body.to_vec()).expect("response should be UTF-8");
        assert!(body.contains("\"status\":\"ok\""));
        assert!(body.contains("\"version\":\"0.1.0\""));
        assert!(body.contains("\"git_sha\":\"unknown\""));
    }
}
