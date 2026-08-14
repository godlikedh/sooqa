//! Server process helpers for sooqa.

use std::future::Future;

use axum::{Router, body::Body, http::header, response::Response, routing::get};
use tokio::net::TcpListener;

const ADMIN_CSP: &str = "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' blob:; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'";

pub fn admin_router() -> Router {
    Router::new()
        .route("/admin", get(admin_index))
        .route("/admin/", get(admin_index))
        .route("/admin/assets/app.js", get(admin_script))
        .route("/admin/assets/styles.css", get(admin_styles))
}

async fn admin_index() -> Response {
    admin_asset(include_str!("../assets/admin/index.html"), "text/html; charset=utf-8")
}

async fn admin_script() -> Response {
    admin_asset(include_str!("../assets/admin/app.js"), "text/javascript; charset=utf-8")
}

async fn admin_styles() -> Response {
    admin_asset(include_str!("../assets/admin/styles.css"), "text/css; charset=utf-8")
}

fn admin_asset(body: &'static str, content_type: &'static str) -> Response {
    Response::builder()
        .status(200)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CACHE_CONTROL, "no-store")
        .header(header::CONTENT_SECURITY_POLICY, ADMIN_CSP)
        .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff")
        .header(header::REFERRER_POLICY, "no-referrer")
        .body(Body::from(body))
        .expect("static admin asset response should be valid")
}

pub async fn serve<F>(listener: TcpListener, app: Router, shutdown: F) -> Result<(), std::io::Error>
where
    F: Future<Output = ()> + Send + 'static,
{
    axum::serve(listener, app).with_graceful_shutdown(shutdown).await.map_err(std::io::Error::other)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::{Router, body::Body, http::Request};
    use tokio::{net::TcpListener, sync::oneshot, time::timeout};
    use tower::ServiceExt;

    #[tokio::test]
    async fn admin_assets_are_local_and_security_headers_are_present() {
        let response = super::admin_router()
            .oneshot(Request::builder().uri("/admin").body(Body::empty()).unwrap())
            .await
            .expect("admin route should respond");
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()["content-type"], "text/html; charset=utf-8");
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert!(response.headers().contains_key("content-security-policy"));
        let body = axum::body::to_bytes(response.into_body(), 256 * 1024)
            .await
            .expect("admin HTML should be bounded");
        let body = String::from_utf8(body.to_vec()).expect("admin HTML should be UTF-8");
        assert!(body.contains("/admin/assets/app.js"));

        let response = super::admin_router()
            .oneshot(Request::builder().uri("/admin/assets/app.js").body(Body::empty()).unwrap())
            .await
            .expect("admin script route should respond");
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()["content-type"], "text/javascript; charset=utf-8");
        let body = axum::body::to_bytes(response.into_body(), 512 * 1024)
            .await
            .expect("admin script should be bounded");
        let body = String::from_utf8(body.to_vec()).expect("admin script should be UTF-8");
        assert!(body.contains("sessionStorage"));
    }

    #[tokio::test]
    async fn server_stops_after_shutdown_signal() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("test listener should bind");
        let (sender, receiver) = oneshot::channel();
        let task = tokio::spawn(super::serve(listener, Router::new(), async move {
            let _ = receiver.await;
        }));

        sender.send(()).expect("shutdown receiver should be alive");
        let result = timeout(Duration::from_secs(1), task)
            .await
            .expect("server should stop promptly")
            .expect("server task should not panic");

        assert!(result.is_ok());
    }
}
