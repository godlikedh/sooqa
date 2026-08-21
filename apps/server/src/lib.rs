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
    use std::{
        path::Path,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use async_trait::async_trait;
    use axum::{Router, body::Body, http::Request, routing::get};
    use sooqa_telegram::{MemoryUpdateStore, TelegramApi, TelegramPollingApi, TelegramRuntime};
    use thiserror::Error;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::oneshot,
        time::timeout,
    };
    use tower::ServiceExt;

    #[derive(Debug, Error)]
    #[error("synthetic Telegram outage")]
    struct LivenessError;

    #[derive(Clone)]
    struct LivenessApi {
        failures_remaining: Arc<AtomicUsize>,
        successful_polls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl TelegramApi for LivenessApi {
        type Error = LivenessError;

        async fn send_text(&self, _chat_id: i64, _text: &str) -> Result<(), Self::Error> {
            Ok(())
        }

        async fn answer_callback_query(&self, _callback_id: &str) -> Result<(), Self::Error> {
            Ok(())
        }

        async fn download_file(
            &self,
            _file_id: &str,
            _destination: &Path,
        ) -> Result<(), Self::Error> {
            Ok(())
        }

        fn is_retryable_error(_error: &Self::Error) -> bool {
            true
        }
    }

    #[async_trait]
    impl TelegramPollingApi for LivenessApi {
        type PollingError = LivenessError;

        async fn verify_storage_chat(&self, _chat_id: i64) -> Result<(), Self::PollingError> {
            Ok(())
        }

        async fn delete_webhook(&self) -> Result<(), Self::PollingError> {
            Ok(())
        }

        async fn get_updates(
            &self,
            _offset: i32,
            _timeout_seconds: u32,
        ) -> Result<Vec<teloxide::types::Update>, Self::PollingError> {
            tokio::task::yield_now().await;
            let remaining = self.failures_remaining.load(Ordering::Relaxed);
            if remaining > 0 {
                self.failures_remaining.fetch_sub(1, Ordering::Relaxed);
                return Err(LivenessError);
            }
            self.successful_polls.fetch_add(1, Ordering::Relaxed);
            Ok(Vec::new())
        }

        fn is_terminal_error(_error: &Self::PollingError) -> bool {
            false
        }
    }

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

    #[tokio::test]
    async fn telegram_outage_does_not_drop_http_liveness_and_recovers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("test listener should bind");
        let address = listener.local_addr().expect("test listener address should be available");
        let (server_stop, server_shutdown) = oneshot::channel();
        let server_task = tokio::spawn(super::serve(
            listener,
            Router::new().route("/health/live", get(|| async { "ok" })),
            async move {
                let _ = server_shutdown.await;
            },
        ));

        let api = LivenessApi {
            failures_remaining: Arc::new(AtomicUsize::new(7)),
            successful_polls: Arc::new(AtomicUsize::new(0)),
        };
        let successful_polls = Arc::clone(&api.successful_polls);
        let runtime = TelegramRuntime::new_with_api(
            api,
            Duration::from_secs(1),
            MemoryUpdateStore::default(),
            [123],
            None,
            (),
        )
        .with_polling_backoff(Duration::from_millis(1), Duration::from_millis(4))
        .expect("test backoff should be valid");
        let (telegram_stop, telegram_shutdown) = oneshot::channel();
        let telegram_task = tokio::spawn(runtime.run_with_shutdown(async move {
            let _ = telegram_shutdown.await;
        }));

        timeout(Duration::from_secs(1), async {
            while successful_polls.load(Ordering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Telegram polling should recover without a process restart");

        let mut connection = TcpStream::connect(address)
            .await
            .expect("HTTP server should remain reachable during Telegram outage");
        connection
            .write_all(b"GET /health/live HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .expect("liveness probe should be written");
        let mut response = Vec::new();
        connection.read_to_end(&mut response).await.expect("liveness response should be readable");
        assert!(response.starts_with(b"HTTP/1.1 200"), "unexpected response: {response:?}");

        telegram_stop.send(()).expect("Telegram supervisor should still be running");
        server_stop.send(()).expect("HTTP server should still be running");
        assert!(telegram_task.await.expect("Telegram task should not panic").is_ok());
        assert!(server_task.await.expect("server task should not panic").is_ok());
    }
}
