//! Server process helpers for sooqa.

use std::future::Future;

use axum::Router;
use tokio::net::TcpListener;

pub async fn serve<F>(listener: TcpListener, app: Router, shutdown: F) -> Result<(), std::io::Error>
where
    F: Future<Output = ()> + Send + 'static,
{
    axum::serve(listener, app).with_graceful_shutdown(shutdown).await.map_err(std::io::Error::other)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::Router;
    use tokio::{net::TcpListener, sync::oneshot, time::timeout};

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
