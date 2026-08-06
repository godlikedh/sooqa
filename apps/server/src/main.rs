//! HTTP server entry point for sooqa.

use std::error::Error;

use axum::Router;
use sooqa_config::{AppConfig, AppRole, CliOptions};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("sooqa-server: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let options = CliOptions::parse(std::env::args().skip(1))?;
    let config = AppConfig::load(AppRole::Server, options.config_path.as_deref())?;

    if options.check_config {
        println!("{}", config.summary());
        return Ok(());
    }

    sooqa_runtime::init_tracing(&config.observability)?;
    let listener = TcpListener::bind(&config.server.listen_address).await?;
    let api_settings = sooqa_api::ApiSettings {
        request_body_limit_bytes: config.server.request_body_limit_bytes,
        request_timeout_seconds: config.server.request_timeout_seconds,
    };
    let app: Router = sooqa_api::router(api_settings);

    tracing::info!(role = %config.role, "sooqa server started");
    sooqa_server::serve(listener, app, sooqa_runtime::shutdown_signal()).await?;
    tracing::info!(role = %config.role, "sooqa server stopped");

    Ok(())
}
