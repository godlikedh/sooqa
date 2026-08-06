//! Durable job worker entry point for sooqa.

use std::error::Error;

use sooqa_config::{AppConfig, AppRole, CliOptions};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("sooqa-worker: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let options = CliOptions::parse(std::env::args().skip(1))?;
    let config = AppConfig::load(AppRole::Worker, options.config_path.as_deref())?;

    if options.check_config {
        println!("{}", config.summary());
        return Ok(());
    }

    sooqa_runtime::init_tracing(&config.observability)?;
    tracing::info!(role = %config.role, "sooqa worker started");
    sooqa_runtime::shutdown_signal().await;
    tracing::info!(role = %config.role, "sooqa worker stopped");

    Ok(())
}
