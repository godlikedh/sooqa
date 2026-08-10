//! Local capture companion entry point for sooqa.

use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use sooqa_companion::serve;
use sooqa_config::{AppConfig, AppRole, CliOptions};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("sooqa-companion: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let options = CliOptions::parse(std::env::args().skip(1))?;
    let config = AppConfig::load(AppRole::Companion, options.config_path.as_deref())?;

    if options.check_config {
        println!("{}", config.summary());
        return Ok(());
    }

    sooqa_runtime::init_tracing(&config.observability)?;
    tracing::info!(
        role = %config.role,
        listen_address = %config.companion.listen_address,
        "sooqa companion started"
    );

    let stop = Arc::new(AtomicBool::new(false));
    let server_stop = Arc::clone(&stop);
    let companion_config = config.companion.clone();
    let mut server = tokio::task::spawn_blocking(move || serve(&companion_config, &server_stop));
    tokio::select! {
        result = &mut server => result??,
        _ = sooqa_runtime::shutdown_signal() => {
            stop.store(true, Ordering::Release);
            server.await??;
        }
    }
    tracing::info!(role = %config.role, "sooqa companion stopped");

    Ok(())
}
