//! Shared application runtime plumbing for sooqa binaries.

use std::error::Error;

use sooqa_config::{LogFormat, ObservabilityConfig};
use tracing_subscriber::filter::LevelFilter;

pub fn init_tracing(config: &ObservabilityConfig) -> Result<(), TracingError> {
    let level = config
        .log_level
        .parse::<LevelFilter>()
        .map_err(|_| TracingError::InvalidLevel(config.log_level.clone()))?;

    match &config.log_format {
        LogFormat::Json => tracing_subscriber::fmt()
            .json()
            .with_max_level(level)
            .try_init()
            .map_err(|_| TracingError::AlreadyInitialized),
        LogFormat::Pretty => tracing_subscriber::fmt()
            .with_max_level(level)
            .try_init()
            .map_err(|_| TracingError::AlreadyInitialized),
    }
}

pub async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = match signal(SignalKind::terminate()) {
            Ok(signal) => signal,
            Err(error) => {
                eprintln!("could not install SIGTERM handler: {error}");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };

        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if let Err(error) = result {
                    eprintln!("could not listen for Ctrl-C: {error}");
                }
            }
            _ = terminate.recv() => {}
        }
    }

    #[cfg(not(unix))]
    if let Err(error) = tokio::signal::ctrl_c().await {
        eprintln!("could not listen for Ctrl-C: {error}");
    }
}

#[derive(Debug)]
pub enum TracingError {
    AlreadyInitialized,
    InvalidLevel(String),
}

impl std::fmt::Display for TracingError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyInitialized => {
                formatter.write_str("tracing subscriber is already initialized")
            }
            Self::InvalidLevel(level) => write!(formatter, "invalid log level: {level}"),
        }
    }
}

impl Error for TracingError {}
