//! HTTP server entry point for sooqa.

use std::error::Error;

use axum::Router;
use sooqa_config::{AppConfig, AppRole, CliCommand, CliOptions, ConfigError};
use sooqa_persistence::{TelegramRepository, TelegramRepositoryError};
use sooqa_telegram::{TelegramRuntime, UpdateClaim, UpdateStore};
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

    if options.command == Some(CliCommand::Migrate) {
        let database_url = config
            .secrets
            .database_url
            .as_ref()
            .ok_or(ConfigError::MissingSecret("database URL"))?;
        let database = sooqa_persistence::Database::connect_secret(
            database_url,
            config.database.max_connections,
        )
        .await?;
        database.migrate().await?;
        println!("sooqa-server: database migrations applied");
        return Ok(());
    }

    sooqa_runtime::init_tracing(&config.observability)?;
    let database_url =
        config.secrets.database_url.as_ref().ok_or(ConfigError::MissingSecret("database URL"))?;
    let database =
        sooqa_persistence::Database::connect_secret(database_url, config.database.max_connections)
            .await?;
    let listener = TcpListener::bind(&config.server.listen_address).await?;
    let api_settings = sooqa_api::ApiSettings {
        request_body_limit_bytes: config.server.request_body_limit_bytes,
        request_timeout_seconds: config.server.request_timeout_seconds,
    };
    let app: Router = sooqa_api::router(
        api_settings,
        sooqa_api::ApiState::new(database.inbox(), database.device_tokens(), database.library()),
    );

    tracing::info!(role = %config.role, "sooqa server started");
    let server = sooqa_server::serve(listener, app, sooqa_runtime::shutdown_signal());
    if let Some(token) =
        config.secrets.telegram_bot_token.as_ref().filter(|token| token.is_configured())
    {
        let telegram = TelegramRuntime::new(
            token.expose_secret(),
            &config.telegram.api_base_url,
            std::time::Duration::from_secs(config.telegram.poll_timeout_seconds),
            DatabaseUpdateStore { repository: database.telegram() },
            config.telegram.admin_user_ids.clone(),
        )?;
        tracing::info!(api_base_url = %config.telegram.api_base_url, "Telegram bot polling enabled");
        tokio::select! {
            result = server => result?,
            result = telegram.run() => result?,
        }
    } else {
        server.await?;
    }
    tracing::info!(role = %config.role, "sooqa server stopped");

    Ok(())
}

#[derive(Clone)]
struct DatabaseUpdateStore {
    repository: TelegramRepository,
}

#[async_trait::async_trait]
impl UpdateStore for DatabaseUpdateStore {
    type Error = TelegramRepositoryError;

    async fn claim_update(&self, update_id: i64) -> Result<Option<UpdateClaim>, Self::Error> {
        self.repository.claim_update(update_id).await.map(|claim| {
            claim.map(|claim| UpdateClaim {
                update_id: claim.update_id,
                claim_token: claim.claim_token,
            })
        })
    }

    async fn complete_update(&self, claim: UpdateClaim) -> Result<(), Self::Error> {
        self.repository
            .complete_update(sooqa_persistence::TelegramUpdateClaim {
                update_id: claim.update_id,
                claim_token: claim.claim_token,
            })
            .await
    }

    async fn release_update(&self, claim: UpdateClaim) -> Result<(), Self::Error> {
        self.repository
            .release_update(sooqa_persistence::TelegramUpdateClaim {
                update_id: claim.update_id,
                claim_token: claim.claim_token,
            })
            .await
    }
}
