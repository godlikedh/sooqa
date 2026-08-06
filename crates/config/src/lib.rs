//! Typed configuration loading and redacted configuration summaries for sooqa.

use std::{
    env, fmt, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    str::FromStr,
};

use serde::Deserialize;
use thiserror::Error;

const DEFAULT_SERVER_LISTEN_ADDRESS: &str = "0.0.0.0:8080";
const DEFAULT_WORKER_POLL_INTERVAL_SECONDS: u64 = 5;
const DEFAULT_COMPANION_LISTEN_ADDRESS: &str = "127.0.0.1:47831";
const DEFAULT_LOG_FORMAT: &str = "json";
const DEFAULT_LOG_LEVEL: &str = "info";

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum AppRole {
    Server,
    Worker,
    Companion,
}

impl fmt::Display for AppRole {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Server => "server",
            Self::Worker => "worker",
            Self::Companion => "companion",
        };
        formatter.write_str(value)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CliOptions {
    pub check_config: bool,
    pub config_path: Option<PathBuf>,
}

impl CliOptions {
    pub fn parse<I, S>(arguments: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut check_config = false;
        let mut config_path = None;
        let mut arguments = arguments.into_iter();

        while let Some(argument) = arguments.next() {
            let argument = argument.into();
            match argument.as_str() {
                "--check-config" => check_config = true,
                "--config" => {
                    let path =
                        arguments.next().ok_or(ConfigError::MissingArgument("--config"))?.into();
                    config_path = Some(PathBuf::from(path));
                }
                "--help" | "-h" => return Err(ConfigError::HelpRequested),
                unknown => return Err(ConfigError::UnknownArgument(unknown.to_owned())),
            }
        }

        Ok(Self { check_config, config_path })
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct SecretString(String);

impl SecretString {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn is_configured(&self) -> bool {
        !self.0.is_empty()
    }

    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("SecretString").field(&"[REDACTED]").finish()
    }
}

impl fmt::Display for SecretString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum LogFormat {
    Json,
    Pretty,
}

impl fmt::Display for LogFormat {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Json => "json",
            Self::Pretty => "pretty",
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ServerConfig {
    pub listen_address: String,
    pub request_body_limit_bytes: usize,
    pub request_timeout_seconds: u64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkerConfig {
    pub poll_interval_seconds: u64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CompanionConfig {
    pub listen_address: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ObservabilityConfig {
    pub log_format: LogFormat,
    pub log_level: String,
}

#[derive(Clone, Eq, PartialEq)]
pub struct SecretConfig {
    pub database_url: Option<SecretString>,
    pub telegram_bot_token: Option<SecretString>,
}

impl fmt::Debug for SecretConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecretConfig")
            .field("database_url", &self.database_url)
            .field("telegram_bot_token", &self.telegram_bot_token)
            .finish()
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AppConfig {
    pub role: AppRole,
    pub config_path: Option<PathBuf>,
    pub server: ServerConfig,
    pub worker: WorkerConfig,
    pub companion: CompanionConfig,
    pub observability: ObservabilityConfig,
    pub secrets: SecretConfig,
}

impl AppConfig {
    pub fn load(role: AppRole, cli_path: Option<&Path>) -> Result<Self, ConfigError> {
        let config_path = match cli_path {
            Some(path) => Some(path.to_path_buf()),
            None => optional_env_path("SOOQA_CONFIG_FILE")?,
        };
        let contents = match config_path.as_deref() {
            Some(path) => fs::read_to_string(path)
                .map_err(|source| ConfigError::ReadFile { path: path.to_path_buf(), source })?,
            None => String::new(),
        };
        let mut config = Self::from_toml_str(role, config_path, &contents)?;
        config.apply_environment()?;
        config.validate()?;
        Ok(config)
    }

    pub fn from_toml_str(
        role: AppRole,
        config_path: Option<PathBuf>,
        contents: &str,
    ) -> Result<Self, ConfigError> {
        let raw: RawConfig = toml::from_str(contents)?;
        let log_format = parse_log_format(
            raw.observability.log_format.as_deref().unwrap_or(DEFAULT_LOG_FORMAT),
        )?;
        let log_level = normalize_log_level(
            raw.observability.log_level.as_deref().unwrap_or(DEFAULT_LOG_LEVEL),
        )?;

        Ok(Self {
            role,
            config_path,
            server: ServerConfig {
                listen_address: raw
                    .server
                    .listen_address
                    .unwrap_or_else(|| DEFAULT_SERVER_LISTEN_ADDRESS.to_owned()),
                request_body_limit_bytes: raw.server.request_body_limit_bytes.unwrap_or(1_048_576),
                request_timeout_seconds: raw.server.request_timeout_seconds.unwrap_or(30),
            },
            worker: WorkerConfig {
                poll_interval_seconds: raw
                    .worker
                    .poll_interval_seconds
                    .unwrap_or(DEFAULT_WORKER_POLL_INTERVAL_SECONDS),
            },
            companion: CompanionConfig {
                listen_address: raw
                    .companion
                    .listen_address
                    .unwrap_or_else(|| DEFAULT_COMPANION_LISTEN_ADDRESS.to_owned()),
            },
            observability: ObservabilityConfig { log_format, log_level },
            secrets: SecretConfig {
                database_url: raw.secrets.database_url.map(SecretString::new),
                telegram_bot_token: raw.secrets.telegram_bot_token.map(SecretString::new),
            },
        })
    }

    pub fn summary(&self) -> String {
        format!(
            "role={} config_file={} server.listen_address={} worker.poll_interval_seconds={} companion.listen_address={} observability.log_format={} observability.log_level={} secret.database_url={} secret.telegram_bot_token={}",
            self.role,
            self.config_path
                .as_deref()
                .map_or_else(|| "<defaults>".to_owned(), |path| path.display().to_string()),
            self.server.listen_address,
            self.worker.poll_interval_seconds,
            self.companion.listen_address,
            self.observability.log_format,
            self.observability.log_level,
            configured_state(self.secrets.database_url.as_ref()),
            configured_state(self.secrets.telegram_bot_token.as_ref()),
        )
    }

    fn apply_environment(&mut self) -> Result<(), ConfigError> {
        if let Some(value) = optional_env_string("SOOQA_SERVER_LISTEN_ADDRESS")? {
            self.server.listen_address = value;
        }
        if let Some(value) = optional_env_string("SOOQA_WORKER_POLL_INTERVAL_SECONDS")? {
            self.worker.poll_interval_seconds =
                value.parse().map_err(|_| ConfigError::InvalidValue {
                    name: "SOOQA_WORKER_POLL_INTERVAL_SECONDS",
                    reason: "expected a positive integer",
                })?;
        }
        if let Some(value) = optional_env_string("SOOQA_COMPANION_LISTEN_ADDRESS")? {
            self.companion.listen_address = value;
        }
        if let Some(value) = optional_env_string("SOOQA_OBSERVABILITY_LOG_FORMAT")? {
            self.observability.log_format = parse_log_format(&value)?;
        }
        if let Some(value) = optional_env_string("SOOQA_OBSERVABILITY_LOG_LEVEL")? {
            self.observability.log_level = normalize_log_level(&value)?;
        }
        if let Some(value) = optional_env_string("SOOQA_DATABASE_URL")? {
            self.secrets.database_url = Some(SecretString::new(value));
        }
        if let Some(value) = optional_env_string("SOOQA_TELEGRAM_BOT_TOKEN")? {
            self.secrets.telegram_bot_token = Some(SecretString::new(value));
        }
        Ok(())
    }

    fn validate(&self) -> Result<(), ConfigError> {
        parse_socket_address("server.listen_address", &self.server.listen_address)?;
        let companion_address =
            parse_socket_address("companion.listen_address", &self.companion.listen_address)?;
        if !companion_address.ip().is_loopback() {
            return Err(ConfigError::InvalidValue {
                name: "companion.listen_address",
                reason: "must use a loopback address",
            });
        }
        if self.worker.poll_interval_seconds == 0 {
            return Err(ConfigError::InvalidValue {
                name: "worker.poll_interval_seconds",
                reason: "must be greater than zero",
            });
        }
        if self.server.request_body_limit_bytes == 0 {
            return Err(ConfigError::InvalidValue {
                name: "server.request_body_limit_bytes",
                reason: "must be greater than zero",
            });
        }
        if self.server.request_timeout_seconds == 0 {
            return Err(ConfigError::InvalidValue {
                name: "server.request_timeout_seconds",
                reason: "must be greater than zero",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct RawConfig {
    server: RawServerConfig,
    worker: RawWorkerConfig,
    companion: RawCompanionConfig,
    observability: RawObservabilityConfig,
    secrets: RawSecretConfig,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct RawServerConfig {
    listen_address: Option<String>,
    request_body_limit_bytes: Option<usize>,
    request_timeout_seconds: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct RawWorkerConfig {
    poll_interval_seconds: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct RawCompanionConfig {
    listen_address: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct RawObservabilityConfig {
    log_format: Option<String>,
    log_level: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct RawSecretConfig {
    database_url: Option<String>,
    telegram_bot_token: Option<String>,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("could not read config file {path}: {source}")]
    ReadFile { path: PathBuf, source: std::io::Error },
    #[error("could not parse TOML config: {0}")]
    ParseToml(#[from] toml::de::Error),
    #[error("environment variable {name} contains invalid UTF-8")]
    InvalidEnvironmentEncoding { name: &'static str },
    #[error("invalid value for {name}: {reason}")]
    InvalidValue { name: &'static str, reason: &'static str },
    #[error("missing argument after {0}")]
    MissingArgument(&'static str),
    #[error("unknown argument: {0}")]
    UnknownArgument(String),
    #[error("help requested")]
    HelpRequested,
}

fn optional_env_path(name: &'static str) -> Result<Option<PathBuf>, ConfigError> {
    Ok(optional_env_string(name)?.map(PathBuf::from))
}

fn optional_env_string(name: &'static str) -> Result<Option<String>, ConfigError> {
    match env::var_os(name) {
        Some(value) => value
            .into_string()
            .map(Some)
            .map_err(|_| ConfigError::InvalidEnvironmentEncoding { name }),
        None => Ok(None),
    }
}

fn parse_socket_address(name: &'static str, value: &str) -> Result<SocketAddr, ConfigError> {
    SocketAddr::from_str(value).map_err(|_| ConfigError::InvalidValue {
        name,
        reason: "expected an IP address and port such as 127.0.0.1:8080",
    })
}

fn parse_log_format(value: &str) -> Result<LogFormat, ConfigError> {
    match value.to_ascii_lowercase().as_str() {
        "json" => Ok(LogFormat::Json),
        "pretty" => Ok(LogFormat::Pretty),
        _ => Err(ConfigError::InvalidValue {
            name: "observability.log_format",
            reason: "expected json or pretty",
        }),
    }
}

fn normalize_log_level(value: &str) -> Result<String, ConfigError> {
    let normalized = value.to_ascii_lowercase();
    match normalized.as_str() {
        "trace" | "debug" | "info" | "warn" | "error" => Ok(normalized),
        _ => Err(ConfigError::InvalidValue {
            name: "observability.log_level",
            reason: "expected trace, debug, info, warn, or error",
        }),
    }
}

fn configured_state(secret: Option<&SecretString>) -> &'static str {
    match secret {
        Some(value) if value.is_configured() => "configured",
        _ => "absent",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid_for_each_role() {
        for role in [AppRole::Server, AppRole::Worker, AppRole::Companion] {
            let config =
                AppConfig::from_toml_str(role, None, "").expect("defaults should parse").validate();
            assert!(config.is_ok());
        }
    }

    #[test]
    fn companion_must_bind_to_loopback() {
        let config = AppConfig::from_toml_str(
            AppRole::Companion,
            None,
            "[companion]\nlisten_address = \"0.0.0.0:47831\"\n",
        )
        .expect("TOML should parse");

        let error = config.validate().expect_err("non-loopback companion must fail");
        assert!(error.to_string().contains("loopback"));
    }

    #[test]
    fn worker_poll_interval_must_be_positive() {
        let config = AppConfig::from_toml_str(
            AppRole::Worker,
            None,
            "[worker]\npoll_interval_seconds = 0\n",
        )
        .expect("TOML should parse");

        let error = config.validate().expect_err("zero interval must fail");
        assert!(error.to_string().contains("greater than zero"));
    }

    #[test]
    fn invalid_log_level_is_rejected() {
        let error = AppConfig::from_toml_str(
            AppRole::Server,
            None,
            "[observability]\nlog_level = \"verbose\"\n",
        )
        .expect_err("invalid log level must fail");

        assert!(error.to_string().contains("expected trace"));
    }

    #[test]
    fn secret_display_and_debug_are_redacted() {
        let secret = SecretString::new("top-secret");
        assert_eq!(secret.to_string(), "[REDACTED]");
        assert!(!format!("{secret:?}").contains("top-secret"));
        assert_eq!(secret.expose_secret(), "top-secret");
    }

    #[test]
    fn command_line_options_parse_config_and_check_flags() {
        let options = CliOptions::parse(["--config", "config.toml", "--check-config"])
            .expect("arguments should parse");

        assert!(options.check_config);
        assert_eq!(options.config_path, Some(PathBuf::from("config.toml")));
    }
}
