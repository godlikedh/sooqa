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
const DEFAULT_WORKER_LEASE_DURATION_SECONDS: u64 = 60;
const DEFAULT_COMPANION_LISTEN_ADDRESS: &str = "127.0.0.1:47831";
const DEFAULT_FFMPEG_PATH: &str = "ffmpeg";
const DEFAULT_FFPROBE_PATH: &str = "ffprobe";
const DEFAULT_YTDLP_PATH: &str = "yt-dlp";
const DEFAULT_YTDLP_FORMAT: &str = "bestvideo*+bestaudio/best";
const DEFAULT_MEDIA_WORK_ROOT: &str = "/var/lib/sooqa/work";
const DEFAULT_TELEGRAM_API_BASE_URL: &str = "https://api.telegram.org";
const DEFAULT_TELEGRAM_POLL_TIMEOUT_SECONDS: u64 = 30;
const DEFAULT_TELEGRAM_MAX_DOWNLOAD_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const DEFAULT_LOG_FORMAT: &str = "json";
const DEFAULT_LOG_LEVEL: &str = "info";

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum AppRole {
    Server,
    Worker,
    Companion,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum CliCommand {
    Migrate,
    Storage(StorageCommand),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum StorageCommand {
    List,
    MarkUnknown {
        media_id: String,
        force: bool,
    },
    Reset {
        media_id: String,
    },
    Attach {
        media_id: String,
        generation: String,
        storage_chat_id: String,
        storage_message_id: String,
        telegram_file_id: String,
        telegram_file_unique_id: String,
    },
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
    pub command: Option<CliCommand>,
    pub config_path: Option<PathBuf>,
}

impl CliOptions {
    pub fn parse<I, S>(arguments: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut check_config = false;
        let mut command = None;
        let mut config_path = None;
        let mut arguments = arguments.into_iter().map(Into::into).collect::<Vec<_>>().into_iter();

        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--check-config" => check_config = true,
                "migrate" => command = Some(CliCommand::Migrate),
                "storage" => {
                    command = Some(CliCommand::Storage(parse_storage_command(&mut arguments)?));
                }
                "--config" => {
                    let path = arguments.next().ok_or(ConfigError::MissingArgument("--config"))?;
                    config_path = Some(PathBuf::from(path));
                }
                "--help" | "-h" => return Err(ConfigError::HelpRequested),
                unknown => return Err(ConfigError::UnknownArgument(unknown.to_owned())),
            }
        }

        Ok(Self { check_config, command, config_path })
    }
}

fn parse_storage_command(
    arguments: &mut impl Iterator<Item = String>,
) -> Result<StorageCommand, ConfigError> {
    let subcommand = arguments.next().ok_or(ConfigError::MissingArgument("storage command"))?;
    match subcommand.as_str() {
        "list" => Ok(StorageCommand::List),
        "mark-unknown" => {
            let media_id = next_storage_argument(arguments, "storage mark-unknown media-id")?;
            let force = match arguments.next() {
                None => false,
                Some(option) if option == "--force" => {
                    if arguments.next().as_deref() != Some("--confirm") {
                        return Err(ConfigError::MissingArgument("--confirm"));
                    }
                    true
                }
                Some(unknown) => return Err(ConfigError::UnknownArgument(unknown)),
            };
            Ok(StorageCommand::MarkUnknown { media_id, force })
        }
        "reset" => {
            let media_id = next_storage_argument(arguments, "storage reset media-id")?;
            match arguments.next().as_deref() {
                Some("--confirm") => Ok(StorageCommand::Reset { media_id }),
                Some(unknown) => Err(ConfigError::UnknownArgument(unknown.to_owned())),
                None => Err(ConfigError::MissingArgument("--confirm")),
            }
        }
        "attach" => Ok(StorageCommand::Attach {
            media_id: next_storage_argument(arguments, "storage attach media-id")?,
            generation: next_storage_argument(arguments, "storage attach generation")?,
            storage_chat_id: next_storage_argument(arguments, "storage attach chat-id")?,
            storage_message_id: next_storage_argument(arguments, "storage attach message-id")?,
            telegram_file_id: next_storage_argument(arguments, "storage attach file-id")?,
            telegram_file_unique_id: next_storage_argument(
                arguments,
                "storage attach file-unique-id",
            )?,
        }),
        unknown => Err(ConfigError::UnknownArgument(unknown.to_owned())),
    }
}

fn next_storage_argument(
    arguments: &mut impl Iterator<Item = String>,
    name: &'static str,
) -> Result<String, ConfigError> {
    arguments.next().ok_or(ConfigError::MissingArgument(name))
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
pub struct DatabaseConfig {
    pub url_env: String,
    pub max_connections: u32,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkerConfig {
    pub poll_interval_seconds: u64,
    pub lease_duration_seconds: u64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MediaConfig {
    pub work_root: PathBuf,
    pub ffmpeg_path: PathBuf,
    pub ffprobe_path: PathBuf,
    pub ytdlp_path: PathBuf,
    pub ytdlp_format: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CompanionConfig {
    pub listen_address: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TelegramConfig {
    pub api_base_url: String,
    pub admin_user_ids: Vec<i64>,
    pub poll_timeout_seconds: u64,
    pub max_download_bytes: u64,
    pub storage_chat_id: Option<i64>,
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
    pub api_token: Option<SecretString>,
}

impl fmt::Debug for SecretConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecretConfig")
            .field("database_url", &self.database_url)
            .field("telegram_bot_token", &self.telegram_bot_token)
            .field("api_token", &self.api_token)
            .finish()
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AppConfig {
    pub role: AppRole,
    pub config_path: Option<PathBuf>,
    pub server: ServerConfig,
    pub worker: WorkerConfig,
    pub media: MediaConfig,
    pub companion: CompanionConfig,
    pub database: DatabaseConfig,
    pub telegram: TelegramConfig,
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
                lease_duration_seconds: raw
                    .worker
                    .lease_duration_seconds
                    .unwrap_or(DEFAULT_WORKER_LEASE_DURATION_SECONDS),
            },
            media: MediaConfig {
                work_root: raw
                    .media
                    .work_root
                    .unwrap_or_else(|| DEFAULT_MEDIA_WORK_ROOT.to_owned())
                    .into(),
                ffmpeg_path: raw
                    .media
                    .ffmpeg_path
                    .unwrap_or_else(|| DEFAULT_FFMPEG_PATH.to_owned())
                    .into(),
                ffprobe_path: raw
                    .media
                    .ffprobe_path
                    .unwrap_or_else(|| DEFAULT_FFPROBE_PATH.to_owned())
                    .into(),
                ytdlp_path: raw
                    .media
                    .ytdlp_path
                    .unwrap_or_else(|| DEFAULT_YTDLP_PATH.to_owned())
                    .into(),
                ytdlp_format: raw
                    .media
                    .ytdlp_format
                    .unwrap_or_else(|| DEFAULT_YTDLP_FORMAT.to_owned()),
            },
            companion: CompanionConfig {
                listen_address: raw
                    .companion
                    .listen_address
                    .unwrap_or_else(|| DEFAULT_COMPANION_LISTEN_ADDRESS.to_owned()),
            },
            database: DatabaseConfig {
                url_env: raw.database.url_env.unwrap_or_else(|| "DATABASE_URL".to_owned()),
                max_connections: raw.database.max_connections.unwrap_or(20),
            },
            telegram: TelegramConfig {
                api_base_url: raw
                    .telegram
                    .api_base_url
                    .unwrap_or_else(|| DEFAULT_TELEGRAM_API_BASE_URL.to_owned()),
                admin_user_ids: raw.telegram.admin_user_ids,
                poll_timeout_seconds: raw
                    .telegram
                    .poll_timeout_seconds
                    .unwrap_or(DEFAULT_TELEGRAM_POLL_TIMEOUT_SECONDS),
                max_download_bytes: raw
                    .telegram
                    .max_download_bytes
                    .unwrap_or(DEFAULT_TELEGRAM_MAX_DOWNLOAD_BYTES),
                storage_chat_id: raw.telegram.storage_chat_id,
            },
            observability: ObservabilityConfig { log_format, log_level },
            secrets: SecretConfig {
                database_url: raw.secrets.database_url.map(SecretString::new),
                telegram_bot_token: raw.secrets.telegram_bot_token.map(SecretString::new),
                api_token: raw.secrets.api_token.map(SecretString::new),
            },
        })
    }

    pub fn summary(&self) -> String {
        format!(
            "role={} config_file={} server.listen_address={} worker.poll_interval_seconds={} worker.lease_duration_seconds={} media.work_root={} media.ffmpeg_path={} media.ffprobe_path={} media.ytdlp_path={} media.ytdlp_format={} companion.listen_address={} database.url_env={} database.max_connections={} telegram.api_base_url={} telegram.admin_user_ids={} telegram.poll_timeout_seconds={} telegram.max_download_bytes={} telegram.storage_chat_id={:?} observability.log_format={} observability.log_level={} secret.database_url={} secret.telegram_bot_token={} secret.api_token={}",
            self.role,
            self.config_path
                .as_deref()
                .map_or_else(|| "<defaults>".to_owned(), |path| path.display().to_string()),
            self.server.listen_address,
            self.worker.poll_interval_seconds,
            self.worker.lease_duration_seconds,
            self.media.work_root.display(),
            self.media.ffmpeg_path.display(),
            self.media.ffprobe_path.display(),
            self.media.ytdlp_path.display(),
            self.media.ytdlp_format,
            self.companion.listen_address,
            self.database.url_env,
            self.database.max_connections,
            self.telegram.api_base_url,
            self.telegram.admin_user_ids.len(),
            self.telegram.poll_timeout_seconds,
            self.telegram.max_download_bytes,
            self.telegram.storage_chat_id,
            self.observability.log_format,
            self.observability.log_level,
            configured_state(self.secrets.database_url.as_ref()),
            configured_state(self.secrets.telegram_bot_token.as_ref()),
            configured_state(self.secrets.api_token.as_ref()),
        )
    }

    fn apply_environment(&mut self) -> Result<(), ConfigError> {
        if let Some(value) = optional_env_string("SOOQA_SERVER_LISTEN_ADDRESS")? {
            self.server.listen_address = value;
        }
        if let Some(value) = optional_env_string("SOOQA_WORKER_POLL_INTERVAL_SECONDS")? {
            self.worker.poll_interval_seconds =
                value.parse().map_err(|_| ConfigError::InvalidValue {
                    name: "SOOQA_WORKER_POLL_INTERVAL_SECONDS".to_owned(),
                    reason: "expected a positive integer",
                })?;
        }
        if let Some(value) = optional_env_string("SOOQA_WORKER_LEASE_DURATION_SECONDS")? {
            self.worker.lease_duration_seconds =
                value.parse().map_err(|_| ConfigError::InvalidValue {
                    name: "SOOQA_WORKER_LEASE_DURATION_SECONDS".to_owned(),
                    reason: "expected a positive integer",
                })?;
        }
        if let Some(value) = optional_env_string("SOOQA_MEDIA_FFMPEG_PATH")? {
            self.media.ffmpeg_path = PathBuf::from(value);
        }
        if let Some(value) = optional_env_string("SOOQA_MEDIA_WORK_ROOT")? {
            self.media.work_root = PathBuf::from(value);
        }
        if let Some(value) = optional_env_string("SOOQA_MEDIA_FFPROBE_PATH")? {
            self.media.ffprobe_path = PathBuf::from(value);
        }
        if let Some(value) = optional_env_string("SOOQA_MEDIA_YTDLP_PATH")? {
            self.media.ytdlp_path = PathBuf::from(value);
        }
        if let Some(value) = optional_env_string("SOOQA_MEDIA_YTDLP_FORMAT")? {
            self.media.ytdlp_format = value;
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
        } else if let Some(value) = optional_env_string(&self.database.url_env)? {
            self.secrets.database_url = Some(SecretString::new(value));
        }
        if let Some(value) = optional_env_string("SOOQA_DATABASE_MAX_CONNECTIONS")? {
            self.database.max_connections =
                value.parse().map_err(|_| ConfigError::InvalidValue {
                    name: "SOOQA_DATABASE_MAX_CONNECTIONS".to_owned(),
                    reason: "expected a positive integer",
                })?;
        }
        if let Some(value) = optional_env_string("SOOQA_TELEGRAM_BOT_TOKEN")? {
            self.secrets.telegram_bot_token = Some(SecretString::new(value));
        }
        if let Some(value) = optional_env_string("SOOQA_API_TOKEN")? {
            self.secrets.api_token = Some(SecretString::new(value));
        }
        if let Some(value) = optional_env_string("SOOQA_TELEGRAM_API_BASE_URL")? {
            self.telegram.api_base_url = value;
        }
        if let Some(value) = optional_env_string("SOOQA_TELEGRAM_ADMIN_USER_IDS")? {
            self.telegram.admin_user_ids =
                parse_admin_user_ids("SOOQA_TELEGRAM_ADMIN_USER_IDS", &value)?;
        }
        if let Some(value) = optional_env_string("SOOQA_TELEGRAM_POLL_TIMEOUT_SECONDS")? {
            self.telegram.poll_timeout_seconds =
                value.parse().map_err(|_| ConfigError::InvalidValue {
                    name: "SOOQA_TELEGRAM_POLL_TIMEOUT_SECONDS".to_owned(),
                    reason: "expected a positive integer",
                })?;
        }
        if let Some(value) = optional_env_string("SOOQA_TELEGRAM_MAX_DOWNLOAD_BYTES")? {
            self.telegram.max_download_bytes =
                value.parse().map_err(|_| ConfigError::InvalidValue {
                    name: "SOOQA_TELEGRAM_MAX_DOWNLOAD_BYTES".to_owned(),
                    reason: "expected a positive integer",
                })?;
        }
        if let Some(value) = optional_env_string("SOOQA_TELEGRAM_STORAGE_CHAT_ID")? {
            self.telegram.storage_chat_id =
                Some(value.parse().map_err(|_| ConfigError::InvalidValue {
                    name: "SOOQA_TELEGRAM_STORAGE_CHAT_ID".to_owned(),
                    reason: "expected a negative Telegram chat ID",
                })?);
        }
        Ok(())
    }

    fn validate(&self) -> Result<(), ConfigError> {
        parse_socket_address("server.listen_address", &self.server.listen_address)?;
        let companion_address =
            parse_socket_address("companion.listen_address", &self.companion.listen_address)?;
        if !companion_address.ip().is_loopback() {
            return Err(ConfigError::InvalidValue {
                name: "companion.listen_address".to_owned(),
                reason: "must use a loopback address",
            });
        }
        if self.worker.poll_interval_seconds == 0 {
            return Err(ConfigError::InvalidValue {
                name: "worker.poll_interval_seconds".to_owned(),
                reason: "must be greater than zero",
            });
        }
        if self.worker.lease_duration_seconds == 0 {
            return Err(ConfigError::InvalidValue {
                name: "worker.lease_duration_seconds".to_owned(),
                reason: "must be greater than zero",
            });
        }
        if self.server.request_body_limit_bytes == 0 {
            return Err(ConfigError::InvalidValue {
                name: "server.request_body_limit_bytes".to_owned(),
                reason: "must be greater than zero",
            });
        }
        if self.server.request_timeout_seconds == 0 {
            return Err(ConfigError::InvalidValue {
                name: "server.request_timeout_seconds".to_owned(),
                reason: "must be greater than zero",
            });
        }
        if self.database.max_connections == 0 {
            return Err(ConfigError::InvalidValue {
                name: "database.max_connections".to_owned(),
                reason: "must be greater than zero",
            });
        }
        let api_base_url = url::Url::parse(&self.telegram.api_base_url).map_err(|_| {
            ConfigError::InvalidValue {
                name: "telegram.api_base_url".to_owned(),
                reason: "must be a valid HTTP(S) URL",
            }
        })?;
        if !matches!(api_base_url.scheme(), "http" | "https")
            || api_base_url.host_str().is_none()
            || !api_base_url.username().is_empty()
            || api_base_url.password().is_some()
        {
            return Err(ConfigError::InvalidValue {
                name: "telegram.api_base_url".to_owned(),
                reason: "must be an HTTP(S) URL without credentials",
            });
        }
        if self.telegram.poll_timeout_seconds == 0 {
            return Err(ConfigError::InvalidValue {
                name: "telegram.poll_timeout_seconds".to_owned(),
                reason: "must be greater than zero",
            });
        }
        if self.telegram.max_download_bytes == 0 {
            return Err(ConfigError::InvalidValue {
                name: "telegram.max_download_bytes".to_owned(),
                reason: "must be greater than zero",
            });
        }
        if self.telegram.storage_chat_id.is_some_and(|id| id >= 0) {
            return Err(ConfigError::InvalidValue {
                name: "telegram.storage_chat_id".to_owned(),
                reason: "must be a negative Telegram channel or group ID",
            });
        }
        if self.secrets.telegram_bot_token.as_ref().is_some_and(SecretString::is_configured)
            && self.telegram.admin_user_ids.is_empty()
        {
            return Err(ConfigError::InvalidValue {
                name: "telegram.admin_user_ids".to_owned(),
                reason: "must configure at least one administrator when a bot token is set",
            });
        }
        if self.telegram.admin_user_ids.iter().any(|id| *id <= 0) {
            return Err(ConfigError::InvalidValue {
                name: "telegram.admin_user_ids".to_owned(),
                reason: "must contain positive Telegram user IDs",
            });
        }
        for (name, path) in [
            ("media.work_root", &self.media.work_root),
            ("media.ffmpeg_path", &self.media.ffmpeg_path),
            ("media.ffprobe_path", &self.media.ffprobe_path),
            ("media.ytdlp_path", &self.media.ytdlp_path),
        ] {
            if path.as_os_str().is_empty() {
                return Err(ConfigError::InvalidValue {
                    name: name.to_owned(),
                    reason: "must not be empty",
                });
            }
        }
        if self.media.ytdlp_format.trim().is_empty() {
            return Err(ConfigError::InvalidValue {
                name: "media.ytdlp_format".to_owned(),
                reason: "must not be empty",
            });
        }
        if self.media.ytdlp_format.starts_with('-')
            || self.media.ytdlp_format.chars().any(char::is_control)
        {
            return Err(ConfigError::InvalidValue {
                name: "media.ytdlp_format".to_owned(),
                reason: "must not start with an option prefix or contain control characters",
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
    media: RawMediaConfig,
    companion: RawCompanionConfig,
    database: RawDatabaseConfig,
    telegram: RawTelegramConfig,
    observability: RawObservabilityConfig,
    secrets: RawSecretConfig,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct RawTelegramConfig {
    api_base_url: Option<String>,
    admin_user_ids: Vec<i64>,
    poll_timeout_seconds: Option<u64>,
    max_download_bytes: Option<u64>,
    storage_chat_id: Option<i64>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct RawDatabaseConfig {
    url_env: Option<String>,
    max_connections: Option<u32>,
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
    lease_duration_seconds: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct RawMediaConfig {
    work_root: Option<String>,
    ffmpeg_path: Option<String>,
    ffprobe_path: Option<String>,
    ytdlp_path: Option<String>,
    ytdlp_format: Option<String>,
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
    api_token: Option<String>,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("could not read config file {path}: {source}")]
    ReadFile { path: PathBuf, source: std::io::Error },
    #[error("could not parse TOML config: {0}")]
    ParseToml(#[from] toml::de::Error),
    #[error("environment variable {name} contains invalid UTF-8")]
    InvalidEnvironmentEncoding { name: String },
    #[error("invalid value for {name}: {reason}")]
    InvalidValue { name: String, reason: &'static str },
    #[error("missing argument after {0}")]
    MissingArgument(&'static str),
    #[error("unknown argument: {0}")]
    UnknownArgument(String),
    #[error("help requested")]
    HelpRequested,
    #[error("required secret is not configured: {0}")]
    MissingSecret(&'static str),
}

fn optional_env_path(name: &str) -> Result<Option<PathBuf>, ConfigError> {
    Ok(optional_env_string(name)?.map(PathBuf::from))
}

fn optional_env_string(name: &str) -> Result<Option<String>, ConfigError> {
    match env::var_os(name) {
        Some(value) => value
            .into_string()
            .map(Some)
            .map_err(|_| ConfigError::InvalidEnvironmentEncoding { name: name.to_owned() }),
        None => Ok(None),
    }
}

fn parse_admin_user_ids(name: &str, value: &str) -> Result<Vec<i64>, ConfigError> {
    value
        .split(',')
        .map(str::trim)
        .map(|value| {
            value.parse::<i64>().map_err(|_| ConfigError::InvalidValue {
                name: name.to_owned(),
                reason: "expected comma-separated positive Telegram user IDs",
            })
        })
        .collect()
}

fn parse_socket_address(name: &str, value: &str) -> Result<SocketAddr, ConfigError> {
    SocketAddr::from_str(value).map_err(|_| ConfigError::InvalidValue {
        name: name.to_owned(),
        reason: "expected an IP address and port such as 127.0.0.1:8080",
    })
}

fn parse_log_format(value: &str) -> Result<LogFormat, ConfigError> {
    match value.to_ascii_lowercase().as_str() {
        "json" => Ok(LogFormat::Json),
        "pretty" => Ok(LogFormat::Pretty),
        _ => Err(ConfigError::InvalidValue {
            name: "observability.log_format".to_owned(),
            reason: "expected json or pretty",
        }),
    }
}

fn normalize_log_level(value: &str) -> Result<String, ConfigError> {
    let normalized = value.to_ascii_lowercase();
    match normalized.as_str() {
        "trace" | "debug" | "info" | "warn" | "error" => Ok(normalized),
        _ => Err(ConfigError::InvalidValue {
            name: "observability.log_level".to_owned(),
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
    fn worker_lease_duration_must_be_positive() {
        let config = AppConfig::from_toml_str(
            AppRole::Worker,
            None,
            "[worker]\nlease_duration_seconds = 0\n",
        )
        .expect("TOML should parse");

        let error = config.validate().expect_err("zero lease duration must fail");
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

    #[test]
    fn storage_commands_parse_with_explicit_reset_confirmation() {
        assert_eq!(
            CliOptions::parse(["storage", "list"]).expect("list command should parse").command,
            Some(CliCommand::Storage(StorageCommand::List))
        );
        assert_eq!(
            CliOptions::parse(["storage", "reset", "media-id", "--confirm"])
                .expect("reset command should parse")
                .command,
            Some(CliCommand::Storage(StorageCommand::Reset { media_id: "media-id".to_owned() }))
        );
        assert!(CliOptions::parse(["storage", "reset", "media-id"]).is_err());
    }

    #[test]
    fn storage_commands_parse_force_and_typed_attach_arguments() {
        assert_eq!(
            CliOptions::parse(["storage", "mark-unknown", "media-id", "--force", "--confirm",])
                .expect("forced mark-unknown should parse")
                .command,
            Some(CliCommand::Storage(StorageCommand::MarkUnknown {
                media_id: "media-id".to_owned(),
                force: true,
            }))
        );
        assert_eq!(
            CliOptions::parse([
                "storage",
                "attach",
                "media-id",
                "3",
                "-100123",
                "789",
                "file-id",
                "unique-id",
            ])
            .expect("typed attach should parse")
            .command,
            Some(CliCommand::Storage(StorageCommand::Attach {
                media_id: "media-id".to_owned(),
                generation: "3".to_owned(),
                storage_chat_id: "-100123".to_owned(),
                storage_message_id: "789".to_owned(),
                telegram_file_id: "file-id".to_owned(),
                telegram_file_unique_id: "unique-id".to_owned(),
            }))
        );
        assert!(CliOptions::parse(["storage", "mark-unknown", "media-id", "--force"]).is_err());
    }

    #[test]
    fn media_binary_paths_are_configurable() {
        let config = AppConfig::from_toml_str(
            AppRole::Worker,
            None,
            "[media]\nffprobe_path = \"/opt/bin/ffprobe\"\n",
        )
        .expect("TOML should parse");

        assert_eq!(config.media.ffmpeg_path, PathBuf::from("ffmpeg"));
        assert_eq!(config.media.work_root, PathBuf::from("/var/lib/sooqa/work"));
        assert_eq!(config.media.ffprobe_path, PathBuf::from("/opt/bin/ffprobe"));
        assert_eq!(config.media.ytdlp_path, PathBuf::from("yt-dlp"));
        assert_eq!(config.media.ytdlp_format, "bestvideo*+bestaudio/best");
    }

    #[test]
    fn telegram_settings_parse_and_validate() {
        let config = AppConfig::from_toml_str(
            AppRole::Server,
            None,
            "[telegram]\napi_base_url = \"http://telegram-bot-api:8081\"\nadmin_user_ids = [123456789]\npoll_timeout_seconds = 45\nstorage_chat_id = -1001234567890\n",
        )
        .expect("TOML should parse");

        assert!(config.validate().is_ok());
        assert_eq!(config.telegram.api_base_url, "http://telegram-bot-api:8081");
        assert_eq!(config.telegram.admin_user_ids, vec![123456789]);
        assert_eq!(config.telegram.poll_timeout_seconds, 45);
        assert_eq!(config.telegram.max_download_bytes, 2 * 1024 * 1024 * 1024);
        assert_eq!(config.telegram.storage_chat_id, Some(-1001234567890));
    }

    #[test]
    fn telegram_storage_chat_must_be_negative() {
        let config =
            AppConfig::from_toml_str(AppRole::Server, None, "[telegram]\nstorage_chat_id = 123\n")
                .expect("TOML should parse");

        assert!(
            matches!(config.validate(), Err(ConfigError::InvalidValue { name, .. }) if name == "telegram.storage_chat_id")
        );
    }

    #[test]
    fn telegram_api_url_rejects_credentials() {
        let config = AppConfig::from_toml_str(
            AppRole::Server,
            None,
            "[telegram]\napi_base_url = \"https://user:password@example.test\"\n",
        )
        .expect("TOML should parse");

        let error = config.validate().expect_err("Telegram API credentials must fail");
        assert!(error.to_string().contains("without credentials"));
    }

    #[test]
    fn unsafe_ytdlp_format_is_rejected() {
        let config = AppConfig::from_toml_str(
            AppRole::Worker,
            None,
            "[media]\nytdlp_format = \"--exec=whoami\"\n",
        )
        .expect("TOML should parse");

        let error = config.validate().expect_err("option-looking format must fail");
        assert!(error.to_string().contains("option prefix"));
    }
}
