//! Teloxide-backed Telegram API and bounded local/cloud file transfer.

use std::{
    io,
    path::{Component, Path, PathBuf},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use async_trait::async_trait;
use teloxide::{
    Bot,
    payloads::GetUpdatesSetters,
    prelude::{Request, Requester},
    types::{CallbackQueryId, ChatId, FileId, Update},
};
use url::Url;
use uuid::Uuid;

use crate::{
    DEFAULT_TELEGRAM_SOURCE_DOWNLOAD_MAX_BYTES, DEFAULT_TELEGRAM_UPLOAD_TIMEOUT_SECONDS,
    StorageUploadApiError, TelegramApi, TelegramApiError, TelegramError, TelegramPollingApi,
    TelegramPollingError, TelegramStorageApi,
};

const TELEGRAM_CLOUD_DOWNLOAD_LIMIT_BYTES: u64 = 20 * 1024 * 1024;
const TELEGRAM_CLOUD_UPLOAD_LIMIT_BYTES: u64 = 50 * 1024 * 1024;
const TELEGRAM_MEDIA_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const TELEGRAM_MEDIA_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);
const TELEGRAM_MAX_UPLOAD_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(24 * 60 * 60);

#[derive(Clone)]
pub struct TeloxideApi {
    control_bot: Bot,
    media_bot: Bot,
    upload_bot: Bot,
    cloud_download_limit_bytes: Option<u64>,
    pub(super) cloud_upload_limit_bytes: Option<u64>,
    source_download_max_bytes: u64,
    local_file_root: Option<PathBuf>,
}

impl TeloxideApi {
    pub fn new(
        token: impl Into<String>,
        api_base_url: &str,
        poll_timeout: Duration,
    ) -> Result<Self, TelegramError> {
        Self::new_with_upload_timeout(
            token,
            api_base_url,
            poll_timeout,
            Duration::from_secs(DEFAULT_TELEGRAM_UPLOAD_TIMEOUT_SECONDS),
        )
    }

    pub fn new_with_upload_timeout(
        token: impl Into<String>,
        api_base_url: &str,
        poll_timeout: Duration,
        upload_timeout: Duration,
    ) -> Result<Self, TelegramError> {
        if poll_timeout.is_zero() || poll_timeout.as_secs() > u64::from(u32::MAX) {
            return Err(TelegramError::InvalidPollTimeout);
        }
        if upload_timeout.is_zero() || upload_timeout > TELEGRAM_MAX_UPLOAD_TIMEOUT {
            return Err(TelegramError::InvalidUploadTimeout);
        }
        let api_base_url = Url::parse(api_base_url)
            .map_err(|error| TelegramError::InvalidApiBaseUrl(error.to_string()))?;
        if !is_safe_api_base_url(&api_base_url) {
            return Err(TelegramError::InvalidApiBaseUrl(
                "must be an HTTP(S) URL without credentials; HTTP URLs must use a private host"
                    .to_owned(),
            ));
        }
        let client_timeout = poll_timeout.checked_add(Duration::from_secs(5)).ok_or_else(|| {
            TelegramError::InvalidApiBaseUrl("poll timeout is too large".to_owned())
        })?;
        let token = token.into();
        let control_client = reqwest::Client::builder()
            .timeout(client_timeout)
            .build()
            .map_err(TelegramError::HttpClient)?;
        let media_client = reqwest::Client::builder()
            .connect_timeout(TELEGRAM_MEDIA_CONNECT_TIMEOUT)
            .read_timeout(TELEGRAM_MEDIA_READ_TIMEOUT)
            .build()
            .map_err(TelegramError::HttpClient)?;
        let is_cloud = api_base_url.host_str() == Some("api.telegram.org");
        let control_bot =
            Bot::with_client(token.clone(), control_client).set_api_url(api_base_url.clone());
        let media_bot =
            Bot::with_client(token.clone(), media_client).set_api_url(api_base_url.clone());
        let upload_bot = upload_bot_with_timeout(&control_bot, upload_timeout)?;
        Ok(Self {
            control_bot,
            media_bot,
            upload_bot,
            cloud_download_limit_bytes: is_cloud.then_some(TELEGRAM_CLOUD_DOWNLOAD_LIMIT_BYTES),
            cloud_upload_limit_bytes: is_cloud.then_some(TELEGRAM_CLOUD_UPLOAD_LIMIT_BYTES),
            source_download_max_bytes: DEFAULT_TELEGRAM_SOURCE_DOWNLOAD_MAX_BYTES,
            local_file_root: None,
        })
    }

    pub fn with_source_download_max_bytes(mut self, max_bytes: u64) -> Self {
        self.source_download_max_bytes = max_bytes;
        self
    }

    pub fn with_local_file_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.local_file_root = Some(root.into());
        self
    }

    pub fn with_upload_timeout(mut self, upload_timeout: Duration) -> Result<Self, TelegramError> {
        self.upload_bot = upload_bot_with_timeout(&self.control_bot, upload_timeout)?;
        Ok(self)
    }

    pub(super) fn bot(&self) -> Bot {
        self.control_bot.clone()
    }

    fn media_bot(&self) -> Bot {
        self.media_bot.clone()
    }

    pub(super) fn upload_bot(&self) -> Bot {
        self.upload_bot.clone()
    }

    #[cfg(test)]
    fn with_test_media_read_timeout(
        mut self,
        read_timeout: Duration,
    ) -> Result<Self, TelegramError> {
        let client = reqwest::Client::builder()
            .connect_timeout(TELEGRAM_MEDIA_CONNECT_TIMEOUT)
            .read_timeout(read_timeout)
            .build()
            .map_err(TelegramError::HttpClient)?;
        self.media_bot = Bot::with_client(self.media_bot.token().to_owned(), client)
            .set_api_url(self.media_bot.api_url());
        Ok(self)
    }

    #[cfg(test)]
    pub(super) fn with_test_cloud_limits(mut self, limit: u64) -> Self {
        self.cloud_download_limit_bytes = Some(limit);
        self.cloud_upload_limit_bytes = Some(limit);
        self
    }
}

fn upload_bot_with_timeout(bot: &Bot, upload_timeout: Duration) -> Result<Bot, TelegramError> {
    if upload_timeout.is_zero() || upload_timeout > TELEGRAM_MAX_UPLOAD_TIMEOUT {
        return Err(TelegramError::InvalidUploadTimeout);
    }
    let client = reqwest::Client::builder()
        .connect_timeout(TELEGRAM_MEDIA_CONNECT_TIMEOUT)
        .timeout(upload_timeout)
        .build()
        .map_err(TelegramError::HttpClient)?;
    Ok(Bot::with_client(bot.token().to_owned(), client).set_api_url(bot.api_url()))
}

fn is_safe_api_base_url(url: &Url) -> bool {
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return false;
    }
    if url.scheme() == "https" {
        return true;
    }

    let Some(host) = url.host_str() else { return false };
    match host.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V4(address)) => {
            !address.is_unspecified()
                && (address.is_loopback() || address.is_private() || address.is_link_local())
        }
        Ok(std::net::IpAddr::V6(address)) => {
            !address.is_unspecified()
                && (address.is_loopback()
                    || address.is_unique_local()
                    || address.is_unicast_link_local())
        }
        Err(_) => {
            host.eq_ignore_ascii_case("localhost")
                || !host.contains('.')
                || host.ends_with(".local")
        }
    }
}

#[async_trait]
impl TelegramApi for TeloxideApi {
    type Error = TelegramApiError;

    fn is_retryable_error(error: &Self::Error) -> bool {
        match error {
            TelegramApiError::Api(teloxide::RequestError::Api(
                teloxide::errors::ApiError::Unknown(_),
            ))
            | TelegramApiError::Api(teloxide::RequestError::Network(_))
            | TelegramApiError::Api(teloxide::RequestError::InvalidJson { .. })
            | TelegramApiError::Api(teloxide::RequestError::RetryAfter(_))
            | TelegramApiError::Download(teloxide::DownloadError::Network(_)) => true,
            TelegramApiError::DownloadLimit { .. }
            | TelegramApiError::MissingFileReference { .. }
            | TelegramApiError::InvalidParseMode
            | TelegramApiError::InvalidMessageId(_)
            | TelegramApiError::LocalFileRootNotConfigured
            | TelegramApiError::LocalFileRootUnavailable
            | TelegramApiError::LocalFilePathRejected
            | TelegramApiError::LocalFileNotRegular
            | TelegramApiError::DownloadSizeMismatch { .. }
            | TelegramApiError::Api(_)
            | TelegramApiError::Download(_)
            | TelegramApiError::Io(_) => false,
        }
    }

    async fn send_text(&self, chat_id: i64, text: &str) -> Result<(), Self::Error> {
        self.bot()
            .send_message(ChatId(chat_id), text.to_owned())
            .await
            .map(|_| ())
            .map_err(TelegramApiError::Api)
    }

    async fn answer_callback_query(&self, callback_id: &str) -> Result<(), Self::Error> {
        self.bot()
            .answer_callback_query(CallbackQueryId(callback_id.to_owned()))
            .await
            .map(|_| ())
            .map_err(TelegramApiError::Api)
    }

    async fn download_file(&self, file_id: &str, destination: &Path) -> Result<(), Self::Error> {
        let file = self
            .media_bot()
            .get_file(FileId(file_id.to_owned()))
            .send()
            .await
            .map_err(TelegramApiError::Api)?;
        let size = u64::from(file.meta.size);
        if Path::new(&file.path).is_absolute() {
            let limit = self.source_download_max_bytes;
            if size > limit {
                return Err(TelegramApiError::DownloadLimit { size, limit });
            }
            let local_root = self
                .local_file_root
                .as_deref()
                .ok_or(TelegramApiError::LocalFileRootNotConfigured)?;
            let local_file = resolve_local_file(local_root, Path::new(&file.path)).await?;
            if local_file.size > limit {
                return Err(TelegramApiError::DownloadLimit { size: local_file.size, limit });
            }
            if local_file.size != size {
                return Err(TelegramApiError::DownloadSizeMismatch {
                    reported: size,
                    actual: local_file.size,
                });
            }
            return copy_local_file(&local_file.path, destination, size, limit).await;
        }
        let limit =
            self.cloud_download_limit_bytes.map_or(self.source_download_max_bytes, |cloud_limit| {
                cloud_limit.min(self.source_download_max_bytes)
            });
        if size > limit {
            return Err(TelegramApiError::DownloadLimit { size, limit });
        }
        let (temporary, mut output) = open_temporary_download(destination).await?;
        let (download_result, exceeded, written) = {
            let mut limited_output = LimitedWriter::new(&mut output, limit);
            let download_result = teloxide::net::Download::download_file(
                &self.media_bot(),
                &file.path,
                &mut limited_output,
            )
            .await;
            (download_result, limited_output.exceeded(), limited_output.written())
        };
        if exceeded {
            return Err(TelegramApiError::DownloadLimit { size: written.saturating_add(1), limit });
        }
        if let Err(error) = download_result {
            return Err(TelegramApiError::Download(error));
        }
        finish_temporary_download(temporary, output, destination, size, limit).await
    }
}

#[async_trait]
impl TelegramPollingApi for TeloxideApi {
    type PollingError = TelegramPollingError;

    async fn verify_storage_chat(&self, chat_id: i64) -> Result<(), Self::PollingError> {
        <Self as TelegramStorageApi>::verify_storage_chat(self, chat_id)
            .await
            .map_err(TelegramPollingError::Storage)
    }

    async fn delete_webhook(&self) -> Result<(), Self::PollingError> {
        self.bot().delete_webhook().send().await.map(|_| ()).map_err(TelegramPollingError::Api)
    }

    async fn get_updates(
        &self,
        offset: i32,
        timeout_seconds: u32,
    ) -> Result<Vec<Update>, Self::PollingError> {
        self.bot()
            .get_updates()
            .offset(offset)
            .timeout(timeout_seconds)
            .send()
            .await
            .map_err(TelegramPollingError::Api)
    }

    fn is_terminal_error(error: &Self::PollingError) -> bool {
        matches!(
            error,
            TelegramPollingError::Api(teloxide::RequestError::Api(
                teloxide::errors::ApiError::InvalidToken,
            )) | TelegramPollingError::Storage(
                StorageUploadApiError::StorageChatNotPrivateChannel
                    | StorageUploadApiError::StorageBotNotAdministrator
                    | StorageUploadApiError::StorageBotCannotPost,
            ) | TelegramPollingError::Storage(StorageUploadApiError::Api(
                teloxide::RequestError::Api(teloxide::errors::ApiError::InvalidToken),
            ))
        )
    }

    fn retry_after(error: &Self::PollingError) -> Option<Duration> {
        let TelegramPollingError::Api(teloxide::RequestError::RetryAfter(seconds)) = error else {
            return None;
        };
        Some(seconds.duration())
    }
}

struct LocalFile {
    path: PathBuf,
    size: u64,
}

async fn resolve_local_file(root: &Path, candidate: &Path) -> Result<LocalFile, TelegramApiError> {
    if candidate.components().any(|component| matches!(component, Component::ParentDir)) {
        return Err(TelegramApiError::LocalFilePathRejected);
    }
    let root = tokio::fs::canonicalize(root)
        .await
        .map_err(|_| TelegramApiError::LocalFileRootUnavailable)?;
    let root_metadata =
        tokio::fs::metadata(&root).await.map_err(|_| TelegramApiError::LocalFileRootUnavailable)?;
    if !root_metadata.is_dir() {
        return Err(TelegramApiError::LocalFileRootUnavailable);
    }
    let candidate = tokio::fs::canonicalize(candidate)
        .await
        .map_err(|_| TelegramApiError::LocalFilePathRejected)?;
    if candidate.strip_prefix(&root).is_err() {
        return Err(TelegramApiError::LocalFilePathRejected);
    }
    let metadata = tokio::fs::metadata(&candidate)
        .await
        .map_err(|_| TelegramApiError::LocalFilePathRejected)?;
    if !metadata.is_file() {
        return Err(TelegramApiError::LocalFileNotRegular);
    }
    Ok(LocalFile { path: candidate, size: metadata.len() })
}

async fn copy_local_file(
    source: &Path,
    destination: &Path,
    expected_size: u64,
    limit: u64,
) -> Result<(), TelegramApiError> {
    let mut input = tokio::fs::File::open(source).await.map_err(TelegramApiError::Io)?;
    let (temporary, mut output) = open_temporary_download(destination).await?;
    let (copy_result, exceeded, written) = {
        let mut limited_output = LimitedWriter::new(&mut output, limit);
        let copy_result = tokio::io::copy(&mut input, &mut limited_output).await;
        (copy_result, limited_output.exceeded(), limited_output.written())
    };
    if exceeded {
        return Err(TelegramApiError::DownloadLimit { size: written.saturating_add(1), limit });
    }
    if let Err(error) = copy_result {
        return Err(TelegramApiError::Io(error));
    }
    finish_temporary_download(temporary, output, destination, expected_size, limit).await
}

async fn open_temporary_download(
    destination: &Path,
) -> Result<(TemporaryDownload, tokio::fs::File), TelegramApiError> {
    let temporary_path =
        destination.with_file_name(format!(".sooqa-download-{}.tmp", Uuid::new_v4()));
    let temporary = TemporaryDownload::new(temporary_path);
    let output = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temporary.path())
        .await
        .map_err(TelegramApiError::Io)?;
    Ok((temporary, output))
}

async fn finish_temporary_download(
    mut temporary: TemporaryDownload,
    mut output: tokio::fs::File,
    destination: &Path,
    expected_size: u64,
    limit: u64,
) -> Result<(), TelegramApiError> {
    if let Err(error) = tokio::io::AsyncWriteExt::flush(&mut output).await {
        return Err(TelegramApiError::Io(error));
    }
    drop(output);
    let actual_size =
        tokio::fs::metadata(temporary.path()).await.map_err(TelegramApiError::Io)?.len();
    if actual_size > limit {
        return Err(TelegramApiError::DownloadLimit { size: actual_size, limit });
    }
    if actual_size != expected_size {
        return Err(TelegramApiError::DownloadSizeMismatch {
            reported: expected_size,
            actual: actual_size,
        });
    }
    tokio::fs::rename(temporary.path(), destination).await.map_err(TelegramApiError::Io)?;
    temporary.commit();
    Ok(())
}

struct TemporaryDownload {
    path: PathBuf,
    committed: bool,
}

impl TemporaryDownload {
    fn new(path: PathBuf) -> Self {
        Self { path, committed: false }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for TemporaryDownload {
    fn drop(&mut self) {
        if !self.committed {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

struct LimitedWriter<'a> {
    inner: &'a mut tokio::fs::File,
    limit: u64,
    written: u64,
    exceeded: bool,
}

impl<'a> LimitedWriter<'a> {
    fn new(inner: &'a mut tokio::fs::File, limit: u64) -> Self {
        Self { inner, limit, written: 0, exceeded: false }
    }

    fn exceeded(&self) -> bool {
        self.exceeded
    }

    fn written(&self) -> u64 {
        self.written
    }
}

impl tokio::io::AsyncWrite for LimitedWriter<'_> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let remaining = self.limit.saturating_sub(self.written);
        if buffer.len() as u64 > remaining {
            self.exceeded = true;
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Telegram download exceeds the configured limit",
            )));
        }
        let result = Pin::new(&mut *self.inner).poll_write(context, buffer);
        if let Poll::Ready(Ok(written)) = result {
            self.written = self.written.saturating_add(written as u64);
        }
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.inner).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.inner).poll_shutdown(context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MediaKind, StorageUploadRequest};
    use tokio::{
        io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
        net::{TcpListener, TcpStream},
    };

    #[derive(Debug)]
    struct HttpRequestSummary {
        target: String,
        body_bytes: u64,
        body_contains_marker: bool,
    }

    async fn read_http_request(
        reader: &mut BufReader<TcpStream>,
    ) -> std::io::Result<HttpRequestSummary> {
        read_http_request_with_options(reader, Duration::ZERO, None).await
    }

    async fn read_http_request_with_options(
        reader: &mut BufReader<TcpStream>,
        body_delay: Duration,
        marker: Option<&[u8]>,
    ) -> std::io::Result<HttpRequestSummary> {
        let mut line = Vec::new();
        reader.read_until(b'\n', &mut line).await?;
        if line.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "request line is missing",
            ));
        }
        let request_line = String::from_utf8_lossy(&line);
        let mut parts = request_line.split_whitespace();
        let _method = parts.next().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "request method is missing")
        })?;
        let target = parts
            .next()
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "request path is missing")
            })?
            .to_owned();

        let mut content_length = None;
        let mut chunked = false;
        loop {
            line.clear();
            reader.read_until(b'\n', &mut line).await?;
            if line == b"\r\n" || line == b"\n" {
                break;
            }
            let header = String::from_utf8_lossy(&line);
            let Some((name, value)) = header.split_once(':') else { continue };
            if name.eq_ignore_ascii_case("content-length") {
                content_length = Some(value.trim().parse::<u64>().map_err(|error| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, error)
                })?);
            } else if name.eq_ignore_ascii_case("transfer-encoding") {
                chunked = value
                    .split(',')
                    .any(|encoding| encoding.trim().eq_ignore_ascii_case("chunked"));
            }
        }

        let mut marker_position = 0;
        let mut body_contains_marker = marker.is_some_and(|marker| marker.is_empty());
        let body_bytes = if chunked {
            read_chunked_body(
                reader,
                body_delay,
                marker,
                &mut marker_position,
                &mut body_contains_marker,
            )
            .await?
        } else if let Some(content_length) = content_length {
            read_exact_bytes(
                reader,
                content_length,
                body_delay,
                marker,
                &mut marker_position,
                &mut body_contains_marker,
            )
            .await?
        } else {
            0
        };
        Ok(HttpRequestSummary { target, body_bytes, body_contains_marker })
    }

    async fn read_exact_bytes(
        reader: &mut BufReader<TcpStream>,
        mut remaining: u64,
        body_delay: Duration,
        marker: Option<&[u8]>,
        marker_position: &mut usize,
        body_contains_marker: &mut bool,
    ) -> std::io::Result<u64> {
        let mut buffer = [0_u8; 8 * 1024];
        let mut read = 0_u64;
        while remaining > 0 {
            let chunk = remaining.min(buffer.len() as u64) as usize;
            reader.read_exact(&mut buffer[..chunk]).await?;
            observe_marker(&buffer[..chunk], marker, marker_position, body_contains_marker);
            if !body_delay.is_zero() {
                tokio::time::sleep(body_delay).await;
            }
            remaining -= chunk as u64;
            read += chunk as u64;
        }
        Ok(read)
    }

    fn observe_marker(
        bytes: &[u8],
        marker: Option<&[u8]>,
        marker_position: &mut usize,
        body_contains_marker: &mut bool,
    ) {
        let Some(marker) = marker else { return };
        if marker.is_empty() || *body_contains_marker {
            *body_contains_marker = true;
            return;
        }
        for byte in bytes {
            if *byte == marker[*marker_position] {
                *marker_position += 1;
                if *marker_position == marker.len() {
                    *body_contains_marker = true;
                    return;
                }
            } else {
                *marker_position = usize::from(*byte == marker[0]);
            }
        }
    }

    async fn read_chunked_body(
        reader: &mut BufReader<TcpStream>,
        body_delay: Duration,
        marker: Option<&[u8]>,
        marker_position: &mut usize,
        body_contains_marker: &mut bool,
    ) -> std::io::Result<u64> {
        let mut line = Vec::new();
        let mut read = 0_u64;
        loop {
            line.clear();
            reader.read_until(b'\n', &mut line).await?;
            let size = String::from_utf8_lossy(&line);
            let size = size.split(';').next().unwrap_or_default().trim();
            let size = u64::from_str_radix(size, 16)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
            if size == 0 {
                loop {
                    line.clear();
                    reader.read_until(b'\n', &mut line).await?;
                    if line == b"\r\n" || line == b"\n" {
                        return Ok(read);
                    }
                }
            }
            read += read_exact_bytes(
                reader,
                size,
                body_delay,
                marker,
                marker_position,
                body_contains_marker,
            )
            .await?;
            let mut line_end = [0_u8; 2];
            reader.read_exact(&mut line_end).await?;
        }
    }

    async fn write_http_response(
        stream: &mut TcpStream,
        content_type: &str,
        body: &[u8],
    ) -> std::io::Result<()> {
        let headers = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(headers.as_bytes()).await?;
        stream.write_all(body).await
    }

    async fn serve_file_metadata(listener: TcpListener, file_size: usize) -> String {
        serve_file_metadata_at_path(listener, file_size, "files/payload.bin".to_owned()).await
    }

    async fn serve_file_metadata_at_path(
        listener: TcpListener,
        file_size: usize,
        file_path: String,
    ) -> String {
        let (stream, _) = listener.accept().await.expect("fake API should accept getFile");
        let mut reader = BufReader::new(stream);
        let request = read_http_request(&mut reader).await.expect("getFile should be readable");
        let response = format!(
            r#"{{"ok":true,"result":{{"file_id":"file-id","file_unique_id":"file-unique-id","file_size":{},"file_path":"{}"}}}}"#,
            file_size, file_path
        );
        let mut stream = reader.into_inner();
        write_http_response(&mut stream, "application/json", response.as_bytes())
            .await
            .expect("getFile response should be writable");
        stream.shutdown().await.expect("getFile connection should close");
        request.target
    }

    async fn serve_download(
        listener: TcpListener,
        payload: Vec<u8>,
        delay_before_payload: Duration,
    ) -> (String, String) {
        let (stream, _) = listener.accept().await.expect("fake API should accept getFile");
        let mut reader = BufReader::new(stream);
        let get_file = read_http_request(&mut reader).await.expect("getFile should be readable");
        let file_response = format!(
            r#"{{"ok":true,"result":{{"file_id":"file-id","file_unique_id":"file-unique-id","file_size":{},"file_path":"files/payload.bin"}}}}"#,
            payload.len()
        );
        let mut stream = reader.into_inner();
        write_http_response(&mut stream, "application/json", file_response.as_bytes())
            .await
            .expect("getFile response should be writable");
        stream.shutdown().await.expect("getFile connection should close");

        let (stream, _) = listener.accept().await.expect("fake API should accept file download");
        let mut reader = BufReader::new(stream);
        let file_download =
            read_http_request(&mut reader).await.expect("file download should be readable");
        let mut stream = reader.into_inner();
        let headers = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/octet-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            payload.len()
        );
        stream.write_all(headers.as_bytes()).await.expect("file headers should be writable");
        if !delay_before_payload.is_zero() {
            tokio::time::sleep(delay_before_payload).await;
        }
        for chunk in payload.chunks(3) {
            stream.write_all(chunk).await.expect("file payload should be writable");
        }
        (get_file.target, file_download.target)
    }

    async fn serve_upload(
        listener: TcpListener,
        body_delay: Duration,
        marker: &'static [u8],
    ) -> HttpRequestSummary {
        let (stream, _) = listener.accept().await.expect("fake API should accept upload");
        let mut reader = BufReader::new(stream);
        let request = read_http_request_with_options(&mut reader, body_delay, Some(marker))
            .await
            .expect("upload should be readable");
        let mut stream = reader.into_inner();
        let response = br#"{"ok":true,"result":{"message_id":7,"date":0,"chat":{"id":-100123,"type":"channel","title":"Storage"},"video":{"file_id":"stored-file-id","file_unique_id":"stored-file-unique-id","width":1,"height":1,"duration":1,"file_size":21,"mime_type":"video/mp4"}}}"#;
        let response_value: serde_json::Value =
            serde_json::from_slice(response).expect("upload fixture should deserialize");
        let video: teloxide::types::Video =
            serde_json::from_value(response_value["result"]["video"].clone())
                .expect("video fixture should deserialize");
        assert_eq!(video.file.id.0, "stored-file-id");
        let message: teloxide::types::Message = serde_json::from_value(
            response_value.get("result").expect("upload fixture should contain a result").clone(),
        )
        .expect("upload result fixture should deserialize");
        assert!(message.video().is_some(), "upload fixture should contain a video: {message:?}");
        write_http_response(&mut stream, "application/json", response)
            .await
            .expect("upload response should be writable");
        request
    }

    async fn assert_no_temporary_downloads(directory: &Path) {
        let mut entries =
            tokio::fs::read_dir(directory).await.expect("directory should be readable");
        while let Some(entry) =
            entries.next_entry().await.expect("directory entry should be readable")
        {
            assert!(
                !entry.file_name().to_string_lossy().starts_with(".sooqa-download-"),
                "temporary download should be cleaned up"
            );
        }
    }

    #[tokio::test]
    async fn teloxide_api_uses_configured_bot_api_url() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("fake API should bind");
        let address = listener.local_addr().expect("fake API address should be available");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("fake API should accept");
            let mut request = [0_u8; 4096];
            let bytes = stream.read(&mut request).await.expect("request should be readable");
            let request = String::from_utf8_lossy(&request[..bytes]).into_owned();
            let body = r#"{"ok":true,"result":{"message_id":1,"date":0,"chat":{"id":42,"type":"private","first_name":"Admin"},"text":"hello"}}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).await.expect("response should be writable");
            request
        });
        let api =
            TeloxideApi::new("test-token", &format!("http://{address}"), Duration::from_secs(1))
                .expect("fake API URL should be accepted");

        api.send_text(42, "hello").await.expect("fake API response should parse");
        let request = server.await.expect("fake API task should finish");
        assert!(
            request.contains("/bottest-token/SendMessage")
                && request.contains("\"text\":\"hello\""),
            "unexpected request: {request}"
        );
    }

    #[tokio::test]
    async fn local_bot_api_copies_confined_absolute_file_path_without_http_file_request() {
        const PAYLOAD: &[u8] = b"local transfer payload";
        const SOURCE_LIMIT: u64 = 64;
        const REDUCED_CLOUD_CEILING: u64 = 4;
        assert!(PAYLOAD.len() as u64 > REDUCED_CLOUD_CEILING);

        let directory =
            std::env::temp_dir().join(format!("sooqa-telegram-local-{}", Uuid::new_v4()));
        let root = directory.join("telegram-bot-api");
        let source = root.join("files/payload.bin");
        let output = directory.join("output");
        tokio::fs::create_dir_all(source.parent().expect("source should have a parent"))
            .await
            .expect("local Bot API fixture directory should be created");
        tokio::fs::create_dir(&output).await.expect("output directory should be created");
        tokio::fs::write(&source, PAYLOAD).await.expect("local fixture should be written");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("fake API should bind");
        let address = listener.local_addr().expect("fake API address should be available");
        let server = tokio::spawn(serve_file_metadata_at_path(
            listener,
            PAYLOAD.len(),
            source.to_string_lossy().into_owned(),
        ));
        let destination = output.join("download.webm");
        let api =
            TeloxideApi::new("test-token", &format!("http://{address}"), Duration::from_secs(1))
                .expect("fake API URL should be accepted")
                .with_source_download_max_bytes(SOURCE_LIMIT)
                .with_test_cloud_limits(REDUCED_CLOUD_CEILING)
                .with_local_file_root(root);

        api.download_file("file-id", &destination)
            .await
            .expect("local Bot API download should succeed");
        assert_eq!(
            tokio::fs::read(&destination).await.expect("download should be readable"),
            PAYLOAD
        );
        let mut entries =
            tokio::fs::read_dir(&output).await.expect("output directory should be readable");
        while let Some(entry) =
            entries.next_entry().await.expect("directory entry should be readable")
        {
            let name = entry.file_name();
            assert!(!name.to_string_lossy().starts_with(".sooqa-download-"));
        }
        let get_file_target = server.await.expect("getFile task should finish");
        assert!(get_file_target.contains("/bottest-token/GetFile"));
        assert!(!get_file_target.contains("/file/bottest-token/"));
        tokio::fs::remove_dir_all(directory).await.expect("test directory should be removed");
    }

    #[tokio::test]
    async fn absolute_local_file_path_without_root_is_terminal_and_never_downloaded_over_http() {
        let directory =
            std::env::temp_dir().join(format!("sooqa-telegram-local-{}", Uuid::new_v4()));
        let source = directory.join("telegram-bot-api/files/payload.bin");
        let output = directory.join("output");
        tokio::fs::create_dir_all(source.parent().expect("source should have a parent"))
            .await
            .expect("local Bot API fixture directory should be created");
        tokio::fs::create_dir(&output).await.expect("output directory should be created");
        tokio::fs::write(&source, b"local payload").await.expect("local fixture should be written");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("fake API should bind");
        let address = listener.local_addr().expect("fake API address should be available");
        let server = tokio::spawn(serve_file_metadata_at_path(
            listener,
            13,
            source.to_string_lossy().into_owned(),
        ));
        let destination = output.join("download.bin");
        let api =
            TeloxideApi::new("test-token", &format!("http://{address}"), Duration::from_secs(1))
                .expect("fake API URL should be accepted")
                .with_source_download_max_bytes(64);

        assert!(matches!(
            api.download_file("file-id", &destination).await,
            Err(TelegramApiError::LocalFileRootNotConfigured)
        ));
        let get_file_target = server.await.expect("getFile task should finish");
        assert!(get_file_target.contains("/bottest-token/GetFile"));
        assert!(tokio::fs::metadata(&destination).await.is_err());
        assert_no_temporary_downloads(&output).await;
        tokio::fs::remove_dir_all(directory).await.expect("test directory should be removed");
    }

    #[tokio::test]
    async fn local_file_path_must_remain_beneath_the_configured_root() {
        let directory =
            std::env::temp_dir().join(format!("sooqa-telegram-local-{}", Uuid::new_v4()));
        let root = directory.join("telegram-bot-api");
        let source = directory.join("outside/payload.bin");
        let output = directory.join("output");
        tokio::fs::create_dir_all(&root).await.expect("local root should be created");
        tokio::fs::create_dir_all(source.parent().expect("source should have a parent"))
            .await
            .expect("outside fixture directory should be created");
        tokio::fs::create_dir(&output).await.expect("output directory should be created");
        tokio::fs::write(&source, b"outside payload")
            .await
            .expect("outside fixture should be written");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("fake API should bind");
        let address = listener.local_addr().expect("fake API address should be available");
        let server = tokio::spawn(serve_file_metadata_at_path(
            listener,
            15,
            source.to_string_lossy().into_owned(),
        ));
        let destination = output.join("download.bin");
        let api =
            TeloxideApi::new("test-token", &format!("http://{address}"), Duration::from_secs(1))
                .expect("fake API URL should be accepted")
                .with_source_download_max_bytes(64)
                .with_local_file_root(root);

        assert!(matches!(
            api.download_file("file-id", &destination).await,
            Err(TelegramApiError::LocalFilePathRejected)
        ));
        server.await.expect("getFile task should finish");
        assert!(tokio::fs::metadata(&destination).await.is_err());
        assert_no_temporary_downloads(&output).await;
        tokio::fs::remove_dir_all(directory).await.expect("test directory should be removed");
    }

    #[tokio::test]
    async fn local_file_path_rejects_parent_directory_traversal() {
        let directory =
            std::env::temp_dir().join(format!("sooqa-telegram-local-{}", Uuid::new_v4()));
        let root = directory.join("telegram-bot-api");
        let source = directory.join("outside/payload.bin");
        let output = directory.join("output");
        tokio::fs::create_dir_all(&root).await.expect("local root should be created");
        tokio::fs::create_dir_all(source.parent().expect("source should have a parent"))
            .await
            .expect("outside fixture directory should be created");
        tokio::fs::create_dir(&output).await.expect("output directory should be created");
        tokio::fs::write(&source, b"outside payload")
            .await
            .expect("outside fixture should be written");
        let candidate = root.join("../outside/payload.bin");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("fake API should bind");
        let address = listener.local_addr().expect("fake API address should be available");
        let server = tokio::spawn(serve_file_metadata_at_path(
            listener,
            15,
            candidate.to_string_lossy().into_owned(),
        ));
        let destination = output.join("download.bin");
        let api =
            TeloxideApi::new("test-token", &format!("http://{address}"), Duration::from_secs(1))
                .expect("fake API URL should be accepted")
                .with_source_download_max_bytes(64)
                .with_local_file_root(root);

        assert!(matches!(
            api.download_file("file-id", &destination).await,
            Err(TelegramApiError::LocalFilePathRejected)
        ));
        server.await.expect("getFile task should finish");
        assert!(tokio::fs::metadata(&destination).await.is_err());
        assert_no_temporary_downloads(&output).await;
        tokio::fs::remove_dir_all(directory).await.expect("test directory should be removed");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_file_path_rejects_symlink_escape() {
        use std::os::unix::fs::symlink;

        let directory =
            std::env::temp_dir().join(format!("sooqa-telegram-local-{}", Uuid::new_v4()));
        let root = directory.join("telegram-bot-api");
        let outside = directory.join("outside/payload.bin");
        let link = root.join("files/escape.bin");
        let output = directory.join("output");
        tokio::fs::create_dir_all(link.parent().expect("link should have a parent"))
            .await
            .expect("local root should be created");
        tokio::fs::create_dir_all(outside.parent().expect("outside should have a parent"))
            .await
            .expect("outside fixture directory should be created");
        tokio::fs::create_dir(&output).await.expect("output directory should be created");
        tokio::fs::write(&outside, b"outside payload")
            .await
            .expect("outside fixture should be written");
        symlink(&outside, &link).expect("symlink fixture should be created");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("fake API should bind");
        let address = listener.local_addr().expect("fake API address should be available");
        let server = tokio::spawn(serve_file_metadata_at_path(
            listener,
            15,
            link.to_string_lossy().into_owned(),
        ));
        let destination = output.join("download.bin");
        let api =
            TeloxideApi::new("test-token", &format!("http://{address}"), Duration::from_secs(1))
                .expect("fake API URL should be accepted")
                .with_source_download_max_bytes(64)
                .with_local_file_root(root);

        assert!(matches!(
            api.download_file("file-id", &destination).await,
            Err(TelegramApiError::LocalFilePathRejected)
        ));
        server.await.expect("getFile task should finish");
        assert!(tokio::fs::metadata(&destination).await.is_err());
        assert_no_temporary_downloads(&output).await;
        tokio::fs::remove_dir_all(directory).await.expect("test directory should be removed");
    }

    #[tokio::test]
    async fn local_file_size_mismatch_is_terminal_and_cleans_temporary_output() {
        let directory =
            std::env::temp_dir().join(format!("sooqa-telegram-local-{}", Uuid::new_v4()));
        let root = directory.join("telegram-bot-api");
        let source = root.join("files/payload.bin");
        let output = directory.join("output");
        tokio::fs::create_dir_all(source.parent().expect("source should have a parent"))
            .await
            .expect("local root should be created");
        tokio::fs::create_dir(&output).await.expect("output directory should be created");
        tokio::fs::write(&source, b"actual payload").await.expect("fixture should be written");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("fake API should bind");
        let address = listener.local_addr().expect("fake API address should be available");
        let server = tokio::spawn(serve_file_metadata_at_path(
            listener,
            5,
            source.to_string_lossy().into_owned(),
        ));
        let destination = output.join("download.bin");
        let api =
            TeloxideApi::new("test-token", &format!("http://{address}"), Duration::from_secs(1))
                .expect("fake API URL should be accepted")
                .with_source_download_max_bytes(64)
                .with_local_file_root(root);

        assert!(matches!(
            api.download_file("file-id", &destination).await,
            Err(TelegramApiError::DownloadSizeMismatch { reported: 5, actual: 14 })
        ));
        server.await.expect("getFile task should finish");
        assert!(tokio::fs::metadata(&destination).await.is_err());
        assert_no_temporary_downloads(&output).await;
        tokio::fs::remove_dir_all(directory).await.expect("test directory should be removed");
    }

    #[tokio::test]
    async fn local_file_size_overflow_is_rejected_before_temporary_publication() {
        let directory =
            std::env::temp_dir().join(format!("sooqa-telegram-local-{}", Uuid::new_v4()));
        let root = directory.join("telegram-bot-api");
        let source = root.join("files/payload.bin");
        let output = directory.join("output");
        tokio::fs::create_dir_all(source.parent().expect("source should have a parent"))
            .await
            .expect("local root should be created");
        tokio::fs::create_dir(&output).await.expect("output directory should be created");
        tokio::fs::write(&source, b"oversized payload").await.expect("fixture should be written");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("fake API should bind");
        let address = listener.local_addr().expect("fake API address should be available");
        let server = tokio::spawn(serve_file_metadata_at_path(
            listener,
            17,
            source.to_string_lossy().into_owned(),
        ));
        let destination = output.join("download.bin");
        let api =
            TeloxideApi::new("test-token", &format!("http://{address}"), Duration::from_secs(1))
                .expect("fake API URL should be accepted")
                .with_source_download_max_bytes(8)
                .with_local_file_root(root);

        assert!(matches!(
            api.download_file("file-id", &destination).await,
            Err(TelegramApiError::DownloadLimit { size: 17, limit: 8 })
        ));
        server.await.expect("getFile task should finish");
        assert!(tokio::fs::metadata(&destination).await.is_err());
        assert_no_temporary_downloads(&output).await;
        tokio::fs::remove_dir_all(directory).await.expect("test directory should be removed");
    }

    #[tokio::test]
    async fn dropped_temporary_download_removes_partial_output() {
        let directory =
            std::env::temp_dir().join(format!("sooqa-telegram-local-{}", Uuid::new_v4()));
        tokio::fs::create_dir(&directory).await.expect("test directory should be created");
        let destination = directory.join("download.bin");
        let (temporary, mut output) =
            open_temporary_download(&destination).await.expect("temporary file should open");
        tokio::io::AsyncWriteExt::write_all(&mut output, b"partial")
            .await
            .expect("partial output should be writable");
        drop(output);
        let temporary_path = temporary.path().to_owned();
        drop(temporary);
        assert!(tokio::fs::metadata(temporary_path).await.is_err());
        tokio::fs::remove_dir_all(directory).await.expect("test directory should be removed");
    }

    #[tokio::test]
    async fn local_file_path_rejects_directories_and_missing_roots() {
        let directory =
            std::env::temp_dir().join(format!("sooqa-telegram-local-{}", Uuid::new_v4()));
        let root = directory.join("telegram-bot-api");
        let candidate = root.join("files");
        let output = directory.join("output");
        tokio::fs::create_dir_all(&candidate).await.expect("local root should be created");
        tokio::fs::create_dir(&output).await.expect("output directory should be created");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("fake API should bind");
        let address = listener.local_addr().expect("fake API address should be available");
        let server = tokio::spawn(serve_file_metadata_at_path(
            listener,
            0,
            candidate.to_string_lossy().into_owned(),
        ));
        let destination = output.join("download.bin");
        let api =
            TeloxideApi::new("test-token", &format!("http://{address}"), Duration::from_secs(1))
                .expect("fake API URL should be accepted")
                .with_source_download_max_bytes(64)
                .with_local_file_root(root);

        assert!(matches!(
            api.download_file("file-id", &destination).await,
            Err(TelegramApiError::LocalFileNotRegular)
        ));
        server.await.expect("getFile task should finish");

        let missing_root = directory.join("missing");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("fake API should bind");
        let address = listener.local_addr().expect("fake API address should be available");
        let server = tokio::spawn(serve_file_metadata_at_path(
            listener,
            0,
            candidate.to_string_lossy().into_owned(),
        ));
        let api =
            TeloxideApi::new("test-token", &format!("http://{address}"), Duration::from_secs(1))
                .expect("fake API URL should be accepted")
                .with_source_download_max_bytes(64)
                .with_local_file_root(missing_root);

        assert!(matches!(
            api.download_file("file-id", &destination).await,
            Err(TelegramApiError::LocalFileRootUnavailable)
        ));
        server.await.expect("getFile task should finish");
        assert_no_temporary_downloads(&output).await;
        tokio::fs::remove_dir_all(directory).await.expect("test directory should be removed");
    }

    #[tokio::test]
    async fn cloud_download_limit_rejects_before_streaming_file() {
        const PAYLOAD_SIZE: u64 = 21;
        const REDUCED_CLOUD_CEILING: u64 = 4;
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("fake API should bind");
        let address = listener.local_addr().expect("fake API address should be available");
        let server = tokio::spawn(serve_file_metadata(listener, PAYLOAD_SIZE as usize));
        let destination = std::env::temp_dir().join(format!("sooqa-telegram-{}", Uuid::new_v4()));
        let api =
            TeloxideApi::new("test-token", &format!("http://{address}"), Duration::from_secs(1))
                .expect("fake API URL should be accepted")
                .with_source_download_max_bytes(64)
                .with_test_cloud_limits(REDUCED_CLOUD_CEILING);

        assert!(matches!(
            api.download_file("file-id", &destination).await,
            Err(TelegramApiError::DownloadLimit {
                size: PAYLOAD_SIZE,
                limit: REDUCED_CLOUD_CEILING
            })
        ));
        let request_target = server.await.expect("fake API task should finish");
        assert!(request_target.contains("/bottest-token/GetFile"));
        assert!(tokio::fs::metadata(&destination).await.is_err());
    }

    #[tokio::test]
    async fn media_download_does_not_use_poll_timeout_for_slow_transfer() {
        const PAYLOAD: &[u8] = b"slow transfer";
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("fake API should bind");
        let address = listener.local_addr().expect("fake API address should be available");
        let server =
            tokio::spawn(serve_download(listener, PAYLOAD.to_vec(), Duration::from_secs(6)));
        let directory = std::env::temp_dir().join(format!("sooqa-telegram-{}", Uuid::new_v4()));
        tokio::fs::create_dir(&directory).await.expect("test directory should be created");
        let destination = directory.join("slow.webm");
        let api =
            TeloxideApi::new("test-token", &format!("http://{address}"), Duration::from_millis(1))
                .expect("fake API URL should be accepted")
                .with_source_download_max_bytes(64);

        let result = tokio::time::timeout(
            Duration::from_secs(8),
            api.download_file("file-id", &destination),
        )
        .await
        .expect("media transfer should finish within the test deadline");
        result.expect("media transfer should not inherit the polling timeout");
        assert_eq!(
            tokio::fs::read(&destination).await.expect("download should be readable"),
            PAYLOAD
        );
        server.await.expect("fake API task should finish");
        tokio::fs::remove_dir_all(directory).await.expect("test directory should be removed");
    }

    #[tokio::test]
    async fn local_bot_api_uploads_file_through_configured_endpoint_without_buffering() {
        const PAYLOAD: &[u8] = b"large upload payload";
        const REDUCED_CLOUD_CEILING: u64 = 4;
        const MEDIA_READ_TIMEOUT: Duration = Duration::from_millis(10);
        const UPLOAD_BODY_DELAY: Duration = Duration::from_millis(100);
        assert!(PAYLOAD.len() as u64 > REDUCED_CLOUD_CEILING);

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("fake API should bind");
        let address = listener.local_addr().expect("fake API address should be available");
        let server = tokio::spawn(serve_upload(
            listener,
            UPLOAD_BODY_DELAY,
            b"name=\"supports_streaming\"\r\n\r\ntrue",
        ));
        let directory = std::env::temp_dir().join(format!("sooqa-telegram-{}", Uuid::new_v4()));
        tokio::fs::create_dir(&directory).await.expect("test directory should be created");
        let path = directory.join("canonical.mp4");
        tokio::fs::write(&path, PAYLOAD).await.expect("upload fixture should be written");
        let api = TeloxideApi::new_with_upload_timeout(
            "test-token",
            &format!("http://{address}"),
            Duration::from_secs(1),
            Duration::from_secs(5),
        )
        .expect("fake API URL should be accepted")
        .with_test_media_read_timeout(MEDIA_READ_TIMEOUT)
        .expect("test media client should be configured");

        let started = std::time::Instant::now();
        let result = api
            .upload_media(StorageUploadRequest {
                storage_chat_id: -100123,
                media_kind: MediaKind::Video,
                local_work_path: path,
                caption: "sooqa".to_owned(),
                duration: Some(1),
                width: Some(320),
                height: Some(240),
                thumbnail_path: None,
            })
            .await
            .expect("local Bot API upload should succeed");
        assert!(started.elapsed() >= UPLOAD_BODY_DELAY);
        assert_eq!(result.storage_message_id, 7);
        assert_eq!(result.telegram_file_id, "stored-file-id");
        let request = server.await.expect("fake API task should finish");
        assert!(request.target.contains("/bottest-token/SendVideo"));
        assert!(request.body_bytes > REDUCED_CLOUD_CEILING);
        assert!(request.body_bytes >= PAYLOAD.len() as u64);
        assert!(
            request.body_contains_marker,
            "SendVideo request must include supports_streaming=true"
        );
        tokio::fs::remove_dir_all(directory).await.expect("test directory should be removed");
    }

    #[test]
    fn cloud_bot_api_limit_is_only_applied_to_cloud_endpoint() {
        let cloud =
            TeloxideApi::new("test-token", "https://api.telegram.org", Duration::from_secs(1))
                .expect("cloud Bot API URL should be accepted");
        assert_eq!(cloud.cloud_download_limit_bytes, Some(20 * 1024 * 1024));
        assert_eq!(cloud.cloud_upload_limit_bytes, Some(50 * 1024 * 1024));
        assert_eq!(cloud.source_download_max_bytes, DEFAULT_TELEGRAM_SOURCE_DOWNLOAD_MAX_BYTES);

        let local =
            TeloxideApi::new("test-token", "http://telegram-bot-api:8081", Duration::from_secs(1))
                .expect("Local Bot API URL should be accepted");
        assert_eq!(local.cloud_download_limit_bytes, None);
        assert_eq!(local.cloud_upload_limit_bytes, None);
        assert_eq!(local.source_download_max_bytes, DEFAULT_TELEGRAM_SOURCE_DOWNLOAD_MAX_BYTES);
    }

    #[test]
    fn upload_timeout_is_positive_and_bounded() {
        assert!(matches!(
            TeloxideApi::new_with_upload_timeout(
                "test-token",
                "http://telegram-bot-api:8081",
                Duration::from_secs(1),
                Duration::ZERO,
            ),
            Err(TelegramError::InvalidUploadTimeout)
        ));
        assert!(matches!(
            TeloxideApi::new_with_upload_timeout(
                "test-token",
                "http://telegram-bot-api:8081",
                Duration::from_secs(1),
                TELEGRAM_MAX_UPLOAD_TIMEOUT + Duration::from_secs(1),
            ),
            Err(TelegramError::InvalidUploadTimeout)
        ));
    }
}
