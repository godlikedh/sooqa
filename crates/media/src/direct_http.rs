use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use futures_util::StreamExt;
use reqwest::{
    Client, Response, StatusCode,
    header::{CONTENT_TYPE, LOCATION, RANGE},
};
use serde_json::json;
use tokio::{fs::File, io::AsyncWriteExt, net::lookup_host};
use url::{Host, Url};
use uuid::Uuid;

use crate::publication::{PublishOutcome, TempArtifact, publish_or_reuse};
use crate::{
    DownloadError, DownloadLimits, DownloadedSource, SourceDownloader, SourceInput,
    SourceInspection, SourceMediaKind,
};

const MAX_SNIFF_BYTES: usize = 512;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ResolvedAddress {
    pub ip: IpAddr,
    pub connect_ip: IpAddr,
}

impl ResolvedAddress {
    fn same(ip: IpAddr) -> Self {
        Self { ip, connect_ip: ip }
    }
}

#[async_trait]
pub trait HostResolver: Send + Sync {
    async fn resolve(&self, host: &str, port: u16) -> Result<Vec<ResolvedAddress>, DownloadError>;
}

#[derive(Debug, Default)]
struct SystemResolver;

#[async_trait]
impl HostResolver for SystemResolver {
    async fn resolve(&self, host: &str, port: u16) -> Result<Vec<ResolvedAddress>, DownloadError> {
        let addresses = lookup_host((host, port))
            .await
            .map_err(|error| DownloadError::retryable("dns_resolution", error.to_string()))?
            .map(|address| ResolvedAddress::same(address.ip()))
            .collect::<Vec<_>>();

        if addresses.is_empty() {
            return Err(DownloadError::retryable(
                "dns_resolution",
                "host resolved to no addresses",
            ));
        }

        Ok(addresses)
    }
}

#[derive(Clone)]
pub struct DirectHttpDownloader {
    resolver: Arc<dyn HostResolver>,
    inspection_limits: DownloadLimits,
}

impl DirectHttpDownloader {
    pub fn new(inspection_limits: DownloadLimits) -> Self {
        Self::with_resolver(inspection_limits, Arc::new(SystemResolver))
    }

    pub fn with_resolver(
        inspection_limits: DownloadLimits,
        resolver: Arc<dyn HostResolver>,
    ) -> Self {
        Self { resolver, inspection_limits }
    }

    async fn fetch(
        &self,
        start_url: Url,
        limits: &DownloadLimits,
        inspect_prefix: bool,
    ) -> Result<FetchResult, DownloadError> {
        validate_limits(limits)?;
        let mut current_url = start_url;

        for redirect_number in 0..=limits.max_redirects {
            let target = self.resolve_target(&current_url).await?;
            let client = self.client_for(&target, limits.timeout)?;
            let request = client.get(target.url.clone());
            let request =
                if inspect_prefix { request.header(RANGE, "bytes=0-511") } else { request };
            let response = request.send().await.map_err(map_request_error)?;

            if response.status().is_redirection() {
                if redirect_number >= limits.max_redirects {
                    return Err(DownloadError::terminal(
                        "redirect_limit",
                        "source exceeded the configured redirect limit",
                    ));
                }

                let next_url = response
                    .headers()
                    .get(LOCATION)
                    .ok_or_else(|| {
                        DownloadError::terminal(
                            "invalid_redirect",
                            "redirect response did not include a Location header",
                        )
                    })
                    .and_then(|location| {
                        let location = location.to_str().map_err(|_| {
                            DownloadError::terminal(
                                "invalid_redirect",
                                "redirect Location header was not valid UTF-8",
                            )
                        })?;
                        current_url.join(location).map_err(|_| {
                            DownloadError::terminal(
                                "invalid_redirect",
                                "redirect Location header was not a valid URL",
                            )
                        })
                    })?;
                current_url = next_url;
                continue;
            }

            return classify_response(response, current_url);
        }

        Err(DownloadError::terminal(
            "redirect_limit",
            "source exceeded the configured redirect limit",
        ))
    }

    async fn resolve_target(&self, url: &Url) -> Result<ResolvedTarget, DownloadError> {
        validate_url(url)?;
        let port = url.port_or_known_default().ok_or_else(|| {
            DownloadError::terminal("missing_source_port", "source URL has no usable port")
        })?;

        let (host, is_domain, addresses) = match url.host() {
            Some(Host::Domain(domain)) => {
                let addresses = self.resolver.resolve(domain, port).await?;
                (domain.to_owned(), true, addresses)
            }
            Some(Host::Ipv4(address)) => {
                let ip = IpAddr::V4(address);
                (address.to_string(), false, vec![ResolvedAddress::same(ip)])
            }
            Some(Host::Ipv6(address)) => {
                let ip = IpAddr::V6(address);
                (address.to_string(), false, vec![ResolvedAddress::same(ip)])
            }
            None => {
                return Err(DownloadError::terminal(
                    "missing_source_host",
                    "source URL has no host",
                ));
            }
        };

        if addresses.is_empty() {
            return Err(DownloadError::retryable(
                "dns_resolution",
                "host resolved to no addresses",
            ));
        }

        if let Some(blocked) = addresses.iter().find(|address| is_blocked_ip(address.ip)) {
            return Err(DownloadError::terminal(
                "ssrf_blocked",
                format!("source resolved to a forbidden address: {}", blocked.ip),
            ));
        }

        Ok(ResolvedTarget {
            url: url.clone(),
            host,
            is_domain,
            port,
            connect_ip: addresses[0].connect_ip,
        })
    }

    fn client_for(
        &self,
        target: &ResolvedTarget,
        timeout: Duration,
    ) -> Result<Client, DownloadError> {
        let mut builder = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .timeout(timeout)
            .connect_timeout(timeout)
            .read_timeout(timeout);
        if target.is_domain {
            builder =
                builder.resolve(&target.host, SocketAddr::new(target.connect_ip, target.port));
        }
        builder.build().map_err(|error| {
            DownloadError::terminal("http_client", format!("could not build HTTP client: {error}"))
        })
    }
}

#[async_trait]
impl SourceDownloader for DirectHttpDownloader {
    async fn inspect(&self, source: &SourceInput) -> Result<SourceInspection, DownloadError> {
        let source_url = parse_url(&source.source_url)?;
        let fetched = self.fetch(source_url, &self.inspection_limits, true).await?;
        let mut response = fetched.response;
        let content_length = response.content_length();
        if content_length.is_some_and(|length| length > self.inspection_limits.max_bytes) {
            return Err(DownloadError::terminal(
                "source_too_large",
                "source content length exceeds the configured byte limit",
            ));
        }

        let mime_type = content_type(&response);
        let prefix = response
            .chunk()
            .await
            .map_err(map_stream_error)?
            .map(|chunk| chunk[..chunk.len().min(MAX_SNIFF_BYTES)].to_vec())
            .unwrap_or_default();
        let media_kind = sniff_media_kind(mime_type.as_deref(), &prefix);

        Ok(SourceInspection {
            adapter: "direct_http".to_owned(),
            source_url: source.source_url.clone(),
            resolved_url: Some(fetched.resolved_url.to_string()),
            media_kind,
            mime_type,
            content_length_bytes: content_length,
            title: None,
            metadata: json!({
                "http_status": response.status().as_u16(),
                "sniffed_media_kind": media_kind,
            }),
        })
    }

    async fn download(
        &self,
        inspection: &SourceInspection,
        destination: &Path,
        limits: &DownloadLimits,
    ) -> Result<DownloadedSource, DownloadError> {
        validate_limits(limits)?;
        let start_url = inspection.resolved_url.as_deref().unwrap_or(&inspection.source_url);
        let fetched = self.fetch(parse_url(start_url)?, limits, false).await?;
        let response = fetched.response;
        if response.content_length().is_some_and(|length| length > limits.max_bytes) {
            return Err(DownloadError::terminal(
                "source_too_large",
                "source content length exceeds the configured byte limit",
            ));
        }

        let mime_type = content_type(&response);
        let temporary = destination.with_file_name(format!(".sooqa-http-{}.tmp", Uuid::new_v4()));
        let mut temporary = TempArtifact::reserve(temporary).await.map_err(|error| {
            DownloadError::terminal(
                "destination_io",
                format!("could not reserve temporary destination: {error}"),
            )
        })?;
        let file = File::options().write(true).open(temporary.path()).await.map_err(|error| {
            DownloadError::terminal(
                "destination_io",
                format!("could not open temporary destination: {error}"),
            )
        })?;
        let result = stream_to_file(response, file, limits.max_bytes).await;
        let bytes = match result {
            Ok(bytes) => bytes,
            Err(error) => {
                return Err(error);
            }
        };
        let published = publish_or_reuse(temporary.path(), destination).await.map_err(|error| {
            DownloadError::terminal(
                "destination_io",
                format!("could not publish downloaded source: {error}"),
            )
        })?;
        let bytes = match published {
            PublishOutcome::Published => bytes,
            PublishOutcome::Reused => tokio::fs::metadata(destination)
                .await
                .map_err(|error| {
                    DownloadError::terminal(
                        "destination_io",
                        format!("could not inspect reused source: {error}"),
                    )
                })?
                .len(),
        };
        temporary.remove().await;

        Ok(DownloadedSource {
            path: PathBuf::from(destination),
            bytes,
            mime_type,
            selected_format: None,
        })
    }
}

async fn stream_to_file(
    response: Response,
    mut file: File,
    max_bytes: u64,
) -> Result<u64, DownloadError> {
    let mut stream = response.bytes_stream();
    let mut bytes = 0_u64;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(map_stream_error)?;
        bytes = bytes.checked_add(chunk.len() as u64).ok_or_else(|| {
            DownloadError::terminal("source_too_large", "source byte count overflowed")
        })?;
        if bytes > max_bytes {
            return Err(DownloadError::terminal(
                "source_too_large",
                "source body exceeds the configured byte limit",
            ));
        }
        file.write_all(&chunk).await.map_err(|error| {
            DownloadError::terminal(
                "destination_io",
                format!("could not write destination: {error}"),
            )
        })?;
    }
    file.flush().await.map_err(|error| {
        DownloadError::terminal("destination_io", format!("could not flush destination: {error}"))
    })?;

    Ok(bytes)
}

#[derive(Debug)]
struct ResolvedTarget {
    url: Url,
    host: String,
    is_domain: bool,
    port: u16,
    connect_ip: IpAddr,
}

struct FetchResult {
    response: Response,
    resolved_url: Url,
}

fn validate_limits(limits: &DownloadLimits) -> Result<(), DownloadError> {
    if limits.max_bytes == 0 || limits.timeout.is_zero() {
        return Err(DownloadError::terminal(
            "invalid_download_limits",
            "download byte limit and timeout must be greater than zero",
        ));
    }
    Ok(())
}

fn parse_url(value: &str) -> Result<Url, DownloadError> {
    Url::parse(value).map_err(|_| {
        DownloadError::terminal("invalid_source_url", "source URL could not be parsed")
    })
}

fn validate_url(url: &Url) -> Result<(), DownloadError> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(DownloadError::terminal(
            "unsupported_scheme",
            "source URL must use http or https",
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(DownloadError::terminal(
            "source_credentials_forbidden",
            "source URL must not contain embedded credentials",
        ));
    }
    if url.host().is_none() {
        return Err(DownloadError::terminal("missing_source_host", "source URL has no host"));
    }
    Ok(())
}

fn classify_response(response: Response, resolved_url: Url) -> Result<FetchResult, DownloadError> {
    let status = response.status();
    if status.is_success() {
        return Ok(FetchResult { response, resolved_url });
    }

    let message = format!("source returned HTTP status {}", status.as_u16());
    if status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
    {
        Err(DownloadError::retryable("upstream_http_status", message))
    } else {
        Err(DownloadError::terminal("upstream_http_status", message))
    }
}

fn map_request_error(error: reqwest::Error) -> DownloadError {
    if error.is_timeout() || error.is_connect() {
        DownloadError::retryable("http_request", error.to_string())
    } else {
        DownloadError::terminal("http_request", error.to_string())
    }
}

fn map_stream_error(error: reqwest::Error) -> DownloadError {
    if error.is_timeout() || error.is_connect() {
        DownloadError::retryable("http_stream", error.to_string())
    } else {
        DownloadError::terminal("http_stream", error.to_string())
    }
}

fn content_type(response: &Response) -> Option<String> {
    response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.split(';').next().unwrap_or_default().trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
}

fn sniff_media_kind(mime_type: Option<&str>, prefix: &[u8]) -> SourceMediaKind {
    if let Some(mime_type) = mime_type {
        if mime_type.starts_with("video/") {
            return SourceMediaKind::Video;
        }
        if mime_type.starts_with("image/") {
            return SourceMediaKind::Image;
        }
        if mime_type.starts_with("audio/") {
            return SourceMediaKind::Audio;
        }
    }

    if prefix.starts_with(&[0xff, 0xd8, 0xff])
        || prefix.starts_with(b"\x89PNG\r\n\x1a\n")
        || prefix.starts_with(b"GIF87a")
        || prefix.starts_with(b"GIF89a")
        || (prefix.len() >= 12 && prefix.starts_with(b"RIFF") && &prefix[8..12] == b"WEBP")
    {
        return SourceMediaKind::Image;
    }
    if prefix.len() >= 8 && &prefix[4..8] == b"ftyp"
        || prefix.starts_with(&[0x1a, 0x45, 0xdf, 0xa3])
    {
        return SourceMediaKind::Video;
    }
    if prefix.starts_with(b"ID3")
        || prefix.starts_with(b"OggS")
        || prefix.starts_with(&[0xff, 0xfb])
    {
        return SourceMediaKind::Audio;
    }
    SourceMediaKind::Unknown
}

fn is_blocked_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_blocked_ipv4(ip),
        IpAddr::V6(ip) => is_blocked_ipv6(ip),
    }
}

fn is_blocked_ipv4(ip: Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    a == 0
        || a == 10
        || (a == 100 && (64..=127).contains(&b))
        || (a == 127)
        || (a == 169 && b == 254)
        || (a == 172 && (16..=31).contains(&b))
        || (a == 192 && b == 0)
        || (a == 192 && b == 168)
        || (a == 198 && (18..=19).contains(&b))
        || (a == 198 && b == 51 && c == 100)
        || (a == 203 && b == 0 && c == 113)
        || a >= 224
}

fn is_blocked_ipv6(ip: Ipv6Addr) -> bool {
    if ip.is_unspecified() || ip.is_loopback() || ip.is_multicast() {
        return true;
    }

    let segments = ip.segments();
    if (segments[0] & 0xfe00) == 0xfc00
        || (segments[0] & 0xffc0) == 0xfe80
        || (segments[0] & 0xffc0) == 0xfec0
        || (segments[0] == 0x2001 && segments[1] == 0x0db8)
    {
        return true;
    }

    if segments[..5].iter().all(|segment| *segment == 0) && segments[5] == 0xffff {
        return is_blocked_ipv4(Ipv4Addr::new(
            (segments[6] >> 8) as u8,
            segments[6] as u8,
            (segments[7] >> 8) as u8,
            segments[7] as u8,
        ));
    }

    false
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        net::{IpAddr, Ipv4Addr},
        sync::Arc,
    };

    use async_trait::async_trait;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use uuid::Uuid;

    use super::*;

    #[derive(Clone)]
    struct StaticResolver {
        addresses: HashMap<String, Vec<ResolvedAddress>>,
    }

    #[async_trait]
    impl HostResolver for StaticResolver {
        async fn resolve(
            &self,
            host: &str,
            _port: u16,
        ) -> Result<Vec<ResolvedAddress>, DownloadError> {
            self.addresses.get(host).cloned().ok_or_else(|| {
                DownloadError::terminal("dns_resolution", "test resolver has no such host")
            })
        }
    }

    async fn spawn_server(responses: Vec<Vec<u8>>) -> std::net::SocketAddr {
        let listener =
            TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.expect("test server should bind");
        let address = listener.local_addr().expect("test server address should be available");
        tokio::spawn(async move {
            for response in responses {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let mut request = [0_u8; 2048];
                let _ = stream.read(&mut request).await;
                let _ = stream.write_all(&response).await;
            }
        });
        address
    }

    fn response(status: &str, extra_headers: &str, body: &[u8]) -> Vec<u8> {
        let header =
            format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\n{extra_headers}\r\n", body.len());
        let mut response = header.into_bytes();
        response.extend_from_slice(body);
        response
    }

    fn response_without_length(status: &str, extra_headers: &str, body: &[u8]) -> Vec<u8> {
        let mut response =
            format!("HTTP/1.1 {status}\r\n{extra_headers}Connection: close\r\n\r\n").into_bytes();
        response.extend_from_slice(body);
        response
    }

    fn resolver_for(address: std::net::SocketAddr) -> StaticResolver {
        StaticResolver {
            addresses: HashMap::from([
                (
                    "media.test".to_owned(),
                    vec![ResolvedAddress {
                        ip: IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34)),
                        connect_ip: address.ip(),
                    }],
                ),
                (
                    "private.test".to_owned(),
                    vec![ResolvedAddress::same(IpAddr::V4(Ipv4Addr::LOCALHOST))],
                ),
            ]),
        }
    }

    fn source(url: String) -> SourceInput {
        SourceInput { ingest_request_id: Uuid::new_v4(), source_url: url, page_url: None }
    }

    #[tokio::test]
    async fn inspects_and_streams_a_direct_media_response() {
        let body = b"\x00\x00\x00\x18ftypmp42media";
        let response = response("200 OK", "Content-Type: application/octet-stream\r\n", body);
        let address = spawn_server(vec![response.clone(), response.clone(), response]).await;
        let limits =
            DownloadLimits { max_bytes: 1024, max_redirects: 2, timeout: Duration::from_secs(5) };
        let downloader =
            DirectHttpDownloader::with_resolver(limits, Arc::new(resolver_for(address)));
        let source_url = format!("http://media.test:{}/video", address.port());

        let inspection = downloader
            .inspect(&source(source_url.clone()))
            .await
            .expect("inspection should succeed");
        assert_eq!(inspection.adapter, "direct_http");
        assert_eq!(inspection.media_kind, SourceMediaKind::Video);
        assert_eq!(inspection.content_length_bytes, Some(body.len() as u64));
        assert_eq!(inspection.resolved_url.as_deref(), Some(source_url.as_str()));

        let destination =
            std::env::temp_dir().join(format!("sooqa-direct-http-{}.bin", Uuid::new_v4()));
        let downloaded = downloader
            .download(&inspection, &destination, &limits)
            .await
            .expect("download should succeed");
        assert_eq!(downloaded.bytes, body.len() as u64);
        assert_eq!(tokio::fs::read(&destination).await.expect("download should be readable"), body);
        let replayed = downloader
            .download(&inspection, &destination, &limits)
            .await
            .expect("retry should reuse the validated published output");
        assert_eq!(replayed.bytes, downloaded.bytes);
        assert_eq!(
            tokio::fs::read(&destination).await.expect("reused output should be readable"),
            body
        );
        tokio::fs::remove_file(destination).await.expect("test file should be removable");
    }

    #[tokio::test]
    async fn rejects_a_private_redirect_before_following_it() {
        let redirect = response("302 Found", "Location: http://private.test/secret\r\n", &[]);
        let address = spawn_server(vec![redirect]).await;
        let limits =
            DownloadLimits { max_bytes: 1024, max_redirects: 2, timeout: Duration::from_secs(5) };
        let downloader =
            DirectHttpDownloader::with_resolver(limits, Arc::new(resolver_for(address)));
        let source_url = format!("http://media.test:{}/redirect", address.port());

        let error =
            downloader.inspect(&source(source_url)).await.expect_err("redirect should be blocked");
        assert!(matches!(error, DownloadError::Terminal { class, .. } if class == "ssrf_blocked"));
    }

    #[tokio::test]
    async fn rejects_a_response_over_the_byte_limit() {
        let body = b"too large";
        let response = response("200 OK", "Content-Type: video/mp4\r\n", body);
        let address = spawn_server(vec![response]).await;
        let limits =
            DownloadLimits { max_bytes: 4, max_redirects: 0, timeout: Duration::from_secs(5) };
        let downloader =
            DirectHttpDownloader::with_resolver(limits, Arc::new(resolver_for(address)));
        let source_url = format!("http://media.test:{}/large", address.port());

        let error =
            downloader.inspect(&source(source_url)).await.expect_err("large response should fail");
        assert!(
            matches!(error, DownloadError::Terminal { class, .. } if class == "source_too_large")
        );
    }

    #[tokio::test]
    async fn enforces_the_byte_limit_while_streaming_without_content_length() {
        let body = b"stream is too large";
        let response = response_without_length("200 OK", "Content-Type: video/mp4\r\n", body);
        let address = spawn_server(vec![response]).await;
        let limits =
            DownloadLimits { max_bytes: 4, max_redirects: 0, timeout: Duration::from_secs(5) };
        let downloader =
            DirectHttpDownloader::with_resolver(limits, Arc::new(resolver_for(address)));
        let source_url = format!("http://media.test:{}/stream", address.port());
        let inspection = SourceInspection {
            adapter: "direct_http".to_owned(),
            source_url: source_url.clone(),
            resolved_url: None,
            media_kind: SourceMediaKind::Video,
            mime_type: Some("video/mp4".to_owned()),
            content_length_bytes: None,
            title: None,
            metadata: serde_json::Value::Null,
        };
        let destination =
            std::env::temp_dir().join(format!("sooqa-direct-http-{}.bin", Uuid::new_v4()));

        let error = downloader
            .download(&inspection, &destination, &limits)
            .await
            .expect_err("stream over the limit should fail");
        assert!(
            matches!(error, DownloadError::Terminal { class, .. } if class == "source_too_large")
        );
        assert!(!destination.exists(), "partial downloads should be removed");
    }

    #[tokio::test]
    async fn dropped_download_removes_the_http_temporary_artifact() {
        let listener =
            TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.expect("test server should bind");
        let address = listener.local_addr().expect("test server address should be available");
        let server = tokio::spawn(async move {
            let Ok((mut stream, _)) = listener.accept().await else { return };
            let mut request = [0_u8; 2048];
            let _ = stream.read(&mut request).await;
            let _ = stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100000\r\nContent-Type: video/mp4\r\n\r\npartial")
                .await;
            tokio::time::sleep(Duration::from_secs(10)).await;
        });
        let root = std::env::temp_dir().join(format!("sooqa-direct-cancel-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&root).await.expect("test root should be created");
        let destination = root.join("final.bin");
        let limits = DownloadLimits {
            max_bytes: 1024 * 1024,
            max_redirects: 0,
            timeout: Duration::from_secs(30),
        };
        let downloader = DirectHttpDownloader::with_resolver(
            limits,
            Arc::new(resolver_for((address.ip(), address.port()).into())),
        );
        let inspection = SourceInspection {
            adapter: "direct_http".to_owned(),
            source_url: format!("http://media.test:{}/cancel", address.port()),
            resolved_url: None,
            media_kind: SourceMediaKind::Video,
            mime_type: Some("video/mp4".to_owned()),
            content_length_bytes: None,
            title: None,
            metadata: serde_json::Value::Null,
        };
        let download =
            tokio::spawn(
                async move { downloader.download(&inspection, &destination, &limits).await },
            );
        tokio::time::sleep(Duration::from_millis(250)).await;
        download.abort();
        let _ = download.await;
        server.abort();

        let mut entries = tokio::fs::read_dir(&root).await.expect("test root should be readable");
        while let Some(entry) = entries.next_entry().await.expect("directory should be readable") {
            let name = entry.file_name().to_string_lossy().into_owned();
            assert!(!name.starts_with(".sooqa-http-"), "HTTP temporary output was left behind");
        }
        tokio::fs::remove_dir_all(root).await.expect("test root should be removed");
    }

    #[test]
    fn blocks_private_and_special_ip_ranges() {
        for ip in [
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254)),
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
            IpAddr::V6("::1".parse().expect("IPv6 literal should parse")),
            IpAddr::V6("fc00::1".parse().expect("IPv6 literal should parse")),
            IpAddr::V6("fe80::1".parse().expect("IPv6 literal should parse")),
        ] {
            assert!(is_blocked_ip(ip), "{ip} should be blocked");
        }
        assert!(!is_blocked_ip(IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34))));
    }
}
