use std::path::Path;

use async_trait::async_trait;
use serde_json::{Value, json};
use url::Url;

use crate::{
    DirectHttpDownloader, DownloadError, DownloadLimits, DownloadedSource, SourceDownloader,
    SourceInput, SourceInspection, SourceMediaKind,
};

/// Official 2ch host aliases are deliberately kept local to this adapter.
/// Generic direct HTTP sources must not receive arbitrary host substitution.
pub const TWO_CH_MIRROR_HOSTS: [&str; 3] = ["2ch.org", "2ch.su", "2ch.life"];

/// Adds the narrowly-scoped official 2ch mirror policy to the existing
/// direct HTTP adapter. Every candidate is still inspected by DirectHttp,
/// which owns URL validation, DNS resolution, SSRF checks, redirects, and
/// response/byte limits.
#[derive(Clone)]
pub struct TwoChMirrorDownloader {
    direct_http: DirectHttpDownloader,
}

impl TwoChMirrorDownloader {
    pub fn new(direct_http: DirectHttpDownloader) -> Self {
        Self { direct_http }
    }
}

#[async_trait]
impl SourceDownloader for TwoChMirrorDownloader {
    async fn inspect(&self, source: &SourceInput) -> Result<SourceInspection, DownloadError> {
        let Some((source_url, submitted_host)) = mirror_source(source) else {
            return self.direct_http.inspect(source).await;
        };

        let mut failures = Vec::new();
        let mut all_failures_retryable = true;
        for selected_host in TWO_CH_MIRROR_HOSTS {
            let candidate_url = candidate_url(&source_url, selected_host);
            let candidate = SourceInput {
                ingest_request_id: source.ingest_request_id,
                source_url: candidate_url.to_string(),
                page_url: source.page_url.clone(),
            };

            match self.direct_http.inspect(&candidate).await {
                Ok(inspection) if inspection.media_kind != SourceMediaKind::Unknown => {
                    return Ok(with_mirror_provenance(
                        inspection,
                        &source.source_url,
                        &submitted_host,
                        selected_host,
                        &candidate_url,
                    ));
                }
                Ok(_) => {
                    return Err(DownloadError::terminal(
                        "unsupported_source",
                        "2ch mirror returned an unsupported media response",
                    ));
                }
                Err(error) if should_try_next_mirror(&error) => {
                    all_failures_retryable &= error.is_retryable();
                    failures.push((selected_host, failure_summary(&error)));
                }
                Err(error) => return Err(error),
            }
        }

        let attempts = failures
            .iter()
            .map(|(host, failure)| format!("{host}={failure}"))
            .collect::<Vec<_>>()
            .join(", ");
        let message = format!("all 2ch mirrors failed ({attempts})");
        if all_failures_retryable {
            Err(DownloadError::retryable("two_ch_mirrors_exhausted", message))
        } else {
            Err(DownloadError::terminal("two_ch_mirrors_exhausted", message))
        }
    }

    async fn download(
        &self,
        inspection: &SourceInspection,
        destination: &Path,
        limits: &DownloadLimits,
    ) -> Result<DownloadedSource, DownloadError> {
        // The inspection contains the selected mirror's resolved URL, so the
        // existing direct adapter reuses it instead of trying mirror order a
        // second time during the download stage.
        self.direct_http.download(inspection, destination, limits).await
    }
}

fn mirror_source(source: &SourceInput) -> Option<(Url, String)> {
    let url = Url::parse(&source.source_url).ok()?;
    let host = url.host_str()?.to_ascii_lowercase();
    TWO_CH_MIRROR_HOSTS.contains(&host.as_str()).then_some((url, host))
}

fn candidate_url(source_url: &Url, host: &str) -> Url {
    let mut candidate = source_url.clone();
    candidate.set_host(Some(host)).expect("2ch mirror host must be valid");
    candidate
}

fn with_mirror_provenance(
    mut inspection: SourceInspection,
    original_source_url: &str,
    submitted_host: &str,
    selected_host: &str,
    selected_url: &Url,
) -> SourceInspection {
    inspection.source_url = original_source_url.to_owned();
    let mirror = json!({
        "submitted_host": submitted_host,
        "selected_host": selected_host,
        "selected_url": selected_url.as_str(),
    });
    let metadata = match std::mem::take(&mut inspection.metadata) {
        Value::Object(mut object) => {
            object.insert("two_ch_mirror".to_owned(), mirror);
            Value::Object(object)
        }
        existing => json!({
            "adapter_metadata": existing,
            "two_ch_mirror": mirror,
        }),
    };
    inspection.metadata = metadata;
    inspection
}

fn should_try_next_mirror(error: &DownloadError) -> bool {
    match error.class() {
        "dns_resolution" | "upstream_http_status" => true,
        "http_request" | "http_stream" => error.is_retryable(),
        _ => false,
    }
}

fn failure_summary(error: &DownloadError) -> String {
    if error.class() == "upstream_http_status" {
        let status = error
            .to_string()
            .strip_prefix("source returned HTTP status ")
            .and_then(|value| value.parse::<u16>().ok());
        if let Some(status) = status {
            return format!("upstream_http_status:{status}");
        }
    }
    error.class().to_owned()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::Arc,
        time::Duration,
    };

    use async_trait::async_trait;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::Mutex,
    };
    use uuid::Uuid;

    use crate::{HostResolver, ResolvedAddress};

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
                DownloadError::retryable("dns_resolution", "test resolver has no such host")
            })
        }
    }

    async fn spawn_server(responses: Vec<Vec<u8>>) -> (SocketAddr, Arc<Mutex<Vec<String>>>) {
        let listener =
            TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.expect("test server should bind");
        let address = listener.local_addr().expect("test server address should be available");
        let hosts = Arc::new(Mutex::new(Vec::new()));
        let recorded_hosts = Arc::clone(&hosts);
        tokio::spawn(async move {
            for response in responses {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let mut request = [0_u8; 4096];
                let bytes_read = stream.read(&mut request).await.unwrap_or_default();
                let request = String::from_utf8_lossy(&request[..bytes_read]);
                let host = request
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("host").then(|| value.trim().to_owned())
                    })
                    .unwrap_or_default();
                recorded_hosts.lock().await.push(host);
                let _ = stream.write_all(&response).await;
            }
        });
        (address, hosts)
    }

    fn response(status: &str, extra_headers: &str, body: &[u8]) -> Vec<u8> {
        let header =
            format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\n{extra_headers}\r\n", body.len());
        let mut response = header.into_bytes();
        response.extend_from_slice(body);
        response
    }

    fn source(url: String, page_url: Option<&str>) -> SourceInput {
        SourceInput {
            ingest_request_id: Uuid::from_u128(1),
            source_url: url,
            page_url: page_url.map(ToOwned::to_owned),
        }
    }

    fn resolver_for(
        address: SocketAddr,
        hosts: impl IntoIterator<Item = &'static str>,
    ) -> StaticResolver {
        let addresses = hosts
            .into_iter()
            .map(|host| {
                (
                    host.to_owned(),
                    vec![ResolvedAddress {
                        ip: IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34)),
                        connect_ip: address.ip(),
                    }],
                )
            })
            .collect();
        StaticResolver { addresses }
    }

    fn downloader(address: SocketAddr) -> TwoChMirrorDownloader {
        let limits =
            DownloadLimits { max_bytes: 1024, max_redirects: 2, timeout: Duration::from_secs(5) };
        TwoChMirrorDownloader::new(DirectHttpDownloader::with_resolver(
            limits,
            Arc::new(resolver_for(address, TWO_CH_MIRROR_HOSTS)),
        ))
    }

    fn media_response(status: &str) -> Vec<u8> {
        response(
            status,
            "Content-Type: application/octet-stream\r\n",
            b"\x00\x00\x00\x18ftypmp42media",
        )
    }

    #[tokio::test]
    async fn first_mirror_succeeds_and_preserves_original_url() {
        let (address, hosts) = spawn_server(vec![media_response("200 OK")]).await;
        let downloader = downloader(address);
        let original_url = format!("http://2ch.su:{}/b/src/clip.webm?download=1", address.port());
        let page_url = "https://2ch.su/b/res/123";

        let inspection = downloader
            .inspect(&source(original_url.clone(), Some(page_url)))
            .await
            .expect("first mirror should succeed");

        let expected_selected_url =
            format!("http://2ch.org:{}/b/src/clip.webm?download=1", address.port());
        assert_eq!(inspection.source_url, original_url);
        assert_eq!(inspection.resolved_url.as_deref(), Some(expected_selected_url.as_str()));
        assert_eq!(inspection.metadata["two_ch_mirror"]["submitted_host"], "2ch.su");
        assert_eq!(inspection.metadata["two_ch_mirror"]["selected_host"], "2ch.org");
        assert_eq!(hosts.lock().await.as_slice(), [format!("2ch.org:{}", address.port())]);
    }

    #[tokio::test]
    async fn failed_first_mirror_uses_second_and_reuses_it_for_download() {
        let body = b"\x00\x00\x00\x18ftypmp42media";
        let responses = vec![
            media_response("403 Forbidden"),
            media_response("200 OK"),
            response("200 OK", "Content-Type: video/webm\r\n", body),
        ];
        let (address, hosts) = spawn_server(responses).await;
        let downloader = downloader(address);
        let original_url = format!("http://2ch.life:{}/b/src/clip.webm", address.port());
        let inspection = downloader
            .inspect(&source(original_url.clone(), None))
            .await
            .expect("second mirror should succeed");

        let expected_selected_url = format!("http://2ch.su:{}/b/src/clip.webm", address.port());
        assert_eq!(inspection.source_url, original_url);
        assert_eq!(inspection.resolved_url.as_deref(), Some(expected_selected_url.as_str()));

        let destination = std::env::temp_dir().join(format!("sooqa-2ch-{}.webm", Uuid::new_v4()));
        downloader
            .download(&inspection, &destination, &DownloadLimits::default())
            .await
            .expect("selected mirror should be downloaded");
        assert_eq!(tokio::fs::read(&destination).await.expect("download should be readable"), body);
        tokio::fs::remove_file(destination).await.expect("test file should be removable");

        assert_eq!(
            hosts.lock().await.as_slice(),
            [
                format!("2ch.org:{}", address.port()),
                format!("2ch.su:{}", address.port()),
                format!("2ch.su:{}", address.port()),
            ]
        );
    }

    #[tokio::test]
    async fn failed_first_two_mirrors_use_third() {
        let (address, hosts) = spawn_server(vec![
            media_response("503 Service Unavailable"),
            media_response("403 Forbidden"),
            media_response("200 OK"),
        ])
        .await;
        let downloader = downloader(address);
        let inspection = downloader
            .inspect(&source(format!("http://2ch.org:{}/b/src/clip.webm", address.port()), None))
            .await
            .expect("third mirror should succeed");

        assert_eq!(inspection.metadata["two_ch_mirror"]["selected_host"], "2ch.life");
        assert_eq!(
            hosts.lock().await.as_slice(),
            [
                format!("2ch.org:{}", address.port()),
                format!("2ch.su:{}", address.port()),
                format!("2ch.life:{}", address.port()),
            ]
        );
    }

    #[tokio::test]
    async fn all_mirrors_fail_with_bounded_host_and_status_diagnostic() {
        let (address, _) = spawn_server(vec![
            media_response("403 Forbidden"),
            media_response("502 Bad Gateway"),
            media_response("503 Service Unavailable"),
        ])
        .await;
        let downloader = downloader(address);
        let error = downloader
            .inspect(&source(format!("http://2ch.org:{}/b/src/clip.webm", address.port()), None))
            .await
            .expect_err("all mirrors should fail");

        assert!(matches!(
            error,
            DownloadError::Terminal { class, message }
                if class == "two_ch_mirrors_exhausted"
                    && message == "all 2ch mirrors failed (2ch.org=upstream_http_status:403, 2ch.su=upstream_http_status:502, 2ch.life=upstream_http_status:503)"
        ));
    }

    #[tokio::test]
    async fn content_policy_failure_does_not_fall_through_to_another_mirror() {
        let (address, hosts) = spawn_server(vec![response(
            "200 OK",
            "Content-Type: application/octet-stream\r\n",
            b"not media",
        )])
        .await;
        let downloader = downloader(address);
        let error = downloader
            .inspect(&source(format!("http://2ch.org:{}/b/src/clip.webm", address.port()), None))
            .await
            .expect_err("unsupported media should be terminal");

        assert!(matches!(
            error,
            DownloadError::Terminal { class, .. } if class == "unsupported_source"
        ));
        assert_eq!(hosts.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn dns_failure_can_fall_through_to_the_next_mirror() {
        let (address, hosts) = spawn_server(vec![media_response("200 OK")]).await;
        let limits =
            DownloadLimits { max_bytes: 1024, max_redirects: 2, timeout: Duration::from_secs(5) };
        let resolver = resolver_for(address, ["2ch.su", "2ch.life"]);
        let downloader = TwoChMirrorDownloader::new(DirectHttpDownloader::with_resolver(
            limits,
            Arc::new(resolver),
        ));

        downloader
            .inspect(&source(format!("http://2ch.org:{}/b/src/clip.webm", address.port()), None))
            .await
            .expect("DNS failure for first mirror should fall through");
        assert_eq!(hosts.lock().await.as_slice(), [format!("2ch.su:{}", address.port())]);
    }

    #[tokio::test]
    async fn non_2ch_sources_delegate_without_host_rewriting() {
        let (address, hosts) = spawn_server(vec![media_response("200 OK")]).await;
        let limits =
            DownloadLimits { max_bytes: 1024, max_redirects: 2, timeout: Duration::from_secs(5) };
        let resolver = resolver_for(address, ["media.test"]);
        let downloader = TwoChMirrorDownloader::new(DirectHttpDownloader::with_resolver(
            limits,
            Arc::new(resolver),
        ));
        let source_url = format!("http://media.test:{}/video.webm", address.port());
        let inspection = downloader
            .inspect(&source(source_url.clone(), None))
            .await
            .expect("non-2ch direct source should succeed");

        assert_eq!(inspection.source_url, source_url);
        assert!(inspection.metadata.get("two_ch_mirror").is_none());
        assert_eq!(hosts.lock().await.as_slice(), [format!("media.test:{}", address.port())]);
    }

    #[test]
    fn only_exact_official_hosts_are_mirrored() {
        let nested_host = source("https://media.2ch.org/b/src/clip.webm".to_owned(), None);
        assert!(mirror_source(&nested_host).is_none());
        let suffix_host = source("https://2ch.org.evil.test/b/src/clip.webm".to_owned(), None);
        assert!(mirror_source(&suffix_host).is_none());
        let uppercase_host = source("https://2CH.ORG/b/src/clip.webm".to_owned(), None);
        assert_eq!(mirror_source(&uppercase_host).expect("host should be matched").1, "2ch.org");
    }
}
