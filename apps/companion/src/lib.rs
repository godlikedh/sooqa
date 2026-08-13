//! Small, credential-isolating localhost capture proxy.

use std::{
    collections::VecDeque,
    io::Read,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sooqa_config::CompanionConfig;
use subtle::ConstantTimeEq;
use thiserror::Error;
use tiny_http::{Header, Method, Request, Response, Server, StatusCode};
use tracing::debug;
use url::Url;

const SUBMIT_PATH: &str = "/v1/submit";
const BACKEND_INGEST_PATH: &str = "/api/v1/ingests";
const MAX_ACTION_ID_CHARS: usize = 128;
const MAX_URL_CHARS: usize = 4_096;
const MAX_PAGE_TITLE_CHARS: usize = 512;
const MAX_DESCRIPTION_CHARS: usize = 2_048;
const MAX_REQUESTED_PUBLISH_AT_CHARS: usize = 64;
// Keep this aligned with sooqa-publisher::MAX_CAPTION_LENGTH.
const MAX_POST_CAPTION_CHARS: usize = 1_024;
const MAX_TAGS: usize = 32;
const MAX_TAG_CHARS: usize = 64;
const RATE_LIMIT_REQUESTS: usize = 60;
const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(60);
const RECEIVE_POLL: Duration = Duration::from_millis(200);

#[derive(Debug, Clone, Copy, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestedAction {
    #[default]
    Save,
    Queue,
    PostNow,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CompanionSubmission {
    pub action_id: String,
    pub url: String,
    #[serde(default)]
    pub page_url: Option<String>,
    #[serde(default)]
    pub page_title: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_requested_action")]
    pub requested_action: Option<RequestedAction>,
    #[serde(default)]
    pub requested_publish_at: Option<String>,
    #[serde(default)]
    pub requested_post_caption: Option<String>,
}

fn deserialize_requested_action<'de, D>(
    deserializer: D,
) -> Result<Option<RequestedAction>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<RequestedAction>::deserialize(deserializer)?
        .map(Some)
        .ok_or_else(|| serde::de::Error::custom("requested_action must not be null"))
}

#[derive(Debug, Serialize)]
struct BackendIngestRequest<'a> {
    url: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_url: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_title: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<&'a str>,
    tags: &'a [String],
    #[serde(skip_serializing_if = "Option::is_none")]
    requested_action: Option<RequestedAction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    requested_publish_at: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    requested_post_caption: Option<&'a str>,
}

#[derive(Debug, Serialize)]
struct AcceptedResponse {
    accepted: bool,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: &'static str,
}

#[derive(Debug, Error)]
pub enum CompanionError {
    #[error("could not bind the companion listener: {0}")]
    Bind(String),
    #[error("the companion listener failed: {0}")]
    Receive(#[source] std::io::Error),
    #[error("the companion response failed: {0}")]
    Respond(String),
}

#[derive(Debug)]
struct RequestLimiter {
    events: Mutex<VecDeque<Instant>>,
}

impl RequestLimiter {
    fn new() -> Self {
        Self { events: Mutex::new(VecDeque::with_capacity(RATE_LIMIT_REQUESTS)) }
    }

    fn allow(&self) -> bool {
        let now = Instant::now();
        let Ok(mut events) = self.events.lock() else { return false };
        while events
            .front()
            .is_some_and(|created_at| now.duration_since(*created_at) >= RATE_LIMIT_WINDOW)
        {
            events.pop_front();
        }
        if events.len() >= RATE_LIMIT_REQUESTS {
            return false;
        }
        events.push_back(now);
        true
    }
}

struct CompanionService<'a> {
    config: &'a CompanionConfig,
    backend_url: String,
    agent: ureq::Agent,
    limiter: Arc<RequestLimiter>,
}

impl<'a> CompanionService<'a> {
    fn new(config: &'a CompanionConfig) -> Self {
        let backend_url =
            format!("{}{}", config.backend_url.trim_end_matches('/'), BACKEND_INGEST_PATH);
        let agent = ureq::AgentBuilder::new()
            .timeout(Duration::from_secs(config.request_timeout_seconds))
            .redirects(0)
            .build();
        Self { config, backend_url, agent, limiter: Arc::new(RequestLimiter::new()) }
    }

    fn handle(&self, mut request: Request) -> Result<(), CompanionError> {
        if request.method() != &Method::Post || request.url() != SUBMIT_PATH {
            return respond_error(request, StatusCode(404), "not_found");
        }
        if !authorized(self.config.local_token.expose_secret(), &request) {
            return respond_error(request, StatusCode(401), "unauthorized");
        }
        if !self.limiter.allow() {
            return respond_error(request, StatusCode(429), "rate_limited");
        }
        if !is_json_request(&request) {
            return respond_error(request, StatusCode(415), "content_type_required");
        }

        let body = match read_body(&mut request, self.config.request_body_limit_bytes) {
            Ok(body) => body,
            Err(ReadBodyError::TooLarge) => {
                return respond_error(request, StatusCode(413), "payload_too_large");
            }
            Err(ReadBodyError::Io) => {
                return respond_error(request, StatusCode(400), "invalid_body");
            }
        };
        let submission = match serde_json::from_slice::<CompanionSubmission>(&body) {
            Ok(submission) => submission,
            Err(_) => return respond_error(request, StatusCode(400), "invalid_json"),
        };
        let submission = match validate_submission(submission) {
            Ok(submission) => submission,
            Err(_) => return respond_error(request, StatusCode(400), "invalid_request"),
        };
        let idempotency_key = format!("companion:{}", submission.action_id);
        let payload = BackendIngestRequest {
            url: &submission.url,
            page_url: submission.page_url.as_deref(),
            page_title: submission.page_title.as_deref(),
            description: submission.description.as_deref(),
            tags: &submission.tags,
            requested_action: submission.requested_action,
            requested_publish_at: submission.requested_publish_at.as_deref(),
            requested_post_caption: submission.requested_post_caption.as_deref(),
        };

        match forward(
            &self.agent,
            &self.backend_url,
            self.config.backend_token.expose_secret(),
            &idempotency_key,
            &payload,
        ) {
            Ok(()) => {
                debug!("companion submission accepted by backend");
                respond_json(request, StatusCode(202), &AcceptedResponse { accepted: true })
            }
            Err(_) => respond_error(request, StatusCode(502), "backend_request_failed"),
        }
    }
}

/// Serve the fixed localhost endpoint until `stop` is set.
pub fn serve(config: &CompanionConfig, stop: &AtomicBool) -> Result<(), CompanionError> {
    let server = Server::http(&config.listen_address)
        .map_err(|error| CompanionError::Bind(error.to_string()))?;
    let service = CompanionService::new(config);
    while !stop.load(Ordering::Acquire) {
        let request = server.recv_timeout(RECEIVE_POLL).map_err(CompanionError::Receive)?;
        if let Some(request) = request {
            service.handle(request)?;
        }
    }
    Ok(())
}

fn authorized(expected_token: &str, request: &Request) -> bool {
    let Some(value) = request
        .headers()
        .iter()
        .find(|header| header.field.equiv("Authorization"))
        .map(|header| header.value.as_str())
    else {
        return false;
    };
    let Some(token) = value
        .split_once(' ')
        .filter(|(scheme, token)| scheme.eq_ignore_ascii_case("Bearer") && !token.is_empty())
        .map(|(_, token)| token)
    else {
        return false;
    };
    let expected = Sha256::digest(expected_token.as_bytes());
    let actual = Sha256::digest(token.as_bytes());
    expected.ct_eq(&actual).unwrap_u8() == 1
}

fn is_json_request(request: &Request) -> bool {
    request.headers().iter().find(|header| header.field.equiv("Content-Type")).is_some_and(
        |header| {
            header
                .value
                .as_str()
                .split(';')
                .next()
                .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
        },
    )
}

enum ReadBodyError {
    TooLarge,
    Io,
}

fn read_body(request: &mut Request, limit: usize) -> Result<Vec<u8>, ReadBodyError> {
    if request.body_length().is_some_and(|length| length > limit) {
        return Err(ReadBodyError::TooLarge);
    }
    let mut body = Vec::with_capacity(request.body_length().unwrap_or_default().min(limit));
    request
        .as_reader()
        .take(u64::try_from(limit).unwrap_or(u64::MAX).saturating_add(1))
        .read_to_end(&mut body)
        .map_err(|_| ReadBodyError::Io)?;
    if body.len() > limit { Err(ReadBodyError::TooLarge) } else { Ok(body) }
}

fn validate_submission(
    mut submission: CompanionSubmission,
) -> Result<CompanionSubmission, &'static str> {
    submission.action_id = bounded_text(submission.action_id, MAX_ACTION_ID_CHARS, "action_id")?;
    if !submission.action_id.is_ascii()
        || submission.action_id.chars().any(|character| character.is_ascii_whitespace())
    {
        return Err("action_id");
    }
    submission.url = validate_url(submission.url, MAX_URL_CHARS, "url")?;
    submission.page_url = submission
        .page_url
        .map(|value| validate_url(value, MAX_URL_CHARS, "page_url"))
        .transpose()?;
    submission.page_title = submission
        .page_title
        .map(|value| bounded_text(value, MAX_PAGE_TITLE_CHARS, "page_title"))
        .transpose()?;
    submission.description =
        optional_multiline_text(submission.description, MAX_DESCRIPTION_CHARS, "description")?;
    submission.requested_publish_at = submission
        .requested_publish_at
        .map(|value| bounded_text(value, MAX_REQUESTED_PUBLISH_AT_CHARS, "requested_publish_at"))
        .transpose()?;
    if let Some(value) = submission.requested_publish_at.as_deref() {
        time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
            .map_err(|_| "requested_publish_at")?;
    }
    submission.requested_post_caption = optional_multiline_text(
        submission.requested_post_caption,
        MAX_POST_CAPTION_CHARS,
        "requested_post_caption",
    )?;

    let action = submission.requested_action.unwrap_or_default();
    match action {
        RequestedAction::Save => {
            if submission.requested_publish_at.is_some()
                || submission.requested_post_caption.is_some()
            {
                return Err("save_intent_fields");
            }
        }
        RequestedAction::Queue => {}
        RequestedAction::PostNow => {
            if submission.requested_publish_at.is_some() {
                return Err("post_now_publish_at");
            }
        }
    }
    if submission.tags.len() > MAX_TAGS {
        return Err("tags");
    }
    let mut tags = Vec::with_capacity(submission.tags.len());
    for tag in submission.tags {
        let tag = bounded_text(tag, MAX_TAG_CHARS, "tag")?;
        if !tags.iter().any(|existing| existing == &tag) {
            tags.push(tag);
        }
    }
    submission.tags = tags;
    Ok(submission)
}

fn bounded_text(
    value: String,
    max_chars: usize,
    _field: &'static str,
) -> Result<String, &'static str> {
    let value = value.trim().to_owned();
    if value.is_empty() || value.chars().count() > max_chars || value.chars().any(char::is_control)
    {
        return Err("bounded_text");
    }
    Ok(value)
}

fn optional_multiline_text(
    value: Option<String>,
    max_chars: usize,
    _field: &'static str,
) -> Result<Option<String>, &'static str> {
    let Some(value) = value else { return Ok(None) };
    let value = value.trim().to_owned();
    if value.is_empty() {
        return Ok(None);
    }
    if value.chars().count() > max_chars
        || value
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
    {
        return Err("multiline_text");
    }
    Ok(Some(value))
}

fn validate_url(
    value: String,
    max_chars: usize,
    field: &'static str,
) -> Result<String, &'static str> {
    let value = bounded_text(value, max_chars, field)?;
    let parsed = Url::parse(&value).map_err(|_| field)?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
    {
        return Err(field);
    }
    Ok(value)
}

fn forward(
    agent: &ureq::Agent,
    backend_url: &str,
    backend_token: &str,
    idempotency_key: &str,
    payload: &BackendIngestRequest<'_>,
) -> Result<(), ()> {
    let authorization = format!("Bearer {backend_token}");
    let response = agent
        .post(backend_url)
        .set("Authorization", &authorization)
        .set("Idempotency-Key", idempotency_key)
        .send_json(payload)
        .map_err(|_| ())?;
    if (200..300).contains(&response.status()) { Ok(()) } else { Err(()) }
}

fn respond_error(
    request: Request,
    status: StatusCode,
    error: &'static str,
) -> Result<(), CompanionError> {
    respond_json(request, status, &ErrorResponse { error })
}

fn respond_json<T: Serialize>(
    request: Request,
    status: StatusCode,
    body: &T,
) -> Result<(), CompanionError> {
    let body = serde_json::to_string(body).expect("companion response must serialize");
    let mut response = Response::from_string(body).with_status_code(status);
    let header = Header::from_bytes(&b"Content-Type"[..], &b"application/json; charset=utf-8"[..])
        .expect("static response header must be valid");
    response.add_header(header);
    request.respond(response).map_err(|error| CompanionError::Respond(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sooqa_config::SecretString;
    use std::{
        net::{SocketAddr, TcpListener},
        thread::{self, JoinHandle},
    };

    #[derive(Debug, Eq, PartialEq)]
    struct CapturedBackendRequest {
        path: String,
        authorization: String,
        idempotency_key: String,
        body: String,
    }

    type BackendCapture = Arc<Mutex<Vec<CapturedBackendRequest>>>;
    type BackendThread = JoinHandle<Result<(), String>>;

    fn test_config(listen_address: SocketAddr, backend_url: String) -> CompanionConfig {
        CompanionConfig {
            listen_address: listen_address.to_string(),
            backend_url,
            local_token: SecretString::new("local-secret"),
            backend_token: SecretString::new("backend-secret"),
            request_body_limit_bytes: 64 * 1024,
            request_timeout_seconds: 5,
        }
    }

    fn free_loopback_address() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
        listener.local_addr().expect("test listener should have an address")
    }

    fn start_backend(statuses: Vec<u16>) -> (String, BackendCapture, BackendThread) {
        let server = Server::http("127.0.0.1:0").expect("fake backend should bind");
        let address = server.server_addr().to_ip().expect("fake backend should use TCP");
        let captured = Arc::new(Mutex::new(Vec::new()));
        let captured_by_server = Arc::clone(&captured);
        let server_thread = thread::spawn(move || {
            for status in statuses {
                let mut request = server
                    .recv_timeout(Duration::from_secs(5))
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| "fake backend timed out".to_owned())?;
                let path = request.url().to_owned();
                let authorization = request
                    .headers()
                    .iter()
                    .find(|header| header.field.equiv("Authorization"))
                    .map(|header| header.value.to_string())
                    .unwrap_or_default();
                let idempotency_key = request
                    .headers()
                    .iter()
                    .find(|header| header.field.equiv("Idempotency-Key"))
                    .map(|header| header.value.to_string())
                    .unwrap_or_default();
                let mut body = String::new();
                request.as_reader().read_to_string(&mut body).map_err(|error| error.to_string())?;
                captured_by_server
                    .lock()
                    .expect("capture lock should not be poisoned")
                    .push(CapturedBackendRequest { path, authorization, idempotency_key, body });
                request
                    .respond(Response::from_string("{}").with_status_code(StatusCode(status)))
                    .map_err(|error| error.to_string())?;
            }
            Ok(())
        });
        (format!("http://{address}"), captured, server_thread)
    }

    fn start_companion(
        config: CompanionConfig,
    ) -> (String, Arc<AtomicBool>, JoinHandle<Result<(), CompanionError>>) {
        let address = config.listen_address.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_for_server = Arc::clone(&stop);
        let server_thread = thread::spawn(move || serve(&config, &stop_for_server));
        let endpoint = format!("http://{address}");
        let agent = ureq::AgentBuilder::new().timeout(Duration::from_millis(500)).build();
        for _ in 0..100 {
            match agent.get(&format!("{endpoint}/")).call() {
                Ok(_) | Err(ureq::Error::Status(_, _)) => return (endpoint, stop, server_thread),
                Err(ureq::Error::Transport(_)) => thread::sleep(Duration::from_millis(10)),
            }
        }
        panic!("companion test server did not start");
    }

    fn send_json(
        agent: &ureq::Agent,
        endpoint: &str,
        token: &str,
        body: &serde_json::Value,
    ) -> (u16, String) {
        let response = agent
            .post(endpoint)
            .set("Authorization", &format!("Bearer {token}"))
            .set("Content-Type", "application/json")
            .send_json(body);
        match response {
            Ok(response) => (response.status(), response.into_string().unwrap_or_default()),
            Err(ureq::Error::Status(status, response)) => {
                (status, response.into_string().unwrap_or_default())
            }
            Err(error) => panic!("companion request failed: {error}"),
        }
    }

    fn submission_value(action_id: &str) -> serde_json::Value {
        serde_json::json!({
            "action_id": action_id,
            "url": "https://2ch.org/b/src/1/clip.webm",
            "page_url": "https://2ch.org/b/res/1.html",
            "page_title": "Thread",
            "description": "Internal note",
            "tags": ["cats"]
        })
    }

    fn submission() -> CompanionSubmission {
        CompanionSubmission {
            action_id: "action-123".to_owned(),
            url: "https://2ch.org/b/src/1/clip.webm".to_owned(),
            page_url: Some("https://2ch.org/b/res/1.html".to_owned()),
            page_title: Some("Thread".to_owned()),
            description: Some("  internal note  ".to_owned()),
            tags: vec!["Cats".to_owned(), "Cats".to_owned()],
            requested_action: None,
            requested_publish_at: None,
            requested_post_caption: None,
        }
    }

    #[test]
    fn token_check_uses_bearer_value_without_exposing_it() {
        let request: Request = tiny_http::TestRequest::new()
            .with_header(
                Header::from_bytes(&b"Authorization"[..], &b"Bearer local-secret"[..])
                    .expect("test header should be valid"),
            )
            .with_method(Method::Post)
            .with_path(SUBMIT_PATH)
            .with_body("{}")
            .into();
        assert!(authorized("local-secret", &request));
        assert!(!authorized("other-secret", &request));
    }

    #[test]
    fn submission_validation_bounds_and_normalizes_metadata() {
        let value = validate_submission(submission()).expect("submission should validate");
        assert_eq!(value.description.as_deref(), Some("internal note"));
        assert_eq!(value.tags, ["Cats"]);
    }

    #[test]
    fn submission_validation_accepts_all_backend_actions_and_keeps_fields_separate() {
        let mut save = submission();
        save.requested_action = Some(RequestedAction::Save);
        assert!(validate_submission(save).is_ok());

        let mut queue = submission();
        queue.requested_action = Some(RequestedAction::Queue);
        queue.requested_post_caption = Some("public queue text".to_owned());
        assert!(validate_submission(queue).is_ok());

        let mut exact_queue = submission();
        exact_queue.requested_action = Some(RequestedAction::Queue);
        exact_queue.requested_publish_at = Some("2099-01-02T03:04:05Z".to_owned());
        exact_queue.requested_post_caption = Some("public exact text".to_owned());
        let exact_queue =
            validate_submission(exact_queue).expect("exact queue submission should validate");
        assert_eq!(exact_queue.description.as_deref(), Some("internal note"));
        assert_eq!(exact_queue.requested_post_caption.as_deref(), Some("public exact text"));

        let mut post_now = submission();
        post_now.requested_action = Some(RequestedAction::PostNow);
        post_now.requested_post_caption = Some("public now text".to_owned());
        assert!(validate_submission(post_now).is_ok());
    }

    #[test]
    fn submission_validation_rejects_intent_shape_errors_and_unknown_fields() {
        let mut save_with_caption = submission();
        save_with_caption.requested_post_caption = Some("public text".to_owned());
        assert!(validate_submission(save_with_caption).is_err());

        let mut save_with_time = submission();
        save_with_time.requested_publish_at = Some("2099-01-02T03:04:05Z".to_owned());
        assert!(validate_submission(save_with_time).is_err());

        let mut post_now_with_time = submission();
        post_now_with_time.requested_action = Some(RequestedAction::PostNow);
        post_now_with_time.requested_publish_at = Some("2099-01-02T03:04:05Z".to_owned());
        assert!(validate_submission(post_now_with_time).is_err());

        let mut malformed_time = submission();
        malformed_time.requested_action = Some(RequestedAction::Queue);
        malformed_time.requested_publish_at = Some("not-an-instant".to_owned());
        assert!(validate_submission(malformed_time).is_err());

        let unknown_action = serde_json::from_value::<CompanionSubmission>(serde_json::json!({
            "action_id": "unknown-action",
            "url": "https://2ch.org/b/src/1/clip.webm",
            "requested_action": "later"
        }));
        assert!(unknown_action.is_err());

        let null_action = serde_json::from_value::<CompanionSubmission>(serde_json::json!({
            "action_id": "null-action",
            "url": "https://2ch.org/b/src/1/clip.webm",
            "requested_action": null
        }));
        assert!(null_action.is_err());

        let unknown_field = serde_json::from_value::<CompanionSubmission>(serde_json::json!({
            "action_id": "unknown-field",
            "url": "https://2ch.org/b/src/1/clip.webm",
            "not_part_of_the_contract": true
        }));
        assert!(unknown_field.is_err());

        let mut oversized_caption = submission();
        oversized_caption.requested_action = Some(RequestedAction::PostNow);
        oversized_caption.requested_post_caption = Some("x".repeat(MAX_POST_CAPTION_CHARS + 1));
        assert!(validate_submission(oversized_caption).is_err());

        let mut exact_caption = submission();
        exact_caption.requested_action = Some(RequestedAction::PostNow);
        exact_caption.requested_post_caption = Some("x".repeat(MAX_POST_CAPTION_CHARS));
        assert!(validate_submission(exact_caption).is_ok());
    }

    #[test]
    fn submission_validation_allows_multiline_optional_text_but_rejects_other_controls() {
        let mut value = submission();
        value.url = "https://user:pass@example.com/clip.webm".to_owned();
        assert!(validate_submission(value).is_err());

        let mut value = submission();
        value.description = Some("note\nwith tab\tand return\r".to_owned());
        assert_eq!(
            validate_submission(value).unwrap().description.as_deref(),
            Some("note\nwith tab\tand return")
        );

        let mut value = submission();
        value.description = Some("note\u{0000}with control".to_owned());
        assert!(validate_submission(value).is_err());
    }

    #[test]
    fn backend_payload_excludes_local_action_and_credentials() {
        let value = validate_submission(submission()).expect("submission should validate");
        let payload = BackendIngestRequest {
            url: &value.url,
            page_url: value.page_url.as_deref(),
            page_title: value.page_title.as_deref(),
            description: value.description.as_deref(),
            tags: &value.tags,
            requested_action: value.requested_action,
            requested_publish_at: value.requested_publish_at.as_deref(),
            requested_post_caption: value.requested_post_caption.as_deref(),
        };
        let encoded = serde_json::to_string(&payload).expect("payload should serialize");
        assert!(encoded.contains("internal note"));
        assert!(!encoded.contains("action-123"));
        assert!(!encoded.contains("local-secret"));
    }

    #[test]
    fn backend_payload_forwards_the_typed_intent_without_conflating_metadata() {
        let mut value = submission();
        value.requested_action = Some(RequestedAction::PostNow);
        value.requested_post_caption = Some("public text".to_owned());
        let value = validate_submission(value).expect("submission should validate");
        let payload = BackendIngestRequest {
            url: &value.url,
            page_url: value.page_url.as_deref(),
            page_title: value.page_title.as_deref(),
            description: value.description.as_deref(),
            tags: &value.tags,
            requested_action: value.requested_action,
            requested_publish_at: value.requested_publish_at.as_deref(),
            requested_post_caption: value.requested_post_caption.as_deref(),
        };
        let encoded = serde_json::to_value(&payload).expect("payload should serialize");
        assert_eq!(encoded["requested_action"], "post_now");
        assert_eq!(encoded["requested_post_caption"], "public text");
        assert_eq!(encoded["description"], "internal note");
        assert_eq!(encoded["tags"], serde_json::json!(["Cats"]));
        assert!(encoded.get("requested_publish_at").is_none());
    }

    #[test]
    fn backend_payload_omits_blank_optional_text_and_preserves_multiline_caption() {
        let mut blank = submission();
        blank.requested_action = Some(RequestedAction::PostNow);
        blank.description = Some(" \n\t ".to_owned());
        blank.requested_post_caption = Some(" \r\n\t ".to_owned());
        let blank = validate_submission(blank).expect("blank optional text should be omitted");
        let payload = BackendIngestRequest {
            url: &blank.url,
            page_url: blank.page_url.as_deref(),
            page_title: blank.page_title.as_deref(),
            description: blank.description.as_deref(),
            tags: &blank.tags,
            requested_action: blank.requested_action,
            requested_publish_at: blank.requested_publish_at.as_deref(),
            requested_post_caption: blank.requested_post_caption.as_deref(),
        };
        let encoded = serde_json::to_value(&payload).expect("payload should serialize");
        assert!(encoded.get("description").is_none());
        assert!(encoded.get("requested_post_caption").is_none());

        let mut multiline = submission();
        multiline.requested_action = Some(RequestedAction::PostNow);
        multiline.requested_post_caption = Some("line one\nline two\tready".to_owned());
        let multiline = validate_submission(multiline).expect("multiline caption should validate");
        assert_eq!(multiline.requested_post_caption.as_deref(), Some("line one\nline two\tready"));
    }

    #[test]
    fn unauthorized_flood_does_not_consume_authenticated_rate_limit() {
        let (backend_url, captured, backend_thread) = start_backend(vec![202]);
        let config = test_config(free_loopback_address(), backend_url);
        let (companion_url, stop, companion_thread) = start_companion(config);
        let agent = ureq::AgentBuilder::new().timeout(Duration::from_secs(2)).build();
        let body = submission_value("after-unauthorized-flood");

        for _ in 0..(RATE_LIMIT_REQUESTS * 2) {
            let response =
                send_json(&agent, &format!("{companion_url}{SUBMIT_PATH}"), "wrong-secret", &body);
            assert_eq!(response.0, 401);
        }
        let accepted =
            send_json(&agent, &format!("{companion_url}{SUBMIT_PATH}"), "local-secret", &body);
        assert_eq!(accepted.0, 202);

        stop.store(true, Ordering::Release);
        companion_thread
            .join()
            .expect("companion thread should join")
            .expect("companion should stop");
        backend_thread.join().expect("backend thread should join").expect("backend should finish");
        assert_eq!(captured.lock().expect("capture lock should not be poisoned").len(), 1);
    }

    #[test]
    fn http_submit_uses_fixed_route_backend_auth_idempotency_and_status_mapping() {
        let (backend_url, captured, backend_thread) = start_backend(vec![202, 500]);
        let config = test_config(free_loopback_address(), backend_url);
        let (companion_url, stop, companion_thread) = start_companion(config);
        let agent = ureq::AgentBuilder::new().timeout(Duration::from_secs(2)).build();
        let body = submission_value("stable-action");

        let wrong_route =
            send_json(&agent, &format!("{companion_url}/v1/other"), "local-secret", &body);
        assert_eq!(wrong_route.0, 404);

        let accepted =
            send_json(&agent, &format!("{companion_url}{SUBMIT_PATH}"), "local-secret", &body);
        assert_eq!(accepted.0, 202);
        let accepted_body =
            serde_json::from_str::<serde_json::Value>(&accepted.1).expect("accepted body is JSON");
        assert_eq!(accepted_body["accepted"], true);

        let failed =
            send_json(&agent, &format!("{companion_url}{SUBMIT_PATH}"), "local-secret", &body);
        assert_eq!(failed.0, 502);
        let failed_body =
            serde_json::from_str::<serde_json::Value>(&failed.1).expect("error body is JSON");
        assert_eq!(failed_body["error"], "backend_request_failed");

        stop.store(true, Ordering::Release);
        companion_thread
            .join()
            .expect("companion thread should join")
            .expect("companion should stop");
        backend_thread.join().expect("backend thread should join").expect("backend should finish");

        let captured = captured.lock().expect("capture lock should not be poisoned");
        assert_eq!(captured.len(), 2);
        for request in captured.iter() {
            assert_eq!(request.path, BACKEND_INGEST_PATH);
            assert_eq!(request.authorization, "Bearer backend-secret");
            assert_eq!(request.idempotency_key, "companion:stable-action");
            let body = serde_json::from_str::<serde_json::Value>(&request.body)
                .expect("backend body should be JSON");
            assert_eq!(body["url"], "https://2ch.org/b/src/1/clip.webm");
            assert!(body.get("action_id").is_none());
        }
    }

    #[test]
    fn http_submit_forwards_each_capture_action_and_exact_time() {
        let (backend_url, captured, backend_thread) = start_backend(vec![202, 202, 202, 202]);
        let config = test_config(free_loopback_address(), backend_url);
        let (companion_url, stop, companion_thread) = start_companion(config);
        let agent = ureq::AgentBuilder::new().timeout(Duration::from_secs(2)).build();

        let mut save = submission_value("action-save");
        save["requested_action"] = serde_json::json!("save");
        let mut queue = submission_value("action-queue");
        queue["requested_action"] = serde_json::json!("queue");
        queue["requested_post_caption"] = serde_json::json!("normal queue text");
        let mut exact_queue = submission_value("action-exact-queue");
        exact_queue["requested_action"] = serde_json::json!("queue");
        exact_queue["requested_publish_at"] = serde_json::json!("2099-01-02T03:04:05Z");
        exact_queue["requested_post_caption"] = serde_json::json!("exact queue text");
        let mut post_now = submission_value("action-post-now");
        post_now["requested_action"] = serde_json::json!("post_now");
        post_now["requested_post_caption"] = serde_json::json!("post now text");

        for body in [&save, &queue, &exact_queue, &post_now] {
            let response =
                send_json(&agent, &format!("{companion_url}{SUBMIT_PATH}"), "local-secret", body);
            assert_eq!(response.0, 202);
        }

        stop.store(true, Ordering::Release);
        companion_thread
            .join()
            .expect("companion thread should join")
            .expect("companion should stop");
        backend_thread.join().expect("backend thread should join").expect("backend should finish");

        let captured = captured.lock().expect("capture lock should not be poisoned");
        assert_eq!(captured.len(), 4);
        let expected = [
            ("action-save", "save", None, None),
            ("action-queue", "queue", None, Some("normal queue text")),
            ("action-exact-queue", "queue", Some("2099-01-02T03:04:05Z"), Some("exact queue text")),
            ("action-post-now", "post_now", None, Some("post now text")),
        ];
        for (request, (action_id, action, publish_at, post_caption)) in
            captured.iter().zip(expected)
        {
            assert_eq!(request.idempotency_key, format!("companion:{action_id}"));
            let body = serde_json::from_str::<serde_json::Value>(&request.body)
                .expect("backend body should be JSON");
            assert_eq!(body["requested_action"], action);
            match publish_at {
                Some(value) => assert_eq!(body["requested_publish_at"], value),
                None => assert!(body.get("requested_publish_at").is_none()),
            }
            match post_caption {
                Some(value) => assert_eq!(body["requested_post_caption"], value),
                None => assert!(body.get("requested_post_caption").is_none()),
            }
            assert_eq!(body["description"], "Internal note");
            assert_eq!(body["tags"], serde_json::json!(["cats"]));
        }
    }

    #[test]
    fn rate_limiter_is_bounded() {
        let limiter = RequestLimiter::new();
        for _ in 0..RATE_LIMIT_REQUESTS {
            assert!(limiter.allow());
        }
        assert!(!limiter.allow());
    }
}
