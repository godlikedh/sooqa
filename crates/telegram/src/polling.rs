//! Long-polling supervision, retry backoff, and offset advancement.

use std::{future::Future, pin::Pin, time::Duration};

use teloxide::types::Update;

use crate::{
    IngestService, TelegramApi, TelegramApiError, TelegramError, TelegramPollingApi,
    TelegramService, TeloxideApi,
};

const RETRY_DELAY: Duration = Duration::from_secs(1);
const MAX_HANDLER_ATTEMPTS: usize = 5;
const MAX_POLL_RETRY_DELAY: Duration = Duration::from_secs(60);

pub struct TelegramRuntime<I, A = TeloxideApi> {
    api: A,
    service: TelegramService<A, I>,
    poll_timeout: Duration,
    storage_chat_id: Option<i64>,
    retry_initial_delay: Duration,
    retry_max_delay: Duration,
}

impl<I> TelegramRuntime<I, TeloxideApi>
where
    I: IngestService,
{
    pub fn new(
        token: impl Into<String>,
        api_base_url: &str,
        poll_timeout: Duration,
        admin_user_ids: impl IntoIterator<Item = i64>,
        storage_chat_id: Option<i64>,
        ingest_service: I,
    ) -> Result<Self, TelegramError> {
        let api = TeloxideApi::new(token, api_base_url, poll_timeout)?;
        Ok(Self::new_with_api(api, poll_timeout, admin_user_ids, storage_chat_id, ingest_service))
    }

    pub fn with_upload_timeout(mut self, upload_timeout: Duration) -> Result<Self, TelegramError> {
        self.api = self.api.with_upload_timeout(upload_timeout)?;
        self.service.api = self.api.clone();
        Ok(self)
    }

    pub fn with_source_download_max_bytes(mut self, max_bytes: u64) -> Self {
        self.api = self.api.with_source_download_max_bytes(max_bytes);
        self.service.api = self.api.clone();
        self.service = self.service.with_source_download_max_bytes(max_bytes);
        self
    }
}

impl<I, A> TelegramRuntime<I, A>
where
    A: TelegramApi + TelegramPollingApi,
    I: IngestService,
{
    pub fn new_with_api(
        api: A,
        poll_timeout: Duration,
        admin_user_ids: impl IntoIterator<Item = i64>,
        storage_chat_id: Option<i64>,
        ingest_service: I,
    ) -> Self {
        let service = TelegramService::with_ingest(api.clone(), admin_user_ids, ingest_service);
        Self {
            api,
            service,
            poll_timeout,
            storage_chat_id,
            retry_initial_delay: RETRY_DELAY,
            retry_max_delay: MAX_POLL_RETRY_DELAY,
        }
    }

    pub fn with_polling_backoff(
        mut self,
        initial_delay: Duration,
        max_delay: Duration,
    ) -> Result<Self, TelegramError> {
        if initial_delay.is_zero() || max_delay.is_zero() || initial_delay > max_delay {
            return Err(TelegramError::InvalidPollingBackoff);
        }
        self.retry_initial_delay = initial_delay;
        self.retry_max_delay = max_delay;
        Ok(self)
    }

    /// Run the Telegram control plane until Ctrl-C.  The server process uses
    /// [`Self::run_with_shutdown`] so the HTTP server owns the process signal
    /// and can drain before this task exits.
    pub async fn run(self) -> Result<(), TelegramError> {
        let shutdown = async {
            if let Err(error) = tokio::signal::ctrl_c().await {
                tracing::error!(
                    target: "sooqa.telegram",
                    status = "shutdown_error",
                    ?error,
                    "could not listen for Ctrl-C while supervising Telegram"
                );
            }
        };
        self.run_with_shutdown(shutdown).await
    }

    pub async fn run_with_shutdown<F>(self, shutdown: F) -> Result<(), TelegramError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if self.poll_timeout.is_zero() || self.poll_timeout.as_secs() > u64::from(u32::MAX) {
            return Err(TelegramError::InvalidPollTimeout);
        }
        let poll_timeout = self.poll_timeout.as_secs() as u32;
        let mut shutdown = Box::pin(shutdown);
        let mut backoff = PollBackoff::new(self.retry_initial_delay, self.retry_max_delay);

        if let Some(storage_chat_id) = self.storage_chat_id {
            let mut failures = 0_usize;
            loop {
                let result = tokio::select! {
                    _ = &mut shutdown => return Ok(()),
                    result = self.api.verify_storage_chat(storage_chat_id) => result,
                };
                match result {
                    Ok(()) => {
                        if failures > 0 {
                            tracing::info!(
                                target: "sooqa.telegram",
                                status = "recovered",
                                phase = "storage_preflight",
                                attempts = failures,
                                storage_chat_id,
                                "Telegram storage preflight recovered"
                            );
                        } else {
                            tracing::info!(
                                target: "sooqa.telegram",
                                status = "ready",
                                phase = "storage_preflight",
                                storage_chat_id,
                                "Telegram storage chat is reachable"
                            );
                        }
                        backoff.reset();
                        break;
                    }
                    Err(error) if A::is_terminal_error(&error) => {
                        tracing::error!(
                            target: "sooqa.telegram",
                            status = "terminally_misconfigured",
                            phase = "storage_preflight",
                            storage_chat_id,
                            error = %error,
                            "Telegram storage configuration or authentication is not usable"
                        );
                        return Err(TelegramError::Api(Box::new(error)));
                    }
                    Err(error) => {
                        failures += 1;
                        let delay = backoff.next(A::retry_after(&error));
                        tracing::warn!(
                            target: "sooqa.telegram",
                            status = if failures == 1 { "degraded" } else { "retrying" },
                            phase = "storage_preflight",
                            attempt = failures,
                            retry_in_ms = delay.as_millis() as u64,
                            storage_chat_id,
                            error = %error,
                            "Telegram storage preflight unavailable; retrying"
                        );
                        if !wait_for_retry(&mut shutdown, delay).await {
                            return Ok(());
                        }
                    }
                }
            }
        }

        let mut failures = 0_usize;
        loop {
            let result = tokio::select! {
                _ = &mut shutdown => return Ok(()),
                result = self.api.delete_webhook() => result,
            };
            match result {
                Ok(()) => {
                    if failures > 0 {
                        tracing::info!(
                            target: "sooqa.telegram",
                            status = "recovered",
                            phase = "startup",
                            attempts = failures,
                            "Telegram startup control call recovered"
                        );
                    }
                    backoff.reset();
                    break;
                }
                Err(error) if A::is_terminal_error(&error) => {
                    tracing::error!(
                        target: "sooqa.telegram",
                        status = "terminally_misconfigured",
                        phase = "startup",
                        error = %error,
                        "Telegram startup control call is not usable"
                    );
                    return Err(TelegramError::Api(Box::new(error)));
                }
                Err(error) => {
                    failures += 1;
                    let delay = backoff.next(A::retry_after(&error));
                    tracing::warn!(
                        target: "sooqa.telegram",
                        status = if failures == 1 { "degraded" } else { "retrying" },
                        phase = "startup",
                        attempt = failures,
                        retry_in_ms = delay.as_millis() as u64,
                        error = %error,
                        "Telegram startup control call unavailable; retrying"
                    );
                    if !wait_for_retry(&mut shutdown, delay).await {
                        return Ok(());
                    }
                }
            }
        }

        let service = self.service;
        let mut offset = 0_i32;
        let mut polling_failures = 0_usize;

        loop {
            let result = tokio::select! {
                _ = &mut shutdown => return Ok(()),
                result = self.api.get_updates(offset, poll_timeout) => result,
            };
            match result {
                Ok(updates) => {
                    if polling_failures > 0 {
                        tracing::info!(
                            target: "sooqa.telegram",
                            status = "recovered",
                            phase = "polling",
                            attempts = polling_failures,
                            offset,
                            "Telegram polling recovered"
                        );
                    }
                    polling_failures = 0;
                    backoff.reset();
                    for update in updates {
                        match handle_update_with_retries(&service, update).await {
                            Ok(next_offset) => offset = next_offset,
                            Err(error) if is_terminal_bot_error(&error) => {
                                tracing::error!(
                                    target: "sooqa.telegram",
                                    status = "terminally_misconfigured",
                                    phase = "update",
                                    offset,
                                    error = %error,
                                    "Telegram update handling reached a terminal configuration error"
                                );
                                return Err(error);
                            }
                            Err(error) => {
                                polling_failures += 1;
                                let delay = backoff.next(Some(retry_delay(&error)));
                                tracing::warn!(
                                    target: "sooqa.telegram",
                                    status = if polling_failures == 1 { "degraded" } else { "retrying" },
                                    phase = "update",
                                    attempt = polling_failures,
                                    retry_in_ms = delay.as_millis() as u64,
                                    offset,
                                    error = %error,
                                    "Telegram update handling failed; retaining offset"
                                );
                                if !wait_for_retry(&mut shutdown, delay).await {
                                    return Ok(());
                                }
                                break;
                            }
                        }
                    }
                }
                Err(error) if A::is_terminal_error(&error) => {
                    tracing::error!(
                        target: "sooqa.telegram",
                        status = "terminally_misconfigured",
                        phase = "polling",
                        offset,
                        error = %error,
                        "Telegram polling stopped because remote authentication or configuration is invalid"
                    );
                    return Err(TelegramError::Api(Box::new(error)));
                }
                Err(error) => {
                    polling_failures += 1;
                    let delay = backoff.next(A::retry_after(&error));
                    tracing::warn!(
                        target: "sooqa.telegram",
                        status = if polling_failures == 1 { "degraded" } else { "retrying" },
                        phase = "polling",
                        attempt = polling_failures,
                        retry_in_ms = delay.as_millis() as u64,
                        offset,
                        error = %error,
                        "Telegram polling failed; retaining offset and retrying"
                    );
                    if !wait_for_retry(&mut shutdown, delay).await {
                        return Ok(());
                    }
                }
            }
        }
    }
}

struct PollBackoff {
    next_delay: Duration,
    initial_delay: Duration,
    max_delay: Duration,
}

impl PollBackoff {
    fn new(initial_delay: Duration, max_delay: Duration) -> Self {
        Self { next_delay: initial_delay, initial_delay, max_delay }
    }

    fn reset(&mut self) {
        self.next_delay = self.initial_delay;
    }

    fn next(&mut self, suggested: Option<Duration>) -> Duration {
        let delay = self.next_delay.max(suggested.unwrap_or_default());
        self.next_delay =
            self.next_delay.checked_mul(2).unwrap_or(self.max_delay).min(self.max_delay);
        delay
    }
}

async fn wait_for_retry<F>(shutdown: &mut Pin<Box<F>>, delay: Duration) -> bool
where
    F: Future<Output = ()> + Send + 'static,
{
    tokio::select! {
        _ = shutdown.as_mut() => false,
        _ = tokio::time::sleep(delay) => true,
    }
}

async fn handle_update_with_retries<A, I>(
    service: &TelegramService<A, I>,
    update: Update,
) -> Result<i32, TelegramError>
where
    A: TelegramApi,
    I: IngestService,
{
    for attempt in 1..=MAX_HANDLER_ATTEMPTS {
        match service.handle_update(update.clone()).await {
            Ok(_) => return Ok(update.id.as_offset()),
            Err(error) if attempt < MAX_HANDLER_ATTEMPTS => {
                tracing::warn!(
                    ?error,
                    update_id = update.id.0,
                    attempt,
                    "Telegram update handling failed; retrying before advancing offset"
                );
                tokio::time::sleep(retry_delay(&error)).await;
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("the bounded Telegram retry loop always returns")
}

fn is_terminal_bot_error(error: &TelegramError) -> bool {
    let TelegramError::Api(source) = error else { return false };
    source.downcast_ref::<teloxide::RequestError>().is_some_and(|error| {
        matches!(error, teloxide::RequestError::Api(teloxide::ApiError::InvalidToken))
    }) || source.downcast_ref::<TelegramApiError>().is_some_and(|error| {
        matches!(
            error,
            TelegramApiError::Api(teloxide::RequestError::Api(teloxide::ApiError::InvalidToken))
        )
    })
}

fn retry_delay(error: &TelegramError) -> Duration {
    let TelegramError::Api(source) = error else { return RETRY_DELAY };
    source
        .downcast_ref::<teloxide::RequestError>()
        .and_then(|error| match error {
            teloxide::RequestError::RetryAfter(seconds) => Some(seconds.duration()),
            _ => None,
        })
        .or_else(|| {
            source.downcast_ref::<TelegramApiError>().and_then(|error| match error {
                TelegramApiError::Api(teloxide::RequestError::RetryAfter(seconds)) => {
                    Some(seconds.duration())
                }
                _ => None,
            })
        })
        .unwrap_or(RETRY_DELAY)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{MockApi, MockIngestService, url_update};
    use teloxide::types::UpdateKind;

    #[test]
    fn polling_backoff_honors_local_growth_and_retry_after_minimums() {
        let cases = vec![
            (
                "local exponential growth and cap",
                Duration::from_secs(1),
                Duration::from_secs(4),
                vec![None, None, None, None],
                vec![1, 2, 4, 4],
            ),
            (
                "short retry-after cannot bypass local backoff",
                Duration::from_secs(4),
                Duration::from_secs(8),
                vec![Some(Duration::from_secs(1)), Some(Duration::from_secs(1)), None],
                vec![4, 8, 8],
            ),
            (
                "long retry-after remains the minimum",
                Duration::from_secs(1),
                Duration::from_secs(4),
                vec![Some(Duration::from_secs(120)), None],
                vec![120, 2],
            ),
        ];

        for (name, initial, maximum, suggestions, expected_seconds) in cases {
            let mut backoff = PollBackoff::new(initial, maximum);
            for (suggestion, expected_seconds) in suggestions.into_iter().zip(expected_seconds) {
                assert_eq!(
                    backoff.next(suggestion),
                    Duration::from_secs(expected_seconds),
                    "{name}"
                );
            }
        }
    }

    #[tokio::test]
    async fn transient_ingest_failure_retries_before_offset_advances() {
        let api = MockApi::default();
        let ingest = MockIngestService::default();
        *ingest.failures_remaining.lock().unwrap() = 2;
        let service = TelegramService::with_ingest(api.clone(), [123], ingest.clone());

        assert_eq!(handle_update_with_retries(&service, url_update(18)).await.unwrap(), 19);
        assert_eq!(*ingest.attempts.lock().unwrap(), 3);
        assert_eq!(ingest.commands.lock().unwrap().len(), 1);
        assert_eq!(api.messages.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn persistent_ingest_failure_stops_at_the_handler_attempt_bound() {
        let api = MockApi::default();
        let ingest = MockIngestService::default();
        *ingest.fail.lock().unwrap() = true;
        let service = TelegramService::with_ingest(api.clone(), [123], ingest.clone());

        let result = handle_update_with_retries(&service, url_update(19)).await;
        assert!(matches!(result, Err(TelegramError::Ingest(_))));
        assert_eq!(*ingest.attempts.lock().unwrap(), MAX_HANDLER_ATTEMPTS);
        assert!(ingest.commands.lock().unwrap().is_empty());
        assert!(api.messages.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn failed_update_retries_before_offset_advances() {
        let api = MockApi::default();
        *api.failures_remaining.lock().unwrap() = 1;
        let service = TelegramService::new(api.clone(), [123]);
        let message: teloxide::types::Message = serde_json::from_value(serde_json::json!({
            "message_id": 1,
            "from": {"id": 123, "is_bot": false, "first_name": "Admin"},
            "chat": {"id": 42, "type": "private", "first_name": "Admin"},
            "date": 0,
            "text": "/status"
        }))
        .expect("Telegram message fixture should deserialize");
        let update =
            Update { id: teloxide::types::UpdateId(7), kind: UpdateKind::Message(message) };

        assert_eq!(handle_update_with_retries(&service, update).await.unwrap(), 8);
        assert_eq!(api.messages.lock().unwrap().len(), 1);
    }
}
