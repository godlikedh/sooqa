use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use sooqa_inbox::{SourceInspection, SourceMediaKind};
use sooqa_media::{DownloadError, SourceDownloader, SourceInput};
use uuid::Uuid;

#[derive(Clone)]
struct FakeSourceDownloader {
    outcome: Arc<SourceInspection>,
    calls: Arc<AtomicUsize>,
}

impl FakeSourceDownloader {
    fn successful(inspection: SourceInspection) -> Self {
        Self { outcome: Arc::new(inspection), calls: Arc::new(AtomicUsize::new(0)) }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl SourceDownloader for FakeSourceDownloader {
    async fn inspect(&self, _source: &SourceInput) -> Result<SourceInspection, DownloadError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(self.outcome.as_ref().clone())
    }
}

#[tokio::test]
async fn source_inspection_adapter_remains_independent_of_persistence() {
    let inspection = SourceInspection {
        adapter: "test".to_owned(),
        source_url: "https://example.test".to_owned(),
        resolved_url: None,
        media_kind: SourceMediaKind::Video,
        mime_type: None,
        content_length_bytes: None,
        title: None,
        metadata: serde_json::json!({}),
    };
    let downloader = FakeSourceDownloader::successful(inspection.clone());
    let result = downloader
        .inspect(&SourceInput {
            ingest_request_id: Uuid::new_v4(),
            source_url: "https://example.test".to_owned(),
            page_url: None,
        })
        .await
        .unwrap();
    assert_eq!(result, inspection);
    assert_eq!(downloader.calls(), 1);
}
