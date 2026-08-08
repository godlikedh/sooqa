use sooqa_inbox::{SourceInspection, SourceMediaKind};
use sooqa_media::{SourceDownloader, SourceInput};
use sooqa_test_support::FakeSourceDownloader;
use uuid::Uuid;

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
