//! Shared test fixtures and helpers for sooqa.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use sooqa_media::{DownloadError, SourceDownloader, SourceInput, SourceInspection};

#[derive(Clone)]
pub struct FakeSourceDownloader {
    outcome: Arc<FakeInspectionOutcome>,
    calls: Arc<AtomicUsize>,
}

#[derive(Clone)]
enum FakeInspectionOutcome {
    Success(SourceInspection),
    Failure(DownloadError),
}

impl FakeSourceDownloader {
    pub fn successful(inspection: SourceInspection) -> Self {
        Self {
            outcome: Arc::new(FakeInspectionOutcome::Success(inspection)),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn failing(error: DownloadError) -> Self {
        Self {
            outcome: Arc::new(FakeInspectionOutcome::Failure(error)),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl SourceDownloader for FakeSourceDownloader {
    async fn inspect(&self, _source: &SourceInput) -> Result<SourceInspection, DownloadError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        match self.outcome.as_ref() {
            FakeInspectionOutcome::Success(inspection) => Ok(inspection.clone()),
            FakeInspectionOutcome::Failure(error) => Err(error.clone()),
        }
    }
}
