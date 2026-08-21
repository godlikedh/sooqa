//! Bounded durable-job worker loop for sooqa.

mod cleanup;
mod common;
mod identity;
mod ingest;
mod normalization;
mod publication;
mod runner;
mod storage;

pub use cleanup::cleanup_workspace_handler;
pub use common::{
    CancellableHandlerFn, HandlerCancellation, HandlerFailure, HandlerFn, HandlerFuture,
    HandlerRegistry, WorkspaceAdmission, media_processing_components,
};
pub use identity::{
    IdentityAlignmentHook, compute_fingerprint_handler, compute_fingerprint_handler_with_admission,
    compute_fingerprint_handler_with_alignment_hook, finalize_ingest_handler,
};
pub use ingest::{
    TelegramSourceDownloader, download_source_handler, download_source_handler_with_admission,
    inspect_source_handler, probe_asset_handler, probe_asset_handler_with_telegram_source,
    probe_asset_handler_with_telegram_source_and_admission,
};
pub use normalization::{normalize_asset_handler, normalize_asset_handler_with_admission};
pub use publication::{materialize_publication_handler, publish_post_handler};
pub use runner::{Worker, WorkerError};
pub use storage::{
    StoragePreflight, spawn_storage_preflight, sync_storage_caption_handler,
    upload_storage_asset_cancellable_handler, upload_storage_asset_handler,
};
