-- Bounded previews and caption synchronization belong to the media aggregate.
-- No source or original-media backfill is performed by this migration.
ALTER TABLE media
    ADD COLUMN preview_bytes bytea,
    ADD COLUMN preview_mime_type text,
    ADD COLUMN preview_width integer,
    ADD COLUMN preview_height integer,
    ADD COLUMN preview_sha256 bytea,
    ADD COLUMN caption_sync_generation integer NOT NULL DEFAULT 0,
    ADD COLUMN caption_sync_state text NOT NULL DEFAULT 'not_required',
    ADD COLUMN caption_sync_error text,
    ADD COLUMN caption_sync_claim_token uuid;

ALTER TABLE media
    ADD CONSTRAINT media_preview_fields_check CHECK (
        (preview_bytes IS NULL
            AND preview_mime_type IS NULL
            AND preview_width IS NULL
            AND preview_height IS NULL
            AND preview_sha256 IS NULL)
        OR (preview_bytes IS NOT NULL
            AND preview_mime_type IN ('image/jpeg', 'image/png')
            AND octet_length(preview_bytes) BETWEEN 1 AND 131072
            AND preview_width BETWEEN 1 AND 320
            AND preview_height BETWEEN 1 AND 320
            AND preview_sha256 IS NOT NULL
            AND octet_length(preview_sha256) = 32)
    ),
    ADD CONSTRAINT media_audio_preview_check CHECK (
        kind <> 'audio' OR preview_bytes IS NULL
    ),
    ADD CONSTRAINT media_caption_sync_generation_check CHECK (
        caption_sync_generation >= 0
    ),
    ADD CONSTRAINT media_caption_sync_state_check CHECK (
        caption_sync_state IN ('not_required', 'pending', 'syncing', 'synced', 'failed')
    );

ALTER TABLE queue.jobs
    DROP CONSTRAINT jobs_kind_check;

ALTER TABLE queue.jobs
    ADD CONSTRAINT jobs_kind_check CHECK (kind IN (
        'inspect_source',
        'download_source',
        'probe_asset',
        'normalize_asset',
        'compute_fingerprint',
        'finalize_ingest',
        'materialize_publication',
        'upload_storage_asset',
        'sync_storage_caption',
        'publish_post',
        'cleanup_workspace',
        'recover_stale_jobs'
    ));
