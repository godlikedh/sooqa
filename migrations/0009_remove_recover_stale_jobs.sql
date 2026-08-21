-- Stale-lease recovery is periodic worker maintenance, not a durable job.
-- Refuse to rewrite an unsupported row if one was inserted by an old binary.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM queue.jobs
        WHERE kind = 'recover_stale_jobs'
    ) THEN
        RAISE EXCEPTION
            'cannot remove recover_stale_jobs while queue.jobs contains that kind';
    END IF;

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
            'cleanup_workspace'
        ));
END
$$;
