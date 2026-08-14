-- Publication materialization stores the requested action, its repeat-review
-- evidence, and the optional originating ingest on the intended post row.
ALTER TABLE posts
    ADD COLUMN origin_ingest_id uuid REFERENCES ingests(id),
    ADD COLUMN requested_action text NOT NULL DEFAULT 'queue',
    ADD COLUMN requested_publish_at timestamptz,
    ADD COLUMN repeat_evidence jsonb,
    ADD COLUMN decision_request_key text,
    ADD COLUMN decision_request_hash bytea;

ALTER TABLE posts
    ADD CONSTRAINT posts_origin_ingest_unique UNIQUE (origin_ingest_id),
    ADD CONSTRAINT posts_requested_action_check
        CHECK (requested_action IN ('queue', 'post_now')),
    ADD CONSTRAINT posts_requested_action_time_check
        CHECK (requested_action <> 'post_now' OR requested_publish_at IS NULL),
    ADD CONSTRAINT posts_repeat_evidence_size_check
        CHECK (repeat_evidence IS NULL OR octet_length(repeat_evidence::text) <= 16384);

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
        'publish_post',
        'cleanup_workspace',
        'recover_stale_jobs'
    ));
