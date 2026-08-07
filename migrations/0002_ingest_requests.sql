-- Owner module: inbox
CREATE TABLE ingest_requests (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    kind text NOT NULL
        CHECK (kind IN ('url', 'telegram_message', 'upload')),
    status text NOT NULL DEFAULT 'received'
        CHECK (status IN (
            'received',
            'queued',
            'downloading',
            'probing',
            'exact_dedup_check',
            'normalizing',
            'fingerprinting',
            'similarity_check',
            'storing',
            'completed',
            'failed_retryable',
            'failed_terminal',
            'cancelled'
        )),
    submitted_via text NOT NULL
        CHECK (submitted_via IN ('api', 'companion', 'telegram_bot')),
    submitted_by_admin_id uuid REFERENCES admins(id),
    original_input jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_url text,
    page_url text,
    page_title text,
    supplied_caption text,
    supplied_tags text[] NOT NULL DEFAULT '{}',
    idempotency_key text,
    error_code text,
    error_message text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    CHECK (kind <> 'url' OR source_url IS NOT NULL)
);

CREATE INDEX ingest_requests_status_idx
    ON ingest_requests (status, created_at);

CREATE UNIQUE INDEX ingest_requests_idempotency_key_idx
    ON ingest_requests (idempotency_key)
    WHERE idempotency_key IS NOT NULL;
