-- Clean five-table MVP baseline. Existing local databases are intentionally
-- incompatible with this migration and must be recreated explicitly.
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE SCHEMA queue;

CREATE TABLE queue.jobs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    kind text NOT NULL CHECK (kind IN (
        'inspect_source',
        'download_source',
        'probe_asset',
        'normalize_asset',
        'compute_fingerprint',
        'finalize_ingest',
        'upload_storage_asset',
        'publish_post',
        'cleanup_workspace',
        'recover_stale_jobs'
    )),
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    state text NOT NULL DEFAULT 'queued'
        CHECK (state IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
    run_at timestamptz NOT NULL DEFAULT now(),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts integer NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
    lease_token uuid,
    lease_owner text,
    lease_expires_at timestamptz,
    last_heartbeat_at timestamptz,
    error_class text,
    error_message text,
    dedupe_key text,
    priority integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    CHECK (
        (state = 'running'
            AND lease_token IS NOT NULL
            AND lease_owner IS NOT NULL
            AND lease_expires_at IS NOT NULL
            AND last_heartbeat_at IS NOT NULL
            AND completed_at IS NULL)
        OR (state <> 'running'
            AND lease_token IS NULL
            AND lease_owner IS NULL
            AND lease_expires_at IS NULL
            AND last_heartbeat_at IS NULL)
    )
);

CREATE INDEX queue_jobs_claim_idx
    ON queue.jobs (state, run_at, priority DESC, created_at);

CREATE UNIQUE INDEX queue_jobs_dedupe_idx
    ON queue.jobs (dedupe_key)
    WHERE dedupe_key IS NOT NULL;

CREATE TABLE ingests (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    input_key text NOT NULL UNIQUE,
    request_hash bytea NOT NULL,
    input_kind text NOT NULL CHECK (input_kind IN ('url', 'telegram_message', 'upload')),
    state text NOT NULL DEFAULT 'queued'
        CHECK (state IN (
            'received',
            'queued',
            'downloading',
            'probing',
            'normalizing',
            'exact_dedup_check',
            'duplicate_pending',
            'fingerprinting',
            'storing',
            'completed',
            'failed_retryable',
            'failed_terminal',
            'cancelled'
        )),
    submitted_via text NOT NULL CHECK (submitted_via IN ('api', 'companion', 'telegram_bot')),
    input_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_url text,
    page_url text,
    page_title text,
    supplied_caption text,
    supplied_tags text[] NOT NULL DEFAULT '{}',
    media_id uuid,
    force_save boolean NOT NULL DEFAULT false,
    duplicate_evidence jsonb,
    telegram_status_chat_id bigint,
    telegram_status_message_id bigint,
    error_code text,
    error_message text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    CHECK (input_kind <> 'url' OR source_url IS NOT NULL)
);

CREATE INDEX ingests_state_idx ON ingests (state, created_at);

CREATE TABLE media (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    kind text NOT NULL CHECK (kind IN ('video', 'image', 'animation', 'audio')),
    storage_state text NOT NULL DEFAULT 'pending_storage'
        CHECK (storage_state IN ('pending_storage', 'ready', 'storage_unknown', 'missing')),
    canonical_sha256 bytea,
    fingerprint_version text,
    fingerprint_data bytea,
    fingerprint_search_tokens bigint[],
    title text,
    description text,
    tags text[] NOT NULL DEFAULT '{}',
    source_url text,
    source_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    mime_type text,
    container text,
    video_codec text,
    audio_codec text,
    width integer CHECK (width IS NULL OR width > 0),
    height integer CHECK (height IS NULL OR height > 0),
    duration_ms bigint CHECK (duration_ms IS NULL OR duration_ms >= 0),
    bit_rate bigint CHECK (bit_rate IS NULL OR bit_rate >= 0),
    file_size_bytes bigint CHECK (file_size_bytes IS NULL OR file_size_bytes >= 0),
    local_work_path text,
    telegram_storage_chat_id bigint,
    telegram_storage_message_id bigint,
    telegram_file_id text,
    telegram_file_unique_id text,
    storage_generation integer NOT NULL DEFAULT 0 CHECK (storage_generation >= 0),
    storage_token uuid,
    storage_started_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    stored_at timestamptz,
    CHECK (canonical_sha256 IS NULL OR octet_length(canonical_sha256) = 32),
    CHECK (
        (storage_state = 'ready'
            AND telegram_storage_chat_id IS NOT NULL
            AND telegram_storage_message_id IS NOT NULL
            AND telegram_file_id IS NOT NULL)
        OR storage_state <> 'ready'
    )
);

CREATE UNIQUE INDEX media_canonical_sha256_idx
    ON media (canonical_sha256);
CREATE INDEX media_tags_gin_idx ON media USING gin (tags);
CREATE INDEX media_video_fingerprint_tokens_gin_idx
    ON media USING gin (fingerprint_search_tokens)
    WHERE kind = 'video'
      AND storage_state IN ('pending_storage', 'ready')
      AND fingerprint_search_tokens IS NOT NULL;
CREATE INDEX media_search_idx ON media (kind, storage_state, updated_at DESC);

ALTER TABLE ingests
    ADD CONSTRAINT ingests_media_fk FOREIGN KEY (media_id) REFERENCES media(id);

CREATE TABLE channels (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    telegram_chat_id bigint NOT NULL UNIQUE,
    name text NOT NULL CHECK (length(btrim(name)) > 0),
    is_enabled boolean NOT NULL DEFAULT true,
    time_zone text NOT NULL DEFAULT 'UTC' CHECK (length(btrim(time_zone)) > 0),
    window_start time NOT NULL DEFAULT '08:00',
    window_end time NOT NULL DEFAULT '22:00',
    interval_minutes integer NOT NULL DEFAULT 30 CHECK (interval_minutes > 0),
    default_parse_mode text,
    default_disable_notification boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (window_start < window_end)
);

CREATE TABLE posts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    request_key text,
    request_hash bytea,
    schedule_request_key text,
    schedule_request_hash bytea,
    media_id uuid NOT NULL REFERENCES media(id),
    channel_id uuid NOT NULL REFERENCES channels(id),
    state text NOT NULL DEFAULT 'draft'
        CHECK (state IN ('draft', 'queued', 'sending', 'published', 'unknown', 'failed', 'cancelled')),
    caption text,
    parse_mode text,
    disable_notification boolean NOT NULL DEFAULT false,
    scheduled_at timestamptz NOT NULL DEFAULT now(),
    cadence_slot_at timestamptz,
    send_generation integer NOT NULL DEFAULT 0 CHECK (send_generation >= 0),
    send_token uuid,
    send_started_at timestamptz,
    telegram_message_id bigint CHECK (telegram_message_id IS NULL OR telegram_message_id > 0),
    published_at timestamptz,
    error_class text,
    error_message text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (
        (state = 'sending' AND send_token IS NOT NULL AND send_started_at IS NOT NULL)
        OR (state <> 'sending' AND send_token IS NULL AND send_started_at IS NULL)
    ),
    CHECK (
        (state = 'published' AND telegram_message_id IS NOT NULL AND published_at IS NOT NULL)
        OR state <> 'published'
    )
);

CREATE UNIQUE INDEX posts_request_key_idx
    ON posts (request_key)
    WHERE request_key IS NOT NULL;
CREATE INDEX posts_queue_idx ON posts (state, scheduled_at, channel_id);
CREATE INDEX posts_media_idx ON posts (media_id, created_at DESC);
