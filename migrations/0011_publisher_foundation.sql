-- Owner module: publisher

CREATE TABLE target_channels (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name text NOT NULL CHECK (length(btrim(name)) > 0),
    telegram_chat_id bigint NOT NULL UNIQUE,
    is_enabled boolean NOT NULL DEFAULT true,
    default_parse_mode text,
    default_disable_notification boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE channel_policies (
    target_channel_id uuid PRIMARY KEY REFERENCES target_channels(id) ON DELETE CASCADE,
    minimum_post_interval_seconds bigint NOT NULL DEFAULT 0
        CHECK (minimum_post_interval_seconds >= 0),
    same_content_cooldown_seconds bigint NOT NULL DEFAULT 0
        CHECK (same_content_cooldown_seconds >= 0),
    similar_content_cooldown_seconds bigint NOT NULL DEFAULT 0
        CHECK (similar_content_cooldown_seconds >= 0),
    similarity_threshold double precision NOT NULL DEFAULT 0.75
        CHECK (similarity_threshold >= 0 AND similarity_threshold <= 1),
    on_cooldown_violation text NOT NULL DEFAULT 'warn'
        CHECK (on_cooldown_violation IN ('warn', 'block', 'allow')),
    allowed_windows_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    max_posts_per_day integer CHECK (max_posts_per_day IS NULL OR max_posts_per_day > 0),
    jitter_seconds bigint NOT NULL DEFAULT 0 CHECK (jitter_seconds >= 0),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE post_drafts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    content_item_id uuid NOT NULL REFERENCES content_items(id),
    asset_id uuid NOT NULL REFERENCES media_assets(id),
    target_channel_id uuid NOT NULL REFERENCES target_channels(id),
    caption text,
    parse_mode text,
    status text NOT NULL DEFAULT 'editing'
        CHECK (status IN ('editing', 'ready', 'scheduled', 'published', 'cancelled')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX post_drafts_content_item_idx ON post_drafts (content_item_id, updated_at DESC);
CREATE INDEX post_drafts_channel_status_idx
    ON post_drafts (target_channel_id, status, updated_at DESC);

CREATE TABLE publication_schedules (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    post_draft_id uuid NOT NULL REFERENCES post_drafts(id),
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'queued', 'publishing', 'published', 'failed', 'cancelled')),
    publish_at timestamptz NOT NULL,
    not_before timestamptz,
    not_after timestamptz,
    priority integer NOT NULL DEFAULT 0,
    cooldown_override boolean,
    idempotency_key text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (not_before IS NULL OR not_after IS NULL OR not_before <= not_after)
);

CREATE INDEX publication_schedules_due_idx
    ON publication_schedules (status, publish_at, priority DESC, created_at)
    WHERE status IN ('pending', 'queued', 'failed');

CREATE TABLE publication_attempts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    publication_schedule_id uuid NOT NULL REFERENCES publication_schedules(id) ON DELETE CASCADE,
    attempt_number integer NOT NULL CHECK (attempt_number > 0),
    status text NOT NULL
        CHECK (status IN ('running', 'succeeded', 'failed', 'unknown')),
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    telegram_request_key text,
    error_class text,
    error_message text,
    response_json jsonb,
    UNIQUE (publication_schedule_id, attempt_number),
    CHECK (
        (status = 'running' AND finished_at IS NULL)
        OR (status <> 'running' AND finished_at IS NOT NULL)
    )
);

CREATE INDEX publication_attempts_schedule_idx
    ON publication_attempts (publication_schedule_id, attempt_number DESC);

CREATE TABLE published_posts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    publication_schedule_id uuid NOT NULL UNIQUE
        REFERENCES publication_schedules(id),
    content_item_id uuid NOT NULL REFERENCES content_items(id),
    asset_id uuid NOT NULL REFERENCES media_assets(id),
    target_channel_id uuid NOT NULL REFERENCES target_channels(id),
    telegram_chat_id bigint NOT NULL,
    telegram_message_id bigint NOT NULL CHECK (telegram_message_id > 0),
    caption_snapshot text,
    published_at timestamptz NOT NULL DEFAULT now(),
    status text NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'edited', 'deleted', 'unknown')),
    UNIQUE (target_channel_id, telegram_message_id)
);

CREATE INDEX published_posts_content_channel_idx
    ON published_posts (content_item_id, target_channel_id, published_at DESC);
