-- Owner module: library
CREATE TABLE content_items (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    kind text NOT NULL CHECK (kind IN ('video', 'image', 'animation')),
    status text NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'archived', 'deleted')),
    canonical_asset_id uuid,
    preferred_title text,
    editorial_description text,
    notes text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    archived_at timestamptz
);

CREATE TABLE media_assets (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    content_item_id uuid NOT NULL REFERENCES content_items(id),
    role text NOT NULL CHECK (role IN ('original', 'canonical', 'preview', 'thumbnail')),
    media_kind text NOT NULL CHECK (media_kind IN ('video', 'image', 'audio', 'animation')),
    mime_type text,
    container text,
    video_codec text,
    audio_codec text,
    width integer CHECK (width IS NULL OR width > 0),
    height integer CHECK (height IS NULL OR height > 0),
    duration_ms bigint CHECK (duration_ms IS NULL OR duration_ms >= 0),
    bit_rate bigint CHECK (bit_rate IS NULL OR bit_rate >= 0),
    file_size_bytes bigint CHECK (file_size_bytes IS NULL OR file_size_bytes >= 0),
    sha256 bytea,
    local_work_path text,
    storage_state text NOT NULL DEFAULT 'local'
        CHECK (storage_state IN ('local', 'uploaded', 'missing')),
    created_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE content_items
    ADD CONSTRAINT content_items_canonical_asset_fk
    FOREIGN KEY (canonical_asset_id) REFERENCES media_assets(id);

CREATE UNIQUE INDEX media_assets_sha256_idx
    ON media_assets (sha256)
    WHERE sha256 IS NOT NULL;

CREATE INDEX media_assets_content_item_idx
    ON media_assets (content_item_id, role);

CREATE TABLE source_records (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    content_item_id uuid NOT NULL REFERENCES content_items(id),
    ingest_request_id uuid REFERENCES ingest_requests(id) ON DELETE SET NULL,
    source_type text NOT NULL
        CHECK (source_type IN ('webpage', 'direct_url', 'youtube', 'telegram', 'upload')),
    original_url text,
    normalized_url text,
    platform text,
    platform_content_id text,
    author_name text,
    source_title text,
    source_description text,
    source_published_at timestamptz,
    retrieved_at timestamptz NOT NULL DEFAULT now(),
    metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE UNIQUE INDEX source_records_normalized_url_idx
    ON source_records (normalized_url)
    WHERE normalized_url IS NOT NULL;

CREATE UNIQUE INDEX source_records_platform_identity_idx
    ON source_records (platform, platform_content_id)
    WHERE platform IS NOT NULL AND platform_content_id IS NOT NULL;

CREATE INDEX source_records_content_item_idx
    ON source_records (content_item_id, retrieved_at DESC);

CREATE TABLE tags (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    normalized_name text NOT NULL UNIQUE,
    display_name text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE content_item_tags (
    content_item_id uuid NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    tag_id uuid NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (content_item_id, tag_id)
);

CREATE INDEX content_item_tags_tag_idx
    ON content_item_tags (tag_id, content_item_id);

CREATE TABLE storage_objects (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    asset_id uuid NOT NULL REFERENCES media_assets(id),
    provider text NOT NULL,
    storage_chat_id bigint NOT NULL,
    storage_message_id bigint NOT NULL,
    telegram_file_id text,
    telegram_file_unique_id text,
    media_kind text NOT NULL CHECK (media_kind IN ('video', 'image', 'audio', 'animation')),
    stored_at timestamptz NOT NULL DEFAULT now(),
    verified_at timestamptz,
    status text NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'missing', 'inaccessible', 'deleted'))
);

CREATE UNIQUE INDEX storage_objects_provider_message_idx
    ON storage_objects (provider, storage_chat_id, storage_message_id);

CREATE INDEX storage_objects_asset_idx
    ON storage_objects (asset_id, status);
