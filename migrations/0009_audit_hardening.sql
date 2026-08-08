-- Owner modules: jobs, library, storage

-- Storage upload reservations are durable leases. A worker owns a pending
-- intent only for the bounded reservation window; an expired reservation is
-- made explicitly ambiguous before another attempt can observe it.
ALTER TABLE idempotency_records
    ADD COLUMN reservation_owner uuid,
    ADD COLUMN reservation_expires_at timestamptz;

CREATE INDEX idempotency_records_storage_reservation_idx
    ON idempotency_records (scope, reservation_expires_at)
    WHERE scope = 'storage:upload' AND resource_id IS NULL;

-- Enforce the digest invariant at the source of truth, not only in Rust
-- repository helpers.
ALTER TABLE media_assets
    ADD CONSTRAINT media_assets_sha256_length_check
    CHECK (sha256 IS NULL OR octet_length(sha256) = 32);

-- A canonical pointer must reference the canonical asset owned by the same
-- content item. PostgreSQL cannot express the role predicate in a normal FK,
-- so keep the invariant in small, local constraint triggers.
CREATE FUNCTION validate_content_item_canonical_asset()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.canonical_asset_id IS NULL THEN
        RETURN NEW;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM media_assets
        WHERE id = NEW.canonical_asset_id
          AND content_item_id = NEW.id
          AND role = 'canonical'
    ) THEN
        RAISE EXCEPTION 'canonical asset must belong to the content item and have canonical role'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION validate_media_asset_canonical_reference()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM content_items
        WHERE canonical_asset_id = OLD.id
    ) THEN
        IF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION 'media asset is referenced as a canonical asset and cannot be deleted'
                USING ERRCODE = '23514';
        END IF;
        IF NEW.content_item_id <> OLD.content_item_id
           OR NEW.role <> 'canonical' THEN
            RAISE EXCEPTION 'media asset is referenced as a canonical asset and cannot change ownership or role'
                USING ERRCODE = '23514';
        END IF;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER content_items_canonical_asset_check
    BEFORE INSERT OR UPDATE OF canonical_asset_id ON content_items
    FOR EACH ROW
    EXECUTE FUNCTION validate_content_item_canonical_asset();

CREATE TRIGGER media_assets_canonical_reference_check
    BEFORE UPDATE OF content_item_id, role OR DELETE ON media_assets
    FOR EACH ROW
    EXECUTE FUNCTION validate_media_asset_canonical_reference();

ALTER TABLE job_attempts
    ADD CONSTRAINT job_attempts_status_check
    CHECK (status IN ('running', 'retry_wait', 'succeeded', 'failed', 'cancelled')),
    ADD CONSTRAINT job_attempts_finished_state_check
    CHECK (
        (status = 'running' AND finished_at IS NULL)
        OR (status <> 'running' AND finished_at IS NOT NULL)
    );

ALTER TABLE jobs
    ADD CONSTRAINT jobs_lease_state_check
    CHECK (
        (status = 'running'
            AND lease_owner IS NOT NULL
            AND lease_expires_at IS NOT NULL
            AND last_heartbeat_at IS NOT NULL
            AND completed_at IS NULL)
        OR (status <> 'running'
            AND lease_owner IS NULL
            AND lease_expires_at IS NULL
            AND last_heartbeat_at IS NULL)
    );
