-- Owner modules: jobs, library, storage

-- Bind each storage intent to the asset and job generation that owns it. The
-- existing request_hash remains the canonical asset digest captured before the
-- external Telegram call.
ALTER TABLE idempotency_records
    ADD COLUMN storage_asset_id uuid REFERENCES media_assets(id),
    ADD COLUMN storage_job_id uuid REFERENCES jobs(id),
    ADD COLUMN storage_generation integer NOT NULL DEFAULT 0
        CHECK (storage_generation >= 0),
    ADD COLUMN storage_provider text,
    ADD COLUMN storage_chat_id bigint;

CREATE INDEX idempotency_records_storage_asset_idx
    ON idempotency_records (storage_asset_id, storage_generation)
    WHERE scope = 'storage:upload';
