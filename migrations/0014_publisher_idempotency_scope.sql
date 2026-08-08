-- Owner module: publisher

-- Schedule and publish-now are different externally retryable commands. Keep
-- the caller's key unchanged while making their durable idempotency scopes
-- explicit in the source-of-truth table.
ALTER TABLE publication_schedules
    DROP CONSTRAINT IF EXISTS publication_schedules_idempotency_key_key;

ALTER TABLE publication_schedules
    ADD COLUMN idempotency_scope text NOT NULL DEFAULT 'schedule'
        CHECK (idempotency_scope IN ('schedule', 'publish_now'));

CREATE UNIQUE INDEX publication_schedules_idempotency_idx
    ON publication_schedules (idempotency_scope, idempotency_key);
