-- Owner module: telegram
CREATE TABLE telegram_update_receipts (
    update_id bigint PRIMARY KEY CHECK (update_id > 0),
    received_at timestamptz NOT NULL DEFAULT now(),
    claim_token uuid,
    claimed_at timestamptz,
    completed_at timestamptz,
    CHECK ((claim_token IS NULL) = (claimed_at IS NULL))
);

CREATE INDEX telegram_update_receipts_active_idx
    ON telegram_update_receipts (claimed_at)
    WHERE completed_at IS NULL;
