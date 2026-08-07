-- Owner module: telegram
CREATE TABLE telegram_update_receipts (
    update_id bigint PRIMARY KEY CHECK (update_id > 0),
    received_at timestamptz NOT NULL DEFAULT now()
);
