-- Owner module: api/security
CREATE TABLE device_tokens (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name text NOT NULL,
    token_prefix text NOT NULL,
    token_hash bytea NOT NULL,
    scopes text[] NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now(),
    last_used_at timestamptz,
    revoked_at timestamptz
);

CREATE UNIQUE INDEX device_tokens_hash_idx
    ON device_tokens (token_hash);

CREATE INDEX device_tokens_active_idx
    ON device_tokens (revoked_at)
    WHERE revoked_at IS NULL;
