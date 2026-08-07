-- Owner module: admin/security
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE admins (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    telegram_user_id bigint NOT NULL UNIQUE,
    display_name text,
    is_enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Owner module: jobs
CREATE TABLE jobs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    job_type text NOT NULL,
    payload_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    status text NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'running', 'succeeded', 'retry_wait', 'failed', 'cancelled')),
    priority integer NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL DEFAULT now(),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts integer NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
    lease_owner text,
    lease_expires_at timestamptz,
    last_heartbeat_at timestamptz,
    last_error_class text,
    last_error_message text,
    idempotency_key text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz
);

CREATE INDEX jobs_claim_idx
    ON jobs (status, available_at, priority DESC, created_at);

CREATE UNIQUE INDEX jobs_idempotency_key_idx
    ON jobs (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- Owner module: jobs
CREATE TABLE job_attempts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt_number integer NOT NULL CHECK (attempt_number > 0),
    status text NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    error_class text,
    error_message text,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (job_id, attempt_number)
);

CREATE INDEX job_attempts_job_idx
    ON job_attempts (job_id, attempt_number DESC);

-- Owner module: api/security
CREATE TABLE idempotency_records (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    scope text NOT NULL,
    idempotency_key text NOT NULL,
    request_hash bytea NOT NULL,
    resource_type text,
    resource_id uuid,
    response_status integer,
    response_body jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz,
    UNIQUE (scope, idempotency_key)
);

CREATE INDEX idempotency_records_expiry_idx
    ON idempotency_records (expires_at)
    WHERE expires_at IS NOT NULL;
