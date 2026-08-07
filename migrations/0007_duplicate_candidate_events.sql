-- Owner module: library
CREATE TABLE duplicate_candidate_events (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id uuid NOT NULL REFERENCES duplicate_candidates(id) ON DELETE CASCADE,
    action text NOT NULL
        CHECK (action IN ('confirm_variant', 'keep_separate', 'dismiss')),
    actor_device_token_id uuid REFERENCES device_tokens(id) ON DELETE SET NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX duplicate_candidate_events_candidate_idx
    ON duplicate_candidate_events (candidate_id, created_at DESC);

ALTER TABLE duplicate_candidates
    ADD CONSTRAINT duplicate_candidates_resolution_timestamp_check
    CHECK (
        (status = 'pending' AND resolved_at IS NULL)
        OR (status <> 'pending' AND resolved_at IS NOT NULL)
    );
