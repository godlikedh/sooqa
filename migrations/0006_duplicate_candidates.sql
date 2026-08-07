-- Owner module: library
CREATE TABLE duplicate_candidates (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    left_content_item_id uuid NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    right_content_item_id uuid NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    algorithm_version text NOT NULL,
    score_basis_points smallint NOT NULL CHECK (score_basis_points BETWEEN 0 AND 10000),
    evidence_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'confirmed_variant', 'kept_separate', 'dismissed')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    resolved_at timestamptz,
    CONSTRAINT duplicate_candidates_distinct_items
        CHECK (left_content_item_id <> right_content_item_id),
    CONSTRAINT duplicate_candidates_ordered_items
        CHECK (left_content_item_id < right_content_item_id),
    UNIQUE (left_content_item_id, right_content_item_id, algorithm_version)
);

CREATE INDEX duplicate_candidates_pending_idx
    ON duplicate_candidates (status, score_basis_points DESC, updated_at DESC)
    WHERE status = 'pending';
