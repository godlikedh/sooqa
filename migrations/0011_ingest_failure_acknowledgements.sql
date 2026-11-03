ALTER TABLE ingests
    ADD COLUMN acknowledged_failure_at timestamptz;

COMMENT ON COLUMN ingests.acknowledged_failure_at IS
    'Failure identity acknowledged on the dashboard: completed_at, or updated_at when completion is absent. Not the acknowledgement time.';
