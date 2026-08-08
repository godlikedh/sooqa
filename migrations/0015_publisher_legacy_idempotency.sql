-- Owner module: publisher

-- Migration 0014 cannot tell whether an existing unscoped row came from the
-- schedule or publish-now command. Preserve those rows as legacy records so
-- the new command scopes never mistake one operation for the other.
ALTER TABLE publication_schedules
    DROP CONSTRAINT IF EXISTS publication_schedules_idempotency_scope_check;

ALTER TABLE publication_schedules
    ADD CONSTRAINT publication_schedules_idempotency_scope_check
        CHECK (idempotency_scope IN ('legacy', 'schedule', 'publish_now'));

ALTER TABLE publication_schedules
    ALTER COLUMN idempotency_scope SET DEFAULT 'legacy';

UPDATE publication_schedules
SET idempotency_scope = 'legacy'
WHERE idempotency_scope = 'schedule';
