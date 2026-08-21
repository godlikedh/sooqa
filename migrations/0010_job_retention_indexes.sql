-- Keep the hot claim and stale-recovery paths on partial indexes.  The
-- original all-state claim index indexed terminal history forever, so replace
-- it in this forward-only migration with the live-state index below.
DROP INDEX IF EXISTS queue.queue_jobs_claim_idx;

CREATE INDEX queue_jobs_queued_claim_idx
    ON queue.jobs (priority DESC, run_at ASC, created_at ASC, id ASC)
    WHERE state = 'queued';

CREATE INDEX queue_jobs_running_expiry_idx
    ON queue.jobs (lease_expires_at ASC, id ASC)
    WHERE state = 'running';

CREATE INDEX queue_jobs_terminal_retention_idx
    ON queue.jobs (completed_at ASC, id ASC)
    WHERE state IN ('succeeded', 'failed', 'cancelled');
