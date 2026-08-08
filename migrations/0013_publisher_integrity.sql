-- Owner modules: publisher, library

-- An ambiguous Telegram result must be held for explicit reconciliation; it
-- must not re-enter the due queue as an ordinary retry.
ALTER TABLE publication_schedules
    DROP CONSTRAINT IF EXISTS publication_schedules_status_check;

ALTER TABLE publication_schedules
    ADD CONSTRAINT publication_schedules_status_check
    CHECK (
        status IN ('pending', 'queued', 'publishing', 'published', 'failed',
                   'unknown', 'cancelled')
    );

-- A draft and its published history must reference an asset owned by the same
-- content item. The existing primary key on media_assets(id) is not enough to
-- express that composite foreign key.
CREATE UNIQUE INDEX media_assets_id_content_item_idx
    ON media_assets (id, content_item_id);

ALTER TABLE post_drafts
    ADD CONSTRAINT post_drafts_asset_content_fk
    FOREIGN KEY (asset_id, content_item_id)
    REFERENCES media_assets (id, content_item_id);

ALTER TABLE published_posts
    ADD CONSTRAINT published_posts_asset_content_fk
    FOREIGN KEY (asset_id, content_item_id)
    REFERENCES media_assets (id, content_item_id);

-- Only one publication attempt may be in flight for a schedule. A retry must
-- first finish or reconcile the previous attempt.
CREATE UNIQUE INDEX publication_attempts_running_schedule_idx
    ON publication_attempts (publication_schedule_id)
    WHERE status = 'running';
