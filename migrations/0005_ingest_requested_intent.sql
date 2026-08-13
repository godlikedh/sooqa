ALTER TABLE ingests
    ADD COLUMN requested_action text NOT NULL DEFAULT 'save',
    ADD COLUMN requested_publish_at timestamptz,
    ADD COLUMN requested_post_caption text,
    ADD COLUMN requested_channel_id uuid REFERENCES channels(id);

ALTER TABLE ingests
    ADD CONSTRAINT ingests_requested_action_check
        CHECK (requested_action IN ('save', 'queue', 'post_now')),
    ADD CONSTRAINT ingests_save_intent_fields_check
        CHECK (
            requested_action <> 'save'
            OR (requested_publish_at IS NULL AND requested_post_caption IS NULL)
        ),
    ADD CONSTRAINT ingests_post_now_intent_time_check
        CHECK (requested_action <> 'post_now' OR requested_publish_at IS NULL),
    ADD CONSTRAINT ingests_requested_channel_check
        CHECK (
            (requested_action = 'save' AND requested_channel_id IS NULL)
            OR (requested_action IN ('queue', 'post_now') AND requested_channel_id IS NOT NULL)
        );
