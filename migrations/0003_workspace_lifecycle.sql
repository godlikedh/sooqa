ALTER TABLE ingests
    ADD COLUMN workspace_id uuid;

-- Before this migration, URL workspaces were rooted at the ingest ID while
-- Telegram workspaces used the durable ID captured in input_json. Preserve
-- both identities so upgrading a live database never makes existing bytes
-- look orphaned to the worker or the reconciler.
UPDATE ingests
SET workspace_id = CASE
    WHEN input_kind = 'telegram_message'
         AND input_json->>'telegram_workspace_id' ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        THEN (input_json->>'telegram_workspace_id')::uuid
    ELSE id
END;

ALTER TABLE ingests
    ALTER COLUMN workspace_id SET NOT NULL;

ALTER TABLE ingests
    ALTER COLUMN workspace_id DROP DEFAULT;
