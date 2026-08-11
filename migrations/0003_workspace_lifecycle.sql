ALTER TABLE ingests
    ADD COLUMN workspace_id uuid NOT NULL DEFAULT gen_random_uuid();

ALTER TABLE ingests
    ALTER COLUMN workspace_id DROP DEFAULT;
