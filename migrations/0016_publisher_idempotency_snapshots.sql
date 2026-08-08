-- Owner module: publisher

-- The first Publisher API stored only an ID/status placeholder in draft
-- idempotency records. Materialize the response at migration time so future
-- retries remain immutable even if the draft changes later.
UPDATE idempotency_records AS record
SET response_body = jsonb_build_object(
    'id', draft.id,
    'content_item_id', draft.content_item_id,
    'asset_id', draft.asset_id,
    'target_channel_id', draft.target_channel_id,
    'caption', draft.caption,
    'parse_mode', draft.parse_mode,
    'status', draft.status,
    'created_at', jsonb_build_array(
        EXTRACT(YEAR FROM (draft.created_at AT TIME ZONE 'UTC'))::integer,
        EXTRACT(DOY FROM (draft.created_at AT TIME ZONE 'UTC'))::integer,
        EXTRACT(HOUR FROM (draft.created_at AT TIME ZONE 'UTC'))::integer,
        EXTRACT(MINUTE FROM (draft.created_at AT TIME ZONE 'UTC'))::integer,
        EXTRACT(SECOND FROM (draft.created_at AT TIME ZONE 'UTC'))::integer,
        EXTRACT(MICROSECONDS FROM (draft.created_at AT TIME ZONE 'UTC'))::integer % 1000000 * 1000,
        0,
        0,
        0
    ),
    'updated_at', jsonb_build_array(
        EXTRACT(YEAR FROM (draft.updated_at AT TIME ZONE 'UTC'))::integer,
        EXTRACT(DOY FROM (draft.updated_at AT TIME ZONE 'UTC'))::integer,
        EXTRACT(HOUR FROM (draft.updated_at AT TIME ZONE 'UTC'))::integer,
        EXTRACT(MINUTE FROM (draft.updated_at AT TIME ZONE 'UTC'))::integer,
        EXTRACT(SECOND FROM (draft.updated_at AT TIME ZONE 'UTC'))::integer,
        EXTRACT(MICROSECONDS FROM (draft.updated_at AT TIME ZONE 'UTC'))::integer % 1000000 * 1000,
        0,
        0,
        0
    )
)
FROM post_drafts AS draft
WHERE record.resource_type = 'post_draft'
  AND record.resource_id = draft.id
  AND record.scope IN ('publisher:draft:create', 'publisher:draft:update')
  AND (record.response_body IS NULL OR NOT (record.response_body ? 'content_item_id'));
