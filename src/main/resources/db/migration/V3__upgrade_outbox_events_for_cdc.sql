ALTER TABLE outbox_events
    ADD COLUMN aggregate_type VARCHAR(100),
    ADD COLUMN aggregate_id VARCHAR(100);

UPDATE outbox_events
SET aggregate_type = 'saga',
    aggregate_id = COALESCE(saga_id::TEXT, correlation_id, id::TEXT);

ALTER TABLE outbox_events
    ALTER COLUMN aggregate_type SET NOT NULL,
    ALTER COLUMN aggregate_id SET NOT NULL,
    ALTER COLUMN correlation_id DROP NOT NULL,
    ALTER COLUMN saga_id DROP NOT NULL,
    ALTER COLUMN topic DROP NOT NULL,
    ALTER COLUMN payload TYPE JSONB USING payload::JSONB,
    ALTER COLUMN created_at TYPE TIMESTAMPTZ USING created_at AT TIME ZONE 'UTC';
