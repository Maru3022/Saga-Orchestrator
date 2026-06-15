-- V2__create_outbox_events.sql
-- Create outbox table for reliable event publishing

CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    correlation_id VARCHAR(36) NOT NULL,
    saga_id UUID NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload TEXT NOT NULL,
    topic VARCHAR(255) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMP,
    last_error TEXT,
    
    CONSTRAINT outbox_status_valid CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED')),
    CONSTRAINT outbox_fk_saga_id FOREIGN KEY (saga_id) REFERENCES saga_instances(id) ON DELETE CASCADE
);

-- Create indexes for outbox
CREATE INDEX idx_outbox_events_status ON outbox_events(status);
CREATE INDEX idx_outbox_events_correlation_id ON outbox_events(correlation_id);
CREATE INDEX idx_outbox_events_saga_id ON outbox_events(saga_id);
CREATE INDEX idx_outbox_events_created_at ON outbox_events(created_at DESC);
CREATE INDEX idx_outbox_events_status_created_at ON outbox_events(status, created_at);

-- Create trigger to automatically update published_at
CREATE OR REPLACE FUNCTION update_outbox_events_published_at()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.status = 'PUBLISHED' AND OLD.status != 'PUBLISHED' THEN
        NEW.published_at = CURRENT_TIMESTAMP;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER outbox_events_published_at_trigger
BEFORE UPDATE ON outbox_events
FOR EACH ROW
EXECUTE PROCEDURE update_outbox_events_published_at();
