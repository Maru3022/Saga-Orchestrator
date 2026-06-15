-- V1__create_saga_instances.sql
-- Create saga_instances table

CREATE TABLE IF NOT EXISTS saga_instances (
    id UUID PRIMARY KEY,
    correlation_id VARCHAR(36) NOT NULL UNIQUE,
    user_id VARCHAR(36) NOT NULL,
    user_email VARCHAR(255) NOT NULL,
    saga_payload TEXT,
    status VARCHAR(50) NOT NULL DEFAULT 'STARTED',
    current_step VARCHAR(50) NOT NULL DEFAULT 'CREATE_USER',
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    
    CONSTRAINT status_valid CHECK (status IN (
        'STARTED', 'USER_CREATED', 'NOTIFICATION_SENT', 'NOTIFICATION_FAILED',
        'CABINET_CREATING', 'CABINET_CREATED', 'CABINET_FAILED',
        'NUTRITION_CALCULATING', 'NUTRITION_CALCULATED', 'NUTRITION_FAILED',
        'COMPLETED', 'COMPENSATING', 'COMPENSATED', 'FAILED'
    )),
    
    CONSTRAINT current_step_valid CHECK (current_step IN (
        'CREATE_USER', 'SEND_NOTIFICATION', 'CREATE_CABINET', 'CALCULATE_NUTRITION'
    ))
);

-- Create indexes
CREATE INDEX idx_saga_instances_correlation_id ON saga_instances(correlation_id);
CREATE INDEX idx_saga_instances_status ON saga_instances(status);
CREATE INDEX idx_saga_instances_user_id ON saga_instances(user_id);
CREATE INDEX idx_saga_instances_created_at ON saga_instances(created_at DESC);
CREATE INDEX idx_saga_instances_updated_at ON saga_instances(updated_at DESC);

-- Create trigger to automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION update_saga_instances_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER saga_instances_timestamp_trigger
BEFORE UPDATE ON saga_instances
FOR EACH ROW
EXECUTE PROCEDURE update_saga_instances_timestamp();
