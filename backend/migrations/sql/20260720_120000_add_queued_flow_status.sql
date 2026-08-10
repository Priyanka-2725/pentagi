-- +goose Up
-- +goose StatementBegin
-- Add queued to the flow_status enum
CREATE TYPE FLOW_STATUS_NEW AS ENUM (
  'created',
  'queued',
  'running',
  'waiting',
  'finished',
  'failed'
);

-- Update the flows table to use the new enum type
ALTER TABLE flows
    ALTER COLUMN status TYPE FLOW_STATUS_NEW USING status::text::FLOW_STATUS_NEW;

-- Drop the old type and rename the new one
DROP TYPE FLOW_STATUS;
ALTER TYPE FLOW_STATUS_NEW RENAME TO FLOW_STATUS;

-- Ensure constraints are preserved
ALTER TABLE flows
    ALTER COLUMN status SET NOT NULL;
ALTER TABLE flows
    ALTER COLUMN status SET DEFAULT 'created';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- Revert queued flows to created before narrowing the enum
UPDATE flows
    SET status = 'created'
    WHERE status = 'queued';

-- Recreate enum without queued
CREATE TYPE FLOW_STATUS_NEW AS ENUM (
  'created',
  'running',
  'waiting',
  'finished',
  'failed'
);

-- Update the flows table to use the reverted enum type
ALTER TABLE flows
    ALTER COLUMN status TYPE FLOW_STATUS_NEW USING status::text::FLOW_STATUS_NEW;

-- Drop the new type and rename the reverted one
DROP TYPE FLOW_STATUS;
ALTER TYPE FLOW_STATUS_NEW RENAME TO FLOW_STATUS;

-- Ensure constraints are preserved
ALTER TABLE flows
    ALTER COLUMN status SET NOT NULL;
ALTER TABLE flows
    ALTER COLUMN status SET DEFAULT 'created';
-- +goose StatementEnd
