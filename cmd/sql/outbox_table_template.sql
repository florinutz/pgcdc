-- pgcdc outbox table: {{.Table}}
-- Rows inserted into this table are polled by pgcdc and forwarded to adapters.

CREATE TABLE IF NOT EXISTS {{.Table}} (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    channel      TEXT NOT NULL,
    operation    TEXT NOT NULL DEFAULT 'NOTIFY',
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at TIMESTAMPTZ
);

-- Index for efficient polling of unprocessed rows.
CREATE INDEX IF NOT EXISTS idx_{{.Table}}_unprocessed
    ON {{.Table}} (created_at)
    WHERE processed_at IS NULL;
