-- Schema registry table for tracking schema versions.
CREATE TABLE IF NOT EXISTS pgcdc_schemas (
    subject    TEXT NOT NULL,
    version    INT NOT NULL,
    columns    JSONB NOT NULL,
    hash       TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (subject, version)
);

CREATE INDEX IF NOT EXISTS idx_pgcdc_schemas_subject
ON pgcdc_schemas (subject, version DESC);
