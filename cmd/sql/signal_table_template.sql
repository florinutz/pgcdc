-- pgcdc signal table: {{.Table}}
-- INSERT rows into this table to trigger incremental snapshots via WAL.
-- The table MUST be included in the WAL publication for signal detection.
--
-- Example: trigger an incremental snapshot of the "orders" table:
--   INSERT INTO {{.Table}} (signal, payload)
--   VALUES ('execute-snapshot', '{"table": "orders"}');
--
-- Example: stop a running snapshot:
--   INSERT INTO {{.Table}} (signal, payload)
--   VALUES ('stop-snapshot', '{"table": "orders"}');
--
-- IMPORTANT: Add this table to your publication:
--   ALTER PUBLICATION <your_publication> ADD TABLE {{.Table}};

CREATE TABLE IF NOT EXISTS {{.Table}} (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    signal     TEXT NOT NULL,
    payload    JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
