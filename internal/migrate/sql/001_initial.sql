-- pgcdc internal tables
CREATE TABLE IF NOT EXISTS pgcdc_checkpoints (
    slot_name TEXT PRIMARY KEY,
    lsn BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pgcdc_dead_letters (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL,
    adapter TEXT NOT NULL,
    error TEXT NOT NULL,
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pgcdc_heartbeat (
    id INT PRIMARY KEY DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
