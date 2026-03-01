-- Add replayed_at column to dead letters table for replay tracking,
-- and make payload NOT NULL to match application expectations.
ALTER TABLE pgcdc_dead_letters ADD COLUMN IF NOT EXISTS replayed_at TIMESTAMPTZ;

DO $$
BEGIN
    -- Make payload NOT NULL if it isn't already.
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'pgcdc_dead_letters'
          AND column_name = 'payload'
          AND is_nullable = 'YES'
    ) THEN
        -- Backfill any existing NULL payloads before adding constraint.
        UPDATE pgcdc_dead_letters SET payload = '{}'::jsonb WHERE payload IS NULL;
        ALTER TABLE pgcdc_dead_letters ALTER COLUMN payload SET NOT NULL;
    END IF;
END $$;
