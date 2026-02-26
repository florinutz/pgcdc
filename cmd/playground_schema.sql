CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    customer TEXT NOT NULL,
    amount NUMERIC(10,2) NOT NULL,
    region TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Trigger for LISTEN/NOTIFY mode
CREATE OR REPLACE FUNCTION pgcdc_notify_orders() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('pgcdc:orders', json_build_object(
        'operation', TG_OP,
        'row', row_to_json(NEW)
    )::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS pgcdc_orders_notify ON orders;
CREATE TRIGGER pgcdc_orders_notify
    AFTER INSERT OR UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION pgcdc_notify_orders();
