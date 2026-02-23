-- pgpipe WAL logical replication setup for table "{{.Table}}"
--
-- Prerequisites:
--   1. PostgreSQL must have wal_level=logical (requires restart if changing)
--      Check: SHOW wal_level;
--   2. max_replication_slots must be > 0 (default: 10)
--   3. The connecting user must have REPLICATION privilege or be a superuser
--
-- pgpipe creates a temporary replication slot automatically on connect.
-- You only need to create the publication below.

CREATE PUBLICATION {{.Publication}} FOR TABLE {{.Table}};
