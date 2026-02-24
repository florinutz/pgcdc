# Detectors

pgcdc supports three event source types (detectors).

## LISTEN/NOTIFY

The simplest approach. Uses PostgreSQL's built-in LISTEN/NOTIFY mechanism.

**Pros**: No PG config changes needed. Works on any PG 12+. Sub-millisecond latency.
**Cons**: Requires triggers. 8KB payload limit. No transaction metadata. Events lost during disconnects.

```bash
pgcdc init --table orders | psql mydb
pgcdc listen -c pgcdc:orders --db postgres://...
```

## WAL Logical Replication

Captures changes directly from the write-ahead log. No triggers needed.

**Pros**: No triggers. No payload size limit. Full row data including old values. Transaction metadata. Persistent slots survive disconnects.
**Cons**: Requires `wal_level=logical`. One replication slot per pgcdc instance.

```bash
pgcdc init --table orders --detector wal | psql mydb
pgcdc listen --detector wal --publication pgcdc_orders --db postgres://...
```

### Features

- **Persistent slots**: `--persistent-slot` creates a named slot that survives disconnects
- **Checkpointing**: LSN position saved to `pgcdc_checkpoints` table
- **Transaction metadata**: `--tx-metadata` adds xid, commit_time, seq to events
- **Transaction markers**: `--tx-markers` wraps transactions in BEGIN/COMMIT events
- **Schema information**: `--include-schema` adds column type metadata
- **Schema evolution**: `--schema-events` emits SCHEMA_CHANGE events
- **Heartbeat**: `--heartbeat-interval 30s` prevents WAL bloat on idle databases
- **Slot lag monitoring**: `pgcdc_slot_lag_bytes` metric + `--slot-lag-warn` threshold

### Slot Management

```bash
pgcdc slot list --db postgres://...
pgcdc slot status --slot-name pgcdc_orders --db postgres://...
pgcdc slot drop --slot-name pgcdc_orders --db postgres://...
```

## Outbox Pattern

Polls a transactional outbox table. Best for applications that need transactional guarantees.

**Pros**: Events are part of the application transaction. No PG config changes. Works with any PG version.
**Cons**: Polling-based (configurable interval, default 500ms). Requires outbox table.

```bash
pgcdc init --table my_outbox --adapter outbox | psql mydb
pgcdc listen --detector outbox --outbox-table my_outbox --db postgres://...
```

Options:
- `--outbox-poll-interval 500ms` — polling frequency
- `--outbox-batch-size 100` — rows per poll
- `--outbox-keep-processed` — UPDATE instead of DELETE (set `processed_at`)

Uses `SELECT ... FOR UPDATE SKIP LOCKED` for safe concurrent processing.
