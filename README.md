# pgpipe

[![CI](https://github.com/florinutz/pgpipe/actions/workflows/ci.yml/badge.svg)](https://github.com/florinutz/pgpipe/actions/workflows/ci.yml)

Lightweight PostgreSQL Change Data Capture. No Kafka. No NATS. Just your PG and a single binary.

pgpipe captures changes from PostgreSQL via LISTEN/NOTIFY or WAL logical replication and delivers them to webhooks, SSE streams, stdout, files, exec processes, PG tables, and WebSockets -- with zero external dependencies beyond PostgreSQL itself.

```
PostgreSQL                    pgpipe (single binary)
┌──────────┐    LISTEN   ┌─────────────┐     ┌───────────┐
│  Table   │────────────▶│  Detector   │────▶│   Bus     │
│  Trigger │  NOTIFY     │ (reconnect) │     │ (fan-out) │
└──────────┘             └─────────────┘     └─────┬─────┘
                                                   │
                              ┌──────────────┼──────────────┐──────────────┐
                              ▼              ▼              ▼              ▼
                        ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
                        │ Webhook  │  │   SSE    │  │  Stdout  │  │   File   │
                        │ (retries)│  │ (stream) │  │  (json)  │  │ (rotate) │
                        └──────────┘  └──────────┘  └──────────┘  └──────────┘
                              ▼              ▼              ▼
                        ┌──────────┐  ┌──────────┐  ┌──────────┐
                        │   Exec   │  │ PG Table │  │WebSocket │
                        │ (stdin)  │  │ (INSERT) │  │ (stream) │
                        └──────────┘  └──────────┘  └──────────┘
```

## Quick Start

### 1. Install

```bash
go install github.com/florinutz/pgpipe@latest
```

Or build from source:

```bash
git clone https://github.com/florinutz/pgpipe.git
cd pgpipe
make build
```

### 2. Create a trigger on your table

```bash
pgpipe init --table orders
```

This prints SQL you can review and apply:

```sql
CREATE OR REPLACE FUNCTION pgpipe_notify_orders() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('pgpipe:orders', json_build_object(
    'op', TG_OP,
    'table', TG_TABLE_NAME,
    'row', CASE WHEN TG_OP = 'DELETE' THEN row_to_json(OLD) ELSE row_to_json(NEW) END,
    'old', CASE WHEN TG_OP = 'UPDATE' THEN row_to_json(OLD) ELSE NULL END
  )::text);
  RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER pgpipe_orders_trigger
  AFTER INSERT OR UPDATE OR DELETE ON orders
  FOR EACH ROW EXECUTE FUNCTION pgpipe_notify_orders();
```

Apply it:

```bash
pgpipe init --table orders | psql mydb
```

### 3. Start listening

```bash
# stdout (JSON lines)
pgpipe listen -c pgpipe:orders --db postgres://localhost:5432/mydb

# webhook
pgpipe listen -c pgpipe:orders -a webhook -u https://example.com/hook --db postgres://...

# SSE server
pgpipe listen -c pgpipe:orders -a sse --sse-addr :8080 --db postgres://...

# file (JSON lines with rotation)
pgpipe listen -c pgpipe:orders -a file --file-path /tmp/events.jsonl --db postgres://...

# exec (pipe to any command)
pgpipe listen -c pgpipe:orders -a exec --exec-command 'jq .payload' --db postgres://...

# pg_table (INSERT into a PostgreSQL table)
pgpipe listen -c pgpipe:orders -a pg_table --pg-table-name audit_events --db postgres://...

# websocket
pgpipe listen -c pgpipe:orders -a ws --sse-addr :8080 --db postgres://...

# multiple adapters at once
pgpipe listen -c pgpipe:orders -a stdout -a webhook -u https://example.com/hook --db postgres://...
```

### Alternative: WAL Logical Replication

No triggers needed. Captures changes directly from the PostgreSQL write-ahead log.

**Prerequisites:** PostgreSQL must have `wal_level=logical` (check with `SHOW wal_level;`).

```bash
# 1. Generate publication SQL
pgpipe init --table orders --detector wal

# 2. Apply it
pgpipe init --table orders --detector wal | psql mydb

# 3. Start listening via WAL
pgpipe listen --detector wal --publication pgpipe_orders --db postgres://localhost:5432/mydb
```

WAL replication events use the same format as LISTEN/NOTIFY (`channel: pgpipe:<table>`, same payload structure), so all adapters and SSE filtering work identically regardless of detector type.

### Transaction Metadata

Enrich WAL events with transaction context:

```bash
pgpipe listen --detector wal --publication pgpipe_orders --tx-metadata --db postgres://...
```

Each event includes a `transaction` field:

```json
{
  "id": "...",
  "channel": "pgpipe:orders",
  "operation": "INSERT",
  "payload": {"op": "INSERT", "table": "orders", "row": {"id": 1}, "old": null},
  "source": "wal_replication",
  "created_at": "2025-01-15T10:30:00Z",
  "transaction": {
    "xid": 1234,
    "commit_time": "2025-01-15T10:30:00Z",
    "seq": 1
  }
}
```

Fields: `xid` (PostgreSQL transaction ID), `commit_time` (when the transaction committed), `seq` (1-based position within the transaction). LISTEN/NOTIFY events omit this field (the protocol has no transaction info).

### Transaction Markers

Wrap each transaction in synthetic BEGIN/COMMIT events:

```bash
pgpipe listen --detector wal --publication pgpipe_orders --tx-markers --db postgres://...
```

`--tx-markers` implies `--tx-metadata`. Output stream:

```json
{"channel":"pgpipe:_txn","operation":"BEGIN","payload":{"xid":1234,"commit_time":"..."},...}
{"channel":"pgpipe:orders","operation":"INSERT","transaction":{"xid":1234,"seq":1},...}
{"channel":"pgpipe:orders","operation":"INSERT","transaction":{"xid":1234,"seq":2},...}
{"channel":"pgpipe:_txn","operation":"COMMIT","payload":{"xid":1234,"event_count":2,"commit_time":"..."},...}
```

Marker events use channel `pgpipe:_txn` (underscore prefix signals synthetic/system events) and have no `transaction` field themselves. The COMMIT payload includes `event_count` for the number of DML events in the transaction.

## Configuration

pgpipe supports three layers of configuration (highest priority wins):

**CLI flags** > **environment variables** > **YAML config file** > **defaults**

### Config file

```yaml
# pgpipe.yaml
database_url: postgres://user:pass@localhost:5432/mydb
channels:
  - pgpipe:orders
  - pgpipe:users

adapters:
  - stdout
  - webhook

log_level: info    # debug, info, warn, error
log_format: text   # text, json

bus:
  buffer_size: 1024

webhook:
  url: https://example.com/webhook
  headers:
    Authorization: "Bearer token123"
  signing_key: "my-secret-key"
  max_retries: 5

sse:
  addr: ":8080"
  cors_origins:
    - "*"
  heartbeat_interval: 15s

file:
  path: /var/log/pgpipe/events.jsonl
  max_size: 104857600  # 100 MB
  max_files: 5

exec:
  command: "jq .payload >> /tmp/payloads.jsonl"

pg_table:
  # url: postgres://...  # defaults to database_url
  table: pgpipe_events

websocket:
  ping_interval: 15s
```

### Environment variables

All config keys are available as environment variables with the `PGPIPE_` prefix:

```bash
export PGPIPE_DATABASE_URL=postgres://localhost:5432/mydb
export PGPIPE_LOG_LEVEL=debug
export PGPIPE_WEBHOOK_URL=https://example.com/hook
```

## Adapters

### stdout

Outputs JSON lines to stdout. Pipe-friendly:

```bash
pgpipe listen -c pgpipe:orders --db postgres://... | jq .payload
```

### webhook

HTTP POST with retries and optional HMAC signing.

Headers on every request:
- `Content-Type: application/json`
- `User-Agent: pgpipe/1.0`
- `X-PGPipe-Event-ID: <uuid>`
- `X-PGPipe-Channel: <channel>`
- `X-PGPipe-Signature: sha256=<hex>` (when signing key is set)

Retry behavior:
- Exponential backoff with full jitter (1s base, 32s cap)
- Retries on 5xx and 429 (Too Many Requests)
- No retry on other 4xx responses
- Default 5 max retries

### SSE (Server-Sent Events)

Starts an HTTP server with SSE endpoints:

- `GET /events` -- stream all channels
- `GET /events/{channel}` -- stream filtered by channel
- `GET /healthz` -- health check

Features:
- Heartbeat comments every 15s to keep connections alive
- `X-Accel-Buffering: no` for nginx compatibility
- Configurable CORS origins

### file

Writes events as JSON lines to a file with automatic rotation:

```bash
pgpipe listen -c pgpipe:orders -a file --file-path /var/log/pgpipe/events.jsonl --db postgres://...
```

Features:
- Append-only JSON lines, fsynced after each event
- Automatic rotation when file exceeds `--file-max-size` (default 100MB)
- Keeps up to `--file-max-files` rotated files (default 5): `events.jsonl.1`, `.2`, etc.

### exec

Pipes events as JSON lines to a long-running subprocess's stdin:

```bash
pgpipe listen -c pgpipe:orders -a exec --exec-command 'jq .payload >> /tmp/payloads.jsonl' --db postgres://...
```

Features:
- Command runs via `sh -c`, so shell features (pipes, redirects) work
- If the process exits, pgpipe restarts it with exponential backoff
- Failed events are re-delivered to the new process

### pg_table

Inserts events into a PostgreSQL table:

```bash
# 1. Generate the CREATE TABLE SQL
pgpipe init --table audit_events --adapter pg_table | psql mydb

# 2. Start piping events to the table
pgpipe listen -c pgpipe:orders -a pg_table --pg-table-name audit_events --db postgres://...
```

Features:
- Table must pre-exist (use `pgpipe init --adapter pg_table` to generate SQL)
- Uses `--pg-table-url` for a separate database, or falls back to `--db`
- Automatic reconnection with backoff on connection loss
- Non-connection errors (constraint violations) skip the event with a warning

### ws (WebSocket)

Starts an HTTP server with WebSocket endpoints:

- `GET /ws` -- stream all channels
- `GET /ws/{channel}` -- stream filtered by channel

```bash
pgpipe listen -c pgpipe:orders -a ws --sse-addr :8080 --db postgres://...
# Then: websocat ws://localhost:8080/ws
```

Features:
- Ping/pong keepalive (default 15s interval)
- Channel filtering via URL path
- Shares the same HTTP server as SSE (use both simultaneously)

## Event Format

```json
{
  "id": "019662a1-b2c3-7def-8901-234567890abc",
  "channel": "pgpipe:orders",
  "operation": "INSERT",
  "payload": {
    "op": "INSERT",
    "table": "orders",
    "row": {"id": 1, "amount": 99.99},
    "old": null
  },
  "source": "listen_notify",
  "created_at": "2025-01-15T10:30:00Z"
}
```

## CLI Reference

```
pgpipe listen [flags]
  -c, --channel strings        PG channels to listen on (repeatable)
  -a, --adapter strings        Adapters: stdout, webhook, sse, file, exec, pg_table, ws (default [stdout])
  -u, --url string             Webhook destination URL
      --sse-addr string        SSE/WS server address (default ":8080")
      --db string              PostgreSQL connection string (env: PGPIPE_DATABASE_URL)
      --retries int            Webhook max retries (default 5)
      --signing-key string     HMAC signing key for webhook
      --detector string        Detector type: listen_notify or wal (default "listen_notify")
      --publication string     PostgreSQL publication name (required for --detector wal)
      --tx-metadata            Include transaction metadata in WAL events (xid, commit_time, seq)
      --tx-markers             Emit BEGIN/COMMIT marker events (implies --tx-metadata)
      --metrics-addr string    Standalone metrics/health server address (e.g. :9090)
      --file-path string       File adapter output path
      --file-max-size int      File rotation size in bytes (default 104857600)
      --file-max-files int     Number of rotated files to keep (default 5)
      --exec-command string    Shell command to pipe events to (via stdin)
      --pg-table-url string    PostgreSQL URL for pg_table adapter (default: same as --db)
      --pg-table-name string   Destination table name (default: pgpipe_events)
      --ws-ping-interval dur   WebSocket ping interval (default 15s)

pgpipe init [flags]
      --table string           Table name (required)
      --channel string         Channel name (default: pgpipe:<table>)
      --detector string        Detector type: listen_notify or wal (default "listen_notify")
      --publication string     Publication name for WAL (default: pgpipe_<table>)
      --adapter string         Adapter type: pg_table (generates events table SQL)

pgpipe version
```

Global flags:
```
      --config string       Config file path (default: ./pgpipe.yaml)
      --log-level string    debug, info, warn, error (default "info")
      --log-format string   text, json (default "text")
```

## Docker

```bash
# Build the image
make docker-build

# Start postgres + pgpipe
make docker-up

# Stop
make docker-down
```

## Development

```bash
make build          # compile binary
make test           # run unit tests
make test-scenarios # run scenario tests (requires Docker)
make test-all       # unit + scenario tests
make lint           # run golangci-lint
make vet            # run go vet
make fmt            # check formatting
make coverage       # generate coverage report
make docker-build   # build Docker image
make clean          # remove build artifacts
```

## Design

pgpipe is built around three core abstractions:

- **Detector** -- sources of change events (LISTEN/NOTIFY or WAL logical replication)
- **Bus** -- fans out events from detectors to adapters via buffered Go channels
- **Adapter** -- delivers events to destinations (stdout, webhook, SSE, file, exec, pg_table, WebSocket)

All components communicate through Go channels. Backpressure propagates naturally: a slow adapter's channel fills up, the bus drops events for that adapter (with a warning log), and other adapters continue unaffected.

Graceful shutdown flows from signal → context cancellation → detector exits → bus closes subscriber channels → adapters drain and exit → HTTP server shuts down.

## License

MIT
