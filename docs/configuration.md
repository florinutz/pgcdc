# Configuration Reference

pgcdc supports three configuration layers (highest priority wins):

**CLI flags** > **Environment variables** (`PGCDC_` prefix) > **YAML config file** > **Defaults**

## Full Config File Example

```yaml
database_url: postgres://user:pass@localhost:5432/mydb
channels: [pgcdc:orders, pgcdc:users]
adapters: [stdout, webhook, redis]
log_level: info          # debug, info, warn, error
log_format: text         # text, json
shutdown_timeout: 5s
metrics_addr: ":9090"    # standalone metrics server

bus:
  buffer_size: 1024

detector:
  type: wal              # listen_notify, wal, outbox
  publication: pgcdc_all
  tx_metadata: true
  tx_markers: false
  heartbeat_interval: 30s
  heartbeat_table: pgcdc_heartbeat
  slot_lag_warn: 104857600  # 100MB

dlq:
  type: pg_table         # stderr, pg_table, none
  table: pgcdc_dead_letters
  # db_url: ...          # defaults to database_url

webhook:
  url: https://example.com/webhook
  headers:
    Authorization: "Bearer token123"
  signing_key: my-secret-key
  max_retries: 5
  timeout: 10s
  backoff_base: 1s
  backoff_cap: 32s

sse:
  addr: ":8080"
  cors_origins: ["*"]
  heartbeat_interval: 15s

file:
  path: /var/log/pgcdc/events.jsonl
  max_size: 104857600    # 100MB
  max_files: 5

exec:
  command: "jq .payload >> /tmp/payloads.jsonl"

pg_table:
  table: pgcdc_events

websocket:
  ping_interval: 15s

grpc:
  addr: ":9090"

nats:
  url: nats://localhost:4222
  subject: pgcdc
  stream: pgcdc
  max_age: 24h

search:
  engine: typesense      # typesense, meilisearch
  url: http://localhost:8108
  api_key: xyz
  index: orders
  id_column: id
  batch_size: 100
  batch_interval: 1s

redis:
  url: redis://localhost:6379
  mode: invalidate       # invalidate, sync
  key_prefix: "orders:"
  id_column: id

embedding:
  api_url: https://api.openai.com/v1/embeddings
  api_key: sk-...
  model: text-embedding-3-small
  columns: [title, body]
  id_column: id
  table: pgcdc_embeddings
  dimension: 1536
  max_retries: 3
  timeout: 30s

outbox:
  table: pgcdc_outbox
  poll_interval: 500ms
  batch_size: 100
  keep_processed: false
```

## Environment Variables

All nested keys use underscores: `PGCDC_WEBHOOK_URL`, `PGCDC_REDIS_MODE`, `PGCDC_DLQ_TYPE`, etc.

```bash
export PGCDC_DATABASE_URL=postgres://localhost:5432/mydb
export PGCDC_LOG_LEVEL=debug
export PGCDC_WEBHOOK_URL=https://example.com/hook
export PGCDC_DLQ_TYPE=pg_table
export PGCDC_REDIS_URL=redis://localhost:6379
```
