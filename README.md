# pgcdc

**Stream database changes anywhere. Single binary. No Kafka.**

[![CI](https://github.com/florinutz/pgcdc/actions/workflows/ci.yml/badge.svg)](https://github.com/florinutz/pgcdc/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/florinutz/pgcdc)](https://goreportcard.com/report/github.com/florinutz/pgcdc)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/florinutz/pgcdc.svg)](https://pkg.go.dev/github.com/florinutz/pgcdc)


```bash
go install github.com/florinutz/pgcdc/cmd/pgcdc@latest
```

```
PostgreSQL ──▶ pgcdc (single Go binary) ──▶ Webhook, SSE, WebSocket, gRPC
MySQL      ──▶  5 detectors:                stdout, file, exec, PG table
MongoDB    ──▶  • LISTEN/NOTIFY             NATS JetStream, Kafka, S3
                • WAL logical replication    Typesense / Meilisearch
                • Outbox table polling       Redis cache invalidation
                • MySQL binlog               pgvector embeddings
                • MongoDB Change Streams     Streaming SQL views
```

## Why pgcdc?

| | pgcdc | Debezium | Sequin | Conduit |
|--|-------|----------|--------|---------|
| Single binary | Yes | No (JVM+Kafka) | No (managed) | Yes |
| Memory (idle) | ~12 MB | ~500 MB+ | N/A | ~50 MB |
| PG/MySQL/MongoDB | Yes | Generic | Yes | Generic |
| Built-in pgvector sync | Yes | No | No | No |
| SSE/WS/gRPC streaming | Yes | No | HTTP only | No |
| Search sync (Typesense/Meili) | Yes | No | No | Plugin |
| Redis cache invalidation | Yes | No | No | No |
| Dead letter queue | Yes | Yes | Yes | No |
| Event routing | Yes | Yes (SMTs) | No | Yes |
| Debezium-compatible output | Yes | Native | No | No |
| External deps | 0 | Kafka+ZK | Managed | 0 |

## Quick Start

### Path 1: LISTEN/NOTIFY (simplest, no PG config needed)

```bash
# Generate trigger SQL
pgcdc init --table orders | psql mydb

# Stream changes to stdout
pgcdc listen -c pgcdc:orders --db postgres://localhost:5432/mydb
```

### Path 2: WAL Replication — zero-config (production, no triggers)

Requires `wal_level=logical` in `postgresql.conf`.

```bash
# Stream ALL tables — no setup needed
pgcdc listen --all-tables --db postgres://localhost:5432/mydb
```

Auto-creates a `FOR ALL TABLES` publication. Add `--persistent-slot` for crash-resilient streaming.

### Path 2b: WAL Replication — per-table

```bash
# Create publication for specific table
pgcdc init --table orders --detector wal | psql mydb

# Stream with persistent slot (survives restarts)
pgcdc listen --detector wal --publication pgcdc_orders \
  --persistent-slot --db postgres://localhost:5432/mydb
```

### Path 3: Outbox Pattern (transactional guarantees)

```bash
# Generate outbox table
pgcdc init --table my_outbox --adapter outbox | psql mydb

# Poll and deliver
pgcdc listen --detector outbox --outbox-table my_outbox --db postgres://...
```

### Path 4: MySQL Binlog (MySQL CDC)

Requires MySQL 8.0+ with `binlog_format=ROW`.

```bash
pgcdc listen --detector mysql \
  --mysql-addr localhost:3306 --mysql-user root --mysql-password secret \
  --mysql-server-id 100 --mysql-tables mydb.orders \
  -a stdout
```

### Path 5: MongoDB Change Streams (MongoDB CDC)

Requires MongoDB replica set or sharded cluster.

```bash
pgcdc listen --detector mongodb \
  --mongodb-uri mongodb://localhost:27017 \
  --mongodb-database mydb --mongodb-collections orders \
  -a stdout
```

## Adapters

| Adapter | Flag | Description |
|---------|------|-------------|
| **stdout** | `-a stdout` | JSON lines to stdout. Pipe-friendly. |
| **webhook** | `-a webhook -u <url>` | HTTP POST with retries, HMAC signing, exponential backoff. |
| **SSE** | `-a sse` | Server-Sent Events with channel filtering. |
| **WebSocket** | `-a ws` | WebSocket streaming with channel filtering. |
| **gRPC** | `-a grpc` | gRPC streaming server with channel filtering. |
| **file** | `-a file --file-path <path>` | JSON lines to file with automatic rotation. |
| **exec** | `-a exec --exec-command <cmd>` | Pipe JSON to subprocess stdin. Auto-restarts. |
| **pg_table** | `-a pg_table` | INSERT into a PostgreSQL table. |
| **NATS** | `-a nats --nats-url <url>` | Publish to NATS JetStream with dedup. |
| **kafka** | `-a kafka --kafka-brokers <addrs>` | Publish to Kafka. `--kafka-transactional-id` for exactly-once. |
| **search** | `-a search --search-url <url>` | Sync to Typesense or Meilisearch. Batched. |
| **redis** | `-a redis --redis-url <url>` | Cache invalidation or sync. DEL/SET per event. |
| **embedding** | `-a embedding --embedding-api-url <url>` | OpenAI-compatible API → pgvector UPSERT. |
| **s3** | `-a s3 --s3-bucket <name>` | Flush to S3-compatible storage (JSON Lines/Parquet). |
| **iceberg** | `-a iceberg --iceberg-warehouse <path>` | Apache Iceberg table writes. |
| **kafkaserver** | `-a kafkaserver --kafkaserver-addr :9092` | Kafka wire protocol server. Any Kafka consumer connects directly — no Kafka cluster needed. Channels become topics, N partitions, consumer groups. |
| **view** | `-a view` (or `views:` YAML / `--view-query`) | Streaming SQL over CDC events. Tumbling, sliding, and session windows. GROUP BY, COUNT/SUM/AVG/MIN/MAX/COUNT(DISTINCT)/STDDEV with HAVING. Results re-injected as `VIEW_RESULT` events. |

Use multiple adapters: `-a stdout -a webhook -a redis`

## Event Routing

Route specific channels to specific adapters:

```bash
pgcdc listen --detector wal --publication pgcdc_all \
  -a webhook -a search -a redis \
  --route webhook=pgcdc:orders,pgcdc:users \
  --route search=pgcdc:articles \
  --route redis=pgcdc:orders \
  --db postgres://...
```

Adapters without a `--route` filter receive all events.

## Dead Letter Queue

Failed events are captured instead of silently dropped:

```bash
# Default: JSON lines to stderr
pgcdc listen ... --dlq stderr

# PostgreSQL table (auto-created)
pgcdc listen ... --dlq pg_table --dlq-table pgcdc_dead_letters

# Disable
pgcdc listen ... --dlq none
```

### DLQ Management

List, replay, and purge failed events stored in the PG table DLQ:

```bash
# List failed events (table or JSON output)
pgcdc dlq list --dlq-db postgres://... --format table
pgcdc dlq list --adapter webhook --since 2026-02-01T00:00:00Z --format json

# Replay failed events through an adapter
pgcdc dlq replay --dlq-db postgres://... --adapter webhook --webhook-url https://...
pgcdc dlq replay --dry-run  # preview without delivering

# Purge old or replayed records
pgcdc dlq purge --dlq-db postgres://... --replayed        # only already-replayed
pgcdc dlq purge --until 2026-01-01T00:00:00Z --force      # by date
```

## Transforms

Apply built-in transforms to events before they reach adapters:

```bash
# Drop sensitive columns
pgcdc listen --all-tables --drop-columns password,api_key --db postgres://...

# Only pass INSERT and UPDATE events
pgcdc listen --all-tables --filter-operations INSERT,UPDATE --db postgres://...

# Rewrite payloads into Debezium envelope (for Kafka Connect sinks, Flink, etc.)
pgcdc listen --all-tables --adapter kafka \
  --kafka-brokers localhost:9092 \
  --debezium-envelope \
  --debezium-connector-name my-pg-source \
  --debezium-database mydb \
  --db postgres://...
```

The `--debezium-envelope` flag reshapes the payload into the standard Debezium before/after/op/source envelope, enabling drop-in migration of downstream consumers without code changes.

Full transform config via YAML (global or per-adapter):

```yaml
transforms:
  global:
    - type: debezium
      debezium:
        connector_name: my-pg-source
        database: orders_db
  adapter:
    kafka:
      - type: drop_columns
        columns: [internal_id, created_by]
```

Built-in transform types: `drop_columns`, `rename_fields`, `mask` (hash/redact/partial), `filter` (by operation or field value), `debezium`, `cloudevents`.

### CloudEvents Envelope

Rewrite events into [CloudEvents v1.0](https://cloudevents.io/) structured-mode JSON:

```yaml
transforms:
  global:
    - type: cloudevents
      cloudevents:
        source: "https://mycompany.com/orders"
        type_prefix: "com.company.order"
```

Produces spec-compliant envelopes with pgcdc extension attributes (`pgcdclsn`, `pgcdctxid`).

## Snapshot: Initial Data Sync

Export existing rows as events, then transition to live streaming:

```bash
# Snapshot only
pgcdc snapshot --table orders --db postgres://...

# Snapshot-first: zero-gap initial sync + live WAL
pgcdc listen --detector wal --publication pgcdc_orders \
  --snapshot-first --snapshot-table orders --db postgres://...
```

### Incremental Snapshots

Run chunk-based snapshots alongside live WAL streaming, triggered via signal table:

```bash
pgcdc listen --detector wal --all-tables --persistent-slot \
  --incremental-snapshot --snapshot-chunk-size 1000 --snapshot-chunk-delay 100ms \
  --db postgres://...

# Trigger a snapshot from SQL:
INSERT INTO pgcdc_signals (signal_type, signal_payload) VALUES ('snapshot', '{"table":"orders"}');
```

Progress is persisted to `pgcdc_snapshot_progress` — crash-resumable.

## Persistent Slots + Checkpointing

Survive crashes and restarts with WAL position tracking:

```bash
pgcdc listen --detector wal --publication pgcdc_orders \
  --persistent-slot --slot-name pgcdc_orders \
  --db postgres://...
```

Checkpoints are stored in `pgcdc_checkpoints` table. The replication slot persists across disconnects.

## Backpressure

Source-aware WAL lag monitoring with automatic throttle/pause/shed to prevent PG disk exhaustion:

```bash
pgcdc listen --detector wal --all-tables --persistent-slot \
  --backpressure --bp-warn-threshold 500MB --bp-critical-threshold 2GB \
  --adapter-priority webhook=critical --adapter-priority search=best-effort \
  --db postgres://...
```

Three zones: green (full speed), yellow (proportional throttle + shed best-effort adapters), red (pause detector + shed normal+best-effort). Hysteresis prevents flapping.

## Transaction Metadata

```bash
# Add xid, commit_time, seq to events
pgcdc listen --detector wal --publication pgcdc_orders --tx-metadata --db postgres://...

# Wrap transactions in BEGIN/COMMIT markers
pgcdc listen --detector wal --publication pgcdc_orders --tx-markers --db postgres://...
```

## Encoding + Schema Registry

Kafka and NATS adapters support Avro, Protobuf, or JSON encoding with optional Confluent Schema Registry integration:

```bash
# Kafka with Avro encoding
pgcdc listen --all-tables -a kafka --kafka-brokers localhost:9092 \
  --kafka-encoding avro --schema-registry-url http://localhost:8081 \
  --db postgres://...

# NATS with Protobuf encoding
pgcdc listen --all-tables -a nats --nats-url nats://localhost:4222 \
  --nats-encoding protobuf --schema-registry-url http://localhost:8081 \
  --db postgres://...
```

Schema Registry auth: `--schema-registry-username`, `--schema-registry-password`.

## Observability

- **Prometheus metrics** at `/metrics` (30+ metrics)
- **Health check** at `/healthz` (per-component status)
- **Standalone metrics server**: `--metrics-addr :9090`
- **Slot lag monitoring**: `pgcdc_slot_lag_bytes` gauge + log warnings
- **Heartbeat**: `--heartbeat-interval 30s` keeps replication slots advancing
- **OpenTelemetry tracing**: `--otel-exporter otlp --otel-endpoint localhost:4317` for distributed tracing (OTLP gRPC or stdout exporter, configurable sample ratio via `--otel-sample-ratio`)

## Configuration

Three layers (highest priority wins): **CLI flags** > **env vars** (`PGCDC_` prefix) > **YAML config file** > **defaults**.

**Hot-reload**: `kill -HUP <pid>` re-reads the YAML config and atomically swaps `transforms:` and `routes:` for all running adapters with zero event loss. Adapters, detectors, and bus mode remain immutable.

```yaml
# pgcdc.yaml
database_url: postgres://user:pass@localhost:5432/mydb
channels: [pgcdc:orders]
adapters: [stdout, webhook]

webhook:
  url: https://example.com/webhook
  signing_key: my-secret
  max_retries: 5

dlq:
  type: pg_table

search:
  engine: typesense
  url: http://localhost:8108
  api_key: xyz
  index: orders

redis:
  url: redis://localhost:6379
  mode: invalidate
  key_prefix: "orders:"
```

## Wasm Plugins

Extend pgcdc with WebAssembly plugins (Extism-based, pure Go, no CGo). Four extension points:

```bash
# Custom transform plugin
pgcdc listen --all-tables --plugin-transform ./my-transform.wasm --db postgres://...

# Custom adapter plugin
pgcdc listen --all-tables --plugin-adapter ./my-sink.wasm --db postgres://...

# Custom DLQ or checkpoint backend
pgcdc listen ... --dlq plugin --dlq-plugin-path ./my-dlq.wasm
pgcdc listen ... --checkpoint-plugin ./my-store.wasm
```

Write plugins in any Extism PDK language (Rust, Go, TypeScript, Python). Host functions available: `pgcdc_log`, `pgcdc_metric_inc`, `pgcdc_http_request`.

## Architecture

```
Signal (SIGINT/SIGTERM)
  |
  v
Context ──▶ Pipeline (pgcdc.go)
              |
  Detector ──▶ Bus (fan-out + routing) ──▶ Adapter ──▶ DLQ (on failure)
  (listen_notify    subscriber chans
   wal              (one per adapter,
   outbox            filtered by route)
   mysql
   mongodb)
```

- **Concurrency**: `errgroup` manages all goroutines. One context cancellation tears everything down.
- **Backpressure**: `--bus-mode fast` (default) drops on full channel. `--bus-mode reliable` blocks detector instead.
- **Routing**: Bus-level filtering before fan-out. No event copies wasted.
- **DLQ**: Failed events captured to stderr or PG table for inspection/replay.
- **Operations**: INSERT, UPDATE, DELETE, TRUNCATE (WAL detector).
- **TOAST cache**: `--toast-cache` resolves unchanged large columns without `REPLICA IDENTITY FULL` (see below).

## TOAST Column Handling

PostgreSQL stores large column values (>2KB) out-of-line via TOAST. With `REPLICA IDENTITY DEFAULT` (the default), unchanged TOAST columns on UPDATE events arrive with no data in the WAL. Without special handling, downstream consumers see `null` for those columns.

pgcdc ships a two-layer solution:

**Layer 1 — In-memory cache** (opt-in):

```bash
pgcdc listen --detector wal --publication pgcdc_orders \
  --toast-cache --toast-cache-max-entries 100000 \
  --db postgres://...
```

The cache stores the most recent full row per `(table, primary key)`. On INSERT it's populated; on UPDATE unchanged columns are backfilled from the cached row. The consumer sees a normal, complete event with no special metadata.

**Layer 2 — Structured metadata** (always-on fallback):

On cache miss (cold start, evicted entry, no PK), the event includes null for the unchanged column and a `_unchanged_toast_columns` array:

```json
{
  "op": "UPDATE",
  "table": "orders",
  "row": {"id": "42", "status": "shipped", "description": null},
  "_unchanged_toast_columns": ["description"]
}
```

**Adapter-aware handling**: the search and redis adapters understand this metadata. The search adapter strips unchanged columns and uses a partial update (PATCH for Typesense, merge for Meilisearch). The redis sync adapter does `GET` → merge → `SET` to avoid overwriting stored data with null.

## Streaming SQL Views

Run real-time analytics over CDC events with SQL-like queries:

```bash
# Tumbling window: fixed 10-second buckets
pgcdc listen --all-tables -a stdout \
  --view-query 'order_counts:SELECT channel, COUNT(*) as cnt FROM pgcdc_events TUMBLING WINDOW 10s GROUP BY channel' \
  --db postgres://...

# Sliding window: 60-second window, slide every 10 seconds
pgcdc listen --all-tables -a stdout \
  --view-query 'smooth_avg:SELECT channel, AVG(CAST(payload.amount AS DECIMAL)) as avg_amount FROM pgcdc_events SLIDING WINDOW 60s SLIDE 10s GROUP BY channel' \
  --db postgres://...

# Session window: group events with 30-second inactivity gap
pgcdc listen --all-tables -a stdout \
  --view-query 'sessions:SELECT channel, COUNT(*) as cnt FROM pgcdc_events SESSION WINDOW 30s GROUP BY channel' \
  --db postgres://...
```

Three window types:
- **Tumbling**: Fixed-size non-overlapping time buckets. Optional `ALLOWED LATENESS <duration>` for late events.
- **Sliding**: Overlapping windows of size W with slide interval S. Smooth metric curves.
- **Session**: Gap-based windows that close after inactivity timeout. Ideal for user behavior analysis.

Aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `COUNT(DISTINCT ...)`, `STDDEV`/`STDDEV_POP`.

Results are emitted as `VIEW_RESULT` events on `pgcdc:_view:<name>` channels, re-injected into the bus for downstream adapters.

Views can also be configured via YAML:

```yaml
views:
  order_counts:
    query: "SELECT channel, COUNT(*) as cnt FROM pgcdc_events TUMBLING WINDOW 10s GROUP BY channel"
    emit: row        # or batch
    max_groups: 1000
```

## Validate and Playground

```bash
# Validate configuration without starting the pipeline
pgcdc validate --db postgres://...

# Start a self-contained demo environment (requires Docker)
pgcdc playground
```

`pgcdc validate` checks database connectivity and adapter configurations, reporting pass/fail status per component.

`pgcdc playground` spins up a PostgreSQL container, creates demo tables with triggers, and streams changes through pgcdc with auto-generated data.

## Startup Options

```bash
# Skip adapter pre-flight validation (e.g., for read-only DB users)
pgcdc listen --all-tables --skip-validation --db postgres://...

# Skip automatic schema migrations
pgcdc listen --all-tables --skip-migrations --db postgres://...
```

## Slim Binary

pgcdc ships a slim build variant that excludes heavy adapters for minimal deployments:

```bash
# Build slim binary (~60% smaller)
make build-slim

# Or with specific build tags
go build -tags "no_kafka,no_grpc,no_nats,no_redis,no_plugins,no_kafkaserver,no_views" ./cmd/pgcdc
```

Available build tags: `no_kafka`, `no_grpc`, `no_iceberg`, `no_nats`, `no_redis`, `no_plugins`, `no_kafkaserver`, `no_views`.

Homebrew: `brew install florinutz/tap/pgcdc` (full) or `brew install florinutz/tap/pgcdc-slim` (slim).

## Development

```bash
make build          # compile binary
make test           # unit tests (~2s)
make test-scenarios # scenario tests with Docker (~30s)
make test-all       # both
make bench          # throughput/latency/memory benchmarks
make lint           # golangci-lint
make coverage       # coverage report
```

See [docs/](docs/) for full documentation.

## License

MIT
