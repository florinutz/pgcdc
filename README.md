# pgcdc

**Stream PostgreSQL changes anywhere. Single binary. No Kafka.**

[![CI](https://github.com/florinutz/pgcdc/actions/workflows/ci.yml/badge.svg)](https://github.com/florinutz/pgcdc/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/florinutz/pgcdc)](https://goreportcard.com/report/github.com/florinutz/pgcdc)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/florinutz/pgcdc.svg)](https://pkg.go.dev/github.com/florinutz/pgcdc)

14 adapters | 3 detectors | WAL + LISTEN/NOTIFY + Outbox | Persistent slots | Dead letter queue | Event routing | Zero infrastructure

```bash
go install github.com/florinutz/pgcdc/cmd/pgcdc@latest
```

```
PostgreSQL ──▶ pgcdc (single Go binary) ──▶ Webhook, SSE, WebSocket, gRPC
               3 detectors:                  stdout, file, exec, PG table
               • LISTEN/NOTIFY               NATS JetStream
               • WAL logical replication      Typesense / Meilisearch
               • Outbox table polling         Redis cache invalidation
                                              pgvector embeddings
```

## Why pgcdc?

| | pgcdc | Debezium | Sequin | Conduit |
|--|-------|----------|--------|---------|
| Single binary | Yes | No (JVM+Kafka) | No (managed) | Yes |
| Memory (idle) | ~12 MB | ~500 MB+ | N/A | ~50 MB |
| PG-optimized | Yes | Generic | Yes | Generic |
| Built-in pgvector sync | Yes | No | No | No |
| SSE/WS/gRPC streaming | Yes | No | HTTP only | No |
| Search sync (Typesense/Meili) | Yes | No | No | Plugin |
| Redis cache invalidation | Yes | No | No | No |
| Dead letter queue | Yes | Yes | Yes | No |
| Event routing | Yes | Yes (SMTs) | No | Yes |
| External deps | 0 | Kafka+ZK | Managed | 0 |

## Quick Start

### Path 1: LISTEN/NOTIFY (simplest, no PG config needed)

```bash
# Generate trigger SQL
pgcdc init --table orders | psql mydb

# Stream changes to stdout
pgcdc listen -c pgcdc:orders --db postgres://localhost:5432/mydb
```

### Path 2: WAL Replication (production, no triggers)

Requires `wal_level=logical` in `postgresql.conf`.

```bash
# Create publication
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
| **search** | `-a search --search-url <url>` | Sync to Typesense or Meilisearch. Batched. |
| **redis** | `-a redis --redis-url <url>` | Cache invalidation or sync. DEL/SET per event. |
| **embedding** | `-a embedding --embedding-api-url <url>` | OpenAI-compatible API → pgvector UPSERT. |
| **iceberg** | `-a iceberg --iceberg-warehouse <path>` | Apache Iceberg table writes. |

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

## Snapshot: Initial Data Sync

Export existing rows as events, then transition to live streaming:

```bash
# Snapshot only
pgcdc snapshot --table orders --db postgres://...

# Snapshot-first: zero-gap initial sync + live WAL
pgcdc listen --detector wal --publication pgcdc_orders \
  --snapshot-first --snapshot-table orders --db postgres://...
```

## Persistent Slots + Checkpointing

Survive crashes and restarts with WAL position tracking:

```bash
pgcdc listen --detector wal --publication pgcdc_orders \
  --persistent-slot --slot-name pgcdc_orders \
  --db postgres://...
```

Checkpoints are stored in `pgcdc_checkpoints` table. The replication slot persists across disconnects.

## Transaction Metadata

```bash
# Add xid, commit_time, seq to events
pgcdc listen --detector wal --publication pgcdc_orders --tx-metadata --db postgres://...

# Wrap transactions in BEGIN/COMMIT markers
pgcdc listen --detector wal --publication pgcdc_orders --tx-markers --db postgres://...
```

## Observability

- **Prometheus metrics** at `/metrics` (30+ metrics)
- **Health check** at `/healthz` (per-component status)
- **Standalone metrics server**: `--metrics-addr :9090`
- **Slot lag monitoring**: `pgcdc_slot_lag_bytes` gauge + log warnings
- **Heartbeat**: `--heartbeat-interval 30s` keeps replication slots advancing

## Configuration

Three layers (highest priority wins): **CLI flags** > **env vars** (`PGCDC_` prefix) > **YAML config file** > **defaults**

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
   outbox)           filtered by route)
```

- **Concurrency**: `errgroup` manages all goroutines. One context cancellation tears everything down.
- **Backpressure**: Non-blocking sends. Full subscriber channel = dropped event + warning log.
- **Routing**: Bus-level filtering before fan-out. No event copies wasted.
- **DLQ**: Failed events captured to stderr or PG table for inspection/replay.

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
