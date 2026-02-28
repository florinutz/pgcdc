# pgcdc

**The CDC tool built for the AI era. Capture → Transform → Deliver. Single binary.**

[![CI](https://github.com/florinutz/pgcdc/actions/workflows/ci.yml/badge.svg)](https://github.com/florinutz/pgcdc/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/florinutz/pgcdc)](https://goreportcard.com/report/github.com/florinutz/pgcdc)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/florinutz/pgcdc.svg)](https://pkg.go.dev/github.com/florinutz/pgcdc)

```bash
go install github.com/florinutz/pgcdc/cmd/pgcdc@latest
```

```
 CAPTURE                    TRANSFORM                   DELIVER
┌──────────────────┐    ┌──────────────────────┐    ┌──────────────────────────┐
│ PostgreSQL       │    │ Drop / rename / mask │    │ Webhook, SSE, WebSocket  │
│  • LISTEN/NOTIFY │───▶│ Filter / CEL filter   │───▶│ gRPC, stdout, file, exec │
│  • WAL logical   │    │ Debezium envelope     │    │ Kafka, NATS JetStream    │
│  • Outbox table  │    │ CloudEvents envelope  │    │ S3, Redis, Typesense     │
│ MySQL binlog     │    │ Streaming SQL views   │    │ pgvector embeddings      │
│ MongoDB streams  │    │ Wasm plugins          │    │ Kafka wire protocol svr  │
└──────────────────┘    └──────────────────────┘    └──────────────────────────┘
 5 detectors               7 built-in transforms       17 adapters
```

pgcdc is the only single-binary CDC tool combining multi-source capture, a built-in Kafka wire protocol server, streaming SQL analytics, and real-time pgvector embedding sync — with zero external dependencies.

## Why pgcdc?

| | pgcdc | Debezium | Sequin | Conduit |
|--|-------|----------|--------|---------|
| Single binary | Yes | No (JVM+Kafka) | No (managed) | Yes |
| Memory (idle) | **15 MB** | 288 MB | N/A | ~50 MB |
| Throughput | **166K events/sec** | 2.2K events/sec | N/A | N/A |
| Latency (p50) | **2.3 ms** | 548 ms | N/A | N/A |
| PG/MySQL/MongoDB | Yes | Generic | Yes | Generic |
| Built-in pgvector sync | Yes | No | No | No |
| Smart embedding skip | Yes | No | No | No |
| Kafka wire protocol server | Yes | No | No | No |
| Streaming SQL views | Yes | No | No | No |
| SSE/WS/gRPC streaming | Yes | No | HTTP only | No |
| Search sync (Typesense/Meili) | Yes | No | No | Plugin |
| Redis cache invalidation | Yes | No | No | No |
| Circuit breaker + rate limiting | Yes | Yes | N/A | No |
| Dead letter queue + replay | Yes | Yes | Yes | No |
| Debezium-compatible output | Yes | Native | No | No |
| Wasm plugin system | Yes | No | No | Go plugins |
| External deps | 0 | Kafka+ZK | Managed | 0 |

### Benchmarks (pgcdc vs Debezium Server)

Automated, reproducible benchmarks running both tools against the same PostgreSQL 16 instance with testcontainers. Debezium Server 2.5 runs in a JVM container with HTTP sink. Apple M2 Max, 16 GB RAM.

| Metric | pgcdc (channel) | pgcdc (HTTP sink) | Debezium Server |
|--------|----------------:|------------------:|----------------:|
| **Startup to first event** | 2.1 s | — | 3.2 s |
| **Memory at idle** | **15 MB** | **15 MB** | 321 MB |
| **Throughput** (10K rows) | 165,615 events/sec | **18,061 events/sec** | 2,221 events/sec |
| **Latency p50** | 2.3 ms | **2.7 ms** | 548 ms |
| **Latency p99** | 5.2 ms | **6.7 ms** | 842 ms |

_pgcdc (channel)_ uses an in-process Go channel — zero network overhead, pgcdc's native fastest path.
_pgcdc (HTTP sink)_ uses the webhook adapter (HTTP POST to localhost) — the same delivery mechanism as Debezium's HTTP sink. **Even with HTTP overhead, pgcdc delivers 8× more throughput and 200× lower p50 latency.**

<details>
<summary>Reproduce these results</summary>

```bash
make bench-debezium  # requires Docker, ~2 min
```

Or run pgcdc-only benchmarks (no Debezium container):
```bash
./bench/run.sh       # throughput, latency, memory
```

</details>

## 5-Minute Demo

See the full system in action — all 5 outputs flowing simultaneously:

```bash
cd examples/showcase
docker compose up --build
# Open http://localhost:3000 for the live dashboard
```

The showcase starts PostgreSQL, pgcdc (WAL mode with stdout + SSE + S3 + Kafka server + streaming SQL), MinIO, a live SSE dashboard, a Kafka consumer, and a data generator. See [examples/showcase/README.md](examples/showcase/README.md).

## Quick Start

### LISTEN/NOTIFY (simplest, no PG config needed)

```bash
pgcdc init --table orders | psql mydb
pgcdc listen -c pgcdc:orders --db postgres://localhost:5432/mydb
```

### WAL Replication — zero-config (production)

Requires `wal_level=logical` in `postgresql.conf`.

```bash
pgcdc listen --all-tables --db postgres://localhost:5432/mydb
```

Auto-creates a `FOR ALL TABLES` publication. Add `--persistent-slot` for crash-resilient streaming.

### WAL Replication — per-table

```bash
pgcdc init --table orders --detector wal | psql mydb
pgcdc listen --detector wal --publication pgcdc_orders \
  --persistent-slot --db postgres://localhost:5432/mydb
```

### Outbox Pattern

```bash
pgcdc init --table my_outbox --adapter outbox | psql mydb
pgcdc listen --detector outbox --outbox-table my_outbox --db postgres://...
```

### MySQL Binlog

```bash
pgcdc listen --detector mysql \
  --mysql-addr localhost:3306 --mysql-user root --mysql-password secret \
  --mysql-server-id 100 --mysql-tables mydb.orders -a stdout
```

### MongoDB Change Streams

```bash
pgcdc listen --detector mongodb \
  --mongodb-uri mongodb://localhost:27017 \
  --mongodb-database mydb --mongodb-collections orders -a stdout
```

## Adapters (17)

| Adapter | Flag | Description |
|---------|------|-------------|
| **stdout** | `-a stdout` | JSON lines to stdout. Pipe-friendly. |
| **webhook** | `-a webhook -u <url>` | HTTP POST with retries, HMAC signing, circuit breaker, rate limiting. |
| **SSE** | `-a sse` | Server-Sent Events with channel filtering. |
| **WebSocket** | `-a ws` | WebSocket streaming with channel filtering. |
| **gRPC** | `-a grpc` | gRPC streaming server with channel filtering. |
| **file** | `-a file --file-path <path>` | JSON lines to file with automatic rotation. |
| **exec** | `-a exec --exec-command <cmd>` | Pipe JSON to subprocess stdin. Auto-restarts. |
| **pg_table** | `-a pg_table` | INSERT into a PostgreSQL table. |
| **NATS** | `-a nats --nats-url <url>` | Publish to NATS JetStream with dedup. |
| **kafka** | `-a kafka --kafka-brokers <addrs>` | Publish to Kafka with circuit breaker. `--kafka-transactional-id` for exactly-once. |
| **search** | `-a search --search-url <url>` | Sync to Typesense or Meilisearch. Batched. |
| **redis** | `-a redis --redis-url <url>` | Cache invalidation or sync. DEL/SET per event. |
| **embedding** | `-a embedding --embedding-api-url <url>` | OpenAI-compatible API → pgvector UPSERT. Smart skip on unchanged columns. |
| **s3** | `-a s3 --s3-bucket <name>` | Flush to S3-compatible storage (JSON Lines/Parquet). |
| **iceberg** | `-a iceberg --iceberg-warehouse <path>` | Apache Iceberg table writes. |
| **kafkaserver** | `-a kafkaserver` | Kafka wire protocol server. Any Kafka consumer connects directly — no Kafka cluster needed. |
| **view** | `--view-query 'name:SQL'` | Streaming SQL over CDC events. Tumbling, sliding, and session windows. |

Use multiple adapters: `-a stdout -a webhook -a redis`

## Circuit Breaker + Rate Limiting

Protect downstream systems from overload:

```bash
# Webhook with circuit breaker (opens after 5 failures, resets after 60s)
pgcdc listen --all-tables -a webhook -u https://... \
  --webhook-cb-failures 5 --webhook-cb-reset 60s \
  --webhook-rate-limit 100 --webhook-rate-burst 20 \
  --db postgres://...

# Embedding with rate limit (respect API quotas)
pgcdc listen --all-tables -a embedding --embedding-api-url https://... \
  --embedding-rate-limit 50 --embedding-rate-burst 10 \
  --db postgres://...
```

When a circuit breaker opens, events go to the DLQ for later replay.

## Smart Embedding Sync

pgcdc's embedding adapter syncs CDC events to pgvector for real-time RAG. On UPDATE, it detects when embedding-relevant columns haven't changed and skips the API call entirely:

```bash
pgcdc listen --all-tables -a embedding \
  --embedding-api-url https://api.openai.com/v1/embeddings \
  --embedding-columns title,description \
  --embedding-skip-unchanged \
  --db postgres://...
```

This skips 60-80% of embedding API calls on typical workloads where non-text columns change frequently.

## Event Routing

```bash
pgcdc listen --all-tables \
  -a webhook -a search -a redis \
  --route webhook=pgcdc:orders,pgcdc:users \
  --route search=pgcdc:articles \
  --route redis=pgcdc:orders \
  --db postgres://...
```

## Dead Letter Queue

```bash
pgcdc listen ... --dlq pg_table
pgcdc dlq list --dlq-db postgres://... --format table
pgcdc dlq replay --adapter webhook --webhook-url https://...
pgcdc dlq purge --replayed
```

## Transforms

```bash
pgcdc listen --all-tables --drop-columns password,api_key --db postgres://...
pgcdc listen --all-tables --filter-operations INSERT,UPDATE --db postgres://...
pgcdc listen --all-tables --debezium-envelope --db postgres://...
```

Built-in: `drop_columns`, `rename_fields`, `mask` (hash/redact), `filter`, `cel_filter`, `debezium`, `cloudevents`. Full config via YAML (global or per-adapter). Custom transforms via Wasm plugins.

CEL filter example:
```bash
pgcdc listen --all-tables --filter-cel 'operation == "INSERT" && table == "orders"' --db postgres://...
```

## Streaming SQL Views

```bash
# Tumbling window
pgcdc listen --all-tables -a stdout \
  --view-query 'counts:SELECT channel, COUNT(*) as cnt FROM pgcdc_events TUMBLING WINDOW 10s GROUP BY channel' \
  --db postgres://...

# Sliding window
pgcdc listen --all-tables -a stdout \
  --view-query 'avg:SELECT channel, AVG(CAST(payload.amount AS DECIMAL)) FROM pgcdc_events SLIDING WINDOW 60s SLIDE 10s GROUP BY channel' \
  --db postgres://...

# Session window
pgcdc listen --all-tables -a stdout \
  --view-query 'sessions:SELECT channel, COUNT(*) FROM pgcdc_events SESSION WINDOW 30s GROUP BY channel' \
  --db postgres://...
```

Aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `COUNT(DISTINCT ...)`, `STDDEV`. Results re-injected as `VIEW_RESULT` events.

## Kafka Wire Protocol Server

Any Kafka consumer library connects directly to pgcdc — no Kafka cluster needed:

```bash
pgcdc listen --all-tables -a kafkaserver --kafkaserver-addr :9092 --db postgres://...

# Then from any Kafka client:
kcat -b localhost:9092 -t pgcdc.public.orders -C
```

Channels become topics, events hash across N partitions, consumer groups with partition assignment and heartbeat reaping. Supports all 11 Kafka API keys.

## Production Features

- **Persistent slots + checkpointing**: `--persistent-slot` survives crashes, WAL position tracked in `pgcdc_checkpoints`
- **Cooperative checkpointing**: `--cooperative-checkpoint` advances only after all adapters acknowledge
- **Backpressure**: `--backpressure` monitors WAL lag with green/yellow/red zones, throttles and sheds
- **TOAST cache**: `--toast-cache` resolves unchanged large columns without `REPLICA IDENTITY FULL`
- **Heartbeat**: `--heartbeat-interval 30s` prevents WAL bloat on idle databases
- **Schema evolution**: `--schema-events` emits `SCHEMA_CHANGE` events when columns change
- **Transaction metadata**: `--tx-metadata` adds xid/commit_time/seq, `--tx-markers` adds BEGIN/COMMIT events
- **Incremental snapshots**: Signal-table triggered, crash-resumable chunk-based snapshots alongside live WAL
- **SIGHUP reload**: `kill -HUP <pid>` atomically swaps transforms + routes with zero event loss
- **Adapter validation**: Pre-flight checks (DNS, connectivity) at startup, skip with `--skip-validation`
- **Multi-pipeline server**: `pgcdc serve --config pipelines.yaml` manages N pipelines with REST API (start/stop/pause/resume)
- **Event inspector**: `--inspect-buffer 100` samples events at tap points, view at `/inspect` or stream via `/inspect/stream`
- **Multi-detector**: Compose detectors with sequential, parallel, or failover modes (`--detector-mode failover`)
- **Connector introspection**: `pgcdc describe kafka` shows all parameters, types, and defaults

## Observability

- **Prometheus metrics** at `/metrics` (60+ metrics covering all adapters, circuit breakers, rate limiters, backpressure, TOAST cache)
- **Health check** at `/healthz` (per-component liveness)
- **Readiness probe** at `/readyz` (Kubernetes-ready)
- **OpenTelemetry tracing**: `--otel-exporter otlp --otel-endpoint localhost:4317`
- **Grafana dashboard**: Pre-built at [examples/observability/](examples/observability/)
- **Alerting rules**: Prometheus alerts at [examples/observability/alerts.yml](examples/observability/alerts.yml)

## Configuration

Three layers (highest wins): **CLI flags** > **env vars** (`PGCDC_` prefix) > **YAML config** > **defaults**.

```bash
# Generate a config template
pgcdc config init --detector wal --adapter webhook,kafka -o pgcdc.yaml

# Validate offline
pgcdc config validate pgcdc.yaml

# Validate with connectivity checks
pgcdc validate --db postgres://...
```

**Shell completion**: `pgcdc completion bash|zsh|fish|powershell`

## Wasm Plugins

Extend pgcdc with WebAssembly plugins (Extism-based, pure Go). Four extension points: transforms, adapters, DLQ backends, checkpoint stores.

```bash
pgcdc listen --all-tables --plugin-transform ./my-transform.wasm --db postgres://...
```

See [examples/plugins/](examples/plugins/) for a complete example.

## Deployment

- **Helm chart**: [deploy/helm/pgcdc/](deploy/helm/pgcdc/) with ServiceMonitor, PDB, documented values
- **Docker**: `make docker-build` or `docker compose up` with [examples/showcase/](examples/showcase/)
- **Homebrew**: `brew install florinutz/tap/pgcdc`
- **Slim binary**: `make build-slim` excludes heavy adapters (~60% smaller)

Available build tags: `no_kafka`, `no_grpc`, `no_iceberg`, `no_nats`, `no_redis`, `no_plugins`, `no_kafkaserver`, `no_views`, `no_s3`.

## Architecture

See [docs/architecture.md](docs/architecture.md) for detailed diagrams.

```
Signal (SIGINT/SIGTERM)
  |
  v
Context ──▶ Pipeline (pgcdc.go)
              |
  Detector ──▶ Bus (fan-out + routing) ──▶ [Transform] ──▶ Adapter ──▶ DLQ
  (5 types)    fast (drop) or                (chain)      (17 types)  (on failure)
               reliable (block)
```

- **Concurrency**: `errgroup` + panic-recovering `safeGo` wrapper. One context cancellation tears everything down.
- **Delivery guarantees**: Per-adapter (at-most-once to exactly-once). See [docs/delivery-guarantees.md](docs/delivery-guarantees.md).
- **Resilience**: Reconnect with backoff, circuit breakers, rate limiters, DLQ, backpressure. See [docs/resilience.md](docs/resilience.md).
- **Scaling**: Single-process with persistent slots for crash resilience. See [docs/adr/006-single-process-ha.md](docs/adr/006-single-process-ha.md).

## Development

```bash
make build          # compile binary
make test           # unit tests (~2s)
make test-scenarios # scenario tests with Docker (~30s)
make test-all       # both
make lint           # golangci-lint
make coverage       # coverage report
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full development guide.

## License

Apache 2.0
