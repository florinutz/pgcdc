# pgcdc

PostgreSQL, MySQL, MongoDB, and SQLite change data capture (LISTEN/NOTIFY, WAL logical replication, outbox pattern, MySQL binlog, MongoDB Change Streams, SQLite change tables, inbound webhook gateway, Kafka consumer, or NATS consumer) streaming to webhooks, SSE, stdout, files, exec processes, PG tables, WebSockets, pgvector embeddings, NATS JetStream, Kafka, Typesense/Meilisearch, Redis, gRPC, S3-compatible object storage, a built-in Kafka protocol server (speak Kafka wire protocol directly — no Kafka cluster needed), GraphQL subscriptions, Apache Arrow Flight, DuckDB in-process analytics, ClickHouse, and streaming SQL views (tumbling, sliding, and session window aggregation over CDC events).

## Architecture

```
Signal (SIGINT/SIGTERM)
  |
  v
Context ──> Pipeline (pgcdc.go orchestrates everything)
              |
  Detector ──> Bus (fan-out + routing) ──> Adapter (stdout)
  (listennotify     |                   ──> Adapter (webhook)     ──> DLQ
   walreplication   |                   ──> Adapter (file/exec)
   outbox           |                   ──> Adapter (pg_table)
   mysql            |                   ──> Adapter (SSE/WS)      ──> HTTP server
   mongodb          |                   ──> Adapter (nats/kafka/search/redis/grpc/s3)
   webhookgw        |                   ──> Adapter (embedding)   ──> DLQ
   kafka_consumer   |                   ──> Adapter (graphql)     ──> HTTP server
   nats_consumer    |                   ──> Adapter (clickhouse)
   or sqlite)       |
                    |                   ──> Adapter (arrow)       ──> gRPC Flight server
                    |                   ──> Adapter (duckdb)      ──> HTTP query endpoint
              ingest chan               ──> Adapter (view)        ──> re-inject to bus
                                subscriber chans (one per adapter, filtered by route)
                                      |
                              Health Checker + Prometheus Metrics
```

- **Concurrency**: `errgroup` + `safeGo` (panic-recovering wrapper). One context cancellation tears everything down.
- **Backpressure (bus)**: `--bus-mode fast` (default) drops on full subscriber channels. `--bus-mode reliable` blocks the detector — loss-free at throughput cost.
- **Event routing**: `--route adapter=channel1,channel2`. Route filtering happens per-adapter in the wrapper goroutine (not bus-level), which is why SIGHUP reload works without event loss.
- **SIGHUP hot-reload**: `kill -HUP <pid>` atomically swaps `transforms:` and `routes:` via `sync/atomic.Pointer[wrapperConfig]` per adapter. Adapters, detectors, bus mode, and CLI flags remain immutable.
- **Cooperative checkpointing**: `--cooperative-checkpoint` (WAL only). Adapters implement `adapter.Acknowledger` and call `AckFunc` after delivery; LSN checkpoint advances to `min(all ack positions)`.
- **Middleware chain**: Adapters implementing `adapter.Deliverer` get circuit breaker, rate-limit, retry, DLQ, tracing, and metrics wired automatically via `adapter/middleware`.
- **Wasm plugins**: 4 extension points (transforms, adapters, DLQ backends, checkpoint stores) via Extism. Zero overhead when unconfigured.
- **Error types**: `pgcdcerr/errors.go` — use typed errors for all cases callers need to branch on with `errors.Is`/`errors.As`.

## Code Conventions

- **Error wrapping**: `fmt.Errorf("verb: %w", err)` — verb describes the failed action (`connect:`, `listen:`, `subscribe:`). Use typed errors from `pgcdcerr/` for errors that callers need to branch on.
- **Logging**: `log/slog` only. Child loggers via `logger.With("component", name)`. Never `log` or `fmt.Printf`.
- **Constructors**: `New()` on every type. Always handle nil logger: `if logger == nil { logger = slog.Default() }`. Duration params default to sensible values when zero.
- **Channel direction**: Always `chan<-` or `<-chan` in function signatures. The bus owns channel lifecycle.
- **Context**: First param on all blocking functions. Use `context.WithTimeout` for cleanup operations.
- **Naming**: Short package names (`bus`, `sse`, `event`). Types named for what they are (`Detector`, `Adapter`, `Bus`).
- **No global state** except logger setup in `cmd/root.go` and Prometheus metrics registration in `metrics/`.
- **Config**: All config flows through `config.Config` struct. Viper handles CLI flags, env vars, and YAML file. All timeouts and backoff values are configurable — no hardcoded magic numbers.
- **Identifiers in SQL**: Use `pgx.Identifier{name}.Sanitize()` for table/channel names. Parameterized queries for values.

## Extending the System

### New adapter

1. Implement `adapter.Adapter` interface (`Start(ctx, <-chan event.Event) error`, `Name() string`) or `adapter.Deliverer` (`Deliver(ctx, event.Event) error`) for middleware-chain support (CB, rate-limit, retry, DLQ, tracing)
2. Optionally implement `adapter.Acknowledger` (`SetAckFunc(fn AckFunc)`) for cooperative checkpointing support — ack after fully handling each event (success, DLQ, or intentional skip)
3. Optionally implement `adapter.Validator` (`Validate(ctx) error`) for startup pre-flight checks (DNS, connectivity, bucket existence)
4. Optionally implement `adapter.Drainer` (`Drain(ctx) error`) for graceful shutdown flush (bounded by `shutdown_timeout`)
5. Optionally implement `adapter.HTTPMountable` (`MountHTTP(r chi.Router)`) if the adapter serves HTTP routes
6. Create `adapter/<name>/` package
7. Create `cmd/register_<name>.go` and register via `registry.RegisterAdapter(registry.AdapterEntry{...})` in `init()`, including `BindFlags` and `ViperKeys` callbacks
8. Add config struct in `internal/config/adapter_config.go`
9. Add defaults in `Default()` in `internal/config/config.go`
10. Add metrics instrumentation (`metrics.EventsDelivered.WithLabelValues("<name>").Inc()`)
11. Add scenario test, register in SCENARIOS.md

### New detector

1. Implement `detector.Detector` interface (`Start(ctx, chan<- event.Event) error`, `Name() string`)
2. Create `detector/<name>/` package
3. **MUST NOT** close the events channel — the bus owns its lifecycle
4. **Use `pgx.Connect`** not pool — LISTEN requires a dedicated connection
5. Create `cmd/register_detector_<name>.go` and register via `registry.RegisterDetector(registry.DetectorEntry{...})` in `init()`
6. Add config struct in `internal/config/detector_config.go` and defaults in `Default()`
7. Health checker registration is handled by the pipeline (`pgcdc.go`) — no manual `checker.Register` call needed
8. Add scenario test, register in SCENARIOS.md

### New CLI command

1. Create `cmd/<name>.go` (see `cmd/init.go` as template)
2. Register via `rootCmd.AddCommand(<name>Cmd)` in the file's `init()`
3. Bind flags to viper if config-file support is needed

## Do NOT

- Use a connection pool for detectors — breaks LISTEN state
- Close the events channel from a detector — bus owns lifecycle
- Block in SSE or WS broadcast — non-blocking sends only (bus reliable mode is explicitly opt-in via `--bus-mode reliable`)
- Use `log` or `fmt.Printf` — slog only
- Put raw SQL identifiers in Go strings — use `pgx.Identifier{}.Sanitize()`
- Add dependencies without justification — prefer stdlib (exceptions: `nats.go` for NATS adapter, `twmb/franz-go` for Kafka adapter)
- Hardcode timeouts or backoff values — put them in config with defaults

## Build Tags for Slim Binary

`no_kafka`, `no_grpc`, `no_iceberg`, `no_nats`, `no_redis`, `no_plugins`, `no_kafkaserver`, `no_views`, `no_s3`, `no_graphql`, `no_arrow`, `no_duckdb`, `no_sqlite`, `no_clickhouse`

## Dependencies

Direct deps (keep minimal): `pgx/v5` (PG driver), `pglogrepl` (WAL logical replication protocol), `cobra` + `viper` (CLI/config), `chi/v5` (HTTP router), `google/uuid` (UUIDv7), `errgroup` (concurrency), `prometheus/client_golang` (metrics), `coder/websocket` (WebSocket adapter), `nats-io/nats.go` (NATS JetStream adapter + consumer detector), `twmb/franz-go` (Kafka adapter + consumer detector), `redis/go-redis/v9` (Redis adapter), `google.golang.org/grpc` + `google.golang.org/protobuf` (gRPC adapter), `aws/aws-sdk-go-v2` (S3 adapter), `parquet-go/parquet-go` (Parquet writer for S3/Iceberg), `hamba/avro/v2` (Avro encoding), `go.opentelemetry.io/otel` (OpenTelemetry tracing), `extism/go-sdk` (Wasm plugin runtime), `go-mysql-org/go-mysql` (MySQL binlog replication), `go-sql-driver/mysql` (MySQL driver for schema queries), `go.mongodb.org/mongo-driver/v2` (MongoDB Change Streams detector), `pingcap/tidb/pkg/parser` (SQL parsing for view adapter), `apache/arrow-go` (Arrow Flight adapter), `modernc.org/sqlite` (SQLite detector), `duckdb/duckdb-go/v2` (DuckDB analytics adapter, CGO), `ClickHouse/clickhouse-go/v2` (ClickHouse adapter), `testcontainers-go` (test only).

## Testing

**Two test surfaces:**
- **Unit tests** (`*_test.go` in package dirs): pure logic only — no I/O, no network, no goroutines.
- **Scenario tests** (`scenarios/*_test.go`): full pipeline with real Postgres (testcontainers). One file per user journey. Primary regression barrier.

**Makefile targets:** `make test` (unit, ~2s) | `make test-scenarios-fast` (PG-only, `-parallel 8`, ~75s) | `make test-scenarios-docker` (external containers, `-parallel 4`, ~160s) | `make test-scenarios` (both tiers sequentially) | `make test-all` | `make lint` | `make coverage` | `make coverage-gate` | `make test-smoke` (<15s) | `make fuzz`

**Agent workflow — after every change:**
1. Run `make test-all` — all tests must pass before task is done.
2. Added new behavior? Check `SCENARIOS.md` — extend an existing scenario or add a new file.
3. Added a new scenario file? Register it in `SCENARIOS.md`.

**When to write unit tests:** Only pure functions with no side effects (backoff math, HMAC, payload parsing, transform logic). I/O or channels → scenario test instead.

**When to write scenario tests:** New user journey or new failure mode at a system boundary. Run `go test -cover ./scenarios/` baseline first — if coverage doesn't increase meaningfully, the test is redundant.

**Do NOT test:** third-party library behavior, stdlib, simple constructors, config defaults, any path already covered by a scenario. Never write both a unit test and a scenario for the same code path.

**Scenario file structure:** `//go:build integration` tag, `TestScenario_<Name>` top-level, `t.Parallel()` as FIRST line, `t.Run("happy path", ...)` + `t.Run("<failure>", ...)`, shared helpers from `scenarios/helpers_test.go`.

**Max scenarios:** 42 — consolidate related ones before adding new ones. Currently at 42.

**Parallel scenarios:** All `TestScenario_*` functions call `t.Parallel()`. The shared PG container supports concurrency: `max_replication_slots=60`, `max_wal_senders=60`, `max_connections=200`. **Never kill all PG backends** — use `terminateSlotBackend(t, connStr, slotName)` or `terminateListenBackend(t, connStr, channel)` to target a specific test's connection.

**testutil/ package:** Pure-Go test helpers importable from any package (not a `_test.go` file). Use `testutil.NewLineCapture()`, `testutil.MakeEvent()`, `testutil.MakeEvents()`, `testutil.StartTestBus()`. Do not duplicate these in individual packages.

**No time.Sleep for init waits:** Use a `ready chan struct{}` pattern (adapter) or probe functions (detector). `time.Sleep` only for external I/O that has no observable side effect.

**Coverage gate:** `make coverage-gate` enforces ≥75% coverage on pure-logic packages (transform, view, cel, bus, ack, backpressure, encoding, internal/circuitbreaker, internal/ratelimit).

**Golden files:** `event/testdata/golden/` holds expected JSON snapshots for event serialization. Regenerate with `go test -run TestEventJSON_Golden -args -update ./event/`. Breaking a golden file (e.g. renaming a payload field) fails `make test`.

**Fuzz targets:** `view/parser_fuzz_test.go` (FuzzParse), `cel/cel_fuzz_test.go` (FuzzCompile), `adapter/kafkaserver/` (protocol), `transform/` (chain). Run all with `make fuzz`.

## Code Organization

```
pgcdc.go        Pipeline type (library entry point)
cmd/            CLI commands (cobra); register_*.go files wire adapters/detectors into registry
adapter/
  adapter.go    Adapter, Deliverer, Acknowledger, Validator, Drainer, Reinjector, HTTPMountable, Batcher interfaces
  middleware/   Middleware chain for Deliverer adapters (CB, rate-limit, retry, DLQ, tracing, metrics)
  chain/        Link-based preprocessing (compress, encrypt links)
  stdout/ webhook/ sse/ file/ exec/ pgtable/ ws/ embedding/ nats/ kafka/
  search/ redis/ grpc/ s3/ iceberg/ kafkaserver/ view/ graphql/ arrow/ duckdb/ clickhouse/
view/           Streaming SQL engine (parser, AST, tumbling/sliding/session windows, aggregators)
registry/       Self-registering component registry (AdapterEntry, DetectorEntry, ParamSpec)
detector/
  listennotify/ walreplication/ outbox/ mysql/ mongodb/ multi/ webhookgw/ sqlite/ kafka/ nats/
  walreplication/toastcache/  LRU cache for unchanged TOAST column resolution
ack/            Cooperative checkpoint LSN tracker (min across all adapter ack positions)
backpressure/   WAL lag backpressure: green/yellow/red zones, throttle/pause/shed
bus/            Event fan-out (fast-drop or reliable-block mode)
transform/      TransformFunc chain; built-ins: drop_columns, rename_fields, mask, filter, debezium, cloudevents, cel_filter, dedup
cel/            CEL expression compiler/evaluator for event filtering
snapshot/       Table snapshot (COPY-based + incremental chunk-based)
inspect/        Ring-buffer event tap (post-detector, post-transform, pre-adapter) + HTTP handlers
schema/         Versioned schema store (memory + PG backends)
checkpoint/     LSN checkpoint store interface + PG implementation
dlq/            DLQ interface + stderr/PGTable/Nop backends + sliding nack window
server/         Multi-pipeline manager + REST API (pgcdc serve)
encoding/       Avro/Protobuf/JSON encoders + Confluent Schema Registry client
tracing/        OpenTelemetry setup + Kafka header carrier
plugin/wasm/    Extism runtime: transform, adapter, DLQ, checkpoint store plugin types
pgcdcerr/       Typed errors for errors.Is/errors.As matching
event/          Event model (UUIDv7, JSON payload, LSN)
health/         Component health checker
metrics/        Prometheus metric definitions (all pgcdc_* names live here)
internal/config/     config.go (core+Default), adapter_config.go, detector_config.go, plugin_config.go, validate.go
internal/server/     HTTP server: SSE, WS, metrics, health (CLI-only)
internal/circuitbreaker/  Three-state CB (closed/open/half-open)
internal/ratelimit/       Token-bucket rate limiter with Prometheus metrics
internal/migrate/         Embedded SQL migrations (pgcdc_migrations table)
internal/safegoroutine/   Panic recovery wrapper for errgroup
internal/output/     Shared --output (table|json) printer for all CLI commands
internal/logcolor/   Colored slog handler (ANSI level labels, TTY detection)
internal/prompt/     Interactive terminal prompts (Select, MultiSelect, Confirm)
scenarios/      Integration tests; helpers_test.go has shared PG container + pipeline wiring
testutil/       Test utilities
```

## Key Files

- `pgcdc.go` — Pipeline type, options, Run/RunSnapshot; start here for pipeline wiring changes
- `cmd/listen.go` — CLI listen command (HTTP servers, SIGHUP handler)
- `cmd/reload.go` — SIGHUP reload: specToTransform, route merging, atomic wrapperConfig swap
- `cmd/register_*.go` — One file per adapter/detector; each calls `registry.Register*` in `init()`
- `adapter/adapter.go` — All adapter interfaces: Adapter, Deliverer, Acknowledger, Validator, Drainer, Reinjector, Traceable
- `adapter/middleware/middleware.go` — Middleware chain: CB + rate-limit + retry + DLQ + tracing + metrics + ack
- `detector/detector.go` — Detector interface
- `registry/registry.go` — AdapterEntry, DetectorEntry; BindFlags, ViperKeys, Create factories
- `registry/spec.go` — ParamSpec, ConnectorSpec for `pgcdc describe`
- `bus/bus.go` — Fan-out: fast (drop) or reliable (block); subscriber channel lifecycle
- `ack/tracker.go` — min-LSN tracker across all acknowledging adapters
- `backpressure/backpressure.go` — Zone transitions, proportional throttle, pause/resume, shedding
- `transform/transform.go` — TransformFunc, Chain, ErrDropEvent; entry point for all transform work
- `event/event.go` — Event model; all pipeline data flows through this type
- `metrics/metrics.go` — All Prometheus metric definitions; add new metrics here
- `pgcdcerr/errors.go` — All typed errors; add new error types here
- `internal/config/config.go` — Core Config + Default(); shared types (Bus, OTel, DLQ, Backpressure)
- `internal/config/adapter_config.go` — Per-adapter config structs
- `internal/config/detector_config.go` — Per-detector config structs
- `internal/migrate/migrate.go` — Migration runner; add migrations to `internal/migrate/sql/`
- `checkpoint/checkpoint.go` — LSN checkpoint store interface + PG implementation
- `dlq/dlq.go` — DLQ interface + StderrDLQ + PGTableDLQ + NopDLQ
- `server/manager.go` — Multi-pipeline manager (pgcdc serve): Add, Remove, Start, Stop, Pause, Resume
- `plugin/wasm/runtime.go` — Extism module compilation + instance pooling; entry point for plugin work
- `adapter/graphql/graphql.go` — GraphQL subscriptions adapter (graphql-transport-ws protocol)
- `adapter/arrow/arrow.go` — Arrow Flight gRPC adapter (per-channel ring buffers)
- `adapter/duckdb/duckdb.go` — DuckDB in-process analytics adapter (buffered ingest + SQL query endpoint)
- `detector/webhookgw/webhookgw.go` — Webhook gateway detector (HTTP ingest endpoint)
- `detector/sqlite/sqlite.go` — SQLite CDC detector (poll-based change table)
- `adapter/clickhouse/clickhouse.go` — ClickHouse batch INSERT adapter (ReplacingMergeTree, auto-create, batch.Runner, async inserts, custom query settings, in-batch dedup)
- `detector/kafka/kafka.go` — Kafka consumer group detector (franz-go, reconnect loop)
- `detector/nats/nats.go` — NATS JetStream durable consumer detector
- `cmd/doctor.go` — System health checker (docker, config, DB, slots, WAL, adapters)
- `cmd/quickstart.go` — Interactive project generator (config, SQL, docker-compose)
- `cmd/plugin_scaffold.go` — Wasm plugin project scaffolding
- `cmd/config_diff.go` — YAML config diff command
- `internal/output/summary.go` — Startup config summary banner
- `internal/config/diff.go` — SIGHUP reload config diffing
- `scenarios/helpers_test.go` — Shared test infra: PG container setup, pipeline wiring helpers
