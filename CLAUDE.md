# pgcdc

PostgreSQL, MySQL, and MongoDB change data capture (LISTEN/NOTIFY, WAL logical replication, outbox pattern, MySQL binlog, or MongoDB Change Streams) streaming to webhooks, SSE, stdout, files, exec processes, PG tables, WebSockets, pgvector embeddings, NATS JetStream, Kafka, Typesense/Meilisearch, Redis, gRPC, and S3-compatible object storage.

## Quick Start

```sh
make build
./pgcdc listen --db postgres://... --channel orders

# Or via Docker
make docker-build
docker compose up -d
```

## Architecture

```
Signal (SIGINT/SIGTERM)
  |
  v
Context ──> Pipeline (pgcdc.go orchestrates everything)
              |
  Detector ──> Bus (fan-out + routing) ──> Adapter (stdout)
  (listennotify     |                   ──> Adapter (webhook)     ──> DLQ
   walreplication   |                   ──> Adapter (file)
   outbox           |                   ──> Adapter (exec)
   mysql            |
   or mongodb)      |
                    |                   ──> Adapter (pg_table)
                    |                   ──> Adapter (SSE broker)  ──> HTTP server
              ingest chan               ──> Adapter (WS broker)   ──> HTTP server
                                        ──> Adapter (NATS JetStream)
                                        ──> Adapter (Kafka)          ──> DLQ
                                        ──> Adapter (search: Typesense/Meilisearch)
                                        ──> Adapter (redis: invalidate/sync)
                                        ──> Adapter (gRPC streaming)
                                        ──> Adapter (S3: JSON Lines/Parquet)
                                        ──> Adapter (embedding)  ──> DLQ
                                subscriber chans (one per adapter, filtered by route)
                                      |
                              Health Checker (per-component status)
                              Prometheus Metrics (/metrics)
```

- **Concurrency**: `errgroup` manages all goroutines. One context cancellation tears everything down.
- **Backpressure**: `--bus-mode fast` (default) drops events on full subscriber channels. `--bus-mode reliable` blocks the detector instead of dropping — loss-free at the cost of throughput.
- **All-tables zero-config**: `--all-tables` auto-creates a `FOR ALL TABLES` publication and switches to WAL detector. Zero setup — no `pgcdc init`, no manual SQL, no `--publication` needed. Idempotent: reuses existing publication on restart. Default publication name: `all` (override with `--publication`).
- **Transaction metadata**: WAL detector optionally enriches events with `transaction.xid`, `transaction.commit_time`, `transaction.seq` when `--tx-metadata` is enabled. `--tx-markers` adds synthetic BEGIN/COMMIT events on channel `pgcdc:_txn` (implies `--tx-metadata`). LISTEN/NOTIFY events omit this field (protocol has no tx info).
- **Snapshot-first**: `--snapshot-first --snapshot-table <table>` on listen (WAL only) runs a table snapshot using the replication slot's exported snapshot before transitioning to live streaming. Zero-gap delivery — snapshot sees exactly the data at the slot's consistent point, WAL streams everything after.
- **Persistent slots + checkpointing**: `--persistent-slot` creates a named, non-temporary replication slot with LSN checkpointing to `pgcdc_checkpoints` table. Survives crash/restart. Configurable via `--slot-name`, `--checkpoint-db`.
- **Type information**: `--include-schema` adds column type metadata (`columns` array with `name`, `type_oid`, `type_name`) to WAL events. OID-to-name mapping for 40+ common PG types.
- **Schema evolution**: `--schema-events` emits `SCHEMA_CHANGE` events on `pgcdc:_schema` channel when RelationMessage columns change (added, removed, type changed).
- **Heartbeat**: `--heartbeat-interval 30s` periodically writes to `pgcdc_heartbeat` table to keep replication slots advancing on idle databases. Prevents WAL bloat.
- **Slot lag monitoring**: `pgcdc_slot_lag_bytes` gauge metric + log warnings when lag exceeds `--slot-lag-warn` threshold (default 100MB).
- **Source-aware backpressure**: `--backpressure` monitors WAL lag and automatically throttles/pauses/sheds to prevent PG disk exhaustion (requires `--detector wal` + `--persistent-slot`). Three zones: green (full speed), yellow (throttle detector + shed best-effort adapters), red (pause detector + shed normal+best-effort). Hysteresis: red exits only when lag drops below warn. Throttle proportional to lag position in yellow band. `--bp-warn-threshold` (default 500MB), `--bp-critical-threshold` (default 2GB), `--bp-max-throttle` (default 500ms), `--bp-poll-interval` (default 10s). `--adapter-priority name=critical|normal|best-effort`. Shed = auto-ack events without delivering (cooperative checkpoint advances normally). Metrics: `pgcdc_backpressure_state`, `pgcdc_backpressure_throttle_duration_seconds`, `pgcdc_backpressure_load_shed_total{adapter}`.
- **NATS JetStream adapter**: `--adapter nats` publishes events to NATS JetStream with subject mapping (`pgcdc:orders` → `pgcdc.orders`), dedup via `Nats-Msg-Id` header, auto-stream creation.
- **Search adapter**: `--adapter search` syncs to Typesense or Meilisearch. Batched upserts, individual deletes. `--search-engine`, `--search-url`, `--search-api-key`, `--search-index`.
- **Redis adapter**: `--adapter redis` for cache invalidation (`DEL` on any change) or sync (`SET`/`DEL`). `--redis-url`, `--redis-mode invalidate|sync`, `--redis-key-prefix`.
- **gRPC adapter**: `--adapter grpc` starts a gRPC streaming server. Clients call `Subscribe(SubscribeRequest)` with optional channel filter. Proto at `adapter/grpc/proto/pgcdc.proto`.
- **S3 adapter**: `--adapter s3` buffers events and periodically flushes partitioned objects (Hive-style `channel=.../year=.../month=.../day=.../`) to any S3-compatible store (AWS S3, MinIO, R2, etc.). JSON Lines (default) or Parquet format. Time+size flush triggers, atomic buffer swap, all-or-nothing upload per flush. `--s3-bucket`, `--s3-prefix`, `--s3-endpoint`, `--s3-region`, `--s3-access-key-id`, `--s3-secret-access-key`, `--s3-format`, `--s3-flush-interval`, `--s3-flush-size`, `--s3-drain-timeout`.
- **Dead letter queue**: `--dlq stderr|pg_table|none`. Failed events captured to stderr (JSON lines) or `pgcdc_dead_letters` table. Adapters with DLQ support: webhook, embedding, kafka.
- **Kafka adapter**: `--adapter kafka` publishes events to Kafka topics with per-event key (`event.ID`), headers (`pgcdc-channel`, `pgcdc-operation`, `pgcdc-event-id`), and `RequireAll` acks. Channel-to-topic mapping: `pgcdc:orders` → `pgcdc.orders`. `--kafka-topic` overrides with a fixed topic. SASL (plain, SCRAM-SHA-256/512) and TLS supported. Terminal Kafka errors (non-retriable) go to DLQ; connection errors trigger reconnect with backoff. `--kafka-transactional-id` enables exactly-once delivery via Kafka transactions (each event produced in its own transaction).
- **Event routing**: `--route adapter=channel1,channel2`. Bus-level filtering before fan-out. Adapters without routes receive all events.
- **Outbox detector**: `--detector outbox` polls a transactional outbox table using `SELECT ... FOR UPDATE SKIP LOCKED` for concurrency-safe processing. Configurable DELETE or `processed_at` update cleanup.
- **MySQL binlog detector**: `--detector mysql` connects to MySQL as a replication slave using `go-mysql-org/go-mysql` `BinlogSyncer`. Captures INSERT/UPDATE/DELETE from MySQL binlog (ROW format required). `--mysql-addr`, `--mysql-user`, `--mysql-password`, `--mysql-server-id` (required, > 0), `--mysql-tables` (schema.table filter), `--mysql-gtid` (GTID mode), `--mysql-flavor` (mysql/mariadb). Events emitted on `pgcdc:<schema>.<table>` channels. Column names resolved from TableMapEvent metadata (MySQL 8.0.1+), `information_schema` fallback, or `col_N` fallback. Position encoded as `(file_seq << 32) | offset` for checkpoint compatibility.
- **MongoDB Change Streams detector**: `--detector mongodb` uses MongoDB's native Change Streams API (requires replica set or sharded cluster). Captures insert/update/replace/delete operations. `--mongodb-uri` (required), `--mongodb-database` (required unless cluster scope), `--mongodb-collections` (collection filter), `--mongodb-scope collection|database|cluster`, `--mongodb-full-document updateLookup|default`. Events emitted on `pgcdc:<database>.<collection>` channels. System events (drop/rename/invalidate) on `pgcdc:_mongo`. Resume tokens persisted to MongoDB metadata collection (`pgcdc_resume_tokens`) for crash-resumable streaming. `--mongodb-metadata-db`, `--mongodb-metadata-coll`. No cooperative checkpoint or backpressure support (no WAL LSN equivalent). Metrics: `pgcdc_mongodb_events_received_total`, `pgcdc_mongodb_errors_total`, `pgcdc_mongodb_resume_token_saves_total`.
- **Shutdown**: Signal cancels root context. Bus closes subscriber channels. HTTP server gets `shutdown_timeout` (default 5s) `context.WithTimeout` for graceful drain.
- **SIGHUP config reload**: `kill -HUP <pid>` re-reads the YAML config file and atomically swaps `transforms:` and `routes:` sections for all running adapters with zero event loss. CLI flags, plugin transforms, adapters, detectors, and bus mode remain immutable. Implementation: `sync/atomic.Pointer[wrapperConfig]` per adapter; the wrapper goroutine loads from this pointer on every event. `Pipeline.Reload(ReloadConfig)` rebuilds and stores atomically. Metrics: `pgcdc_config_reloads_total`, `pgcdc_config_reload_errors_total`. YAML `routes:` section maps adapter names to channel lists (CLI `--route` wins for same adapter name).
- **Wiring**: `pgcdc.go` provides the reusable `Pipeline` type (detector + bus + adapters). `cmd/listen.go` adds CLI-specific HTTP servers on top.
- **Observability**: Prometheus metrics exposed at `/metrics`. Rich health check at `/healthz` returns per-component status (200 when all up, 503 when any down). Standalone metrics server via `--metrics-addr`.
- **Embedding adapter**: `--adapter embedding` with `--embedding-api-url`, `--embedding-columns`, `--embedding-api-key`. Calls any OpenAI-compatible endpoint, UPSERTs vector into pgvector table. INSERT/UPDATE → embed+upsert, DELETE → delete vector. Zero new deps — vectors stored as strings with `::vector` cast.
- **Incremental snapshots**: `--incremental-snapshot` enables chunk-based `SELECT ... WHERE pk > ? LIMIT N` snapshots running alongside live WAL streaming. Signal-table triggered (`pgcdc_signals`), progress persisted to `pgcdc_snapshot_progress`, crash-resumable. Emits `SNAPSHOT_STARTED`, `SNAPSHOT` (row), and `SNAPSHOT_COMPLETED` events on `pgcdc:_snapshot`. `--snapshot-chunk-size`, `--snapshot-chunk-delay`, `--snapshot-progress-db`.
- **Transform pipeline**: `--drop-columns col1,col2` and `--filter-operations INSERT,UPDATE` as CLI shortcuts. Full config via `transforms.global` and `transforms.adapter.<name>` in YAML. Built-in types: `drop_columns`, `rename_fields`, `mask` (zero/hash/redact modes), `filter` (by field value or operation), `debezium` (rewrites payload into Debezium envelope with before/after/op/source/transaction blocks — `--debezium-envelope`, `--debezium-connector-name`, `--debezium-database`). Applied per-adapter or globally; dropped events increment `pgcdc_transform_dropped_total`, errors increment `pgcdc_transform_errors_total`.
- **Cooperative checkpointing**: `--cooperative-checkpoint` (requires `--persistent-slot` + `--detector wal`). Adapters call `AckFunc` after delivery; checkpoint only advances to `min(all adapter ack positions)`. Non-`Acknowledger` adapters are auto-acked on channel send. Metrics: `pgcdc_ack_position{adapter}`, `pgcdc_cooperative_checkpoint_lsn`.
- **Wasm plugin system**: Extism-based (pure Go, no CGo) plugin system for 4 extension points: transforms, adapters, DLQ backends, checkpoint stores. Plugins compiled to `.wasm` from any Extism PDK language (Rust, Go, Python, TypeScript). Zero overhead when no plugins configured. JSON serialization (protobuf opt-in). Host functions: `pgcdc_log`, `pgcdc_metric_inc`, `pgcdc_http_request`. Config via `plugins:` YAML block or `--plugin-transform`, `--plugin-adapter`, `--dlq plugin`, `--checkpoint-plugin` CLI flags. Metrics: `pgcdc_plugin_calls_total`, `pgcdc_plugin_duration_seconds`, `pgcdc_plugin_errors_total`.
- **Error types**: `pgcdcerr/` provides typed errors (`ErrBusClosed`, `WebhookDeliveryError`, `DetectorDisconnectedError`, `ExecProcessError`, `EmbeddingDeliveryError`, `NatsPublishError`, `OutboxProcessError`, `IcebergFlushError`, `S3UploadError`, `IncrementalSnapshotError`, `PluginError`, `MongoDBChangeStreamError`) for `errors.Is`/`errors.As` matching.

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

1. Implement `adapter.Adapter` interface (`Start(ctx, <-chan event.Event) error`, `Name() string`)
2. Optionally implement `adapter.Acknowledger` (`SetAckFunc(fn AckFunc)`) for cooperative checkpointing support — ack after fully handling each event (success, DLQ, or intentional skip)
3. Create `adapter/<name>/` package
4. Add switch case in `cmd/listen.go` adapter loop
5. Add config struct in `internal/config/config.go`
6. Add CLI flags in `cmd/listen.go` `init()`
7. Add metrics instrumentation (`metrics.EventsDelivered.WithLabelValues("<name>").Inc()`)
8. Add scenario test, register in SCENARIOS.md

### New detector

1. Implement `detector.Detector` interface (`Start(ctx, chan<- event.Event) error`, `Name() string`)
2. Create `detector/<name>/` package
3. **MUST NOT** close the events channel — the bus owns its lifecycle
4. **Use `pgx.Connect`** not pool — LISTEN requires a dedicated connection
5. Add selection logic in `cmd/listen.go`
6. Register with health checker (`checker.Register("<name>")`, `checker.SetStatus(...)`)
7. Add scenario test, register in SCENARIOS.md

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

## Dependencies

Direct deps (keep minimal): `pgx/v5` (PG driver), `pglogrepl` (WAL logical replication protocol), `cobra` + `viper` (CLI/config), `chi/v5` (HTTP router), `google/uuid` (UUIDv7), `errgroup` (concurrency), `prometheus/client_golang` (metrics), `coder/websocket` (WebSocket adapter), `nats-io/nats.go` (NATS JetStream adapter), `twmb/franz-go` (Kafka adapter), `redis/go-redis/v9` (Redis adapter), `google.golang.org/grpc` + `google.golang.org/protobuf` (gRPC adapter), `aws/aws-sdk-go-v2` (S3 adapter), `extism/go-sdk` (Wasm plugin runtime), `go-mysql-org/go-mysql` (MySQL binlog replication), `go-sql-driver/mysql` (MySQL driver for schema queries), `go.mongodb.org/mongo-driver/v2` (MongoDB Change Streams detector), `testcontainers-go` (test only).

## Testing

### Philosophy

Tests are the steering wheel for AI-assisted development. The agent writes code to make tests pass, then runs all tests to catch regressions. The test surface must be lean, non-overlapping, and focused on system boundaries.

### Two surfaces

- **Unit tests** (`*_test.go` in package dirs): Pure algorithmic logic only. No I/O, no network, no goroutines. Fast.
- **Scenario tests** (`scenarios/*_test.go`): Full pipeline tests with real Postgres (testcontainers). One file per user journey. The primary regression barrier.

### Makefile targets

- `make test` — unit tests only, no Docker needed (~2s)
- `make test-scenarios` — scenario tests only, Docker required (~30s)
- `make test-all` — both unit + scenarios
- `make coverage` — generate coverage report
- `make lint` — run golangci-lint
- `make docker-build` — build Docker image

### Agent workflow

After every implementation change:
1. Run `make test-all`
2. All tests must pass before considering the task done
3. If you added new behavior, check SCENARIOS.md — extend an existing scenario or add a new one
4. If you added a new scenario, register it in SCENARIOS.md

### When to write a unit test

Only for pure functions with no side effects: backoff calculations, payload parsing, HMAC computation, data transformations, error type contracts. If the function touches I/O, channels, or network — it belongs in a scenario test.

### When to write a scenario test

When you add a new user journey or a new way the system can fail at a boundary. Check SCENARIOS.md first. If an existing scenario covers the behavior, add a subtest to it. If it's a genuinely new journey, create a new file and register it.

Before adding any test, run `go test -cover ./scenarios/` and `go test -cover ./...` to establish a baseline. After adding the test, verify coverage meaningfully increased. If it didn't, the test is redundant — don't add it.

### When to delete a test

- If your change makes a scenario's failure subtest redundant (e.g. you removed the feature it tests), delete that subtest
- If a unit test now overlaps with a scenario (tests the same code path), delete the unit test
- Run coverage after deletion to verify no loss

### Do NOT test

- Third-party library behavior (pgx, chi, cobra, viper, prometheus)
- Go standard library behavior
- Simple getters, setters, or struct constructors
- Config struct defaults
- Any code path already proven by a scenario
- Do NOT write a unit test AND a scenario for the same behavior

### Scenario structure

Each scenario file follows this pattern:
- `//go:build integration` build tag
- `TestScenario_<Name>` as the top-level test
- `t.Run("happy path", ...)` for the golden path
- `t.Run("<specific failure>", ...)` for one critical failure mode
- Uses shared helpers from `scenarios/helpers_test.go`

### Max scenario count

Target: ~20-30 scenarios for a project this size. If approaching 30, consolidate related scenarios before adding new ones.

## Code Organization

```
pgcdc.go       Pipeline type (library entry point)
cmd/            CLI commands (cobra)
  pgcdc/       Binary entry point (main.go)
adapter/        Output adapter interface + implementations
  stdout/       JSON-lines to io.Writer
  webhook/      HTTP POST with retries
  sse/          Server-Sent Events broker
  file/         JSON-lines to file with rotation
  exec/         JSON-lines to subprocess stdin
  pgtable/      INSERT into PostgreSQL table
  ws/           WebSocket broker
  embedding/    Embed text columns → UPSERT into pgvector table
  nats/         NATS JetStream publish
  kafka/        Kafka topic publish
  search/       Typesense / Meilisearch sync
  redis/        Redis cache invalidation / sync
  grpc/         gRPC streaming server
  s3/           S3-compatible object storage (JSON Lines/Parquet)
ack/            Cooperative checkpoint LSN tracker
backpressure/   Source-aware WAL lag backpressure (throttle/pause/shed)
bus/            Event fan-out (fast or reliable mode)
transform/      Event transform pipeline (drop, rename, mask, filter)
snapshot/       Table snapshot (COPY-based row export + incremental chunk-based)
detector/       Change detection interface + implementations
  listennotify/ PostgreSQL LISTEN/NOTIFY
  walreplication/ PostgreSQL WAL logical replication
  outbox/       Transactional outbox table polling
  mysql/        MySQL binlog replication
  mongodb/      MongoDB Change Streams
checkpoint/     LSN checkpoint storage
event/          Event model
health/         Component health checker
metrics/        Prometheus metrics definitions
dlq/            Dead letter queue (stderr + PG table backends)
plugin/         Wasm plugin system
  wasm/          Extism runtime, transforms, adapters, DLQ, checkpoint
  proto/         Protobuf event definition (opt-in encoding for plugins)
pgcdcerr/      Typed error types
internal/       CLI-specific internals (not importable)
  config/       Viper-based configuration structs
  server/       HTTP server for SSE + WS + metrics + health
scenarios/      Integration/scenario tests (testcontainers)
testutil/       Test utilities
```

## Key Files

- `pgcdc.go` — Pipeline type, options, Run/RunSnapshot methods (library entry point)
- `cmd/listen.go` — CLI wiring (uses Pipeline + CLI-specific HTTP servers, SIGHUP handler)
- `cmd/reload.go` — SIGHUP reload helpers (CLI/YAML transform extraction, route merging, specToTransform)
- `cmd/snapshot.go` — Snapshot CLI subcommand
- `snapshot/snapshot.go` — Table snapshot using REPEATABLE READ + SELECT *
- `adapter/adapter.go` — Adapter interface
- `detector/detector.go` — Detector interface
- `bus/bus.go` — Fan-out with configurable fast (drop) or reliable (block) mode
- `ack/tracker.go` — Cooperative checkpoint LSN tracker (min across all adapters)
- `backpressure/backpressure.go` — Source-aware backpressure controller: zone transitions (green/yellow/red), proportional throttle, pause/resume, adapter shedding by priority
- `transform/transform.go` — TransformFunc interface, Chain, ErrDropEvent; `drop_columns.go`, `rename_fields.go`, `mask.go`, `filter.go`, `debezium.go` for built-in transforms
- `internal/config/config.go` — All config structs + defaults (CLI-only)
- `event/event.go` — Event model (UUIDv7, JSON payload, LSN for WAL events)
- `health/health.go` — Component health checker
- `metrics/metrics.go` — Prometheus metric definitions
- `pgcdcerr/errors.go` — Typed errors (ErrBusClosed, WebhookDeliveryError, DetectorDisconnectedError, ExecProcessError, EmbeddingDeliveryError, NatsPublishError, OutboxProcessError, IcebergFlushError, IncrementalSnapshotError, MongoDBChangeStreamError)
- `adapter/embedding/embedding.go` — Embedding adapter: OpenAI-compatible API + pgvector UPSERT/DELETE
- `adapter/nats/nats.go` — NATS JetStream adapter: publish with dedup + auto-stream creation
- `adapter/kafka/kafka.go` — Kafka adapter: publish with RequireAll acks, SASL/TLS, DLQ for terminal errors
- `adapter/search/search.go` — Search adapter: Typesense/Meilisearch sync with batching
- `adapter/redis/redis.go` — Redis adapter: cache invalidation (DEL) or sync (SET/DEL)
- `adapter/grpc/grpc.go` — gRPC streaming adapter: broker pattern like SSE/WS
- `adapter/s3/s3.go` — S3 adapter: buffered flush to S3-compatible stores, Hive-partitioned keys
- `adapter/s3/writer.go` — S3 format writers: JSON Lines + Parquet (Snappy)
- `dlq/dlq.go` — DLQ interface + StderrDLQ + PGTableDLQ + NopDLQ
- `detector/outbox/outbox.go` — Outbox detector: poll-based with FOR UPDATE SKIP LOCKED
- `detector/mysql/mysql.go` — MySQL binlog detector: BinlogSyncer-based CDC with reconnect loop
- `detector/mysql/position.go` — MySQL binlog position ↔ uint64 encoding for checkpoint compatibility
- `detector/mongodb/mongodb.go` — MongoDB Change Streams detector: watch → emit loop with reconnect
- `detector/mongodb/resume.go` — Resume token load/save to MongoDB metadata collection
- `detector/walreplication/oidmap.go` — Static OID → type name mapping for 40+ PG types
- `checkpoint/checkpoint.go` — LSN checkpoint store interface + PG implementation
- `cmd/slot.go` — Replication slot management CLI (list, status, drop)
- `plugin/wasm/runtime.go` — Extism module compilation, instance pooling
- `plugin/wasm/transform.go` — WasmTransform → transform.TransformFunc (preserves LSN across boundary)
- `plugin/wasm/adapter.go` — WasmAdapter → adapter.Adapter + Acknowledger + DLQAware
- `plugin/wasm/dlq.go` — WasmDLQ → dlq.DLQ
- `plugin/wasm/checkpoint.go` — WasmCheckpointStore → checkpoint.Store
- `plugin/wasm/host.go` — Host functions: pgcdc_log, pgcdc_metric_inc, pgcdc_http_request
- `plugin/proto/event.proto` — Protobuf event definition for high-throughput plugins
- `internal/server/server.go` — HTTP server with SSE, WS, metrics, and health endpoints (CLI-only)
- `scenarios/helpers_test.go` — Shared test infrastructure (PG container, pipeline wiring)
