# pgcdc

PostgreSQL, MySQL, and MongoDB change data capture (LISTEN/NOTIFY, WAL logical replication, outbox pattern, MySQL binlog, or MongoDB Change Streams) streaming to webhooks, SSE, stdout, files, exec processes, PG tables, WebSockets, pgvector embeddings, NATS JetStream, Kafka, Typesense/Meilisearch, Redis, gRPC, S3-compatible object storage, a built-in Kafka protocol server (speak Kafka wire protocol directly — no Kafka cluster needed), and streaming SQL views (tumbling, sliding, and session window aggregation over CDC events).

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
                                        ──> Adapter (view)       ──> re-inject to bus
                                subscriber chans (one per adapter, filtered by route)
                                      |
                              Health Checker (per-component status)
                              Prometheus Metrics (/metrics)
```

- **Concurrency**: `errgroup` manages all goroutines via `safeGo` (panic-recovering wrapper). One context cancellation tears everything down. Panics in any goroutine are recovered, logged with stack trace, and counted via `pgcdc_panics_recovered_total{component}`.
- **Backpressure**: `--bus-mode fast` (default) drops events on full subscriber channels. `--bus-mode reliable` blocks the detector instead of dropping — loss-free at the cost of throughput.
- **All-tables zero-config**: `--all-tables` auto-creates a `FOR ALL TABLES` publication and switches to WAL detector. Zero setup — no `pgcdc init`, no manual SQL, no `--publication` needed. Idempotent: reuses existing publication on restart. Default publication name: `all` (override with `--publication`).
- **Transaction metadata**: WAL detector optionally enriches events with `transaction.xid`, `transaction.commit_time`, `transaction.seq` when `--tx-metadata` is enabled. `--tx-markers` adds synthetic BEGIN/COMMIT events on channel `pgcdc:_txn` (implies `--tx-metadata`). LISTEN/NOTIFY events omit this field (protocol has no tx info).
- **Snapshot-first**: `--snapshot-first --snapshot-table <table>` on listen (WAL only) runs a table snapshot using the replication slot's exported snapshot before transitioning to live streaming. Zero-gap delivery — snapshot sees exactly the data at the slot's consistent point, WAL streams everything after.
- **Persistent slots + checkpointing**: `--persistent-slot` creates a named, non-temporary replication slot with LSN checkpointing to `pgcdc_checkpoints` table. Survives crash/restart. Configurable via `--slot-name`, `--checkpoint-db`.
- **Type information**: `--include-schema` adds column type metadata (`columns` array with `name`, `type_oid`, `type_name`) to WAL events. OID-to-name mapping for 40+ common PG types.
- **Schema evolution**: `--schema-events` emits `SCHEMA_CHANGE` events on `pgcdc:_schema` channel when RelationMessage columns change (added, removed, type changed).
- **TOAST column cache**: `--toast-cache` enables in-memory LRU cache to resolve unchanged TOAST columns without `REPLICA IDENTITY FULL`. Keyed by `(RelationID, PK)`. INSERT populates cache, UPDATE backfills from cache, DELETE evicts, TRUNCATE/schema change evicts relation. `--toast-cache-max-entries` (default 100K). Cache miss: column set to `null` + `_unchanged_toast_columns` array in payload. Search adapter strips unchanged columns and uses partial update (PATCH). Redis sync mode merges with GET before SET. Metrics: `pgcdc_toast_cache_hits_total`, `pgcdc_toast_cache_misses_total`, `pgcdc_toast_cache_evictions_total`, `pgcdc_toast_cache_entries`.
- **Heartbeat**: `--heartbeat-interval 30s` periodically writes to `pgcdc_heartbeat` table to keep replication slots advancing on idle databases. Prevents WAL bloat.
- **Slot lag monitoring**: `pgcdc_slot_lag_bytes` gauge metric + log warnings when lag exceeds `--slot-lag-warn` threshold (default 100MB).
- **Source-aware backpressure**: `--backpressure` monitors WAL lag and automatically throttles/pauses/sheds to prevent PG disk exhaustion (requires `--detector wal` + `--persistent-slot`). Three zones: green (full speed), yellow (throttle detector + shed best-effort adapters), red (pause detector + shed normal+best-effort). Hysteresis: red exits only when lag drops below warn. Throttle proportional to lag position in yellow band. `--bp-warn-threshold` (default 500MB), `--bp-critical-threshold` (default 2GB), `--bp-max-throttle` (default 500ms), `--bp-poll-interval` (default 10s). `--adapter-priority name=critical|normal|best-effort`. Shed = auto-ack events without delivering (cooperative checkpoint advances normally). Metrics: `pgcdc_backpressure_state`, `pgcdc_backpressure_throttle_duration_seconds`, `pgcdc_backpressure_load_shed_total{adapter}`.
- **NATS JetStream adapter**: `--adapter nats` publishes events to NATS JetStream with subject mapping (`pgcdc:orders` → `pgcdc.orders`), dedup via `Nats-Msg-Id` header, auto-stream creation.
- **Search adapter**: `--adapter search` syncs to Typesense or Meilisearch. Batched upserts, individual deletes. `--search-engine`, `--search-url`, `--search-api-key`, `--search-index`.
- **Redis adapter**: `--adapter redis` for cache invalidation (`DEL` on any change) or sync (`SET`/`DEL`). `--redis-url`, `--redis-mode invalidate|sync`, `--redis-key-prefix`.
- **gRPC adapter**: `--adapter grpc` starts a gRPC streaming server. Clients call `Subscribe(SubscribeRequest)` with optional channel filter. Proto at `adapter/grpc/proto/pgcdc.proto`.
- **S3 adapter**: `--adapter s3` buffers events and periodically flushes partitioned objects (Hive-style `channel=.../year=.../month=.../day=.../`) to any S3-compatible store (AWS S3, MinIO, R2, etc.). JSON Lines (default) or Parquet format. Time+size flush triggers, atomic buffer swap, all-or-nothing upload per flush. `--s3-bucket`, `--s3-prefix`, `--s3-endpoint`, `--s3-region`, `--s3-access-key-id`, `--s3-secret-access-key`, `--s3-format`, `--s3-flush-interval`, `--s3-flush-size`, `--s3-drain-timeout`.
- **Kafka protocol server**: `--adapter kafkaserver` starts a TCP server on `:9092` (configurable) that speaks the Kafka wire protocol. Any Kafka consumer library (librdkafka, franz-go, sarama, Java client, confluent-kafka-python) connects directly to pgcdc — no Kafka cluster needed. pgcdc channels become Kafka topics (`pgcdc:orders` → `pgcdc.orders`), events hash across N partitions (FNV-1a on key column or event ID), consumer groups with partition assignment and heartbeat session reaping. Supports all 11 API keys: ApiVersions, Metadata, FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup, ListOffsets, Fetch (RecordBatch v2 with headers), OffsetCommit, OffsetFetch. Flexible encoding for ApiVersions v3+ (compact arrays, tagged fields). Offset persistence via `checkpoint.Store` with keys `kafka:{group}:{topic}:{partition}`. Long-poll Fetch with per-partition waiters. `--kafkaserver-addr` (default `:9092`), `--kafkaserver-partitions` (default 8), `--kafkaserver-buffer-size` (default 10000 records/partition), `--kafkaserver-session-timeout` (default 30s), `--kafkaserver-key-column` (default `id`), `--kafkaserver-checkpoint-db`.
- **View adapter**: `--adapter view` (or auto-created from `views:` YAML config or `--view-query 'name:query'` CLI flag) runs streaming SQL analytics over CDC events. SQL-like queries against virtual `pgcdc_events` table with three window types: `TUMBLING WINDOW <duration>`, `SLIDING WINDOW <duration> SLIDE <duration>`, `SESSION WINDOW <gap>`. Optional `ALLOWED LATENESS <duration>` for late event handling in tumbling windows. Supports `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `COUNT(DISTINCT ...)`, `STDDEV`/`STDDEV_POP` with `GROUP BY` and `HAVING`. Results emitted as `VIEW_RESULT` events on `pgcdc:_view:<name>` channels, re-injected into the bus. Emit modes: `row` (one event per group) or `batch` (single event with rows array). `max_groups` caps cardinality. Loop prevention: events from `pgcdc:_view:` channels are skipped. SQL parsing via TiDB parser. `--view-query 'name:SELECT ...'` for inline view definitions (repeatable, merged with YAML `views:` — CLI wins on name conflict).
- **Dead letter queue**: `--dlq stderr|pg_table|none`. Failed events captured to stderr (JSON lines) or `pgcdc_dead_letters` table. Adapters with DLQ support: webhook, embedding, kafka, search, redis, s3. (kafkaserver has no DLQ — in-memory ring buffer; consumers that fall behind get OFFSET_OUT_OF_RANGE.)
- **Kafka adapter**: `--adapter kafka` publishes events to Kafka topics with per-event key (`event.ID`), headers (`pgcdc-channel`, `pgcdc-operation`, `pgcdc-event-id`), and `RequireAll` acks. Channel-to-topic mapping: `pgcdc:orders` → `pgcdc.orders`. `--kafka-topic` overrides with a fixed topic. SASL (plain, SCRAM-SHA-256/512) and TLS supported. Terminal Kafka errors (non-retriable) go to DLQ; connection errors trigger reconnect with backoff. `--kafka-transactional-id` enables exactly-once delivery via Kafka transactions (each event produced in its own transaction).
- **Event routing**: `--route adapter=channel1,channel2`. Bus-level filtering before fan-out. Adapters without routes receive all events.
- **Outbox detector**: `--detector outbox` polls a transactional outbox table using `SELECT ... FOR UPDATE SKIP LOCKED` for concurrency-safe processing. Configurable DELETE or `processed_at` update cleanup.
- **MySQL binlog detector**: `--detector mysql` connects to MySQL as a replication slave using `go-mysql-org/go-mysql` `BinlogSyncer`. Captures INSERT/UPDATE/DELETE from MySQL binlog (ROW format required). `--mysql-addr`, `--mysql-user`, `--mysql-password`, `--mysql-server-id` (required, > 0), `--mysql-tables` (schema.table filter), `--mysql-gtid` (GTID mode), `--mysql-flavor` (mysql/mariadb). Events emitted on `pgcdc:<schema>.<table>` channels. Column names resolved from TableMapEvent metadata (MySQL 8.0.1+), `information_schema` fallback, or `col_N` fallback. Position encoded as `(file_seq << 32) | offset` for checkpoint compatibility.
- **MongoDB Change Streams detector**: `--detector mongodb` uses MongoDB's native Change Streams API (requires replica set or sharded cluster). Captures insert/update/replace/delete operations. `--mongodb-uri` (required), `--mongodb-database` (required unless cluster scope), `--mongodb-collections` (collection filter), `--mongodb-scope collection|database|cluster`, `--mongodb-full-document updateLookup|default`. Events emitted on `pgcdc:<database>.<collection>` channels. System events (drop/rename/invalidate) on `pgcdc:_mongo`. Resume tokens persisted to MongoDB metadata collection (`pgcdc_resume_tokens`) for crash-resumable streaming. `--mongodb-metadata-db`, `--mongodb-metadata-coll`. No cooperative checkpoint or backpressure support (no WAL LSN equivalent). Metrics: `pgcdc_mongodb_events_received_total`, `pgcdc_mongodb_errors_total`, `pgcdc_mongodb_resume_token_saves_total`.
- **Adapter validation**: Adapters optionally implement `adapter.Validator` (`Validate(ctx) error`) for pre-flight checks (DNS resolution, bucket existence, DB connectivity, API reachability). `Pipeline.Validate()` runs all validators before starting; failures abort startup unless `--skip-validation`. Metrics: `pgcdc_validation_duration_seconds{adapter}`.
- **Shutdown + drain**: Signal cancels root context. Bus closes subscriber channels. Adapters optionally implement `adapter.Drainer` (`Drain(ctx) error`) to flush in-flight work (e.g., webhook WaitGroup, S3 buffer flush) bounded by `shutdown_timeout` (default 5s).
- **Schema migrations**: `internal/migrate` runs embedded SQL migrations (`internal/migrate/sql/`) at startup, tracking versions in `pgcdc_migrations` table. Idempotent, skip with `--skip-migrations`. Initial migration creates `pgcdc_checkpoints`, `pgcdc_dead_letters`, `pgcdc_heartbeat` tables.
- **Circuit breaker**: `internal/circuitbreaker` provides three-state (closed/open/half-open) circuit breaker with configurable max failures and reset timeout. Per-adapter config: `--webhook-cb-failures`, `--webhook-cb-reset`, `--embedding-cb-failures`, `--embedding-cb-reset`. Wired into the `adapter/middleware` chain for adapters implementing `adapter.Deliverer`.
- **Rate limiter**: `internal/ratelimit` provides token-bucket rate limiter wrapping `golang.org/x/time/rate` with Prometheus metrics. Per-adapter config: `--webhook-rate-limit`, `--webhook-rate-burst`, `--embedding-rate-limit`, `--embedding-rate-burst`. Wired into the `adapter/middleware` chain for adapters implementing `adapter.Deliverer`.
- **SIGHUP config reload**: `kill -HUP <pid>` re-reads the YAML config file and atomically swaps `transforms:` and `routes:` sections for all running adapters with zero event loss. CLI flags, plugin transforms, adapters, detectors, and bus mode remain immutable. Implementation: `sync/atomic.Pointer[wrapperConfig]` per adapter; the wrapper goroutine loads from this pointer on every event. `Pipeline.Reload(ReloadConfig)` rebuilds and stores atomically. Metrics: `pgcdc_config_reloads_total`, `pgcdc_config_reload_errors_total`. YAML `routes:` section maps adapter names to channel lists (CLI `--route` wins for same adapter name).
- **Wiring**: `pgcdc.go` provides the reusable `Pipeline` type (detector + bus + adapters). `cmd/listen.go` adds CLI-specific HTTP servers on top.
- **Observability**: Prometheus metrics exposed at `/metrics`. Rich health check at `/healthz` returns per-component status (200 when all up, 503 when any down). Standalone metrics server via `--metrics-addr`.
- **Embedding adapter**: `--adapter embedding` with `--embedding-api-url`, `--embedding-columns`, `--embedding-api-key`. Calls any OpenAI-compatible endpoint, UPSERTs vector into pgvector table. INSERT/UPDATE → embed+upsert, DELETE → delete vector. Zero new deps — vectors stored as strings with `::vector` cast.
- **Incremental snapshots**: `--incremental-snapshot` enables chunk-based `SELECT ... WHERE pk > ? LIMIT N` snapshots running alongside live WAL streaming. Signal-table triggered (`pgcdc_signals`), progress persisted to `pgcdc_snapshot_progress`, crash-resumable. Emits `SNAPSHOT_STARTED`, `SNAPSHOT` (row), and `SNAPSHOT_COMPLETED` events on `pgcdc:_snapshot`. `--snapshot-chunk-size`, `--snapshot-chunk-delay`, `--snapshot-progress-db`.
- **Transform pipeline**: `--drop-columns col1,col2` and `--filter-operations INSERT,UPDATE,TRUNCATE` as CLI shortcuts. Full config via `transforms.global` and `transforms.adapter.<name>` in YAML. Built-in types: `drop_columns`, `rename_fields`, `mask` (zero/hash/redact modes), `filter` (by field value or operation), `debezium` (rewrites payload into Debezium envelope with before/after/op/source/transaction blocks — `--debezium-envelope`, `--debezium-connector-name`, `--debezium-database`), `cloudevents` (rewrites payload into CloudEvents v1.0 structured-mode JSON with pgcdc extension attributes — configurable `source` and `type_prefix` via YAML), `cel_filter` (expression-based event filtering using CEL — `--filter-cel 'operation == "INSERT"'`). Applied per-adapter or globally; dropped events increment `pgcdc_transform_dropped_total`, errors increment `pgcdc_transform_errors_total`.
- **Inspector**: `--inspect-buffer N` (default 100) enables ring-buffer event sampling at three tap points (post-detector, post-transform, pre-adapter). HTTP endpoints `/inspect` (JSON snapshot with `?point=&limit=` query params) and `/inspect/stream` (SSE live stream). Wired into Pipeline via `WithInspector()`.
- **Chain adapter**: `adapter/chain` provides link-based event preprocessing before terminal adapter delivery. Each `chain.Link` implements `Process(ctx, event) (event, error)`. Built-in links: compress (gzip), encrypt (AES-GCM). Chain wraps any `adapter.Adapter` and runs links in sequence before delegating to the inner adapter.
- **Multi-detector**: `detector/multi` composes multiple detectors with three modes: `sequential` (run one after another), `parallel` (run all concurrently, merge events), `failover` (try each in order, switch on error). Parallel mode supports optional `DedupWindow` to filter duplicate events across detectors. `--detector-mode sequential|parallel|failover`.
- **Schema store**: `schema/` provides versioned schema tracking. `Store` interface with `Register`, `Get`, `Latest`, `List` methods. Memory and PostgreSQL implementations. Wired into Pipeline via `WithSchemaStore()`; detectors implementing `SchemaStoreAware` receive the store. `--schema-store`, `--schema-store-type memory|pg`, `--schema-db`.
- **Nack window**: `dlq/window.go` provides a sliding window tracking delivery ack/nack outcomes per adapter. When nack count exceeds threshold, events are skipped (adaptive DLQ). Wired into Pipeline's `wrapSubscription`. `--dlq-window-size` (default 100), `--dlq-window-threshold` (default 50). Metrics: `pgcdc_nack_window_exceeded_total{adapter}`.
- **Multi-pipeline server**: `pgcdc serve --config pipelines.yaml` runs N independent pipelines from a YAML config file. REST API for runtime management: `GET /api/v1/pipelines`, `GET /api/v1/pipelines/:id`, `POST /api/v1/pipelines` (create), `DELETE /api/v1/pipelines/:id` (remove stopped), `POST .../start`, `POST .../stop`, `POST .../pause`, `POST .../resume`. Shared HTTP server with metrics, health checks, and API. Pipeline builder uses the component registry.
- **Connector introspection**: `pgcdc describe <connector>` shows typed parameter specs for any registered adapter or detector. `registry.ParamSpec` on all adapter and detector entries. `registry.GetConnectorSpec()` looks up by name.
- **Cooperative checkpointing**: `--cooperative-checkpoint` (requires `--persistent-slot` + `--detector wal`). Adapters call `AckFunc` after delivery; checkpoint only advances to `min(all adapter ack positions)`. Non-`Acknowledger` adapters are auto-acked on channel send. Metrics: `pgcdc_ack_position{adapter}`, `pgcdc_cooperative_checkpoint_lsn`.
- **Wasm plugin system**: Extism-based (pure Go, no CGo) plugin system for 4 extension points: transforms, adapters, DLQ backends, checkpoint stores. Plugins compiled to `.wasm` from any Extism PDK language (Rust, Go, Python, TypeScript). Zero overhead when no plugins configured. JSON serialization (protobuf opt-in). Host functions: `pgcdc_log`, `pgcdc_metric_inc`, `pgcdc_http_request`. Config via `plugins:` YAML block or `--plugin-transform`, `--plugin-adapter`, `--dlq plugin`, `--checkpoint-plugin` CLI flags. Metrics: `pgcdc_plugin_calls_total`, `pgcdc_plugin_duration_seconds`, `pgcdc_plugin_errors_total`.
- **Encoding + Schema Registry**: `--kafka-encoding avro|protobuf|json` and `--nats-encoding avro|protobuf|json` with optional Confluent Schema Registry (`--schema-registry-url`, `--schema-registry-username`, `--schema-registry-password`). `encoding/` package: Avro (hamba/avro), Protobuf, JSON encoders. `encoding/registry/` package: Schema Registry HTTP client with wire format (magic byte + schema ID prefix).
- **OpenTelemetry tracing**: `--otel-exporter none|stdout|otlp`, `--otel-endpoint`, `--otel-sample-ratio`. OTLP gRPC exporter for distributed tracing across the pipeline. `tracing/` package: setup, shutdown, span creation. `tracing/carrier.go`: Kafka header carrier for trace context propagation.
- **DLQ management CLI**: `pgcdc dlq list|replay|purge` commands for inspecting, replaying, and purging dead letter queue records. Filter by adapter, time range, ID. Replay supports `--dry-run` and adapter-specific overrides (`--webhook-url`, `--kafka-brokers`). `cmd/dlq.go` + `cmd/dlq_replay_kafka.go`.
- **TRUNCATE support**: WAL detector emits `TRUNCATE` operation type alongside INSERT/UPDATE/DELETE.
- **Error types**: `pgcdcerr/` provides typed errors (`ErrBusClosed`, `WebhookDeliveryError`, `DetectorDisconnectedError`, `ExecProcessError`, `EmbeddingDeliveryError`, `NatsPublishError`, `OutboxProcessError`, `IcebergFlushError`, `S3UploadError`, `IncrementalSnapshotError`, `PluginError`, `MongoDBChangeStreamError`, `MySQLReplicationError`, `SchemaRegistryError`, `KafkaServerError`, `ViewError`, `ValidationError`, `CircuitBreakerOpenError`, `RateLimitExceededError`) for `errors.Is`/`errors.As` matching.

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
5. Create `adapter/<name>/` package
6. Create `cmd/register_<name>.go` and register via `registry.RegisterAdapter(registry.AdapterEntry{...})` in `init()`, including `BindFlags` and `ViperKeys` callbacks
7. Add config struct in `internal/config/adapter_config.go`
8. Add defaults in `Default()` in `internal/config/config.go`
9. Add metrics instrumentation (`metrics.EventsDelivered.WithLabelValues("<name>").Inc()`)
10. Add scenario test, register in SCENARIOS.md

### New detector

1. Implement `detector.Detector` interface (`Start(ctx, chan<- event.Event) error`, `Name() string`)
2. Create `detector/<name>/` package
3. **MUST NOT** close the events channel — the bus owns its lifecycle
4. **Use `pgx.Connect`** not pool — LISTEN requires a dedicated connection
5. Create `cmd/register_detector_<name>.go` and register via `registry.RegisterDetector(registry.DetectorEntry{...})` in `init()`
6. Add config struct in `internal/config/detector_config.go` and defaults in `Default()`
7. Register with health checker (`checker.Register("<name>")`, `checker.SetStatus(...)`)
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

## Dependencies

Direct deps (keep minimal): `pgx/v5` (PG driver), `pglogrepl` (WAL logical replication protocol), `cobra` + `viper` (CLI/config), `chi/v5` (HTTP router), `google/uuid` (UUIDv7), `errgroup` (concurrency), `prometheus/client_golang` (metrics), `coder/websocket` (WebSocket adapter), `nats-io/nats.go` (NATS JetStream adapter), `twmb/franz-go` (Kafka adapter), `redis/go-redis/v9` (Redis adapter), `google.golang.org/grpc` + `google.golang.org/protobuf` (gRPC adapter), `aws/aws-sdk-go-v2` (S3 adapter), `parquet-go/parquet-go` (Parquet writer for S3/Iceberg), `hamba/avro/v2` (Avro encoding), `go.opentelemetry.io/otel` (OpenTelemetry tracing), `extism/go-sdk` (Wasm plugin runtime), `go-mysql-org/go-mysql` (MySQL binlog replication), `go-sql-driver/mysql` (MySQL driver for schema queries), `go.mongodb.org/mongo-driver/v2` (MongoDB Change Streams detector), `pingcap/tidb/pkg/parser` (SQL parsing for view adapter), `testcontainers-go` (test only).

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

Target: keep scenarios focused and non-overlapping. Currently 41 scenarios — consolidate related ones before adding new ones.

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
  iceberg/      Apache Iceberg table writes (Hadoop catalog, Parquet)
  kafkaserver/  Kafka wire protocol server (no Kafka cluster needed)
  view/         Streaming SQL view engine adapter
  middleware/   Middleware chain for Deliverer adapters (metrics, tracing, CB, rate-limit, retry, DLQ, ack)
  chain/        Link-based event preprocessing chain (compress, encrypt links)
view/           Streaming SQL engine (parser, evaluator, tumbling/sliding/session windows, aggregators)
registry/       Self-registering component registry (adapters, detectors, transforms, connector specs)
ack/            Cooperative checkpoint LSN tracker
backpressure/   Source-aware WAL lag backpressure (throttle/pause/shed)
bus/            Event fan-out (fast or reliable mode)
cel/            CEL expression compiler/evaluator for event filtering
transform/      Event transform pipeline (drop, rename, mask, filter, debezium, cloudevents, cel_filter)
snapshot/       Table snapshot (COPY-based row export + incremental chunk-based)
inspect/        Ring-buffer event sampling at tap points (post-detector, post-transform, pre-adapter)
schema/         Versioned schema store (Store interface, MemoryStore, PGStore)
detector/       Change detection interface + implementations
  listennotify/ PostgreSQL LISTEN/NOTIFY
  walreplication/ PostgreSQL WAL logical replication
    toastcache/  In-memory LRU cache for TOAST column resolution
  outbox/       Transactional outbox table polling
  mysql/        MySQL binlog replication
  mongodb/      MongoDB Change Streams
  multi/        Multi-detector composition (sequential, parallel, failover modes)
checkpoint/     LSN checkpoint storage
event/          Event model
health/         Component health checker
metrics/        Prometheus metrics definitions
dlq/            Dead letter queue (stderr + PG table backends + nack window)
server/         Multi-pipeline manager + REST API for pgcdc serve command
encoding/       Event encoding (Avro, Protobuf, JSON) + Schema Registry client
  registry/     Confluent Schema Registry HTTP client + wire format
tracing/        OpenTelemetry tracing setup + Kafka carrier
plugin/         Wasm plugin system
  wasm/          Extism runtime, transforms, adapters, DLQ, checkpoint
  proto/         Protobuf event definition (opt-in encoding for plugins)
pgcdcerr/      Typed error types
internal/       CLI-specific internals (not importable)
  config/       Viper-based configuration structs
  server/       HTTP server for SSE + WS + metrics + health
  circuitbreaker/ Three-state circuit breaker (closed/open/half-open)
  ratelimit/    Token-bucket rate limiter with Prometheus metrics
  migrate/      Embedded SQL migration system with version tracking
  safegoroutine/ Panic recovery wrapper for errgroup
scenarios/      Integration/scenario tests (testcontainers)
testutil/       Test utilities
```

## Key Files

- `pgcdc.go` — Pipeline type, options, Run/RunSnapshot methods (library entry point)
- `cmd/listen.go` — CLI wiring (uses Pipeline + CLI-specific HTTP servers, SIGHUP handler)
- `cmd/reload.go` — SIGHUP reload helpers (CLI/YAML transform extraction, route merging, specToTransform)
- `cmd/snapshot.go` — Snapshot CLI subcommand
- `snapshot/snapshot.go` — Table snapshot using REPEATABLE READ + SELECT *
- `adapter/adapter.go` — Adapter interface + optional Validator, Drainer, Acknowledger, DLQAware, Reinjector, Traceable interfaces
- `detector/detector.go` — Detector interface
- `bus/bus.go` — Fan-out with configurable fast (drop) or reliable (block) mode
- `ack/tracker.go` — Cooperative checkpoint LSN tracker (min across all adapters)
- `backpressure/backpressure.go` — Source-aware backpressure controller: zone transitions (green/yellow/red), proportional throttle, pause/resume, adapter shedding by priority
- `transform/transform.go` — TransformFunc interface, Chain, ErrDropEvent; `drop_columns.go`, `rename_fields.go`, `mask.go`, `filter.go`, `debezium.go`, `cloudevents.go`, `cel_filter.go` for built-in transforms
- `internal/config/config.go` — Core Config struct, shared types (Bus, OTel, DLQ, Transform, Backpressure), Default() (CLI-only)
- `internal/config/adapter_config.go` — Adapter-specific config structs
- `internal/config/detector_config.go` — Detector-specific config structs
- `internal/config/plugin_config.go` — Plugin config structs
- `internal/config/validate.go` — Config validation logic
- `event/event.go` — Event model (UUIDv7, JSON payload, LSN for WAL events)
- `health/health.go` — Component health checker
- `metrics/metrics.go` — Prometheus metric definitions
- `pgcdcerr/errors.go` — Typed errors (ErrBusClosed, WebhookDeliveryError, DetectorDisconnectedError, ExecProcessError, EmbeddingDeliveryError, NatsPublishError, OutboxProcessError, IcebergFlushError, S3UploadError, IncrementalSnapshotError, PluginError, MongoDBChangeStreamError, MySQLReplicationError, SchemaRegistryError, KafkaServerError, ViewError, ValidationError, CircuitBreakerOpenError, RateLimitExceededError)
- `adapter/view/view.go` — View adapter: wraps view.Engine, implements adapter.Reinjector for bus re-injection
- `view/engine.go` — View engine: manages windows (tumbling/sliding/session), processes events, flushes results
- `view/parser.go` — SQL parser: TiDB-based parsing of streaming SQL with TUMBLING/SLIDING/SESSION WINDOW extensions
- `view/ast.go` — ViewDef AST: WindowType enum, AggFunc enum, SelectField, parsed view definition
- `view/window.go` — Tumbling window: time-based aggregation with group-by, HAVING, and late event handling (AllowedLateness)
- `view/sliding_window.go` — Sliding window: overlapping sub-windows with cross-window merge (Welford's for STDDEV)
- `view/session_window.go` — Session window: gap-based windows that close after inactivity timeout
- `view/aggregate.go` — Aggregator implementations: COUNT, SUM, AVG, MIN, MAX, COUNT_DISTINCT, STDDEV
- `adapter/embedding/embedding.go` — Embedding adapter: OpenAI-compatible API + pgvector UPSERT/DELETE
- `adapter/nats/nats.go` — NATS JetStream adapter: publish with dedup + auto-stream creation
- `adapter/kafka/kafka.go` — Kafka adapter: publish with RequireAll acks, SASL/TLS, DLQ for terminal errors
- `adapter/search/search.go` — Search adapter: Typesense/Meilisearch sync with batching
- `adapter/redis/redis.go` — Redis adapter: cache invalidation (DEL) or sync (SET/DEL)
- `adapter/grpc/grpc.go` — gRPC streaming adapter: broker pattern like SSE/WS
- `adapter/s3/s3.go` — S3 adapter: buffered flush to S3-compatible stores, Hive-partitioned keys
- `adapter/s3/writer.go` — S3 format writers: JSON Lines + Parquet (Snappy)
- `adapter/kafkaserver/server.go` — Kafka protocol server adapter: TCP accept loop, ingest goroutine
- `adapter/kafkaserver/protocol.go` — Wire framing: readRequest (flexible + non-flexible headers), writeResponse, RecordBatch v2 encoding
- `adapter/kafkaserver/handler.go` — API dispatch: 11 API key handlers, version-aware encoding, Fetch long-poll
- `adapter/kafkaserver/broker.go` — Topic registry, channel→topic mapping, event ingestion
- `adapter/kafkaserver/partition.go` — Ring buffer per partition with waiter notification
- `adapter/kafkaserver/group.go` — Consumer group state machine (Empty/PreparingRebalance/CompletingRebalance/Stable)
- `adapter/kafkaserver/offset_store.go` — Offset persistence via checkpoint.Store (`kafka:{group}:{topic}:{partition}`)
- `adapter/kafkaserver/hash.go` — Key extraction from JSON payload + FNV-1a partition hash
- `dlq/dlq.go` — DLQ interface + StderrDLQ + PGTableDLQ + NopDLQ
- `cmd/dlq.go` — DLQ management CLI: list, replay, purge commands
- `encoding/encoder.go` — Encoder interface + Avro/Protobuf/JSON implementations
- `encoding/registry/client.go` — Confluent Schema Registry HTTP client
- `tracing/tracing.go` — OpenTelemetry tracing setup (OTLP gRPC, stdout, noop exporters)
- `tracing/carrier.go` — Kafka header carrier for OTel trace context propagation
- `detector/outbox/outbox.go` — Outbox detector: poll-based with FOR UPDATE SKIP LOCKED
- `detector/mysql/mysql.go` — MySQL binlog detector: BinlogSyncer-based CDC with reconnect loop
- `detector/mysql/position.go` — MySQL binlog position ↔ uint64 encoding for checkpoint compatibility
- `detector/mongodb/mongodb.go` — MongoDB Change Streams detector: watch → emit loop with reconnect
- `detector/mongodb/resume.go` — Resume token load/save to MongoDB metadata collection
- `detector/walreplication/oidmap.go` — Static OID → type name mapping for 40+ PG types
- `detector/walreplication/toastcache/cache.go` — LRU cache for TOAST column resolution (keyed by RelationID+PK)
- `checkpoint/checkpoint.go` — LSN checkpoint store interface + PG implementation
- `cmd/slot.go` — Replication slot management CLI (list, status, drop)
- `plugin/wasm/runtime.go` — Extism module compilation, instance pooling
- `plugin/wasm/base.go` — Shared basePlugin struct (call, closePlugin) reused by all wasm plugin types
- `plugin/wasm/transform.go` — WasmTransform → transform.TransformFunc (preserves LSN across boundary)
- `plugin/wasm/adapter.go` — WasmAdapter → adapter.Adapter + Acknowledger + DLQAware
- `plugin/wasm/dlq.go` — WasmDLQ → dlq.DLQ
- `plugin/wasm/checkpoint.go` — WasmCheckpointStore → checkpoint.Store
- `plugin/wasm/host.go` — Host functions: pgcdc_log, pgcdc_metric_inc, pgcdc_http_request
- `plugin/proto/event.proto` — Protobuf event definition for high-throughput plugins
- `internal/circuitbreaker/circuitbreaker.go` — Three-state circuit breaker (closed/open/half-open) with configurable max failures and reset timeout
- `internal/ratelimit/ratelimit.go` — Token-bucket rate limiter wrapping `golang.org/x/time/rate` with Prometheus metrics
- `internal/migrate/migrate.go` — Embedded SQL migration system with version tracking (`pgcdc_migrations` table)
- `internal/safegoroutine/safe.go` — Panic recovery wrapper for errgroup goroutines
- `internal/server/server.go` — HTTP server with SSE, WS, metrics, and health endpoints (CLI-only)
- `registry/registry.go` — Component registry: AdapterEntry, DetectorEntry, TransformEntry with Create factories, DetectorContext/DetectorResult, BindFlags helpers
- `registry/spec.go` — ParamSpec, ConnectorSpec, GetConnectorSpec for connector introspection
- `adapter/middleware/middleware.go` — Middleware chain for Deliverer adapters: metrics, tracing, CB, rate-limit, retry, DLQ, ack
- `adapter/chain/chain.go` — Chain adapter: Link interface, wraps inner adapter with preprocessing links
- `adapter/chain/encrypt.go` — AES-GCM encrypt link for chain adapter
- `inspect/inspector.go` — Ring-buffer event sampling at tap points with HTTP handlers
- `schema/schema.go` — Schema store interface (Register, Get, Latest, List) + MemoryStore
- `schema/pg_store.go` — PostgreSQL-backed schema store implementation
- `cel/cel.go` — CEL expression compiler/evaluator (Compile, Eval with event variables)
- `transform/cel_filter.go` — CEL-based event filter transform (FilterCEL function)
- `detector/multi/multi.go` — Multi-detector composition (Sequential, Parallel, Failover modes + DedupWindow)
- `dlq/window.go` — Sliding nack window for adaptive DLQ (NewNackWindow, RecordAck, RecordNack, Exceeded)
- `server/manager.go` — Multi-pipeline manager: Add, Remove, Start, Stop, Pause, Resume, List, Get
- `server/api.go` — REST API handler for pipeline management (APIHandler)
- `cmd/serve.go` — `pgcdc serve` command: multi-pipeline server with registry-based pipeline builder
- `cmd/describe.go` — `pgcdc describe` command: connector parameter introspection
- `cmd/validate.go` — `pgcdc validate` command: pre-flight configuration validation without starting the pipeline
- `cmd/playground.go` — `pgcdc playground` command: Docker-based demo environment with auto-generated data
- `scenarios/helpers_test.go` — Shared test infrastructure (PG container, pipeline wiring)
