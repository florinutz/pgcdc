# Scenario Test Registry

Each scenario = one file, one user journey, happy path + one critical failure.

| # | Scenario | File | Proves | Failure Mode |
|---|----------|------|--------|--------------|
| 1 | Stdout delivery | `stdout_delivery_test.go` | pg_notify -> detector -> bus -> stdout adapter outputs correct JSON line | Malformed PG payload handled gracefully |
| 2 | Webhook delivery | `webhook_delivery_test.go` | pg_notify -> detector -> bus -> webhook POST with correct headers + HMAC | Webhook returns 500, pgcdc retries and succeeds; all retries exhausted → event skipped, adapter continues |
| 3 | SSE streaming | `sse_streaming_test.go` | pg_notify -> detector -> bus -> SSE broker -> HTTP client receives SSE-formatted event | Channel filter: client subscribing to "orders" doesn't see "users" events |
| 4 | Multi-adapter fan-out | `multi_adapter_test.go` | Same pg_notify delivered to both stdout and webhook simultaneously | Slow webhook doesn't block stdout (backpressure) |
| 5 | Detector reconnection | `reconnection_test.go` | LISTEN/NOTIFY detector recovers after PG connection is terminated mid-listen | Events resume flowing after reconnect |
| 6 | CLI validation | `cli_validation_test.go` | `pgcdc listen` errors on missing --db, missing --channel, unknown adapter, webhook without --url, tx flags without WAL | N/A (all failure paths) |
| 7 | Trigger SQL generation | `trigger_sql_test.go` | `pgcdc init --table orders` outputs valid SQL that can be executed against PG | Invalid table name rejected, invalid channel name rejected |
| 8 | Health endpoint | `health_endpoint_test.go` | GET /healthz returns 200 + `{"status":"ok"}` when SSE server is running | CORS preflight returns correct headers |
| 9 | WAL replication | `wal_replication_test.go` | WAL logical replication captures INSERT/UPDATE/DELETE/TRUNCATE without triggers; events match LISTEN/NOTIFY format; tx metadata and markers work | UPDATE includes old row, DELETE includes deleted row, TRUNCATE has null row/old, tx metadata shared across transaction, BEGIN/COMMIT markers wrap DML |
| 10 | File delivery | `file_delivery_test.go` | pg_notify -> detector -> bus -> file adapter writes correct JSON lines to disk | File rotation: tiny MaxSize triggers .1 rotated file |
| 11 | Exec delivery | `exec_delivery_test.go` | pg_notify -> detector -> bus -> exec adapter pipes JSON lines to subprocess stdin | Subprocess exit: process restarts and second event arrives |
| 12 | PG table delivery | `pgtable_delivery_test.go` | pg_notify -> detector -> bus -> pg_table adapter INSERTs row into events table | Reconnect: adapter recovers after pg_terminate_backend; constraint violation → event skipped, no reconnect |
| 13 | WS streaming | `ws_streaming_test.go` | pg_notify -> detector -> bus -> WS broker -> WebSocket client receives JSON message | Channel filter: /ws/orders only receives matching events |
| 14 | WAL reconnection | `reconnection_test.go` | WAL detector recovers after PG connection is terminated mid-replication | Events resume flowing after WAL reconnect |
| 15 | Snapshot | `snapshot_test.go` | Snapshot exports 100 existing rows as SNAPSHOT events through stdout adapter | Non-existent table returns clear error |
| 16 | Snapshot-first | `snapshot_first_test.go` | WAL detector exports existing rows as SNAPSHOT events then transitions to live WAL streaming with zero gap | `--snapshot-first` without WAL detector or table errors cleanly (covered in CLI validation) |
| 17 | Embedding delivery | `embedding_test.go` | NOTIFY → detector → bus → embedding adapter calls OpenAI-compatible API and UPSERTs vector into pgvector table; UPDATE re-embeds; DELETE removes vector | API returns 500 twice then succeeds; adapter retries and event eventually delivered |
| 18 | Iceberg append | `iceberg_test.go` | NOTIFY → detector → bus → iceberg adapter buffers events, flushes to Parquet data files with Avro manifests and Iceberg v2 metadata.json via hadoop catalog | Write failure (read-only dir): adapter retries on next flush and events eventually written |
| 19 | Persistent slot | `persistent_slot_test.go` | WAL detector with persistent slot creates non-temporary replication slot; checkpoint table stores LSN; events survive reconnect | Slot survives disconnect: detector reconnects to existing slot, events resume flowing |
| 20 | NATS JetStream | `nats_test.go` | NOTIFY → detector → bus → NATS adapter publishes to JetStream with correct subject mapping and dedup ID | N/A (single happy path with full round-trip verification) |
| 21 | Outbox pattern | `outbox_test.go` | Outbox detector polls table, emits events, deletes processed rows; events arrive at stdout with correct channel/operation/payload | Keep-processed mode: rows have processed_at set instead of being deleted |
| 22 | Search sync | `search_test.go` | NOTIFY → detector → bus → search adapter upserts document to Typesense; DELETE removes document | N/A (happy path with full round-trip) |
| 23 | Redis cache | `redis_test.go` | NOTIFY → detector → bus → redis adapter DELs key in invalidate mode; SET key in sync mode | N/A (happy path with both modes) |
| 24 | gRPC streaming | `grpc_test.go` | NOTIFY → detector → bus → gRPC adapter streams event to connected client; channel filtering works | N/A (happy path with channel filter) |
| 25 | Incremental snapshot | `incremental_snapshot_test.go` | Signal table INSERT triggers chunked snapshot alongside live WAL streaming; SNAPSHOT_STARTED + data events + SNAPSHOT_COMPLETED emitted | Crash recovery: cancel mid-snapshot, verify progress saved, restart resumes from last chunk |
| 26 | Transform pipeline | `transform_test.go` | NOTIFY → detector → bus → transform chain (drop columns, mask, rename, CloudEvents envelope) → stdout outputs transformed payload | Filter drops non-matching events: FilterField("op","UPDATE") drops INSERT, passes UPDATE |
| 27 | Cooperative checkpoint | `cooperative_checkpoint_test.go` | WAL+persistent slot+reliable bus mode: stdout adapter acks events, checkpoint advances to min acked LSN | Slow adapter delays checkpoint: checkpoint stalls at slow adapter's position, advances after release |
| 28 | Kafka delivery | `kafka_test.go` | NOTIFY → detector → bus → kafka adapter publishes to topic with correct key/headers; channel name maps to topic (`pgcdc:orders`→`pgcdc.orders`) | Terminal topic error: event recorded to DLQ, adapter continues |
| 29 | Wasm plugins | `plugin_test.go` | Wasm transform drops field from payload; Wasm transform returns empty = event dropped; Wasm adapter receives events via handle(); DLQ plugin records; checkpoint store load/save | Wasm adapter error → DLQ |
| 30 | All-tables zero-config | `all_tables_test.go` | FOR ALL TABLES publication captures INSERT events from multiple tables simultaneously; both `pgcdc:at_orders` and `pgcdc:at_customers` channels deliver events | N/A (single happy path) |
| 31 | Source-aware backpressure | `backpressure_test.go` | WAL+persistent slot+cooperative checkpoint: backpressure controller transitions zones, events arrive throttled but not lost | Load shedding: critical adapter receives all events, best-effort adapter is shed in yellow zone |
| 32 | Avro/Protobuf encoding | `encoding_test.go` | NOTIFY → detector → bus → Kafka adapter with Avro encoder produces binary Avro message; Schema Registry integration registers schema and prepends Confluent wire format header | Schema Registry unavailable: encoding error goes to DLQ, adapter continues |
| 33 | OTel tracing propagation | `tracing_test.go` | NOTIFY → detector → bus → webhook adapter with stdout tracer: W3C `traceparent` header present in webhook HTTP requests with valid format | N/A (single happy path) |
| 34 | DLQ commands | `dlq_test.go` | DLQ list shows failed records; replay delivers to new webhook and marks replayed_at; purge --replayed removes replayed records | Replay to still-failing adapter: record NOT marked as replayed |
| 35 | SIGHUP config reload | `reload_test.go` | Start with `drop_columns: [secret]`, send event, verify `secret` dropped; call `Reload` with `drop_columns: [name]`, send event, verify `secret` present and `name` dropped | Reload with ErrDropEvent transform drops all events; reload back to good config restores delivery |
| 36 | S3 object storage | `s3_test.go` | NOTIFY → detector → bus → S3 adapter buffers events, flushes partitioned objects (channel+date) to MinIO with correct `.jsonl` extension and parseable content | Parquet format: same flow produces `.parquet` objects with valid Parquet content |
| 37 | MySQL binlog detector | `mysql_test.go` | MySQL binlog replication captures INSERT/UPDATE/DELETE from MySQL 8.0; events have correct channel (`pgcdc:schema.table`), operation, payload with row/old, source=`mysql_binlog` | Reconnect after disconnect: KILL binlog dump connection, detector reconnects, new events arrive |
| 38 | MongoDB change streams | `mongodb_test.go` | MongoDB change stream captures INSERT/UPDATE/DELETE from MongoDB 7.0 replica set; events have correct channel (`pgcdc:db.coll`), operation, payload with row/document_key, source=`mongodb_changestream` | Resume after disconnect: stop detector, insert while down, restart, verify missed event arrives via resume token |

## Adding a new scenario

1. Create `scenarios/<name>_test.go`
2. Follow the structure: `TestScenario_<Name>` with `t.Run("happy path")` and `t.Run("<failure>")`
3. Use shared helpers from `helpers_test.go`
4. Register it in this table
5. Run `make test-scenarios` to verify
