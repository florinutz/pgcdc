# Scenario Test Registry

Each scenario = one file, one user journey, happy path + one critical failure.

| # | Scenario | File | Proves | Failure Mode |
|---|----------|------|--------|--------------|
| 1 | Stdout delivery | `stdout_delivery_test.go` | pg_notify -> detector -> bus -> stdout adapter outputs correct JSON line | Malformed PG payload handled gracefully |
| 2 | Webhook delivery | `webhook_delivery_test.go` | pg_notify -> detector -> bus -> webhook POST with correct headers + HMAC | Webhook returns 500, pgpipe retries and succeeds |
| 3 | SSE streaming | `sse_streaming_test.go` | pg_notify -> detector -> bus -> SSE broker -> HTTP client receives SSE-formatted event | Channel filter: client subscribing to "orders" doesn't see "users" events |
| 4 | Multi-adapter fan-out | `multi_adapter_test.go` | Same pg_notify delivered to both stdout and webhook simultaneously | Slow webhook doesn't block stdout (backpressure) |
| 5 | Detector reconnection | `reconnection_test.go` | Detector recovers after PG connection is terminated mid-listen | Events resume flowing after reconnect |
| 6 | CLI validation | `cli_validation_test.go` | `pgpipe listen` errors on missing --db, missing --channel, unknown adapter, webhook without --url | N/A (all failure paths) |
| 7 | Trigger SQL generation | `trigger_sql_test.go` | `pgpipe init --table orders` outputs valid SQL that can be executed against PG | Invalid table name rejected, invalid channel name rejected |
| 8 | Health endpoint | `health_endpoint_test.go` | GET /healthz returns 200 + `{"status":"ok"}` when SSE server is running | CORS preflight returns correct headers |
| 9 | WAL replication | `wal_replication_test.go` | WAL logical replication captures INSERT/UPDATE/DELETE without triggers, events match LISTEN/NOTIFY format | UPDATE includes old row, DELETE includes deleted row |

## Adding a new scenario

1. Create `scenarios/<name>_test.go`
2. Follow the structure: `TestScenario_<Name>` with `t.Run("happy path")` and `t.Run("<failure>")`
3. Use shared helpers from `helpers_test.go`
4. Register it in this table
5. Run `make test-scenarios` to verify
