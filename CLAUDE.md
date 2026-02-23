# pgpipe

PostgreSQL change data capture (LISTEN/NOTIFY or WAL logical replication) streaming to webhooks, SSE, stdout, files, exec processes, PG tables, and WebSockets.

## Quick Start

```sh
make build
./pgpipe listen --db postgres://... --channel orders

# Or via Docker
make docker-build
docker compose up -d
```

## Architecture

```
Signal (SIGINT/SIGTERM)
  |
  v
Context ──> Pipeline (pgpipe.go orchestrates everything)
              |
  Detector ──> Bus (fan-out) ──> Adapter (stdout)
  (listennotify     |          ──> Adapter (webhook)
   or walreplication)          ──> Adapter (file, with rotation)
                    |          ──> Adapter (exec, stdin JSON lines)
                    |          ──> Adapter (pg_table, INSERT)
                    |          ──> Adapter (SSE broker) ──> HTTP server
              ingest chan      ──> Adapter (WS broker)  ──> HTTP server
                                subscriber chans (one per adapter)
                                      |
                              Health Checker (per-component status)
                              Prometheus Metrics (/metrics)
```

- **Concurrency**: `errgroup` manages all goroutines. One context cancellation tears everything down.
- **Backpressure**: Bus uses non-blocking sends. If a subscriber channel is full, the event is dropped and a warning is logged. No adapter can block the pipeline.
- **Shutdown**: Signal cancels root context. Bus closes subscriber channels. HTTP server gets `shutdown_timeout` (default 5s) `context.WithTimeout` for graceful drain.
- **Wiring**: `pgpipe.go` provides the reusable `Pipeline` type (detector + bus + adapters). `cmd/listen.go` adds CLI-specific HTTP servers on top.
- **Observability**: Prometheus metrics exposed at `/metrics`. Rich health check at `/healthz` returns per-component status (200 when all up, 503 when any down). Standalone metrics server via `--metrics-addr`.
- **Error types**: `pgpipeerr/` provides typed errors (`ErrBusClosed`, `WebhookDeliveryError`, `DetectorDisconnectedError`, `ExecProcessError`) for `errors.Is`/`errors.As` matching.

## Code Conventions

- **Error wrapping**: `fmt.Errorf("verb: %w", err)` — verb describes the failed action (`connect:`, `listen:`, `subscribe:`). Use typed errors from `pgpipeerr/` for errors that callers need to branch on.
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
2. Create `adapter/<name>/` package
3. Add switch case in `cmd/listen.go` adapter loop
4. Add config struct in `internal/config/config.go`
5. Add CLI flags in `cmd/listen.go` `init()`
6. Add metrics instrumentation (`metrics.EventsDelivered.WithLabelValues("<name>").Inc()`)
7. Add scenario test, register in SCENARIOS.md

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
- Block in bus, SSE, or WS broadcast — non-blocking sends only
- Add external brokers (Redis, Kafka) — zero infra beyond PG is a design choice
- Use `log` or `fmt.Printf` — slog only
- Put raw SQL identifiers in Go strings — use `pgx.Identifier{}.Sanitize()`
- Add dependencies without justification — prefer stdlib
- Hardcode timeouts or backoff values — put them in config with defaults

## Dependencies

Direct deps (keep minimal): `pgx/v5` (PG driver), `pglogrepl` (WAL logical replication protocol), `cobra` + `viper` (CLI/config), `chi/v5` (HTTP router), `google/uuid` (UUIDv7), `errgroup` (concurrency), `prometheus/client_golang` (metrics), `coder/websocket` (WebSocket adapter), `testcontainers-go` (test only).

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

Target: ~8-20 scenarios for a project this size. If approaching 20, consolidate related scenarios before adding new ones.

## Code Organization

```
pgpipe.go       Pipeline type (library entry point)
cmd/            CLI commands (cobra)
  pgpipe/       Binary entry point (main.go)
adapter/        Output adapter interface + implementations
  stdout/       JSON-lines to io.Writer
  webhook/      HTTP POST with retries
  sse/          Server-Sent Events broker
  file/         JSON-lines to file with rotation
  exec/         JSON-lines to subprocess stdin
  pgtable/      INSERT into PostgreSQL table
  ws/           WebSocket broker
bus/            Event fan-out
detector/       Change detection interface + implementations
  listennotify/ PostgreSQL LISTEN/NOTIFY
  walreplication/ PostgreSQL WAL logical replication
event/          Event model
health/         Component health checker
metrics/        Prometheus metrics definitions
pgpipeerr/      Typed error types
internal/       CLI-specific internals (not importable)
  config/       Viper-based configuration structs
  server/       HTTP server for SSE + WS + metrics + health
scenarios/      Integration/scenario tests (testcontainers)
testutil/       Test utilities
```

## Key Files

- `pgpipe.go` — Pipeline type, options, Run method (library entry point)
- `cmd/listen.go` — CLI wiring (uses Pipeline + CLI-specific HTTP servers)
- `adapter/adapter.go` — Adapter interface
- `detector/detector.go` — Detector interface
- `bus/bus.go` — Fan-out with non-blocking sends
- `internal/config/config.go` — All config structs + defaults (CLI-only)
- `event/event.go` — Event model (UUIDv7, JSON payload)
- `health/health.go` — Component health checker
- `metrics/metrics.go` — Prometheus metric definitions
- `pgpipeerr/errors.go` — Typed errors (ErrBusClosed, WebhookDeliveryError, DetectorDisconnectedError, ExecProcessError)
- `internal/server/server.go` — HTTP server with SSE, WS, metrics, and health endpoints (CLI-only)
- `scenarios/helpers_test.go` — Shared test infrastructure (PG container, pipeline wiring)
