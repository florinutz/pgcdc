# Contributing to pgcdc

Thank you for your interest in contributing to pgcdc. This guide covers the development workflow, code conventions, and PR process.

## Prerequisites

- **Go 1.22+** (module: `github.com/florinutz/pgcdc`)
- **Docker** (required for integration/scenario tests via testcontainers)
- **golangci-lint** (for linting)

Optional:
- **benchstat** (`go install golang.org/x/perf/cmd/benchstat@latest`) for benchmark comparison

## Getting Started

```sh
git clone https://github.com/florinutz/pgcdc.git
cd pgcdc

# Build the binary
make build

# Run unit tests (fast, no Docker)
make test

# Run all tests including integration (requires Docker)
make test-all
```

## Make Targets

| Target | Description | Docker Required |
|--------|-------------|-----------------|
| `make build` | Compile the full binary | No |
| `make build-slim` | Binary without Kafka/gRPC/Iceberg/NATS/Redis/Wasm/Views | No |
| `make test` | Unit tests only (~2s) | No |
| `make test-scenarios` | Scenario/integration tests (~30s) | Yes |
| `make test-all` | Unit + scenario tests | Yes |
| `make lint` | Run golangci-lint | No |
| `make vet` | Run go vet | No |
| `make fmt` | Check formatting | No |
| `make coverage` | Generate coverage report | No |
| `make bench` | Run integration benchmarks | Yes |
| `make bench-unit` | Run micro-benchmarks | No |
| `make fuzz` | Run fuzz tests (30s each) | No |
| `make docker-build` | Build Docker image | Yes |

## Code Conventions

### Error Handling

- Wrap errors with context: `fmt.Errorf("verb: %w", err)` where the verb describes the failed action.
- Use typed errors from `pgcdcerr/` for errors that callers need to branch on with `errors.Is`/`errors.As`.

### Logging

- Use `log/slog` exclusively. Never `log` or `fmt.Printf`.
- Create child loggers: `logger.With("component", name)`.

### Constructors

- Every type has a `New()` constructor.
- Always handle nil logger: `if logger == nil { logger = slog.Default() }`.
- Duration parameters default to sensible values when zero.

### Channels

- Always use directional channel types (`chan<-` or `<-chan`) in function signatures.
- The bus owns channel lifecycle. Detectors must not close the events channel.

### SQL Safety

- Use `pgx.Identifier{name}.Sanitize()` for table/channel names.
- Use parameterized queries for values.

### Naming

- Short package names: `bus`, `sse`, `event`.
- Types named for what they are: `Detector`, `Adapter`, `Bus`.
- No global state except logger setup in `cmd/root.go` and Prometheus metrics in `metrics/`.

### Configuration

- All config flows through `config.Config` struct (in `internal/config/`).
- All timeouts and backoff values are configurable with defaults. No hardcoded magic numbers.

### Dependencies

- Prefer stdlib. Justify any new dependency.
- Current direct dependencies: pgx, pglogrepl, cobra, viper, chi, uuid, errgroup, prometheus, websocket, nats.go, franz-go, go-redis, grpc, protobuf, aws-sdk-go-v2, parquet-go, hamba/avro, otel, extism, go-mysql, mongo-driver, tidb/parser.

## Adding a New Adapter

1. **Implement the `adapter.Adapter` interface** (`Start(ctx, <-chan event.Event) error`, `Name() string`).
2. **Optionally implement**:
   - `adapter.Acknowledger` (`SetAckFunc(fn AckFunc)`) for cooperative checkpointing.
   - `adapter.Validator` (`Validate(ctx) error`) for startup pre-flight checks.
   - `adapter.Drainer` (`Drain(ctx) error`) for graceful shutdown flush.
   - `adapter.Traceable` (`SetTracer(t trace.Tracer)`) for OpenTelemetry tracing.
   - `adapter.Reinjector` (`SetIngestChan(ch chan<- event.Event)`) for bus re-injection.
3. **Create package** `adapter/<name>/`.
4. **Add switch case** in `cmd/listen.go` adapter loop.
5. **Add config struct** in `internal/config/config.go`.
6. **Add CLI flags** in `cmd/listen.go` `init()`.
7. **Add metrics** instrumentation: `metrics.EventsDelivered.WithLabelValues("<name>").Inc()`.
8. **Add scenario test**, register in `SCENARIOS.md`.

## Adding a New Detector

1. **Implement the `detector.Detector` interface** (`Start(ctx, chan<- event.Event) error`, `Name() string`).
2. **Create package** `detector/<name>/`.
3. **Must NOT close** the events channel (the bus owns its lifecycle).
4. **Use `pgx.Connect`** (not pool) for PostgreSQL detectors -- LISTEN requires a dedicated connection.
5. **Add selection logic** in `cmd/listen.go`.
6. **Health checker registration** is handled by the pipeline (`pgcdc.go`) — no manual `checker.Register` call needed.
7. **Add scenario test**, register in `SCENARIOS.md`.

## Adding a CLI Command

1. Create `cmd/<name>.go` (see `cmd/init.go` as template).
2. Register via `rootCmd.AddCommand(<name>Cmd)` in the file's `init()`.
3. Bind flags to viper if config-file support is needed.
4. Be aware that viper flag bindings are global -- avoid binding the same key from multiple commands.

## Testing Philosophy

Tests are the primary quality gate. The test surface is lean, non-overlapping, and focused on system boundaries.

### Two Test Surfaces

- **Unit tests** (`*_test.go` in package dirs): Pure algorithmic logic only. No I/O, no network, no goroutines.
- **Scenario tests** (`scenarios/*_test.go`): Full pipeline tests with real PostgreSQL (testcontainers). One file per user journey.

### When to Write a Unit Test

Only for pure functions with no side effects: backoff calculations, payload parsing, HMAC computation, data transformations, error type contracts.

### When to Write a Scenario Test

When you add a new user journey or a new way the system can fail at a boundary. Check `SCENARIOS.md` first. If an existing scenario covers the behavior, add a subtest. If it is a genuinely new journey, create a new file.

### What NOT to Test

- Third-party library behavior (pgx, chi, cobra, viper, prometheus).
- Simple getters, setters, or struct constructors.
- Config struct defaults.
- Code paths already proven by a scenario.
- Do NOT write a unit test AND a scenario for the same behavior.

### Coverage Workflow

Before adding a test:
```sh
go test -cover ./...
go test -cover ./scenarios/
```

After adding the test, verify coverage meaningfully increased. If it did not, the test is redundant.

## PR Process

1. **Fork and branch**: Create a feature branch from `main`.
2. **Write code**: Follow the conventions above.
3. **Test**: Run `make test-all`. All tests must pass.
4. **Lint**: Run `make lint`. Fix any issues.
5. **Commit**: Write clear commit messages. Reference issue numbers if applicable.
6. **PR description**: Describe what changed and why. Include scenario test results if adding new behavior.
7. **Review**: Address feedback. Keep commits clean (squash fixups).

### PR Checklist

- [ ] `make test-all` passes
- [ ] `make lint` passes
- [ ] New behavior has a scenario test (or extends an existing one)
- [ ] Scenario registered in `SCENARIOS.md` if new
- [ ] Config structs updated in `internal/config/config.go` if new flags added
- [ ] Metrics added for observable behavior
- [ ] No hardcoded timeouts or magic numbers
- [ ] Error wrapping follows conventions (`fmt.Errorf("verb: %w", err)`)
- [ ] No global state introduced

## Do NOT

- Use a connection pool for detectors (breaks LISTEN state).
- Close the events channel from a detector (bus owns lifecycle).
- Block in SSE or WS broadcast (non-blocking sends only).
- Use `log` or `fmt.Printf` (slog only).
- Put raw SQL identifiers in Go strings (use `pgx.Identifier{}.Sanitize()`).
- Add dependencies without justification.
- Hardcode timeouts or backoff values.
