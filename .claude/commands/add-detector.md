Add a new event source (detector) to pgpipe.

## Steps

1. Read the detector interface: `detector/detector.go`
2. Read the listennotify detector as reference: `detector/listennotify/listennotify.go`
3. Create a new package: `detector/$ARGUMENTS/`
4. Implement the `detector.Detector` interface:
   - `Start(ctx context.Context, events chan<- event.Event) error` — push events until ctx cancelled
   - `Name() string` — return detector name
   - `New()` constructor must handle nil logger: `if logger == nil { logger = slog.Default() }`

### Critical rules

- **MUST NOT close the events channel** — the bus owns channel lifecycle
- **Use `pgx.Connect` not pool** — LISTEN requires a dedicated, persistent connection
- Use `pgx.Identifier{name}.Sanitize()` for any SQL identifiers
- Implement reconnection with exponential backoff (see `listennotify.go` backoff function)
- Return `ctx.Err()` when context is cancelled (clean shutdown)

5. Add selection logic in `cmd/listen.go` to choose between detectors (e.g., a `--detector` flag or config field)
6. Add config fields in `internal/config/config.go` if needed
7. Add CLI flags and viper bindings in `cmd/listen.go`
8. Write a scenario test: `scenarios/<name>_test.go`
   - Happy path: event source → detector → bus → adapter receives event
   - Failure mode: detector recovers from connection loss
   - Use helpers from `scenarios/helpers_test.go`
9. Register the scenario in `SCENARIOS.md`
10. Run `make test-all` — all tests must pass
