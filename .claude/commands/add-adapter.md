Add a new output adapter to pgpipe.

## Steps

1. Read the adapter interface: `adapter/adapter.go`
2. Read the stdout adapter as a reference implementation: `adapter/stdout/` (simplest adapter)
3. Read the webhook adapter for a more complex example: `adapter/webhook/`
4. Create a new package: `adapter/$ARGUMENTS/`
5. Implement the `adapter.Adapter` interface:
   - `Start(ctx context.Context, events <-chan event.Event) error` — consume events until ctx cancelled
   - `Name() string` — return adapter name
   - `New()` constructor must handle nil logger: `if logger == nil { logger = slog.Default() }`
6. Add a switch case in `cmd/listen.go` inside the adapter creation loop (~line 105)
7. Add config fields in `internal/config/config.go` if the adapter needs configuration
8. Add CLI flags in `cmd/listen.go` `init()` function, bind to viper
9. Add validation in `cmd/listen.go` `runListen()` if the adapter has required fields (like webhook requires --url)
10. Write a scenario test: `scenarios/<name>_delivery_test.go`
    - Follow pattern: `//go:build integration`, `TestScenario_<Name>Delivery`
    - Happy path: pg_notify → detector → bus → your adapter receives correct event
    - Failure mode: one critical failure specific to this adapter
    - Use helpers from `scenarios/helpers_test.go` (startPipeline, sendNotify, etc.)
11. Register the scenario in `SCENARIOS.md`
12. Run `make test-all` — all tests must pass
