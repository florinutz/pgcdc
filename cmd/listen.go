package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/florinutz/pgcdc"
	"github.com/florinutz/pgcdc/bus"
	"github.com/florinutz/pgcdc/detector"
	"github.com/florinutz/pgcdc/health"
	"github.com/florinutz/pgcdc/inspect"
	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/internal/logcolor"
	"github.com/florinutz/pgcdc/internal/migrate"
	"github.com/florinutz/pgcdc/internal/output"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/registry"
	"github.com/florinutz/pgcdc/schema"
	"github.com/florinutz/pgcdc/tracing"
	"github.com/florinutz/pgcdc/transform"
	"github.com/go-chi/chi/v5"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"
)

var listenCmd = &cobra.Command{
	Use:   "listen",
	Short: "Listen for PostgreSQL NOTIFY events and forward them to adapters",
	Long: `Connects to PostgreSQL, subscribes to one or more channels via
LISTEN/NOTIFY, and fans out every notification to the configured adapters
(stdout, webhook, sse).`,
	PreRunE: bindListenFlags,
	RunE:    runListen,
}

func runListen(cmd *cobra.Command, args []string) error {
	// Load config with defaults.
	cfg := config.Default()
	if err := viper.Unmarshal(&cfg); err != nil {
		return fmt.Errorf("unmarshal config: %w", err)
	}

	// tx-markers implies tx-metadata.
	if cfg.Detector.TxMarkers {
		cfg.Detector.TxMetadata = true
	}

	// --all-tables: switch to WAL detector and set default publication.
	if cfg.Detector.AllTables {
		if cfg.Detector.Type == "outbox" {
			return fmt.Errorf("--all-tables is not compatible with --detector outbox")
		}
		if cfg.Detector.Type == "listen_notify" {
			cfg.Detector.Type = "wal"
		}
		if cfg.Detector.Publication == "" {
			cfg.Detector.Publication = "all"
		}
	}

	// Parse bus mode from config.
	var busMode bus.BusMode
	switch cfg.Bus.Mode {
	case "reliable":
		busMode = bus.BusModeReliable
	case "fast", "":
		busMode = bus.BusModeFast
	default:
		return fmt.Errorf("unknown bus mode: %q (expected fast or reliable)", cfg.Bus.Mode)
	}

	// Populate validation sets from registry before validating config.
	config.SetKnownAdapters(registry.AdapterNames())

	requiresDB := make(map[string]bool)
	for _, d := range registry.Detectors() {
		requiresDB[d.Name] = d.RequiresDB
	}
	config.SetDetectorRequiresDB(requiresDB)

	// Run unified config validation.
	if err := cfg.Validate(); err != nil {
		return err
	}

	// Print startup summary (only for human-readable log formats).
	logFmt := viper.GetString("log_format")
	if logFmt != "json" {
		useColor := logcolor.ShouldColor(os.Stderr.Fd(), logFmt)
		output.PrintSummary(os.Stderr, &cfg, useColor)
	}

	logger := slog.Default()

	// Root context: cancelled on SIGINT or SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Set up OpenTelemetry tracing.
	tp, otelShutdown, otelErr := tracing.Setup(ctx, tracing.Config{
		Exporter:     cfg.OTel.Exporter,
		Endpoint:     cfg.OTel.Endpoint,
		SampleRatio:  cfg.OTel.SampleRatio,
		DetectorType: cfg.Detector.Type,
	}, logger)
	if otelErr != nil {
		return fmt.Errorf("setup otel tracing: %w", otelErr)
	}
	defer otelShutdown()

	// Build base pipeline options.
	opts := []pgcdc.Option{
		pgcdc.WithBusBuffer(cfg.Bus.BufferSize),
		pgcdc.WithBusMode(busMode),
		pgcdc.WithLogger(logger),
		pgcdc.WithTracerProvider(tp),
		pgcdc.WithShutdownTimeout(cfg.ShutdownTimeout),
	}
	if cfg.SkipValidation {
		opts = append(opts, pgcdc.WithSkipValidation(true))
	}
	if cfg.Detector.CooperativeCheckpoint {
		opts = append(opts, pgcdc.WithCooperativeCheckpoint(true))
	}

	// Backpressure controller.
	bpCtrl, bpErr := setupBackpressure(cfg, cmd, logger)
	if bpErr != nil {
		return bpErr
	}
	if bpCtrl != nil {
		opts = append(opts, pgcdc.WithBackpressure(bpCtrl))
	}

	// Build encoders for Kafka and NATS (nil = JSON passthrough).
	kafkaEncoder, natsEncoder, encErr := buildEncoders(cfg, logger)
	if encErr != nil {
		return encErr
	}
	defer func() {
		if kafkaEncoder != nil {
			_ = kafkaEncoder.Close()
		}
		if natsEncoder != nil {
			_ = natsEncoder.Close()
		}
	}()

	// Wasm runtime (created eagerly when any plugin is configured).
	var wasmRT any
	if anyPluginsConfigured(cfg, cmd) {
		var wasmCleanup func()
		wasmRT, wasmCleanup = initPluginRuntime(ctx, logger)
		_ = wasmCleanup
	}
	defer closePluginRuntime(ctx, wasmRT)

	// Set up DLQ.
	dlqInstance, dlqErr := setupDLQ(ctx, cfg, cmd, wasmRT, logger)
	if dlqErr != nil {
		return dlqErr
	}
	defer func() { _ = dlqInstance.Close() }()
	opts = append(opts, pgcdc.WithDLQ(dlqInstance))

	// Parse routes (CLI + YAML merged).
	cliRoutes := buildCLIRoutes(cmd)
	allRoutes := mergeRoutes(cliRoutes, cfg.Routes)
	for adapterName, channels := range allRoutes {
		opts = append(opts, pgcdc.WithRoute(adapterName, channels...))
	}

	// Auto-create FOR ALL TABLES publication when --all-tables is set.
	if cfg.Detector.AllTables {
		if err := ensureAllTablesPublication(ctx, cfg.DatabaseURL, cfg.Detector.Publication, logger); err != nil {
			return fmt.Errorf("ensure all-tables publication: %w", err)
		}
	}

	// Create detector.
	detResult, detOpts, detErr := setupDetector(ctx, cfg, cmd, tp, wasmRT, logger)
	if detErr != nil {
		return detErr
	}
	if detResult.Cleanup != nil {
		defer detResult.Cleanup()
	}
	opts = append(opts, detOpts...)

	// Check if detector needs HTTP routes.
	var detectorMountFn func(chi.Router)
	if m, ok := detResult.Detector.(detector.HTTPMountable); ok {
		detectorMountFn = m.MountHTTP
	}

	// Create adapters.
	adapterOpts, sseBroker, wsBroker, adapterMountFns, adapterCleanup, adapterErr := setupAdapters(ctx, cfg, cmd, wasmRT, kafkaEncoder, natsEncoder, logger)
	if adapterErr != nil {
		return adapterErr
	}
	defer adapterCleanup()
	opts = append(opts, adapterOpts...)

	// Build transforms.
	immutableCLI, plugTfx, tfxOpts, tfxCleanup := setupTransforms(ctx, cfg, cmd, wasmRT, logger)
	defer tfxCleanup()
	opts = append(opts, tfxOpts...)

	// CEL filter CLI shortcut.
	if celExpr, _ := cmd.Flags().GetString("filter-cel"); celExpr != "" {
		celFn, celErr := transform.FilterCEL(celExpr)
		if celErr != nil {
			return fmt.Errorf("compile --filter-cel expression: %w", celErr)
		}
		opts = append(opts, pgcdc.WithTransform(celFn))
		// Include in immutable CLI transforms for SIGHUP reload.
		immutableCLI = append(immutableCLI, celFn)
	}

	// Dedup CLI shortcut.
	if dedupWindow, _ := cmd.Flags().GetDuration("dedup-window"); dedupWindow > 0 {
		dedupKey, _ := cmd.Flags().GetString("dedup-key")
		dedupMax, _ := cmd.Flags().GetInt("dedup-max-keys")
		dedupFn := transform.Dedup(dedupKey, dedupWindow, dedupMax)
		opts = append(opts, pgcdc.WithTransform(dedupFn))
		immutableCLI = append(immutableCLI, dedupFn)
	}

	// Inspector.
	var insp *inspect.Inspector
	if cfg.Inspector.BufferSize > 0 {
		insp = inspect.New(cfg.Inspector.BufferSize)
		opts = append(opts, pgcdc.WithInspector(insp))
	}

	// Schema store.
	if cfg.Schema.Enabled {
		var schemaStore schema.Store
		switch cfg.Schema.Store {
		case "pg":
			dbURL := cfg.Schema.DBURL
			if dbURL == "" {
				dbURL = cfg.DatabaseURL
			}
			pgStore := schema.NewPGStore(dbURL, logger)
			if err := pgStore.Init(ctx); err != nil {
				return fmt.Errorf("init schema store: %w", err)
			}
			schemaStore = pgStore
		default:
			schemaStore = schema.NewMemoryStore()
		}
		opts = append(opts, pgcdc.WithSchemaStore(schemaStore))
	}

	// Nack window.
	if cfg.DLQ.WindowSize > 0 {
		opts = append(opts, pgcdc.WithNackWindow(cfg.DLQ.WindowSize, cfg.DLQ.WindowThreshold))
	}

	// Run schema migrations (unless --skip-migrations).
	if !cfg.SkipMigrations && cfg.DatabaseURL != "" {
		if err := migrate.Run(ctx, cfg.DatabaseURL, logger); err != nil {
			logger.Warn("schema migration failed (use --skip-migrations to skip)", "error", err)
		}
	}

	// Build pipeline.
	p := pgcdc.NewPipeline(detResult.Detector, opts...)

	// Readiness checker: starts not-ready, set ready after pipeline starts.
	readiness := health.NewReadinessChecker()

	// Use an errgroup to run the pipeline alongside CLI-specific HTTP servers.
	g, gCtx := errgroup.WithContext(ctx)

	// Run the core pipeline (detector + bus + adapters).
	g.Go(func() error {
		readiness.SetReady(true)
		defer readiness.SetReady(false)
		return p.Run(gCtx)
	})

	// SIGHUP handler: reload transforms and routes from YAML config.
	startSIGHUPHandler(g, gCtx, p, immutableCLI, plugTfx, cliRoutes, cfg, logger)

	// HTTP servers: SSE/WS combined and standalone metrics.
	allMountFns := adapterMountFns
	if detectorMountFn != nil {
		allMountFns = append(allMountFns, detectorMountFn)
	}
	startHTTPServers(g, gCtx, cfg, sseBroker, wsBroker, p.Health(), readiness, insp, allMountFns, logger)

	// Wait for errgroup to finish (happens when context is cancelled).
	err := g.Wait()
	if err != nil && errors.Is(err, context.Canceled) {
		logger.Info("shutdown complete")
		return nil
	}
	return err
}

// startSIGHUPHandler registers a goroutine in g that listens for SIGHUP and reloads transforms and routes.
func startSIGHUPHandler(g *errgroup.Group, gCtx context.Context, p *pgcdc.Pipeline, immutableCLI []transform.TransformFunc, plugTfx pluginTransforms, cliRoutes map[string][]string, initialCfg config.Config, logger *slog.Logger) {
	sighupCh := make(chan os.Signal, 1)
	signal.Notify(sighupCh, syscall.SIGHUP)

	prevSummary := config.BuildReloadSummary(initialCfg)

	g.Go(func() error {
		defer signal.Stop(sighupCh)
		for {
			select {
			case <-gCtx.Done():
				return nil
			case <-sighupCh:
				logger.Info("SIGHUP received, reloading config")

				if err := viper.ReadInConfig(); err != nil {
					logger.Error("config reload: read config file", "error", err)
					metrics.ConfigReloadErrors.Inc()
					continue
				}
				var newCfg config.Config
				newCfg = config.Default()
				if err := viper.Unmarshal(&newCfg); err != nil {
					logger.Error("config reload: unmarshal config", "error", err)
					metrics.ConfigReloadErrors.Inc()
					continue
				}

				// Warn if adapters changed — they cannot be hot-reloaded.
				if !slicesEqual(initialCfg.Adapters, newCfg.Adapters) {
					logger.Warn("adapters changed in config file but cannot be hot-reloaded; restart required",
						"initial", initialCfg.Adapters,
						"new", newCfg.Adapters,
					)
				}

				yamlGlobal, yamlAdapter := buildYAMLTransforms(newCfg)

				allGlobal := make([]transform.TransformFunc, 0, len(immutableCLI)+len(plugTfx.global)+len(yamlGlobal))
				allGlobal = append(allGlobal, immutableCLI...)
				allGlobal = append(allGlobal, plugTfx.global...)
				allGlobal = append(allGlobal, yamlGlobal...)

				allAdapter := make(map[string][]transform.TransformFunc)
				for k, v := range plugTfx.adapter {
					allAdapter[k] = append(allAdapter[k], v...)
				}
				for k, v := range yamlAdapter {
					allAdapter[k] = append(allAdapter[k], v...)
				}

				reloadedRoutes := mergeRoutes(cliRoutes, newCfg.Routes)

				if err := p.Reload(pgcdc.ReloadConfig{
					Transforms:        allGlobal,
					AdapterTransforms: allAdapter,
					Routes:            reloadedRoutes,
				}); err != nil {
					logger.Error("config reload: apply", "error", err)
					metrics.ConfigReloadErrors.Inc()
					continue
				}

				newSummary := config.BuildReloadSummary(newCfg)
				diff := config.DiffReload(prevSummary, newSummary)
				logger.Info("config reloaded", "diff", diff)
				prevSummary = newSummary
			}
		}
	})
}

// slicesEqual reports whether two string slices have the same elements in the same order.
func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
