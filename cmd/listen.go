package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/florinutz/pgcdc"
	"github.com/florinutz/pgcdc/adapter/middleware"
	"github.com/florinutz/pgcdc/adapter/sse"
	"github.com/florinutz/pgcdc/adapter/ws"
	"github.com/florinutz/pgcdc/backpressure"
	"github.com/florinutz/pgcdc/bus"
	"github.com/florinutz/pgcdc/dlq"
	"github.com/florinutz/pgcdc/encoding"
	encregistry "github.com/florinutz/pgcdc/encoding/registry"
	"github.com/florinutz/pgcdc/health"
	"github.com/florinutz/pgcdc/inspect"
	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/internal/migrate"
	"github.com/florinutz/pgcdc/internal/server"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/registry"
	"github.com/florinutz/pgcdc/schema"
	"github.com/florinutz/pgcdc/tracing"
	"github.com/florinutz/pgcdc/transform"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.opentelemetry.io/otel/trace"
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

func init() {
	f := listenCmd.Flags()

	f.StringSliceP("channel", "c", nil, "PG channels to listen on (repeatable)")
	f.StringSliceP("adapter", "a", []string{"stdout"}, "adapters: webhook, sse, stdout (repeatable)")
	f.String("db", "", "PostgreSQL connection string (env: PGCDC_DATABASE_URL)")
	f.String("metrics-addr", "", "standalone metrics/health server address (e.g. :9090)")
	f.String("detector", "listen_notify", "detector type: listen_notify, wal, outbox, mysql, or mongodb")
	f.String("publication", "", "PostgreSQL publication name (required for --detector wal)")
	f.Bool("all-tables", false, "auto-create a FOR ALL TABLES publication and start WAL streaming (zero-config)")
	f.Bool("tx-metadata", false, "include transaction metadata in WAL events (xid, commit_time, seq)")
	f.Bool("tx-markers", false, "emit BEGIN/COMMIT marker events (implies --tx-metadata)")

	// Persistent slot flags.
	f.Bool("persistent-slot", false, "use a named persistent replication slot (survives disconnects)")
	f.String("slot-name", "", "replication slot name (default: pgcdc_<publication>)")
	f.String("checkpoint-db", "", "PostgreSQL URL for checkpoint storage (default: same as --db)")

	// Schema flags.
	f.Bool("include-schema", false, "include column type metadata in WAL events")
	f.Bool("schema-events", false, "emit SCHEMA_CHANGE events when table columns change")

	// Heartbeat flags.
	f.Duration("heartbeat-interval", 30*time.Second, "heartbeat interval for WAL slot advancement (0 to disable)")
	f.String("heartbeat-table", "pgcdc_heartbeat", "heartbeat table name")
	f.Int64("slot-lag-warn", 100*1024*1024, "slot lag warning threshold in bytes (default 100MB)")

	// Snapshot-first flags.
	f.Bool("snapshot-first", false, "run a table snapshot before live WAL streaming (requires --detector wal)")
	f.String("snapshot-table", "", "table to snapshot (required with --snapshot-first)")
	f.String("snapshot-where", "", "optional WHERE clause for snapshot")

	// TOAST cache flags.
	f.Bool("toast-cache", false, "enable in-memory TOAST column cache to resolve unchanged columns without REPLICA IDENTITY FULL (requires --detector wal)")
	f.Int("toast-cache-max-entries", 100000, "maximum number of rows in the TOAST cache")

	// Incremental snapshot flags.
	f.Bool("incremental-snapshot", false, "enable incremental snapshots via signal table (requires --detector wal)")
	f.String("snapshot-signal-table", "pgcdc_signals", "signal table name")
	f.Int("snapshot-chunk-size", 1000, "rows per chunk for incremental snapshots")
	f.Duration("snapshot-chunk-delay", 0, "delay between chunks for incremental snapshots")
	f.String("snapshot-progress-db", "", "PostgreSQL URL for progress storage (default: same as --db)")

	// Outbox detector flags.
	f.String("outbox-table", "pgcdc_outbox", "outbox table name")
	f.Duration("outbox-poll-interval", 500*time.Millisecond, "outbox polling interval")
	f.Int("outbox-batch-size", 100, "outbox batch size per poll")
	f.Bool("outbox-keep-processed", false, "keep processed outbox rows (set processed_at instead of DELETE)")

	// MySQL detector flags.
	f.String("mysql-addr", "", "MySQL address (host:port)")
	f.String("mysql-user", "", "MySQL user")
	f.String("mysql-password", "", "MySQL password")
	f.Uint32("mysql-server-id", 0, "MySQL server ID for replication (must be > 0)")
	f.StringSlice("mysql-tables", nil, "MySQL tables to replicate (schema.table format, repeatable)")
	f.Bool("mysql-gtid", false, "use GTID-based replication instead of file+position")
	f.String("mysql-flavor", "mysql", "MySQL flavor: mysql or mariadb")
	f.String("mysql-binlog-prefix", "mysql-bin", "binlog filename prefix for position decoding")

	// MongoDB detector flags.
	f.String("mongodb-uri", "", "MongoDB connection URI")
	f.String("mongodb-scope", "collection", "MongoDB watch scope: collection, database, or cluster")
	f.String("mongodb-database", "", "MongoDB database name")
	f.StringSlice("mongodb-collections", nil, "MongoDB collections to watch (repeatable)")
	f.String("mongodb-full-document", "updateLookup", "MongoDB fullDocument option: updateLookup, default, whenAvailable, required")
	f.String("mongodb-metadata-db", "", "MongoDB database for resume token storage (default: same as --mongodb-database)")
	f.String("mongodb-metadata-coll", "pgcdc_resume_tokens", "MongoDB collection for resume token storage")

	// Bus mode flag.
	f.String("bus-mode", "fast", "bus fan-out mode: fast (drop on full) or reliable (block on full)")

	// Cooperative checkpoint flag.
	f.Bool("cooperative-checkpoint", false, "checkpoint advances only after all adapters acknowledge (requires --persistent-slot and --detector wal)")

	// Transform flags.
	f.StringSlice("drop-columns", nil, "global: drop these columns from event payloads (repeatable)")
	f.StringSlice("filter-operations", nil, "global: only pass events with these operations (e.g. INSERT,UPDATE)")
	f.Bool("debezium-envelope", false, "global: rewrite payloads into Debezium-compatible envelope format")
	f.String("debezium-connector-name", "pgcdc", "Debezium source.name field (requires --debezium-envelope)")
	f.String("debezium-database", "", "Debezium source.db field (requires --debezium-envelope)")
	f.Bool("cloudevents-envelope", false, "global: rewrite payloads into CloudEvents structured-mode JSON")
	f.String("cloudevents-source", "/pgcdc", "CloudEvents source URI-reference (requires --cloudevents-envelope)")
	f.String("cloudevents-type-prefix", "io.pgcdc.change", "CloudEvents type prefix (requires --cloudevents-envelope)")

	// Backpressure flags.
	f.Bool("backpressure", false, "enable source-aware backpressure (requires --detector wal and --persistent-slot)")
	f.Int64("bp-warn-threshold", 500*1024*1024, "yellow zone threshold in bytes (default 500MB)")
	f.Int64("bp-critical-threshold", 2*1024*1024*1024, "red zone threshold in bytes (default 2GB)")
	f.Duration("bp-max-throttle", 500*time.Millisecond, "max sleep between WAL reads in yellow zone")
	f.Duration("bp-poll-interval", 10*time.Second, "backpressure lag polling interval")
	f.StringSlice("adapter-priority", nil, "adapter priority: name=critical|normal|best-effort (repeatable)")

	// Plugin flags.
	f.StringSlice("plugin-transform", nil, "wasm transform plugin paths (repeatable)")
	f.StringSlice("plugin-transform-config", nil, "JSON config for each plugin-transform (parallel order)")
	f.StringSlice("plugin-adapter", nil, "wasm adapter plugin paths (repeatable)")
	f.StringSlice("plugin-adapter-name", nil, "names for each plugin-adapter (parallel order)")
	f.StringSlice("plugin-adapter-config", nil, "JSON config for each plugin-adapter (parallel order)")
	f.String("dlq-plugin-path", "", "wasm DLQ plugin path (use with --dlq plugin)")
	f.String("dlq-plugin-config", "", "JSON config for DLQ plugin")
	f.String("checkpoint-plugin", "", "wasm checkpoint store plugin path")
	f.String("checkpoint-plugin-config", "", "JSON config for checkpoint plugin")

	// DLQ flags.
	f.String("dlq", "stderr", "dead letter queue backend: stderr, pg_table, or none")
	f.String("dlq-table", "pgcdc_dead_letters", "DLQ table name (for pg_table backend)")
	f.String("dlq-db", "", "PostgreSQL URL for DLQ table (default: same as --db)")

	// Route flags.
	f.StringSlice("route", nil, "route events to adapter: adapter=channel1,channel2 (repeatable)")

	// Startup validation flag.
	f.Bool("skip-validation", false, "skip adapter startup validation")

	// Schema migrations flag.
	f.Bool("skip-migrations", false, "skip internal schema migrations (for read-only DB users)")

	// View query flag (repeatable, name:query format).
	f.StringSlice("view-query", nil, "streaming SQL view query: name:query (repeatable, e.g. --view-query 'counts:SELECT COUNT(*) FROM pgcdc_events GROUP BY channel TUMBLING WINDOW 1m')")

	// Shared encoding flags (used by kafka and nats; kept here so they are always
	// registered regardless of which adapters are built in or out).
	f.String("schema-registry-url", "", "Confluent Schema Registry URL")
	f.String("schema-registry-username", "", "Schema Registry basic auth username")
	f.String("schema-registry-password", "", "Schema Registry basic auth password")

	// OpenTelemetry flags.
	f.String("otel-exporter", "none", "OTel trace exporter: none, stdout, or otlp")
	f.String("otel-endpoint", "", "OTLP gRPC endpoint (overrides OTEL_EXPORTER_OTLP_ENDPOINT)")
	f.Float64("otel-sample-ratio", 1.0, "trace sampling ratio (0.0-1.0)")

	// Inspector flags.
	f.Int("inspect-buffer", 100, "inspector ring buffer size per tap point (0 to disable)")

	// Schema store flags.
	f.Bool("schema-store", false, "enable schema versioning (track column changes)")
	f.String("schema-store-type", "memory", "schema store backend: memory or pg")
	f.String("schema-db", "", "PostgreSQL URL for schema store (default: same as --db)")

	// Nack window flags.
	f.Int("dlq-window-size", 0, "nack window size for adaptive DLQ (0 to disable)")
	f.Int("dlq-window-threshold", 0, "nack count to trigger adapter pause (default: window_size/2)")

	// Multi-detector flags.
	f.String("detector-mode", "sequential", "multi-detector mode: sequential, parallel, or failover")

	// CEL filter CLI shortcut.
	f.String("filter-cel", "", "global CEL filter expression (e.g. 'operation == \"INSERT\"')")
}

// bindListenFlags binds all listen command flags to viper keys. Called in PreRunE
// so only the active command's bindings are established (avoids global viper key
// collisions between listen and snapshot commands).
func bindListenFlags(cmd *cobra.Command, _ []string) error {
	if err := bindFlagsToViper(cmd.Flags(), [][2]string{
		// Core.
		{"channel", "channels"},
		{"adapter", "adapters"},
		{"db", "database_url"},
		{"bus-mode", "bus.mode"},
		{"metrics-addr", "metrics_addr"},
		{"skip-validation", "skip_validation"},
		{"skip-migrations", "skip_migrations"},

		// Detector.
		{"detector", "detector.type"},
		{"publication", "detector.publication"},
		{"all-tables", "detector.all_tables"},
		{"tx-metadata", "detector.tx_metadata"},
		{"tx-markers", "detector.tx_markers"},
		{"persistent-slot", "detector.persistent_slot"},
		{"slot-name", "detector.slot_name"},
		{"checkpoint-db", "detector.checkpoint_db"},
		{"include-schema", "detector.include_schema"},
		{"schema-events", "detector.schema_events"},
		{"heartbeat-interval", "detector.heartbeat_interval"},
		{"heartbeat-table", "detector.heartbeat_table"},
		{"slot-lag-warn", "detector.slot_lag_warn"},
		{"snapshot-first", "detector.snapshot_first"},
		{"toast-cache", "detector.toast_cache"},
		{"toast-cache-max-entries", "detector.toast_cache_max_entries"},
		{"cooperative-checkpoint", "detector.cooperative_checkpoint"},

		// Snapshot.
		{"snapshot-table", "snapshot.table"},
		{"snapshot-where", "snapshot.where"},

		// Incremental snapshot.
		{"incremental-snapshot", "incremental_snapshot.enabled"},
		{"snapshot-signal-table", "incremental_snapshot.signal_table"},
		{"snapshot-chunk-size", "incremental_snapshot.chunk_size"},
		{"snapshot-chunk-delay", "incremental_snapshot.chunk_delay"},
		{"snapshot-progress-db", "incremental_snapshot.progress_db"},

		// Backpressure.
		{"backpressure", "backpressure.enabled"},
		{"bp-warn-threshold", "backpressure.warn_threshold"},
		{"bp-critical-threshold", "backpressure.critical_threshold"},
		{"bp-max-throttle", "backpressure.max_throttle"},
		{"bp-poll-interval", "backpressure.poll_interval"},

		// DLQ.
		{"dlq", "dlq.type"},
		{"dlq-table", "dlq.table"},
		{"dlq-db", "dlq.db_url"},

		// Outbox.
		{"outbox-table", "outbox.table"},
		{"outbox-poll-interval", "outbox.poll_interval"},
		{"outbox-batch-size", "outbox.batch_size"},
		{"outbox-keep-processed", "outbox.keep_processed"},

		// MySQL.
		{"mysql-addr", "mysql.addr"},
		{"mysql-user", "mysql.user"},
		{"mysql-password", "mysql.password"},
		{"mysql-server-id", "mysql.server_id"},
		{"mysql-tables", "mysql.tables"},
		{"mysql-gtid", "mysql.use_gtid"},
		{"mysql-flavor", "mysql.flavor"},
		{"mysql-binlog-prefix", "mysql.binlog_prefix"},

		// MongoDB.
		{"mongodb-uri", "mongodb.uri"},
		{"mongodb-scope", "mongodb.scope"},
		{"mongodb-database", "mongodb.database"},
		{"mongodb-collections", "mongodb.collections"},
		{"mongodb-full-document", "mongodb.full_document"},
		{"mongodb-metadata-db", "mongodb.metadata_db"},
		{"mongodb-metadata-coll", "mongodb.metadata_coll"},

		// Shared encoding / Schema Registry.
		{"schema-registry-url", "encoding.schema_registry_url"},
		{"schema-registry-username", "encoding.schema_registry_username"},
		{"schema-registry-password", "encoding.schema_registry_password"},

		// OpenTelemetry.
		{"otel-exporter", "otel.exporter"},
		{"otel-endpoint", "otel.endpoint"},
		{"otel-sample-ratio", "otel.sample_ratio"},

		// Inspector.
		{"inspect-buffer", "inspector.buffer_size"},

		// Schema store.
		{"schema-store", "schema.enabled"},
		{"schema-store-type", "schema.store"},
		{"schema-db", "schema.db_url"},

		// Nack window.
		{"dlq-window-size", "dlq.window_size"},
		{"dlq-window-threshold", "dlq.window_threshold"},

		// Multi-detector.
		{"detector-mode", "detector_mode"},
	}); err != nil {
		return err
	}

	// Adapter-specific viper bindings from registry entries.
	return registry.BindAdapterViperKeys(cmd.Flags(), bindFlagsToViper)
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

	// Run unified config validation.
	if err := cfg.Validate(); err != nil {
		return err
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

	// Create adapters.
	adapterOpts, sseBroker, wsBroker, adapterCleanup, adapterErr := setupAdapters(ctx, cfg, cmd, wasmRT, kafkaEncoder, natsEncoder, logger)
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
	startSIGHUPHandler(g, gCtx, p, immutableCLI, plugTfx, cliRoutes, logger)

	// HTTP servers: SSE/WS combined and standalone metrics.
	startHTTPServers(g, gCtx, cfg, sseBroker, wsBroker, p.Health(), readiness, insp, logger)

	// Wait for errgroup to finish (happens when context is cancelled).
	err := g.Wait()
	if err != nil && errors.Is(err, context.Canceled) {
		logger.Info("shutdown complete")
		return nil
	}
	return err
}

// ensureAllTablesPublication creates a FOR ALL TABLES publication if it doesn't already exist.
func ensureAllTablesPublication(ctx context.Context, dbURL, publication string, logger *slog.Logger) error {
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	_, err = conn.Exec(ctx, fmt.Sprintf(
		"CREATE PUBLICATION %s FOR ALL TABLES",
		pgx.Identifier{publication}.Sanitize(),
	))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42710" { // duplicate_object
			logger.Info("all-tables publication already exists, reusing", "publication", publication)
			return nil
		}
		return fmt.Errorf("create publication: %w", err)
	}
	logger.Info("created all-tables publication", "publication", publication)
	return nil
}

// buildEncoders creates encoders for Kafka and NATS based on config.
// Returns nil encoders for JSON encoding (default, no-op passthrough).
func buildEncoders(cfg config.Config, logger *slog.Logger) (kafkaEnc, natsEnc encoding.Encoder, err error) {
	// Parse encoding types.
	kafkaEncType, err := encoding.ParseEncodingType(cfg.Kafka.Encoding)
	if err != nil {
		return nil, nil, fmt.Errorf("kafka encoding: %w", err)
	}
	natsEncType, err := encoding.ParseEncodingType(cfg.Nats.Encoding)
	if err != nil {
		return nil, nil, fmt.Errorf("nats encoding: %w", err)
	}

	// If both are JSON, no encoders needed.
	if kafkaEncType == encoding.EncodingJSON && natsEncType == encoding.EncodingJSON {
		return nil, nil, nil
	}

	// Build schema registry client if any non-JSON encoding is configured.
	var regClient *encregistry.Client
	if cfg.Encoding.SchemaRegistryURL != "" {
		regClient = encregistry.New(
			cfg.Encoding.SchemaRegistryURL,
			cfg.Encoding.SchemaRegistryUsername,
			cfg.Encoding.SchemaRegistryPassword,
		)
	}

	kafkaEnc = makeEncoder(kafkaEncType, regClient, logger)
	natsEnc = makeEncoder(natsEncType, regClient, logger)

	return kafkaEnc, natsEnc, nil
}

func makeEncoder(encType encoding.EncodingType, regClient *encregistry.Client, logger *slog.Logger) encoding.Encoder {
	switch encType {
	case encoding.EncodingAvro:
		opts := []encoding.AvroOption{}
		if regClient != nil {
			opts = append(opts, encoding.WithRegistry(regClient))
		}
		return encoding.NewAvroEncoder(logger, opts...)
	case encoding.EncodingProtobuf:
		return encoding.NewProtobufEncoder(regClient, logger)
	default:
		return nil // JSON = nil encoder, adapters use raw bytes
	}
}

// middlewareConfigFrom builds a middleware.Config from per-adapter config fields.
// Used for backward compatibility: existing --webhook-cb-failures etc. flags
// populate the middleware config that wraps the Deliverer.

func webhookMiddlewareConfig(cfg config.Config) middleware.Config {
	return middleware.Config{
		CircuitBreaker: cbConfig(cfg.Webhook.CBMaxFailures, cfg.Webhook.CBResetTimeout),
		RateLimit:      rlConfig(cfg.Webhook.RateLimit, cfg.Webhook.RateLimitBurst),
	}
}

func cbConfig(maxFailures int, resetTimeout time.Duration) *middleware.CircuitBreakerConfig {
	if maxFailures <= 0 {
		return nil
	}
	return &middleware.CircuitBreakerConfig{
		MaxFailures:  maxFailures,
		ResetTimeout: resetTimeout,
	}
}

func rlConfig(eps float64, burst int) *middleware.RateLimitConfig {
	if eps <= 0 {
		return nil
	}
	return &middleware.RateLimitConfig{
		EventsPerSecond: eps,
		Burst:           burst,
	}
}

// setupBackpressure creates the backpressure controller and parses adapter priorities.
// Returns nil if backpressure is disabled.
func setupBackpressure(cfg config.Config, cmd *cobra.Command, logger *slog.Logger) (*backpressure.Controller, error) {
	if !cfg.Backpressure.Enabled {
		return nil, nil
	}
	bpCtrl := backpressure.New(
		cfg.Backpressure.WarnThreshold,
		cfg.Backpressure.CriticalThreshold,
		cfg.Backpressure.MaxThrottle,
		cfg.Backpressure.PollInterval,
		nil, // health checker injected by pipeline
		logger,
	)
	adapterPriorityFlags, _ := cmd.Flags().GetStringSlice("adapter-priority")
	for _, ap := range adapterPriorityFlags {
		parts := strings.SplitN(ap, "=", 2)
		if len(parts) != 2 {
			logger.Warn("ignoring malformed adapter-priority (expected name=level)", "value", ap)
			continue
		}
		var prio backpressure.AdapterPriority
		switch parts[1] {
		case "critical":
			prio = backpressure.PriorityCritical
		case "normal":
			prio = backpressure.PriorityNormal
		case "best-effort":
			prio = backpressure.PriorityBestEffort
		default:
			return nil, fmt.Errorf("unknown adapter priority %q for %q (expected critical, normal, or best-effort)", parts[1], parts[0])
		}
		bpCtrl.SetAdapterPriority(parts[0], prio)
	}
	return bpCtrl, nil
}

// setupDLQ creates the dead letter queue backend.
func setupDLQ(ctx context.Context, cfg config.Config, cmd *cobra.Command, wasmRT any, logger *slog.Logger) (dlq.DLQ, error) {
	switch cfg.DLQ.Type {
	case "stderr", "":
		return dlq.NewStderrDLQ(logger), nil
	case "pg_table":
		dlqDB := cfg.DLQ.DBURL
		if dlqDB == "" {
			dlqDB = cfg.DatabaseURL
		}
		return dlq.NewPGTableDLQ(dlqDB, cfg.DLQ.Table, logger), nil
	case "none":
		return dlq.NopDLQ{}, nil
	case "plugin":
		dlqPluginPath, _ := cmd.Flags().GetString("dlq-plugin-path")
		if dlqPluginPath == "" && cfg.Plugins.DLQ != nil {
			dlqPluginPath = cfg.Plugins.DLQ.Path
		}
		if dlqPluginPath == "" {
			return nil, fmt.Errorf("--dlq plugin requires --dlq-plugin-path or plugins.dlq.path in config")
		}
		dlqPluginCfgStr, _ := cmd.Flags().GetString("dlq-plugin-config")
		dlqPluginCfg := map[string]any{}
		if dlqPluginCfgStr != "" {
			if err := json.Unmarshal([]byte(dlqPluginCfgStr), &dlqPluginCfg); err != nil {
				return nil, fmt.Errorf("parse --dlq-plugin-config: %w", err)
			}
		} else if cfg.Plugins.DLQ != nil && cfg.Plugins.DLQ.Config != nil {
			dlqPluginCfg = cfg.Plugins.DLQ.Config
		}
		dlqInst, err := makePluginDLQ(ctx, wasmRT, dlqPluginPath, dlqPluginCfg, logger)
		if err != nil {
			return nil, fmt.Errorf("create wasm dlq: %w", err)
		}
		return dlqInst, nil
	default:
		return nil, fmt.Errorf("unknown DLQ type: %q (expected stderr, pg_table, plugin, or none)", cfg.DLQ.Type)
	}
}

// setupDetector creates and configures the change data capture detector via the registry.
// Returns the detector, additional pipeline options (checkpoint store), and a cleanup function.
func setupDetector(ctx context.Context, cfg config.Config, cmd *cobra.Command, tp trace.TracerProvider, wasmRT any, logger *slog.Logger) (registry.DetectorResult, []pgcdc.Option, error) {
	// Pre-process checkpoint plugin config from CLI flag (JSON string → map) before
	// calling the factory, since the factory only reads from cfg.
	cpPluginPath, _ := cmd.Flags().GetString("checkpoint-plugin")
	cpPluginCfgStr, _ := cmd.Flags().GetString("checkpoint-plugin-config")
	if cpPluginPath != "" || cpPluginCfgStr != "" {
		if cfg.Plugins.Checkpoint == nil {
			cfg.Plugins.Checkpoint = &config.PluginSpec{}
		}
		if cpPluginPath != "" {
			cfg.Plugins.Checkpoint.Path = cpPluginPath
		}
		if cpPluginCfgStr != "" {
			cfg.Plugins.Checkpoint.Config = map[string]any{}
			if err := json.Unmarshal([]byte(cpPluginCfgStr), &cfg.Plugins.Checkpoint.Config); err != nil {
				return registry.DetectorResult{}, nil, fmt.Errorf("parse --checkpoint-plugin-config: %w", err)
			}
		}
	}

	detType := cfg.Detector.Type
	if detType == "" {
		detType = "listen_notify"
	}

	detCtx := registry.DetectorContext{
		Cfg:            cfg,
		Logger:         logger,
		Ctx:            ctx,
		TracerProvider: tp,
		WasmRT:         wasmRT,
	}

	result, err := registry.CreateDetector(detType, detCtx)
	if err != nil {
		return registry.DetectorResult{}, nil, err
	}

	var opts []pgcdc.Option
	if result.CheckpointStore != nil {
		opts = append(opts, pgcdc.WithCheckpointStore(result.CheckpointStore))
	}
	return result, opts, nil
}

// setupAdapters creates all configured adapters via the registry, plus view and plugin adapters.
// Returns pipeline options, SSE/WS brokers for HTTP server setup, and a cleanup function.
func setupAdapters(ctx context.Context, cfg config.Config, cmd *cobra.Command, wasmRT any, kafkaEncoder, natsEncoder encoding.Encoder, logger *slog.Logger) ([]pgcdc.Option, *sse.Broker, *ws.Broker, func(), error) {
	adapterCtx := registry.AdapterContext{
		Cfg:          cfg,
		Logger:       logger,
		Ctx:          ctx,
		KafkaEncoder: kafkaEncoder,
		NatsEncoder:  natsEncoder,
	}

	var opts []pgcdc.Option
	var sseBroker *sse.Broker
	var wsBroker *ws.Broker

	for _, name := range cfg.Adapters {
		result, aErr := registry.CreateAdapter(name, adapterCtx)
		if aErr != nil {
			return nil, nil, nil, func() {}, fmt.Errorf("create adapter %s: %w", name, aErr)
		}
		opts = append(opts, pgcdc.WithAdapter(result.Adapter))

		if result.MiddlewareConfig != nil {
			opts = append(opts, pgcdc.WithMiddlewareConfig(name, *result.MiddlewareConfig))
		}
		if result.SSEBroker != nil {
			sseBroker = result.SSEBroker
		}
		if result.WSBroker != nil {
			wsBroker = result.WSBroker
		}
	}

	// Parse --view-query CLI flags and merge with YAML views (CLI wins on name conflict).
	viewQueryFlags, _ := cmd.Flags().GetStringSlice("view-query")
	for _, vq := range viewQueryFlags {
		parts := strings.SplitN(vq, ":", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, nil, nil, func() {}, fmt.Errorf("invalid --view-query format %q: expected name:query", vq)
		}
		replaced := false
		for i, vc := range cfg.Views {
			if vc.Name == parts[0] {
				cfg.Views[i].Query = parts[1]
				replaced = true
				break
			}
		}
		if !replaced {
			cfg.Views = append(cfg.Views, config.ViewConfig{
				Name:  parts[0],
				Query: parts[1],
			})
		}
	}

	// Auto-create view adapter from YAML views or --view-query (if not already added via --adapter view).
	if len(cfg.Views) > 0 {
		hasViewAdapter := false
		for _, name := range cfg.Adapters {
			if name == "view" {
				hasViewAdapter = true
				break
			}
		}
		if !hasViewAdapter {
			adapterCtx.Cfg = cfg // reflect updated views
			result, aErr := registry.CreateAdapter("view", adapterCtx)
			if aErr != nil {
				return nil, nil, nil, func() {}, fmt.Errorf("create view adapter: %w", aErr)
			}
			opts = append(opts, pgcdc.WithAdapter(result.Adapter))
		}
	}

	// Create plugin adapters.
	pluginAdapterOpts, pluginAdapterCleanup, paErr := wirePluginAdapters(ctx, wasmRT, cmd, cfg, logger)
	if paErr != nil {
		return nil, nil, nil, func() {}, paErr
	}
	opts = append(opts, pluginAdapterOpts...)

	return opts, sseBroker, wsBroker, pluginAdapterCleanup, nil
}

// setupTransforms builds the complete transform chain from CLI flags, plugins, and YAML config.
// Returns the immutable CLI transforms (for SIGHUP reload), plugin transforms, all pipeline options, and a cleanup function.
func setupTransforms(ctx context.Context, cfg config.Config, cmd *cobra.Command, wasmRT any, logger *slog.Logger) ([]transform.TransformFunc, pluginTransforms, []pgcdc.Option, func()) {
	globalTransforms, adapterTransforms := buildTransformOpts(cfg, cmd)
	pluginTransformOpts, plugTfx, pluginTransformCleanup := buildPluginTransformOpts(ctx, wasmRT, cmd, cfg, logger)
	immutableCLI := buildCLITransforms(cmd)

	var opts []pgcdc.Option
	for _, fn := range globalTransforms {
		opts = append(opts, pgcdc.WithTransform(fn))
	}
	for adapterName, fns := range adapterTransforms {
		for _, fn := range fns {
			opts = append(opts, pgcdc.WithAdapterTransform(adapterName, fn))
		}
	}
	opts = append(opts, pluginTransformOpts...)

	return immutableCLI, plugTfx, opts, pluginTransformCleanup
}

// startSIGHUPHandler registers a goroutine in g that listens for SIGHUP and reloads transforms and routes.
func startSIGHUPHandler(g *errgroup.Group, gCtx context.Context, p *pgcdc.Pipeline, immutableCLI []transform.TransformFunc, plugTfx pluginTransforms, cliRoutes map[string][]string, logger *slog.Logger) {
	sighupCh := make(chan os.Signal, 1)
	signal.Notify(sighupCh, syscall.SIGHUP)
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
			}
		}
	})
}

// startHTTPServers registers goroutines in g for the SSE/WS HTTP server (when either broker is active)
// and the standalone metrics server (when --metrics-addr is set).
func startHTTPServers(g *errgroup.Group, gCtx context.Context, cfg config.Config, sseBroker *sse.Broker, wsBroker *ws.Broker, checker *health.Checker, readiness *health.ReadinessChecker, insp *inspect.Inspector, logger *slog.Logger) {
	if sseBroker != nil || wsBroker != nil || insp != nil {
		var serverOpts []server.ServerOption
		if insp != nil {
			serverOpts = append(serverOpts, server.WithInspector(insp))
		}
		httpServer := server.New(sseBroker, wsBroker, cfg.SSE.CORSOrigins, cfg.SSE.ReadTimeout, cfg.SSE.IdleTimeout, checker, readiness, serverOpts...)
		httpServer.Addr = cfg.SSE.Addr

		g.Go(func() error {
			logger.Info("http server starting", "addr", cfg.SSE.Addr)
			ln, err := net.Listen("tcp", cfg.SSE.Addr)
			if err != nil {
				return fmt.Errorf("http listen: %w", err)
			}
			if err := httpServer.Serve(ln); err != nil && err != http.ErrServerClosed {
				return fmt.Errorf("http serve: %w", err)
			}
			return nil
		})

		g.Go(func() error {
			<-gCtx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
			defer cancel()
			logger.Info("shutting down http server")
			return httpServer.Shutdown(shutdownCtx)
		})
	}

	if cfg.MetricsAddr != "" {
		metricsServer := server.NewMetricsServer(checker, readiness)
		metricsServer.Addr = cfg.MetricsAddr

		g.Go(func() error {
			logger.Info("metrics server starting", "addr", cfg.MetricsAddr)
			ln, err := net.Listen("tcp", cfg.MetricsAddr)
			if err != nil {
				return fmt.Errorf("metrics listen: %w", err)
			}
			if err := metricsServer.Serve(ln); err != nil && err != http.ErrServerClosed {
				return fmt.Errorf("metrics serve: %w", err)
			}
			return nil
		})

		g.Go(func() error {
			<-gCtx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
			defer cancel()
			logger.Info("shutting down metrics server")
			return metricsServer.Shutdown(shutdownCtx)
		})
	}
}
