package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os/signal"
	"syscall"

	"github.com/florinutz/pgpipe"
	execadapter "github.com/florinutz/pgpipe/adapter/exec"
	fileadapter "github.com/florinutz/pgpipe/adapter/file"
	"github.com/florinutz/pgpipe/adapter/pgtable"
	"github.com/florinutz/pgpipe/adapter/sse"
	"github.com/florinutz/pgpipe/adapter/stdout"
	"github.com/florinutz/pgpipe/adapter/webhook"
	"github.com/florinutz/pgpipe/adapter/ws"
	"github.com/florinutz/pgpipe/detector"
	"github.com/florinutz/pgpipe/detector/listennotify"
	"github.com/florinutz/pgpipe/detector/walreplication"
	"github.com/florinutz/pgpipe/internal/config"
	"github.com/florinutz/pgpipe/internal/server"
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
	RunE: runListen,
}

func init() {
	f := listenCmd.Flags()

	f.StringSliceP("channel", "c", nil, "PG channels to listen on (repeatable)")
	f.StringSliceP("adapter", "a", []string{"stdout"}, "adapters: webhook, sse, stdout (repeatable)")
	f.StringP("url", "u", "", "webhook destination URL")
	f.String("sse-addr", ":8080", "SSE server listen address")
	f.String("db", "", "PostgreSQL connection string (env: PGPIPE_DATABASE_URL)")
	f.Int("retries", 5, "webhook max retries")
	f.String("signing-key", "", "HMAC signing key for webhook")
	f.String("metrics-addr", "", "standalone metrics/health server address (e.g. :9090)")
	f.String("detector", "listen_notify", "detector type: listen_notify or wal")
	f.String("publication", "", "PostgreSQL publication name (required for --detector wal)")
	f.Bool("tx-metadata", false, "include transaction metadata in WAL events (xid, commit_time, seq)")
	f.Bool("tx-markers", false, "emit BEGIN/COMMIT marker events (implies --tx-metadata)")

	// File adapter flags.
	f.String("file-path", "", "file adapter output path")
	f.Int64("file-max-size", 0, "file rotation size in bytes (default 100MB)")
	f.Int("file-max-files", 0, "number of rotated files to keep (default 5)")

	// Exec adapter flags.
	f.String("exec-command", "", "shell command to pipe events to (via stdin)")

	// PG table adapter flags.
	f.String("pg-table-url", "", "PostgreSQL URL for pg_table adapter (default: same as --db)")
	f.String("pg-table-name", "", "destination table name (default: pgpipe_events)")

	// WebSocket adapter flags.
	f.Duration("ws-ping-interval", 0, "WebSocket ping interval (default 15s)")

	mustBindPFlag("channels", f.Lookup("channel"))
	mustBindPFlag("adapters", f.Lookup("adapter"))
	mustBindPFlag("webhook.url", f.Lookup("url"))
	mustBindPFlag("sse.addr", f.Lookup("sse-addr"))
	mustBindPFlag("database_url", f.Lookup("db"))
	mustBindPFlag("webhook.max_retries", f.Lookup("retries"))
	mustBindPFlag("webhook.signing_key", f.Lookup("signing-key"))
	mustBindPFlag("metrics_addr", f.Lookup("metrics-addr"))
	mustBindPFlag("detector.type", f.Lookup("detector"))
	mustBindPFlag("detector.publication", f.Lookup("publication"))
	mustBindPFlag("detector.tx_metadata", f.Lookup("tx-metadata"))
	mustBindPFlag("detector.tx_markers", f.Lookup("tx-markers"))
	mustBindPFlag("file.path", f.Lookup("file-path"))
	mustBindPFlag("file.max_size", f.Lookup("file-max-size"))
	mustBindPFlag("file.max_files", f.Lookup("file-max-files"))
	mustBindPFlag("exec.command", f.Lookup("exec-command"))
	mustBindPFlag("pg_table.url", f.Lookup("pg-table-url"))
	mustBindPFlag("pg_table.table", f.Lookup("pg-table-name"))
	mustBindPFlag("websocket.ping_interval", f.Lookup("ws-ping-interval"))
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

	// Validation.
	if cfg.Detector.Type != "wal" && (cfg.Detector.TxMetadata || cfg.Detector.TxMarkers) {
		return fmt.Errorf("--tx-metadata and --tx-markers require --detector wal")
	}
	if cfg.DatabaseURL == "" {
		return fmt.Errorf("no database URL specified; use --db, set database_url in config, or export PGPIPE_DATABASE_URL")
	}
	if cfg.Detector.Type != "wal" && len(cfg.Channels) == 0 {
		return fmt.Errorf("no channels specified; use --channel or set channels in config file")
	}
	if cfg.Detector.Type == "wal" && cfg.Detector.Publication == "" {
		return fmt.Errorf("WAL detector requires a publication; use --publication or set detector.publication in config")
	}

	// Validate adapters and check requirements early.
	hasWebhook := false
	hasSSE := false
	hasFile := false
	hasExec := false
	hasPGTable := false
	hasWS := false
	for _, name := range cfg.Adapters {
		switch name {
		case "stdout":
			// ok
		case "webhook":
			hasWebhook = true
		case "sse":
			hasSSE = true
		case "file":
			hasFile = true
		case "exec":
			hasExec = true
		case "pg_table":
			hasPGTable = true
		case "ws":
			hasWS = true
		default:
			return fmt.Errorf("unknown adapter: %q (expected stdout, webhook, sse, file, exec, pg_table, or ws)", name)
		}
	}
	if hasWebhook && cfg.Webhook.URL == "" {
		return fmt.Errorf("webhook adapter requires a URL; use --url or set webhook.url in config")
	}
	if hasFile && cfg.File.Path == "" {
		return fmt.Errorf("file adapter requires a path; use --file-path or set file.path in config")
	}
	if hasExec && cfg.Exec.Command == "" {
		return fmt.Errorf("exec adapter requires a command; use --exec-command or set exec.command in config")
	}
	_ = hasPGTable // validated via config defaults

	logger := slog.Default()

	// Root context: cancelled on SIGINT or SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Create detector.
	var det detector.Detector
	switch cfg.Detector.Type {
	case "listen_notify", "":
		det = listennotify.New(cfg.DatabaseURL, cfg.Channels, cfg.Detector.BackoffBase, cfg.Detector.BackoffCap, logger)
	case "wal":
		det = walreplication.New(cfg.DatabaseURL, cfg.Detector.Publication, cfg.Detector.BackoffBase, cfg.Detector.BackoffCap, cfg.Detector.TxMetadata, cfg.Detector.TxMarkers, logger)
	default:
		return fmt.Errorf("unknown detector type: %q (expected listen_notify or wal)", cfg.Detector.Type)
	}

	// Build pipeline options.
	opts := []pgpipe.Option{
		pgpipe.WithBusBuffer(cfg.Bus.BufferSize),
		pgpipe.WithLogger(logger),
	}

	// Create adapters.
	var sseBroker *sse.Broker
	var wsBroker *ws.Broker
	for _, name := range cfg.Adapters {
		switch name {
		case "stdout":
			opts = append(opts, pgpipe.WithAdapter(stdout.New(nil, logger)))
		case "webhook":
			opts = append(opts, pgpipe.WithAdapter(webhook.New(
				cfg.Webhook.URL,
				cfg.Webhook.Headers,
				cfg.Webhook.SigningKey,
				cfg.Webhook.MaxRetries,
				cfg.Webhook.Timeout,
				cfg.Webhook.BackoffBase,
				cfg.Webhook.BackoffCap,
				logger,
			)))
		case "sse":
			sseBroker = sse.New(cfg.Bus.BufferSize, cfg.SSE.HeartbeatInterval, logger)
			opts = append(opts, pgpipe.WithAdapter(sseBroker))
		case "file":
			opts = append(opts, pgpipe.WithAdapter(fileadapter.New(cfg.File.Path, cfg.File.MaxSize, cfg.File.MaxFiles, logger)))
		case "exec":
			opts = append(opts, pgpipe.WithAdapter(execadapter.New(cfg.Exec.Command, cfg.Exec.BackoffBase, cfg.Exec.BackoffCap, logger)))
		case "pg_table":
			pgTableURL := cfg.PGTable.URL
			if pgTableURL == "" {
				pgTableURL = cfg.DatabaseURL
			}
			opts = append(opts, pgpipe.WithAdapter(pgtable.New(pgTableURL, cfg.PGTable.Table, cfg.PGTable.BackoffBase, cfg.PGTable.BackoffCap, logger)))
		case "ws":
			wsBroker = ws.New(cfg.Bus.BufferSize, cfg.WebSocket.PingInterval, logger)
			opts = append(opts, pgpipe.WithAdapter(wsBroker))
		}
	}

	// Build pipeline.
	p := pgpipe.NewPipeline(det, opts...)

	// Use an errgroup to run the pipeline alongside CLI-specific HTTP servers.
	g, gCtx := errgroup.WithContext(ctx)

	// Run the core pipeline (detector + bus + adapters).
	g.Go(func() error {
		return p.Run(gCtx)
	})

	// If SSE or WS is active, start the HTTP server with SSE/WS + metrics + health.
	if (hasSSE && sseBroker != nil) || (hasWS && wsBroker != nil) {
		httpServer := server.New(sseBroker, wsBroker, cfg.SSE.CORSOrigins, cfg.SSE.ReadTimeout, cfg.SSE.IdleTimeout, p.Health())
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

	// If --metrics-addr is set, start a dedicated metrics/health server.
	if cfg.MetricsAddr != "" {
		metricsServer := server.NewMetricsServer(p.Health())
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

	// Wait for errgroup to finish (happens when context is cancelled).
	err := g.Wait()

	// context.Canceled is expected on clean shutdown.
	if err != nil && gCtx.Err() != nil {
		logger.Info("shutdown complete")
		return nil
	}

	return err
}
