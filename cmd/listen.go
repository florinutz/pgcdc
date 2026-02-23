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
	"github.com/florinutz/pgpipe/adapter/sse"
	"github.com/florinutz/pgpipe/adapter/stdout"
	"github.com/florinutz/pgpipe/adapter/webhook"
	"github.com/florinutz/pgpipe/detector/listennotify"
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

	mustBindPFlag("channels", f.Lookup("channel"))
	mustBindPFlag("adapters", f.Lookup("adapter"))
	mustBindPFlag("webhook.url", f.Lookup("url"))
	mustBindPFlag("sse.addr", f.Lookup("sse-addr"))
	mustBindPFlag("database_url", f.Lookup("db"))
	mustBindPFlag("webhook.max_retries", f.Lookup("retries"))
	mustBindPFlag("webhook.signing_key", f.Lookup("signing-key"))
	mustBindPFlag("metrics_addr", f.Lookup("metrics-addr"))
}

func runListen(cmd *cobra.Command, args []string) error {
	// Load config with defaults.
	cfg := config.Default()
	if err := viper.Unmarshal(&cfg); err != nil {
		return fmt.Errorf("unmarshal config: %w", err)
	}

	// Validation.
	if len(cfg.Channels) == 0 {
		return fmt.Errorf("no channels specified; use --channel or set channels in config file")
	}
	if cfg.DatabaseURL == "" {
		return fmt.Errorf("no database URL specified; use --db, set database_url in config, or export PGPIPE_DATABASE_URL")
	}

	// Validate adapters and check webhook requirement early.
	hasWebhook := false
	hasSSE := false
	for _, name := range cfg.Adapters {
		switch name {
		case "stdout":
			// ok
		case "webhook":
			hasWebhook = true
		case "sse":
			hasSSE = true
		default:
			return fmt.Errorf("unknown adapter: %q (expected stdout, webhook, or sse)", name)
		}
	}
	if hasWebhook && cfg.Webhook.URL == "" {
		return fmt.Errorf("webhook adapter requires a URL; use --url or set webhook.url in config")
	}

	logger := slog.Default()

	// Root context: cancelled on SIGINT or SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Create detector.
	det := listennotify.New(cfg.DatabaseURL, cfg.Channels, cfg.Detector.BackoffBase, cfg.Detector.BackoffCap, logger)

	// Build pipeline options.
	opts := []pgpipe.Option{
		pgpipe.WithBusBuffer(cfg.Bus.BufferSize),
		pgpipe.WithLogger(logger),
	}

	// Create adapters.
	var sseBroker *sse.Broker
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

	// If SSE is active, start the HTTP server with SSE + metrics + health.
	if hasSSE && sseBroker != nil {
		httpServer := server.New(sseBroker, cfg.SSE.CORSOrigins, cfg.SSE.ReadTimeout, cfg.SSE.IdleTimeout, p.Health())
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
