package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"

	pgcdc "github.com/florinutz/pgcdc"
	embeddingadapter "github.com/florinutz/pgcdc/adapter/embedding"
	execadapter "github.com/florinutz/pgcdc/adapter/exec"
	fileadapter "github.com/florinutz/pgcdc/adapter/file"
	"github.com/florinutz/pgcdc/adapter/pgtable"
	"github.com/florinutz/pgcdc/adapter/stdout"
	"github.com/florinutz/pgcdc/adapter/webhook"
	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/snapshot"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"
)

var snapshotCmd = &cobra.Command{
	Use:   "snapshot",
	Short: "Export existing table rows as SNAPSHOT events",
	Long: `Connects to PostgreSQL, reads all rows from the specified table using a
consistent REPEATABLE READ transaction, and streams them as SNAPSHOT events
through the configured adapters.

This is useful for initial data synchronization before switching to live
change capture with 'pgcdc listen'.`,
	RunE: runSnapshot,
}

func init() {
	f := snapshotCmd.Flags()

	f.String("db", "", "PostgreSQL connection string (env: PGCDC_DATABASE_URL)")
	f.String("table", "", "table to snapshot (required)")
	f.String("where", "", "optional WHERE clause to filter rows")
	f.Int("batch-size", 1000, "progress logging interval in rows")
	f.StringSliceP("adapter", "a", []string{"stdout"}, "adapters: stdout, webhook, file, exec, pg_table, embedding (repeatable)")

	// Adapter flags reused from listen.
	f.StringP("url", "u", "", "webhook destination URL")
	f.Int("retries", 5, "webhook max retries")
	f.String("signing-key", "", "HMAC signing key for webhook")
	f.String("file-path", "", "file adapter output path")
	f.Int64("file-max-size", 0, "file rotation size in bytes (default 100MB)")
	f.Int("file-max-files", 0, "number of rotated files to keep (default 5)")
	f.String("exec-command", "", "shell command to pipe events to (via stdin)")
	f.String("pg-table-url", "", "PostgreSQL URL for pg_table adapter (default: same as --db)")
	f.String("pg-table-name", "", "destination table name (default: pgcdc_events)")

	// Embedding adapter flags (read directly to avoid viper collision with listen).
	f.String("embedding-api-url", "", "OpenAI-compatible embedding API URL")
	f.String("embedding-api-key", "", "API key for embedding service")
	f.String("embedding-model", "", "embedding model name (default: text-embedding-3-small)")
	f.StringSlice("embedding-columns", nil, "columns to embed from event payload row")
	f.String("embedding-id-column", "", "source row ID column (default: id)")
	f.String("embedding-table", "", "destination pgvector table (default: pgcdc_embeddings)")
	f.String("embedding-db-url", "", "PostgreSQL URL for pgvector table (default: same as --db)")
	f.Int("embedding-dimension", 0, "vector dimension (default: 1536)")

	// Only bind snapshot-specific keys that don't collide with listen.
	mustBindPFlag("snapshot.table", f.Lookup("table"))
	mustBindPFlag("snapshot.where", f.Lookup("where"))
	mustBindPFlag("snapshot.batch_size", f.Lookup("batch-size"))

	rootCmd.AddCommand(snapshotCmd)
}

func runSnapshot(cmd *cobra.Command, args []string) error {
	cfg := config.Default()
	if err := viper.Unmarshal(&cfg); err != nil {
		return fmt.Errorf("unmarshal config: %w", err)
	}

	// Read flags directly to avoid viper key collisions with listen command.
	if db, _ := cmd.Flags().GetString("db"); db != "" {
		cfg.DatabaseURL = db
	}
	if adapters, _ := cmd.Flags().GetStringSlice("adapter"); len(adapters) > 0 {
		cfg.Adapters = adapters
	}
	if v, _ := cmd.Flags().GetString("url"); v != "" {
		cfg.Webhook.URL = v
	}
	if v, _ := cmd.Flags().GetInt("retries"); cmd.Flags().Changed("retries") {
		cfg.Webhook.MaxRetries = v
	}
	if v, _ := cmd.Flags().GetString("signing-key"); v != "" {
		cfg.Webhook.SigningKey = v
	}
	if v, _ := cmd.Flags().GetString("file-path"); v != "" {
		cfg.File.Path = v
	}
	if v, _ := cmd.Flags().GetInt64("file-max-size"); v > 0 {
		cfg.File.MaxSize = v
	}
	if v, _ := cmd.Flags().GetInt("file-max-files"); v > 0 {
		cfg.File.MaxFiles = v
	}
	if v, _ := cmd.Flags().GetString("exec-command"); v != "" {
		cfg.Exec.Command = v
	}
	if v, _ := cmd.Flags().GetString("pg-table-url"); v != "" {
		cfg.PGTable.URL = v
	}
	if v, _ := cmd.Flags().GetString("pg-table-name"); v != "" {
		cfg.PGTable.Table = v
	}
	if v, _ := cmd.Flags().GetString("embedding-api-url"); v != "" {
		cfg.Embedding.APIURL = v
	}
	if v, _ := cmd.Flags().GetString("embedding-api-key"); v != "" {
		cfg.Embedding.APIKey = v
	}
	if v, _ := cmd.Flags().GetString("embedding-model"); v != "" {
		cfg.Embedding.Model = v
	}
	if v, _ := cmd.Flags().GetStringSlice("embedding-columns"); len(v) > 0 {
		cfg.Embedding.Columns = v
	}
	if v, _ := cmd.Flags().GetString("embedding-id-column"); v != "" {
		cfg.Embedding.IDColumn = v
	}
	if v, _ := cmd.Flags().GetString("embedding-table"); v != "" {
		cfg.Embedding.Table = v
	}
	if v, _ := cmd.Flags().GetString("embedding-db-url"); v != "" {
		cfg.Embedding.DBURL = v
	}
	if v, _ := cmd.Flags().GetInt("embedding-dimension"); v > 0 {
		cfg.Embedding.Dimension = v
	}

	if cfg.DatabaseURL == "" {
		return fmt.Errorf("no database URL specified; use --db, set database_url in config, or export PGCDC_DATABASE_URL")
	}
	if cfg.Snapshot.Table == "" {
		return fmt.Errorf("no table specified; use --table or set snapshot.table in config")
	}

	// Validate adapters (SSE and WS don't make sense for snapshot).
	hasWebhook := false
	hasFile := false
	hasExec := false
	hasPGTable := false
	hasEmbedding := false
	for _, name := range cfg.Adapters {
		switch name {
		case "stdout":
			// ok
		case "webhook":
			hasWebhook = true
		case "file":
			hasFile = true
		case "exec":
			hasExec = true
		case "pg_table":
			hasPGTable = true
		case "embedding":
			hasEmbedding = true
		case "sse", "ws":
			return fmt.Errorf("adapter %q is not supported for snapshot (use stdout, webhook, file, exec, pg_table, or embedding)", name)
		default:
			return fmt.Errorf("unknown adapter: %q", name)
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
	if hasPGTable && cfg.PGTable.URL == "" && cfg.DatabaseURL == "" {
		return fmt.Errorf("pg_table adapter requires a database URL; use --db or --pg-table-url")
	}
	if hasEmbedding && cfg.Embedding.APIURL == "" {
		return fmt.Errorf("embedding adapter requires an API URL; use --embedding-api-url or set embedding.api_url in config")
	}
	if hasEmbedding && len(cfg.Embedding.Columns) == 0 {
		return fmt.Errorf("embedding adapter requires at least one column; use --embedding-columns or set embedding.columns in config")
	}

	logger := slog.Default()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Build adapter options.
	var opts []pgcdc.Option
	opts = append(opts, pgcdc.WithBusBuffer(cfg.Bus.BufferSize))
	opts = append(opts, pgcdc.WithLogger(logger))

	for _, name := range cfg.Adapters {
		switch name {
		case "stdout":
			opts = append(opts, pgcdc.WithAdapter(stdout.New(nil, logger)))
		case "webhook":
			opts = append(opts, pgcdc.WithAdapter(webhook.New(
				cfg.Webhook.URL,
				cfg.Webhook.Headers,
				cfg.Webhook.SigningKey,
				cfg.Webhook.MaxRetries,
				cfg.Webhook.Timeout,
				cfg.Webhook.BackoffBase,
				cfg.Webhook.BackoffCap,
				logger,
			)))
		case "file":
			opts = append(opts, pgcdc.WithAdapter(fileadapter.New(cfg.File.Path, cfg.File.MaxSize, cfg.File.MaxFiles, logger)))
		case "exec":
			opts = append(opts, pgcdc.WithAdapter(execadapter.New(cfg.Exec.Command, cfg.Exec.BackoffBase, cfg.Exec.BackoffCap, logger)))
		case "pg_table":
			pgTableURL := cfg.PGTable.URL
			if pgTableURL == "" {
				pgTableURL = cfg.DatabaseURL
			}
			opts = append(opts, pgcdc.WithAdapter(pgtable.New(pgTableURL, cfg.PGTable.Table, cfg.PGTable.BackoffBase, cfg.PGTable.BackoffCap, logger)))
		case "embedding":
			embDBURL := cfg.Embedding.DBURL
			if embDBURL == "" {
				embDBURL = cfg.DatabaseURL
			}
			opts = append(opts, pgcdc.WithAdapter(embeddingadapter.New(
				cfg.Embedding.APIURL,
				cfg.Embedding.APIKey,
				cfg.Embedding.Model,
				cfg.Embedding.Columns,
				cfg.Embedding.IDColumn,
				embDBURL,
				cfg.Embedding.Table,
				cfg.Embedding.Dimension,
				cfg.Embedding.MaxRetries,
				cfg.Embedding.Timeout,
				cfg.Embedding.BackoffBase,
				cfg.Embedding.BackoffCap,
				logger,
			)))
		}
	}

	// Create a snapshot-only pipeline: snapshot feeds into bus -> adapters.
	snap := snapshot.New(cfg.DatabaseURL, cfg.Snapshot.Table, cfg.Snapshot.Where, cfg.Snapshot.BatchSize, logger)
	p := pgcdc.NewSnapshotPipeline(snap, opts...)

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return p.RunSnapshot(gCtx)
	})

	err := g.Wait()
	if err != nil && gCtx.Err() != nil {
		logger.Info("snapshot complete")
		return nil
	}
	return err
}
