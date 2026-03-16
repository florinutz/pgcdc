package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"
	"time"

	pgcdc "github.com/florinutz/pgcdc"
	embeddingadapter "github.com/florinutz/pgcdc/adapter/embedding"
	execadapter "github.com/florinutz/pgcdc/adapter/exec"
	fileadapter "github.com/florinutz/pgcdc/adapter/file"
	icebergadapter "github.com/florinutz/pgcdc/adapter/iceberg"
	natsadapter "github.com/florinutz/pgcdc/adapter/nats"
	"github.com/florinutz/pgcdc/adapter/pgtable"
	redisadapter "github.com/florinutz/pgcdc/adapter/redis"
	searchadapter "github.com/florinutz/pgcdc/adapter/search"
	"github.com/florinutz/pgcdc/adapter/stdout"
	"github.com/florinutz/pgcdc/adapter/webhook"
	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/snapshot"
	"github.com/florinutz/pgcdc/transform"
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
	PreRunE: bindSnapshotFlags,
	RunE:    runSnapshot,
}

func init() {
	f := snapshotCmd.Flags()

	f.String("db", "", "PostgreSQL connection string (env: PGCDC_DATABASE_URL)")
	f.String("table", "", "table to snapshot (required)")
	f.String("where", "", "optional WHERE clause to filter rows")
	f.Int("batch-size", 1000, "progress logging interval in rows")
	f.StringSliceP("adapter", "a", []string{"stdout"}, "adapters: stdout, webhook, file, exec, pg_table, embedding, iceberg (repeatable)")

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

	// Iceberg adapter flags (read directly to avoid viper collision with listen).
	f.String("iceberg-catalog", "hadoop", "Iceberg catalog type: hadoop, rest, sql")
	f.String("iceberg-catalog-uri", "", "REST catalog URL")
	f.String("iceberg-warehouse", "", "Iceberg warehouse path (s3://... or local path)")
	f.String("iceberg-namespace", "pgcdc", "Iceberg namespace (dot-separated)")
	f.String("iceberg-table", "", "Iceberg table name")
	f.String("iceberg-mode", "append", "Iceberg write mode: append or upsert")
	f.String("iceberg-schema", "raw", "Iceberg schema mode: auto or raw")
	f.StringSlice("iceberg-pk", nil, "primary key columns for upsert mode (repeatable)")
	f.Duration("iceberg-flush-interval", 0, "Iceberg flush interval (default 1m)")
	f.Int("iceberg-flush-size", 0, "Iceberg flush size in events (default 10000)")

	// NATS adapter flags.
	f.String("nats-url", "nats://localhost:4222", "NATS server URL")
	f.String("nats-subject", "pgcdc", "NATS subject prefix")
	f.String("nats-stream", "pgcdc", "NATS JetStream stream name")
	f.String("nats-cred-file", "", "NATS credentials file")
	f.Duration("nats-max-age", 0, "NATS stream max message age (default 24h)")

	// Search adapter flags.
	f.String("search-engine", "typesense", "search engine: typesense or meilisearch")
	f.String("search-url", "", "search engine URL")
	f.String("search-api-key", "", "search engine API key")
	f.String("search-index", "", "search index name")
	f.String("search-id-column", "id", "row ID column for search documents")
	f.Int("search-batch-size", 100, "search batch size")
	f.Duration("search-batch-interval", time.Second, "search batch flush interval")

	// Redis adapter flags.
	f.String("redis-url", "", "Redis URL")
	f.String("redis-mode", "invalidate", "Redis mode: invalidate or sync")
	f.String("redis-key-prefix", "", "Redis key prefix")
	f.String("redis-id-column", "id", "row ID column for Redis keys")

	// Transform flags (read directly, not viper-bound).
	f.StringSlice("drop-columns", nil, "global: drop these columns from event payloads (repeatable)")
	f.StringSlice("filter-operations", nil, "global: only pass events with these operations (e.g. INSERT,UPDATE)")

	rootCmd.AddCommand(snapshotCmd)
}

// bindSnapshotFlags binds all snapshot command flags to viper keys. Called in
// PreRunE so only the active command's bindings are established.
func bindSnapshotFlags(cmd *cobra.Command, _ []string) error {
	return bindFlagsToViper(cmd.Flags(), [][2]string{
		// Core.
		{"db", "database_url"},
		{"adapter", "adapters"},

		// Snapshot.
		{"table", "snapshot.table"},
		{"where", "snapshot.where"},
		{"batch-size", "snapshot.batch_size"},

		// Webhook.
		{"url", "webhook.url"},
		{"retries", "webhook.max_retries"},
		{"signing-key", "webhook.signing_key"},

		// File.
		{"file-path", "file.path"},
		{"file-max-size", "file.max_size"},
		{"file-max-files", "file.max_files"},

		// Exec.
		{"exec-command", "exec.command"},

		// PG table.
		{"pg-table-url", "pg_table.url"},
		{"pg-table-name", "pg_table.table"},

		// Embedding.
		{"embedding-api-url", "embedding.api_url"},
		{"embedding-api-key", "embedding.api_key"},
		{"embedding-model", "embedding.model"},
		{"embedding-columns", "embedding.columns"},
		{"embedding-id-column", "embedding.id_column"},
		{"embedding-table", "embedding.table"},
		{"embedding-db-url", "embedding.db_url"},
		{"embedding-dimension", "embedding.dimension"},

		// Iceberg.
		{"iceberg-catalog", "iceberg.catalog_type"},
		{"iceberg-catalog-uri", "iceberg.catalog_uri"},
		{"iceberg-warehouse", "iceberg.warehouse"},
		{"iceberg-namespace", "iceberg.namespace"},
		{"iceberg-table", "iceberg.table"},
		{"iceberg-mode", "iceberg.mode"},
		{"iceberg-schema", "iceberg.schema_mode"},
		{"iceberg-pk", "iceberg.primary_keys"},
		{"iceberg-flush-interval", "iceberg.flush_interval"},
		{"iceberg-flush-size", "iceberg.flush_size"},

		// NATS.
		{"nats-url", "nats.url"},
		{"nats-subject", "nats.subject"},
		{"nats-stream", "nats.stream"},
		{"nats-cred-file", "nats.cred_file"},
		{"nats-max-age", "nats.max_age"},

		// Search.
		{"search-engine", "search.engine"},
		{"search-url", "search.url"},
		{"search-api-key", "search.api_key"},
		{"search-index", "search.index"},
		{"search-id-column", "search.id_column"},
		{"search-batch-size", "search.batch_size"},
		{"search-batch-interval", "search.batch_interval"},

		// Redis.
		{"redis-url", "redis.url"},
		{"redis-mode", "redis.mode"},
		{"redis-key-prefix", "redis.key_prefix"},
		{"redis-id-column", "redis.id_column"},
	})
}

func runSnapshot(cmd *cobra.Command, args []string) error {
	cfg := config.Default()
	if err := viper.Unmarshal(&cfg); err != nil {
		return fmt.Errorf("unmarshal config: %w", err)
	}

	if cfg.DatabaseURL == "" {
		return fmt.Errorf("no database URL specified; use --db, set database_url in config, or export PGCDC_DATABASE_URL")
	}
	if cfg.Snapshot.Table == "" {
		return fmt.Errorf("no table specified; use --table or set snapshot.table in config")
	}

	// Validate adapters (SSE and WS don't make sense for snapshot).
	adapterSet := make(map[string]bool, len(cfg.Adapters))
	for _, name := range cfg.Adapters {
		switch name {
		case "stdout", "webhook", "file", "exec", "pg_table", "embedding", "iceberg", "nats", "search", "redis":
			adapterSet[name] = true
		case "sse", "ws", "grpc":
			return fmt.Errorf("adapter %q is not supported for snapshot (use stdout, webhook, file, exec, pg_table, embedding, iceberg, nats, search, or redis)", name)
		default:
			return fmt.Errorf("unknown adapter: %q", name)
		}
	}
	if adapterSet["webhook"] && cfg.Webhook.URL == "" {
		return fmt.Errorf("webhook adapter requires a URL; use --url or set webhook.url in config")
	}
	if adapterSet["file"] && cfg.File.Path == "" {
		return fmt.Errorf("file adapter requires a path; use --file-path or set file.path in config")
	}
	if adapterSet["exec"] && cfg.Exec.Command == "" {
		return fmt.Errorf("exec adapter requires a command; use --exec-command or set exec.command in config")
	}
	if adapterSet["pg_table"] && cfg.PGTable.URL == "" && cfg.DatabaseURL == "" {
		return fmt.Errorf("pg_table adapter requires a database URL; use --db or --pg-table-url")
	}
	if adapterSet["embedding"] && cfg.Embedding.APIURL == "" {
		return fmt.Errorf("embedding adapter requires an API URL; use --embedding-api-url or set embedding.api_url in config")
	}
	if adapterSet["embedding"] && len(cfg.Embedding.Columns) == 0 {
		return fmt.Errorf("embedding adapter requires at least one column; use --embedding-columns or set embedding.columns in config")
	}
	if adapterSet["iceberg"] && cfg.Iceberg.Warehouse == "" {
		return fmt.Errorf("iceberg adapter requires a warehouse; use --iceberg-warehouse or set iceberg.warehouse in config")
	}
	if adapterSet["iceberg"] && cfg.Iceberg.Table == "" {
		return fmt.Errorf("iceberg adapter requires a table name; use --iceberg-table or set iceberg.table in config")
	}
	if adapterSet["search"] && cfg.Search.URL == "" {
		return fmt.Errorf("search adapter requires a URL; use --search-url")
	}
	if adapterSet["search"] && cfg.Search.Index == "" {
		return fmt.Errorf("search adapter requires an index; use --search-index")
	}
	if adapterSet["redis"] && cfg.Redis.URL == "" {
		return fmt.Errorf("redis adapter requires a URL; use --redis-url")
	}
	if adapterSet["nats"] && cfg.Nats.URL == "" {
		return fmt.Errorf("nats adapter requires a URL; use --nats-url or set nats.url in config")
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
			opts = append(opts, pgcdc.WithAdapter(webhook.New(webhook.Config{
				URL:         cfg.Webhook.URL,
				Headers:     cfg.Webhook.Headers,
				SigningKey:  cfg.Webhook.SigningKey,
				MaxRetries:  cfg.Webhook.MaxRetries,
				Timeout:     cfg.Webhook.Timeout,
				BackoffBase: cfg.Webhook.BackoffBase,
				BackoffCap:  cfg.Webhook.BackoffCap,
			}, logger)))
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
			opts = append(opts, pgcdc.WithAdapter(embeddingadapter.New(embeddingadapter.Config{
				APIURL:        cfg.Embedding.APIURL,
				APIKey:        cfg.Embedding.APIKey,
				Model:         cfg.Embedding.Model,
				Columns:       cfg.Embedding.Columns,
				IDColumn:      cfg.Embedding.IDColumn,
				DBURL:         embDBURL,
				Table:         cfg.Embedding.Table,
				Dimension:     cfg.Embedding.Dimension,
				MaxRetries:    cfg.Embedding.MaxRetries,
				Timeout:       cfg.Embedding.Timeout,
				BackoffBase:   cfg.Embedding.BackoffBase,
				BackoffCap:    cfg.Embedding.BackoffCap,
				SkipUnchanged: cfg.Embedding.SkipUnchanged,
			}, logger)))
		case "iceberg":
			opts = append(opts, pgcdc.WithAdapter(icebergadapter.New(icebergadapter.Config{
				CatalogType:   cfg.Iceberg.CatalogType,
				CatalogURI:    cfg.Iceberg.CatalogURI,
				Warehouse:     cfg.Iceberg.Warehouse,
				Namespace:     cfg.Iceberg.Namespace,
				TableName:     cfg.Iceberg.Table,
				Mode:          cfg.Iceberg.Mode,
				SchemaMode:    cfg.Iceberg.SchemaMode,
				PrimaryKeys:   cfg.Iceberg.PrimaryKeys,
				FlushInterval: cfg.Iceberg.FlushInterval,
				FlushSize:     cfg.Iceberg.FlushSize,
				BackoffBase:   cfg.Iceberg.BackoffBase,
				BackoffCap:    cfg.Iceberg.BackoffCap,
			}, logger)))
		case "nats":
			opts = append(opts, pgcdc.WithAdapter(natsadapter.New(natsadapter.Config{
				URL:           cfg.Nats.URL,
				SubjectPrefix: cfg.Nats.Subject,
				StreamName:    cfg.Nats.Stream,
				CredFile:      cfg.Nats.CredFile,
				MaxAge:        cfg.Nats.MaxAge,
				BackoffBase:   cfg.Nats.BackoffBase,
				BackoffCap:    cfg.Nats.BackoffCap,
			}, logger)))
		case "search":
			opts = append(opts, pgcdc.WithAdapter(searchadapter.New(searchadapter.Config{
				Engine:        cfg.Search.Engine,
				URL:           cfg.Search.URL,
				APIKey:        cfg.Search.APIKey,
				Index:         cfg.Search.Index,
				IDColumn:      cfg.Search.IDColumn,
				BatchSize:     cfg.Search.BatchSize,
				BatchInterval: cfg.Search.BatchInterval,
				BackoffBase:   cfg.Search.BackoffBase,
				BackoffCap:    cfg.Search.BackoffCap,
			}, logger)))
		case "redis":
			opts = append(opts, pgcdc.WithAdapter(redisadapter.New(redisadapter.Config{
				URL:         cfg.Redis.URL,
				Mode:        cfg.Redis.Mode,
				KeyPrefix:   cfg.Redis.KeyPrefix,
				IDColumn:    cfg.Redis.IDColumn,
				BackoffBase: cfg.Redis.BackoffBase,
				BackoffCap:  cfg.Redis.BackoffCap,
			}, logger)))
		}
	}

	// Build transform options from CLI flags and config.
	opts = append(opts, buildSnapshotTransformOpts(cfg, cmd)...)

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

// buildSnapshotTransformOpts parses CLI flags and config to produce pipeline
// transform options for the snapshot command.
func buildSnapshotTransformOpts(cfg config.Config, cmd *cobra.Command) []pgcdc.Option {
	var opts []pgcdc.Option

	// CLI flag shortcuts → global transforms.
	if cols, _ := cmd.Flags().GetStringSlice("drop-columns"); len(cols) > 0 {
		opts = append(opts, pgcdc.WithTransform(transform.DropColumns(cols...)))
	}
	if ops, _ := cmd.Flags().GetStringSlice("filter-operations"); len(ops) > 0 {
		opts = append(opts, pgcdc.WithTransform(transform.FilterOperation(ops...)))
	}

	// Config file: global transforms.
	for _, spec := range cfg.Transforms.Global {
		if fn := specToTransform(spec); fn != nil {
			opts = append(opts, pgcdc.WithTransform(fn))
		}
	}

	// Config file: per-adapter transforms.
	for adapterName, specs := range cfg.Transforms.Adapter {
		for _, spec := range specs {
			if fn := specToTransform(spec); fn != nil {
				opts = append(opts, pgcdc.WithAdapterTransform(adapterName, fn))
			}
		}
	}

	return opts
}
