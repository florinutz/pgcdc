package cmd

import (
	"time"

	embeddingadapter "github.com/florinutz/pgcdc/adapter/embedding"
	execadapter "github.com/florinutz/pgcdc/adapter/exec"
	fileadapter "github.com/florinutz/pgcdc/adapter/file"
	"github.com/florinutz/pgcdc/adapter/middleware"
	"github.com/florinutz/pgcdc/adapter/pgtable"
	searchadapter "github.com/florinutz/pgcdc/adapter/search"
	"github.com/florinutz/pgcdc/adapter/sse"
	"github.com/florinutz/pgcdc/adapter/stdout"
	"github.com/florinutz/pgcdc/adapter/webhook"
	"github.com/florinutz/pgcdc/adapter/ws"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "stdout",
		Description: "JSON-lines to stdout",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			return registry.AdapterResult{
				Adapter: stdout.New(nil, ctx.Logger),
			}, nil
		},
		Spec: []registry.ParamSpec{
			{
				Name:        "adapter",
				Type:        "string",
				Default:     "stdout",
				Description: "Adapter name (stdout requires no additional configuration)",
			},
		},
	})

	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "webhook",
		Description: "HTTP POST to a webhook URL",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			a := webhook.New(
				cfg.Webhook.URL,
				cfg.Webhook.Headers,
				cfg.Webhook.SigningKey,
				cfg.Webhook.MaxRetries,
				cfg.Webhook.Timeout,
				cfg.Webhook.BackoffBase,
				cfg.Webhook.BackoffCap,
				ctx.Logger,
			)
			mwCfg := webhookMiddlewareConfig(cfg)
			return registry.AdapterResult{
				Adapter:          a,
				MiddlewareConfig: &mwCfg,
			}, nil
		},
		ViperKeys: [][2]string{
			{"url", "webhook.url"},
			{"retries", "webhook.max_retries"},
			{"signing-key", "webhook.signing_key"},
			{"webhook-cb-failures", "webhook.cb_max_failures"},
			{"webhook-cb-reset", "webhook.cb_reset_timeout"},
			{"webhook-rate-limit", "webhook.rate_limit"},
			{"webhook-rate-burst", "webhook.rate_limit_burst"},
		},
		Spec: []registry.ParamSpec{
			{
				Name:        "url",
				Type:        "string",
				Required:    true,
				Description: "Webhook destination URL",
			},
			{
				Name:        "retries",
				Type:        "int",
				Default:     5,
				Description: "Maximum number of delivery retries",
				Validations: []string{"min:0", "max:50"},
			},
			{
				Name:        "signing-key",
				Type:        "string",
				Description: "HMAC-SHA256 signing key for X-PGCDC-Signature header",
			},
			{
				Name:        "webhook-cb-failures",
				Type:        "int",
				Default:     0,
				Description: "Circuit breaker max failures before open (0 = disabled)",
				Validations: []string{"min:0"},
			},
			{
				Name:        "webhook-rate-limit",
				Type:        "float64",
				Default:     0,
				Description: "Rate limit in events/second (0 = unlimited)",
				Validations: []string{"min:0"},
			},
		},
	})

	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "sse",
		Description: "Server-Sent Events broker",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			broker := sse.New(cfg.Bus.BufferSize, cfg.SSE.HeartbeatInterval, ctx.Logger)
			return registry.AdapterResult{
				Adapter:   broker,
				SSEBroker: broker,
			}, nil
		},
		ViperKeys: [][2]string{
			{"sse-addr", "sse.addr"},
		},
	})

	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "file",
		Description: "JSON-lines to file with rotation",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			return registry.AdapterResult{
				Adapter: fileadapter.New(cfg.File.Path, cfg.File.MaxSize, cfg.File.MaxFiles, ctx.Logger),
			}, nil
		},
		ViperKeys: [][2]string{
			{"file-path", "file.path"},
			{"file-max-size", "file.max_size"},
			{"file-max-files", "file.max_files"},
		},
	})

	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "exec",
		Description: "JSON-lines to subprocess stdin",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			return registry.AdapterResult{
				Adapter: execadapter.New(cfg.Exec.Command, cfg.Exec.BackoffBase, cfg.Exec.BackoffCap, ctx.Logger),
			}, nil
		},
		ViperKeys: [][2]string{
			{"exec-command", "exec.command"},
		},
	})

	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "pg_table",
		Description: "INSERT into PostgreSQL table",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			pgTableURL := cfg.PGTable.URL
			if pgTableURL == "" {
				pgTableURL = cfg.DatabaseURL
			}
			return registry.AdapterResult{
				Adapter: pgtable.New(pgTableURL, cfg.PGTable.Table, cfg.PGTable.BackoffBase, cfg.PGTable.BackoffCap, ctx.Logger),
			}, nil
		},
		ViperKeys: [][2]string{
			{"pg-table-url", "pg_table.url"},
			{"pg-table-name", "pg_table.table"},
		},
	})

	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "ws",
		Description: "WebSocket broker",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			broker := ws.New(cfg.Bus.BufferSize, cfg.WebSocket.PingInterval, ctx.Logger)
			return registry.AdapterResult{
				Adapter:  broker,
				WSBroker: broker,
			}, nil
		},
		ViperKeys: [][2]string{
			{"ws-ping-interval", "websocket.ping_interval"},
		},
	})

	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "embedding",
		Description: "Embed text columns into pgvector table",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			embDBURL := cfg.Embedding.DBURL
			if embDBURL == "" {
				embDBURL = cfg.DatabaseURL
			}
			mwCfg := middleware.Config{
				CircuitBreaker: cbConfig(cfg.Embedding.CBMaxFailures, cfg.Embedding.CBResetTimeout),
				RateLimit:      rlConfig(cfg.Embedding.RateLimit, cfg.Embedding.RateLimitBurst),
			}
			return registry.AdapterResult{
				Adapter: embeddingadapter.New(
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
					cfg.Embedding.SkipUnchanged,
					ctx.Logger,
				),
				MiddlewareConfig: &mwCfg,
			}, nil
		},
		ViperKeys: [][2]string{
			{"embedding-api-url", "embedding.api_url"},
			{"embedding-api-key", "embedding.api_key"},
			{"embedding-model", "embedding.model"},
			{"embedding-columns", "embedding.columns"},
			{"embedding-id-column", "embedding.id_column"},
			{"embedding-table", "embedding.table"},
			{"embedding-db-url", "embedding.db_url"},
			{"embedding-dimension", "embedding.dimension"},
			{"embedding-skip-unchanged", "embedding.skip_unchanged"},
			{"embedding-cb-failures", "embedding.cb_max_failures"},
			{"embedding-cb-reset", "embedding.cb_reset_timeout"},
			{"embedding-rate-limit", "embedding.rate_limit"},
			{"embedding-rate-burst", "embedding.rate_limit_burst"},
		},
	})

	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "search",
		Description: "Typesense / Meilisearch sync",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			return registry.AdapterResult{
				Adapter: searchadapter.New(
					cfg.Search.Engine,
					cfg.Search.URL,
					cfg.Search.APIKey,
					cfg.Search.Index,
					cfg.Search.IDColumn,
					cfg.Search.BatchSize,
					cfg.Search.BatchInterval,
					cfg.Search.BackoffBase,
					cfg.Search.BackoffCap,
					ctx.Logger,
				),
			}, nil
		},
		ViperKeys: [][2]string{
			{"search-engine", "search.engine"},
			{"search-url", "search.url"},
			{"search-api-key", "search.api_key"},
			{"search-index", "search.index"},
			{"search-id-column", "search.id_column"},
			{"search-batch-size", "search.batch_size"},
			{"search-batch-interval", "search.batch_interval"},
		},
	})

	// Register adapter-specific flags directly on listenCmd.
	// register_core.go runs its init() after listen.go (alphabetical: 'r' > 'l'),
	// so listenCmd is already defined when this runs.
	f := listenCmd.Flags()

	// Webhook flags.
	f.StringP("url", "u", "", "webhook destination URL")
	f.Int("retries", 5, "webhook max retries")
	f.String("signing-key", "", "HMAC signing key for webhook")
	f.Int("webhook-cb-failures", 0, "webhook circuit breaker max failures before open (0 = disabled)")
	f.Duration("webhook-cb-reset", 60*time.Second, "webhook circuit breaker reset timeout")
	f.Float64("webhook-rate-limit", 0, "webhook rate limit in events/second (0 = unlimited)")
	f.Int("webhook-rate-burst", 0, "webhook rate limit burst size")

	// SSE flags.
	f.String("sse-addr", ":8080", "SSE server listen address")

	// File adapter flags.
	f.String("file-path", "", "file adapter output path")
	f.Int64("file-max-size", 0, "file rotation size in bytes (default 100MB)")
	f.Int("file-max-files", 0, "number of rotated files to keep (default 5)")

	// Exec adapter flags.
	f.String("exec-command", "", "shell command to pipe events to (via stdin)")

	// PG table adapter flags.
	f.String("pg-table-url", "", "PostgreSQL URL for pg_table adapter (default: same as --db)")
	f.String("pg-table-name", "", "destination table name (default: pgcdc_events)")

	// WebSocket adapter flags.
	f.Duration("ws-ping-interval", 0, "WebSocket ping interval (default 15s)")

	// Embedding adapter flags.
	f.String("embedding-api-url", "", "OpenAI-compatible embedding API URL")
	f.String("embedding-api-key", "", "API key for embedding service")
	f.String("embedding-model", "", "embedding model name (default: text-embedding-3-small)")
	f.StringSlice("embedding-columns", nil, "columns to embed from event payload row (required for embedding adapter)")
	f.String("embedding-id-column", "", "source row ID column for UPSERT/DELETE (default: id)")
	f.String("embedding-table", "", "destination pgvector table (default: pgcdc_embeddings)")
	f.String("embedding-db-url", "", "PostgreSQL URL for pgvector table (default: same as --db)")
	f.Int("embedding-dimension", 0, "vector dimension (default: 1536)")
	f.Bool("embedding-skip-unchanged", false, "skip embedding when embedding columns haven't changed on UPDATE")
	f.Int("embedding-cb-failures", 0, "embedding circuit breaker max failures (0 = disabled)")
	f.Duration("embedding-cb-reset", 60*time.Second, "embedding circuit breaker reset timeout")
	f.Float64("embedding-rate-limit", 0, "embedding rate limit in events/second (0 = unlimited)")
	f.Int("embedding-rate-burst", 0, "embedding rate limit burst size")

	// Search adapter flags.
	f.String("search-engine", "typesense", "search engine: typesense or meilisearch")
	f.String("search-url", "", "search engine URL")
	f.String("search-api-key", "", "search engine API key")
	f.String("search-index", "", "search index name")
	f.String("search-id-column", "id", "row ID column for search documents")
	f.Int("search-batch-size", 100, "search batch size")
	f.Duration("search-batch-interval", time.Second, "search batch flush interval")
}
