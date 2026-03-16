//go:build !no_duckdb

package cmd

import (
	"time"

	duckdbadapter "github.com/florinutz/pgcdc/adapter/duckdb"
	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "duckdb",
		Description: "DuckDB in-process analytics database with SQL query endpoint",
		ConfigKey:   "duckdb",
		DefaultConfig: func() any {
			return &config.DuckDBConfig{
				Path:          ":memory:",
				Retention:     1 * time.Hour,
				FlushInterval: 5 * time.Second,
				FlushSize:     1000,
			}
		},
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			return registry.AdapterResult{
				Adapter: duckdbadapter.New(
					cfg.DuckDB.Path,
					cfg.DuckDB.Retention,
					cfg.DuckDB.FlushInterval,
					cfg.DuckDB.FlushSize,
					cfg.DuckDB.QueryToken,
					ctx.Logger,
				),
			}, nil
		},
		ViperKeys: [][2]string{
			{"duckdb-path", "duckdb.path"},
			{"duckdb-retention", "duckdb.retention"},
			{"duckdb-flush-interval", "duckdb.flush_interval"},
			{"duckdb-flush-size", "duckdb.flush_size"},
			{"duckdb-query-token", "duckdb.query_token"},
		},
		Spec: []registry.ParamSpec{
			{Name: "duckdb-path", Type: "string", Default: ":memory:", Description: "DuckDB database path (:memory: for in-memory)"},
			{Name: "duckdb-retention", Type: "duration", Default: "1h", Description: "Event retention duration (TTL cleanup)"},
			{Name: "duckdb-flush-interval", Type: "duration", Default: "5s", Description: "Buffer flush interval"},
			{Name: "duckdb-flush-size", Type: "int", Default: 1000, Description: "Buffer flush size threshold"},
		},
	})

	f := listenCmd.Flags()
	f.String("duckdb-path", ":memory:", "DuckDB database path (:memory: for in-memory)")
	f.Duration("duckdb-retention", 1*time.Hour, "DuckDB event retention duration")
	f.Duration("duckdb-flush-interval", 5*time.Second, "DuckDB buffer flush interval")
	f.Int("duckdb-flush-size", 1000, "DuckDB buffer flush size threshold")
	f.String("duckdb-query-token", "", "Bearer token required for DuckDB /query endpoint (empty = disabled)")
}
