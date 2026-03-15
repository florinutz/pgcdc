//go:build !no_clickhouse

package cmd

import (
	"strings"

	clickhouseadapter "github.com/florinutz/pgcdc/adapter/clickhouse"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "clickhouse",
		Description: "ClickHouse analytics database (batch INSERT via native protocol)",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			// Merge CLI --clickhouse-setting flags into config settings map.
			settings := cfg.ClickHouse.Settings
			if settingFlags, _ := listenCmd.Flags().GetStringSlice("clickhouse-setting"); len(settingFlags) > 0 {
				if settings == nil {
					settings = make(map[string]string, len(settingFlags))
				}
				for _, s := range settingFlags {
					parts := strings.SplitN(s, "=", 2)
					if len(parts) == 2 {
						settings[parts[0]] = parts[1]
					}
				}
			}
			return registry.AdapterResult{
				Adapter: clickhouseadapter.New(
					cfg.ClickHouse.DSN,
					cfg.ClickHouse.Table,
					cfg.ClickHouse.AutoCreate,
					cfg.ClickHouse.AsyncInsert,
					settings,
					cfg.ClickHouse.BatchSize,
					cfg.ClickHouse.FlushInterval,
					cfg.ClickHouse.BackoffBase,
					cfg.ClickHouse.BackoffCap,
					ctx.Logger,
				),
			}, nil
		},
		ViperKeys: [][2]string{
			{"clickhouse-dsn", "clickhouse.dsn"},
			{"clickhouse-table", "clickhouse.table"},
			{"clickhouse-auto-create", "clickhouse.auto_create"},
			{"clickhouse-async-insert", "clickhouse.async_insert"},
			{"clickhouse-batch-size", "clickhouse.batch_size"},
			{"clickhouse-flush-interval", "clickhouse.flush_interval"},
		},
		Spec: []registry.ParamSpec{
			{
				Name:        "clickhouse-dsn",
				Type:        "string",
				Required:    true,
				Description: "ClickHouse connection DSN",
			},
			{
				Name:        "clickhouse-table",
				Type:        "string",
				Default:     "pgcdc_events",
				Description: "Target table name",
			},
			{
				Name:        "clickhouse-auto-create",
				Type:        "bool",
				Default:     false,
				Description: "Auto-create table on startup",
			},
			{
				Name:        "clickhouse-async-insert",
				Type:        "bool",
				Default:     false,
				Description: "Use ClickHouse async inserts for higher throughput",
			},
			{
				Name:        "clickhouse-setting",
				Type:        "[]string",
				Description: "ClickHouse session settings (key=value, repeatable)",
			},
			{
				Name:        "clickhouse-batch-size",
				Type:        "int",
				Default:     10000,
				Description: "Batch size for INSERT",
				Validations: []string{"min:1"},
			},
			{
				Name:        "clickhouse-flush-interval",
				Type:        "duration",
				Default:     "1s",
				Description: "Flush interval",
			},
		},
	})

	// ClickHouse adapter flags.
	f := listenCmd.Flags()
	f.String("clickhouse-dsn", "", "ClickHouse connection DSN (e.g. clickhouse://localhost:9000/default)")
	f.String("clickhouse-table", "pgcdc_events", "ClickHouse target table name")
	f.Bool("clickhouse-auto-create", false, "auto-create ClickHouse table on startup")
	f.Bool("clickhouse-async-insert", false, "use ClickHouse async inserts for higher throughput")
	f.StringSlice("clickhouse-setting", nil, "ClickHouse session setting (key=value, repeatable)")
	f.Int("clickhouse-batch-size", 0, "ClickHouse batch size (default 10000)")
	f.Duration("clickhouse-flush-interval", 0, "ClickHouse flush interval (default 1s)")
}
