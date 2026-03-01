//go:build !no_sqlite

package cmd

import (
	"fmt"
	"time"

	sqlitedetector "github.com/florinutz/pgcdc/detector/sqlite"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterDetector(registry.DetectorEntry{
		Name:        "sqlite",
		Description: "SQLite change tracking (polling-based, local-first CDC)",
		ViperKeys: [][2]string{
			{"sqlite-db", "sqlite.db_path"},
			{"sqlite-poll-interval", "sqlite.poll_interval"},
			{"sqlite-batch-size", "sqlite.batch_size"},
			{"sqlite-keep-processed", "sqlite.keep_processed"},
		},
		Spec: []registry.ParamSpec{
			{
				Name:        "sqlite-db",
				Type:        "string",
				Required:    true,
				Description: "SQLite database file path",
			},
			{
				Name:        "sqlite-poll-interval",
				Type:        "duration",
				Default:     "500ms",
				Description: "Polling interval",
			},
			{
				Name:        "sqlite-batch-size",
				Type:        "int",
				Default:     100,
				Description: "Maximum rows per poll cycle",
				Validations: []string{"min:1"},
			},
			{
				Name:        "sqlite-keep-processed",
				Type:        "bool",
				Default:     false,
				Description: "Keep processed rows (mark instead of delete)",
			},
		},
		Create: func(ctx registry.DetectorContext) (registry.DetectorResult, error) {
			cfg := ctx.Cfg
			if cfg.SQLite.DBPath == "" {
				return registry.DetectorResult{}, fmt.Errorf("--sqlite-db is required for sqlite detector")
			}
			det := sqlitedetector.New(
				cfg.SQLite.DBPath,
				cfg.SQLite.PollInterval,
				cfg.SQLite.BatchSize,
				cfg.SQLite.KeepProcessed,
				ctx.Logger,
			)
			return registry.DetectorResult{Detector: det}, nil
		},
	})

	f := listenCmd.Flags()
	f.String("sqlite-db", "", "SQLite database file path")
	f.Duration("sqlite-poll-interval", 500*time.Millisecond, "SQLite polling interval")
	f.Int("sqlite-batch-size", 100, "SQLite maximum rows per poll cycle")
	f.Bool("sqlite-keep-processed", false, "keep processed SQLite change rows")
}
