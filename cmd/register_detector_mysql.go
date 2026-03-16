//go:build !no_mysql

package cmd

import (
	"fmt"
	"time"

	mysqldetector "github.com/florinutz/pgcdc/detector/mysql"
	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterDetector(registry.DetectorEntry{
		Name:        "mysql",
		Description: "MySQL binlog replication",
		RequiresDB:  false,
		ConfigKey:   "mysql",
		DefaultConfig: func() any {
			return &config.MySQLConfig{
				Flavor:       "mysql",
				BinlogPrefix: "mysql-bin",
				BackoffBase:  5 * time.Second,
				BackoffCap:   60 * time.Second,
			}
		},
		ViperKeys: [][2]string{
			{"mysql-addr", "mysql.addr"},
			{"mysql-user", "mysql.user"},
			{"mysql-password", "mysql.password"},
			{"mysql-server-id", "mysql.server_id"},
			{"mysql-tables", "mysql.tables"},
			{"mysql-gtid", "mysql.use_gtid"},
			{"mysql-flavor", "mysql.flavor"},
			{"mysql-binlog-prefix", "mysql.binlog_prefix"},
		},
		Spec: []registry.ParamSpec{
			{
				Name:        "mysql-addr",
				Type:        "string",
				Required:    true,
				Description: "MySQL address (host:port)",
			},
			{
				Name:        "mysql-user",
				Type:        "string",
				Description: "MySQL user",
			},
			{
				Name:        "mysql-password",
				Type:        "string",
				Description: "MySQL password",
			},
			{
				Name:        "mysql-server-id",
				Type:        "int",
				Required:    true,
				Description: "MySQL server ID for replication (must be > 0)",
				Validations: []string{"min:1"},
			},
			{
				Name:        "mysql-tables",
				Type:        "[]string",
				Description: "MySQL tables to replicate (schema.table format, repeatable)",
			},
			{
				Name:        "mysql-gtid",
				Type:        "bool",
				Default:     false,
				Description: "Use GTID-based replication instead of file+position",
			},
			{
				Name:        "mysql-flavor",
				Type:        "string",
				Default:     "mysql",
				Description: "MySQL flavor: mysql or mariadb",
				Validations: []string{"oneof:mysql,mariadb"},
			},
			{
				Name:        "mysql-binlog-prefix",
				Type:        "string",
				Default:     "mysql-bin",
				Description: "Binlog filename prefix for position decoding",
			},
		},
		Create: func(ctx registry.DetectorContext) (registry.DetectorResult, error) {
			cfg := ctx.Cfg
			if cfg.MySQL.ServerID == 0 {
				return registry.DetectorResult{}, fmt.Errorf("--mysql-server-id is required and must be > 0 for MySQL detector")
			}
			if cfg.MySQL.Addr == "" {
				return registry.DetectorResult{}, fmt.Errorf("--mysql-addr is required for MySQL detector")
			}
			det := mysqldetector.New(mysqldetector.Config{
				Addr:         cfg.MySQL.Addr,
				User:         cfg.MySQL.User,
				Password:     cfg.MySQL.Password,
				ServerID:     cfg.MySQL.ServerID,
				Tables:       cfg.MySQL.Tables,
				UseGTID:      cfg.MySQL.UseGTID,
				Flavor:       cfg.MySQL.Flavor,
				BinlogPrefix: cfg.MySQL.BinlogPrefix,
				BackoffBase:  cfg.MySQL.BackoffBase,
				BackoffCap:   cfg.MySQL.BackoffCap,
			}, ctx.Logger)
			if ctx.TracerProvider != nil {
				det.SetTracer(ctx.TracerProvider.Tracer("pgcdc"))
			}
			return registry.DetectorResult{Detector: det}, nil
		},
	})
}
