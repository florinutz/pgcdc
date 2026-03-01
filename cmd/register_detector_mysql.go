package cmd

import (
	"fmt"

	mysqldetector "github.com/florinutz/pgcdc/detector/mysql"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterDetector(registry.DetectorEntry{
		Name:        "mysql",
		Description: "MySQL binlog replication",
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
			det := mysqldetector.New(
				cfg.MySQL.Addr, cfg.MySQL.User, cfg.MySQL.Password,
				cfg.MySQL.ServerID, cfg.MySQL.Tables, cfg.MySQL.UseGTID,
				cfg.MySQL.Flavor, cfg.MySQL.BinlogPrefix,
				cfg.MySQL.BackoffBase, cfg.MySQL.BackoffCap,
				ctx.Logger,
			)
			if ctx.TracerProvider != nil {
				det.SetTracer(ctx.TracerProvider.Tracer("pgcdc"))
			}
			return registry.DetectorResult{Detector: det}, nil
		},
	})
}
