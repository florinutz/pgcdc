//go:build !no_iceberg

package cmd

import (
	icebergadapter "github.com/florinutz/pgcdc/adapter/iceberg"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "iceberg",
		Description: "Apache Iceberg table writes (Hadoop catalog, Parquet)",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			return registry.AdapterResult{
				Adapter: icebergadapter.New(
					cfg.Iceberg.CatalogType,
					cfg.Iceberg.CatalogURI,
					cfg.Iceberg.Warehouse,
					cfg.Iceberg.Namespace,
					cfg.Iceberg.Table,
					cfg.Iceberg.Mode,
					cfg.Iceberg.SchemaMode,
					cfg.Iceberg.PrimaryKeys,
					cfg.Iceberg.FlushInterval,
					cfg.Iceberg.FlushSize,
					cfg.Iceberg.BackoffBase,
					cfg.Iceberg.BackoffCap,
					ctx.Logger,
				),
			}, nil
		},
		ViperKeys: [][2]string{
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
		},
		Spec: []registry.ParamSpec{
			{
				Name:        "iceberg-catalog",
				Type:        "string",
				Default:     "hadoop",
				Description: "Iceberg catalog type: hadoop, rest, sql",
				Validations: []string{"oneof:hadoop,rest,sql"},
			},
			{
				Name:        "iceberg-catalog-uri",
				Type:        "string",
				Description: "REST catalog URL",
			},
			{
				Name:        "iceberg-warehouse",
				Type:        "string",
				Required:    true,
				Description: "Iceberg warehouse path (s3://... or local path)",
			},
			{
				Name:        "iceberg-namespace",
				Type:        "string",
				Default:     "pgcdc",
				Description: "Iceberg namespace (dot-separated)",
			},
			{
				Name:        "iceberg-table",
				Type:        "string",
				Required:    true,
				Description: "Iceberg table name",
			},
			{
				Name:        "iceberg-mode",
				Type:        "string",
				Default:     "append",
				Description: "Iceberg write mode: append or upsert",
				Validations: []string{"oneof:append,upsert"},
			},
			{
				Name:        "iceberg-schema",
				Type:        "string",
				Default:     "raw",
				Description: "Iceberg schema mode: auto or raw",
				Validations: []string{"oneof:auto,raw"},
			},
			{
				Name:        "iceberg-pk",
				Type:        "[]string",
				Description: "Primary key columns for upsert mode (repeatable)",
			},
			{
				Name:        "iceberg-flush-interval",
				Type:        "duration",
				Default:     "1m",
				Description: "Iceberg flush interval (default 1m)",
			},
			{
				Name:        "iceberg-flush-size",
				Type:        "int",
				Default:     10000,
				Description: "Iceberg flush size in events",
				Validations: []string{"min:1"},
			},
		},
	})

	// Iceberg adapter flags.
	f := listenCmd.Flags()
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
}
