//go:build !no_iceberg

package cmd

import (
	"log/slog"

	"github.com/florinutz/pgcdc/adapter"
	icebergadapter "github.com/florinutz/pgcdc/adapter/iceberg"
	"github.com/florinutz/pgcdc/internal/config"
)

func makeIcebergAdapter(cfg config.Config, logger *slog.Logger) (adapter.Adapter, error) {
	return icebergadapter.New(
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
		logger,
	), nil
}
