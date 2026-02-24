//go:build no_iceberg

package cmd

import (
	"fmt"
	"log/slog"

	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/internal/config"
)

func makeIcebergAdapter(_ config.Config, _ *slog.Logger) (adapter.Adapter, error) {
	return nil, fmt.Errorf("iceberg adapter not available (built with -tags no_iceberg)")
}
