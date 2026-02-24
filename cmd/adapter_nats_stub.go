//go:build no_nats

package cmd

import (
	"fmt"
	"log/slog"

	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/internal/config"
)

func makeNATSAdapter(_ config.Config, _ *slog.Logger) (adapter.Adapter, error) {
	return nil, fmt.Errorf("nats adapter not available (built with -tags no_nats)")
}
