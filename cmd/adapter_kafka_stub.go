//go:build no_kafka

package cmd

import (
	"fmt"
	"log/slog"

	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/internal/config"
)

func makeKafkaAdapter(_ config.Config, _ *slog.Logger) (adapter.Adapter, error) {
	return nil, fmt.Errorf("kafka adapter not available (built with -tags no_kafka)")
}
