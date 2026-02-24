//go:build no_redis

package cmd

import (
	"fmt"
	"log/slog"

	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/internal/config"
)

func makeRedisAdapter(_ config.Config, _ *slog.Logger) (adapter.Adapter, error) {
	return nil, fmt.Errorf("redis adapter not available (built with -tags no_redis)")
}
