//go:build !no_redis

package cmd

import (
	"log/slog"

	"github.com/florinutz/pgcdc/adapter"
	redisadapter "github.com/florinutz/pgcdc/adapter/redis"
	"github.com/florinutz/pgcdc/internal/config"
)

func makeRedisAdapter(cfg config.Config, logger *slog.Logger) (adapter.Adapter, error) {
	return redisadapter.New(
		cfg.Redis.URL,
		cfg.Redis.Mode,
		cfg.Redis.KeyPrefix,
		cfg.Redis.IDColumn,
		cfg.Redis.BackoffBase,
		cfg.Redis.BackoffCap,
		logger,
	), nil
}
