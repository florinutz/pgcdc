//go:build !no_redis

package cmd

import (
	"time"

	redisadapter "github.com/florinutz/pgcdc/adapter/redis"
	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "redis",
		Description: "Redis cache invalidation / sync",
		ConfigKey:   "redis",
		DefaultConfig: func() any {
			return &config.RedisConfig{
				Mode:        "invalidate",
				IDColumn:    "id",
				BackoffBase: 1 * time.Second,
				BackoffCap:  30 * time.Second,
			}
		},
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			return registry.AdapterResult{
				Adapter: redisadapter.New(redisadapter.Config{
					URL:         cfg.Redis.URL,
					Mode:        cfg.Redis.Mode,
					KeyPrefix:   cfg.Redis.KeyPrefix,
					IDColumn:    cfg.Redis.IDColumn,
					BackoffBase: cfg.Redis.BackoffBase,
					BackoffCap:  cfg.Redis.BackoffCap,
				}, ctx.Logger),
			}, nil
		},
		ViperKeys: [][2]string{
			{"redis-url", "redis.url"},
			{"redis-mode", "redis.mode"},
			{"redis-key-prefix", "redis.key_prefix"},
			{"redis-id-column", "redis.id_column"},
		},
		Spec: []registry.ParamSpec{
			{
				Name:        "redis-url",
				Type:        "string",
				Required:    true,
				Description: "Redis URL (e.g. redis://localhost:6379)",
			},
			{
				Name:        "redis-mode",
				Type:        "string",
				Default:     "invalidate",
				Description: "Redis mode",
				Validations: []string{"oneof:invalidate,sync"},
			},
			{
				Name:        "redis-key-prefix",
				Type:        "string",
				Description: "Redis key prefix (e.g. orders:)",
			},
			{
				Name:        "redis-id-column",
				Type:        "string",
				Default:     "id",
				Description: "Row ID column for Redis keys",
			},
		},
	})

	// Redis adapter flags.
	f := listenCmd.Flags()
	f.String("redis-url", "", "Redis URL (e.g. redis://localhost:6379)")
	f.String("redis-mode", "invalidate", "Redis mode: invalidate or sync")
	f.String("redis-key-prefix", "", "Redis key prefix (e.g. orders:)")
	f.String("redis-id-column", "id", "row ID column for Redis keys")
}
