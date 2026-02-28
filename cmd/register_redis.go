//go:build !no_redis

package cmd

import (
	redisadapter "github.com/florinutz/pgcdc/adapter/redis"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "redis",
		Description: "Redis cache invalidation / sync",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			return registry.AdapterResult{
				Adapter: redisadapter.New(
					cfg.Redis.URL,
					cfg.Redis.Mode,
					cfg.Redis.KeyPrefix,
					cfg.Redis.IDColumn,
					cfg.Redis.BackoffBase,
					cfg.Redis.BackoffCap,
					ctx.Logger,
				),
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
