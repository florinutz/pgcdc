//go:build !no_graphql

package cmd

import (
	"time"

	"github.com/florinutz/pgcdc/adapter/graphql"
	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "graphql",
		Description: "GraphQL subscriptions over WebSocket (graphql-transport-ws protocol)",
		ConfigKey:   "graphql",
		DefaultConfig: func() any {
			return &config.GraphQLConfig{
				Path:              "/graphql",
				BufferSize:        256,
				KeepaliveInterval: 15 * time.Second,
				MaxClients:        1000,
			}
		},
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			return registry.AdapterResult{
				Adapter: graphql.New(
					cfg.GraphQL.Path,
					cfg.GraphQL.SchemaAware,
					cfg.GraphQL.BufferSize,
					cfg.GraphQL.KeepaliveInterval,
					cfg.GraphQL.MaxClients,
					nil, // schema store wired by pipeline when --schema-store is enabled
					ctx.Logger,
				),
			}, nil
		},
		ViperKeys: [][2]string{
			{"graphql-path", "graphql.path"},
			{"graphql-schema-aware", "graphql.schema_aware"},
			{"graphql-buffer-size", "graphql.buffer_size"},
			{"graphql-keepalive-interval", "graphql.keepalive_interval"},
			{"graphql-max-clients", "graphql.max_clients"},
		},
		Spec: []registry.ParamSpec{
			{Name: "graphql-path", Type: "string", Default: "/graphql", Description: "WebSocket endpoint path"},
			{Name: "graphql-schema-aware", Type: "bool", Default: false, Description: "Auto-generate types from schema store"},
			{Name: "graphql-buffer-size", Type: "int", Default: 256, Description: "Per-client channel buffer size"},
			{Name: "graphql-keepalive-interval", Type: "duration", Default: "15s", Description: "WebSocket ping interval"},
		},
	})

	f := listenCmd.Flags()
	f.String("graphql-path", "/graphql", "GraphQL WebSocket endpoint path")
	f.Bool("graphql-schema-aware", false, "auto-generate GraphQL types from schema store")
	f.Int("graphql-buffer-size", 256, "per-client GraphQL subscription buffer size")
	f.Duration("graphql-keepalive-interval", 15*time.Second, "GraphQL WebSocket keepalive ping interval")
	f.Int("graphql-max-clients", 1000, "maximum concurrent GraphQL subscription clients")
}
