//go:build !no_kafkaserver

package cmd

import (
	"fmt"
	"time"

	kafkaserveradapter "github.com/florinutz/pgcdc/adapter/kafkaserver"
	"github.com/florinutz/pgcdc/checkpoint"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "kafkaserver",
		Description: "Kafka wire protocol server (no Kafka cluster needed)",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg

			// Create checkpoint store for offset persistence.
			var cpStore checkpoint.Store
			ksCheckpointDB := cfg.KafkaServer.CheckpointDB
			if ksCheckpointDB == "" {
				ksCheckpointDB = cfg.DatabaseURL
			}
			if ksCheckpointDB != "" {
				var err error
				cpStore, err = checkpoint.NewPGStore(ctx.Ctx, ksCheckpointDB, ctx.Logger)
				if err != nil {
					return registry.AdapterResult{}, fmt.Errorf("create kafkaserver checkpoint store: %w", err)
				}
			}

			return registry.AdapterResult{
				Adapter: kafkaserveradapter.New(
					cfg.KafkaServer.Addr,
					cfg.KafkaServer.PartitionCount,
					cfg.KafkaServer.BufferSize,
					cfg.KafkaServer.SessionTimeout,
					cfg.KafkaServer.KeyColumn,
					cpStore,
					ctx.Logger,
				),
			}, nil
		},
		ViperKeys: [][2]string{
			{"kafkaserver-addr", "kafkaserver.addr"},
			{"kafkaserver-partitions", "kafkaserver.partition_count"},
			{"kafkaserver-buffer-size", "kafkaserver.buffer_size"},
			{"kafkaserver-session-timeout", "kafkaserver.session_timeout"},
			{"kafkaserver-checkpoint-db", "kafkaserver.checkpoint_db"},
			{"kafkaserver-key-column", "kafkaserver.key_column"},
		},
		Spec: []registry.ParamSpec{
			{
				Name:        "kafkaserver-addr",
				Type:        "string",
				Default:     ":9092",
				Description: "Kafka protocol server listen address",
			},
			{
				Name:        "kafkaserver-partitions",
				Type:        "int",
				Default:     8,
				Description: "Number of partitions per topic",
				Validations: []string{"min:1"},
			},
			{
				Name:        "kafkaserver-buffer-size",
				Type:        "int",
				Default:     10000,
				Description: "Ring buffer size per partition",
				Validations: []string{"min:1"},
			},
			{
				Name:        "kafkaserver-session-timeout",
				Type:        "duration",
				Default:     30 * time.Second,
				Description: "Consumer group session timeout",
			},
			{
				Name:        "kafkaserver-key-column",
				Type:        "string",
				Default:     "id",
				Description: "JSON field used as partition key",
			},
			{
				Name:        "kafkaserver-checkpoint-db",
				Type:        "string",
				Description: "PostgreSQL URL for offset storage (default: same as --db)",
			},
		},
	})

	// Kafka server adapter flags.
	f := listenCmd.Flags()
	f.String("kafkaserver-addr", ":9092", "Kafka protocol server listen address")
	f.Int("kafkaserver-partitions", 8, "number of partitions per topic")
	f.Int("kafkaserver-buffer-size", 10000, "ring buffer size per partition")
	f.Duration("kafkaserver-session-timeout", 30*time.Second, "consumer group session timeout")
	f.String("kafkaserver-checkpoint-db", "", "PostgreSQL URL for offset storage (default: same as --db)")
	f.String("kafkaserver-key-column", "id", "JSON field used as partition key")
}
