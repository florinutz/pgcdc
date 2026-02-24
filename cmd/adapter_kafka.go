//go:build !no_kafka

package cmd

import (
	"log/slog"

	"github.com/florinutz/pgcdc/adapter"
	kafkaadapter "github.com/florinutz/pgcdc/adapter/kafka"
	"github.com/florinutz/pgcdc/internal/config"
)

func makeKafkaAdapter(cfg config.Config, logger *slog.Logger) (adapter.Adapter, error) {
	return kafkaadapter.New(
		cfg.Kafka.Brokers,
		cfg.Kafka.Topic,
		cfg.Kafka.SASLMechanism,
		cfg.Kafka.SASLUsername,
		cfg.Kafka.SASLPassword,
		cfg.Kafka.TLSCAFile,
		cfg.Kafka.TLS,
		cfg.Kafka.BackoffBase,
		cfg.Kafka.BackoffCap,
		logger,
	), nil
}
