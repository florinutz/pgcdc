//go:build !no_kafka

package cmd

import (
	"log/slog"

	"github.com/florinutz/pgcdc/adapter"
	kafkaadapter "github.com/florinutz/pgcdc/adapter/kafka"
	"github.com/florinutz/pgcdc/encoding"
	"github.com/spf13/cobra"
)

func buildKafkaReplayAdapter(cmd *cobra.Command, logger *slog.Logger) (adapter.Adapter, error) {
	brokers, _ := cmd.Flags().GetStringSlice("kafka-brokers")
	topic, _ := cmd.Flags().GetString("kafka-topic")
	return kafkaadapter.New(kafkaadapter.Config{
		Brokers: brokers,
		Topic:   topic,
		Encoder: encoding.JSONEncoder{},
	}, logger), nil
}
