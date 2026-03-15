//go:build !no_kafka

package cmd

import (
	"fmt"

	kafkadetector "github.com/florinutz/pgcdc/detector/kafka"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterDetector(registry.DetectorEntry{
		Name:        "kafka_consumer",
		Description: "Kafka topic consumer (consumer group)",
		ViperKeys: [][2]string{
			{"kafka-consumer-topics", "kafka_consumer.topics"},
			{"kafka-consumer-group", "kafka_consumer.group"},
			{"kafka-consumer-offset", "kafka_consumer.offset"},
		},
		Spec: []registry.ParamSpec{
			{
				Name:        "kafka-consumer-topics",
				Type:        "[]string",
				Required:    true,
				Description: "Kafka topics to consume",
			},
			{
				Name:        "kafka-consumer-group",
				Type:        "string",
				Default:     "pgcdc",
				Description: "Consumer group ID",
			},
			{
				Name:        "kafka-consumer-offset",
				Type:        "string",
				Default:     "earliest",
				Description: "Initial offset: earliest or latest",
				Validations: []string{"oneof:earliest,latest"},
			},
		},
		Create: func(ctx registry.DetectorContext) (registry.DetectorResult, error) {
			cfg := ctx.Cfg
			if len(cfg.KafkaConsumer.Topics) == 0 {
				return registry.DetectorResult{}, fmt.Errorf("--kafka-consumer-topics is required for kafka_consumer detector")
			}
			det := kafkadetector.New(
				cfg.Kafka.Brokers,
				cfg.KafkaConsumer.Topics,
				cfg.KafkaConsumer.Group,
				cfg.KafkaConsumer.Offset,
				cfg.Kafka.SASLMechanism,
				cfg.Kafka.SASLUsername,
				cfg.Kafka.SASLPassword,
				cfg.Kafka.TLSCAFile,
				cfg.Kafka.TLS,
				cfg.Detector.BackoffBase,
				cfg.Detector.BackoffCap,
				ctx.Logger,
			)
			return registry.DetectorResult{Detector: det}, nil
		},
	})

	f := listenCmd.Flags()
	f.StringSlice("kafka-consumer-topics", nil, "Kafka topics to consume (repeatable)")
	f.String("kafka-consumer-group", "pgcdc", "Kafka consumer group ID")
	f.String("kafka-consumer-offset", "earliest", "initial offset: earliest or latest")
}
