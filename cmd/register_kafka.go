//go:build !no_kafka

package cmd

import (
	kafkaadapter "github.com/florinutz/pgcdc/adapter/kafka"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "kafka",
		Description: "Kafka topic publish",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			return registry.AdapterResult{
				Adapter: kafkaadapter.New(
					cfg.Kafka.Brokers,
					cfg.Kafka.Topic,
					cfg.Kafka.SASLMechanism,
					cfg.Kafka.SASLUsername,
					cfg.Kafka.SASLPassword,
					cfg.Kafka.TLSCAFile,
					cfg.Kafka.TLS,
					cfg.Kafka.BackoffBase,
					cfg.Kafka.BackoffCap,
					ctx.KafkaEncoder,
					ctx.Logger,
					cfg.Kafka.TransactionalID,
					cfg.Kafka.CBMaxFailures,
					cfg.Kafka.CBResetTimeout,
					cfg.Kafka.RateLimit,
					cfg.Kafka.RateLimitBurst,
				),
			}, nil
		},
		ViperKeys: [][2]string{
			{"kafka-brokers", "kafka.brokers"},
			{"kafka-topic", "kafka.topic"},
			{"kafka-sasl-mechanism", "kafka.sasl_mechanism"},
			{"kafka-sasl-username", "kafka.sasl_username"},
			{"kafka-sasl-password", "kafka.sasl_password"},
			{"kafka-tls", "kafka.tls"},
			{"kafka-tls-ca-file", "kafka.tls_ca_file"},
			{"kafka-transactional-id", "kafka.transactional_id"},
			{"kafka-encoding", "kafka.encoding"},
			// Schema Registry and NATS encoding are shared infrastructure registered
			// in listen.go init() alongside core flags, so they are not listed here.
		},
		Spec: []registry.ParamSpec{
			{
				Name:        "kafka-brokers",
				Type:        "[]string",
				Default:     "localhost:9092",
				Required:    true,
				Description: "Kafka broker addresses",
				Validations: []string{"min:1"},
			},
			{
				Name:        "kafka-topic",
				Type:        "string",
				Description: "Fixed Kafka topic (empty = per-channel mapping, pgcdc:orders -> pgcdc.orders)",
			},
			{
				Name:        "kafka-sasl-mechanism",
				Type:        "string",
				Description: "SASL authentication mechanism",
				Validations: []string{"oneof:plain,scram-sha-256,scram-sha-512"},
			},
			{
				Name:        "kafka-tls",
				Type:        "bool",
				Default:     false,
				Description: "Enable TLS for Kafka connection",
			},
			{
				Name:        "kafka-encoding",
				Type:        "string",
				Default:     "json",
				Description: "Message encoding format",
				Validations: []string{"oneof:json,avro,protobuf"},
			},
		},
	})

	// Kafka adapter flags.
	f := listenCmd.Flags()
	f.StringSlice("kafka-brokers", []string{"localhost:9092"}, "Kafka broker addresses")
	f.String("kafka-topic", "", "fixed Kafka topic (empty = per-channel, pgcdc:orders→pgcdc.orders)")
	f.String("kafka-sasl-mechanism", "", "SASL mechanism: plain, scram-sha-256, scram-sha-512")
	f.String("kafka-sasl-username", "", "SASL username")
	f.String("kafka-sasl-password", "", "SASL password")
	f.Bool("kafka-tls", false, "enable TLS for Kafka connection")
	f.String("kafka-tls-ca-file", "", "CA certificate file for Kafka TLS")
	f.String("kafka-transactional-id", "", "Kafka transactional.id for exactly-once delivery (empty = idempotent only)")
	f.String("kafka-encoding", "json", "Kafka message encoding: json, avro, or protobuf")
}
