//go:build !no_nats

package cmd

import (
	"fmt"

	natsdetector "github.com/florinutz/pgcdc/detector/nats"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterDetector(registry.DetectorEntry{
		Name:        "nats_consumer",
		Description: "NATS JetStream consumer",
		ViperKeys: [][2]string{
			{"nats-consumer-stream", "nats_consumer.stream"},
			{"nats-consumer-subjects", "nats_consumer.subjects"},
			{"nats-consumer-durable", "nats_consumer.durable"},
		},
		Spec: []registry.ParamSpec{
			{Name: "nats-consumer-stream", Type: "string", Default: "pgcdc", Required: true, Description: "JetStream stream name"},
			{Name: "nats-consumer-subjects", Type: "[]string", Description: "Subject filters (default: all stream subjects)"},
			{Name: "nats-consumer-durable", Type: "string", Default: "pgcdc", Description: "Durable consumer name"},
		},
		Create: func(ctx registry.DetectorContext) (registry.DetectorResult, error) {
			cfg := ctx.Cfg
			if cfg.Nats.URL == "" {
				return registry.DetectorResult{}, fmt.Errorf("--nats-url is required for nats_consumer detector")
			}
			det := natsdetector.New(
				cfg.Nats.URL,
				cfg.NatsConsumer.Stream,
				cfg.NatsConsumer.Subjects,
				cfg.NatsConsumer.Durable,
				cfg.Nats.CredFile,
				cfg.Detector.BackoffBase,
				cfg.Detector.BackoffCap,
				ctx.Logger,
			)
			return registry.DetectorResult{Detector: det}, nil
		},
	})

	f := listenCmd.Flags()
	f.String("nats-consumer-stream", "pgcdc", "NATS JetStream stream to consume from")
	f.StringSlice("nats-consumer-subjects", nil, "NATS subject filters (repeatable)")
	f.String("nats-consumer-durable", "pgcdc", "NATS durable consumer name")
}
