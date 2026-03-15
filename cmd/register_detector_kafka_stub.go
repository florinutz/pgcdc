//go:build no_kafka

package cmd

import (
	"fmt"

	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterDetector(registry.DetectorEntry{
		Name:        "kafka_consumer",
		Description: "Kafka topic consumer (not available — built with -tags no_kafka)",
		Create: func(_ registry.DetectorContext) (registry.DetectorResult, error) {
			return registry.DetectorResult{}, fmt.Errorf("kafka_consumer detector not available (built with -tags no_kafka)")
		},
	})
}
