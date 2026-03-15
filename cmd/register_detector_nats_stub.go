//go:build no_nats

package cmd

import (
	"fmt"

	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterDetector(registry.DetectorEntry{
		Name:        "nats_consumer",
		Description: "NATS JetStream consumer (not available — built with -tags no_nats)",
		Create: func(_ registry.DetectorContext) (registry.DetectorResult, error) {
			return registry.DetectorResult{}, fmt.Errorf("nats_consumer detector not available (built with -tags no_nats)")
		},
	})
}
