//go:build no_webhookgw

package cmd

import (
	"fmt"

	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterDetector(registry.DetectorEntry{
		Name:        "webhook-gateway",
		Description: "Inbound webhook gateway (not available — built with -tags no_webhookgw)",
		Create: func(_ registry.DetectorContext) (registry.DetectorResult, error) {
			return registry.DetectorResult{}, fmt.Errorf("webhook-gateway detector not available (built with -tags no_webhookgw)")
		},
	})
}
