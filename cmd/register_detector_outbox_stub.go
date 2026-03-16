//go:build no_outbox

package cmd

import (
	"fmt"

	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterDetector(registry.DetectorEntry{
		Name:        "outbox",
		Description: "Outbox pattern polling (not available — built with -tags no_outbox)",
		Create: func(_ registry.DetectorContext) (registry.DetectorResult, error) {
			return registry.DetectorResult{}, fmt.Errorf("outbox detector not available (built with -tags no_outbox)")
		},
	})
}
