//go:build no_mongodb

package cmd

import (
	"fmt"

	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterDetector(registry.DetectorEntry{
		Name:        "mongodb",
		Description: "MongoDB Change Streams (not available — built with -tags no_mongodb)",
		Create: func(_ registry.DetectorContext) (registry.DetectorResult, error) {
			return registry.DetectorResult{}, fmt.Errorf("mongodb detector not available (built with -tags no_mongodb)")
		},
	})
}
