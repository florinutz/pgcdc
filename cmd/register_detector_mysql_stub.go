//go:build no_mysql

package cmd

import (
	"fmt"

	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterDetector(registry.DetectorEntry{
		Name:        "mysql",
		Description: "MySQL binlog replication (not available — built with -tags no_mysql)",
		Create: func(_ registry.DetectorContext) (registry.DetectorResult, error) {
			return registry.DetectorResult{}, fmt.Errorf("mysql detector not available (built with -tags no_mysql)")
		},
	})
}
