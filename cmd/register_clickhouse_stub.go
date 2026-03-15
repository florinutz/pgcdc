//go:build no_clickhouse

package cmd

import (
	"fmt"

	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "clickhouse",
		Description: "ClickHouse analytics database (not available — built with -tags no_clickhouse)",
		Create: func(_ registry.AdapterContext) (registry.AdapterResult, error) {
			return registry.AdapterResult{}, fmt.Errorf("clickhouse adapter not available (built with -tags no_clickhouse)")
		},
	})
}
