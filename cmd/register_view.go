//go:build !no_views

package cmd

import (
	"fmt"
	"strings"

	viewadapter "github.com/florinutz/pgcdc/adapter/view"
	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/registry"
	"github.com/florinutz/pgcdc/view"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "view",
		Description: "Streaming SQL view engine (tumbling/sliding/session windows)",
		Spec: []registry.ParamSpec{
			{
				Name:        "view-query",
				Type:        "[]string",
				Description: "Streaming SQL view query: name:query (repeatable, e.g. --view-query 'counts:SELECT COUNT(*) FROM pgcdc_events GROUP BY channel TUMBLING WINDOW 1m')",
			},
		},
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			defs, err := parseViewConfigs(ctx.Cfg.Views)
			if err != nil {
				return registry.AdapterResult{}, err
			}
			if len(defs) == 0 {
				return registry.AdapterResult{}, fmt.Errorf("view adapter requires at least one view definition in views: config")
			}

			engine := view.NewEngine(defs, ctx.Logger)
			return registry.AdapterResult{
				Adapter: viewadapter.New(engine, ctx.Logger),
			}, nil
		},
	})
}

func parseViewConfigs(views []config.ViewConfig) ([]*view.ViewDef, error) {
	var defs []*view.ViewDef
	for _, vc := range views {
		if vc.Name == "" {
			return nil, fmt.Errorf("view definition missing name")
		}
		if vc.Query == "" {
			return nil, fmt.Errorf("view %q missing query", vc.Name)
		}

		if strings.Contains(vc.Query, "pgcdc:_view:") {
			return nil, fmt.Errorf("view %q: WHERE clause must not reference pgcdc:_view: channels (loop prevention)", vc.Name)
		}

		var emit view.EmitMode
		switch strings.ToLower(vc.Emit) {
		case "", "row":
			emit = view.EmitRow
		case "batch":
			emit = view.EmitBatch
		default:
			return nil, fmt.Errorf("view %q: unknown emit mode %q (expected row or batch)", vc.Name, vc.Emit)
		}

		maxGroups := vc.MaxGroups
		if maxGroups <= 0 {
			maxGroups = 100000
		}

		def, err := view.Parse(vc.Name, vc.Query, emit, maxGroups)
		if err != nil {
			return nil, fmt.Errorf("view %q: %w", vc.Name, err)
		}
		defs = append(defs, def)
	}
	return defs, nil
}
