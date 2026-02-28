package cmd

import (
	"github.com/florinutz/pgcdc/detector/outbox"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterDetector(registry.DetectorEntry{
		Name:        "outbox",
		Description: "Transactional outbox table polling",
		Spec: []registry.ParamSpec{
			{
				Name:        "db",
				Type:        "string",
				Required:    true,
				Description: "PostgreSQL connection string",
			},
			{
				Name:        "outbox-table",
				Type:        "string",
				Default:     "pgcdc_outbox",
				Description: "Outbox table name",
			},
			{
				Name:        "outbox-poll-interval",
				Type:        "duration",
				Default:     "500ms",
				Description: "Outbox polling interval",
			},
			{
				Name:        "outbox-batch-size",
				Type:        "int",
				Default:     100,
				Description: "Outbox batch size per poll",
				Validations: []string{"min:1"},
			},
			{
				Name:        "outbox-keep-processed",
				Type:        "bool",
				Default:     false,
				Description: "Keep processed outbox rows (set processed_at instead of DELETE)",
			},
		},
		Create: func(ctx registry.DetectorContext) (registry.DetectorResult, error) {
			cfg := ctx.Cfg
			det := outbox.New(
				cfg.DatabaseURL,
				cfg.Outbox.Table,
				cfg.Outbox.PollInterval,
				cfg.Outbox.BatchSize,
				cfg.Outbox.KeepProcessed,
				cfg.Outbox.BackoffBase,
				cfg.Outbox.BackoffCap,
				ctx.Logger,
			)
			return registry.DetectorResult{Detector: det}, nil
		},
	})
}
