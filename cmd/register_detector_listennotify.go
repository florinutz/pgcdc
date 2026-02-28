package cmd

import (
	"github.com/florinutz/pgcdc/detector/listennotify"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterDetector(registry.DetectorEntry{
		Name:        "listen_notify",
		Description: "PostgreSQL LISTEN/NOTIFY",
		Spec: []registry.ParamSpec{
			{
				Name:        "db",
				Type:        "string",
				Required:    true,
				Description: "PostgreSQL connection string",
			},
			{
				Name:        "channel",
				Type:        "[]string",
				Required:    true,
				Description: "PG channels to listen on (repeatable)",
			},
		},
		Create: func(ctx registry.DetectorContext) (registry.DetectorResult, error) {
			cfg := ctx.Cfg
			det := listennotify.New(cfg.DatabaseURL, cfg.Channels, cfg.Detector.BackoffBase, cfg.Detector.BackoffCap, ctx.Logger)
			return registry.DetectorResult{Detector: det}, nil
		},
	})
}
