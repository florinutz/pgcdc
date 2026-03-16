//go:build !no_webhookgw

package cmd

import (
	"fmt"
	"strings"

	"github.com/florinutz/pgcdc/detector/webhookgw"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterDetector(registry.DetectorEntry{
		Name:        "webhook-gateway",
		Description: "Inbound webhook gateway (receives HTTP webhooks from external services)",
		RequiresDB:  false,
		ConfigKey:   "webhook_gateway",
		DefaultConfig: func() any {
			return &config.WebhookGatewayConfig{
				MaxBodySize: 1024 * 1024,
			}
		},
		Spec: []registry.ParamSpec{
			{
				Name:        "webhookgw-source",
				Type:        "[]string",
				Description: "Webhook source: name:secret=...,header=...,prefix=... (repeatable)",
			},
			{
				Name:        "webhookgw-max-body",
				Type:        "int",
				Default:     1048576,
				Description: "Maximum request body size in bytes",
			},
		},
		Create: func(ctx registry.DetectorContext) (registry.DetectorResult, error) {
			cfg := ctx.Cfg
			var sources []*webhookgw.Source

			// From config file.
			for name, sc := range cfg.WebhookGateway.Sources {
				prefix := sc.ChannelPrefix
				if prefix == "" {
					prefix = event.ChannelPrefix + name
				}
				sources = append(sources, &webhookgw.Source{
					Name:            name,
					Secret:          sc.Secret,
					SignatureHeader: sc.SignatureHeader,
					ChannelPrefix:   prefix,
				})
			}

			// From CLI flags (--webhookgw-source name:key=val,key=val).
			for _, raw := range cfg.WebhookGateway.CLISources {
				src, err := parseSourceFlag(raw)
				if err != nil {
					return registry.DetectorResult{}, fmt.Errorf("parse --webhookgw-source %q: %w", raw, err)
				}
				sources = append(sources, src)
			}

			if len(sources) == 0 {
				return registry.DetectorResult{}, fmt.Errorf("webhook-gateway detector requires at least one source (--webhookgw-source or webhook_gateway.sources in config)")
			}

			det := webhookgw.New(sources, cfg.WebhookGateway.MaxBodySize, ctx.Logger)
			return registry.DetectorResult{Detector: det}, nil
		},
	})

	// Register CLI flags on the listen command.
	listenCmd.Flags().StringSlice("webhookgw-source", nil, "webhook source: name:secret=...,header=...,prefix=... (repeatable)")
	listenCmd.Flags().Int64("webhookgw-max-body", 1024*1024, "maximum webhook request body size in bytes")
}

// parseSourceFlag parses a --webhookgw-source flag value.
// Format: name:secret=...,header=...,prefix=...
// Example: stripe:secret=whsec_xxx,header=Stripe-Signature
func parseSourceFlag(raw string) (*webhookgw.Source, error) {
	parts := strings.SplitN(raw, ":", 2)
	if len(parts) == 0 || parts[0] == "" {
		return nil, fmt.Errorf("source name is required")
	}

	src := &webhookgw.Source{
		Name: parts[0],
	}

	if len(parts) == 2 && parts[1] != "" {
		for _, kv := range strings.Split(parts[1], ",") {
			pair := strings.SplitN(kv, "=", 2)
			if len(pair) != 2 {
				continue
			}
			switch pair[0] {
			case "secret":
				src.Secret = pair[1]
			case "header":
				src.SignatureHeader = pair[1]
			case "prefix":
				src.ChannelPrefix = pair[1]
			default:
				return nil, fmt.Errorf("unknown source option %q", pair[0])
			}
		}
	}

	if src.ChannelPrefix == "" {
		src.ChannelPrefix = event.ChannelPrefix + src.Name
	}

	return src, nil
}
