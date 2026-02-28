package cmd

import (
	"log/slog"

	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/registry"
	"github.com/florinutz/pgcdc/transform"
)

func init() {
	registry.RegisterTransform(registry.TransformEntry{
		Name:        "drop_columns",
		Description: "Remove specified columns from event payloads",
		Create: func(spec config.TransformSpec) transform.TransformFunc {
			if len(spec.Columns) == 0 {
				return nil
			}
			return transform.DropColumns(spec.Columns...)
		},
	})

	registry.RegisterTransform(registry.TransformEntry{
		Name:        "rename_fields",
		Description: "Rename fields in event payloads",
		Create: func(spec config.TransformSpec) transform.TransformFunc {
			if len(spec.Mapping) == 0 {
				return nil
			}
			return transform.RenameFields(spec.Mapping)
		},
	})

	registry.RegisterTransform(registry.TransformEntry{
		Name:        "mask",
		Description: "Mask sensitive fields (zero/hash/redact)",
		Create: func(spec config.TransformSpec) transform.TransformFunc {
			if len(spec.Fields) == 0 {
				return nil
			}
			fields := make([]transform.MaskField, len(spec.Fields))
			for i, f := range spec.Fields {
				fields[i] = transform.MaskField{
					Field: f.Field,
					Mode:  transform.MaskMode(f.Mode),
				}
			}
			return transform.Mask(fields...)
		},
	})

	registry.RegisterTransform(registry.TransformEntry{
		Name:        "filter",
		Description: "Filter events by operation or field value",
		Create: func(spec config.TransformSpec) transform.TransformFunc {
			if len(spec.Filter.Operations) > 0 {
				return transform.FilterOperation(spec.Filter.Operations...)
			}
			if spec.Filter.Field != "" && len(spec.Filter.In) > 0 {
				vals := make([]any, len(spec.Filter.In))
				for i, v := range spec.Filter.In {
					vals[i] = v
				}
				return transform.FilterFieldIn(spec.Filter.Field, vals...)
			}
			if spec.Filter.Field != "" && spec.Filter.Equals != "" {
				return transform.FilterField(spec.Filter.Field, spec.Filter.Equals)
			}
			return nil
		},
	})

	registry.RegisterTransform(registry.TransformEntry{
		Name:        "debezium",
		Description: "Rewrite payload into Debezium envelope format",
		Create: func(spec config.TransformSpec) transform.TransformFunc {
			var dopts []transform.DebeziumOption
			if spec.Debezium.ConnectorName != "" {
				dopts = append(dopts, transform.WithConnectorName(spec.Debezium.ConnectorName))
			}
			if spec.Debezium.Database != "" {
				dopts = append(dopts, transform.WithDatabase(spec.Debezium.Database))
			}
			return transform.Debezium(dopts...)
		},
	})

	registry.RegisterTransform(registry.TransformEntry{
		Name:        "cloudevents",
		Description: "Rewrite payload into CloudEvents v1.0 structured-mode JSON",
		Create: func(spec config.TransformSpec) transform.TransformFunc {
			var copts []transform.CloudEventsOption
			if spec.CloudEvents.Source != "" {
				copts = append(copts, transform.WithSource(spec.CloudEvents.Source))
			}
			if spec.CloudEvents.TypePrefix != "" {
				copts = append(copts, transform.WithTypePrefix(spec.CloudEvents.TypePrefix))
			}
			return transform.CloudEvents(copts...)
		},
	})

	registry.RegisterTransform(registry.TransformEntry{
		Name:        "cel_filter",
		Description: "Filter events using a CEL expression",
		Create: func(spec config.TransformSpec) transform.TransformFunc {
			if spec.Expression == "" {
				return nil
			}
			fn, err := transform.FilterCEL(spec.Expression)
			if err != nil {
				slog.Warn("invalid cel_filter expression, skipping", "error", err)
				return nil
			}
			return fn
		},
	})
}
