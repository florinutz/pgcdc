package transform

import (
	"encoding/json"
	"fmt"

	"github.com/florinutz/pgcdc/event"
)

// CloudEventsOption configures the CloudEvents envelope transform.
type CloudEventsOption func(*cloudeventsConfig)

type cloudeventsConfig struct {
	source     string
	typePrefix string
}

// WithSource sets the CloudEvents source URI-reference.
func WithSource(s string) CloudEventsOption {
	return func(c *cloudeventsConfig) { c.source = s }
}

// WithTypePrefix sets the prefix for the CloudEvents type field.
// The final type is "<prefix>.<operation>".
func WithTypePrefix(p string) CloudEventsOption {
	return func(c *cloudeventsConfig) { c.typePrefix = p }
}

// CloudEvents returns a transform that rewrites the event payload into
// CloudEvents structured-mode JSON (spec v1.0). The pgcdc event envelope
// (ID, Channel, Operation, etc.) is preserved; only the Payload field is
// replaced. The original payload becomes the "data" field as-is.
func CloudEvents(opts ...CloudEventsOption) TransformFunc {
	cfg := cloudeventsConfig{
		source:     "/pgcdc",
		typePrefix: "io.pgcdc.change",
	}
	for _, o := range opts {
		o(&cfg)
	}

	return func(ev event.Event) (event.Event, error) {
		// Ensure payload is materialized from Record if present.
		ev.EnsurePayload()

		// Determine timestamp.
		ts := ev.CreatedAt
		if ev.Transaction != nil && !ev.Transaction.CommitTime.IsZero() {
			ts = ev.Transaction.CommitTime
		}

		envelope := map[string]any{
			"specversion":     "1.0",
			"id":              ev.ID,
			"type":            cfg.typePrefix + "." + ev.Operation,
			"source":          cfg.source,
			"subject":         ev.Channel,
			"time":            ts.UTC().Format("2006-01-02T15:04:05Z"),
			"datacontenttype": "application/json",
		}

		// data = original payload preserved as-is.
		if len(ev.Payload) > 0 {
			envelope["data"] = json.RawMessage(ev.Payload)
		} else {
			envelope["data"] = nil
		}

		// Extension attributes — only present when non-zero.
		if ev.LSN > 0 {
			envelope["pgcdclsn"] = ev.LSN
		}
		if ev.Transaction != nil && ev.Transaction.Xid > 0 {
			envelope["pgcdctxid"] = ev.Transaction.Xid
		}

		raw, err := json.Marshal(envelope)
		if err != nil {
			return ev, fmt.Errorf("marshal cloudevents envelope: %w", err)
		}
		ev.Payload = raw
		return ev, nil
	}
}
