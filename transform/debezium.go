package transform

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/florinutz/pgcdc/event"
)

// DebeziumOption configures the Debezium envelope transform.
type DebeziumOption func(*debeziumConfig)

type debeziumConfig struct {
	connectorName string
	database      string
}

// WithConnectorName sets the "name" field in the Debezium source block.
func WithConnectorName(name string) DebeziumOption {
	return func(c *debeziumConfig) { c.connectorName = name }
}

// WithDatabase sets the "db" field in the Debezium source block.
func WithDatabase(db string) DebeziumOption {
	return func(c *debeziumConfig) { c.database = db }
}

// Debezium returns a transform that rewrites the event payload into a
// Debezium-compatible envelope (before/after/op/source/transaction). The pgcdc
// event envelope (ID, Channel, Operation, etc.) is preserved; only the Payload
// field is replaced.
func Debezium(opts ...DebeziumOption) TransformFunc {
	cfg := debeziumConfig{
		connectorName: "pgcdc",
	}
	for _, o := range opts {
		o(&cfg)
	}

	return func(ev event.Event) (event.Event, error) {
		var before, after any

		// Structured record path: use Record directly without JSON parsing.
		rec := ev.Record()
		if rec != nil && rec.Operation != 0 {
			if rec.Change.Before != nil {
				before = rec.Change.Before.ToMap()
			}
			if rec.Change.After != nil {
				after = rec.Change.After.ToMap()
			}

			// Determine timestamp.
			tsMs := ev.CreatedAt.UnixMilli()
			if ev.Transaction != nil && !ev.Transaction.CommitTime.IsZero() {
				tsMs = ev.Transaction.CommitTime.UnixMilli()
			}

			schemaName := "public"
			if s, ok := rec.Metadata[event.MetaSchema]; ok && s != "" {
				schemaName = s
			}
			tableName := rec.Metadata[event.MetaTable]

			var txID uint32
			if ev.Transaction != nil {
				txID = ev.Transaction.Xid
			}

			source := map[string]any{
				"version":   "pgcdc",
				"connector": "pgcdc",
				"name":      cfg.connectorName,
				"ts_ms":     tsMs,
				"snapshot":  ev.Operation == event.OpSnapshot,
				"db":        cfg.database,
				"schema":    schemaName,
				"table":     tableName,
				"txId":      txID,
				"lsn":       ev.LSN,
			}

			envelope := map[string]any{
				"before": before,
				"after":  after,
				"op":     debeziumOp(ev.Operation),
				"source": source,
			}

			if ev.Transaction != nil {
				envelope["transaction"] = map[string]any{
					"id":                    fmt.Sprintf("%d:%d", ev.Transaction.Xid, ev.LSN),
					"total_order":           ev.Transaction.Seq,
					"data_collection_order": ev.Transaction.Seq,
				}
			}

			raw, err := json.Marshal(envelope)
			if err != nil {
				return ev, fmt.Errorf("marshal debezium envelope: %w", err)
			}
			ev.Payload = raw
			return ev, nil
		}

		// Legacy path: parse payload JSON.
		var payload map[string]any
		var rawPayload any
		hasStructuredPayload := false

		if len(ev.Payload) > 0 {
			if err := json.Unmarshal(ev.Payload, &payload); err != nil {
				if err2 := json.Unmarshal(ev.Payload, &rawPayload); err2 != nil {
					rawPayload = string(ev.Payload)
				}
			} else {
				hasStructuredPayload = true
			}
		}

		switch ev.Operation {
		case "INSERT":
			if hasStructuredPayload {
				after = payload["row"]
			} else {
				after = rawPayload
			}
		case "UPDATE":
			if hasStructuredPayload {
				before = payload["old"]
				after = payload["row"]
			} else {
				after = rawPayload
			}
		case "DELETE":
			if hasStructuredPayload {
				before = payload["row"]
			} else {
				before = rawPayload
			}
		case event.OpSnapshot:
			if hasStructuredPayload {
				after = payload["row"]
			} else {
				after = rawPayload
			}
		default:
			if hasStructuredPayload {
				after = payload
			} else {
				after = rawPayload
			}
		}

		tsMs := ev.CreatedAt.UnixMilli()
		if ev.Transaction != nil && !ev.Transaction.CommitTime.IsZero() {
			tsMs = ev.Transaction.CommitTime.UnixMilli()
		}

		schemaName := "public"
		tableName := ""
		if hasStructuredPayload {
			if s, ok := payload["schema"].(string); ok && s != "" {
				schemaName = s
			}
			if t, ok := payload["table"].(string); ok && t != "" {
				tableName = t
			}
		}

		var txID uint32
		if ev.Transaction != nil {
			txID = ev.Transaction.Xid
		}

		source := map[string]any{
			"version":   "pgcdc",
			"connector": "pgcdc",
			"name":      cfg.connectorName,
			"ts_ms":     tsMs,
			"snapshot":  ev.Operation == event.OpSnapshot,
			"db":        cfg.database,
			"schema":    schemaName,
			"table":     tableName,
			"txId":      txID,
			"lsn":       ev.LSN,
		}

		envelope := map[string]any{
			"before": before,
			"after":  after,
			"op":     debeziumOp(ev.Operation),
			"source": source,
		}

		if ev.Transaction != nil {
			envelope["transaction"] = map[string]any{
				"id":                    fmt.Sprintf("%d:%d", ev.Transaction.Xid, ev.LSN),
				"total_order":           ev.Transaction.Seq,
				"data_collection_order": ev.Transaction.Seq,
			}
		}

		raw, err := json.Marshal(envelope)
		if err != nil {
			return ev, fmt.Errorf("marshal debezium envelope: %w", err)
		}
		ev.Payload = raw
		return ev, nil
	}
}

// debeziumOp maps pgcdc operation names to Debezium operation codes.
func debeziumOp(op string) string {
	switch op {
	case "INSERT":
		return "c"
	case "UPDATE":
		return "u"
	case "DELETE":
		return "d"
	case event.OpSnapshot:
		return "r"
	default:
		if len(op) == 0 {
			return ""
		}
		return strings.ToLower(op[:1])
	}
}
