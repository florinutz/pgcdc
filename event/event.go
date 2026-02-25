package event

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"go.opentelemetry.io/otel/trace"
)

// Common operation constants.
const (
	OpSnapshot          = "SNAPSHOT"
	OpSnapshotStarted   = "SNAPSHOT_STARTED"
	OpSnapshotCompleted = "SNAPSHOT_COMPLETED"
)

// TransactionInfo contains optional PostgreSQL transaction metadata.
// Only populated by the WAL detector when --tx-metadata is enabled.
type TransactionInfo struct {
	Xid        uint32    `json:"xid"`
	CommitTime time.Time `json:"commit_time"`
	Seq        int       `json:"seq"`
}

type Event struct {
	ID          string            `json:"id"`
	Channel     string            `json:"channel"`
	Operation   string            `json:"operation"`
	Payload     json.RawMessage   `json:"payload"`
	Source      string            `json:"source"`
	CreatedAt   time.Time         `json:"created_at"`
	Transaction *TransactionInfo  `json:"transaction,omitempty"`
	LSN         uint64            `json:"-"` // WAL position; 0 for non-WAL sources and snapshot events
	SpanContext trace.SpanContext `json:"-"` // Trace context; zero value when tracing disabled
}

func New(channel, operation string, payload json.RawMessage, source string) (Event, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return Event{}, fmt.Errorf("generate event id: %w", err)
	}
	return Event{
		ID:        id.String(),
		Channel:   channel,
		Operation: operation,
		Payload:   payload,
		Source:    source,
		CreatedAt: time.Now().UTC(),
	}, nil
}
