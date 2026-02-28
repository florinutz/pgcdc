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

	// ParsedPayload is a transient cache used by transform.Chain to avoid
	// repeated JSON unmarshal/marshal of the Payload across multiple transforms.
	// It is never serialized and must not be relied upon outside the transform
	// package. When non-nil, it holds a *transform.payloadCache (as any).
	ParsedPayload any `json:"-"`

	// record holds the structured representation of the event's payload.
	// Lazily parsed from Payload on first Record() call, or set directly
	// by NewFromRecord(). When set via SetRecord(), payloadDirty is true
	// and MarshalJSON will recompute Payload from the record.
	record       *Record `json:"-"`
	payloadDirty bool    `json:"-"`
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

// NewFromRecord creates an Event from a structured Record. The Event's Payload
// is lazily computed from the Record on first marshal. The Operation and LSN
// fields are populated from the Record.
func NewFromRecord(channel string, r *Record, source string) (Event, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return Event{}, fmt.Errorf("generate event id: %w", err)
	}
	return Event{
		ID:           id.String(),
		Channel:      channel,
		Operation:    r.Operation.String(),
		Source:       source,
		CreatedAt:    time.Now().UTC(),
		LSN:          r.Position.Uint64(),
		record:       r,
		payloadDirty: true,
	}, nil
}

// Record returns the structured Record for this event. On first call for
// events created via New() (legacy path), the Payload is lazily parsed into
// a Record. Events created via NewFromRecord() return the Record directly.
// Returns nil if the payload cannot be parsed as a structured CDC event.
func (e *Event) Record() *Record {
	if e.record != nil {
		return e.record
	}
	e.record = parseRecordFromEvent(e)
	return e.record
}

// SetRecord replaces the event's structured record and marks the payload
// as dirty. The next MarshalJSON call will recompute Payload from the Record.
func (e *Event) SetRecord(r *Record) {
	e.record = r
	e.payloadDirty = true
}

// InvalidatePayload marks the payload as dirty, forcing recomputation from
// the Record on next MarshalJSON. Call this after modifying the Record's
// Change, Metadata, or Schema fields directly.
func (e *Event) InvalidatePayload() {
	if e.record != nil {
		e.payloadDirty = true
	}
}

// HasRecord reports whether this event has a structured Record (either set
// directly or lazily parsed).
func (e *Event) HasRecord() bool {
	return e.record != nil
}

// eventAlias is used by MarshalJSON to avoid infinite recursion.
type eventAlias Event

// MarshalJSON implements json.Marshaler. When the event has a dirty Record
// (set via NewFromRecord or SetRecord), Payload is recomputed from the Record
// to produce the legacy JSON format. This ensures backward compatibility:
// json.Marshal(ev) produces identical output regardless of whether the event
// was created from a Record or a raw payload.
func (e Event) MarshalJSON() ([]byte, error) {
	if e.payloadDirty && e.record != nil {
		e.Payload = e.record.MarshalCompatPayload()
		e.payloadDirty = false
	}
	return json.Marshal(eventAlias(e))
}

// EnsurePayload materializes the Payload from the Record if dirty. This is
// useful when code needs to access ev.Payload directly without going through
// MarshalJSON (e.g., adapters that send ev.Payload over the wire).
func (e *Event) EnsurePayload() {
	if e.payloadDirty && e.record != nil {
		e.Payload = e.record.MarshalCompatPayload()
		e.payloadDirty = false
	}
}
