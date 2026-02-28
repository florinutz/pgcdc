package chain

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/florinutz/pgcdc/event"
)

// Batch is a Link that passes events through unchanged. For full batching
// semantics (accumulate N events, emit one), use BatchEvents at the adapter
// level. This link exists as a placeholder for chain composition and to
// provide the BatchEvents helper.
type Batch struct {
	maxSize int
}

// NewBatch creates a batch link. maxSize is the maximum batch size used
// by BatchEvents.
func NewBatch(maxSize int) *Batch {
	if maxSize <= 0 {
		maxSize = 100
	}
	return &Batch{
		maxSize: maxSize,
	}
}

func (b *Batch) Name() string { return "batch" }

// MaxSize returns the configured maximum batch size.
func (b *Batch) MaxSize() int { return b.maxSize }

// Process passes events through unchanged. Stateful batching requires
// integration at the adapter level via BatchEvents.
func (b *Batch) Process(_ context.Context, ev event.Event) (event.Event, error) {
	return ev, nil
}

// BatchEvents groups a slice of events into a single event with a JSON array payload.
func BatchEvents(events []event.Event) (event.Event, error) {
	if len(events) == 0 {
		return event.Event{}, fmt.Errorf("empty batch")
	}

	payloads := make([]json.RawMessage, len(events))
	for i, ev := range events {
		ev.EnsurePayload()
		payloads[i] = ev.Payload
	}

	data, err := json.Marshal(payloads)
	if err != nil {
		return event.Event{}, fmt.Errorf("marshal batch: %w", err)
	}

	// Use the first event as the base, replace payload with batch.
	result := events[0]
	result.Payload = data
	return result, nil
}
