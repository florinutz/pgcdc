package transform

import (
	"encoding/json"
	"errors"

	"github.com/florinutz/pgcdc/event"
)

// TransformFunc transforms an event. Return ErrDropEvent to silently skip the
// event. Any other error causes the event to be skipped with an error metric.
type TransformFunc func(event.Event) (event.Event, error)

// ErrDropEvent is a sentinel error returned by filter transforms to silently
// drop an event from the pipeline.
var ErrDropEvent = errors.New("drop event")

// payloadCache is used by Chain to share a parsed payload map across multiple
// transforms, avoiding repeated JSON unmarshal/marshal round-trips. It is stored
// in event.ParsedPayload (typed as any) for the duration of the chain execution.
type payloadCache struct {
	m      map[string]any
	loaded bool // true after first parse attempt
	dirty  bool // true if any mutator modified the map
	notObj bool // true if payload is not a JSON object
}

// getOrParse returns the cached parsed map, parsing from raw on first call.
// Returns nil, false if the payload is empty or not a JSON object.
func (c *payloadCache) getOrParse(raw json.RawMessage) (map[string]any, bool) {
	if c.loaded {
		if c.notObj {
			return nil, false
		}
		return c.m, true
	}
	c.loaded = true
	if len(raw) == 0 {
		c.notObj = true
		return nil, false
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		c.notObj = true
		return nil, false
	}
	c.m = m
	return c.m, true
}

// invalidate resets the cache, forcing a re-parse on next access.
func (c *payloadCache) invalidate() {
	c.m = nil
	c.loaded = false
	c.dirty = false
	c.notObj = false
}

// getCache extracts the payloadCache from an event, or returns nil if the event
// is not being processed inside a Chain.
func getCache(ev *event.Event) *payloadCache {
	if ev.ParsedPayload == nil {
		return nil
	}
	c, _ := ev.ParsedPayload.(*payloadCache)
	return c
}

// Chain composes transforms left-to-right. Short-circuits on error or drop.
// Returns nil if no functions are provided.
//
// Performance: When multiple transforms in the chain access the payload (via
// withPayload or readPayload), Chain unmarshals JSON once on first access,
// shares the parsed map via a payloadCache stored in event.ParsedPayload, and
// remarshals once after all transforms complete. This reduces N unmarshal+marshal
// cycles to 1+1 for a chain of N payload-mutating transforms.
func Chain(fns ...TransformFunc) TransformFunc {
	if len(fns) == 0 {
		return nil
	}
	if len(fns) == 1 {
		return fns[0]
	}
	return func(ev event.Event) (event.Event, error) {
		cache := &payloadCache{}
		ev.ParsedPayload = cache

		var err error
		for _, fn := range fns {
			prevPayload := ev.Payload

			ev, err = fn(ev)
			if err != nil {
				ev.ParsedPayload = nil
				return ev, err
			}

			// If a non-withPayload transform replaced ev.Payload (e.g. Debezium,
			// CloudEvents), invalidate the cache so the next transform re-parses
			// the new payload. withPayload in chain mode does not modify
			// ev.Payload, so this only fires for non-cached transforms.
			if payloadReplaced(prevPayload, ev.Payload) {
				cache.invalidate()
			}

			// Re-attach cache if a non-cacheable transform cleared it.
			if ev.ParsedPayload == nil {
				ev.ParsedPayload = cache
			}
		}

		// Remarshal once if any withPayload transform mutated the cached map.
		if cache.dirty && cache.m != nil {
			raw, merr := json.Marshal(cache.m)
			if merr != nil {
				ev.ParsedPayload = nil
				return ev, merr
			}
			ev.Payload = raw
		}

		ev.ParsedPayload = nil
		return ev, nil
	}
}

// payloadReplaced reports whether the raw payload was replaced by a transform.
// Uses a cheap data-pointer comparison on the underlying byte slice.
func payloadReplaced(before, after json.RawMessage) bool {
	if len(before) == 0 && len(after) == 0 {
		return false
	}
	if len(before) == 0 || len(after) == 0 {
		return true
	}
	return &before[0] != &after[0]
}

// withPayload unmarshals the event payload as a JSON object, calls the mutator,
// and remarshals. Returns the event unchanged if payload is empty or not a JSON
// object (e.g. array, string, number).
//
// When called within a Chain (detected via payloadCache in ev.ParsedPayload),
// the parsed map is cached and reused across subsequent transforms. Marshalling
// is deferred to Chain's completion, so only one JSON round-trip occurs for the
// entire chain.
func withPayload(ev event.Event, mutate func(map[string]any)) (event.Event, error) {
	if len(ev.Payload) == 0 {
		return ev, nil
	}

	if cache := getCache(&ev); cache != nil {
		// Chain mode: use the shared cache.
		m, ok := cache.getOrParse(ev.Payload)
		if !ok {
			return ev, nil
		}
		mutate(m)
		cache.dirty = true
		return ev, nil
	}

	// Standalone path: unmarshal, mutate, remarshal.
	var m map[string]any
	if err := json.Unmarshal(ev.Payload, &m); err != nil {
		return ev, nil
	}

	mutate(m)

	raw, err := json.Marshal(m)
	if err != nil {
		return ev, err
	}
	ev.Payload = raw
	return ev, nil
}

// readPayload returns the parsed payload map for read-only access.
// Benefits from the chain cache when available. Does not mark the cache dirty.
func readPayload(ev event.Event) (map[string]any, bool) {
	if cache := getCache(&ev); cache != nil {
		return cache.getOrParse(ev.Payload)
	}
	if len(ev.Payload) == 0 {
		return nil, false
	}
	var m map[string]any
	if err := json.Unmarshal(ev.Payload, &m); err != nil {
		return nil, false
	}
	return m, true
}

// withRecordData applies a mutator to both Before and After StructuredData
// in an event's Record. If the event has a structured Record, the mutator
// operates directly on the StructuredData (zero JSON parsing). Falls back
// to withPayload for legacy events. The mutator is called once per non-nil
// side (Before/After).
func withRecordData(ev event.Event, mutate func(sd *event.StructuredData)) (event.Event, error) {
	rec := ev.Record()
	if rec == nil {
		return ev, nil
	}
	if rec.Change.Before != nil {
		mutate(rec.Change.Before)
	}
	if rec.Change.After != nil {
		mutate(rec.Change.After)
	}
	ev.InvalidatePayload()
	return ev, nil
}

// readRecordData returns the "primary" StructuredData from an event's Record.
// For most operations: After. For DELETE: Before. Returns nil if no Record
// or no data available.
func readRecordData(ev event.Event) *event.StructuredData {
	rec := ev.Record()
	if rec == nil {
		return nil
	}
	if rec.Change.After != nil {
		return rec.Change.After
	}
	return rec.Change.Before
}
