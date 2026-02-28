package transform

import (
	"fmt"

	"github.com/florinutz/pgcdc/event"
)

// FilterExpression returns a transform that drops events where the predicate
// returns false for the row data map. For structured records, the predicate
// receives the primary StructuredData (After for most ops, Before for DELETE)
// as a map. Non-object payloads pass through. Benefits from Chain's payload
// cache when available.
func FilterExpression(predicate func(map[string]any) bool) TransformFunc {
	return func(ev event.Event) (event.Event, error) {
		if ev.HasRecord() {
			sd := readRecordData(ev)
			if sd == nil {
				return ev, nil
			}
			if !predicate(sd.ToMap()) {
				return ev, ErrDropEvent
			}
			return ev, nil
		}
		if len(ev.Payload) == 0 {
			return ev, nil
		}
		m, ok := readPayload(ev)
		if !ok {
			return ev, nil
		}
		if !predicate(m) {
			return ev, ErrDropEvent
		}
		return ev, nil
	}
}

// FilterField returns a transform that drops events where the payload field
// does not equal the expected value (compared as fmt-printed strings).
func FilterField(field string, expected any) TransformFunc {
	return FilterExpression(func(m map[string]any) bool {
		val, ok := m[field]
		if !ok {
			return false
		}
		return fmt.Sprintf("%v", val) == fmt.Sprintf("%v", expected)
	})
}

// FilterFieldIn returns a transform that drops events where the payload field
// value is not in the allowed set (compared as fmt-printed strings).
func FilterFieldIn(field string, allowed ...any) TransformFunc {
	set := make(map[string]struct{}, len(allowed))
	for _, v := range allowed {
		set[fmt.Sprintf("%v", v)] = struct{}{}
	}
	return FilterExpression(func(m map[string]any) bool {
		val, ok := m[field]
		if !ok {
			return false
		}
		_, found := set[fmt.Sprintf("%v", val)]
		return found
	})
}

// FilterOperation returns a transform that drops events whose Operation is not
// in the allowed set. This operates on the event envelope, not the payload, so
// no JSON parsing is needed.
func FilterOperation(ops ...string) TransformFunc {
	set := make(map[string]struct{}, len(ops))
	for _, op := range ops {
		set[op] = struct{}{}
	}
	return func(ev event.Event) (event.Event, error) {
		if _, ok := set[ev.Operation]; !ok {
			return ev, ErrDropEvent
		}
		return ev, nil
	}
}
