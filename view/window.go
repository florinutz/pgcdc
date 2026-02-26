package view

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/metrics"
)

// groupState holds the aggregation state for one group key.
type groupState struct {
	key         string         // serialized group key
	keyValues   map[string]any // group-by field -> value
	aggregators []Aggregator   // one per SELECT aggregate item
}

// closedWindow holds a recently closed window's state for late event handling.
type closedWindow struct {
	groups   map[string]*groupState
	start    time.Time
	end      time.Time
	closedAt time.Time
	reEmit   bool // true if a late event updated this window
}

// TumblingWindow manages a single tumbling window for one ViewDef.
type TumblingWindow struct {
	def    *ViewDef
	logger *slog.Logger

	mu          sync.Mutex
	groups      map[string]*groupState
	windowStart time.Time

	// aggItemIndices maps SELECT item index to aggregate index.
	// Only items with Aggregate != nil are tracked.
	aggItemIndices []int // len == len(def.SelectItems), -1 for non-aggregate items

	// closedWindows holds recently closed windows for late event handling.
	// Only populated when AllowedLateness > 0.
	closedWindows []*closedWindow
}

// NewTumblingWindow creates a new tumbling window for the given view definition.
func NewTumblingWindow(def *ViewDef, logger *slog.Logger) *TumblingWindow {
	if logger == nil {
		logger = slog.Default()
	}

	// Pre-compute aggregate item indices.
	aggIdx := 0
	indices := make([]int, len(def.SelectItems))
	for i, si := range def.SelectItems {
		if si.Aggregate != nil {
			indices[i] = aggIdx
			aggIdx++
		} else {
			indices[i] = -1
		}
	}

	return &TumblingWindow{
		def:            def,
		logger:         logger,
		groups:         make(map[string]*groupState),
		windowStart:    time.Now().UTC(),
		aggItemIndices: indices,
	}
}

// aggCount returns the number of aggregate items in the select list.
func (tw *TumblingWindow) aggCount() int {
	n := 0
	for _, idx := range tw.aggItemIndices {
		if idx >= 0 {
			n++
		}
	}
	return n
}

// Add processes one event, updating the appropriate group's aggregators.
func (tw *TumblingWindow) Add(meta EventMeta, payload map[string]any) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	// Build group key.
	groupKey, keyValues := tw.buildGroupKey(meta, payload)

	// Check if this event belongs to a closed window (late event handling).
	if tw.def.AllowedLateness > 0 {
		now := time.Now().UTC()
		for _, cw := range tw.closedWindows {
			// Skip expired closed windows.
			if now.Sub(cw.closedAt) >= tw.def.AllowedLateness {
				continue
			}
			if gs, ok := cw.groups[groupKey]; ok {
				// Late event for a known group in a closed window — update it.
				tw.addToGroupState(gs, meta, payload)
				cw.reEmit = true
				metrics.ViewEventsProcessed.WithLabelValues(tw.def.Name).Inc()
				return
			}
		}
	}

	gs, ok := tw.groups[groupKey]
	if !ok {
		if len(tw.groups) >= tw.def.MaxGroups {
			metrics.ViewGroupsOverflow.WithLabelValues(tw.def.Name).Inc()
			tw.logger.Warn("view group cardinality exceeded max_groups",
				slog.String("view", tw.def.Name),
				slog.Int("max_groups", tw.def.MaxGroups),
			)
			return
		}

		// Create new group state.
		aggs := make([]Aggregator, tw.aggCount())
		aggIdx := 0
		for _, si := range tw.def.SelectItems {
			if si.Aggregate != nil {
				aggs[aggIdx] = NewAggregator(*si.Aggregate)
				aggIdx++
			}
		}
		gs = &groupState{
			key:         groupKey,
			keyValues:   keyValues,
			aggregators: aggs,
		}
		tw.groups[groupKey] = gs
	}

	tw.addToGroupState(gs, meta, payload)
	metrics.ViewEventsProcessed.WithLabelValues(tw.def.Name).Inc()
}

// addToGroupState updates a group's aggregators with one event's values.
func (tw *TumblingWindow) addToGroupState(gs *groupState, meta EventMeta, payload map[string]any) {
	aggIdx := 0
	for _, si := range tw.def.SelectItems {
		if si.Aggregate == nil {
			continue
		}
		var val any
		if si.Field != "" {
			val = resolveField(si.Field, meta, payload)
		} else if *si.Aggregate == AggCount {
			// COUNT(*): always count — use non-nil sentinel.
			val = true
		}
		if !gs.aggregators[aggIdx].Add(val) {
			metrics.ViewTypeErrors.WithLabelValues(tw.def.Name).Inc()
		}
		aggIdx++
	}
}

// Flush closes the current window, applies HAVING, emits results, and resets.
// Returns the emitted events (may be 0 if window was empty or all filtered by HAVING).
func (tw *TumblingWindow) Flush() []event.Event {
	tw.mu.Lock()
	windowStart := tw.windowStart
	windowEnd := time.Now().UTC()
	groups := tw.groups

	// Reset for next window.
	tw.groups = make(map[string]*groupState)
	tw.windowStart = windowEnd

	// Handle late event support: save closed window state.
	if tw.def.AllowedLateness > 0 && len(groups) > 0 {
		tw.closedWindows = append(tw.closedWindows, &closedWindow{
			groups:   groups,
			start:    windowStart,
			end:      windowEnd,
			closedAt: windowEnd,
		})
	}

	// Expire old closed windows.
	if tw.def.AllowedLateness > 0 {
		tw.expireClosedWindows(windowEnd)
	}

	// Collect re-emit events from updated closed windows.
	var lateEvents []event.Event
	if tw.def.AllowedLateness > 0 {
		lateEvents = tw.flushLateUpdates()
	}

	tw.mu.Unlock()

	var events []event.Event

	if len(groups) > 0 {
		metrics.ViewGroups.WithLabelValues(tw.def.Name).Set(float64(len(groups)))
		events = tw.emitGroups(groups, windowStart, windowEnd)
	}

	events = append(events, lateEvents...)

	if len(events) > 0 {
		metrics.ViewWindowsEmitted.WithLabelValues(tw.def.Name).Add(float64(len(events)))
	}

	return events
}

// expireClosedWindows removes closed windows past the allowed lateness. Must be called with mu held.
func (tw *TumblingWindow) expireClosedWindows(now time.Time) {
	n := 0
	for _, cw := range tw.closedWindows {
		if now.Sub(cw.closedAt) < tw.def.AllowedLateness {
			tw.closedWindows[n] = cw
			n++
		}
	}
	tw.closedWindows = tw.closedWindows[:n]
}

// flushLateUpdates emits corrected results for closed windows that were updated by late events.
// Must be called with mu held.
func (tw *TumblingWindow) flushLateUpdates() []event.Event {
	var events []event.Event
	for _, cw := range tw.closedWindows {
		if !cw.reEmit {
			continue
		}
		cw.reEmit = false
		events = append(events, tw.emitGroups(cw.groups, cw.start, cw.end)...)
	}
	return events
}

// emitGroups builds and returns events for the given groups.
func (tw *TumblingWindow) emitGroups(groups map[string]*groupState, windowStart, windowEnd time.Time) []event.Event {
	// Build result rows.
	var rows []map[string]any
	for _, gs := range groups {
		row := tw.buildRow(gs)

		// Apply HAVING filter.
		if tw.def.Having != nil {
			// For HAVING, meta is irrelevant; the predicate operates on the aggregate row.
			if !tw.def.Having(EventMeta{}, row) {
				continue
			}
		}

		rows = append(rows, row)
	}

	if len(rows) == 0 {
		return nil
	}

	windowInfo := map[string]any{
		"start": windowStart.Format(time.RFC3339),
		"end":   windowEnd.Format(time.RFC3339),
	}

	channel := "pgcdc:_view:" + tw.def.Name
	source := "view"

	var events []event.Event

	switch tw.def.Emit {
	case EmitBatch:
		payload := map[string]any{
			"_window": windowInfo,
			"rows":    rows,
		}
		ev, err := makeViewEvent(channel, source, payload)
		if err != nil {
			tw.logger.Error("create view batch event", "error", err, "view", tw.def.Name)
			return nil
		}
		events = append(events, ev)

	default: // EmitRow
		for _, row := range rows {
			row["_window"] = windowInfo
			ev, err := makeViewEvent(channel, source, row)
			if err != nil {
				tw.logger.Error("create view row event", "error", err, "view", tw.def.Name)
				continue
			}
			events = append(events, ev)
		}
	}

	return events
}

// buildGroupKey constructs a group key string and the key values map.
func (tw *TumblingWindow) buildGroupKey(meta EventMeta, payload map[string]any) (string, map[string]any) {
	if len(tw.def.GroupBy) == 0 {
		// Global aggregation — single implicit group.
		return "_global_", nil
	}

	keyValues := make(map[string]any, len(tw.def.GroupBy))
	var parts []string
	for _, field := range tw.def.GroupBy {
		val := resolveField(field, meta, payload)
		keyValues[field] = val
		parts = append(parts, fmt.Sprintf("%v", val))
	}
	return strings.Join(parts, "\x00"), keyValues
}

// buildRow constructs the output row for a group.
func (tw *TumblingWindow) buildRow(gs *groupState) map[string]any {
	row := make(map[string]any)

	// Add group-by key values.
	for _, si := range tw.def.SelectItems {
		if si.IsGroupKey && gs.keyValues != nil {
			if v, ok := gs.keyValues[si.Field]; ok {
				row[si.Alias] = v
			}
		}
	}

	// Add aggregate results.
	aggIdx := 0
	for _, si := range tw.def.SelectItems {
		if si.Aggregate == nil {
			continue
		}
		row[si.Alias] = gs.aggregators[aggIdx].Result()
		aggIdx++
	}

	return row
}

// makeViewEvent creates an event with the given payload map.
func makeViewEvent(channel, source string, payload map[string]any) (event.Event, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return event.Event{}, fmt.Errorf("marshal view payload: %w", err)
	}
	return event.New(channel, "VIEW_RESULT", payloadBytes, source)
}
