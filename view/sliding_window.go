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

// SlidingWindow implements a sliding window of size W with slide interval S.
// It maintains ceil(W/S) tumbling sub-windows. On each slide, results are
// aggregated across all sub-windows and the oldest sub-window is evicted.
type SlidingWindow struct {
	def    *ViewDef
	logger *slog.Logger

	mu         sync.Mutex
	subWindows []*subWindow
	current    int // index of the current (newest) sub-window
	numSlots   int

	windowStart time.Time
}

// subWindow holds aggregation state for one slide interval.
type subWindow struct {
	groups map[string]*groupState
}

func newSubWindow() *subWindow {
	return &subWindow{groups: make(map[string]*groupState)}
}

// NewSlidingWindow creates a sliding window for the given view definition.
func NewSlidingWindow(def *ViewDef, logger *slog.Logger) *SlidingWindow {
	if logger == nil {
		logger = slog.Default()
	}

	numSlots := int((def.WindowSize + def.SlideSize - 1) / def.SlideSize)

	subs := make([]*subWindow, numSlots)
	for i := range subs {
		subs[i] = newSubWindow()
	}

	return &SlidingWindow{
		def:         def,
		logger:      logger,
		subWindows:  subs,
		current:     0,
		numSlots:    numSlots,
		windowStart: time.Now().UTC(),
	}
}

// aggCount returns the number of aggregate items in the select list.
func (sw *SlidingWindow) aggCount() int {
	n := 0
	for _, si := range sw.def.SelectItems {
		if si.Aggregate != nil {
			n++
		}
	}
	return n
}

// Add processes one event into the current sub-window.
func (sw *SlidingWindow) Add(meta EventMeta, payload map[string]any) {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	sub := sw.subWindows[sw.current]

	groupKey, keyValues := sw.buildGroupKey(meta, payload)

	gs, ok := sub.groups[groupKey]
	if !ok {
		totalGroups := sw.countTotalGroups()
		if totalGroups >= sw.def.MaxGroups {
			metrics.ViewGroupsOverflow.WithLabelValues(sw.def.Name).Inc()
			sw.logger.Warn("view group cardinality exceeded max_groups",
				slog.String("view", sw.def.Name),
				slog.Int("max_groups", sw.def.MaxGroups),
			)
			return
		}

		aggs := make([]Aggregator, sw.aggCount())
		aggIdx := 0
		for _, si := range sw.def.SelectItems {
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
		sub.groups[groupKey] = gs
	}

	// Update aggregators.
	aggIdx := 0
	for _, si := range sw.def.SelectItems {
		if si.Aggregate == nil {
			continue
		}
		var val any
		if si.Field != "" {
			val = resolveField(si.Field, meta, payload)
		} else if *si.Aggregate == AggCount {
			val = true
		}
		if !gs.aggregators[aggIdx].Add(val) {
			metrics.ViewTypeErrors.WithLabelValues(sw.def.Name).Inc()
		}
		aggIdx++
	}

	metrics.ViewEventsProcessed.WithLabelValues(sw.def.Name).Inc()
}

// countTotalGroups returns the total unique groups across all sub-windows.
func (sw *SlidingWindow) countTotalGroups() int {
	seen := make(map[string]struct{})
	for _, sub := range sw.subWindows {
		for k := range sub.groups {
			seen[k] = struct{}{}
		}
	}
	return len(seen)
}

// Flush aggregates across all sub-windows, emits results, evicts the oldest
// sub-window, and advances the current pointer.
func (sw *SlidingWindow) Flush() []event.Event {
	sw.mu.Lock()
	windowStart := sw.windowStart
	windowEnd := time.Now().UTC()

	// Merge all sub-windows into combined groups.
	combined := sw.mergeSubWindows()

	// Evict the oldest sub-window (the slot after current in the ring).
	oldest := (sw.current + 1) % sw.numSlots
	sw.subWindows[oldest] = newSubWindow()

	// Advance current to the freshly evicted slot.
	sw.current = oldest
	sw.windowStart = windowEnd
	sw.mu.Unlock()

	if len(combined) == 0 {
		return nil
	}

	metrics.ViewGroups.WithLabelValues(sw.def.Name).Set(float64(len(combined)))

	// Build result rows.
	var rows []map[string]any
	for _, gs := range combined {
		row := sw.buildRow(gs)

		if sw.def.Having != nil {
			if !sw.def.Having(EventMeta{}, row) {
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

	channel := "pgcdc:_view:" + sw.def.Name
	source := "view"

	var events []event.Event

	switch sw.def.Emit {
	case EmitBatch:
		payload := map[string]any{
			"_window": windowInfo,
			"rows":    rows,
		}
		ev, err := makeViewEvent(channel, source, payload)
		if err != nil {
			sw.logger.Error("create view batch event", "error", err, "view", sw.def.Name)
			return nil
		}
		events = append(events, ev)

	default: // EmitRow
		for _, row := range rows {
			row["_window"] = windowInfo
			ev, err := makeViewEvent(channel, source, row)
			if err != nil {
				sw.logger.Error("create view row event", "error", err, "view", sw.def.Name)
				continue
			}
			events = append(events, ev)
		}
	}

	metrics.ViewWindowsEmitted.WithLabelValues(sw.def.Name).Add(float64(len(events)))
	return events
}

// mergeSubWindows combines aggregation state across all sub-windows.
// For non-mergeable aggregators (AVG, STDDEV, COUNT_DISTINCT), we re-aggregate
// from scratch using stored raw data is not feasible, so we use
// a best-effort merge: sum partial counts/sums for AVG.
// For simplicity, we use a fresh aggregator set per group and replay
// the partial results as single values.
func (sw *SlidingWindow) mergeSubWindows() map[string]*groupState {
	combined := make(map[string]*groupState)

	for _, sub := range sw.subWindows {
		for key, gs := range sub.groups {
			cgs, ok := combined[key]
			if !ok {
				// First sub-window with this group — clone aggregator state.
				aggs := make([]Aggregator, len(gs.aggregators))
				for i, a := range gs.aggregators {
					aggs[i] = cloneAggregator(a)
				}
				cgs = &groupState{
					key:         key,
					keyValues:   gs.keyValues,
					aggregators: aggs,
				}
				combined[key] = cgs
				continue
			}
			// Merge subsequent sub-windows by adding the partial result.
			for i, a := range gs.aggregators {
				mergeAggregator(cgs.aggregators[i], a)
			}
		}
	}

	return combined
}

// cloneAggregator creates a new aggregator with the same state.
func cloneAggregator(a Aggregator) Aggregator {
	result := a.Result()
	switch orig := a.(type) {
	case *countAgg:
		c := &countAgg{}
		if n, ok := result.(int64); ok {
			c.n = n
		}
		return c
	case *sumAgg:
		s := &sumAgg{}
		if f, ok := result.(float64); ok {
			s.sum = f
			s.any = true
		}
		return s
	case *avgAgg:
		return &avgAgg{sum: orig.sum, count: orig.count}
	case *minAgg:
		m := &minAgg{}
		if f, ok := result.(float64); ok {
			m.min = f
			m.any = true
		}
		return m
	case *maxAgg:
		m := &maxAgg{}
		if f, ok := result.(float64); ok {
			m.max = f
			m.any = true
		}
		return m
	case *countDistinctAgg:
		newSeen := make(map[string]struct{}, len(orig.seen))
		for k := range orig.seen {
			newSeen[k] = struct{}{}
		}
		return &countDistinctAgg{seen: newSeen}
	case *stddevAgg:
		return &stddevAgg{n: orig.n, mean: orig.mean, m2: orig.m2}
	default:
		// Fallback: create fresh aggregator and add the result.
		fresh := &countAgg{}
		fresh.Add(result)
		return fresh
	}
}

// mergeAggregator merges the state of src into dst.
func mergeAggregator(dst, src Aggregator) {
	switch d := dst.(type) {
	case *countAgg:
		if s, ok := src.(*countAgg); ok {
			d.n += s.n
		}
	case *sumAgg:
		if s, ok := src.(*sumAgg); ok && s.any {
			d.sum += s.sum
			d.any = true
		}
	case *avgAgg:
		if s, ok := src.(*avgAgg); ok && s.count > 0 {
			d.sum += s.sum
			d.count += s.count
		}
	case *minAgg:
		if s, ok := src.(*minAgg); ok && s.any {
			if !d.any || s.min < d.min {
				d.min = s.min
			}
			d.any = true
		}
	case *maxAgg:
		if s, ok := src.(*maxAgg); ok && s.any {
			if !d.any || s.max > d.max {
				d.max = s.max
			}
			d.any = true
		}
	case *countDistinctAgg:
		if s, ok := src.(*countDistinctAgg); ok {
			for k := range s.seen {
				d.seen[k] = struct{}{}
			}
		}
	case *stddevAgg:
		// Merge Welford states using parallel algorithm.
		if s, ok := src.(*stddevAgg); ok && s.n > 0 {
			if d.n == 0 {
				d.n = s.n
				d.mean = s.mean
				d.m2 = s.m2
			} else {
				totalN := d.n + s.n
				delta := s.mean - d.mean
				d.m2 = d.m2 + s.m2 + delta*delta*float64(d.n)*float64(s.n)/float64(totalN)
				d.mean = (d.mean*float64(d.n) + s.mean*float64(s.n)) / float64(totalN)
				d.n = totalN
			}
		}
	}
}

func (sw *SlidingWindow) buildGroupKey(meta EventMeta, payload map[string]any) (string, map[string]any) {
	if len(sw.def.GroupBy) == 0 {
		return "_global_", nil
	}

	keyValues := make(map[string]any, len(sw.def.GroupBy))
	var parts []string
	for _, field := range sw.def.GroupBy {
		val := resolveField(field, meta, payload)
		keyValues[field] = val
		parts = append(parts, fmt.Sprintf("%v", val))
	}
	return strings.Join(parts, "\x00"), keyValues
}

func (sw *SlidingWindow) buildRow(gs *groupState) map[string]any {
	row := make(map[string]any)

	for _, si := range sw.def.SelectItems {
		if si.IsGroupKey && gs.keyValues != nil {
			if v, ok := gs.keyValues[si.Field]; ok {
				row[si.Alias] = v
			}
		}
	}

	aggIdx := 0
	for _, si := range sw.def.SelectItems {
		if si.Aggregate == nil {
			continue
		}
		result := gs.aggregators[aggIdx].Result()
		// JSON-encode then decode to normalize types (int64 -> float64).
		b, _ := json.Marshal(result)
		var normalized any
		_ = json.Unmarshal(b, &normalized)
		row[si.Alias] = normalized
		aggIdx++
	}

	return row
}
