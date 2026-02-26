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

// SessionWindow implements gap-based windows that stay open while events
// arrive within the gap timeout. When no events arrive for a group within
// the gap duration, the session is closed and results are emitted.
type SessionWindow struct {
	def    *ViewDef
	logger *slog.Logger

	mu       sync.Mutex
	sessions map[string]*sessionState
}

type sessionState struct {
	groups   map[string]*groupState
	lastSeen time.Time
	start    time.Time
}

// NewSessionWindow creates a session window for the given view definition.
func NewSessionWindow(def *ViewDef, logger *slog.Logger) *SessionWindow {
	if logger == nil {
		logger = slog.Default()
	}

	return &SessionWindow{
		def:      def,
		logger:   logger,
		sessions: make(map[string]*sessionState),
	}
}

// aggCount returns the number of aggregate items in the select list.
func (sw *SessionWindow) aggCount() int {
	n := 0
	for _, si := range sw.def.SelectItems {
		if si.Aggregate != nil {
			n++
		}
	}
	return n
}

// Add processes one event, finding or creating a session for its group key.
func (sw *SessionWindow) Add(meta EventMeta, payload map[string]any) {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	groupKey, keyValues := sw.buildGroupKey(meta, payload)

	sess, ok := sw.sessions[groupKey]
	if !ok {
		if len(sw.sessions) >= sw.def.MaxGroups {
			metrics.ViewGroupsOverflow.WithLabelValues(sw.def.Name).Inc()
			sw.logger.Warn("view session count exceeded max_groups",
				slog.String("view", sw.def.Name),
				slog.Int("max_groups", sw.def.MaxGroups),
			)
			return
		}

		now := time.Now().UTC()
		// Create new session with a single global group (sessions are keyed by group-by fields).
		aggs := make([]Aggregator, sw.aggCount())
		aggIdx := 0
		for _, si := range sw.def.SelectItems {
			if si.Aggregate != nil {
				aggs[aggIdx] = NewAggregator(*si.Aggregate)
				aggIdx++
			}
		}
		gs := &groupState{
			key:         groupKey,
			keyValues:   keyValues,
			aggregators: aggs,
		}
		sess = &sessionState{
			groups:   map[string]*groupState{groupKey: gs},
			lastSeen: now,
			start:    now,
		}
		sw.sessions[groupKey] = sess
	} else {
		sess.lastSeen = time.Now().UTC()
	}

	gs := sess.groups[groupKey]

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

// Flush checks all sessions, emits results for expired sessions
// (lastSeen + gap < now), and removes them.
func (sw *SessionWindow) Flush() []event.Event {
	sw.mu.Lock()
	now := time.Now().UTC()

	var expired []*sessionState
	var expiredKeys []string

	for key, sess := range sw.sessions {
		if now.Sub(sess.lastSeen) >= sw.def.SessionGap {
			expired = append(expired, sess)
			expiredKeys = append(expiredKeys, key)
		}
	}

	for _, key := range expiredKeys {
		delete(sw.sessions, key)
	}
	sw.mu.Unlock()

	if len(expired) == 0 {
		return nil
	}

	metrics.ViewGroups.WithLabelValues(sw.def.Name).Set(float64(len(expired)))

	var rows []map[string]any
	var windowInfos []map[string]any
	for _, sess := range expired {
		for _, gs := range sess.groups {
			row := sw.buildRow(gs)

			if sw.def.Having != nil {
				if !sw.def.Having(EventMeta{}, row) {
					continue
				}
			}

			windowInfo := map[string]any{
				"start": sess.start.Format(time.RFC3339),
				"end":   sess.lastSeen.Format(time.RFC3339),
			}
			rows = append(rows, row)
			windowInfos = append(windowInfos, windowInfo)
		}
	}

	if len(rows) == 0 {
		return nil
	}

	channel := "pgcdc:_view:" + sw.def.Name
	source := "view"

	var events []event.Event

	switch sw.def.Emit {
	case EmitBatch:
		// Use the earliest start and latest end across all sessions.
		payload := map[string]any{
			"_window": windowInfos[0], // each batch is per session
			"rows":    rows,
		}
		ev, err := makeViewEvent(channel, source, payload)
		if err != nil {
			sw.logger.Error("create view batch event", "error", err, "view", sw.def.Name)
			return nil
		}
		events = append(events, ev)

	default: // EmitRow
		for i, row := range rows {
			row["_window"] = windowInfos[i]
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

func (sw *SessionWindow) buildGroupKey(meta EventMeta, payload map[string]any) (string, map[string]any) {
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

func (sw *SessionWindow) buildRow(gs *groupState) map[string]any {
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
		b, _ := json.Marshal(result)
		var normalized any
		_ = json.Unmarshal(b, &normalized)
		row[si.Alias] = normalized
		aggIdx++
	}

	return row
}
