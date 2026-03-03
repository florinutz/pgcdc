package view

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/metrics"
)

// Window is the common interface for all window types.
type Window interface {
	Add(meta EventMeta, payload map[string]any, eventTime time.Time)
	Flush() []event.Event
	FlushUpTo(wm time.Time) []event.Event
}

// Engine orchestrates multiple views, each with its own window.
type Engine struct {
	views  []*viewInstance
	logger *slog.Logger
}

type viewInstance struct {
	def    *ViewDef
	window Window
	// For event-time watermarks:
	watermarkMu   sync.Mutex
	highWatermark time.Time
	// For interval joins:
	join        *IntervalJoinWindow // non-nil when def.Join != nil
	joinResults chan event.Event    // buffered channel for join matches
}

// NewEngine creates an engine from a set of view definitions.
func NewEngine(defs []*ViewDef, logger *slog.Logger) *Engine {
	if logger == nil {
		logger = slog.Default()
	}

	views := make([]*viewInstance, len(defs))
	for i, def := range defs {
		vi := &viewInstance{def: def}
		if def.Join != nil {
			vi.join = NewIntervalJoinWindow(def.Join)
			vi.joinResults = make(chan event.Event, 1000)
		} else {
			vi.window = newWindow(def, logger)
		}
		views[i] = vi
	}

	return &Engine{
		views:  views,
		logger: logger,
	}
}

// newWindow creates the appropriate window type for a view definition.
func newWindow(def *ViewDef, logger *slog.Logger) Window {
	switch def.WindowType {
	case WindowSliding:
		return NewSlidingWindow(def, logger)
	case WindowSession:
		return NewSessionWindow(def, logger)
	default:
		return NewTumblingWindow(def, logger)
	}
}

// Process evaluates a single event against all views.
// It returns immediately; window results are emitted asynchronously via the ticker.
func (e *Engine) Process(ev event.Event) {
	// Loop prevention: skip events from view channels.
	if strings.HasPrefix(ev.Channel, "pgcdc:_view:") {
		return
	}

	// Parse payload once for all views. Prefer structured record (zero JSON parsing).
	// Build the legacy-compatible format (op, table, row, old) so field paths like
	// "payload.row.region" work consistently regardless of event origin.
	var payload map[string]any
	if rec := ev.Record(); rec != nil && rec.Operation != 0 &&
		(rec.Change.After != nil || rec.Change.Before != nil) {
		payload = make(map[string]any, 4)
		payload["op"] = rec.Operation.String()
		if table, ok := rec.Metadata[event.MetaTable]; ok {
			payload["table"] = table
		}
		if rec.Change.After != nil {
			payload["row"] = rec.Change.After.ToMap()
		}
		if rec.Change.Before != nil {
			payload["old"] = rec.Change.Before.ToMap()
		}
	} else if len(ev.Payload) > 0 {
		if err := json.Unmarshal(ev.Payload, &payload); err != nil {
			e.logger.Debug("view engine: unmarshal payload", "error", err, "event_id", ev.ID)
			return
		}
	}

	meta := EventMeta{
		Channel:   ev.Channel,
		Operation: ev.Operation,
		Source:    ev.Source,
	}

	for _, vi := range e.views {
		// Handle interval join routing.
		if vi.def.Join != nil {
			jd := vi.def.Join
			switch ev.Channel {
			case jd.LeftChan:
				matches := vi.join.AddLeft(time.Now(), payload)
				for _, m := range matches {
					select {
					case vi.joinResults <- m:
					default:
					}
				}
			case jd.RightChan:
				matches := vi.join.AddRight(time.Now(), payload)
				for _, m := range matches {
					select {
					case vi.joinResults <- m:
					default:
					}
				}
			}
			continue
		}

		// Evaluate WHERE predicate.
		if vi.def.Where != nil && !vi.def.Where(meta, payload) {
			continue
		}

		if vi.def.EventTimeField != "" {
			// Event-time mode: extract timestamp and advance watermark.
			et := extractEventTime(payload, vi.def.EventTimeField)
			vi.window.Add(meta, payload, et)
			if !et.IsZero() {
				vi.watermarkMu.Lock()
				wm := et.Add(-vi.def.AllowedLateness)
				if wm.After(vi.highWatermark) {
					vi.highWatermark = wm
				}
				vi.watermarkMu.Unlock()
			}
		} else {
			// Processing-time mode.
			vi.window.Add(meta, payload, time.Time{})
		}
	}
}

// Run starts the window tickers and emits results to the emit channel.
// Blocks until ctx is cancelled.
func (e *Engine) Run(ctx context.Context, emit chan<- event.Event) error {
	var wg sync.WaitGroup

	for _, vi := range e.views {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.runView(ctx, vi, emit)
		}()
	}

	wg.Wait()
	return ctx.Err()
}

// tickInterval returns the flush interval for a view based on its window type.
func tickInterval(def *ViewDef) time.Duration {
	switch def.WindowType {
	case WindowSliding:
		return def.SlideSize
	case WindowSession:
		return def.SessionGap
	default:
		return def.WindowSize
	}
}

// runView runs a single view's ticker loop.
func (e *Engine) runView(ctx context.Context, vi *viewInstance, emit chan<- event.Event) {
	// Join views: drain joinResults channel, no ticker needed for flush.
	if vi.def.Join != nil {
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-vi.joinResults:
				select {
				case emit <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}

	// Standard window views.
	ticker := time.NewTicker(tickInterval(vi.def))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Flush remaining window on shutdown.
			events := vi.window.Flush()
			for _, ev := range events {
				select {
				case emit <- ev:
				default:
					// Bus full during shutdown — drop.
				}
			}
			return

		case <-ticker.C:
			start := time.Now()
			var events []event.Event
			if vi.def.EventTimeField != "" {
				// Event-time mode: flush based on watermark.
				vi.watermarkMu.Lock()
				wm := vi.highWatermark
				vi.watermarkMu.Unlock()
				if !wm.IsZero() {
					events = vi.window.FlushUpTo(wm)
				}
			} else {
				events = vi.window.Flush()
			}
			metrics.ViewWindowDuration.WithLabelValues(vi.def.Name).Observe(time.Since(start).Seconds())

			for _, ev := range events {
				select {
				case emit <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// FlushAll flushes all view windows synchronously and returns the results.
// Used for testing.
func (e *Engine) FlushAll() []event.Event {
	all := make([]event.Event, 0, len(e.views))
	for _, vi := range e.views {
		if vi.window != nil {
			all = append(all, vi.window.Flush()...)
		}
	}
	return all
}

// extractEventTime extracts a time.Time from a payload using a dotted field path.
// Returns zero time if field not found or not a valid RFC3339 timestamp.
func extractEventTime(payload map[string]any, field string) time.Time {
	if payload == nil || field == "" {
		return time.Time{}
	}
	parts := strings.Split(field, ".")
	// Strip the leading "payload" component to match resolveFieldParsed semantics:
	// the engine's payload map IS the top-level JSON object, so "payload.row.ts"
	// should navigate ["row","ts"], not ["payload","row","ts"].
	if len(parts) > 1 && parts[0] == "payload" {
		parts = parts[1:]
	}
	var val any = payload
	for _, part := range parts {
		m, ok := val.(map[string]any)
		if !ok {
			return time.Time{}
		}
		val = m[part]
	}
	s, ok := val.(string)
	if !ok {
		return time.Time{}
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t
	}
	return time.Time{}
}
