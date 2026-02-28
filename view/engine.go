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
	Add(meta EventMeta, payload map[string]any)
	Flush() []event.Event
}

// Engine orchestrates multiple views, each with its own window.
type Engine struct {
	views  []*viewInstance
	logger *slog.Logger
}

type viewInstance struct {
	def    *ViewDef
	window Window
}

// NewEngine creates an engine from a set of view definitions.
func NewEngine(defs []*ViewDef, logger *slog.Logger) *Engine {
	if logger == nil {
		logger = slog.Default()
	}

	views := make([]*viewInstance, len(defs))
	for i, def := range defs {
		views[i] = &viewInstance{
			def:    def,
			window: newWindow(def, logger),
		}
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
	var payload map[string]any
	if rec := ev.Record(); rec != nil && rec.Operation != 0 &&
		(rec.Change.After != nil || rec.Change.Before != nil) {
		if rec.Change.After != nil {
			payload = rec.Change.After.ToMap()
		} else {
			payload = rec.Change.Before.ToMap()
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
		// Evaluate WHERE predicate.
		if vi.def.Where != nil && !vi.def.Where(meta, payload) {
			continue
		}
		vi.window.Add(meta, payload)
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
			events := vi.window.Flush()
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
		all = append(all, vi.window.Flush()...)
	}
	return all
}
