package view

import (
	"context"
	"log/slog"
	"strings"

	"github.com/florinutz/pgcdc/event"
	viewpkg "github.com/florinutz/pgcdc/view"
	"golang.org/x/sync/errgroup"
)

// viewChannelPrefix is the channel prefix for re-injected VIEW_RESULT events.
// Events on this prefix are skipped to prevent infinite re-injection loops.
const viewChannelPrefix = "pgcdc:_view:"

// Adapter is a view adapter that processes events through streaming SQL views
// and re-injects VIEW_RESULT events back into the bus.
type Adapter struct {
	engine   *viewpkg.Engine
	logger   *slog.Logger
	ingestCh chan<- event.Event
}

// New creates a view adapter wrapping the given engine.
func New(engine *viewpkg.Engine, logger *slog.Logger) *Adapter {
	if logger == nil {
		logger = slog.Default()
	}
	return &Adapter{
		engine: engine,
		logger: logger.With("component", "view_adapter"),
	}
}

// Name returns the adapter name.
func (a *Adapter) Name() string { return "view" }

// SetIngestChan implements adapter.Reinjector.
func (a *Adapter) SetIngestChan(ch chan<- event.Event) {
	a.ingestCh = ch
}

// Start reads events from the subscription channel, feeds them to the engine,
// and re-injects window results back into the bus.
func (a *Adapter) Start(ctx context.Context, events <-chan event.Event) error {
	if a.ingestCh == nil {
		a.logger.Error("view adapter: ingest channel not set (SetIngestChan not called)")
		return nil
	}

	g, gCtx := errgroup.WithContext(ctx)

	// Goroutine 1: Read events and feed to engine.
	g.Go(func() error {
		for {
			select {
			case <-gCtx.Done():
				return gCtx.Err()
			case ev, ok := <-events:
				if !ok {
					return nil
				}
				if strings.HasPrefix(ev.Channel, viewChannelPrefix) {
					continue // skip re-injected VIEW_RESULT events to prevent loops
				}
				a.engine.Process(ev)
			}
		}
	})

	// Goroutine 2: Run window tickers and emit results to bus.
	g.Go(func() error {
		return a.engine.Run(gCtx, a.ingestCh)
	})

	return g.Wait()
}
