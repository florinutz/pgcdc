package chain

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/metrics"
)

// Link processes an event and returns the (possibly transformed) event.
// Links are composed into chains where each link's output feeds the next.
type Link interface {
	// Process transforms the event. Return the modified event and nil error
	// to pass to the next link. Return a non-nil error to abort the chain
	// for this event.
	Process(ctx context.Context, ev event.Event) (event.Event, error)
	Name() string
}

// Adapter composes a sequence of Links followed by a terminal adapter.
// Each event passes through all links in order before reaching the terminal.
type Adapter struct {
	links    []Link
	terminal TerminalAdapter
	logger   *slog.Logger
	name     string
}

// TerminalAdapter is the final adapter in a chain that actually delivers events.
type TerminalAdapter interface {
	Start(ctx context.Context, events <-chan event.Event) error
	Name() string
}

// New creates a chain adapter. Links are applied in order before the terminal.
func New(name string, terminal TerminalAdapter, logger *slog.Logger, links ...Link) *Adapter {
	if logger == nil {
		logger = slog.Default()
	}
	if name == "" {
		name = "chain:" + terminal.Name()
	}
	return &Adapter{
		links:    links,
		terminal: terminal,
		logger:   logger.With("adapter", name),
		name:     name,
	}
}

func (a *Adapter) Name() string { return a.name }

// Start consumes events, passes each through the link chain, and forwards
// the result to the terminal adapter.
func (a *Adapter) Start(ctx context.Context, events <-chan event.Event) error {
	// Create a channel for the terminal adapter.
	processed := make(chan event.Event, cap(events))

	// Start the terminal adapter in a goroutine.
	errCh := make(chan error, 1)
	go func() {
		errCh <- a.terminal.Start(ctx, processed)
	}()

	// Process events through the chain.
	for {
		select {
		case <-ctx.Done():
			close(processed)
			// Drain the terminal goroutine but return the context error,
			// matching the standard adapter contract.
			<-errCh
			return ctx.Err()

		case ev, ok := <-events:
			if !ok {
				close(processed)
				return <-errCh
			}

			result, err := a.processChain(ctx, ev)
			if err != nil {
				a.logger.Warn("chain link error, skipping event",
					"event_id", ev.ID,
					"error", err,
				)
				metrics.ChainLinkErrors.WithLabelValues(a.name).Inc()
				continue
			}

			metrics.ChainEventsProcessed.WithLabelValues(a.name).Inc()

			select {
			case processed <- result:
			case <-ctx.Done():
				close(processed)
				<-errCh
				return ctx.Err()
			}
		}
	}
}

func (a *Adapter) processChain(ctx context.Context, ev event.Event) (event.Event, error) {
	var err error
	for _, link := range a.links {
		ev, err = link.Process(ctx, ev)
		if err != nil {
			return ev, fmt.Errorf("link %s: %w", link.Name(), err)
		}
	}
	return ev, nil
}
