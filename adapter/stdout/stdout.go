package stdout

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/florinutz/pgpipe/event"
	"github.com/florinutz/pgpipe/metrics"
)

// Adapter writes events as JSON-lines to an io.Writer.
// It is the simplest adapter, useful for debugging and unix piping
// (e.g. pgpipe listen -c orders | jq .payload).
type Adapter struct {
	w      io.Writer
	logger *slog.Logger
}

// New creates a stdout adapter that writes JSON-lines to w.
// If w is nil it defaults to os.Stdout.
func New(w io.Writer, logger *slog.Logger) *Adapter {
	if w == nil {
		w = os.Stdout
	}
	return &Adapter{
		w:      w,
		logger: logger,
	}
}

// Start blocks, consuming events from the channel and writing each one as a
// JSON line to the underlying writer. It returns nil when the channel is closed
// or the context is cancelled.
func (a *Adapter) Start(ctx context.Context, events <-chan event.Event) error {
	a.logger.Info("stdout adapter started")

	enc := json.NewEncoder(a.w)

	for {
		select {
		case <-ctx.Done():
			a.logger.Info("stdout adapter stopping", "reason", "context cancelled")
			return ctx.Err()

		case ev, ok := <-events:
			if !ok {
				a.logger.Info("stdout adapter stopping", "reason", "channel closed")
				return nil
			}
			if err := enc.Encode(ev); err != nil {
				return fmt.Errorf("stdout adapter: encode event %s: %w", ev.ID, err)
			}
			metrics.EventsDelivered.WithLabelValues("stdout").Inc()
		}
	}
}

// Name returns the adapter name.
func (a *Adapter) Name() string {
	return "stdout"
}
