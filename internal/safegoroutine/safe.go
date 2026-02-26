package safegoroutine

import (
	"fmt"
	"log/slog"
	"runtime/debug"

	"github.com/florinutz/pgcdc/metrics"
	"golang.org/x/sync/errgroup"
)

// Go wraps errgroup.Go with panic recovery. If fn panics, it recovers the panic,
// logs the stack trace, increments the panics_recovered metric, and returns an error.
func Go(g *errgroup.Group, logger *slog.Logger, name string, fn func() error) {
	if logger == nil {
		logger = slog.Default()
	}
	g.Go(func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				metrics.PanicsRecovered.WithLabelValues(name).Inc()
				logger.Error("panic recovered",
					"component", name,
					"panic", r,
					"stack", string(debug.Stack()),
				)
				err = fmt.Errorf("panic in %s: %v", name, r)
			}
		}()
		return fn()
	})
}
