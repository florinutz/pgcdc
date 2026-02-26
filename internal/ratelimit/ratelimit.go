package ratelimit

import (
	"context"
	"log/slog"
	"time"

	"github.com/florinutz/pgcdc/metrics"
	"golang.org/x/time/rate"
)

// Limiter wraps a token-bucket rate limiter with metrics.
type Limiter struct {
	limiter *rate.Limiter
	adapter string
	logger  *slog.Logger
}

// New creates a rate limiter. If eventsPerSecond is <= 0, the limiter is a no-op
// (allows everything immediately).
func New(eventsPerSecond float64, burst int, adapter string, logger *slog.Logger) *Limiter {
	if logger == nil {
		logger = slog.Default()
	}
	var l *rate.Limiter
	if eventsPerSecond > 0 {
		l = rate.NewLimiter(rate.Limit(eventsPerSecond), burst)
	}
	return &Limiter{
		limiter: l,
		adapter: adapter,
		logger:  logger,
	}
}

// Wait blocks until the limiter allows an event, or ctx is cancelled.
// Returns nil immediately in no-op mode.
func (l *Limiter) Wait(ctx context.Context) error {
	if l.limiter == nil {
		return nil
	}

	start := time.Now()
	err := l.limiter.Wait(ctx)
	elapsed := time.Since(start)

	if err == nil && elapsed > time.Millisecond {
		metrics.RateLimitWaits.WithLabelValues(l.adapter).Inc()
		metrics.RateLimitWaitDuration.WithLabelValues(l.adapter).Observe(elapsed.Seconds())
	}

	return err
}
