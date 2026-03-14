package middleware

import (
	"context"
	"log/slog"
	"time"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/circuitbreaker"
	"github.com/florinutz/pgcdc/internal/ratelimit"
)

// DeliverFunc is the signature for a single-event delivery function.
// Middleware wraps one DeliverFunc and returns another.
type DeliverFunc func(ctx context.Context, ev event.Event) error

// Middleware wraps a DeliverFunc with additional behavior.
type Middleware func(next DeliverFunc) DeliverFunc

// Config holds per-adapter middleware configuration.
type Config struct {
	Retry          *RetryConfig
	CircuitBreaker *CircuitBreakerConfig
	RateLimit      *RateLimitConfig
	Timeout        time.Duration
}

// RetryConfig controls exponential backoff retry behavior.
type RetryConfig struct {
	MaxRetries  int
	BackoffBase time.Duration
	BackoffCap  time.Duration
}

// CircuitBreakerConfig controls circuit breaker behavior.
type CircuitBreakerConfig struct {
	MaxFailures  int
	ResetTimeout time.Duration
}

// RateLimitConfig controls token-bucket rate limiting.
type RateLimitConfig struct {
	EventsPerSecond float64
	Burst           int
}

// Stack composes middleware in order. The first middleware in the slice is the
// outermost wrapper (executed first). For the standard stack:
//
//	Metrics → Tracing → CircuitBreaker → RateLimit → Retry → DLQ → Ack → Deliver
//
// Stack(metrics, tracing, cb, rl, retry, dlqMW, ack) wraps deliver in that order.
func Stack(mws ...Middleware) Middleware {
	return func(next DeliverFunc) DeliverFunc {
		for i := len(mws) - 1; i >= 0; i-- {
			next = mws[i](next)
		}
		return next
	}
}

// Build constructs the standard middleware chain for an adapter. Nil config
// fields mean the corresponding middleware is skipped (no-op).
func Build(name string, cfg Config, opts ...Option) DeliverFunc {
	o := options{}
	for _, fn := range opts {
		fn(&o)
	}

	var mws []Middleware

	// Outermost: metrics (always on).
	mws = append(mws, Metrics(name))

	// Tracing (when tracer is set).
	if o.tracer != nil {
		mws = append(mws, Tracing(name, o.tracer))
	}

	// Circuit breaker (when configured).
	if cfg.CircuitBreaker != nil && cfg.CircuitBreaker.MaxFailures > 0 {
		cb := circuitbreaker.New(cfg.CircuitBreaker.MaxFailures, cfg.CircuitBreaker.ResetTimeout, o.logger)
		mws = append(mws, CircuitBreak(name, cb, o.dlq))
	}

	// Rate limiter (when configured).
	if cfg.RateLimit != nil && cfg.RateLimit.EventsPerSecond > 0 {
		lim := ratelimit.New(cfg.RateLimit.EventsPerSecond, cfg.RateLimit.Burst, name, o.logger)
		mws = append(mws, RateLimit(lim))
	}

	// Timeout (when configured).
	if cfg.Timeout > 0 {
		mws = append(mws, Timeout(cfg.Timeout))
	}

	// Retry (when configured).
	if cfg.Retry != nil && cfg.Retry.MaxRetries > 0 {
		mws = append(mws, Retry(cfg.Retry.MaxRetries, cfg.Retry.BackoffBase, cfg.Retry.BackoffCap, o.logger))
	}

	// DLQ (when configured) — catches terminal errors from inner layers.
	if o.dlq != nil {
		mws = append(mws, DLQ(name, o.dlq))
	}

	// Ack (when configured) — always innermost before deliver.
	if o.ackFn != nil {
		mws = append(mws, Ack(o.ackFn))
	}

	// Compose and wrap the adapter's deliver function.
	deliver := o.deliver
	if deliver == nil {
		deliver = nopDeliver
	}
	chain := Stack(mws...)
	return chain(deliver)
}

// StartLoop reads events from the channel and calls the middleware chain for
// each event. It returns nil when the channel is closed or ctx.Err() when the
// context is cancelled.
func StartLoop(ctx context.Context, events <-chan event.Event, deliver DeliverFunc, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-events:
			if !ok {
				return nil
			}
			// The middleware chain handles all error paths (retry, DLQ, ack).
			// Errors returned here are fatal (context cancelled).
			if err := deliver(ctx, ev); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				// Non-context errors from the chain are terminal delivery failures.
				// They've already been handled (DLQ'd, acked). Log and continue.
				logger.Warn("delivery chain error", "event_id", ev.ID, "error", err)
			}
		}
	}
}
