package batch

import (
	"context"
	"log/slog"
	"time"

	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/dlq"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/metrics"
)

// Runner wraps a Batcher adapter with accumulation, timer/size flush triggers,
// shutdown drain, DLQ for failed events, and cooperative checkpoint ack at
// batch boundaries.
type Runner struct {
	name          string
	batcher       adapter.Batcher
	maxSize       int
	flushInterval time.Duration
	logger        *slog.Logger
	dlq           dlq.DLQ
	ackFn         adapter.AckFunc
}

// New creates a batch Runner for the given Batcher.
func New(name string, b adapter.Batcher, maxSize int, flushInterval time.Duration, logger *slog.Logger) *Runner {
	if logger == nil {
		logger = slog.Default()
	}
	if maxSize <= 0 {
		maxSize = 1000
	}
	if flushInterval <= 0 {
		flushInterval = 1 * time.Minute
	}
	return &Runner{
		name:          name,
		batcher:       b,
		maxSize:       maxSize,
		flushInterval: flushInterval,
		logger:        logger.With("adapter", name),
	}
}

// SetDLQ sets the dead letter queue for failed events in a batch.
func (r *Runner) SetDLQ(d dlq.DLQ) { r.dlq = d }

// SetAckFunc sets the cooperative checkpoint acknowledgement function.
func (r *Runner) SetAckFunc(fn adapter.AckFunc) { r.ackFn = fn }

// Start consumes events from the channel, accumulates them, and flushes on
// size trigger, timer trigger, or shutdown.
func (r *Runner) Start(ctx context.Context, events <-chan event.Event) error {
	r.logger.Info("batch runner started",
		"max_size", r.maxSize,
		"flush_interval", r.flushInterval,
	)

	buf := make([]event.Event, 0, r.maxSize)
	ticker := time.NewTicker(r.flushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(buf) == 0 {
			return
		}
		batch := buf
		buf = make([]event.Event, 0, r.maxSize)
		metrics.BatchBufferSize.WithLabelValues(r.name).Set(0)
		r.flushBatch(ctx, batch)
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return ctx.Err()

		case <-ticker.C:
			flush()

		case ev, ok := <-events:
			if !ok {
				flush()
				return nil
			}

			buf = append(buf, ev)
			metrics.BatchBufferSize.WithLabelValues(r.name).Set(float64(len(buf)))

			if len(buf) >= r.maxSize {
				flush()
			}
		}
	}
}

func (r *Runner) flushBatch(ctx context.Context, batch []event.Event) {
	start := time.Now()
	result := r.batcher.Flush(ctx, batch)
	duration := time.Since(start)

	metrics.BatchFlushDuration.WithLabelValues(r.name).Observe(duration.Seconds())
	metrics.BatchFlushSize.WithLabelValues(r.name).Observe(float64(len(batch)))

	if result.Err != nil {
		metrics.BatchFlushes.WithLabelValues(r.name, "error").Inc()
		r.logger.Error("batch flush failed",
			"events", len(batch),
			"error", result.Err,
			"duration", duration,
		)
		// On fatal error, DLQ all events.
		if r.dlq != nil {
			for _, ev := range batch {
				_ = r.dlq.Record(ctx, ev, r.name, result.Err)
			}
		} else {
			metrics.BatchEventsLost.WithLabelValues(r.name).Add(float64(len(batch)))
		}
		// Ack all events (they're handled — either successfully or DLQ'd).
		r.ackBatch(batch)
		return
	}

	metrics.BatchFlushes.WithLabelValues(r.name, "ok").Inc()

	// DLQ individual failures.
	if r.dlq != nil {
		for _, fe := range result.Failed {
			_ = r.dlq.Record(ctx, fe.Event, r.name, fe.Err)
		}
	}

	// Ack entire batch after flush (success + DLQ'd events are all "handled").
	r.ackBatch(batch)

	r.logger.Info("batch flush complete",
		"events", len(batch),
		"delivered", result.Delivered,
		"failed", len(result.Failed),
		"duration", duration,
	)
}

func (r *Runner) ackBatch(batch []event.Event) {
	if r.ackFn == nil {
		return
	}
	for _, ev := range batch {
		if ev.LSN > 0 {
			r.ackFn(ev.LSN)
		}
	}
}
