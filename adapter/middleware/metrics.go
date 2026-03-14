package middleware

import (
	"context"
	"time"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/metrics"
)

// Metrics wraps a DeliverFunc with delivery metrics: delivered count and
// delivery duration histogram.
func Metrics(adapterName string) Middleware {
	return func(next DeliverFunc) DeliverFunc {
		return func(ctx context.Context, ev event.Event) error {
			start := time.Now()
			err := next(ctx, ev)
			duration := time.Since(start).Seconds()

			metrics.MiddlewareDeliveryDuration.WithLabelValues(adapterName).Observe(duration)
			if err == nil {
				metrics.EventsDelivered.WithLabelValues(adapterName).Inc()
				if !ev.CreatedAt.IsZero() {
					metrics.EventDeliveryLag.WithLabelValues(adapterName).Observe(time.Since(ev.CreatedAt).Seconds())
				}
			}
			return err
		}
	}
}
