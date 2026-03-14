package middleware

import (
	"context"
	"fmt"
	"time"

	"github.com/florinutz/pgcdc/event"
)

// Timeout wraps a DeliverFunc with a context timeout. If the inner function
// takes longer than d, the context is cancelled and a timeout error is returned.
func Timeout(d time.Duration) Middleware {
	return func(next DeliverFunc) DeliverFunc {
		return func(ctx context.Context, ev event.Event) error {
			tctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()
			err := next(tctx, ev)
			if tctx.Err() == context.DeadlineExceeded && ctx.Err() == nil {
				return fmt.Errorf("delivery timeout after %s", d)
			}
			return err
		}
	}
}
