package adapter

import (
	"context"

	"github.com/florinutz/pgcdc/event"
	"go.opentelemetry.io/otel/trace"
)

type Adapter interface {
	Start(ctx context.Context, events <-chan event.Event) error
	Name() string
}

// AckFunc is called by an adapter after it has fully processed an event.
// The lsn argument is the WAL position of the event (event.Event.LSN).
// Adapters must only call this when they are done with the event and do not
// need it again (successful delivery, DLQ record, or intentional skip).
type AckFunc func(lsn uint64)

// Acknowledger is implemented by adapters that support cooperative
// checkpointing. Following the DLQAware pattern, the pipeline injects an
// AckFunc via SetAckFunc when cooperative checkpointing is enabled.
type Acknowledger interface {
	SetAckFunc(fn AckFunc)
}

// Traceable is implemented by adapters that support OpenTelemetry tracing.
// The pipeline injects a tracer when tracing is enabled.
type Traceable interface {
	SetTracer(t trace.Tracer)
}
