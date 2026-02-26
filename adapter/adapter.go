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

// Validator is implemented by adapters that can verify their external
// dependencies (e.g. DNS resolution, connectivity, bucket existence) before
// the pipeline starts. Pipeline calls Validate() after construction; failures
// abort startup unless --skip-validation is set.
type Validator interface {
	Validate(ctx context.Context) error
}

// Drainer is implemented by adapters that need to flush in-flight work during
// graceful shutdown. Pipeline calls Drain() after the errgroup returns, with a
// context bounded by shutdown_timeout.
type Drainer interface {
	Drain(ctx context.Context) error
}

// Reinjector is implemented by adapters that produce events back into the bus
// (e.g. the view adapter emits VIEW_RESULT events). The pipeline injects the
// bus ingest channel before starting adapters.
type Reinjector interface {
	SetIngestChan(ch chan<- event.Event)
}
