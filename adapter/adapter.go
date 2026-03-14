package adapter

import (
	"context"
	"time"

	"github.com/florinutz/pgcdc/dlq"
	"github.com/florinutz/pgcdc/event"
	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel/trace"
)

type Adapter interface {
	Start(ctx context.Context, events <-chan event.Event) error
	Name() string
}

// Deliverer is implemented by adapters that deliver events one at a time.
// Adapters implementing Deliverer get the middleware stack (retry, circuit
// breaker, rate limiting, DLQ, metrics, tracing, ack) for free. The middleware
// provides the Start() event loop — Deliverer adapters should still implement
// Start() as a fallback but the pipeline will prefer the middleware path.
type Deliverer interface {
	Deliver(ctx context.Context, ev event.Event) error
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

// HTTPMountable is optionally implemented by adapters that serve HTTP routes
// (e.g., graphql, duckdb). The pipeline mounts their routes on the shared HTTP server.
type HTTPMountable interface {
	MountHTTP(r chi.Router)
}

// DLQAware is implemented by adapters that can record failed events to a DLQ.
// The pipeline injects a DLQ via SetDLQ when a DLQ backend is configured.
type DLQAware interface {
	SetDLQ(d dlq.DLQ)
}

// Batcher is implemented by adapters that accumulate events and flush them
// in batches (e.g., S3, search). The batch runner provides the event loop,
// timer/size flush triggers, shutdown drain, DLQ, and cooperative ack.
type Batcher interface {
	Flush(ctx context.Context, batch []event.Event) FlushResult
	BatchConfig() BatchConfig
}

// BatchConfig controls batch accumulation behavior.
type BatchConfig struct {
	MaxSize       int           // flush when batch reaches this size
	FlushInterval time.Duration // flush on this timer
}

// FlushResult reports the outcome of a batch flush.
type FlushResult struct {
	Delivered int           // number of events successfully delivered
	Failed    []FailedEvent // individual failures sent to DLQ
	Err       error         // fatal error → retry entire batch
}

// FailedEvent pairs an event with the error that caused its failure.
type FailedEvent struct {
	Event event.Event
	Err   error
}
