package pgcdc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/florinutz/pgcdc/ack"
	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/adapter/batch"
	"github.com/florinutz/pgcdc/adapter/middleware"
	"github.com/florinutz/pgcdc/backpressure"
	"github.com/florinutz/pgcdc/bus"
	"github.com/florinutz/pgcdc/checkpoint"
	"github.com/florinutz/pgcdc/detector"
	"github.com/florinutz/pgcdc/dlq"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/health"
	"github.com/florinutz/pgcdc/inspect"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/pgcdcerr"
	"github.com/florinutz/pgcdc/schema"
	"github.com/florinutz/pgcdc/snapshot"
	"github.com/florinutz/pgcdc/transform"
	"golang.org/x/sync/errgroup"
)

// CooperativeCheckpointer is implemented by detectors that support cooperative
// checkpointing. When enabled, the detector uses the provided function to
// determine the LSN reported to PostgreSQL, preventing WAL recycling past what
// all adapters have confirmed.
type CooperativeCheckpointer interface {
	SetCooperativeLSN(fn func() uint64)
}

// Throttleable is implemented by detectors that support source-aware
// backpressure. The controller throttles or pauses the detector when WAL
// lag exceeds configured thresholds.
type Throttleable interface {
	SetBackpressureController(ctrl *backpressure.Controller)
}

// Traceable is implemented by detectors that support OpenTelemetry tracing.
// When enabled, the detector creates a PRODUCER span per event and stores
// its SpanContext on the event for downstream correlation.
type Traceable interface {
	SetTracer(t trace.Tracer)
}

// wrapperConfig bundles the transform chain and route filter for a single adapter.
// Stored behind an atomic.Pointer so the wrapSubscription goroutine always sees
// a consistent pair without a mutex on the hot path.
type wrapperConfig struct {
	transformFn transform.TransformFunc
	routeFilter bus.FilterFunc
}

// ReloadConfig carries the new transforms and routes for a hot-reload.
// CLI-flag transforms and plugin transforms should be included by the caller —
// the Pipeline has no knowledge of their origin.
type ReloadConfig struct {
	Transforms        []transform.TransformFunc            // global transforms (CLI + plugin + YAML)
	AdapterTransforms map[string][]transform.TransformFunc // per-adapter transforms (plugin + YAML)
	Routes            map[string][]string                  // adapter -> allowed channels
}

// Pipeline orchestrates a detector, bus, and set of adapters into a running
// event pipeline. It is the primary entry point for using pgcdc as a library.
type Pipeline struct {
	detector          detector.Detector
	snapshot          *snapshot.Snapshot
	bus               *bus.Bus
	adapters          []adapter.Adapter
	health            *health.Checker
	logger            *slog.Logger
	busBuffer         int
	busMode           bus.BusMode
	checkpointStore   checkpoint.Store
	dlq               dlq.DLQ
	routes            map[string][]string                  // adapter name -> allowed channels
	transforms        []transform.TransformFunc            // global transforms
	adapterTransforms map[string][]transform.TransformFunc // per-adapter transforms

	// Cooperative checkpointing fields.
	cooperativeCheckpoint bool
	ackTracker            *ack.Tracker
	autoAckAdapters       map[string]bool // adapter name -> true if auto-acked on channel send

	// Backpressure controller (nil when disabled).
	backpressureCtrl *backpressure.Controller

	// Hot-reload: atomic wrapper configs per adapter (name -> pointer).
	wrapperConfigs map[string]*atomic.Pointer[wrapperConfig]

	// OpenTelemetry tracer provider (noop when not configured).
	tracerProvider trace.TracerProvider

	// Startup validation: skip Validate() calls on adapters.
	skipValidation bool

	// Shutdown timeout for graceful drain.
	shutdownTimeout time.Duration

	// Per-adapter middleware configuration.
	middlewareConfigs map[string]middleware.Config

	// Inspector for event sampling at tap points.
	inspector *inspect.Inspector

	// Schema store for versioned schema tracking.
	schemaStore schema.Store

	// Nack window configuration for adaptive DLQ.
	nackWindowSize      int
	nackWindowThreshold int

	// Per-pipeline name for metric labeling.
	pipelineName string
}

// Option configures a Pipeline.
type Option func(*Pipeline)

// WithAdapter adds an adapter to the pipeline.
func WithAdapter(a adapter.Adapter) Option {
	return func(p *Pipeline) {
		p.adapters = append(p.adapters, a)
	}
}

// WithBusBuffer sets the bus and subscriber channel buffer size.
// If not set or <= 0, the bus default (1024) is used.
func WithBusBuffer(size int) Option {
	return func(p *Pipeline) {
		p.busBuffer = size
	}
}

// WithBusMode sets the bus fan-out mode (fast or reliable).
// BusModeFast (default): non-blocking sends, drops events on full channels.
// BusModeReliable: blocking sends, no drops, back-pressures the detector.
func WithBusMode(mode bus.BusMode) Option {
	return func(p *Pipeline) {
		p.busMode = mode
	}
}

// WithLogger sets the logger for the pipeline and bus.
// If not set, slog.Default() is used.
func WithLogger(l *slog.Logger) Option {
	return func(p *Pipeline) {
		p.logger = l
	}
}

// WithHealthChecker sets the health checker for the pipeline.
// If not set, a new checker is created with "detector" and "bus" registered.
func WithHealthChecker(c *health.Checker) Option {
	return func(p *Pipeline) {
		p.health = c
	}
}

// WithCheckpointStore sets the checkpoint store for LSN persistence.
// The pipeline will close the store on shutdown.
func WithCheckpointStore(s checkpoint.Store) Option {
	return func(p *Pipeline) {
		p.checkpointStore = s
	}
}

// WithDLQ sets the dead letter queue for failed event delivery.
func WithDLQ(d dlq.DLQ) Option {
	return func(p *Pipeline) {
		p.dlq = d
	}
}

// WithRoute restricts an adapter to specific channels. Events on channels not
// in the list are skipped for that adapter. If no route is set, the adapter
// receives all events.
func WithRoute(adapterName string, channels ...string) Option {
	return func(p *Pipeline) {
		if p.routes == nil {
			p.routes = make(map[string][]string)
		}
		p.routes[adapterName] = channels
	}
}

// WithTransform appends a global transform that runs for all adapters.
func WithTransform(fn transform.TransformFunc) Option {
	return func(p *Pipeline) {
		p.transforms = append(p.transforms, fn)
	}
}

// WithAdapterTransform appends a transform that runs only for the named adapter.
func WithAdapterTransform(adapterName string, fn transform.TransformFunc) Option {
	return func(p *Pipeline) {
		if p.adapterTransforms == nil {
			p.adapterTransforms = make(map[string][]transform.TransformFunc)
		}
		p.adapterTransforms[adapterName] = append(p.adapterTransforms[adapterName], fn)
	}
}

// WithCooperativeCheckpoint enables cooperative checkpointing: the WAL
// detector only reports LSN positions that all adapters have acknowledged.
// Requires --persistent-slot and WAL detector (see cmd/listen.go validation).
func WithCooperativeCheckpoint(enabled bool) Option {
	return func(p *Pipeline) {
		p.cooperativeCheckpoint = enabled
	}
}

// WithBackpressure sets the backpressure controller for the pipeline.
// The controller monitors WAL lag and throttles/pauses the detector when
// thresholds are exceeded.
func WithBackpressure(ctrl *backpressure.Controller) Option {
	return func(p *Pipeline) {
		p.backpressureCtrl = ctrl
	}
}

// WithTracerProvider sets the OpenTelemetry TracerProvider for the pipeline.
// If not set, a noop provider is used (zero overhead).
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(p *Pipeline) {
		p.tracerProvider = tp
	}
}

// WithSkipValidation disables adapter startup validation.
func WithSkipValidation(skip bool) Option {
	return func(p *Pipeline) {
		p.skipValidation = skip
	}
}

// WithShutdownTimeout sets the timeout for graceful adapter drain on shutdown.
// Defaults to 5s.
func WithShutdownTimeout(d time.Duration) Option {
	return func(p *Pipeline) {
		p.shutdownTimeout = d
	}
}

// WithMiddlewareConfig sets the middleware configuration for a specific adapter.
// Adapters implementing adapter.Deliverer will use this config for their
// middleware stack (retry, circuit breaker, rate limiting).
func WithMiddlewareConfig(adapterName string, cfg middleware.Config) Option {
	return func(p *Pipeline) {
		if p.middlewareConfigs == nil {
			p.middlewareConfigs = make(map[string]middleware.Config)
		}
		p.middlewareConfigs[adapterName] = cfg
	}
}

// WithInspector sets the event inspector for pipeline tap-point sampling.
// The inspector records events at post-detector, post-transform, and
// pre-adapter stages for debugging and introspection.
func WithInspector(insp *inspect.Inspector) Option {
	return func(p *Pipeline) {
		p.inspector = insp
	}
}

// WithSchemaStore sets the schema store for versioned schema tracking.
// When set, the WAL detector will register schema changes as they arrive.
func WithSchemaStore(s schema.Store) Option {
	return func(p *Pipeline) {
		p.schemaStore = s
	}
}

// WithNackWindow configures the nack window for adaptive DLQ monitoring.
// When the number of nacks in the sliding window exceeds the threshold,
// the adapter is paused.
func WithNackWindow(size, threshold int) Option {
	return func(p *Pipeline) {
		p.nackWindowSize = size
		p.nackWindowThreshold = threshold
	}
}

// WithPipelineName sets the pipeline name used for per-pipeline metric labels.
func WithPipelineName(name string) Option {
	return func(p *Pipeline) {
		p.pipelineName = name
	}
}

// SchemaStoreAware is implemented by detectors that support schema versioning.
// When a schema store is set on the pipeline, it is passed to the detector.
type SchemaStoreAware interface {
	SetSchemaStore(s schema.Store)
}

// NewPipeline creates a Pipeline that will use the given detector and options.
// Call Run to start the pipeline.
func NewPipeline(det detector.Detector, opts ...Option) *Pipeline {
	p := &Pipeline{
		detector:        det,
		shutdownTimeout: 5 * time.Second,
	}
	for _, o := range opts {
		o(p)
	}
	if p.logger == nil {
		p.logger = slog.Default()
	}
	if p.tracerProvider == nil {
		p.tracerProvider = noop.NewTracerProvider()
	}
	if p.health == nil {
		p.health = health.NewChecker()
		p.health.Register("detector")
		p.health.Register("bus")
	}

	// Wire tracer and DLQ into adapters.
	// Deliverer/Batcher adapters get these via middleware/batch runner instead.
	tracer := p.tracerProvider.Tracer("github.com/florinutz/pgcdc")
	for _, a := range p.adapters {
		p.health.Register(a.Name())
		isMiddlewareManaged := false
		if _, ok := a.(adapter.Deliverer); ok {
			isMiddlewareManaged = true
		}
		if _, ok := a.(adapter.Batcher); ok {
			isMiddlewareManaged = true
		}
		if !isMiddlewareManaged {
			if da, ok := a.(adapter.DLQAware); ok && p.dlq != nil {
				da.SetDLQ(p.dlq)
			}
			if ta, ok := a.(adapter.Traceable); ok {
				ta.SetTracer(tracer)
			}
		}
	}
	if p.backpressureCtrl != nil {
		p.health.Register("backpressure")
	}

	// Set up cooperative checkpointing.
	// Deliverer/Batcher adapters get ack via middleware/batch runner.
	if p.cooperativeCheckpoint {
		p.ackTracker = ack.New()
		p.autoAckAdapters = make(map[string]bool)
		for _, a := range p.adapters {
			p.ackTracker.Register(a.Name(), 0)

			// Middleware-managed adapters get ack from startAdapter.
			isMiddlewareManaged := false
			if _, ok := a.(adapter.Deliverer); ok {
				isMiddlewareManaged = true
			}
			if _, ok := a.(adapter.Batcher); ok {
				isMiddlewareManaged = true
			}
			if isMiddlewareManaged {
				continue
			}

			if ackable, ok := a.(adapter.Acknowledger); ok {
				name := a.Name()
				ackable.SetAckFunc(func(lsn uint64) {
					p.ackTracker.Ack(name, lsn)
				})
			} else {
				p.logger.Warn("adapter does not implement Acknowledger; auto-acking on channel send",
					slog.String("adapter", a.Name()))
				p.autoAckAdapters[a.Name()] = true
			}
		}
	}

	// Pre-populate wrapperConfigs for all adapters. The map is immutable after
	// NewPipeline returns: subscribeAdapter only stores into existing pointers,
	// and Reload only iterates existing entries. This prevents a data race
	// between Run (which calls subscribeAdapter) and Reload.
	p.wrapperConfigs = make(map[string]*atomic.Pointer[wrapperConfig], len(p.adapters))
	for _, a := range p.adapters {
		cfgPtr := &atomic.Pointer[wrapperConfig]{}
		cfgPtr.Store(&wrapperConfig{}) // empty initial config; subscribeAdapter will overwrite
		p.wrapperConfigs[a.Name()] = cfgPtr
	}
	p.bus = bus.New(p.busBuffer, p.logger)
	p.bus.SetMode(p.busMode)
	return p
}

// Validate runs startup validation on all adapters that implement the Validator
// interface. Returns the first error encountered, or nil if all pass. Skipped
// when WithSkipValidation(true) is set.
func (p *Pipeline) Validate(ctx context.Context) error {
	if p.skipValidation {
		p.logger.Info("adapter validation skipped")
		return nil
	}
	for _, a := range p.adapters {
		v, ok := a.(adapter.Validator)
		if !ok {
			continue
		}
		start := time.Now()
		if err := v.Validate(ctx); err != nil {
			metrics.ValidationDuration.WithLabelValues(a.Name()).Observe(time.Since(start).Seconds())
			return &pgcdcerr.ValidationError{Adapter: a.Name(), Err: err}
		}
		metrics.ValidationDuration.WithLabelValues(a.Name()).Observe(time.Since(start).Seconds())
		p.logger.Info("adapter validated", "adapter", a.Name(), "duration", time.Since(start))
	}
	return nil
}

// safeGo wraps errgroup.Go with panic recovery. If the function panics,
// the panic is recovered, logged, and returned as an error.
func (p *Pipeline) safeGo(g *errgroup.Group, name string, fn func() error) {
	g.Go(func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				metrics.PanicsRecovered.WithLabelValues(name).Inc()
				p.logger.Error("panic recovered",
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

// drainAdapters calls Drain() on adapters that implement the Drainer interface.
func (p *Pipeline) drainAdapters() {
	ctx, cancel := context.WithTimeout(context.Background(), p.shutdownTimeout)
	defer cancel()

	for _, a := range p.adapters {
		d, ok := a.(adapter.Drainer)
		if !ok {
			continue
		}
		start := time.Now()
		if err := d.Drain(ctx); err != nil {
			p.logger.Error("adapter drain failed",
				"adapter", a.Name(),
				"error", err,
				"duration", time.Since(start),
			)
		} else {
			p.logger.Info("adapter drained",
				"adapter", a.Name(),
				"duration", time.Since(start),
			)
		}
	}
}

// Run starts the pipeline and blocks until ctx is cancelled or a fatal error
// occurs. Context cancellation triggers graceful shutdown. The returned error
// is nil on clean shutdown (context.Canceled).
func (p *Pipeline) Run(ctx context.Context) error {
	if p.checkpointStore != nil {
		defer func() { _ = p.checkpointStore.Close() }()
	}

	// Run startup validation.
	if err := p.Validate(ctx); err != nil {
		return fmt.Errorf("startup validation: %w", err)
	}

	// Wire cooperative checkpointing into the detector if supported.
	if p.cooperativeCheckpoint && p.ackTracker != nil {
		if cc, ok := p.detector.(CooperativeCheckpointer); ok {
			cc.SetCooperativeLSN(p.ackTracker.MinAckedLSN)
		}
	}

	// Wire backpressure controller into the detector if supported.
	if p.backpressureCtrl != nil {
		if t, ok := p.detector.(Throttleable); ok {
			t.SetBackpressureController(p.backpressureCtrl)
		}
	}

	// Wire tracer into detector if supported.
	if t, ok := p.detector.(Traceable); ok {
		tracer := p.tracerProvider.Tracer("github.com/florinutz/pgcdc")
		t.SetTracer(tracer)
	}

	// Wire schema store into detector if supported.
	if p.schemaStore != nil {
		if sa, ok := p.detector.(SchemaStoreAware); ok {
			sa.SetSchemaStore(p.schemaStore)
		}
	}

	g, gCtx := errgroup.WithContext(ctx)

	// Start backpressure controller if configured.
	if p.backpressureCtrl != nil {
		p.safeGo(g, "backpressure", func() error {
			return p.backpressureCtrl.Run(gCtx)
		})
	}

	// Start the bus.
	p.safeGo(g, "bus", func() error {
		p.logger.Info("bus started", "buffer_size", p.busBuffer)
		p.health.SetStatus("bus", health.StatusUp)
		defer p.health.SetStatus("bus", health.StatusDown)
		return p.bus.Start(gCtx)
	})

	// Start the detector.
	p.safeGo(g, p.detector.Name(), func() error {
		p.logger.Info("detector started", "detector", p.detector.Name())
		p.health.SetStatus("detector", health.StatusUp)
		defer p.health.SetStatus("detector", health.StatusDown)
		return p.detector.Start(gCtx, p.bus.Ingest())
	})

	// Wire Reinjector adapters (e.g. view adapter) before starting.
	for _, a := range p.adapters {
		if r, ok := a.(adapter.Reinjector); ok {
			r.SetIngestChan(p.bus.Ingest())
		}
	}

	// Subscribe and start each adapter.
	for _, a := range p.adapters {
		sub, err := p.subscribeAdapter(a.Name())
		if err != nil {
			return fmt.Errorf("subscribe adapter %s: %w", a.Name(), err)
		}
		p.safeGo(g, a.Name(), func() error {
			p.logger.Info("adapter started", "adapter", a.Name())
			p.health.SetStatus(a.Name(), health.StatusUp)
			defer p.health.SetStatus(a.Name(), health.StatusDown)
			return p.startAdapter(gCtx, a, sub)
		})
	}

	err := g.Wait()

	// Drain adapters with in-flight work.
	p.drainAdapters()

	// context.Canceled is expected on clean shutdown.
	if err != nil && gCtx.Err() != nil {
		return nil
	}
	return err
}

// Bus returns the underlying bus for advanced use cases such as subscribing
// custom consumers.
func (p *Pipeline) Bus() *bus.Bus {
	return p.bus
}

// Health returns the pipeline's health checker.
func (p *Pipeline) Health() *health.Checker {
	return p.health
}

// DLQ returns the pipeline's dead letter queue, or nil if not configured.
func (p *Pipeline) DLQ() dlq.DLQ {
	return p.dlq
}

// Inspector returns the pipeline's event inspector, or nil if not configured.
func (p *Pipeline) Inspector() *inspect.Inspector {
	return p.inspector
}

// channelFilter returns a bus.FilterFunc that passes only events whose channel
// is in the allowed set. Returns nil if the set is empty.
func channelFilter(channels []string) bus.FilterFunc {
	if len(channels) == 0 {
		return nil
	}
	allowed := make(map[string]struct{}, len(channels))
	for _, c := range channels {
		allowed[c] = struct{}{}
	}
	return func(ev event.Event) bool {
		_, ok := allowed[ev.Channel]
		return ok
	}
}

// subscribeAdapter creates a bus subscription for an adapter. All route
// filtering is done in the wrapper goroutine (not at bus level) so that
// Reload() can swap routes atomically. The wrapper is always started to
// support hot-reload even when no transforms or routes are initially configured.
func (p *Pipeline) subscribeAdapter(name string) (<-chan event.Event, error) {
	// Always plain subscribe — route filtering is in the wrapper.
	ch, err := p.bus.Subscribe(name)
	if err != nil {
		return nil, err
	}

	// Build initial wrapperConfig and store into the pre-allocated pointer.
	wcfg := &wrapperConfig{
		transformFn: p.buildTransformChain(name),
		routeFilter: channelFilter(p.routes[name]),
	}
	cfgPtr := p.wrapperConfigs[name]
	cfgPtr.Store(wcfg)

	autoAck := p.autoAckAdapters[name]
	return p.wrapSubscription(name, ch, cfgPtr, autoAck), nil
}

// buildTransformChain returns a composed TransformFunc for the named adapter by
// concatenating global transforms + per-adapter transforms. Returns nil if no
// transforms are configured.
func (p *Pipeline) buildTransformChain(name string) transform.TransformFunc {
	fns := make([]transform.TransformFunc, 0, len(p.transforms)+len(p.adapterTransforms[name]))
	fns = append(fns, p.transforms...)
	fns = append(fns, p.adapterTransforms[name]...)
	return transform.Chain(fns...)
}

// wrapSubscription spawns a goroutine that reads from inCh, loads the current
// wrapperConfig atomically on each event, applies route filtering and transforms,
// and writes surviving events to outCh. The atomic pointer allows Reload() to
// swap transforms and routes with zero event loss.
func (p *Pipeline) wrapSubscription(name string, inCh <-chan event.Event, cfgPtr *atomic.Pointer[wrapperConfig], autoAck bool) <-chan event.Event {
	// Create per-adapter nack window if configured.
	var nackWin *dlq.NackWindow
	if p.nackWindowSize > 0 {
		nackWin = dlq.NewNackWindow(p.nackWindowSize, p.nackWindowThreshold)
	}

	out := make(chan event.Event, cap(inCh))
	go func() {
		defer close(out)
		for ev := range inCh {
			// Inspector: record post-detector event.
			if p.inspector != nil {
				p.inspector.Record(inspect.TapPostDetector, ev)
			}

			// Load the current config atomically.
			wcfg := cfgPtr.Load()

			// Save the original LSN before transforms may alter it.
			origLSN := ev.LSN

			// Route filter: auto-ack filtered (dropped) events.
			if wcfg.routeFilter != nil && !wcfg.routeFilter(ev) {
				if p.ackTracker != nil && origLSN > 0 {
					p.ackTracker.Ack(name, origLSN)
				}
				continue
			}

			// Backpressure load shedding: auto-ack and skip shed adapters.
			if p.backpressureCtrl != nil && p.backpressureCtrl.IsShed(name) {
				if p.ackTracker != nil && origLSN > 0 {
					p.ackTracker.Ack(name, origLSN)
				}
				metrics.BackpressureLoadShed.WithLabelValues(name).Inc()
				continue
			}

			// Nack window: skip events when adapter is paused due to excessive nacks.
			if nackWin != nil && nackWin.Exceeded() {
				if p.ackTracker != nil && origLSN > 0 {
					p.ackTracker.Ack(name, origLSN)
				}
				metrics.NackWindowExceeded.WithLabelValues(name).Inc()
				continue
			}

			// Transform chain: auto-ack dropped/errored events.
			if wcfg.transformFn != nil {
				result, err := wcfg.transformFn(ev)
				if err != nil {
					if errors.Is(err, transform.ErrDropEvent) {
						metrics.TransformDropped.WithLabelValues(name).Inc()
					} else {
						metrics.TransformErrors.WithLabelValues(name).Inc()
						p.logger.Warn("transform error, skipping event",
							slog.String("adapter", name),
							slog.String("event_id", ev.ID),
							slog.String("error", err.Error()),
						)
					}
					if nackWin != nil {
						nackWin.RecordNack()
					}
					if p.ackTracker != nil && origLSN > 0 {
						p.ackTracker.Ack(name, origLSN)
					}
					continue
				}
				ev = result
			}

			// Inspector: record post-transform event.
			if p.inspector != nil {
				p.inspector.Record(inspect.TapPostTransform, ev)
			}

			// Inspector: record pre-adapter event.
			if p.inspector != nil {
				p.inspector.Record(inspect.TapPreAdapter, ev)
			}

			if nackWin != nil {
				nackWin.RecordAck()
			}

			out <- ev

			// Auto-ack for non-Acknowledger adapters: ack on channel send.
			if autoAck && p.ackTracker != nil && origLSN > 0 {
				p.ackTracker.Ack(name, origLSN)
			}
		}
	}()
	return out
}

// startAdapter dispatches to the appropriate execution strategy:
//  1. Batcher adapters → batch.Runner (timer/size flush, DLQ, ack)
//  2. Deliverer adapters → middleware chain (retry, CB, RL, DLQ, tracing, ack)
//  3. Legacy adapters → direct Start() call (SSE, WS, gRPC, kafkaserver, view)
func (p *Pipeline) startAdapter(ctx context.Context, a adapter.Adapter, events <-chan event.Event) error {
	tracer := p.tracerProvider.Tracer("github.com/florinutz/pgcdc")

	// Path 1: Batcher adapters get the batch runner.
	if b, ok := a.(adapter.Batcher); ok {
		cfg := b.BatchConfig()
		runner := batch.New(a.Name(), b, cfg.MaxSize, cfg.FlushInterval, p.logger)
		if p.dlq != nil {
			runner.SetDLQ(p.dlq)
		}
		if p.cooperativeCheckpoint && p.ackTracker != nil {
			name := a.Name()
			runner.SetAckFunc(func(lsn uint64) {
				p.ackTracker.Ack(name, lsn)
			})
		}
		return runner.Start(ctx, events)
	}

	// Path 2: Deliverer adapters get the middleware chain.
	if d, ok := a.(adapter.Deliverer); ok {
		mwCfg := p.middlewareConfigs[a.Name()]

		var mwOpts []middleware.Option
		mwOpts = append(mwOpts, middleware.WithDeliver(d.Deliver))
		mwOpts = append(mwOpts, middleware.WithLogger(p.logger.With("adapter", a.Name())))
		mwOpts = append(mwOpts, middleware.WithTracer(tracer))
		if p.dlq != nil {
			mwOpts = append(mwOpts, middleware.WithDLQ(p.dlq))
		}
		if p.cooperativeCheckpoint && p.ackTracker != nil {
			name := a.Name()
			mwOpts = append(mwOpts, middleware.WithAckFunc(func(lsn uint64) {
				p.ackTracker.Ack(name, lsn)
			}))
		}

		chain := middleware.Build(a.Name(), mwCfg, mwOpts...)
		return middleware.StartLoop(ctx, events, chain, p.logger.With("adapter", a.Name()))
	}

	// Path 3: Legacy adapters (brokers/servers) use Start() directly.
	return a.Start(ctx, events)
}

// Reload atomically swaps the transform chains and route filters for all
// running adapters. It is safe to call from any goroutine (e.g. a SIGHUP
// handler). Unknown adapter names in the config are silently ignored.
func (p *Pipeline) Reload(cfg ReloadConfig) error {
	reloaded := 0
	for name, cfgPtr := range p.wrapperConfigs {
		// Build per-adapter transform chain: global + adapter-specific.
		fns := make([]transform.TransformFunc, 0, len(cfg.Transforms)+len(cfg.AdapterTransforms[name]))
		fns = append(fns, cfg.Transforms...)
		fns = append(fns, cfg.AdapterTransforms[name]...)

		wcfg := &wrapperConfig{
			transformFn: transform.Chain(fns...),
			routeFilter: channelFilter(cfg.Routes[name]),
		}
		cfgPtr.Store(wcfg)
		reloaded++
	}

	metrics.ConfigReloads.Inc()
	p.logger.Info("pipeline config reloaded",
		slog.Int("adapters", reloaded),
		slog.Int("global_transforms", len(cfg.Transforms)),
		slog.Int("routes", len(cfg.Routes)),
	)
	return nil
}

// NewSnapshotPipeline creates a Pipeline that uses a snapshot as the event
// source instead of a live detector. Call RunSnapshot to start.
func NewSnapshotPipeline(snap *snapshot.Snapshot, opts ...Option) *Pipeline {
	p := &Pipeline{
		snapshot: snap,
	}
	for _, o := range opts {
		o(p)
	}
	if p.logger == nil {
		p.logger = slog.Default()
	}
	if p.health == nil {
		p.health = health.NewChecker()
		p.health.Register("snapshot")
		p.health.Register("bus")
	}
	for _, a := range p.adapters {
		p.health.Register(a.Name())
		if da, ok := a.(adapter.DLQAware); ok && p.dlq != nil {
			da.SetDLQ(p.dlq)
		}
	}
	p.bus = bus.New(p.busBuffer, p.logger)
	p.bus.SetMode(p.busMode)
	return p
}

// RunSnapshot runs the snapshot pipeline: snapshot feeds events into the bus,
// which fans out to adapters. Returns when the snapshot is complete and all
// events have been delivered, or when ctx is cancelled.
func (p *Pipeline) RunSnapshot(ctx context.Context) error {
	// Create a cancellable context so we can shut down after the snapshot finishes.
	snapCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	g, gCtx := errgroup.WithContext(snapCtx)

	// Ingest channel for the snapshot to push events into.
	ingest := p.bus.Ingest()

	// Start the bus.
	p.safeGo(g, "bus", func() error {
		p.logger.Info("bus started", "buffer_size", p.busBuffer)
		p.health.SetStatus("bus", health.StatusUp)
		defer p.health.SetStatus("bus", health.StatusDown)
		return p.bus.Start(gCtx)
	})

	// Subscribe and start each adapter.
	for _, a := range p.adapters {
		sub, err := p.subscribeAdapter(a.Name())
		if err != nil {
			return fmt.Errorf("subscribe adapter %s: %w", a.Name(), err)
		}
		p.safeGo(g, a.Name(), func() error {
			p.logger.Info("adapter started", "adapter", a.Name())
			p.health.SetStatus(a.Name(), health.StatusUp)
			defer p.health.SetStatus(a.Name(), health.StatusDown)
			return p.startAdapter(gCtx, a, sub)
		})
	}

	// Run the snapshot. When it completes, cancel the context to shut down
	// the bus and adapters.
	p.safeGo(g, "snapshot", func() error {
		p.health.SetStatus("snapshot", health.StatusUp)
		defer p.health.SetStatus("snapshot", health.StatusDown)

		err := p.snapshot.Run(gCtx, ingest)
		// Whether successful or not, cancel to tear down the pipeline.
		cancel()
		return err
	})

	err := g.Wait()
	// context.Canceled is expected on clean shutdown after snapshot finishes.
	if err != nil && gCtx.Err() != nil {
		return nil
	}
	return err
}
