package pgcdc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/florinutz/pgcdc/ack"
	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/backpressure"
	"github.com/florinutz/pgcdc/bus"
	"github.com/florinutz/pgcdc/checkpoint"
	"github.com/florinutz/pgcdc/detector"
	"github.com/florinutz/pgcdc/dlq"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/health"
	"github.com/florinutz/pgcdc/metrics"
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

// DLQAware is implemented by adapters that can record failed events to a DLQ.
type DLQAware interface {
	SetDLQ(d dlq.DLQ)
}

// NewPipeline creates a Pipeline that will use the given detector and options.
// Call Run to start the pipeline.
func NewPipeline(det detector.Detector, opts ...Option) *Pipeline {
	p := &Pipeline{
		detector: det,
	}
	for _, o := range opts {
		o(p)
	}
	if p.logger == nil {
		p.logger = slog.Default()
	}
	if p.health == nil {
		p.health = health.NewChecker()
		p.health.Register("detector")
		p.health.Register("bus")
	}
	for _, a := range p.adapters {
		p.health.Register(a.Name())
		if da, ok := a.(DLQAware); ok && p.dlq != nil {
			da.SetDLQ(p.dlq)
		}
	}
	if p.backpressureCtrl != nil {
		p.health.Register("backpressure")
	}

	// Set up cooperative checkpointing.
	if p.cooperativeCheckpoint {
		p.ackTracker = ack.New()
		p.autoAckAdapters = make(map[string]bool)
		for _, a := range p.adapters {
			p.ackTracker.Register(a.Name(), 0)
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

	p.bus = bus.New(p.busBuffer, p.logger)
	p.bus.SetMode(p.busMode)
	return p
}

// Run starts the pipeline and blocks until ctx is cancelled or a fatal error
// occurs. Context cancellation triggers graceful shutdown. The returned error
// is nil on clean shutdown (context.Canceled).
func (p *Pipeline) Run(ctx context.Context) error {
	if p.checkpointStore != nil {
		defer func() { _ = p.checkpointStore.Close() }()
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

	g, gCtx := errgroup.WithContext(ctx)

	// Start backpressure controller if configured.
	if p.backpressureCtrl != nil {
		g.Go(func() error {
			return p.backpressureCtrl.Run(gCtx)
		})
	}

	// Start the bus.
	g.Go(func() error {
		p.logger.Info("bus started", "buffer_size", p.busBuffer)
		p.health.SetStatus("bus", health.StatusUp)
		defer p.health.SetStatus("bus", health.StatusDown)
		return p.bus.Start(gCtx)
	})

	// Start the detector.
	g.Go(func() error {
		p.logger.Info("detector started", "detector", p.detector.Name())
		p.health.SetStatus("detector", health.StatusUp)
		defer p.health.SetStatus("detector", health.StatusDown)
		return p.detector.Start(gCtx, p.bus.Ingest())
	})

	// Subscribe and start each adapter.
	for _, a := range p.adapters {
		sub, err := p.subscribeAdapter(a.Name())
		if err != nil {
			return fmt.Errorf("subscribe adapter %s: %w", a.Name(), err)
		}
		g.Go(func() error {
			p.logger.Info("adapter started", "adapter", a.Name())
			p.health.SetStatus(a.Name(), health.StatusUp)
			defer p.health.SetStatus(a.Name(), health.StatusDown)
			return a.Start(gCtx, sub)
		})
	}

	err := g.Wait()

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

// subscribeAdapter creates a bus subscription for an adapter, applying a route
// filter if one has been configured for that adapter name. If transforms are
// configured (global or per-adapter), the subscription channel is wrapped with
// a transform goroutine. When cooperative checkpointing is active, route
// filtering is moved to the wrapper so filtered events can be auto-acked.
func (p *Pipeline) subscribeAdapter(name string) (<-chan event.Event, error) {
	var ch <-chan event.Event
	var err error
	var routeFilter bus.FilterFunc

	channels, hasRoute := p.routes[name]
	if hasRoute && len(channels) > 0 {
		if p.ackTracker != nil {
			// Cooperative checkpoint active: move filter to wrapper so filtered
			// events can be auto-acked before being dropped.
			ch, err = p.bus.Subscribe(name)
			routeFilter = channelFilter(channels)
		} else {
			// No cooperative checkpoint: filter at bus level (efficient).
			ch, err = p.bus.SubscribeWithFilter(name, channelFilter(channels))
		}
	} else {
		ch, err = p.bus.Subscribe(name)
	}
	if err != nil {
		return nil, err
	}

	fn := p.buildTransformChain(name)
	autoAck := p.autoAckAdapters[name]

	if fn == nil && routeFilter == nil && !autoAck {
		return ch, nil
	}

	return p.wrapSubscription(name, ch, routeFilter, fn, autoAck), nil
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

// wrapSubscription spawns a goroutine that reads from inCh, applies optional
// route filtering (auto-acking filtered events), applies an optional transform
// chain (auto-acking dropped/errored events), and writes to outCh. When
// autoAck is true, events forwarded to outCh are immediately acked (for
// non-Acknowledger adapters in cooperative checkpoint mode).
func (p *Pipeline) wrapSubscription(name string, inCh <-chan event.Event, routeFilter bus.FilterFunc, fn transform.TransformFunc, autoAck bool) <-chan event.Event {
	out := make(chan event.Event, cap(inCh))
	go func() {
		defer close(out)
		for ev := range inCh {
			// Save the original LSN before transforms may alter it.
			origLSN := ev.LSN

			// Route filter: auto-ack filtered (dropped) events.
			if routeFilter != nil && !routeFilter(ev) {
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

			// Transform chain: auto-ack dropped/errored events.
			if fn != nil {
				result, err := fn(ev)
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
					if p.ackTracker != nil && origLSN > 0 {
						p.ackTracker.Ack(name, origLSN)
					}
					continue
				}
				ev = result
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
		if da, ok := a.(DLQAware); ok && p.dlq != nil {
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
	g.Go(func() error {
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
		g.Go(func() error {
			p.logger.Info("adapter started", "adapter", a.Name())
			p.health.SetStatus(a.Name(), health.StatusUp)
			defer p.health.SetStatus(a.Name(), health.StatusDown)
			return a.Start(gCtx, sub)
		})
	}

	// Run the snapshot. When it completes, cancel the context to shut down
	// the bus and adapters.
	g.Go(func() error {
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
