package pgpipe

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/florinutz/pgpipe/adapter"
	"github.com/florinutz/pgpipe/bus"
	"github.com/florinutz/pgpipe/detector"
	"github.com/florinutz/pgpipe/health"
	"golang.org/x/sync/errgroup"
)

// Pipeline orchestrates a detector, bus, and set of adapters into a running
// event pipeline. It is the primary entry point for using pgpipe as a library.
type Pipeline struct {
	detector  detector.Detector
	bus       *bus.Bus
	adapters  []adapter.Adapter
	health    *health.Checker
	logger    *slog.Logger
	busBuffer int
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
	p.bus = bus.New(p.busBuffer, p.logger)
	return p
}

// Run starts the pipeline and blocks until ctx is cancelled or a fatal error
// occurs. Context cancellation triggers graceful shutdown. The returned error
// is nil on clean shutdown (context.Canceled).
func (p *Pipeline) Run(ctx context.Context) error {
	g, gCtx := errgroup.WithContext(ctx)

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
		sub, err := p.bus.Subscribe(a.Name())
		if err != nil {
			return fmt.Errorf("subscribe adapter %s: %w", a.Name(), err)
		}
		g.Go(func() error {
			p.logger.Info("adapter started", "adapter", a.Name())
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
