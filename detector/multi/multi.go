package multi

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/florinutz/pgcdc/detector"
	"github.com/florinutz/pgcdc/event"
)

// Mode defines how multiple detectors are composed.
type Mode string

const (
	Sequential Mode = "sequential" // run detectors one after another
	Parallel   Mode = "parallel"   // run all detectors concurrently
	Failover   Mode = "failover"   // run first detector, on error try next
)

// Detector composes multiple detectors into one.
type Detector struct {
	detectors   []ChildDetector
	mode        Mode
	logger      *slog.Logger
	DedupWindow time.Duration // when > 0 in parallel mode, dedup events by ID within this window
}

// ChildDetector is a named detector instance.
type ChildDetector struct {
	Name     string
	Detector detector.Detector
}

// New creates a multi-detector.
func New(mode Mode, logger *slog.Logger, children ...ChildDetector) *Detector {
	if logger == nil {
		logger = slog.Default()
	}
	return &Detector{
		detectors: children,
		mode:      mode,
		logger:    logger.With("component", "multi-detector"),
	}
}

func (d *Detector) Name() string { return "multi" }

// Start runs the child detectors according to the configured mode.
func (d *Detector) Start(ctx context.Context, events chan<- event.Event) error {
	if len(d.detectors) == 0 {
		return fmt.Errorf("multi-detector: no child detectors configured")
	}

	switch d.mode {
	case Parallel:
		return d.startParallel(ctx, events)
	case Failover:
		return d.startFailover(ctx, events)
	default:
		return d.startSequential(ctx, events)
	}
}

func (d *Detector) startSequential(ctx context.Context, events chan<- event.Event) error {
	for i, child := range d.detectors {
		d.logger.Info("starting sequential detector",
			"index", i,
			"name", child.Detector.Name(),
		)
		if err := child.Detector.Start(ctx, events); err != nil {
			// If context was cancelled, this is expected for the final detector
			if ctx.Err() != nil {
				return ctx.Err()
			}
			d.logger.Error("sequential detector failed",
				"name", child.Detector.Name(),
				"error", err,
			)
			return fmt.Errorf("detector %s: %w", child.Detector.Name(), err)
		}
		d.logger.Info("sequential detector completed",
			"name", child.Detector.Name(),
		)
	}
	return nil
}

func (d *Detector) startFailover(ctx context.Context, events chan<- event.Event) error {
	var lastErr error
	for i, child := range d.detectors {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		d.logger.Info("starting failover detector",
			"index", i,
			"name", child.Detector.Name(),
		)

		childCtx, childCancel := context.WithCancel(ctx)
		err := child.Detector.Start(childCtx, events)
		childCancel()

		if err == nil {
			return nil
		}

		// If the parent context was cancelled, propagate that directly.
		if ctx.Err() != nil {
			return ctx.Err()
		}

		d.logger.Warn("failover detector failed, trying next",
			"name", child.Detector.Name(),
			"error", err,
			"remaining", len(d.detectors)-i-1,
		)
		lastErr = fmt.Errorf("detector %s: %w", child.Detector.Name(), err)
	}
	return fmt.Errorf("all failover detectors exhausted: %w", lastErr)
}

// dedupTracker tracks recently-seen event IDs for deduplication.
type dedupTracker struct {
	mu     sync.Mutex
	seen   map[string]time.Time
	window time.Duration
	stopCh chan struct{}
}

func newDedupTracker(window time.Duration) *dedupTracker {
	dt := &dedupTracker{
		seen:   make(map[string]time.Time),
		window: window,
		stopCh: make(chan struct{}),
	}
	go dt.reapLoop()
	return dt
}

// isDuplicate returns true if the event ID was recently seen. If not seen,
// it records the ID and returns false.
func (dt *dedupTracker) isDuplicate(id string) bool {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	if _, exists := dt.seen[id]; exists {
		return true
	}
	dt.seen[id] = time.Now()
	return false
}

// reapLoop periodically removes expired entries from the seen map.
func (dt *dedupTracker) reapLoop() {
	ticker := time.NewTicker(dt.window / 2)
	defer ticker.Stop()
	for {
		select {
		case <-dt.stopCh:
			return
		case <-ticker.C:
			dt.reap()
		}
	}
}

func (dt *dedupTracker) reap() {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	cutoff := time.Now().Add(-dt.window)
	for id, ts := range dt.seen {
		if ts.Before(cutoff) {
			delete(dt.seen, id)
		}
	}
}

func (dt *dedupTracker) stop() {
	close(dt.stopCh)
}

func (d *Detector) startParallel(ctx context.Context, events chan<- event.Event) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(d.detectors))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// If dedup is enabled, wrap the events channel with a dedup filter.
	var target chan<- event.Event
	var tracker *dedupTracker
	var proxy chan event.Event
	if d.DedupWindow > 0 {
		tracker = newDedupTracker(d.DedupWindow)
		defer tracker.stop()

		proxy = make(chan event.Event, cap(events))
		target = proxy

		// Forward non-duplicate events from proxy to the real events channel.
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ev := range proxy {
				if !tracker.isDuplicate(ev.ID) {
					events <- ev
				} else {
					d.logger.Debug("dedup: dropping duplicate event",
						"event_id", ev.ID,
					)
				}
			}
		}()
	} else {
		target = events
	}

	var detectorWg sync.WaitGroup
	for _, child := range d.detectors {
		detectorWg.Add(1)
		go func() {
			defer detectorWg.Done()
			d.logger.Info("starting parallel detector",
				"name", child.Detector.Name(),
			)
			if err := child.Detector.Start(ctx, target); err != nil {
				if ctx.Err() == nil {
					d.logger.Error("parallel detector failed",
						"name", child.Detector.Name(),
						"error", err,
					)
					errCh <- fmt.Errorf("detector %s: %w", child.Detector.Name(), err)
					cancel()
				}
			}
		}()
	}

	detectorWg.Wait()
	close(errCh)

	// If we have a proxy channel, close it so the forwarder goroutine exits.
	if proxy != nil {
		close(proxy)
	}

	wg.Wait()

	// Return first error if any
	for err := range errCh {
		return err
	}
	return ctx.Err()
}
