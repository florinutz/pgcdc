// Package backpressure provides source-aware backpressure for WAL replication.
// It monitors WAL retention lag and automatically throttles, pauses, or sheds
// adapters to prevent PostgreSQL disk exhaustion.
package backpressure

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/florinutz/pgcdc/health"
	"github.com/florinutz/pgcdc/metrics"
)

// Zone represents the backpressure state.
type Zone int32

const (
	ZoneGreen  Zone = 0 // lag < warn: full speed
	ZoneYellow Zone = 1 // warn <= lag < critical: throttle + shed best-effort
	ZoneRed    Zone = 2 // lag >= critical: pause detector + shed normal+best-effort
)

func (z Zone) String() string {
	switch z {
	case ZoneGreen:
		return "green"
	case ZoneYellow:
		return "yellow"
	case ZoneRed:
		return "red"
	default:
		return "unknown"
	}
}

// AdapterPriority determines when an adapter is shed.
type AdapterPriority int

const (
	PriorityCritical   AdapterPriority = 0 // never shed
	PriorityNormal     AdapterPriority = 1 // shed in red zone only
	PriorityBestEffort AdapterPriority = 2 // shed in yellow+red zone
)

const (
	defaultPollInterval = 10 * time.Second
	defaultMaxThrottle  = 500 * time.Millisecond
)

// Controller monitors WAL lag and manages backpressure zones.
type Controller struct {
	warnThreshold     int64
	criticalThreshold int64
	maxThrottle       time.Duration
	pollInterval      time.Duration

	zone          atomic.Int32  // current Zone
	throttleNanos atomic.Int64  // sleep duration (ns) for yellow zone
	paused        atomic.Bool   // true in red zone
	shedSet       atomic.Value  // map[string]bool of currently-shed adapters
	pauseCh       chan struct{} // closed to wake detector from red

	adapterPriorities map[string]AdapterPriority
	lagFn             func() int64 // injected: returns current WAL lag bytes
	health            *health.Checker
	logger            *slog.Logger
	mu                sync.Mutex // protects pauseCh replacement
}

// New creates a backpressure controller with the given thresholds.
func New(warn, critical int64, maxThrottle time.Duration, pollInterval time.Duration, h *health.Checker, logger *slog.Logger) *Controller {
	if logger == nil {
		logger = slog.Default()
	}
	if maxThrottle <= 0 {
		maxThrottle = defaultMaxThrottle
	}
	if pollInterval <= 0 {
		pollInterval = defaultPollInterval
	}
	c := &Controller{
		warnThreshold:     warn,
		criticalThreshold: critical,
		maxThrottle:       maxThrottle,
		pollInterval:      pollInterval,
		pauseCh:           make(chan struct{}),
		adapterPriorities: make(map[string]AdapterPriority),
		health:            h,
		logger:            logger.With("component", "backpressure"),
	}
	c.shedSet.Store(map[string]bool{})
	return c
}

// SetLagFunc injects the function that returns current WAL lag in bytes.
// Safe to call concurrently with Run/evaluate.
func (c *Controller) SetLagFunc(fn func() int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lagFn = fn
}

// SetAdapterPriority sets the shedding priority for an adapter.
func (c *Controller) SetAdapterPriority(name string, priority AdapterPriority) {
	c.adapterPriorities[name] = priority
}

// Zone returns the current backpressure zone (atomic read).
func (c *Controller) Zone() Zone {
	return Zone(c.zone.Load())
}

// ThrottleDuration returns the current throttle sleep duration (atomic read).
// Returns 0 in green zone, proportional to lag position in yellow zone.
func (c *Controller) ThrottleDuration() time.Duration {
	return time.Duration(c.throttleNanos.Load())
}

// IsPaused returns true when the detector should be paused (red zone).
func (c *Controller) IsPaused() bool {
	return c.paused.Load()
}

// WaitResume blocks until the controller exits the paused state or ctx is cancelled.
// It re-checks after each signal to handle rapid red→green→red transitions where
// a stale pauseCh from a previous cycle may have been closed.
func (c *Controller) WaitResume(ctx context.Context) error {
	for {
		c.mu.Lock()
		ch := c.pauseCh
		paused := c.paused.Load()
		c.mu.Unlock()

		if !paused {
			return nil
		}

		select {
		case <-ch:
			// ch was closed; loop and re-check in case a new red cycle started.
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// IsShed returns true if the named adapter is currently being shed.
func (c *Controller) IsShed(adapterName string) bool {
	shed, _ := c.shedSet.Load().(map[string]bool)
	return shed[adapterName]
}

// Run starts the backpressure control loop. It polls lagFn and updates zone,
// throttle, pause, and shed state. Blocks until ctx is cancelled.
// If lagFn has not been set yet (via SetLagFunc), poll ticks are skipped
// until it becomes available — this avoids ordering dependencies between
// backpressure setup and detector creation.
func (c *Controller) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			c.evaluate()
		}
	}
}

// evaluate reads current lag and updates all controller state.
func (c *Controller) evaluate() {
	c.mu.Lock()
	fn := c.lagFn
	c.mu.Unlock()
	if fn == nil {
		return // lagFn not set yet; skip this tick
	}
	lag := fn()
	if lag < 0 {
		lag = 0
	}

	prevZone := c.Zone()
	newZone := c.computeZone(lag, prevZone)

	c.zone.Store(int32(newZone))
	metrics.BackpressureState.Set(float64(newZone))

	// Update throttle.
	switch newZone {
	case ZoneGreen:
		c.throttleNanos.Store(0)
	case ZoneYellow:
		c.throttleNanos.Store(int64(c.computeThrottle(lag)))
	case ZoneRed:
		c.throttleNanos.Store(int64(c.maxThrottle))
	}

	// Update pause state.
	switch {
	case newZone == ZoneRed:
		c.mu.Lock()
		if !c.paused.Load() {
			c.paused.Store(true)
			c.pauseCh = make(chan struct{})
		}
		c.mu.Unlock()
	case newZone != ZoneRed:
		c.mu.Lock()
		if c.paused.Load() {
			c.paused.Store(false)
			close(c.pauseCh)
		}
		c.mu.Unlock()
	}

	// Update shed set.
	c.shedSet.Store(c.computeShedSet(newZone))

	// Update health.
	if c.health != nil {
		switch newZone {
		case ZoneGreen:
			c.health.SetStatus("backpressure", health.StatusUp)
		case ZoneYellow:
			c.health.SetStatus("backpressure", health.StatusDegraded)
		case ZoneRed:
			c.health.SetStatus("backpressure", health.StatusDown)
		}
	}

	// Log zone transitions.
	if newZone != prevZone {
		c.logger.Warn("zone transition",
			slog.String("from", prevZone.String()),
			slog.String("to", newZone.String()),
			slog.Int64("lag_bytes", lag),
		)
	}
}

// computeZone determines the zone for the given lag, applying hysteresis:
// red exits only when lag drops below warn (not critical).
func (c *Controller) computeZone(lag int64, current Zone) Zone {
	if lag >= c.criticalThreshold {
		return ZoneRed
	}
	// Hysteresis: red→green only when lag drops below warn.
	if current == ZoneRed {
		if lag < c.warnThreshold {
			return ZoneGreen
		}
		return ZoneRed
	}
	if lag >= c.warnThreshold {
		return ZoneYellow
	}
	return ZoneGreen
}

// computeThrottle returns a proportional throttle duration within the yellow band.
func (c *Controller) computeThrottle(lag int64) time.Duration {
	band := c.criticalThreshold - c.warnThreshold
	if band <= 0 {
		return c.maxThrottle
	}
	position := lag - c.warnThreshold
	if position < 0 {
		return 0
	}
	ratio := float64(position) / float64(band)
	if ratio > 1.0 {
		ratio = 1.0
	}
	return time.Duration(float64(c.maxThrottle) * ratio)
}

// computeShedSet builds the set of adapters to shed based on the current zone.
func (c *Controller) computeShedSet(zone Zone) map[string]bool {
	shed := make(map[string]bool)
	for name, priority := range c.adapterPriorities {
		switch {
		case priority == PriorityBestEffort && zone >= ZoneYellow:
			shed[name] = true
		case priority == PriorityNormal && zone >= ZoneRed:
			shed[name] = true
		}
	}
	return shed
}
