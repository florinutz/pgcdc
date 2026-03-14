package bus

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/pgcdcerr"
)

const defaultBufferSize = 1024

// BusMode controls how the bus handles full subscriber channels.
type BusMode int

const (
	BusModeFast     BusMode = iota // default: non-blocking sends, drops on full
	BusModeReliable                // blocking sends, no drops (back-pressures the detector)
)

// FilterFunc returns true if the event should be delivered to the subscriber.
// Used by wrapper goroutines for route filtering (not by the bus dispatch loop).
type FilterFunc func(event.Event) bool

// subscriber pairs a channel with its adapter name for metrics labeling.
type subscriber struct {
	ch   chan event.Event
	name string
}

// subscriberList is an immutable snapshot of subscribers, stored in an
// atomic.Pointer to eliminate mutex acquisition on the hot dispatch path.
type subscriberList struct {
	subs []subscriber
}

// Bus receives events on an ingest channel and fans them out to all subscriber
// channels. Subscribers that fall behind (full channel) are skipped with a
// warning log rather than blocking the pipeline (fast mode), or block the
// detector until space is available (reliable mode).
type Bus struct {
	ingest     chan event.Event
	subsPtr    atomic.Pointer[subscriberList] // lock-free read on hot path
	bufferSize int
	mode       BusMode
	closed     bool
	mu         sync.Mutex // protects Subscribe/closeSubscribers (not hot path)
	logger     *slog.Logger
}

// New creates a Bus with the given buffer size applied to every channel it
// manages. If bufferSize <= 0 the default of 1024 is used.
func New(bufferSize int, logger *slog.Logger) *Bus {
	if bufferSize <= 0 {
		bufferSize = defaultBufferSize
	}
	if logger == nil {
		logger = slog.Default()
	}
	b := &Bus{
		ingest:     make(chan event.Event, bufferSize),
		bufferSize: bufferSize,
		logger:     logger,
	}
	b.subsPtr.Store(&subscriberList{})
	return b
}

// SetMode sets the fan-out mode. Must be called before Start.
func (b *Bus) SetMode(mode BusMode) {
	b.mode = mode
}

// Ingest returns the send-only ingest channel. Detectors push events here.
func (b *Bus) Ingest() chan<- event.Event {
	return b.ingest
}

// Subscribe creates a new buffered subscriber channel and returns it as
// receive-only. The name identifies the subscriber (typically the adapter name)
// and is used for metrics labels. Returns an error if the bus has already been stopped.
func (b *Bus) Subscribe(name string) (<-chan event.Event, error) {
	ch := make(chan event.Event, b.bufferSize)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		close(ch)
		return ch, pgcdcerr.ErrBusClosed
	}
	// Copy-on-write: create a new slice with the added subscriber and
	// atomically store it. The dispatch goroutine will pick it up on the
	// next iteration without any lock.
	old := b.subsPtr.Load()
	newSubs := make([]subscriber, len(old.subs), len(old.subs)+1)
	copy(newSubs, old.subs)
	newSubs = append(newSubs, subscriber{ch: ch, name: name})
	b.subsPtr.Store(&subscriberList{subs: newSubs})
	metrics.BusSubscribers.Set(float64(len(newSubs)))
	return ch, nil
}

// Start blocks, reading events from the ingest channel and fanning each event
// out to every subscriber. When ctx is cancelled it closes all subscriber
// channels and returns the context error. The ingest channel is NOT closed
// (the caller/detector owns it).
func (b *Bus) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			b.closeSubscribers()
			return ctx.Err()
		case ev := <-b.ingest:
			metrics.EventsReceived.Inc()
			// Load the subscriber list atomically — no mutex on the hot path.
			subs := b.subsPtr.Load().subs
			if b.mode == BusModeReliable {
				for _, sub := range subs {
					select {
					case sub.ch <- ev:
					default:
						// Channel full: increment backpressure metric, then block.
						metrics.BusBackpressure.WithLabelValues(sub.name).Inc()
						select {
						case sub.ch <- ev:
						case <-ctx.Done():
							b.closeSubscribers()
							return ctx.Err()
						}
					}
				}
			} else {
				for _, sub := range subs {
					select {
					case sub.ch <- ev:
					default:
						metrics.EventsDropped.WithLabelValues(sub.name).Inc()
						b.logger.Warn("subscriber channel full, dropping event",
							slog.String("event_id", ev.ID),
							slog.String("adapter", sub.name),
						)
					}
				}
			}
		}
	}
}

// ReportQueueDepths sets the per-adapter queue depth gauge for all subscribers.
func (b *Bus) ReportQueueDepths() {
	subs := b.subsPtr.Load().subs
	for _, sub := range subs {
		metrics.AdapterQueueDepth.WithLabelValues(sub.name).Set(float64(len(sub.ch)))
	}
}

// closeSubscribers closes all subscriber channels and marks the bus as closed.
func (b *Bus) closeSubscribers() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	subs := b.subsPtr.Load().subs
	for _, sub := range subs {
		close(sub.ch)
	}
	b.subsPtr.Store(&subscriberList{})
	metrics.BusSubscribers.Set(0)
}
