package bus

import (
	"context"
	"log/slog"
	"sync"

	"github.com/florinutz/pgpipe/event"
	"github.com/florinutz/pgpipe/metrics"
	"github.com/florinutz/pgpipe/pgpipeerr"
)

const defaultBufferSize = 1024

// subscriber pairs a channel with its adapter name for metrics labeling.
type subscriber struct {
	ch   chan event.Event
	name string
}

// Bus receives events on an ingest channel and fans them out to all subscriber
// channels. Subscribers that fall behind (full channel) are skipped with a
// warning log rather than blocking the pipeline.
type Bus struct {
	ingest      chan event.Event
	subscribers []subscriber
	bufferSize  int
	closed      bool
	mu          sync.RWMutex
	logger      *slog.Logger
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
	return &Bus{
		ingest:     make(chan event.Event, bufferSize),
		bufferSize: bufferSize,
		logger:     logger,
	}
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
		return ch, pgpipeerr.ErrBusClosed
	}
	b.subscribers = append(b.subscribers, subscriber{ch: ch, name: name})
	metrics.BusSubscribers.Set(float64(len(b.subscribers)))
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
			b.mu.Lock()
			b.closed = true
			for _, sub := range b.subscribers {
				close(sub.ch)
			}
			b.subscribers = nil
			metrics.BusSubscribers.Set(0)
			b.mu.Unlock()
			return ctx.Err()
		case ev := <-b.ingest:
			metrics.EventsReceived.WithLabelValues(ev.Channel).Inc()
			b.mu.RLock()
			for _, sub := range b.subscribers {
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
			b.mu.RUnlock()
		}
	}
}
