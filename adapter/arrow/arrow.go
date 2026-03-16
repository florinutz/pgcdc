//go:build !no_arrow

package arrow

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/schema"
)

// Adapter ingests CDC events and serves them via Arrow Flight gRPC.
// Events are buffered in per-channel ring buffers and streamed to clients
// as Arrow RecordBatches via DoGet.
type Adapter struct {
	addr        string
	bufferSize  int
	logger      *slog.Logger
	schemaStore schema.Store

	mu       sync.RWMutex
	channels map[string]*channelBuffer
}

// channelBuffer is a fixed-size ring buffer of events for a single channel.
type channelBuffer struct {
	events []event.Event
	head   int
	count  int
	size   int
}

func newChannelBuffer(size int) *channelBuffer {
	return &channelBuffer{
		events: make([]event.Event, size),
		size:   size,
	}
}

// add appends an event to the ring buffer, overwriting the oldest if full.
func (b *channelBuffer) add(ev event.Event) {
	idx := (b.head + b.count) % b.size
	b.events[idx] = ev
	if b.count == b.size {
		b.head = (b.head + 1) % b.size
	} else {
		b.count++
	}
}

// get returns events starting from the given offset. Offset is relative to
// the logical start of the buffer (0 = oldest available event).
func (b *channelBuffer) get(offset int) []event.Event {
	if offset >= b.count {
		return nil
	}
	start := offset
	n := b.count - start
	result := make([]event.Event, n)
	for i := 0; i < n; i++ {
		result[i] = b.events[(b.head+start+i)%b.size]
	}
	return result
}

// New creates an Arrow Flight adapter.
func New(addr string, bufferSize int, schemaStore schema.Store, logger *slog.Logger) *Adapter {
	if addr == "" {
		addr = ":8815"
	}
	if bufferSize <= 0 {
		bufferSize = 10000
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Adapter{
		addr:        addr,
		bufferSize:  bufferSize,
		logger:      logger.With("adapter", "arrow"),
		schemaStore: schemaStore,
		channels:    make(map[string]*channelBuffer),
	}
}

func (a *Adapter) Name() string { return "arrow" }

// Validate checks required configuration.
func (a *Adapter) Validate(_ context.Context) error {
	if a.addr == "" {
		return fmt.Errorf("arrow: addr is required")
	}
	return nil
}

// Drain implements adapter.Drainer. The Arrow Flight adapter shuts down
// the gRPC server in Start() on context cancellation, so Drain is a no-op.
func (a *Adapter) Drain(_ context.Context) error { return nil }

// Start consumes events from the bus and serves them via Arrow Flight.
// Blocks until ctx is cancelled.
func (a *Adapter) Start(ctx context.Context, events <-chan event.Event) error {
	a.logger.Info("arrow flight adapter started", "addr", a.addr)

	ln, err := net.Listen("tcp", a.addr)
	if err != nil {
		return fmt.Errorf("arrow listen: %w", err)
	}

	srv := flight.NewServerWithMiddleware(nil)
	srv.InitListener(ln)
	srv.RegisterFlightService(&flightServer{adapter: a})

	errCh := make(chan error, 1)
	go func() {
		if err := srv.Serve(); err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
		close(errCh)
	}()

	for {
		select {
		case <-ctx.Done():
			srv.Shutdown()
			<-errCh
			return ctx.Err()
		case ev, ok := <-events:
			if !ok {
				srv.Shutdown()
				<-errCh
				return nil
			}
			a.ingest(ev)
		}
	}
}

// SetSchemaStore sets the schema store for type-aware Arrow schema generation.
func (a *Adapter) SetSchemaStore(s schema.Store) {
	a.schemaStore = s
}

func (a *Adapter) ingest(ev event.Event) {
	a.mu.Lock()
	buf, ok := a.channels[ev.Channel]
	if !ok {
		buf = newChannelBuffer(a.bufferSize)
		a.channels[ev.Channel] = buf
	}
	buf.add(ev)
	a.mu.Unlock()
	metrics.EventsDelivered.WithLabelValues("arrow").Inc()
}

// channelNames returns sorted channel names (under read lock).
func (a *Adapter) channelNames() []string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	names := make([]string, 0, len(a.channels))
	for name := range a.channels {
		names = append(names, name)
	}
	return names
}

// getEvents returns buffered events for a channel starting at offset.
func (a *Adapter) getEvents(channel string, offset int) []event.Event {
	a.mu.RLock()
	defer a.mu.RUnlock()
	buf, ok := a.channels[channel]
	if !ok {
		return nil
	}
	return buf.get(offset)
}

// bufferCount returns the event count for a channel.
func (a *Adapter) bufferCount(channel string) int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	buf, ok := a.channels[channel]
	if !ok {
		return 0
	}
	return buf.count
}
