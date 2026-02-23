package sse

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/florinutz/pgpipe/event"
	"github.com/florinutz/pgpipe/metrics"
)

const (
	defaultBufferSize = 256
	defaultHeartbeat  = 15 * time.Second
)

// Broker fans out events to SSE-connected HTTP clients.
// It implements the adapter.Adapter interface and http.Handler.
type Broker struct {
	clients    map[chan event.Event]string // channel -> optional channel filter
	mu         sync.RWMutex
	bufferSize int
	heartbeat  time.Duration
	logger     *slog.Logger
}

// New creates a Broker. bufferSize controls per-client channel capacity
// (defaults to 256 if <= 0). heartbeat sets the interval for keep-alive
// comments sent to idle connections (defaults to 15s if <= 0).
func New(bufferSize int, heartbeat time.Duration, logger *slog.Logger) *Broker {
	if bufferSize <= 0 {
		bufferSize = defaultBufferSize
	}
	if heartbeat <= 0 {
		heartbeat = defaultHeartbeat
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Broker{
		clients:    make(map[chan event.Event]string),
		bufferSize: bufferSize,
		heartbeat:  heartbeat,
		logger:     logger,
	}
}

// Start blocks, reading events from the input channel and broadcasting each
// one to every connected SSE client. It returns nil when the events channel is
// closed, or ctx.Err() when the context is cancelled.
func (b *Broker) Start(ctx context.Context, events <-chan event.Event) error {
	b.logger.Info("sse adapter started")

	for {
		select {
		case <-ctx.Done():
			b.logger.Info("sse adapter stopping", "reason", "context cancelled")
			return ctx.Err()

		case ev, ok := <-events:
			if !ok {
				b.logger.Info("sse adapter stopping", "reason", "channel closed")
				return nil
			}
			b.broadcast(ev)
		}
	}
}

// Name returns the adapter name.
func (b *Broker) Name() string {
	return "sse"
}

// Subscribe creates a buffered event channel and registers it with an optional
// channelFilter. When channelFilter is non-empty only events whose Channel
// field matches will be forwarded. The returned func must be called to
// unsubscribe and close the channel.
func (b *Broker) Subscribe(channelFilter string) (<-chan event.Event, func()) {
	ch := make(chan event.Event, b.bufferSize)

	b.mu.Lock()
	b.clients[ch] = channelFilter
	b.mu.Unlock()

	label := channelFilter
	if label == "" {
		label = "*"
	}
	metrics.SSEClientsActive.WithLabelValues(label).Inc()
	b.logger.Debug("client subscribed", "filter", channelFilter, "clients", b.clientCount())

	unsubscribe := func() {
		b.mu.Lock()
		delete(b.clients, ch)
		close(ch)
		b.mu.Unlock()

		metrics.SSEClientsActive.WithLabelValues(label).Dec()
		b.logger.Debug("client unsubscribed", "filter", channelFilter, "clients", b.clientCount())
	}

	return ch, unsubscribe
}

// ServeHTTP handles an SSE connection. It subscribes the client, streams
// events until the client disconnects, and sends periodic heartbeat comments
// to keep the connection alive.
func (b *Broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	// Determine optional channel filter from the request context (set by router).
	channelFilter := ChannelFromContext(r.Context())

	ch, unsubscribe := b.Subscribe(channelFilter)
	defer unsubscribe()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	flusher.Flush()

	heartbeatTicker := time.NewTicker(b.heartbeat)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return

		case ev, ok := <-ch:
			if !ok {
				return
			}
			if err := writeSSE(w, ev); err != nil {
				b.logger.Warn("failed to write SSE event", "event_id", ev.ID, "error", err)
				return
			}
			flusher.Flush()

		case <-heartbeatTicker.C:
			if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// broadcast sends ev to every registered client. For clients with a channel
// filter, only matching events are forwarded. Sends are non-blocking; a full
// client channel causes the event to be dropped for that client.
func (b *Broker) broadcast(ev event.Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for ch, filter := range b.clients {
		if filter != "" && filter != ev.Channel {
			continue
		}
		select {
		case ch <- ev:
			metrics.EventsDelivered.WithLabelValues("sse").Inc()
		default:
			metrics.EventsDropped.WithLabelValues("sse").Inc()
			b.logger.Warn("dropping event for slow client",
				"event_id", ev.ID,
				"channel", ev.Channel,
			)
		}
	}
}

func (b *Broker) clientCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.clients)
}

// writeSSE formats and writes a single event in SSE wire format.
func writeSSE(w io.Writer, ev event.Event) error {
	data, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal event %s: %w", ev.ID, err)
	}
	_, err = fmt.Fprintf(w, "id: %s\nevent: %s\ndata: %s\n\n", ev.ID, ev.Channel, data)
	return err
}

// channelKey is an unexported context key for passing the channel filter
// from the router to ServeHTTP.
type channelKey struct{}

// WithChannel returns a child context carrying the SSE channel filter value.
func WithChannel(ctx context.Context, channel string) context.Context {
	return context.WithValue(ctx, channelKey{}, channel)
}

// ChannelFromContext extracts the SSE channel filter from the context, or
// returns "" if none is set.
func ChannelFromContext(ctx context.Context) string {
	v, _ := ctx.Value(channelKey{}).(string)
	return v
}
