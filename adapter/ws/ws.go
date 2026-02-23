package ws

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/florinutz/pgpipe/event"
	"github.com/florinutz/pgpipe/metrics"
)

const (
	defaultBufferSize   = 256
	defaultPingInterval = 15 * time.Second
)

// Broker fans out events to WebSocket-connected HTTP clients.
// It implements the adapter.Adapter interface and http.Handler.
type Broker struct {
	clients      map[chan event.Event]string // channel -> optional channel filter
	mu           sync.RWMutex
	bufferSize   int
	pingInterval time.Duration
	logger       *slog.Logger
}

// New creates a Broker. bufferSize controls per-client channel capacity
// (defaults to 256 if <= 0). pingInterval sets the WebSocket ping interval
// (defaults to 15s if <= 0).
func New(bufferSize int, pingInterval time.Duration, logger *slog.Logger) *Broker {
	if bufferSize <= 0 {
		bufferSize = defaultBufferSize
	}
	if pingInterval <= 0 {
		pingInterval = defaultPingInterval
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Broker{
		clients:      make(map[chan event.Event]string),
		bufferSize:   bufferSize,
		pingInterval: pingInterval,
		logger:       logger,
	}
}

// Start blocks, reading events from the input channel and broadcasting each
// one to every connected WebSocket client.
func (b *Broker) Start(ctx context.Context, events <-chan event.Event) error {
	b.logger.Info("ws adapter started")

	for {
		select {
		case <-ctx.Done():
			b.logger.Info("ws adapter stopping", "reason", "context cancelled")
			return ctx.Err()

		case ev, ok := <-events:
			if !ok {
				b.logger.Info("ws adapter stopping", "reason", "channel closed")
				return nil
			}
			b.broadcast(ev)
		}
	}
}

// Name returns the adapter name.
func (b *Broker) Name() string {
	return "ws"
}

// Subscribe creates a buffered event channel and registers it with an optional
// channelFilter. The returned func must be called to unsubscribe.
func (b *Broker) Subscribe(channelFilter string) (<-chan event.Event, func()) {
	ch := make(chan event.Event, b.bufferSize)

	b.mu.Lock()
	b.clients[ch] = channelFilter
	b.mu.Unlock()

	label := channelFilter
	if label == "" {
		label = "*"
	}
	metrics.WSClientsActive.WithLabelValues(label).Inc()
	b.logger.Debug("ws client subscribed", "filter", channelFilter, "clients", b.clientCount())

	unsubscribe := func() {
		b.mu.Lock()
		delete(b.clients, ch)
		close(ch)
		b.mu.Unlock()

		metrics.WSClientsActive.WithLabelValues(label).Dec()
		b.logger.Debug("ws client unsubscribed", "filter", channelFilter, "clients", b.clientCount())
	}

	return ch, unsubscribe
}

// ServeHTTP upgrades the connection to WebSocket and streams events.
func (b *Broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true, // CORS handled by middleware
	})
	if err != nil {
		b.logger.Warn("websocket accept failed", "error", err)
		return
	}
	defer func() { _ = c.CloseNow() }()

	channelFilter := ChannelFromContext(r.Context())
	ch, unsubscribe := b.Subscribe(channelFilter)
	defer unsubscribe()

	ctx := r.Context()

	// Read goroutine: detect client disconnect.
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			_, _, err := c.Read(ctx)
			if err != nil {
				return
			}
		}
	}()

	pingTicker := time.NewTicker(b.pingInterval)
	defer pingTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			_ = c.Close(websocket.StatusNormalClosure, "server shutting down")
			return

		case <-readDone:
			return

		case ev, ok := <-ch:
			if !ok {
				_ = c.Close(websocket.StatusNormalClosure, "stream ended")
				return
			}
			data, err := json.Marshal(ev)
			if err != nil {
				b.logger.Warn("failed to marshal event", "event_id", ev.ID, "error", err)
				continue
			}
			if err := c.Write(ctx, websocket.MessageText, data); err != nil {
				b.logger.Debug("ws write failed", "error", err)
				return
			}

		case <-pingTicker.C:
			if err := c.Ping(ctx); err != nil {
				b.logger.Debug("ws ping failed", "error", err)
				return
			}
		}
	}
}

func (b *Broker) broadcast(ev event.Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for ch, filter := range b.clients {
		if filter != "" && filter != ev.Channel {
			continue
		}
		select {
		case ch <- ev:
			metrics.EventsDelivered.WithLabelValues("ws").Inc()
		default:
			metrics.EventsDropped.WithLabelValues("ws").Inc()
			b.logger.Warn("dropping event for slow ws client",
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

// channelKey is an unexported context key for passing the channel filter.
type channelKey struct{}

// WithChannel returns a child context carrying the WebSocket channel filter.
func WithChannel(ctx context.Context, channel string) context.Context {
	return context.WithValue(ctx, channelKey{}, channel)
}

// ChannelFromContext extracts the WebSocket channel filter from the context.
func ChannelFromContext(ctx context.Context) string {
	v, _ := ctx.Value(channelKey{}).(string)
	return v
}
