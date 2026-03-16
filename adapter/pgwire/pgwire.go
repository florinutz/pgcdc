//go:build !no_pgwire

package pgwire

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/metrics"
)

const (
	defaultAddr       = ":5433"
	defaultBufferSize = 10000
)

// Adapter speaks PostgreSQL wire protocol, allowing clients to connect with
// psql or any PG driver and query CDC events as SQL result sets.
type Adapter struct {
	addr       string
	bufferSize int
	password   string
	tlsCert    string
	tlsKey     string
	logger     *slog.Logger

	mu     sync.RWMutex
	buffer []event.Event // ring buffer
	pos    int           // next write position
	count  int           // total events ever written
}

// New creates a PGWire adapter.
func New(addr string, bufferSize int, password, tlsCert, tlsKey string, logger *slog.Logger) *Adapter {
	if addr == "" {
		addr = defaultAddr
	}
	if bufferSize <= 0 {
		bufferSize = defaultBufferSize
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Adapter{
		addr:       addr,
		bufferSize: bufferSize,
		password:   password,
		tlsCert:    tlsCert,
		tlsKey:     tlsKey,
		logger:     logger.With("adapter", "pgwire"),
	}
}

// Name returns the adapter name.
func (a *Adapter) Name() string { return "pgwire" }

// Validate implements adapter.Validator: checks that the listen address is non-empty.
func (a *Adapter) Validate(_ context.Context) error {
	if a.addr == "" {
		return fmt.Errorf("pgwire: addr is required")
	}
	return nil
}

// eventsColumns defines the column schema for pgcdc_events.
var eventsColumns = wire.Columns{
	{Table: 0, Name: "id", Oid: pgtype.TextOID, Width: 256},
	{Table: 0, Name: "channel", Oid: pgtype.TextOID, Width: 256},
	{Table: 0, Name: "operation", Oid: pgtype.TextOID, Width: 64},
	{Table: 0, Name: "source", Oid: pgtype.TextOID, Width: 256},
	{Table: 0, Name: "payload", Oid: pgtype.TextOID, Width: -1},
	{Table: 0, Name: "created_at", Oid: pgtype.TimestamptzOID, Width: 8},
	{Table: 0, Name: "lsn", Oid: pgtype.TextOID, Width: 32},
}

// channelsColumns defines the column schema for pgcdc_channels.
var channelsColumns = wire.Columns{
	{Table: 0, Name: "channel", Oid: pgtype.TextOID, Width: 256},
	{Table: 0, Name: "event_count", Oid: pgtype.Int4OID, Width: 4},
}

// statusColumns defines the column schema for pgcdc_status.
var statusColumns = wire.Columns{
	{Table: 0, Name: "key", Oid: pgtype.TextOID, Width: 256},
	{Table: 0, Name: "value", Oid: pgtype.TextOID, Width: 256},
}

// Start consumes events from the channel into the ring buffer and starts the
// psql-wire server.
func (a *Adapter) Start(ctx context.Context, events <-chan event.Event) error {
	a.buffer = make([]event.Event, a.bufferSize)

	a.logger.Info("pgwire adapter started", "addr", a.addr, "buffer_size", a.bufferSize)

	// Build server options.
	opts := []wire.OptionFn{
		wire.Logger(a.logger),
		wire.Version("17.0"),
		wire.SessionMiddleware(func(sctx context.Context) (context.Context, error) {
			metrics.PGWireClientsActive.Inc()
			a.logger.Debug("pgwire client connected", "remote", wire.RemoteAddress(sctx))
			return sctx, nil
		}),
		wire.TerminateConn(func(sctx context.Context) error {
			metrics.PGWireClientsActive.Dec()
			a.logger.Debug("pgwire client disconnected", "remote", wire.RemoteAddress(sctx))
			return nil
		}),
	}

	// Optional password auth.
	if a.password != "" {
		pw := a.password
		opts = append(opts, wire.SessionAuthStrategy(
			wire.ClearTextPassword(func(ctx context.Context, _, _, password string) (context.Context, bool, error) {
				return ctx, password == pw, nil
			}),
		))
	}

	// Optional TLS.
	if a.tlsCert != "" && a.tlsKey != "" {
		cert, err := tls.LoadX509KeyPair(a.tlsCert, a.tlsKey)
		if err != nil {
			return fmt.Errorf("load tls keypair: %w", err)
		}
		opts = append(opts, wire.TLSConfig(&tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}))
	}

	srv, err := wire.NewServer(a.handleQuery, opts...)
	if err != nil {
		return fmt.Errorf("create pgwire server: %w", err)
	}

	listener, err := net.Listen("tcp", a.addr)
	if err != nil {
		return fmt.Errorf("listen pgwire: %w", err)
	}

	// Ingest events into ring buffer in the background.
	go a.ingest(ctx, events)

	// Shut down the server when the context is cancelled.
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutCtx); err != nil {
			a.logger.Warn("pgwire server shutdown error", "error", err)
		}
	}()

	err = srv.Serve(listener)
	if ctx.Err() != nil {
		return nil // normal shutdown
	}
	return err
}

// ingest reads events from the channel and stores them in the ring buffer.
func (a *Adapter) ingest(ctx context.Context, events <-chan event.Event) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			ev.EnsurePayload()
			a.mu.Lock()
			a.buffer[a.pos] = ev
			a.pos = (a.pos + 1) % a.bufferSize
			a.count++
			a.mu.Unlock()
			metrics.EventsDelivered.WithLabelValues("pgwire").Inc()
		}
	}
}

// snapshot returns a copy of all buffered events in insertion order.
func (a *Adapter) snapshot() []event.Event {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.count == 0 {
		return nil
	}

	n := a.count
	if n > a.bufferSize {
		n = a.bufferSize
	}
	result := make([]event.Event, 0, n)

	if a.count <= a.bufferSize {
		// Buffer not yet full: events are at positions [0, pos).
		result = append(result, a.buffer[:a.pos]...)
	} else {
		// Buffer wrapped: oldest event is at pos, read to end then wrap.
		result = append(result, a.buffer[a.pos:]...)
		result = append(result, a.buffer[:a.pos]...)
	}
	return result
}
