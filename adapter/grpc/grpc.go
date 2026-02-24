package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	pb "github.com/florinutz/pgcdc/adapter/grpc/proto"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Adapter starts a gRPC server that streams CDC events to connected clients.
// Similar to the SSE/WS brokers: maintains a set of connected clients and
// broadcasts events from the bus to all of them.
type Adapter struct {
	addr   string
	logger *slog.Logger

	mu      sync.RWMutex
	clients map[uint64]*client
	nextID  uint64
}

type client struct {
	ch       chan *pb.Event
	channels map[string]struct{} // nil = all channels
}

// New creates a gRPC streaming adapter.
func New(addr string, logger *slog.Logger) *Adapter {
	if addr == "" {
		addr = ":9090"
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Adapter{
		addr:    addr,
		logger:  logger.With("adapter", "grpc"),
		clients: make(map[uint64]*client),
	}
}

func (a *Adapter) Name() string { return "grpc" }

// Start consumes events from the bus and broadcasts them to connected gRPC
// clients. It also starts the gRPC server. Blocks until ctx is cancelled.
func (a *Adapter) Start(ctx context.Context, events <-chan event.Event) error {
	a.logger.Info("grpc adapter started", "addr", a.addr)

	ln, err := net.Listen("tcp", a.addr)
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}

	srv := grpc.NewServer()
	pb.RegisterEventStreamServer(srv, &grpcServer{adapter: a})

	// Serve in background.
	errCh := make(chan error, 1)
	go func() {
		if err := srv.Serve(ln); err != nil {
			errCh <- err
		}
		close(errCh)
	}()

	// Fan-out events to connected clients.
	go func() {
		for ev := range events {
			a.broadcast(ev)
		}
	}()

	// Wait for context cancellation.
	<-ctx.Done()
	srv.GracefulStop()

	select {
	case err := <-errCh:
		if err != nil {
			return err
		}
	default:
	}
	return ctx.Err()
}

func (a *Adapter) addClient(channels []string) (uint64, *client) {
	c := &client{
		ch: make(chan *pb.Event, 256),
	}
	if len(channels) > 0 {
		c.channels = make(map[string]struct{}, len(channels))
		for _, ch := range channels {
			c.channels[ch] = struct{}{}
		}
	}

	a.mu.Lock()
	id := a.nextID
	a.nextID++
	a.clients[id] = c
	metrics.GRPCActiveClients.Set(float64(len(a.clients)))
	a.mu.Unlock()

	return id, c
}

func (a *Adapter) removeClient(id uint64) {
	a.mu.Lock()
	if c, ok := a.clients[id]; ok {
		close(c.ch)
		delete(a.clients, id)
	}
	metrics.GRPCActiveClients.Set(float64(len(a.clients)))
	a.mu.Unlock()
}

func (a *Adapter) broadcast(ev event.Event) {
	protoEv := &pb.Event{
		Id:        ev.ID,
		Channel:   ev.Channel,
		Operation: ev.Operation,
		Payload:   ev.Payload,
		Source:    ev.Source,
		CreatedAt: ev.CreatedAt.Format(time.RFC3339Nano),
	}

	a.mu.RLock()
	for _, c := range a.clients {
		if c.channels != nil {
			if _, ok := c.channels[ev.Channel]; !ok {
				continue
			}
		}
		select {
		case c.ch <- protoEv:
			metrics.GRPCEventsSent.Inc()
		default:
			// Client too slow, drop event.
		}
	}
	a.mu.RUnlock()
}

// ── gRPC server implementation ──────────────────────────────────────────────

type grpcServer struct {
	pb.UnimplementedEventStreamServer
	adapter *Adapter
}

func (s *grpcServer) Subscribe(req *pb.SubscribeRequest, stream grpc.ServerStreamingServer[pb.Event]) error {
	id, c := s.adapter.addClient(req.GetChannels())
	defer s.adapter.removeClient(id)

	s.adapter.logger.Info("grpc client connected", "id", id, "channels", req.GetChannels())

	for {
		select {
		case <-stream.Context().Done():
			s.adapter.logger.Info("grpc client disconnected", "id", id)
			return nil
		case ev, ok := <-c.ch:
			if !ok {
				return status.Error(codes.Unavailable, "server shutting down")
			}
			if err := stream.Send(ev); err != nil {
				return err
			}
		}
	}
}
