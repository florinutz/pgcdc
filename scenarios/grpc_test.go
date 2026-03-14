//go:build integration

package scenarios

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	grpcadapter "github.com/florinutz/pgcdc/adapter/grpc"
	pb "github.com/florinutz/pgcdc/adapter/grpc/proto"
	"github.com/florinutz/pgcdc/bus"
	"github.com/florinutz/pgcdc/detector/listennotify"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestScenario_GRPC(t *testing.T) {
	t.Parallel()
	connStr := startPostgres(t)

	t.Run("happy path", func(t *testing.T) {
		// Find a free port for the gRPC server.
		grpcAddr := getFreeAddr(t)

		// Create the gRPC adapter.
		logger := testLogger()
		a := grpcadapter.New(grpcAddr, logger)

		// Wire pipeline: LISTEN/NOTIFY detector -> bus -> gRPC adapter.
		pipelineCtx, pipelineCancel := context.WithCancel(context.Background())
		channel := "grpc_test"
		det := listennotify.New(connStr, []string{channel}, 0, 0, logger)
		b := bus.New(64, logger)

		g, gCtx := errgroup.WithContext(pipelineCtx)
		g.Go(func() error { return b.Start(gCtx) })
		g.Go(func() error { return det.Start(gCtx, b.Ingest()) })

		sub, err := b.Subscribe(a.Name())
		if err != nil {
			pipelineCancel()
			t.Fatalf("subscribe grpc: %v", err)
		}
		g.Go(func() error { return a.Start(gCtx, sub) })

		t.Cleanup(func() {
			pipelineCancel()
			g.Wait()
		})

		// Wait for the gRPC server to start.
		waitForTCP(t, grpcAddr, 5*time.Second)

		// Connect a gRPC client.
		conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("dial grpc: %v", err)
		}
		defer conn.Close()

		client := pb.NewEventStreamClient(conn)
		streamCtx, streamCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer streamCancel()

		stream, err := client.Subscribe(streamCtx, &pb.SubscribeRequest{})
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}

		// Send NOTIFY events periodically until one arrives on the gRPC stream.
		// The detector may not be ready immediately after TCP listen.
		payload := `{"op":"INSERT","table":"orders","row":{"id":1,"item":"widget"}}`
		done := make(chan struct{})
		go func() {
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			for {
				sendNotify(t, connStr, channel, payload)
				select {
				case <-done:
					return
				case <-ticker.C:
				}
			}
		}()

		// Receive the event via gRPC stream.
		ev, err := stream.Recv()
		close(done)
		if err != nil {
			t.Fatalf("recv: %v", err)
		}

		if ev.Channel != channel {
			t.Errorf("channel = %q, want %q", ev.Channel, channel)
		}
		if ev.Operation != "INSERT" {
			t.Errorf("operation = %q, want %q", ev.Operation, "INSERT")
		}
		if ev.Source != "listen_notify" {
			t.Errorf("source = %q, want %q", ev.Source, "listen_notify")
		}
		if ev.Id == "" {
			t.Error("event ID is empty")
		}

		// Verify the payload content.
		var payloadData map[string]interface{}
		if err := json.Unmarshal(ev.Payload, &payloadData); err != nil {
			t.Fatalf("unmarshal payload: %v", err)
		}
		if payloadData["op"] != "INSERT" {
			t.Errorf("payload op = %v, want INSERT", payloadData["op"])
		}

		fmt.Fprintf(io.Discard, "gRPC event received: id=%s channel=%s op=%s\n", ev.Id, ev.Channel, ev.Operation)
	})

	t.Run("channel filter", func(t *testing.T) {
		// Find a free port for the gRPC server.
		grpcAddr := getFreeAddr(t)

		// Create the gRPC adapter.
		logger := testLogger()
		a := grpcadapter.New(grpcAddr, logger)

		// Wire pipeline with two channels.
		pipelineCtx, pipelineCancel := context.WithCancel(context.Background())
		channels := []string{"grpc_orders", "grpc_users"}
		det := listennotify.New(connStr, channels, 0, 0, logger)
		b := bus.New(64, logger)

		g, gCtx := errgroup.WithContext(pipelineCtx)
		g.Go(func() error { return b.Start(gCtx) })
		g.Go(func() error { return det.Start(gCtx, b.Ingest()) })

		sub, err := b.Subscribe(a.Name())
		if err != nil {
			pipelineCancel()
			t.Fatalf("subscribe grpc: %v", err)
		}
		g.Go(func() error { return a.Start(gCtx, sub) })

		t.Cleanup(func() {
			pipelineCancel()
			g.Wait()
		})

		waitForTCP(t, grpcAddr, 5*time.Second)

		// Connect a client that only subscribes to "grpc_orders".
		conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("dial grpc: %v", err)
		}
		defer conn.Close()

		client := pb.NewEventStreamClient(conn)
		streamCtx, streamCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer streamCancel()

		stream, err := client.Subscribe(streamCtx, &pb.SubscribeRequest{
			Channels: []string{"grpc_orders"},
		})
		if err != nil {
			t.Fatalf("subscribe with filter: %v", err)
		}

		// Send events periodically until the orders event arrives on the gRPC stream.
		usersPayload := `{"op":"INSERT","table":"users","row":{"id":1,"name":"alice"}}`
		ordersPayload := `{"op":"INSERT","table":"orders","row":{"id":1,"item":"widget"}}`
		done := make(chan struct{})
		go func() {
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			for {
				sendNotify(t, connStr, "grpc_users", usersPayload)
				sendNotify(t, connStr, "grpc_orders", ordersPayload)
				select {
				case <-done:
					return
				case <-ticker.C:
				}
			}
		}()

		// We should receive the orders event (not users).
		ev, err := stream.Recv()
		close(done)
		if err != nil {
			t.Fatalf("recv: %v", err)
		}

		if ev.Channel != "grpc_orders" {
			t.Errorf("expected event from grpc_orders, got channel=%q", ev.Channel)
		}
	})
}

// getFreeAddr finds a free TCP port and returns the address.
func getFreeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}
