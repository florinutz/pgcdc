//go:build integration && !no_arrow

package scenarios

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	arrowadapter "github.com/florinutz/pgcdc/adapter/arrow"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestScenario_ArrowFlight(t *testing.T) {
	connStr := startPostgres(t)

	t.Run("happy path", func(t *testing.T) {
		table := "arrow_test_orders"
		channel := createTrigger(t, connStr, table)

		// Find a free port for the Flight server.
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		addr := ln.Addr().String()
		ln.Close()

		adapter := arrowadapter.New(addr, 1000, nil, testLogger())
		startPipeline(t, connStr, []string{channel}, adapter)

		// Wait for the Flight gRPC server to accept connections.
		waitFor(t, 10*time.Second, func() bool {
			conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
			if err != nil {
				return false
			}
			conn.Close()
			return true
		})

		// Insert rows via trigger to generate events.
		time.Sleep(1 * time.Second)
		for i := range 3 {
			insertRow(t, connStr, table, map[string]any{"item": fmt.Sprintf("arrow_%d", i)})
		}

		// Wait for events to be ingested by the adapter.
		// Retry inserts if the LISTEN/NOTIFY detector wasn't ready.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		client, err := flight.NewClientWithMiddleware(addr, nil, nil,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("flight client: %v", err)
		}
		defer client.Close()

		// Poll ListFlights until we see our channel.
		var found bool
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			stream, err := client.ListFlights(ctx, &flight.Criteria{})
			if err != nil {
				time.Sleep(500 * time.Millisecond)
				continue
			}
			for {
				info, err := stream.Recv()
				if err != nil {
					break
				}
				if len(info.FlightDescriptor.Path) > 0 && info.FlightDescriptor.Path[0] == channel {
					found = true
					break
				}
			}
			if found {
				break
			}
			// Re-insert to ensure events reach the adapter.
			insertRow(t, connStr, table, map[string]any{"item": "retry"})
			time.Sleep(500 * time.Millisecond)
		}
		if !found {
			t.Fatal("channel not found in ListFlights")
		}

		// DoGet: retrieve events for the channel.
		ticket, _ := json.Marshal(map[string]any{"channel": channel, "offset": 0})
		stream, err := client.DoGet(ctx, &flight.Ticket{Ticket: ticket})
		if err != nil {
			t.Fatalf("DoGet: %v", err)
		}

		// Read at least one FlightData message.
		msg, err := stream.Recv()
		if err != nil {
			t.Fatalf("DoGet Recv: %v", err)
		}
		if len(msg.DataBody) == 0 && len(msg.DataHeader) == 0 {
			t.Error("DoGet returned empty FlightData")
		}
	})
}
