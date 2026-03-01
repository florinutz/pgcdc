//go:build integration

package scenarios

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/adapter/sse"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/server"
)

func TestScenario_SSEStreaming(t *testing.T) {
	connStr := startPostgres(t)
	logger := testLogger()

	broker := sse.New(256, 100*time.Millisecond, logger)
	startPipeline(t, connStr, []string{"sse_orders", "sse_users"}, broker)

	srv := server.New(broker, nil, []string{"*"}, 0, 0, nil, nil)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })

	baseURL := "http://" + ln.Addr().String()

	// Wait for detector to connect — send probes until one arrives via SSE.
	waitForSSEDetector(t, connStr, "sse_orders", baseURL+"/events")

	t.Run("happy path", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		events := connectSSE(ctx, t, baseURL+"/events")
		// Allow time for SSE subscription to register.
		time.Sleep(200 * time.Millisecond)

		sendNotify(t, connStr, "sse_orders", `{"op":"INSERT","table":"orders","row":{"id":1}}`)

		select {
		case ev := <-events:
			if ev.Event != "sse_orders" {
				t.Errorf("SSE event type = %q, want %q", ev.Event, "sse_orders")
			}
			var decoded event.Event
			if err := json.Unmarshal([]byte(ev.Data), &decoded); err != nil {
				t.Fatalf("invalid SSE data: %v", err)
			}
			if decoded.Operation != "INSERT" {
				t.Errorf("operation = %q, want INSERT", decoded.Operation)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for SSE event")
		}
	})

	t.Run("channel filter", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ordersOnly := connectSSE(ctx, t, baseURL+"/events/sse_orders")
		time.Sleep(200 * time.Millisecond)

		// Send on "users" channel — should NOT pass the filter.
		sendNotify(t, connStr, "sse_users", `{"op":"INSERT","table":"users","row":{"id":1}}`)
		// Send on "orders" channel — should be received.
		sendNotify(t, connStr, "sse_orders", `{"op":"DELETE","table":"orders","row":{"id":2}}`)

		select {
		case ev := <-ordersOnly:
			if ev.Event != "sse_orders" {
				t.Errorf("SSE event type = %q, want sse_orders", ev.Event)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for filtered SSE event")
		}

		// Verify no "users" event leaked through.
		select {
		case ev := <-ordersOnly:
			t.Fatalf("unexpected event on filtered channel: %+v", ev)
		case <-time.After(500 * time.Millisecond):
			// Good — no leakage.
		}
	})
}
