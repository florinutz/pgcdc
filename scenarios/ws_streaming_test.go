//go:build integration

package scenarios

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/florinutz/pgpipe/adapter/ws"
	"github.com/florinutz/pgpipe/bus"
	"github.com/florinutz/pgpipe/detector/listennotify"
	"github.com/florinutz/pgpipe/event"
	"github.com/florinutz/pgpipe/internal/server"
	"golang.org/x/sync/errgroup"
)

func TestScenario_WSStreaming(t *testing.T) {
	connStr := startPostgres(t)

	t.Run("happy path", func(t *testing.T) {
		addr, cleanup := startPipelineWithWS(t, connStr, []string{"ws_test_happy"})
		defer cleanup()

		// Connect WebSocket client.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		messages := connectWS(ctx, t, "ws://"+addr+"/ws")

		// Allow connection to establish.
		time.Sleep(300 * time.Millisecond)

		sendNotify(t, connStr, "ws_test_happy", `{"item":"gadget"}`)

		select {
		case msg := <-messages:
			var ev event.Event
			if err := json.Unmarshal([]byte(msg), &ev); err != nil {
				t.Fatalf("unmarshal ws event: %v", err)
			}
			if ev.Channel != "ws_test_happy" {
				t.Errorf("channel = %q, want ws_test_happy", ev.Channel)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for ws message")
		}
	})

	t.Run("channel filter", func(t *testing.T) {
		addr, cleanup := startPipelineWithWS(t, connStr, []string{"ws_orders", "ws_users"})
		defer cleanup()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Connect to filtered endpoint.
		messages := connectWS(ctx, t, "ws://"+addr+"/ws/ws_orders")

		time.Sleep(300 * time.Millisecond)

		// Send to non-matching channel first.
		sendNotify(t, connStr, "ws_users", `{"name":"alice"}`)
		time.Sleep(300 * time.Millisecond)

		// Send to matching channel.
		sendNotify(t, connStr, "ws_orders", `{"order":99}`)

		select {
		case msg := <-messages:
			var ev event.Event
			if err := json.Unmarshal([]byte(msg), &ev); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if ev.Channel != "ws_orders" {
				t.Errorf("expected ws_orders event, got channel=%q", ev.Channel)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for filtered ws message")
		}
	})
}

// ─── WS test helpers ─────────────────────────────────────────────────────────

// connectWS dials a WebSocket endpoint and returns a channel of text messages.
func connectWS(ctx context.Context, t *testing.T, url string) <-chan string {
	t.Helper()
	c, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	messages := make(chan string, 100)
	go func() {
		defer c.CloseNow()
		for {
			_, data, err := c.Read(ctx)
			if err != nil {
				return
			}
			messages <- string(data)
		}
	}()
	t.Cleanup(func() { _ = c.Close(websocket.StatusNormalClosure, "test done") })
	return messages
}

// startPipelineWithWS wires detector -> bus -> ws broker + HTTP server and
// returns the listening address and a cleanup function.
func startPipelineWithWS(t *testing.T, connStr string, channels []string) (string, func()) {
	t.Helper()

	logger := testLogger()
	ctx, cancel := context.WithCancel(context.Background())

	det := listennotify.New(connStr, channels, 0, 0, logger)
	b := bus.New(64, logger)
	wsBroker := ws.New(64, 15*time.Second, logger)

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error { return b.Start(gCtx) })
	g.Go(func() error { return det.Start(gCtx, b.Ingest()) })

	sub, err := b.Subscribe("ws")
	if err != nil {
		cancel()
		t.Fatalf("subscribe ws: %v", err)
	}
	g.Go(func() error { return wsBroker.Start(gCtx, sub) })

	// Start HTTP server with WS routes.
	httpServer := server.New(nil, wsBroker, nil, 0, 0, nil)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		cancel()
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	g.Go(func() error {
		if err := httpServer.Serve(ln); err != nil && err != http.ErrServerClosed {
			return fmt.Errorf("http serve: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		<-gCtx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		return httpServer.Shutdown(shutdownCtx)
	})

	cleanup := func() {
		cancel()
		_ = g.Wait()
	}

	return addr, cleanup
}
