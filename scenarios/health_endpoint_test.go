//go:build integration

package scenarios

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/florinutz/pgpipe/adapter/sse"
	"github.com/florinutz/pgpipe/event"
	"github.com/florinutz/pgpipe/health"
	"github.com/florinutz/pgpipe/internal/server"
)

func TestScenario_HealthEndpoint(t *testing.T) {
	logger := testLogger()
	broker := sse.New(256, 15*time.Second, logger)

	// The broker needs an events channel even though this test won't send events.
	events := make(chan event.Event)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel() })
	go broker.Start(ctx, events)

	checker := health.NewChecker()
	checker.Register("detector")
	checker.Register("bus")
	checker.SetStatus("detector", health.StatusUp)
	checker.SetStatus("bus", health.StatusUp)

	srv := server.New(broker, []string{"*"}, 0, 0, checker)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })

	baseURL := "http://" + ln.Addr().String()

	t.Run("happy path", func(t *testing.T) {
		resp, err := http.Get(baseURL + "/healthz")
		if err != nil {
			t.Fatalf("GET /healthz: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("status = %d, want 200", resp.StatusCode)
		}
		if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", ct)
		}

		var body struct {
			Status     string            `json:"status"`
			Components map[string]string `json:"components"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if body.Status != "up" {
			t.Errorf("status = %q, want up", body.Status)
		}
		if body.Components["detector"] != "up" {
			t.Errorf("detector component = %q, want up", body.Components["detector"])
		}
		if body.Components["bus"] != "up" {
			t.Errorf("bus component = %q, want up", body.Components["bus"])
		}
	})

	t.Run("CORS preflight", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodOptions, baseURL+"/healthz", nil)
		req.Header.Set("Origin", "http://example.com")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("OPTIONS /healthz: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNoContent {
			t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusNoContent)
		}
		if acao := resp.Header.Get("Access-Control-Allow-Origin"); acao != "*" {
			t.Errorf("Access-Control-Allow-Origin = %q, want *", acao)
		}
		if acam := resp.Header.Get("Access-Control-Allow-Methods"); !strings.Contains(acam, "GET") {
			t.Errorf("Access-Control-Allow-Methods = %q, want contains GET", acam)
		}
		if acah := resp.Header.Get("Access-Control-Allow-Headers"); !strings.Contains(acah, "Last-Event-ID") {
			t.Errorf("Access-Control-Allow-Headers = %q, want contains Last-Event-ID", acah)
		}
	})
}
