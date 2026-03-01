//go:build integration

package scenarios

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/adapter/stdout"
	"github.com/florinutz/pgcdc/bus"
	"github.com/florinutz/pgcdc/detector/webhookgw"
	"github.com/florinutz/pgcdc/event"
	"github.com/go-chi/chi/v5"
	"golang.org/x/sync/errgroup"
)

func TestScenario_WebhookGateway(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		sources := []*webhookgw.Source{{Name: "test", ChannelPrefix: "pgcdc:test"}}
		det := webhookgw.New(sources, 0, testLogger())

		r := chi.NewRouter()
		det.MountHTTP(r)
		srv := httptest.NewServer(r)
		defer srv.Close()

		capture := newLineCapture()
		logger := testLogger()
		b := bus.New(64, logger)
		stdoutAdapter := stdout.New(capture, logger)

		sub, err := b.Subscribe(stdoutAdapter.Name())
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error { return b.Start(gCtx) })
		g.Go(func() error { return det.Start(gCtx, b.Ingest()) })
		g.Go(func() error { return stdoutAdapter.Start(gCtx, sub) })
		t.Cleanup(func() {
			cancel()
			_ = g.Wait()
		})

		// Wait for detector readiness (Start closes d.ready immediately).
		time.Sleep(200 * time.Millisecond)

		resp, err := http.Post(srv.URL+"/ingest/test", "application/json",
			strings.NewReader(`{"hello":"world"}`))
		if err != nil {
			t.Fatalf("POST /ingest/test: %v", err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("POST /ingest/test: status %d, body: %s", resp.StatusCode, body)
		}

		var respBody map[string]string
		json.Unmarshal(body, &respBody)
		if respBody["status"] != "ok" {
			t.Errorf("response status = %q, want ok", respBody["status"])
		}
		if respBody["event_id"] == "" {
			t.Error("response event_id is empty")
		}

		line := capture.waitLine(t, 5*time.Second)
		var ev event.Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("invalid JSON: %v\nraw: %s", err, line)
		}
		if ev.Channel != "pgcdc:test" {
			t.Errorf("channel = %q, want pgcdc:test", ev.Channel)
		}
		if ev.Operation != "INSERT" {
			t.Errorf("operation = %q, want INSERT", ev.Operation)
		}
		if ev.Source != "webhook_gateway" {
			t.Errorf("source = %q, want webhook_gateway", ev.Source)
		}

		var payload map[string]any
		if err := json.Unmarshal(ev.Payload, &payload); err != nil {
			t.Fatalf("invalid payload JSON: %v", err)
		}
		if payload["hello"] != "world" {
			t.Errorf("payload.hello = %v, want world", payload["hello"])
		}
	})

	t.Run("unknown source returns 404", func(t *testing.T) {
		sources := []*webhookgw.Source{{Name: "test"}}
		det := webhookgw.New(sources, 0, testLogger())

		r := chi.NewRouter()
		det.MountHTTP(r)
		srv := httptest.NewServer(r)
		defer srv.Close()

		ctx, cancel := context.WithCancel(context.Background())
		g, gCtx := errgroup.WithContext(ctx)
		events := make(chan event.Event, 10)
		g.Go(func() error { return det.Start(gCtx, events) })
		t.Cleanup(func() {
			cancel()
			_ = g.Wait()
		})
		time.Sleep(100 * time.Millisecond)

		resp, err := http.Post(srv.URL+"/ingest/unknown", "application/json",
			strings.NewReader(`{}`))
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("status = %d, want 404", resp.StatusCode)
		}
	})

	t.Run("body too large returns 413", func(t *testing.T) {
		sources := []*webhookgw.Source{{Name: "test"}}
		det := webhookgw.New(sources, 100, testLogger()) // 100 byte limit

		r := chi.NewRouter()
		det.MountHTTP(r)
		srv := httptest.NewServer(r)
		defer srv.Close()

		ctx, cancel := context.WithCancel(context.Background())
		g, gCtx := errgroup.WithContext(ctx)
		events := make(chan event.Event, 10)
		g.Go(func() error { return det.Start(gCtx, events) })
		t.Cleanup(func() {
			cancel()
			_ = g.Wait()
		})
		time.Sleep(100 * time.Millisecond)

		// Send body larger than 100 bytes.
		largeBody := bytes.Repeat([]byte("x"), 200)
		resp, err := http.Post(srv.URL+"/ingest/test", "application/json",
			bytes.NewReader(largeBody))
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusRequestEntityTooLarge {
			t.Errorf("status = %d, want 413", resp.StatusCode)
		}
	})
}
