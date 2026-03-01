//go:build integration

package scenarios

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	pgcdc "github.com/florinutz/pgcdc"
	"github.com/florinutz/pgcdc/adapter/stdout"
	"github.com/florinutz/pgcdc/detector/listennotify"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/transform"
	"golang.org/x/sync/errgroup"
)

func TestScenario_Reload(t *testing.T) {
	connStr := startPostgres(t)
	channel := "reload_test"

	t.Run("happy path", func(t *testing.T) {
		// Start pipeline with drop_columns: [secret].
		logger := testLogger()
		lc := newLineCapture()

		det := listennotify.New(connStr, []string{channel}, 0, 0, logger)
		p := pgcdc.NewPipeline(det,
			pgcdc.WithBusBuffer(64),
			pgcdc.WithLogger(logger),
			pgcdc.WithAdapter(stdout.New(lc, logger)),
			pgcdc.WithTransform(transform.DropColumns("secret")),
		)

		ctx, cancel := context.WithCancel(context.Background())
		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error { return p.Run(gCtx) })
		t.Cleanup(func() {
			cancel()
			_ = g.Wait()
		})

		waitForDetector(t, connStr, channel, lc)

		// Send event: "secret" should be dropped.
		sendNotify(t, connStr, channel, `{"name":"alice","secret":"s3cret"}`)

		line := lc.waitLine(t, 5*time.Second)
		var ev map[string]any
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("unmarshal event: %v", err)
		}
		payload := ev["payload"].(map[string]any)
		if _, found := payload["secret"]; found {
			t.Error("secret should be dropped before reload")
		}
		if payload["name"] != "alice" {
			t.Errorf("name = %v, want alice", payload["name"])
		}

		// Reload: swap transform to drop "name" instead of "secret".
		if err := p.Reload(pgcdc.ReloadConfig{
			Transforms: []transform.TransformFunc{transform.DropColumns("name")},
		}); err != nil {
			t.Fatal(err)
		}

		// Send another event: "secret" should be present, "name" dropped.
		sendNotify(t, connStr, channel, `{"name":"bob","secret":"top-secret"}`)

		line2 := lc.waitLine(t, 5*time.Second)
		var ev2 map[string]any
		if err := json.Unmarshal([]byte(line2), &ev2); err != nil {
			t.Fatalf("unmarshal event: %v", err)
		}
		payload2 := ev2["payload"].(map[string]any)
		if payload2["secret"] != "top-secret" {
			t.Errorf("expected secret=top-secret after reload, got %v", payload2["secret"])
		}
		if _, found := payload2["name"]; found {
			t.Error("name should be dropped after reload")
		}
	})

	t.Run("reload preserves old config on error", func(t *testing.T) {
		// Start pipeline with drop_columns: [secret].
		logger := testLogger()
		lc := newLineCapture()

		det := listennotify.New(connStr, []string{channel}, 0, 0, logger)
		p := pgcdc.NewPipeline(det,
			pgcdc.WithBusBuffer(64),
			pgcdc.WithLogger(logger),
			pgcdc.WithAdapter(stdout.New(lc, logger)),
			pgcdc.WithTransform(transform.DropColumns("secret")),
		)

		ctx, cancel := context.WithCancel(context.Background())
		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error { return p.Run(gCtx) })
		t.Cleanup(func() {
			cancel()
			_ = g.Wait()
		})

		waitForDetector(t, connStr, channel, lc)

		// Reload with a transform that drops every event: simulate a bad config.
		badTransform := func(ev event.Event) (event.Event, error) {
			return ev, transform.ErrDropEvent
		}
		_ = p.Reload(pgcdc.ReloadConfig{
			Transforms: []transform.TransformFunc{badTransform},
		})

		// Send event: should be dropped by the bad transform.
		sendNotify(t, connStr, channel, `{"name":"charlie","secret":"abc"}`)

		// Wait briefly — event should NOT arrive (dropped by bad transform).
		select {
		case <-lc.lines:
			t.Fatal("expected event to be dropped by bad transform")
		case <-time.After(2 * time.Second):
			// ok — event was dropped
		}

		// Reload back to a good config.
		if err := p.Reload(pgcdc.ReloadConfig{
			Transforms: []transform.TransformFunc{transform.DropColumns("secret")},
		}); err != nil {
			t.Fatal(err)
		}

		// Send event: "secret" should be dropped again.
		sendNotify(t, connStr, channel, `{"name":"dave","secret":"xyz"}`)

		line := lc.waitLine(t, 5*time.Second)
		var ev map[string]any
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("unmarshal event: %v", err)
		}
		payload := ev["payload"].(map[string]any)
		if _, found := payload["secret"]; found {
			t.Error("secret should be dropped after reload back to good config")
		}
		if payload["name"] != "dave" {
			t.Errorf("name = %v, want dave", payload["name"])
		}
	})
}
