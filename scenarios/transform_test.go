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
	"github.com/florinutz/pgcdc/transform"
	"golang.org/x/sync/errgroup"
)

func TestScenario_Transform(t *testing.T) {
	connStr := startPostgres(t)
	channel := "transform_test"

	t.Run("happy path", func(t *testing.T) {
		// Pipeline with:
		// - global: drop ssn column
		// - per-adapter: mask email with hash, rename created_at → createdAt
		logger := testLogger()
		lc := newLineCapture()

		det := listennotify.New(connStr, []string{channel}, 0, 0, logger)
		p := pgcdc.NewPipeline(det,
			pgcdc.WithBusBuffer(64),
			pgcdc.WithLogger(logger),
			pgcdc.WithAdapter(stdout.New(lc, logger)),
			// Global transforms.
			pgcdc.WithTransform(transform.DropColumns("ssn")),
			// Per-adapter transforms.
			pgcdc.WithAdapterTransform("stdout", transform.Mask(
				transform.MaskField{Field: "email", Mode: transform.MaskHash},
			)),
			pgcdc.WithAdapterTransform("stdout", transform.RenameFields(map[string]string{
				"created_at": "createdAt",
			})),
		)

		ctx, cancel := context.WithCancel(context.Background())
		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error { return p.Run(gCtx) })
		t.Cleanup(func() {
			cancel()
			_ = g.Wait()
		})

		// Wait for detector to connect.
		waitForDetector(t, connStr, channel, lc)

		// Send a flat JSON payload with fields at top level.
		sendNotify(t, connStr, channel,
			`{"name":"Alice","ssn":"123-45-6789","email":"alice@example.com","created_at":"2024-01-01"}`)

		line := lc.waitLine(t, 5*time.Second)
		var ev map[string]any
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("unmarshal event: %v", err)
		}

		payload, ok := ev["payload"].(map[string]any)
		if !ok {
			t.Fatal("payload is not a map")
		}

		// ssn should be dropped.
		if _, found := payload["ssn"]; found {
			t.Error("ssn should have been dropped by DropColumns")
		}

		// email should be masked (SHA-256 hash, not the original).
		email, _ := payload["email"].(string)
		if email == "alice@example.com" || email == "" {
			t.Errorf("email should be hashed, got %q", email)
		}
		// Hash should be 64 hex chars.
		if len(email) != 64 {
			t.Errorf("email hash length = %d, want 64", len(email))
		}

		// created_at should be renamed to createdAt.
		if _, found := payload["created_at"]; found {
			t.Error("created_at should have been renamed")
		}
		if _, found := payload["createdAt"]; !found {
			t.Error("createdAt should exist after rename")
		}

		// Name should be untouched.
		if payload["name"] != "Alice" {
			t.Errorf("name = %v, want Alice", payload["name"])
		}
	})

	t.Run("cloudevents envelope", func(t *testing.T) {
		logger := testLogger()
		lc := newLineCapture()

		det := listennotify.New(connStr, []string{channel}, 0, 0, logger)
		p := pgcdc.NewPipeline(det,
			pgcdc.WithBusBuffer(64),
			pgcdc.WithLogger(logger),
			pgcdc.WithAdapter(stdout.New(lc, logger)),
			pgcdc.WithTransform(transform.CloudEvents()),
		)

		ctx, cancel := context.WithCancel(context.Background())
		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error { return p.Run(gCtx) })
		t.Cleanup(func() {
			cancel()
			_ = g.Wait()
		})

		waitForDetector(t, connStr, channel, lc)

		sendNotify(t, connStr, channel, `{"id":1,"name":"Alice"}`)

		line := lc.waitLine(t, 5*time.Second)
		var ev map[string]any
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("unmarshal event: %v", err)
		}

		payload, ok := ev["payload"].(map[string]any)
		if !ok {
			t.Fatal("payload is not a map")
		}

		// Verify CloudEvents envelope fields.
		if payload["specversion"] != "1.0" {
			t.Errorf("specversion = %v, want 1.0", payload["specversion"])
		}
		if payload["source"] != "/pgcdc" {
			t.Errorf("source = %v, want /pgcdc", payload["source"])
		}
		if payload["subject"] != channel {
			t.Errorf("subject = %v, want %s", payload["subject"], channel)
		}
		if payload["datacontenttype"] != "application/json" {
			t.Errorf("datacontenttype = %v, want application/json", payload["datacontenttype"])
		}

		// type should contain the operation (LISTEN/NOTIFY events have empty operation).
		ceType, _ := payload["type"].(string)
		if ceType == "" {
			t.Error("type should not be empty")
		}

		// data should contain the original payload.
		data, ok := payload["data"].(map[string]any)
		if !ok {
			t.Fatalf("data is not a map: %T", payload["data"])
		}
		if data["name"] != "Alice" {
			t.Errorf("data.name = %v, want Alice", data["name"])
		}
	})

	t.Run("filter drops non-matching events", func(t *testing.T) {
		// Pipeline with content-based filter: only pass events where payload
		// field "status" equals "published".
		logger := testLogger()
		lc := newLineCapture()

		det := listennotify.New(connStr, []string{channel}, 0, 0, logger)
		p := pgcdc.NewPipeline(det,
			pgcdc.WithBusBuffer(64),
			pgcdc.WithLogger(logger),
			pgcdc.WithAdapter(stdout.New(lc, logger)),
			pgcdc.WithTransform(transform.FilterField("status", "published")),
		)

		ctx, cancel := context.WithCancel(context.Background())
		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error { return p.Run(gCtx) })
		t.Cleanup(func() {
			cancel()
			_ = g.Wait()
		})

		// Wait for detector — send a probe that matches the filter.
		sendNotify(t, connStr, channel, `{"status":"published","__probe":true}`)
		lc.waitLine(t, 10*time.Second)

		// Send a "draft" event — should be filtered out.
		sendNotify(t, connStr, channel, `{"id":1,"status":"draft","title":"Draft Post"}`)

		// Send a "published" event — should pass through.
		sendNotify(t, connStr, channel, `{"id":2,"status":"published","title":"Live Post"}`)

		line := lc.waitLine(t, 5*time.Second)
		var ev map[string]any
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("unmarshal event: %v", err)
		}

		payload, ok := ev["payload"].(map[string]any)
		if !ok {
			t.Fatal("payload is not a map")
		}

		// The event that came through should be the "published" one.
		if payload["status"] != "published" {
			t.Errorf("expected published event to pass filter, got status=%v", payload["status"])
		}
		if payload["title"] != "Live Post" {
			t.Errorf("title = %v, want Live Post", payload["title"])
		}
	})
}
