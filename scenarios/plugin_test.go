//go:build integration

package scenarios

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/florinutz/pgcdc"
	"github.com/florinutz/pgcdc/adapter/stdout"
	"github.com/florinutz/pgcdc/detector/listennotify"
	"github.com/florinutz/pgcdc/dlq"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/config"
	wasmplug "github.com/florinutz/pgcdc/plugin/wasm"
	"golang.org/x/sync/errgroup"
)

const pluginDir = "../testdata/plugins/"

func TestScenario_WasmPlugin(t *testing.T) {
	connStr := startPostgres(t)

	t.Run("transform drops field", func(t *testing.T) {
		// Use a raw NOTIFY channel with a payload that has "secret" at the top level.
		channel := "pgcdc:plugin_secret"

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		logger := testLogger()
		rt := wasmplug.NewRuntime(logger)
		defer rt.Close(ctx)

		// Create the drop_field wasm transform.
		wt, err := wasmplug.NewTransform(ctx, rt, config.PluginTransformSpec{
			Name: "drop-secret",
			Path: pluginDir + "drop_field.wasm",
		}, logger)
		if err != nil {
			t.Fatalf("NewTransform: %v", err)
		}
		defer wt.Close(ctx)

		capture := newLineCapture()
		stdoutAdapter := stdout.New(capture, logger)

		det := listennotify.New(connStr, []string{channel}, 0, 0, logger)

		p := pgcdc.NewPipeline(det,
			pgcdc.WithAdapter(stdoutAdapter),
			pgcdc.WithBusBuffer(64),
			pgcdc.WithLogger(logger),
			pgcdc.WithTransform(wt.TransformFunc()),
		)

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error { return p.Run(gCtx) })

		// Wait for detector to connect.
		waitForDetector(t, connStr, channel, capture)

		// Send a NOTIFY with a payload that has "secret" at the top level.
		sendNotify(t, connStr, channel, `{"name":"alice","secret":"s3cret-value"}`)

		line := capture.waitLine(t, 5*time.Second)

		var ev event.Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("unmarshal event: %v", err)
		}

		var payload map[string]any
		if err := json.Unmarshal(ev.Payload, &payload); err != nil {
			t.Fatalf("unmarshal payload: %v", err)
		}

		if _, hasSecret := payload["secret"]; hasSecret {
			t.Error("secret field should have been removed from payload")
		}
		if payload["name"] != "alice" {
			t.Errorf("name field mismatch: got %v", payload["name"])
		}

		cancel()
		g.Wait()
	})

	t.Run("transform returns empty drops event", func(t *testing.T) {
		channel := createTrigger(t, connStr, "plugin_drop_event")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		logger := testLogger()
		rt := wasmplug.NewRuntime(logger)
		defer rt.Close(ctx)

		// Create the drop_event wasm transform (drops everything).
		wt, err := wasmplug.NewTransform(ctx, rt, config.PluginTransformSpec{
			Name: "drop-all",
			Path: pluginDir + "drop_event.wasm",
		}, logger)
		if err != nil {
			t.Fatalf("NewTransform: %v", err)
		}
		defer wt.Close(ctx)

		capture := newLineCapture()
		stdoutAdapter := stdout.New(capture, logger)

		det := listennotify.New(connStr, []string{channel}, 0, 0, logger)

		p := pgcdc.NewPipeline(det,
			pgcdc.WithAdapter(stdoutAdapter),
			pgcdc.WithBusBuffer(64),
			pgcdc.WithLogger(logger),
			pgcdc.WithTransform(wt.TransformFunc()),
		)

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error { return p.Run(gCtx) })

		time.Sleep(500 * time.Millisecond)

		// Insert events — they should all be dropped by the transform.
		insertRow(t, connStr, "plugin_drop_event", map[string]any{"data": "should-be-dropped"})

		// Wait briefly and confirm no output.
		select {
		case line := <-capture.lines:
			t.Fatalf("expected no output, got: %s", line)
		case <-time.After(2 * time.Second):
			// Good: no event received.
		}

		cancel()
		g.Wait()
	})

	t.Run("adapter receives events via handle", func(t *testing.T) {
		channel := createTrigger(t, connStr, "plugin_echo_adapter")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		logger := testLogger()
		rt := wasmplug.NewRuntime(logger)
		defer rt.Close(ctx)

		// Create wasm adapter.
		wa, err := wasmplug.NewAdapter(ctx, rt, config.PluginAdapterSpec{
			Name: "echo-test",
			Path: pluginDir + "echo_adapter.wasm",
		}, logger)
		if err != nil {
			t.Fatalf("NewAdapter: %v", err)
		}

		// Also add stdout so we can verify the event arrived.
		capture := newLineCapture()
		stdoutAdapter := stdout.New(capture, logger)

		det := listennotify.New(connStr, []string{channel}, 0, 0, logger)

		p := pgcdc.NewPipeline(det,
			pgcdc.WithAdapter(stdoutAdapter),
			pgcdc.WithAdapter(wa),
			pgcdc.WithBusBuffer(64),
			pgcdc.WithLogger(logger),
		)

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error { return p.Run(gCtx) })

		time.Sleep(500 * time.Millisecond)

		insertRow(t, connStr, "plugin_echo_adapter", map[string]any{"test": "echo"})

		// Verify stdout got the event (proves wasm adapter didn't break the pipeline).
		line := capture.waitLine(t, 5*time.Second)
		if line == "" {
			t.Fatal("expected stdout output")
		}

		cancel()
		g.Wait()
	})

	t.Run("adapter error goes to DLQ", func(t *testing.T) {
		channel := createTrigger(t, connStr, "plugin_error_adapter")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		logger := testLogger()
		rt := wasmplug.NewRuntime(logger)
		defer rt.Close(ctx)

		// Create wasm adapter that always errors.
		wa, err := wasmplug.NewAdapter(ctx, rt, config.PluginAdapterSpec{
			Name: "error-test",
			Path: pluginDir + "error_adapter.wasm",
		}, logger)
		if err != nil {
			t.Fatalf("NewAdapter: %v", err)
		}

		// Set up stderr DLQ to track records.
		dlqCapture := &countingDLQ{}
		wa.SetDLQ(dlqCapture)

		det := listennotify.New(connStr, []string{channel}, 0, 0, logger)

		p := pgcdc.NewPipeline(det,
			pgcdc.WithAdapter(wa),
			pgcdc.WithBusBuffer(64),
			pgcdc.WithLogger(logger),
			pgcdc.WithDLQ(dlqCapture),
		)

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error { return p.Run(gCtx) })

		time.Sleep(500 * time.Millisecond)

		insertRow(t, connStr, "plugin_error_adapter", map[string]any{"test": "error"})

		// Wait for the DLQ record.
		deadline := time.After(5 * time.Second)
		for {
			if dlqCapture.Count() > 0 {
				break
			}
			select {
			case <-deadline:
				t.Fatal("timed out waiting for DLQ record")
			case <-time.After(100 * time.Millisecond):
			}
		}

		cancel()
		g.Wait()
	})

	t.Run("DLQ plugin records failed events", func(t *testing.T) {
		ctx := context.Background()
		rt := wasmplug.NewRuntime(testLogger())
		defer rt.Close(ctx)

		d, err := wasmplug.NewDLQ(ctx, rt, "test-dlq", &config.PluginSpec{
			Path: pluginDir + "mem_dlq.wasm",
		}, testLogger())
		if err != nil {
			t.Fatalf("NewDLQ: %v", err)
		}
		defer d.Close()

		ev := event.Event{
			ID:        "dlq-test-1",
			Channel:   "orders",
			Operation: "INSERT",
			Payload:   json.RawMessage(`{"data":"value"}`),
			Source:    "test",
			CreatedAt: time.Now().UTC(),
		}

		if err := d.Record(ctx, ev, "test-adapter", errors.New("delivery failed")); err != nil {
			t.Fatalf("Record: %v", err)
		}
	})

	t.Run("checkpoint store load/save round-trip", func(t *testing.T) {
		ctx := context.Background()
		rt := wasmplug.NewRuntime(testLogger())
		defer rt.Close(ctx)

		s, err := wasmplug.NewCheckpointStore(ctx, rt, "test-cp", &config.PluginSpec{
			Path: pluginDir + "mem_checkpoint.wasm",
		}, testLogger())
		if err != nil {
			t.Fatalf("NewCheckpointStore: %v", err)
		}
		defer s.Close()

		// Load with no prior save.
		lsn, err := s.Load(ctx, "test-slot")
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if lsn != 0 {
			t.Errorf("expected LSN 0, got %d", lsn)
		}

		// Save and load back.
		if err := s.Save(ctx, "test-slot", 99999); err != nil {
			t.Fatalf("Save: %v", err)
		}

		lsn, err = s.Load(ctx, "test-slot")
		if err != nil {
			t.Fatalf("Load after save: %v", err)
		}
		if lsn != 99999 {
			t.Errorf("expected LSN 99999, got %d", lsn)
		}
	})
}

// countingDLQ counts DLQ records for testing (thread-safe).
type countingDLQ struct {
	count atomic.Int32
}

func (d *countingDLQ) Record(_ context.Context, _ event.Event, _ string, _ error) error {
	d.count.Add(1)
	return nil
}

func (d *countingDLQ) Count() int32 { return d.count.Load() }

func (d *countingDLQ) Close() error { return nil }

// Ensure countingDLQ satisfies dlq.DLQ at compile time.
var _ dlq.DLQ = (*countingDLQ)(nil)
