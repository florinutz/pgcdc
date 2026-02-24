package wasm

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/pgcdcerr"
	"github.com/florinutz/pgcdc/transform"
)

const testPluginDir = "../../testdata/plugins/"

func TestWasmTransform_Noop(t *testing.T) {
	ctx := context.Background()
	rt := NewRuntime(nil)
	defer rt.Close(ctx)

	spec := config.PluginTransformSpec{
		Name: "noop",
		Path: testPluginDir + "noop.wasm",
	}
	wt, err := NewTransform(ctx, rt, spec, nil)
	if err != nil {
		t.Fatalf("NewTransform: %v", err)
	}
	defer wt.Close(ctx)

	ev := event.Event{
		ID:        "test-1",
		Channel:   "orders",
		Operation: "INSERT",
		Payload:   json.RawMessage(`{"name":"alice","age":30}`),
		Source:    "test",
		CreatedAt: time.Now().UTC(),
		LSN:       42,
	}

	fn := wt.TransformFunc()
	result, err := fn(ev)
	if err != nil {
		t.Fatalf("transform: %v", err)
	}

	if result.ID != ev.ID {
		t.Errorf("ID mismatch: got %s, want %s", result.ID, ev.ID)
	}
	if result.Channel != ev.Channel {
		t.Errorf("Channel mismatch: got %s, want %s", result.Channel, ev.Channel)
	}
	if result.LSN != ev.LSN {
		t.Errorf("LSN not preserved: got %d, want %d", result.LSN, ev.LSN)
	}
}

func TestWasmTransform_DropField(t *testing.T) {
	ctx := context.Background()
	rt := NewRuntime(nil)
	defer rt.Close(ctx)

	spec := config.PluginTransformSpec{
		Name: "drop_field",
		Path: testPluginDir + "drop_field.wasm",
	}
	wt, err := NewTransform(ctx, rt, spec, nil)
	if err != nil {
		t.Fatalf("NewTransform: %v", err)
	}
	defer wt.Close(ctx)

	ev := event.Event{
		ID:        "test-2",
		Channel:   "orders",
		Operation: "INSERT",
		Payload:   json.RawMessage(`{"name":"alice","secret":"s3cret"}`),
		Source:    "test",
		CreatedAt: time.Now().UTC(),
	}

	fn := wt.TransformFunc()
	result, err := fn(ev)
	if err != nil {
		t.Fatalf("transform: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(result.Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}

	if _, ok := payload["secret"]; ok {
		t.Error("secret field should have been removed")
	}
	if payload["name"] != "alice" {
		t.Errorf("name field mismatch: got %v", payload["name"])
	}
}

func TestWasmTransform_DropEvent(t *testing.T) {
	ctx := context.Background()
	rt := NewRuntime(nil)
	defer rt.Close(ctx)

	spec := config.PluginTransformSpec{
		Name: "drop_event",
		Path: testPluginDir + "drop_event.wasm",
	}
	wt, err := NewTransform(ctx, rt, spec, nil)
	if err != nil {
		t.Fatalf("NewTransform: %v", err)
	}
	defer wt.Close(ctx)

	ev := event.Event{
		ID:        "test-3",
		Channel:   "orders",
		Operation: "INSERT",
		Payload:   json.RawMessage(`{"data":"value"}`),
		Source:    "test",
		CreatedAt: time.Now().UTC(),
	}

	fn := wt.TransformFunc()
	_, err = fn(ev)
	if !errors.Is(err, transform.ErrDropEvent) {
		t.Fatalf("expected ErrDropEvent, got: %v", err)
	}
}

func TestWasmAdapter_EchoSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rt := NewRuntime(nil)
	defer rt.Close(ctx)

	spec := config.PluginAdapterSpec{
		Name: "echo",
		Path: testPluginDir + "echo_adapter.wasm",
	}
	wa, err := NewAdapter(ctx, rt, spec, nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}

	if wa.Name() != "echo" {
		t.Errorf("Name: got %s, want echo", wa.Name())
	}

	events := make(chan event.Event, 1)
	events <- event.Event{
		ID:        "test-4",
		Channel:   "orders",
		Operation: "INSERT",
		Payload:   json.RawMessage(`{"hello":"world"}`),
		Source:    "test",
		CreatedAt: time.Now().UTC(),
	}
	close(events)

	err = wa.Start(ctx, events)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
}

func TestWasmAdapter_ErrorGoesToDLQ(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rt := NewRuntime(nil)
	defer rt.Close(ctx)

	spec := config.PluginAdapterSpec{
		Name: "error-adapter",
		Path: testPluginDir + "error_adapter.wasm",
	}
	wa, err := NewAdapter(ctx, rt, spec, nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}

	// Track DLQ records.
	dlq := &testDLQ{}
	wa.SetDLQ(dlq)

	events := make(chan event.Event, 1)
	events <- event.Event{
		ID:        "test-5",
		Channel:   "orders",
		Operation: "INSERT",
		Payload:   json.RawMessage(`{"data":"value"}`),
		Source:    "test",
		CreatedAt: time.Now().UTC(),
	}
	close(events)

	err = wa.Start(ctx, events)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if dlq.count != 1 {
		t.Errorf("expected 1 DLQ record, got %d", dlq.count)
	}
}

func TestWasmDLQ_Record(t *testing.T) {
	ctx := context.Background()
	rt := NewRuntime(nil)
	defer rt.Close(ctx)

	spec := &config.PluginSpec{
		Path: testPluginDir + "mem_dlq.wasm",
	}
	d, err := NewDLQ(ctx, rt, "test-dlq", spec, nil)
	if err != nil {
		t.Fatalf("NewDLQ: %v", err)
	}
	defer func() { _ = d.Close() }()

	ev := event.Event{
		ID:        "test-6",
		Channel:   "orders",
		Operation: "INSERT",
		Payload:   json.RawMessage(`{"data":"value"}`),
		Source:    "test",
		CreatedAt: time.Now().UTC(),
	}

	err = d.Record(ctx, ev, "test-adapter", errors.New("delivery failed"))
	if err != nil {
		t.Fatalf("Record: %v", err)
	}
}

func TestWasmCheckpointStore_LoadSave(t *testing.T) {
	ctx := context.Background()
	rt := NewRuntime(nil)
	defer rt.Close(ctx)

	spec := &config.PluginSpec{
		Path: testPluginDir + "mem_checkpoint.wasm",
	}
	s, err := NewCheckpointStore(ctx, rt, "test-cp", spec, nil)
	if err != nil {
		t.Fatalf("NewCheckpointStore: %v", err)
	}
	defer func() { _ = s.Close() }()

	// Load with no prior save should return 0.
	lsn, err := s.Load(ctx, "test-slot")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if lsn != 0 {
		t.Errorf("expected LSN 0, got %d", lsn)
	}

	// Save and load back.
	if err := s.Save(ctx, "test-slot", 12345); err != nil {
		t.Fatalf("Save: %v", err)
	}

	lsn, err = s.Load(ctx, "test-slot")
	if err != nil {
		t.Fatalf("Load after save: %v", err)
	}
	if lsn != 12345 {
		t.Errorf("expected LSN 12345, got %d", lsn)
	}
}

func TestWasmTransform_BadPath(t *testing.T) {
	ctx := context.Background()
	rt := NewRuntime(nil)
	defer rt.Close(ctx)

	spec := config.PluginTransformSpec{
		Name: "bad",
		Path: "/nonexistent/plugin.wasm",
	}
	_, err := NewTransform(ctx, rt, spec, nil)
	if err == nil {
		t.Fatal("expected error for bad path")
	}
}

func TestPluginError_Unwrap(t *testing.T) {
	inner := errors.New("inner error")
	pe := &pgcdcerr.PluginError{Plugin: "test", Type: "transform", Err: inner}
	if !errors.Is(pe, inner) {
		t.Error("PluginError should unwrap to inner error")
	}
	if pe.Error() != "plugin test (transform): inner error" {
		t.Errorf("unexpected error string: %s", pe.Error())
	}
}

// testDLQ is a simple DLQ for unit testing that counts records.
type testDLQ struct {
	count int
}

func (d *testDLQ) Record(_ context.Context, _ event.Event, _ string, _ error) error {
	d.count++
	return nil
}

func (d *testDLQ) Close() error { return nil }
