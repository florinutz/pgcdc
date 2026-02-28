package chain_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/adapter/chain"
	"github.com/florinutz/pgcdc/event"
)

// --- test helpers ---

// mockLink records calls and optionally transforms or errors.
type mockLink struct {
	name      string
	transform func(event.Event) event.Event // nil = identity
	err       error                         // non-nil = return error
	calls     []event.Event
	mu        sync.Mutex
}

func (m *mockLink) Name() string { return m.name }

func (m *mockLink) Process(_ context.Context, ev event.Event) (event.Event, error) {
	m.mu.Lock()
	m.calls = append(m.calls, ev)
	m.mu.Unlock()
	if m.err != nil {
		return ev, m.err
	}
	if m.transform != nil {
		return m.transform(ev), nil
	}
	return ev, nil
}

func (m *mockLink) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

// collectTerminal collects all events received by the terminal adapter.
type collectTerminal struct {
	name   string
	events []event.Event
	mu     sync.Mutex
}

func (c *collectTerminal) Name() string { return c.name }

func (c *collectTerminal) Start(ctx context.Context, events <-chan event.Event) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-events:
			if !ok {
				return nil
			}
			c.mu.Lock()
			c.events = append(c.events, ev)
			c.mu.Unlock()
		}
	}
}

func (c *collectTerminal) received() []event.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	dst := make([]event.Event, len(c.events))
	copy(dst, c.events)
	return dst
}

func testEvent(id, payload string) event.Event {
	return event.Event{
		ID:        id,
		Channel:   "test-channel",
		Operation: "INSERT",
		Payload:   json.RawMessage(payload),
		Source:    "test",
		CreatedAt: time.Now().UTC(),
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// --- tests ---

func TestChain_SingleLink(t *testing.T) {
	link := &mockLink{
		name: "upper",
		transform: func(ev event.Event) event.Event {
			ev.Channel = "modified"
			return ev
		},
	}

	terminal := &collectTerminal{name: "collect"}
	a := chain.New("", terminal, discardLogger(), link)

	if a.Name() != "chain:collect" {
		t.Errorf("Name() = %q, want %q", a.Name(), "chain:collect")
	}

	ch := make(chan event.Event, 1)
	ch <- testEvent("e1", `{"key":"value"}`)
	close(ch)

	if err := a.Start(context.Background(), ch); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	got := terminal.received()
	if len(got) != 1 {
		t.Fatalf("terminal received %d events, want 1", len(got))
	}
	if got[0].Channel != "modified" {
		t.Errorf("event channel = %q, want %q", got[0].Channel, "modified")
	}
	if got[0].ID != "e1" {
		t.Errorf("event ID = %q, want %q", got[0].ID, "e1")
	}
	if link.callCount() != 1 {
		t.Errorf("link called %d times, want 1", link.callCount())
	}
}

func TestChain_MultipleLinks(t *testing.T) {
	// First link sets channel, second link sets operation.
	link1 := &mockLink{
		name: "set-channel",
		transform: func(ev event.Event) event.Event {
			ev.Channel = "step1"
			return ev
		},
	}
	link2 := &mockLink{
		name: "set-op",
		transform: func(ev event.Event) event.Event {
			// Verify it received the output of link1.
			if ev.Channel != "step1" {
				panic(fmt.Sprintf("link2 expected channel=step1, got %q", ev.Channel))
			}
			ev.Operation = "UPDATE"
			return ev
		},
	}

	terminal := &collectTerminal{name: "collect"}
	a := chain.New("my-chain", terminal, discardLogger(), link1, link2)

	if a.Name() != "my-chain" {
		t.Errorf("Name() = %q, want %q", a.Name(), "my-chain")
	}

	ch := make(chan event.Event, 1)
	ch <- testEvent("e1", `{"key":"value"}`)
	close(ch)

	if err := a.Start(context.Background(), ch); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	got := terminal.received()
	if len(got) != 1 {
		t.Fatalf("terminal received %d events, want 1", len(got))
	}
	if got[0].Channel != "step1" {
		t.Errorf("event channel = %q, want %q", got[0].Channel, "step1")
	}
	if got[0].Operation != "UPDATE" {
		t.Errorf("event operation = %q, want %q", got[0].Operation, "UPDATE")
	}
	if link1.callCount() != 1 {
		t.Errorf("link1 called %d times, want 1", link1.callCount())
	}
	if link2.callCount() != 1 {
		t.Errorf("link2 called %d times, want 1", link2.callCount())
	}
}

func TestChain_LinkError(t *testing.T) {
	errBoom := errors.New("boom")

	// First link always errors.
	link := &mockLink{name: "fail", err: errBoom}
	terminal := &collectTerminal{name: "collect"}
	a := chain.New("", terminal, discardLogger(), link)

	ch := make(chan event.Event, 2)
	ch <- testEvent("e1", `{"a":1}`)
	ch <- testEvent("e2", `{"a":2}`)
	close(ch)

	if err := a.Start(context.Background(), ch); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	got := terminal.received()
	if len(got) != 0 {
		t.Errorf("terminal received %d events, want 0 (both should be skipped)", len(got))
	}
	if link.callCount() != 2 {
		t.Errorf("link called %d times, want 2", link.callCount())
	}
}

func TestChain_SecondLinkError(t *testing.T) {
	// First link succeeds, second errors. Verify first link output is
	// discarded when second fails.
	link1 := &mockLink{
		name: "ok",
		transform: func(ev event.Event) event.Event {
			ev.Channel = "modified"
			return ev
		},
	}
	link2 := &mockLink{name: "fail", err: errors.New("link2 error")}

	terminal := &collectTerminal{name: "collect"}
	a := chain.New("", terminal, discardLogger(), link1, link2)

	ch := make(chan event.Event, 1)
	ch <- testEvent("e1", `{}`)
	close(ch)

	if err := a.Start(context.Background(), ch); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	got := terminal.received()
	if len(got) != 0 {
		t.Errorf("terminal received %d events, want 0", len(got))
	}
}

func TestChain_NoLinks(t *testing.T) {
	// Chain with no links passes events directly to terminal.
	terminal := &collectTerminal{name: "collect"}
	a := chain.New("", terminal, discardLogger())

	ch := make(chan event.Event, 1)
	ev := testEvent("e1", `{"pass":"through"}`)
	ch <- ev
	close(ch)

	if err := a.Start(context.Background(), ch); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	got := terminal.received()
	if len(got) != 1 {
		t.Fatalf("terminal received %d events, want 1", len(got))
	}
	if got[0].ID != "e1" {
		t.Errorf("event ID = %q, want %q", got[0].ID, "e1")
	}
}

func TestChain_ContextCancel(t *testing.T) {
	terminal := &collectTerminal{name: "collect"}
	a := chain.New("", terminal, discardLogger())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ch := make(chan event.Event)
	err := a.Start(ctx, ch)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestChain_MultipleEvents(t *testing.T) {
	counter := 0
	link := &mockLink{
		name: "count",
		transform: func(ev event.Event) event.Event {
			counter++
			ev.Source = fmt.Sprintf("count-%d", counter)
			return ev
		},
	}

	terminal := &collectTerminal{name: "collect"}
	a := chain.New("", terminal, discardLogger(), link)

	ch := make(chan event.Event, 3)
	ch <- testEvent("e1", `{}`)
	ch <- testEvent("e2", `{}`)
	ch <- testEvent("e3", `{}`)
	close(ch)

	if err := a.Start(context.Background(), ch); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	got := terminal.received()
	if len(got) != 3 {
		t.Fatalf("terminal received %d events, want 3", len(got))
	}
	if got[0].Source != "count-1" {
		t.Errorf("event[0].Source = %q, want %q", got[0].Source, "count-1")
	}
	if got[2].Source != "count-3" {
		t.Errorf("event[2].Source = %q, want %q", got[2].Source, "count-3")
	}
}

func TestCompress_RoundTrip(t *testing.T) {
	c := chain.NewCompress(gzip.BestSpeed)

	if c.Name() != "compress" {
		t.Errorf("Name() = %q, want %q", c.Name(), "compress")
	}

	original := `{"key":"value","nested":{"a":1,"b":2}}`
	ev := testEvent("e1", original)

	result, err := c.Process(context.Background(), ev)
	if err != nil {
		t.Fatalf("Process returned error: %v", err)
	}

	// Compressed data should differ from original.
	if bytes.Equal(result.Payload, ev.Payload) {
		t.Error("compressed payload equals original; expected different bytes")
	}

	// Decompress and verify round-trip.
	r, err := gzip.NewReader(bytes.NewReader(result.Payload))
	if err != nil {
		t.Fatalf("create gzip reader: %v", err)
	}
	decompressed, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read gzip: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close gzip reader: %v", err)
	}

	if string(decompressed) != original {
		t.Errorf("decompressed = %q, want %q", string(decompressed), original)
	}
}

func TestCompress_EmptyPayload(t *testing.T) {
	c := chain.NewCompress(0) // 0 triggers default level

	ev := testEvent("e1", "")
	ev.Payload = nil

	result, err := c.Process(context.Background(), ev)
	if err != nil {
		t.Fatalf("Process returned error: %v", err)
	}
	if result.Payload != nil {
		t.Errorf("expected nil payload for empty input, got %d bytes", len(result.Payload))
	}
}

func TestCompress_DefaultLevel(t *testing.T) {
	// Verify that level <= 0 defaults to gzip.DefaultCompression (no panic).
	c := chain.NewCompress(-1)
	ev := testEvent("e1", `{"test":true}`)

	result, err := c.Process(context.Background(), ev)
	if err != nil {
		t.Fatalf("Process returned error: %v", err)
	}
	if len(result.Payload) == 0 {
		t.Error("expected non-empty compressed payload")
	}
}

func TestBatchEvents(t *testing.T) {
	events := []event.Event{
		testEvent("e1", `{"a":1}`),
		testEvent("e2", `{"b":2}`),
		testEvent("e3", `{"c":3}`),
	}

	result, err := chain.BatchEvents(events)
	if err != nil {
		t.Fatalf("BatchEvents returned error: %v", err)
	}

	// Result should use first event's ID.
	if result.ID != "e1" {
		t.Errorf("batch event ID = %q, want %q", result.ID, "e1")
	}

	// Payload should be a JSON array.
	var payloads []json.RawMessage
	if err := json.Unmarshal(result.Payload, &payloads); err != nil {
		t.Fatalf("unmarshal batch payload: %v", err)
	}
	if len(payloads) != 3 {
		t.Fatalf("batch contains %d payloads, want 3", len(payloads))
	}

	// Verify individual payloads.
	expected := []string{`{"a":1}`, `{"b":2}`, `{"c":3}`}
	for i, p := range payloads {
		if string(p) != expected[i] {
			t.Errorf("payload[%d] = %s, want %s", i, string(p), expected[i])
		}
	}
}

func TestBatchEvents_Empty(t *testing.T) {
	_, err := chain.BatchEvents(nil)
	if err == nil {
		t.Error("expected error for empty batch, got nil")
	}
}

func TestBatchEvents_SingleEvent(t *testing.T) {
	events := []event.Event{testEvent("e1", `{"only":true}`)}

	result, err := chain.BatchEvents(events)
	if err != nil {
		t.Fatalf("BatchEvents returned error: %v", err)
	}

	var payloads []json.RawMessage
	if err := json.Unmarshal(result.Payload, &payloads); err != nil {
		t.Fatalf("unmarshal batch payload: %v", err)
	}
	if len(payloads) != 1 {
		t.Fatalf("batch contains %d payloads, want 1", len(payloads))
	}
	if string(payloads[0]) != `{"only":true}` {
		t.Errorf("payload[0] = %s, want %s", string(payloads[0]), `{"only":true}`)
	}
}

func TestBatch_PassThrough(t *testing.T) {
	b := chain.NewBatch(50)

	if b.Name() != "batch" {
		t.Errorf("Name() = %q, want %q", b.Name(), "batch")
	}
	if b.MaxSize() != 50 {
		t.Errorf("MaxSize() = %d, want %d", b.MaxSize(), 50)
	}

	ev := testEvent("e1", `{"pass":"through"}`)
	result, err := b.Process(context.Background(), ev)
	if err != nil {
		t.Fatalf("Process returned error: %v", err)
	}
	if result.ID != ev.ID {
		t.Errorf("event ID = %q, want %q", result.ID, ev.ID)
	}
}

func TestBatch_DefaultMaxSize(t *testing.T) {
	b := chain.NewBatch(0) // should default to 100
	if b.MaxSize() != 100 {
		t.Errorf("MaxSize() = %d, want 100 (default)", b.MaxSize())
	}
}

func TestChain_NilLogger(t *testing.T) {
	// Verify that a nil logger does not panic.
	terminal := &collectTerminal{name: "collect"}
	a := chain.New("", terminal, nil)

	ch := make(chan event.Event, 1)
	ch <- testEvent("e1", `{}`)
	close(ch)

	if err := a.Start(context.Background(), ch); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	got := terminal.received()
	if len(got) != 1 {
		t.Fatalf("terminal received %d events, want 1", len(got))
	}
}
