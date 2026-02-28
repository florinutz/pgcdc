package multi

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/event"
)

// mockDetector emits N events then returns. If err is set, returns it instead.
type mockDetector struct {
	name    string
	count   int
	err     error
	delay   time.Duration // delay before returning (simulates long-running detector)
	started chan struct{} // closed when Start is called
}

func newMockDetector(name string, count int) *mockDetector {
	return &mockDetector{
		name:    name,
		count:   count,
		started: make(chan struct{}),
	}
}

func newFailingDetector(name string, err error) *mockDetector {
	return &mockDetector{
		name:    name,
		err:     err,
		started: make(chan struct{}),
	}
}

func (m *mockDetector) Name() string { return m.name }

func (m *mockDetector) Start(ctx context.Context, events chan<- event.Event) error {
	close(m.started)

	if m.err != nil {
		return m.err
	}

	for i := range m.count {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		ev, err := event.New(
			fmt.Sprintf("pgcdc:%s", m.name),
			"INSERT",
			json.RawMessage(fmt.Sprintf(`{"i":%d,"source":"%s"}`, i, m.name)),
			m.name,
		)
		if err != nil {
			return fmt.Errorf("create event: %w", err)
		}
		events <- ev
	}

	if m.delay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(m.delay):
		}
	}

	return nil
}

func TestMultiDetector_Sequential(t *testing.T) {
	d1 := newMockDetector("detector-a", 3)
	d2 := newMockDetector("detector-b", 2)

	md := New(Sequential, nil,
		ChildDetector{Name: "a", Detector: d1},
		ChildDetector{Name: "b", Detector: d2},
	)

	if md.Name() != "multi" {
		t.Fatalf("expected name 'multi', got %q", md.Name())
	}

	events := make(chan event.Event, 10)
	ctx := context.Background()

	err := md.Start(ctx, events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	close(events)
	var received []event.Event
	for ev := range events {
		received = append(received, ev)
	}

	if len(received) != 5 {
		t.Fatalf("expected 5 events, got %d", len(received))
	}

	// In sequential mode, detector-a events come first, then detector-b
	for i := 0; i < 3; i++ {
		if received[i].Source != "detector-a" {
			t.Errorf("event %d: expected source 'detector-a', got %q", i, received[i].Source)
		}
	}
	for i := 3; i < 5; i++ {
		if received[i].Source != "detector-b" {
			t.Errorf("event %d: expected source 'detector-b', got %q", i, received[i].Source)
		}
	}
}

func TestMultiDetector_Parallel(t *testing.T) {
	d1 := newMockDetector("detector-a", 3)
	d2 := newMockDetector("detector-b", 2)

	md := New(Parallel, nil,
		ChildDetector{Name: "a", Detector: d1},
		ChildDetector{Name: "b", Detector: d2},
	)

	events := make(chan event.Event, 10)
	ctx := context.Background()

	err := md.Start(ctx, events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	close(events)
	var received []event.Event
	for ev := range events {
		received = append(received, ev)
	}

	if len(received) != 5 {
		t.Fatalf("expected 5 events, got %d", len(received))
	}

	// Both sources should be present (order is non-deterministic in parallel)
	sources := map[string]int{}
	for _, ev := range received {
		sources[ev.Source]++
	}
	if sources["detector-a"] != 3 {
		t.Errorf("expected 3 events from detector-a, got %d", sources["detector-a"])
	}
	if sources["detector-b"] != 2 {
		t.Errorf("expected 2 events from detector-b, got %d", sources["detector-b"])
	}
}

func TestMultiDetector_SequentialError(t *testing.T) {
	d1 := newMockDetector("detector-a", 2)
	d2 := newFailingDetector("detector-b", fmt.Errorf("connection refused"))

	md := New(Sequential, nil,
		ChildDetector{Name: "a", Detector: d1},
		ChildDetector{Name: "b", Detector: d2},
	)

	events := make(chan event.Event, 10)
	ctx := context.Background()

	err := md.Start(ctx, events)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if got := err.Error(); got != "detector detector-b: connection refused" {
		t.Fatalf("unexpected error: %q", got)
	}

	// First detector's events should still have been emitted
	close(events)
	var received []event.Event
	for ev := range events {
		received = append(received, ev)
	}
	if len(received) != 2 {
		t.Fatalf("expected 2 events from first detector, got %d", len(received))
	}
}

func TestMultiDetector_ParallelError(t *testing.T) {
	// d1 runs with a delay so it's still running when d2 fails
	d1 := newMockDetector("detector-a", 1)
	d1.delay = 2 * time.Second

	d2 := newFailingDetector("detector-b", fmt.Errorf("connection refused"))

	md := New(Parallel, nil,
		ChildDetector{Name: "a", Detector: d1},
		ChildDetector{Name: "b", Detector: d2},
	)

	events := make(chan event.Event, 10)
	ctx := context.Background()

	err := md.Start(ctx, events)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if got := err.Error(); got != "detector detector-b: connection refused" {
		t.Fatalf("unexpected error: %q", got)
	}
}

func TestMultiDetector_Empty(t *testing.T) {
	md := New(Sequential, nil)

	events := make(chan event.Event, 1)
	err := md.Start(context.Background(), events)
	if err == nil {
		t.Fatal("expected error for empty multi-detector")
	}
	if got := err.Error(); got != "multi-detector: no child detectors configured" {
		t.Fatalf("unexpected error: %q", got)
	}
}

func TestMultiDetector_ParallelConcurrency(t *testing.T) {
	// Verify both detectors truly run concurrently by checking that both
	// have started before either finishes.
	d1 := newMockDetector("detector-a", 0)
	d1.delay = 100 * time.Millisecond
	d2 := newMockDetector("detector-b", 0)
	d2.delay = 100 * time.Millisecond

	md := New(Parallel, nil,
		ChildDetector{Name: "a", Detector: d1},
		ChildDetector{Name: "b", Detector: d2},
	)

	events := make(chan event.Event, 10)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		md.Start(context.Background(), events)
	}()

	// Both should start within a short window
	select {
	case <-d1.started:
	case <-time.After(time.Second):
		t.Fatal("detector-a did not start in time")
	}
	select {
	case <-d2.started:
	case <-time.After(time.Second):
		t.Fatal("detector-b did not start in time")
	}

	wg.Wait()
}

func TestMultiDetector_SequentialContextCancel(t *testing.T) {
	// A long-running detector should respect context cancellation
	d1 := newMockDetector("detector-a", 0)
	d1.delay = 5 * time.Second

	md := New(Sequential, nil,
		ChildDetector{Name: "a", Detector: d1},
	)

	events := make(chan event.Event, 10)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- md.Start(ctx, events)
	}()

	// Wait for detector to start, then cancel
	select {
	case <-d1.started:
	case <-time.After(time.Second):
		t.Fatal("detector did not start in time")
	}
	cancel()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after context cancel")
	}
}

func TestMultiDetector_SequentialOrder(t *testing.T) {
	// Verify strict ordering: events from each detector arrive in the order
	// that detector was listed, with no interleaving.
	names := []string{"first", "second", "third"}
	var children []ChildDetector
	for _, name := range names {
		children = append(children, ChildDetector{
			Name:     name,
			Detector: newMockDetector(name, 2),
		})
	}

	md := New(Sequential, nil, children...)
	events := make(chan event.Event, 20)

	err := md.Start(context.Background(), events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	close(events)
	var sources []string
	for ev := range events {
		sources = append(sources, ev.Source)
	}

	expected := []string{"first", "first", "second", "second", "third", "third"}
	if len(sources) != len(expected) {
		t.Fatalf("expected %d events, got %d", len(expected), len(sources))
	}
	for i := range expected {
		if sources[i] != expected[i] {
			t.Errorf("event %d: expected source %q, got %q", i, expected[i], sources[i])
		}
	}
}

func TestMultiDetector_ParallelAllSources(t *testing.T) {
	// Verify all sources contribute events, sorted for deterministic comparison.
	names := []string{"alpha", "beta", "gamma"}
	var children []ChildDetector
	for _, name := range names {
		children = append(children, ChildDetector{
			Name:     name,
			Detector: newMockDetector(name, 1),
		})
	}

	md := New(Parallel, nil, children...)
	events := make(chan event.Event, 10)

	err := md.Start(context.Background(), events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	close(events)
	var sources []string
	for ev := range events {
		sources = append(sources, ev.Source)
	}

	sort.Strings(sources)
	expected := []string{"alpha", "beta", "gamma"}
	if len(sources) != len(expected) {
		t.Fatalf("expected %d events, got %d", len(expected), len(sources))
	}
	for i := range expected {
		if sources[i] != expected[i] {
			t.Errorf("source %d: expected %q, got %q", i, expected[i], sources[i])
		}
	}
}

func TestMultiDetector_Failover(t *testing.T) {
	// First two detectors fail, third succeeds.
	d1 := newFailingDetector("detector-a", fmt.Errorf("connection refused"))
	d2 := newFailingDetector("detector-b", fmt.Errorf("timeout"))
	d3 := newMockDetector("detector-c", 3)

	md := New(Failover, nil,
		ChildDetector{Name: "a", Detector: d1},
		ChildDetector{Name: "b", Detector: d2},
		ChildDetector{Name: "c", Detector: d3},
	)

	events := make(chan event.Event, 10)
	err := md.Start(context.Background(), events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	close(events)
	var received []event.Event
	for ev := range events {
		received = append(received, ev)
	}

	if len(received) != 3 {
		t.Fatalf("expected 3 events from third detector, got %d", len(received))
	}
	for i, ev := range received {
		if ev.Source != "detector-c" {
			t.Errorf("event %d: expected source 'detector-c', got %q", i, ev.Source)
		}
	}
}

func TestMultiDetector_FailoverAllFail(t *testing.T) {
	// All detectors fail — error wraps the last one.
	d1 := newFailingDetector("detector-a", fmt.Errorf("connection refused"))
	d2 := newFailingDetector("detector-b", fmt.Errorf("timeout"))
	d3 := newFailingDetector("detector-c", fmt.Errorf("auth failed"))

	md := New(Failover, nil,
		ChildDetector{Name: "a", Detector: d1},
		ChildDetector{Name: "b", Detector: d2},
		ChildDetector{Name: "c", Detector: d3},
	)

	events := make(chan event.Event, 10)
	err := md.Start(context.Background(), events)
	if err == nil {
		t.Fatal("expected error when all failover detectors fail, got nil")
	}

	expected := "all failover detectors exhausted: detector detector-c: auth failed"
	if got := err.Error(); got != expected {
		t.Fatalf("expected error %q, got %q", expected, got)
	}
}

func TestMultiDetector_FailoverContextCancel(t *testing.T) {
	// A long-running failover detector should respect context cancellation.
	d1 := newMockDetector("detector-a", 0)
	d1.delay = 5 * time.Second

	md := New(Failover, nil,
		ChildDetector{Name: "a", Detector: d1},
	)

	events := make(chan event.Event, 10)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- md.Start(ctx, events)
	}()

	select {
	case <-d1.started:
	case <-time.After(time.Second):
		t.Fatal("detector did not start in time")
	}
	cancel()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after context cancel")
	}
}

// duplicatingDetector emits events with specific IDs (for dedup testing).
type duplicatingDetector struct {
	name     string
	eventIDs []string
	started  chan struct{}
}

func newDuplicatingDetector(name string, ids []string) *duplicatingDetector {
	return &duplicatingDetector{
		name:     name,
		eventIDs: ids,
		started:  make(chan struct{}),
	}
}

func (d *duplicatingDetector) Name() string { return d.name }

func (d *duplicatingDetector) Start(ctx context.Context, events chan<- event.Event) error {
	close(d.started)
	for _, id := range d.eventIDs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		ev := event.Event{
			ID:        id,
			Channel:   fmt.Sprintf("pgcdc:%s", d.name),
			Operation: "INSERT",
			Payload:   json.RawMessage(fmt.Sprintf(`{"source":"%s","id":"%s"}`, d.name, id)),
			Source:    d.name,
		}
		events <- ev
	}
	return nil
}

func TestMultiDetector_ParallelDedupWindow(t *testing.T) {
	// Two detectors emit events with overlapping IDs. With dedup enabled,
	// each unique ID should be delivered only once.
	sharedIDs := []string{"aaa", "bbb", "ccc"}
	d1 := newDuplicatingDetector("detector-a", sharedIDs)
	d2 := newDuplicatingDetector("detector-b", sharedIDs)

	md := New(Parallel, nil,
		ChildDetector{Name: "a", Detector: d1},
		ChildDetector{Name: "b", Detector: d2},
	)
	md.DedupWindow = 5 * time.Second

	events := make(chan event.Event, 20)
	err := md.Start(context.Background(), events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	close(events)
	var received []event.Event
	for ev := range events {
		received = append(received, ev)
	}

	// Each shared ID should appear exactly once (3 unique IDs total).
	if len(received) != 3 {
		t.Fatalf("expected 3 deduplicated events, got %d", len(received))
	}

	seenIDs := map[string]bool{}
	for _, ev := range received {
		if seenIDs[ev.ID] {
			t.Errorf("duplicate event ID delivered: %s", ev.ID)
		}
		seenIDs[ev.ID] = true
	}

	for _, id := range sharedIDs {
		if !seenIDs[id] {
			t.Errorf("missing event ID: %s", id)
		}
	}
}

func TestMultiDetector_ParallelNoDedupWindow(t *testing.T) {
	// Without dedup, duplicate IDs are delivered as-is.
	sharedIDs := []string{"aaa", "bbb"}
	d1 := newDuplicatingDetector("detector-a", sharedIDs)
	d2 := newDuplicatingDetector("detector-b", sharedIDs)

	md := New(Parallel, nil,
		ChildDetector{Name: "a", Detector: d1},
		ChildDetector{Name: "b", Detector: d2},
	)
	// DedupWindow is zero (default), no dedup.

	events := make(chan event.Event, 20)
	err := md.Start(context.Background(), events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	close(events)
	var received []event.Event
	for ev := range events {
		received = append(received, ev)
	}

	// All 4 events should be delivered (2 from each detector).
	if len(received) != 4 {
		t.Fatalf("expected 4 events without dedup, got %d", len(received))
	}
}
