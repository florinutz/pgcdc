package pgcdc

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/transform"
)

// testDetector pushes events on demand via its events channel.
type testDetector struct {
	events chan event.Event
}

func (d *testDetector) Start(ctx context.Context, out chan<- event.Event) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev := <-d.events:
			out <- ev
		}
	}
}

func (d *testDetector) Name() string { return "test" }

// collectAdapter collects events into a channel.
type collectAdapter struct {
	name     string
	received chan event.Event
}

func (a *collectAdapter) Start(ctx context.Context, events <-chan event.Event) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-events:
			if !ok {
				return nil
			}
			a.received <- ev
		}
	}
}

func (a *collectAdapter) Name() string { return a.name }

func makeEvent(channel string, payload map[string]any) event.Event {
	raw, _ := json.Marshal(payload)
	ev, _ := event.New(channel, "INSERT", raw, "test")
	return ev
}

func waitEvent(t *testing.T, ch <-chan event.Event, timeout time.Duration) event.Event {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(timeout):
		t.Fatal("timed out waiting for event")
		return event.Event{}
	}
}

func assertNoEvent(t *testing.T, ch <-chan event.Event, wait time.Duration) {
	t.Helper()
	select {
	case ev := <-ch:
		t.Fatalf("unexpected event: %+v", ev)
	case <-time.After(wait):
		// ok
	}
}

func TestPipeline_CooperativeCheckpoint_RejectsNonAcknowledger(t *testing.T) {
	det := &testDetector{events: make(chan event.Event, 10)}
	// collectAdapter does not implement Acknowledger.
	collector := &collectAdapter{name: "no-ack", received: make(chan event.Event, 10)}

	p := NewPipeline(det,
		WithAdapter(collector),
		WithCooperativeCheckpoint(true),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := p.Run(ctx)
	if err == nil {
		t.Fatal("expected error when cooperative checkpoint used with non-Acknowledger adapter")
	}
	if !contains(err.Error(), "cooperative checkpoint requires all adapters to implement Acknowledger") {
		t.Errorf("unexpected error: %v", err)
	}
	if !contains(err.Error(), "no-ack") {
		t.Errorf("expected adapter name in error, got: %v", err)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestPipeline_Reload_SwapsTransforms(t *testing.T) {
	det := &testDetector{events: make(chan event.Event, 10)}
	collector := &collectAdapter{name: "test-adapter", received: make(chan event.Event, 10)}

	// Start with a transform that drops the "secret" column.
	p := NewPipeline(det,
		WithAdapter(collector),
		WithTransform(transform.DropColumns("secret")),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- p.Run(ctx) }()

	// Give pipeline time to start.
	time.Sleep(50 * time.Millisecond)

	// Send event with "secret" and "name" fields.
	det.events <- makeEvent("pgcdc:orders", map[string]any{"name": "alice", "secret": "s3cret"})

	ev := waitEvent(t, collector.received, 2*time.Second)
	var payload map[string]any
	if err := json.Unmarshal(ev.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if _, ok := payload["secret"]; ok {
		t.Error("expected 'secret' to be dropped before reload")
	}
	if payload["name"] != "alice" {
		t.Errorf("expected name=alice, got %v", payload["name"])
	}

	// Reload: swap transform to drop "name" instead of "secret".
	if err := p.Reload(ReloadConfig{
		Transforms: []transform.TransformFunc{transform.DropColumns("name")},
	}); err != nil {
		t.Fatal(err)
	}

	// Send another event.
	det.events <- makeEvent("pgcdc:orders", map[string]any{"name": "bob", "secret": "top-secret"})

	ev2 := waitEvent(t, collector.received, 2*time.Second)
	var payload2 map[string]any
	if err := json.Unmarshal(ev2.Payload, &payload2); err != nil {
		t.Fatal(err)
	}
	if payload2["secret"] != "top-secret" {
		t.Errorf("expected secret=top-secret after reload, got %v", payload2["secret"])
	}
	if _, ok := payload2["name"]; ok {
		t.Error("expected 'name' to be dropped after reload")
	}

	cancel()
}

func TestPipeline_Reload_SwapsRoutes(t *testing.T) {
	det := &testDetector{events: make(chan event.Event, 10)}
	collector := &collectAdapter{name: "test-adapter", received: make(chan event.Event, 10)}

	// Start with a route that only allows pgcdc:orders.
	p := NewPipeline(det,
		WithAdapter(collector),
		WithRoute("test-adapter", "pgcdc:orders"),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- p.Run(ctx) }()

	time.Sleep(50 * time.Millisecond)

	// Send an orders event — should pass.
	det.events <- makeEvent("pgcdc:orders", map[string]any{"id": 1})
	waitEvent(t, collector.received, 2*time.Second)

	// Send a users event — should be filtered.
	det.events <- makeEvent("pgcdc:users", map[string]any{"id": 2})
	assertNoEvent(t, collector.received, 200*time.Millisecond)

	// Reload: change route to allow pgcdc:users only.
	if err := p.Reload(ReloadConfig{
		Routes: map[string][]string{"test-adapter": {"pgcdc:users"}},
	}); err != nil {
		t.Fatal(err)
	}

	// Now orders should be filtered.
	det.events <- makeEvent("pgcdc:orders", map[string]any{"id": 3})
	assertNoEvent(t, collector.received, 200*time.Millisecond)

	// And users should pass.
	det.events <- makeEvent("pgcdc:users", map[string]any{"id": 4})
	ev := waitEvent(t, collector.received, 2*time.Second)
	if ev.Channel != "pgcdc:users" {
		t.Errorf("expected channel pgcdc:users, got %s", ev.Channel)
	}

	cancel()
}
