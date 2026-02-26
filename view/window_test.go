package view

import (
	"encoding/json"
	"testing"
	"time"
)

func testViewDef(t *testing.T, query string, emit EmitMode) *ViewDef {
	t.Helper()
	def, err := Parse("test_view", query, emit, 0)
	if err != nil {
		t.Fatalf("parse view: %v", err)
	}
	return def
}

func TestTumblingWindow_BasicCount(t *testing.T) {
	def := testViewDef(t, "SELECT COUNT(*) as n FROM pgcdc_events TUMBLING WINDOW 1m", EmitRow)
	w := NewTumblingWindow(def, nil)

	w.Add(EventMeta{Channel: "pgcdc:orders", Operation: "INSERT"}, map[string]any{"id": 1})
	w.Add(EventMeta{Channel: "pgcdc:orders", Operation: "INSERT"}, map[string]any{"id": 2})
	w.Add(EventMeta{Channel: "pgcdc:orders", Operation: "INSERT"}, map[string]any{"id": 3})

	events := w.Flush()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	var payload map[string]any
	if err := json.Unmarshal(events[0].Payload, &payload); err != nil {
		t.Fatal(err)
	}

	n, ok := payload["n"].(float64)
	if !ok {
		t.Fatalf("n is %T, expected float64 (from JSON unmarshal of int64)", payload["n"])
	}
	if int64(n) != 3 {
		t.Errorf("n = %v, want 3", n)
	}

	if events[0].Channel != "pgcdc:_view:test_view" {
		t.Errorf("channel = %q, want pgcdc:_view:test_view", events[0].Channel)
	}
	if events[0].Operation != "VIEW_RESULT" {
		t.Errorf("operation = %q, want VIEW_RESULT", events[0].Operation)
	}
}

func TestTumblingWindow_GroupBy(t *testing.T) {
	def := testViewDef(t,
		"SELECT COUNT(*) as cnt, SUM(payload.amount) as total, payload.region FROM pgcdc_events GROUP BY payload.region TUMBLING WINDOW 1m",
		EmitRow)
	w := NewTumblingWindow(def, nil)

	w.Add(EventMeta{}, map[string]any{"region": "us-east", "amount": float64(100)})
	w.Add(EventMeta{}, map[string]any{"region": "us-east", "amount": float64(200)})
	w.Add(EventMeta{}, map[string]any{"region": "eu-west", "amount": float64(50)})

	events := w.Flush()
	if len(events) != 2 {
		t.Fatalf("expected 2 events (one per group), got %d", len(events))
	}

	results := make(map[string]map[string]any)
	for _, ev := range events {
		var p map[string]any
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		region := p["region"].(string)
		results[region] = p
	}

	usEast := results["us-east"]
	if int64(usEast["cnt"].(float64)) != 2 {
		t.Errorf("us-east cnt = %v, want 2", usEast["cnt"])
	}
	if usEast["total"].(float64) != 300 {
		t.Errorf("us-east total = %v, want 300", usEast["total"])
	}

	euWest := results["eu-west"]
	if int64(euWest["cnt"].(float64)) != 1 {
		t.Errorf("eu-west cnt = %v, want 1", euWest["cnt"])
	}
	if euWest["total"].(float64) != 50 {
		t.Errorf("eu-west total = %v, want 50", euWest["total"])
	}
}

func TestTumblingWindow_EmptyWindow(t *testing.T) {
	def := testViewDef(t, "SELECT COUNT(*) as n FROM pgcdc_events TUMBLING WINDOW 1m", EmitRow)
	w := NewTumblingWindow(def, nil)

	events := w.Flush()
	if len(events) != 0 {
		t.Errorf("expected 0 events for empty window, got %d", len(events))
	}
}

func TestTumblingWindow_Having(t *testing.T) {
	def := testViewDef(t,
		"SELECT COUNT(*) as cnt, payload.region FROM pgcdc_events GROUP BY payload.region HAVING COUNT(*) > 2 TUMBLING WINDOW 1m",
		EmitRow)
	w := NewTumblingWindow(def, nil)

	// us-east: 3 events (passes HAVING > 2)
	w.Add(EventMeta{}, map[string]any{"region": "us-east"})
	w.Add(EventMeta{}, map[string]any{"region": "us-east"})
	w.Add(EventMeta{}, map[string]any{"region": "us-east"})

	// eu-west: 1 event (filtered by HAVING)
	w.Add(EventMeta{}, map[string]any{"region": "eu-west"})

	events := w.Flush()
	if len(events) != 1 {
		t.Fatalf("expected 1 event (HAVING filters eu-west), got %d", len(events))
	}

	var p map[string]any
	if err := json.Unmarshal(events[0].Payload, &p); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if p["region"] != "us-east" {
		t.Errorf("remaining group region = %v, want us-east", p["region"])
	}
	if int64(p["cnt"].(float64)) != 3 {
		t.Errorf("cnt = %v, want 3", p["cnt"])
	}
}

func TestTumblingWindow_BatchEmit(t *testing.T) {
	def := testViewDef(t,
		"SELECT COUNT(*) as cnt, payload.region FROM pgcdc_events GROUP BY payload.region TUMBLING WINDOW 1m",
		EmitBatch)
	w := NewTumblingWindow(def, nil)

	w.Add(EventMeta{}, map[string]any{"region": "us-east"})
	w.Add(EventMeta{}, map[string]any{"region": "eu-west"})

	events := w.Flush()
	if len(events) != 1 {
		t.Fatalf("expected 1 batch event, got %d", len(events))
	}

	var p map[string]any
	if err := json.Unmarshal(events[0].Payload, &p); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	rows, ok := p["rows"].([]any)
	if !ok {
		t.Fatalf("rows is %T, expected []any", p["rows"])
	}
	if len(rows) != 2 {
		t.Errorf("rows count = %d, want 2", len(rows))
	}

	if p["_window"] == nil {
		t.Error("missing _window in batch payload")
	}
}

func TestTumblingWindow_MaxGroups(t *testing.T) {
	def, err := Parse("test", "SELECT COUNT(*) as cnt, payload.id FROM pgcdc_events GROUP BY payload.id TUMBLING WINDOW 1m", EmitRow, 3)
	if err != nil {
		t.Fatal(err)
	}

	w := NewTumblingWindow(def, nil)

	// Add 5 unique groups — only 3 should be stored.
	for i := 0; i < 5; i++ {
		w.Add(EventMeta{}, map[string]any{"id": float64(i)})
	}

	events := w.Flush()
	if len(events) != 3 {
		t.Errorf("expected 3 events (max_groups=3), got %d", len(events))
	}
}

func TestTumblingWindow_WindowPayloadHasTimestamps(t *testing.T) {
	def := testViewDef(t, "SELECT COUNT(*) as n FROM pgcdc_events TUMBLING WINDOW 1m", EmitRow)
	w := NewTumblingWindow(def, nil)

	w.Add(EventMeta{}, map[string]any{})

	events := w.Flush()
	if len(events) != 1 {
		t.Fatal("expected 1 event")
	}

	var p map[string]any
	if err := json.Unmarshal(events[0].Payload, &p); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	win, ok := p["_window"].(map[string]any)
	if !ok {
		t.Fatal("missing _window")
	}

	startStr, _ := win["start"].(string)
	endStr, _ := win["end"].(string)

	start, err := time.Parse(time.RFC3339, startStr)
	if err != nil {
		t.Fatalf("parse start: %v", err)
	}
	end, err := time.Parse(time.RFC3339, endStr)
	if err != nil {
		t.Fatalf("parse end: %v", err)
	}

	if end.Before(start) {
		t.Errorf("end (%v) should not be before start (%v)", end, start)
	}
}

func TestTumblingWindow_FlushResets(t *testing.T) {
	def := testViewDef(t, "SELECT COUNT(*) as n FROM pgcdc_events TUMBLING WINDOW 1m", EmitRow)
	w := NewTumblingWindow(def, nil)

	w.Add(EventMeta{}, map[string]any{})
	w.Add(EventMeta{}, map[string]any{})

	events := w.Flush()
	if len(events) != 1 {
		t.Fatal("expected 1 event for first flush")
	}

	var p map[string]any
	if err := json.Unmarshal(events[0].Payload, &p); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if int64(p["n"].(float64)) != 2 {
		t.Errorf("first flush n = %v, want 2", p["n"])
	}

	// Second flush should be empty.
	events = w.Flush()
	if len(events) != 0 {
		t.Errorf("expected 0 events for second flush, got %d", len(events))
	}
}

func TestTumblingWindow_LateEvent(t *testing.T) {
	def, err := Parse("late_test",
		"SELECT COUNT(*) as n, payload.region FROM pgcdc_events GROUP BY payload.region ALLOWED LATENESS 5s TUMBLING WINDOW 1m",
		EmitRow, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	w := NewTumblingWindow(def, nil)

	// Add events and flush the first window.
	w.Add(EventMeta{}, map[string]any{"region": "us-east"})
	w.Add(EventMeta{}, map[string]any{"region": "us-east"})

	events := w.Flush()
	if len(events) != 1 {
		t.Fatalf("first flush: expected 1 event, got %d", len(events))
	}

	var p map[string]any
	_ = json.Unmarshal(events[0].Payload, &p)
	if int64(p["n"].(float64)) != 2 {
		t.Errorf("first flush n = %v, want 2", p["n"])
	}

	// Now add a late event for the same group key.
	w.Add(EventMeta{}, map[string]any{"region": "us-east"})

	// Flush again: the late event should cause a re-emit of the corrected window.
	events = w.Flush()

	// Look for the corrected result.
	var foundCorrected bool
	for _, ev := range events {
		_ = json.Unmarshal(ev.Payload, &p)
		if p["region"] == "us-east" && int64(p["n"].(float64)) == 3 {
			foundCorrected = true
		}
	}
	if !foundCorrected {
		t.Error("expected corrected re-emit with n=3 for us-east after late event")
	}
}

func TestTumblingWindow_LateEventExpiry(t *testing.T) {
	def, err := Parse("expiry_test",
		"SELECT COUNT(*) as n, payload.region FROM pgcdc_events GROUP BY payload.region ALLOWED LATENESS 50ms TUMBLING WINDOW 1m",
		EmitRow, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	w := NewTumblingWindow(def, nil)

	// Add events and flush.
	w.Add(EventMeta{}, map[string]any{"region": "us-east"})
	w.Flush()

	// Wait for the allowed lateness to expire.
	time.Sleep(60 * time.Millisecond)

	// Add a late event — it should NOT be added to the expired closed window.
	w.Add(EventMeta{}, map[string]any{"region": "us-east"})

	// Flush: the late event should be in the current window (not re-emitted from closed).
	events := w.Flush()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	var p map[string]any
	_ = json.Unmarshal(events[0].Payload, &p)
	// Should be count=1 (current window only, not the expired closed window).
	if int64(p["n"].(float64)) != 1 {
		t.Errorf("n = %v, want 1 (closed window should have expired)", p["n"])
	}
}
