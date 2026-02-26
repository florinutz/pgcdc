package view

import (
	"encoding/json"
	"testing"
	"time"
)

func TestSessionWindow_GapExpiry(t *testing.T) {
	def, err := Parse("test_session", "SELECT COUNT(*) as n FROM pgcdc_events SESSION WINDOW 50ms", EmitRow, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	sw := NewSessionWindow(def, nil)

	// Add events.
	sw.Add(EventMeta{Channel: "pgcdc:orders", Operation: "INSERT"}, map[string]any{"id": 1})
	sw.Add(EventMeta{Channel: "pgcdc:orders", Operation: "INSERT"}, map[string]any{"id": 2})

	// Flush immediately: session should still be active (within gap).
	events := sw.Flush()
	if len(events) != 0 {
		t.Fatalf("expected 0 events (session still active), got %d", len(events))
	}

	// Wait for the session gap to expire.
	time.Sleep(60 * time.Millisecond)

	// Flush again: session should now be expired.
	events = sw.Flush()
	if len(events) != 1 {
		t.Fatalf("expected 1 event (session expired), got %d", len(events))
	}

	var p map[string]any
	if err := json.Unmarshal(events[0].Payload, &p); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if int64(p["n"].(float64)) != 2 {
		t.Errorf("n = %v, want 2", p["n"])
	}
}

func TestSessionWindow_ActiveSession(t *testing.T) {
	def, err := Parse("test_active", "SELECT COUNT(*) as n FROM pgcdc_events SESSION WINDOW 100ms", EmitRow, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	sw := NewSessionWindow(def, nil)

	// Add event.
	sw.Add(EventMeta{}, map[string]any{"id": 1})

	// Wait less than gap.
	time.Sleep(30 * time.Millisecond)

	// Add another event (keeps session alive).
	sw.Add(EventMeta{}, map[string]any{"id": 2})

	// Wait less than gap.
	time.Sleep(30 * time.Millisecond)

	// Flush: session should still be active.
	events := sw.Flush()
	if len(events) != 0 {
		t.Fatalf("expected 0 events (session still active), got %d", len(events))
	}

	// Wait for gap to expire.
	time.Sleep(110 * time.Millisecond)

	events = sw.Flush()
	if len(events) != 1 {
		t.Fatalf("expected 1 event (session expired), got %d", len(events))
	}

	var p map[string]any
	_ = json.Unmarshal(events[0].Payload, &p)
	if int64(p["n"].(float64)) != 2 {
		t.Errorf("n = %v, want 2 (both events in same session)", p["n"])
	}
}

func TestSessionWindow_MultipleGroups(t *testing.T) {
	def, err := Parse("test_multi_sess",
		"SELECT COUNT(*) as cnt, payload.region FROM pgcdc_events GROUP BY payload.region SESSION WINDOW 50ms",
		EmitRow, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	sw := NewSessionWindow(def, nil)

	sw.Add(EventMeta{}, map[string]any{"region": "us-east"})
	sw.Add(EventMeta{}, map[string]any{"region": "us-east"})
	sw.Add(EventMeta{}, map[string]any{"region": "eu-west"})

	// Wait for gap to expire.
	time.Sleep(60 * time.Millisecond)

	events := sw.Flush()
	if len(events) != 2 {
		t.Fatalf("expected 2 events (2 groups), got %d", len(events))
	}

	results := make(map[string]float64)
	for _, ev := range events {
		var p map[string]any
		_ = json.Unmarshal(ev.Payload, &p)
		region := p["region"].(string)
		results[region] = p["cnt"].(float64)
	}

	if results["us-east"] != 2 {
		t.Errorf("us-east cnt = %v, want 2", results["us-east"])
	}
	if results["eu-west"] != 1 {
		t.Errorf("eu-west cnt = %v, want 1", results["eu-west"])
	}
}

func TestSessionWindow_WindowInfo(t *testing.T) {
	def, err := Parse("test_sess_info", "SELECT COUNT(*) as n FROM pgcdc_events SESSION WINDOW 50ms", EmitRow, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	sw := NewSessionWindow(def, nil)
	sw.Add(EventMeta{}, map[string]any{})

	time.Sleep(60 * time.Millisecond)

	events := sw.Flush()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	var p map[string]any
	_ = json.Unmarshal(events[0].Payload, &p)

	win, ok := p["_window"].(map[string]any)
	if !ok {
		t.Fatal("missing _window")
	}

	startStr, _ := win["start"].(string)
	endStr, _ := win["end"].(string)
	_, err = time.Parse(time.RFC3339, startStr)
	if err != nil {
		t.Errorf("invalid start time: %v", err)
	}
	_, err = time.Parse(time.RFC3339, endStr)
	if err != nil {
		t.Errorf("invalid end time: %v", err)
	}
}

func TestSessionWindow_EngineIntegration(t *testing.T) {
	def, err := Parse("sess_view", "SELECT COUNT(*) as n FROM pgcdc_events SESSION WINDOW 50ms", EmitRow, 0)
	if err != nil {
		t.Fatal(err)
	}

	if def.WindowType != WindowSession {
		t.Fatalf("window type = %v, want WindowSession", def.WindowType)
	}

	engine := NewEngine([]*ViewDef{def}, nil)

	engine.Process(makeTestEvent(t, "pgcdc:orders", "INSERT", map[string]any{"id": 1}))
	engine.Process(makeTestEvent(t, "pgcdc:orders", "INSERT", map[string]any{"id": 2}))

	// Immediate flush: sessions still active.
	results := engine.FlushAll()
	if len(results) != 0 {
		t.Fatalf("expected 0 results (sessions active), got %d", len(results))
	}

	// Wait for gap expiry.
	time.Sleep(60 * time.Millisecond)

	results = engine.FlushAll()
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	var p map[string]any
	_ = json.Unmarshal(results[0].Payload, &p)
	if int64(p["n"].(float64)) != 2 {
		t.Errorf("n = %v, want 2", p["n"])
	}
}
