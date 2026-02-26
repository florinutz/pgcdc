package view

import (
	"encoding/json"
	"testing"
	"time"
)

func TestSlidingWindow_OverlappingResults(t *testing.T) {
	def, err := Parse("test_sliding", "SELECT COUNT(*) as n FROM pgcdc_events SLIDING WINDOW 3m SLIDE 1m", EmitRow, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	sw := NewSlidingWindow(def, nil)

	// Add events to the current sub-window (slot 0).
	sw.Add(EventMeta{Channel: "pgcdc:orders", Operation: "INSERT"}, map[string]any{"id": 1})
	sw.Add(EventMeta{Channel: "pgcdc:orders", Operation: "INSERT"}, map[string]any{"id": 2})

	// First flush: aggregates across all sub-windows (only slot 0 has data).
	// Then evicts oldest and rotates.
	events := sw.Flush()
	if len(events) != 1 {
		t.Fatalf("first flush: expected 1 event, got %d", len(events))
	}

	var p map[string]any
	if err := json.Unmarshal(events[0].Payload, &p); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	n := p["n"].(float64)
	if int64(n) != 2 {
		t.Errorf("first flush n = %v, want 2", n)
	}

	// Add more events to the new current slot.
	sw.Add(EventMeta{Channel: "pgcdc:orders", Operation: "INSERT"}, map[string]any{"id": 3})

	// Second flush: should see events from the remaining old slot + new slot.
	// The first slot's 2 events are still in the window, plus the new 1 event = 3 total.
	events = sw.Flush()
	if len(events) != 1 {
		t.Fatalf("second flush: expected 1 event, got %d", len(events))
	}

	if err := json.Unmarshal(events[0].Payload, &p); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	n = p["n"].(float64)
	if int64(n) != 3 {
		t.Errorf("second flush n = %v, want 3", n)
	}
}

func TestSlidingWindow_SubWindowEviction(t *testing.T) {
	// Window size = 2m, slide = 1m -> 2 sub-windows.
	def, err := Parse("test_evict", "SELECT COUNT(*) as n FROM pgcdc_events SLIDING WINDOW 2m SLIDE 1m", EmitRow, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	sw := NewSlidingWindow(def, nil)

	// Add events to first slot.
	sw.Add(EventMeta{}, map[string]any{"id": 1})
	sw.Add(EventMeta{}, map[string]any{"id": 2})

	// First flush: both events visible, evicts oldest slot (slot 1), current moves to slot 1.
	events := sw.Flush()
	if len(events) != 1 {
		t.Fatalf("first flush: expected 1 event, got %d", len(events))
	}
	var p map[string]any
	_ = json.Unmarshal(events[0].Payload, &p)
	if int64(p["n"].(float64)) != 2 {
		t.Errorf("first flush n = %v, want 2", p["n"])
	}

	// Add event to second slot.
	sw.Add(EventMeta{}, map[string]any{"id": 3})

	// Second flush: slot 0 still has 2 events, slot 1 has 1 event = 3 total.
	// Then evicts slot 0, current moves to slot 0.
	events = sw.Flush()
	if len(events) != 1 {
		t.Fatalf("second flush: expected 1 event, got %d", len(events))
	}
	_ = json.Unmarshal(events[0].Payload, &p)
	if int64(p["n"].(float64)) != 3 {
		t.Errorf("second flush n = %v, want 3", p["n"])
	}

	// Third flush: slot 0 was evicted (now empty), slot 1 has 1 event.
	// Evicts slot 1, current moves to slot 1.
	events = sw.Flush()
	if len(events) != 1 {
		t.Fatalf("third flush: expected 1 event, got %d", len(events))
	}
	_ = json.Unmarshal(events[0].Payload, &p)
	if int64(p["n"].(float64)) != 1 {
		t.Errorf("third flush n = %v, want 1", p["n"])
	}

	// Fourth flush: all slots empty.
	events = sw.Flush()
	if len(events) != 0 {
		t.Errorf("fourth flush: expected 0 events, got %d", len(events))
	}
}

func TestSlidingWindow_GroupBy(t *testing.T) {
	def, err := Parse("test_slide_group",
		"SELECT COUNT(*) as cnt, payload.region FROM pgcdc_events GROUP BY payload.region SLIDING WINDOW 2m SLIDE 1m",
		EmitRow, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	sw := NewSlidingWindow(def, nil)

	sw.Add(EventMeta{}, map[string]any{"region": "us-east"})
	sw.Add(EventMeta{}, map[string]any{"region": "eu-west"})

	events := sw.Flush()
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}

	results := make(map[string]float64)
	for _, ev := range events {
		var p map[string]any
		_ = json.Unmarshal(ev.Payload, &p)
		region := p["region"].(string)
		results[region] = p["cnt"].(float64)
	}

	if results["us-east"] != 1 {
		t.Errorf("us-east cnt = %v, want 1", results["us-east"])
	}
	if results["eu-west"] != 1 {
		t.Errorf("eu-west cnt = %v, want 1", results["eu-west"])
	}
}

func TestSlidingWindow_EngineIntegration(t *testing.T) {
	def, err := Parse("slide_view", "SELECT COUNT(*) as n FROM pgcdc_events SLIDING WINDOW 2m SLIDE 1m", EmitRow, 0)
	if err != nil {
		t.Fatal(err)
	}

	if def.WindowType != WindowSliding {
		t.Fatalf("window type = %v, want WindowSliding", def.WindowType)
	}

	engine := NewEngine([]*ViewDef{def}, nil)

	engine.Process(makeTestEvent(t, "pgcdc:orders", "INSERT", map[string]any{"id": 1}))
	engine.Process(makeTestEvent(t, "pgcdc:orders", "INSERT", map[string]any{"id": 2}))

	results := engine.FlushAll()
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	var p map[string]any
	_ = json.Unmarshal(results[0].Payload, &p)
	if int64(p["n"].(float64)) != 2 {
		t.Errorf("n = %v, want 2", p["n"])
	}
}

func TestSlidingWindow_WindowInfo(t *testing.T) {
	def, err := Parse("test_info", "SELECT COUNT(*) as n FROM pgcdc_events SLIDING WINDOW 2m SLIDE 1m", EmitRow, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	sw := NewSlidingWindow(def, nil)
	sw.Add(EventMeta{}, map[string]any{})

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
