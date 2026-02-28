package inspect

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/event"
)

func makeEvent(channel string, seq int) event.Event {
	return event.Event{
		ID:        fmt.Sprintf("evt-%d", seq),
		Channel:   channel,
		Operation: "INSERT",
		Payload:   json.RawMessage(fmt.Sprintf(`{"seq":%d}`, seq)),
		Source:    "test",
		CreatedAt: time.Now().UTC(),
	}
}

func TestInspector_Record_Snapshot(t *testing.T) {
	insp := New(100)

	ev1 := makeEvent("orders", 1)
	ev2 := makeEvent("orders", 2)
	ev3 := makeEvent("users", 3)

	insp.Record(TapPostDetector, ev1)
	insp.Record(TapPostDetector, ev2)
	insp.Record(TapPostDetector, ev3)

	events := insp.Snapshot(TapPostDetector, 0)
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
	if events[0].ID != "evt-1" {
		t.Errorf("expected first event ID evt-1, got %s", events[0].ID)
	}
	if events[1].ID != "evt-2" {
		t.Errorf("expected second event ID evt-2, got %s", events[1].ID)
	}
	if events[2].ID != "evt-3" {
		t.Errorf("expected third event ID evt-3, got %s", events[2].ID)
	}
}

func TestInspector_RingBuffer_Overflow(t *testing.T) {
	insp := New(3)

	for i := 1; i <= 5; i++ {
		insp.Record(TapPostDetector, makeEvent("ch", i))
	}

	events := insp.Snapshot(TapPostDetector, 0)
	if len(events) != 3 {
		t.Fatalf("expected 3 events (buffer size), got %d", len(events))
	}
	// Should contain events 3, 4, 5 (oldest two evicted).
	if events[0].ID != "evt-3" {
		t.Errorf("expected oldest event evt-3, got %s", events[0].ID)
	}
	if events[1].ID != "evt-4" {
		t.Errorf("expected middle event evt-4, got %s", events[1].ID)
	}
	if events[2].ID != "evt-5" {
		t.Errorf("expected newest event evt-5, got %s", events[2].ID)
	}
}

func TestInspector_Snapshot_Limit(t *testing.T) {
	insp := New(100)

	for i := 1; i <= 10; i++ {
		insp.Record(TapPostTransform, makeEvent("ch", i))
	}

	events := insp.Snapshot(TapPostTransform, 3)
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
	// Should return the last 3 events.
	if events[0].ID != "evt-8" {
		t.Errorf("expected evt-8, got %s", events[0].ID)
	}
	if events[1].ID != "evt-9" {
		t.Errorf("expected evt-9, got %s", events[1].ID)
	}
	if events[2].ID != "evt-10" {
		t.Errorf("expected evt-10, got %s", events[2].ID)
	}

	// Limit larger than count returns all.
	all := insp.Snapshot(TapPostTransform, 50)
	if len(all) != 10 {
		t.Fatalf("expected 10 events, got %d", len(all))
	}
}

func TestInspector_Subscribe(t *testing.T) {
	insp := New(100)

	ch, cancel := insp.Subscribe(TapPreAdapter)
	defer cancel()

	ev := makeEvent("orders", 42)
	insp.Record(TapPreAdapter, ev)

	select {
	case received := <-ch:
		if received.ID != "evt-42" {
			t.Errorf("expected evt-42, got %s", received.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for subscribed event")
	}
}

func TestInspector_Subscribe_Cancel(t *testing.T) {
	insp := New(100)

	ch, cancel := insp.Subscribe(TapPostDetector)

	// Record one event that the subscriber receives.
	insp.Record(TapPostDetector, makeEvent("ch", 1))
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first event")
	}

	// Cancel the subscription.
	cancel()

	// Channel should be closed.
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected channel to be closed after cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for channel close")
	}

	// Recording after cancel should not panic.
	insp.Record(TapPostDetector, makeEvent("ch", 2))
}

func TestInspector_UnknownTapPoint(t *testing.T) {
	insp := New(100)
	unknown := TapPoint("nonexistent")

	// Record on unknown point should not panic.
	insp.Record(unknown, makeEvent("ch", 1))

	// Snapshot on unknown point returns nil.
	events := insp.Snapshot(unknown, 10)
	if events != nil {
		t.Errorf("expected nil for unknown tap point, got %v", events)
	}

	// Subscribe on unknown point returns closed channel.
	ch, cancel := insp.Subscribe(unknown)
	defer cancel()

	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected closed channel for unknown tap point")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out; channel should be immediately closed for unknown tap point")
	}
}

func TestInspector_ConcurrentAccess(t *testing.T) {
	insp := New(50)
	var wg sync.WaitGroup

	// Concurrent writers.
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(group int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				insp.Record(TapPostDetector, makeEvent("ch", group*100+i))
			}
		}(g)
	}

	// Concurrent readers.
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				insp.Snapshot(TapPostDetector, 10)
			}
		}()
	}

	wg.Wait()

	// Should have exactly 50 events (buffer size).
	events := insp.Snapshot(TapPostDetector, 0)
	if len(events) != 50 {
		t.Errorf("expected 50 events after overflow, got %d", len(events))
	}
}

func TestInspector_DefaultBufferSize(t *testing.T) {
	insp := New(0) // should default to 100

	for i := 0; i < 150; i++ {
		insp.Record(TapPostDetector, makeEvent("ch", i))
	}

	events := insp.Snapshot(TapPostDetector, 0)
	if len(events) != 100 {
		t.Errorf("expected default buffer size 100, got %d events", len(events))
	}
}

func TestInspector_TapPointIsolation(t *testing.T) {
	insp := New(100)

	insp.Record(TapPostDetector, makeEvent("det", 1))
	insp.Record(TapPostTransform, makeEvent("tx", 2))
	insp.Record(TapPreAdapter, makeEvent("adp", 3))

	det := insp.Snapshot(TapPostDetector, 0)
	tx := insp.Snapshot(TapPostTransform, 0)
	adp := insp.Snapshot(TapPreAdapter, 0)

	if len(det) != 1 || det[0].Channel != "det" {
		t.Errorf("post-detector: expected 1 event on 'det', got %d", len(det))
	}
	if len(tx) != 1 || tx[0].Channel != "tx" {
		t.Errorf("post-transform: expected 1 event on 'tx', got %d", len(tx))
	}
	if len(adp) != 1 || adp[0].Channel != "adp" {
		t.Errorf("pre-adapter: expected 1 event on 'adp', got %d", len(adp))
	}
}
