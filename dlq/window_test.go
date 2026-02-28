package dlq

import "testing"

func TestNackWindow_Basic(t *testing.T) {
	w := NewNackWindow(10, 3)

	// Acks should not exceed threshold.
	for i := 0; i < 10; i++ {
		if exceeded := w.RecordAck(); exceeded {
			t.Fatalf("RecordAck should not exceed threshold after %d acks", i+1)
		}
	}

	if w.Exceeded() {
		t.Fatal("Exceeded should be false when only acks recorded")
	}

	// Reset and test nacks reaching threshold.
	w.Reset()

	if exceeded := w.RecordNack(); exceeded {
		t.Fatal("1 nack should not exceed threshold of 3")
	}
	if exceeded := w.RecordNack(); exceeded {
		t.Fatal("2 nacks should not exceed threshold of 3")
	}
	if exceeded := w.RecordNack(); !exceeded {
		t.Fatal("3 nacks should exceed threshold of 3")
	}

	if !w.Exceeded() {
		t.Fatal("Exceeded should be true after 3 nacks with threshold 3")
	}
}

func TestNackWindow_SlidingEviction(t *testing.T) {
	// Window of size 5, threshold 3.
	w := NewNackWindow(5, 3)

	// Fill with 3 nacks then 2 acks: [N, N, N, A, A] — exceeded.
	w.RecordNack()
	w.RecordNack()
	w.RecordNack()
	w.RecordAck()
	w.RecordAck()

	if !w.Exceeded() {
		t.Fatal("should be exceeded: 3 nacks in window of 5")
	}

	// Add 3 more acks. This slides out the 3 oldest entries (the nacks).
	// After first ack:  [N, N, A, A, A] — evicts first N → 2 nacks.
	// After second ack: [N, A, A, A, A] — evicts second N → 1 nack.
	// After third ack:  [A, A, A, A, A] — evicts third N → 0 nacks.
	w.RecordAck()
	w.RecordAck()
	exceeded := w.RecordAck()

	if exceeded {
		t.Fatal("should not be exceeded after sliding out all nacks")
	}

	total, nacks, threshold := w.Stats()
	if total != 5 {
		t.Fatalf("expected total=5, got %d", total)
	}
	if nacks != 0 {
		t.Fatalf("expected nacks=0, got %d", nacks)
	}
	if threshold != 3 {
		t.Fatalf("expected threshold=3, got %d", threshold)
	}
}

func TestNackWindow_Reset(t *testing.T) {
	w := NewNackWindow(10, 2)

	w.RecordNack()
	w.RecordNack()
	w.RecordNack()

	if !w.Exceeded() {
		t.Fatal("should be exceeded before reset")
	}

	w.Reset()

	if w.Exceeded() {
		t.Fatal("should not be exceeded after reset")
	}

	total, nacks, _ := w.Stats()
	if total != 0 || nacks != 0 {
		t.Fatalf("expected total=0 nacks=0 after reset, got total=%d nacks=%d", total, nacks)
	}
}

func TestNackWindow_Stats(t *testing.T) {
	w := NewNackWindow(10, 5)

	w.RecordAck()
	w.RecordNack()
	w.RecordAck()
	w.RecordNack()

	total, nacks, threshold := w.Stats()
	if total != 4 {
		t.Fatalf("expected total=4, got %d", total)
	}
	if nacks != 2 {
		t.Fatalf("expected nacks=2, got %d", nacks)
	}
	if threshold != 5 {
		t.Fatalf("expected threshold=5, got %d", threshold)
	}
}

func TestNackWindow_Defaults(t *testing.T) {
	// Zero/negative size and threshold should get defaults.
	w := NewNackWindow(0, 0)

	total, _, threshold := w.Stats()
	if total != 0 {
		t.Fatalf("expected total=0 on fresh window, got %d", total)
	}
	// Default size=100, threshold=50.
	if threshold != 50 {
		t.Fatalf("expected default threshold=50, got %d", threshold)
	}
}

func TestNackWindow_ExactThreshold(t *testing.T) {
	w := NewNackWindow(5, 2)

	// 1 nack, 1 ack — not exceeded.
	w.RecordNack()
	exceeded := w.RecordAck()
	if exceeded {
		t.Fatal("1 nack should not exceed threshold of 2")
	}

	// 2nd nack — exactly at threshold.
	exceeded = w.RecordNack()
	if !exceeded {
		t.Fatal("2 nacks should meet threshold of 2")
	}
}

func TestNackWindow_FullCycleWrap(t *testing.T) {
	// Small window to verify multiple full wraps.
	w := NewNackWindow(3, 2)

	// Fill: [N, A, N] → 2 nacks → exceeded.
	w.RecordNack()
	w.RecordAck()
	exceeded := w.RecordNack()
	if !exceeded {
		t.Fatal("2 nacks in window of 3 should exceed threshold 2")
	}

	// Slide: [A, N, A] → evict N, add A → 1 nack.
	exceeded = w.RecordAck()
	if exceeded {
		t.Fatal("should not be exceeded after evicting a nack")
	}

	// Slide: [N, A, A] → evict A, add A → 1 nack.
	exceeded = w.RecordAck()
	if exceeded {
		t.Fatal("should not be exceeded: only 1 nack in window")
	}

	// Slide: [A, A, N] → evict N, add N → 1 nack.
	exceeded = w.RecordNack()
	if exceeded {
		t.Fatal("should not be exceeded: only 1 nack after evicting a nack")
	}

	total, nacks, _ := w.Stats()
	if total != 3 {
		t.Fatalf("expected total=3, got %d", total)
	}
	if nacks != 1 {
		t.Fatalf("expected nacks=1, got %d", nacks)
	}
}
