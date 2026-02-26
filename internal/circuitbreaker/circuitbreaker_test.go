package circuitbreaker

import (
	"sync"
	"testing"
	"time"
)

func TestCircuitBreaker_ClosedState(t *testing.T) {
	cb := New(3, time.Second, nil)

	if s := cb.State(); s != StateClosed {
		t.Fatalf("expected closed, got %s", s)
	}
	if !cb.Allow() {
		t.Fatal("closed breaker should allow requests")
	}
}

func TestCircuitBreaker_TripsOnMaxFailures(t *testing.T) {
	cb := New(3, time.Second, nil)

	cb.RecordFailure()
	cb.RecordFailure()
	if s := cb.State(); s != StateClosed {
		t.Fatalf("expected closed after 2 failures, got %s", s)
	}

	cb.RecordFailure()
	if s := cb.State(); s != StateOpen {
		t.Fatalf("expected open after 3 failures, got %s", s)
	}
	if f := cb.Failures(); f != 3 {
		t.Fatalf("expected 3 failures, got %d", f)
	}
}

func TestCircuitBreaker_OpenRejectsRequests(t *testing.T) {
	cb := New(1, time.Hour, nil) // long timeout so it stays open
	cb.RecordFailure()

	if cb.Allow() {
		t.Fatal("open breaker should reject requests")
	}
}

func TestCircuitBreaker_HalfOpenOnTimeout(t *testing.T) {
	cb := New(1, 10*time.Millisecond, nil)
	cb.RecordFailure()

	if cb.Allow() {
		t.Fatal("should reject immediately after tripping")
	}

	time.Sleep(20 * time.Millisecond)

	if !cb.Allow() {
		t.Fatal("should allow after reset timeout (half-open)")
	}
	if s := cb.State(); s != StateHalfOpen {
		t.Fatalf("expected half_open, got %s", s)
	}
}

func TestCircuitBreaker_HalfOpenSuccess(t *testing.T) {
	cb := New(1, 10*time.Millisecond, nil)
	cb.RecordFailure()

	time.Sleep(20 * time.Millisecond)
	cb.Allow() // triggers half-open

	cb.RecordSuccess()
	if s := cb.State(); s != StateClosed {
		t.Fatalf("expected closed after half-open success, got %s", s)
	}
	if f := cb.Failures(); f != 0 {
		t.Fatalf("expected 0 failures after success, got %d", f)
	}
}

func TestCircuitBreaker_HalfOpenFailure(t *testing.T) {
	cb := New(1, 10*time.Millisecond, nil)
	cb.RecordFailure()

	time.Sleep(20 * time.Millisecond)
	cb.Allow() // triggers half-open

	cb.RecordFailure()
	if s := cb.State(); s != StateOpen {
		t.Fatalf("expected open after half-open failure, got %s", s)
	}
}

func TestCircuitBreaker_Reset(t *testing.T) {
	cb := New(1, time.Hour, nil)
	cb.RecordFailure()

	if s := cb.State(); s != StateOpen {
		t.Fatalf("expected open, got %s", s)
	}

	cb.Reset()
	if s := cb.State(); s != StateClosed {
		t.Fatalf("expected closed after reset, got %s", s)
	}
	if f := cb.Failures(); f != 0 {
		t.Fatalf("expected 0 failures after reset, got %d", f)
	}
	if !cb.Allow() {
		t.Fatal("should allow after reset")
	}
}

func TestCircuitBreaker_ConcurrentAccess(t *testing.T) {
	cb := New(100, 10*time.Millisecond, nil)

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cb.Allow()
			cb.RecordFailure()
			cb.Allow()
			cb.RecordSuccess()
			_ = cb.State()
			_ = cb.Failures()
		}()
	}
	wg.Wait()

	// No panics or data races means the test passes.
	// State should be valid.
	s := cb.State()
	if s != StateClosed && s != StateOpen && s != StateHalfOpen {
		t.Fatalf("unexpected state: %s", s)
	}
}
