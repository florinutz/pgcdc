package ratelimit

import (
	"context"
	"testing"
	"time"
)

func TestLimiter_NoOpMode(t *testing.T) {
	l := New(0, 1, "test", nil)
	ctx := context.Background()

	start := time.Now()
	for i := 0; i < 100; i++ {
		if err := l.Wait(ctx); err != nil {
			t.Fatalf("no-op limiter returned error: %v", err)
		}
	}
	elapsed := time.Since(start)
	if elapsed > 50*time.Millisecond {
		t.Fatalf("no-op limiter took too long: %v", elapsed)
	}

	// Negative rate is also no-op.
	l2 := New(-1, 1, "test", nil)
	if err := l2.Wait(ctx); err != nil {
		t.Fatalf("negative-rate limiter returned error: %v", err)
	}
}

func TestLimiter_RespectRate(t *testing.T) {
	// 10 events/sec with burst of 1 means ~100ms between events after the first.
	l := New(10, 1, "test", nil)
	ctx := context.Background()

	start := time.Now()
	for i := 0; i < 5; i++ {
		if err := l.Wait(ctx); err != nil {
			t.Fatalf("wait returned error: %v", err)
		}
	}
	elapsed := time.Since(start)

	// 5 events at 10/sec = first is free (burst), remaining 4 need ~400ms.
	// Allow some tolerance.
	if elapsed < 300*time.Millisecond {
		t.Fatalf("rate limiter too fast: %v for 5 events at 10/sec", elapsed)
	}
	if elapsed > 800*time.Millisecond {
		t.Fatalf("rate limiter too slow: %v for 5 events at 10/sec", elapsed)
	}
}

func TestLimiter_CancelledContext(t *testing.T) {
	l := New(1, 1, "test", nil)

	// Exhaust the burst token.
	ctx := context.Background()
	if err := l.Wait(ctx); err != nil {
		t.Fatalf("first wait should succeed: %v", err)
	}

	// Cancel immediately — the second call should fail.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := l.Wait(ctx)
	if err == nil {
		t.Fatal("expected error on cancelled context")
	}
}
