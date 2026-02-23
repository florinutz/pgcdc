package backoff

import (
	"testing"
	"time"
)

func TestJitter_ExponentialGrowth(t *testing.T) {
	base := 1 * time.Second
	cap := 32 * time.Second

	for _, tc := range []struct {
		attempt int
		maxCap  time.Duration
	}{
		{0, 1 * time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 16 * time.Second},
		{5, 32 * time.Second},
		{6, 32 * time.Second},  // capped
		{10, 32 * time.Second}, // capped
	} {
		for range 1000 {
			d := Jitter(tc.attempt, base, cap)
			if d > tc.maxCap {
				t.Errorf("Jitter(%d) = %v, exceeds expected cap %v", tc.attempt, d, tc.maxCap)
			}
		}
	}
}

func TestJitter_CapEnforcement(t *testing.T) {
	base := 5 * time.Second
	cap := 60 * time.Second
	const minFloor = 100 * time.Millisecond

	// High attempt: should be capped.
	for range 1000 {
		d := Jitter(100, base, cap)
		if d < minFloor || d >= cap {
			t.Fatalf("attempt 100: got %v, want [%v, %v)", d, minFloor, cap)
		}
	}
}

func TestJitter_MinimumFloor(t *testing.T) {
	base := 5 * time.Second
	cap := 60 * time.Second
	const minFloor = 100 * time.Millisecond

	// Attempt 0: max = base * 2^0 = 5s, so delay must be in [100ms, 5s).
	for range 1000 {
		d := Jitter(0, base, cap)
		if d < minFloor {
			t.Fatalf("attempt 0: got %v, want >= %v", d, minFloor)
		}
		if d >= base {
			t.Fatalf("attempt 0: got %v, want < %v", d, base)
		}
	}
}
