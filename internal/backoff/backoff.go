package backoff

import (
	"math"
	"math/rand/v2"
	"time"
)

// Jitter returns an exponential delay with full jitter.
//
//	delay = max(minDelay, rand(0, min(cap, base * 2^attempt)))
func Jitter(attempt int, base, cap time.Duration) time.Duration {
	const minDelay = 100 * time.Millisecond
	exp := float64(base) * math.Pow(2, float64(attempt))
	if exp > float64(cap) || exp <= 0 { // overflow guard
		exp = float64(cap)
	}
	jitter := time.Duration(rand.Int64N(int64(exp)))
	if jitter < minDelay {
		jitter = minDelay
	}
	return jitter
}
