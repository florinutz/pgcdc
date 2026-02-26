package circuitbreaker

import (
	"log/slog"
	"sync"
	"time"
)

// State represents the current state of the circuit breaker.
type State string

const (
	StateClosed   State = "closed"
	StateOpen     State = "open"
	StateHalfOpen State = "half_open"
)

// CircuitBreaker implements a three-state circuit breaker pattern.
type CircuitBreaker struct {
	mu           sync.Mutex
	state        State
	failures     int
	maxFailures  int
	resetTimeout time.Duration
	lastFailure  time.Time
	logger       *slog.Logger
}

// New creates a new CircuitBreaker with the given threshold and reset timeout.
func New(maxFailures int, resetTimeout time.Duration, logger *slog.Logger) *CircuitBreaker {
	if logger == nil {
		logger = slog.Default()
	}
	return &CircuitBreaker{
		state:        StateClosed,
		maxFailures:  maxFailures,
		resetTimeout: resetTimeout,
		logger:       logger,
	}
}

// Allow returns true if the request should proceed.
// In Closed state, always allows. In Open state, rejects unless resetTimeout
// has elapsed (transitions to HalfOpen). In HalfOpen state, allows (trial request).
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		return true
	case StateOpen:
		if time.Since(cb.lastFailure) >= cb.resetTimeout {
			cb.state = StateHalfOpen
			cb.logger.Info("circuit breaker half-open", "previous_failures", cb.failures)
			return true
		}
		return false
	case StateHalfOpen:
		return true
	default:
		return true
	}
}

// RecordSuccess resets the failure count and transitions to Closed state.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures = 0
	if cb.state == StateHalfOpen {
		cb.logger.Info("circuit breaker closed after successful trial")
	}
	cb.state = StateClosed
}

// RecordFailure increments the failure counter. When failures reach maxFailures,
// the circuit breaker transitions to Open state.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures++
	cb.lastFailure = time.Now()

	if cb.state == StateHalfOpen {
		cb.state = StateOpen
		cb.logger.Warn("circuit breaker re-opened after half-open failure", "failures", cb.failures)
		return
	}

	if cb.failures >= cb.maxFailures {
		cb.state = StateOpen
		cb.logger.Warn("circuit breaker opened", "failures", cb.failures, "max_failures", cb.maxFailures)
	}
}

// State returns the current state of the circuit breaker.
func (cb *CircuitBreaker) State() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

// Failures returns the current failure count.
func (cb *CircuitBreaker) Failures() int {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.failures
}

// Reset forces the circuit breaker back to Closed state with zero failures.
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.state = StateClosed
	cb.failures = 0
	cb.logger.Info("circuit breaker reset")
}
