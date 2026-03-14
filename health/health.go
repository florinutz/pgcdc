package health

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// Status represents the health state of a component.
type Status string

const (
	StatusUp       Status = "up"
	StatusDown     Status = "down"
	StatusDegraded Status = "degraded"
)

// componentInfo holds the internal health state of a single component.
type componentInfo struct {
	Status    Status
	LastError string
	LastSeen  time.Time
}

// Checker tracks the health of registered components.
type Checker struct {
	mu         sync.RWMutex
	components map[string]componentInfo
}

// NewChecker creates a Checker with no registered components.
func NewChecker() *Checker {
	return &Checker{
		components: make(map[string]componentInfo),
	}
}

// Register adds a component with an initial status of down.
func (c *Checker) Register(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.components[name] = componentInfo{Status: StatusDown}
}

// SetStatus updates the health status of a named component.
// LastSeen is set to the current time. LastError is cleared when status is Up.
func (c *Checker) SetStatus(name string, status Status) {
	c.mu.Lock()
	defer c.mu.Unlock()
	info := c.components[name]
	info.Status = status
	info.LastSeen = time.Now().UTC()
	if status == StatusUp {
		info.LastError = ""
	}
	c.components[name] = info
}

// SetStatusDetail updates the health status, last error, and last-seen time of a named component.
func (c *Checker) SetStatusDetail(name string, status Status, lastErr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.components[name] = componentInfo{
		Status:    status,
		LastError: lastErr,
		LastSeen:  time.Now().UTC(),
	}
}

// componentDetail is the per-component JSON representation in health responses.
type componentDetail struct {
	Status    Status    `json:"status"`
	LastError string    `json:"last_error,omitempty"`
	LastSeen  time.Time `json:"last_seen,omitempty"`
}

type response struct {
	Status     Status                     `json:"status"`
	Components map[string]componentDetail `json:"components"`
}

// ServeHTTP responds with the aggregated health status.
// Returns 200 when all components are up, 503 when any is down.
func (c *Checker) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	c.mu.RLock()
	overall := StatusUp
	comps := make(map[string]componentDetail, len(c.components))
	for name, info := range c.components {
		comps[name] = componentDetail(info)
		switch info.Status {
		case StatusDown:
			overall = StatusDown
		case StatusDegraded:
			if overall == StatusUp {
				overall = StatusDegraded
			}
		}
	}
	c.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	if overall == StatusDown {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(response{
		Status:     overall,
		Components: comps,
	})
}

// ReadinessChecker tracks whether the system is ready to serve traffic.
// Separate from liveness (Checker) for Kubernetes readiness probes.
type ReadinessChecker struct {
	mu    sync.RWMutex
	ready bool
}

// NewReadinessChecker creates a ReadinessChecker in not-ready state.
func NewReadinessChecker() *ReadinessChecker {
	return &ReadinessChecker{}
}

// SetReady updates the readiness state.
func (r *ReadinessChecker) SetReady(ready bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ready = ready
}

// ServeHTTP responds with readiness status.
// Returns 200 when ready, 503 when not ready.
func (r *ReadinessChecker) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	r.mu.RLock()
	ready := r.ready
	r.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	if !ready {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(map[string]bool{"ready": ready})
}
