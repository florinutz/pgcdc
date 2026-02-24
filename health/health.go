package health

import (
	"encoding/json"
	"net/http"
	"sync"
)

// Status represents the health state of a component.
type Status string

const (
	StatusUp       Status = "up"
	StatusDown     Status = "down"
	StatusDegraded Status = "degraded"
)

// Checker tracks the health of registered components.
type Checker struct {
	mu         sync.RWMutex
	components map[string]Status
}

// NewChecker creates a Checker with no registered components.
func NewChecker() *Checker {
	return &Checker{
		components: make(map[string]Status),
	}
}

// Register adds a component with an initial status of down.
func (c *Checker) Register(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.components[name] = StatusDown
}

// SetStatus updates the health status of a named component.
func (c *Checker) SetStatus(name string, status Status) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.components[name] = status
}

type response struct {
	Status     Status            `json:"status"`
	Components map[string]Status `json:"components"`
}

// ServeHTTP responds with the aggregated health status.
// Returns 200 when all components are up, 503 when any is down.
func (c *Checker) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	c.mu.RLock()
	overall := StatusUp
	comps := make(map[string]Status, len(c.components))
	for name, status := range c.components {
		comps[name] = status
		switch status {
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
