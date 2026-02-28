package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/florinutz/pgcdc"
	"github.com/florinutz/pgcdc/health"
)

// PipelineStatus represents the current state of a pipeline.
type PipelineStatus string

const (
	StatusStopped  PipelineStatus = "stopped"
	StatusStarting PipelineStatus = "starting"
	StatusRunning  PipelineStatus = "running"
	StatusPaused   PipelineStatus = "paused"
	StatusError    PipelineStatus = "error"
)

// PipelineConfig defines a pipeline in YAML/JSON configuration.
type PipelineConfig struct {
	Name       string              `json:"name" yaml:"name"`
	Detector   DetectorSpec        `json:"detector" yaml:"detector"`
	Adapters   []AdapterSpec       `json:"adapters" yaml:"adapters"`
	Transforms []TransformSpec     `json:"transforms,omitempty" yaml:"transforms,omitempty"`
	Routes     map[string][]string `json:"routes,omitempty" yaml:"routes,omitempty"`
	Bus        BusSpec             `json:"bus,omitempty" yaml:"bus,omitempty"`
}

// DetectorSpec describes the detector for a pipeline.
type DetectorSpec struct {
	Type   string         `json:"type" yaml:"type"`
	Config map[string]any `json:"config,omitempty" yaml:"config,omitempty"`
}

// AdapterSpec describes an adapter for a pipeline.
type AdapterSpec struct {
	Name   string         `json:"name" yaml:"name"`
	Type   string         `json:"type" yaml:"type"`
	Config map[string]any `json:"config,omitempty" yaml:"config,omitempty"`
}

// TransformSpec describes a transform for a pipeline.
type TransformSpec struct {
	Type   string         `json:"type" yaml:"type"`
	Config map[string]any `json:"config,omitempty" yaml:"config,omitempty"`
}

// BusSpec describes bus configuration for a pipeline.
type BusSpec struct {
	BufferSize int    `json:"buffer_size,omitempty" yaml:"buffer_size,omitempty"`
	Mode       string `json:"mode,omitempty" yaml:"mode,omitempty"`
}

// PipelineInfo provides runtime status for a managed pipeline.
type PipelineInfo struct {
	Name      string         `json:"name"`
	Status    PipelineStatus `json:"status"`
	StartedAt *time.Time     `json:"started_at,omitempty"`
	Error     string         `json:"error,omitempty"`
	Config    PipelineConfig `json:"config"`
}

// ManagedPipeline wraps a Pipeline with lifecycle state.
type ManagedPipeline struct {
	config    PipelineConfig
	pipeline  *pgcdc.Pipeline
	status    PipelineStatus
	startedAt *time.Time
	lastErr   string
	cancel    context.CancelFunc
	done      chan struct{}
}

// Manager manages multiple pipelines running in a single process.
type Manager struct {
	mu        sync.RWMutex
	pipelines map[string]*ManagedPipeline
	health    *health.Checker
	logger    *slog.Logger
	builder   PipelineBuilder
}

// PipelineBuilder constructs a Pipeline from a PipelineConfig.
// This abstraction lets cmd/serve.go wire registry-based construction
// without the server package importing the full CLI wiring.
type PipelineBuilder func(cfg PipelineConfig, logger *slog.Logger) (*pgcdc.Pipeline, error)

// NewManager creates a new pipeline manager.
func NewManager(builder PipelineBuilder, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{
		pipelines: make(map[string]*ManagedPipeline),
		health:    health.NewChecker(),
		logger:    logger,
		builder:   builder,
	}
}

// Health returns the manager's health checker.
func (m *Manager) Health() *health.Checker {
	return m.health
}

// Add registers a pipeline configuration. Does not start it.
func (m *Manager) Add(cfg PipelineConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if cfg.Name == "" {
		return fmt.Errorf("pipeline name is required")
	}
	if _, exists := m.pipelines[cfg.Name]; exists {
		return fmt.Errorf("pipeline %q already exists", cfg.Name)
	}

	m.pipelines[cfg.Name] = &ManagedPipeline{
		config: cfg,
		status: StatusStopped,
	}
	m.health.Register("pipeline:" + cfg.Name)
	m.logger.Info("pipeline added", "pipeline", cfg.Name)
	return nil
}

// Remove removes a stopped pipeline. Returns error if running.
func (m *Manager) Remove(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	mp, ok := m.pipelines[name]
	if !ok {
		return fmt.Errorf("pipeline %q not found", name)
	}
	if mp.status == StatusRunning || mp.status == StatusStarting || mp.status == StatusPaused {
		return fmt.Errorf("pipeline %q is %s; stop it first", name, mp.status)
	}

	delete(m.pipelines, name)
	m.logger.Info("pipeline removed", "pipeline", name)
	return nil
}

// Start builds and starts a pipeline.
func (m *Manager) Start(ctx context.Context, name string) error {
	m.mu.Lock()
	mp, ok := m.pipelines[name]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("pipeline %q not found", name)
	}
	if mp.status == StatusRunning || mp.status == StatusStarting {
		m.mu.Unlock()
		return fmt.Errorf("pipeline %q is already %s", name, mp.status)
	}

	mp.status = StatusStarting
	mp.lastErr = ""
	m.mu.Unlock()

	p, err := m.builder(mp.config, m.logger.With("pipeline", name))
	if err != nil {
		m.mu.Lock()
		mp.status = StatusError
		mp.lastErr = err.Error()
		m.mu.Unlock()
		return fmt.Errorf("build pipeline %q: %w", name, err)
	}

	pipeCtx, cancel := context.WithCancel(ctx)

	m.mu.Lock()
	mp.pipeline = p
	mp.cancel = cancel
	mp.done = make(chan struct{})
	now := time.Now().UTC()
	mp.startedAt = &now
	mp.status = StatusRunning
	m.health.SetStatus("pipeline:"+name, health.StatusUp)
	m.mu.Unlock()

	m.logger.Info("pipeline started", "pipeline", name)

	// Run in background goroutine.
	go func() {
		defer close(mp.done)
		runErr := p.Run(pipeCtx)

		m.mu.Lock()
		defer m.mu.Unlock()

		if runErr != nil {
			mp.status = StatusError
			mp.lastErr = runErr.Error()
			m.health.SetStatus("pipeline:"+name, health.StatusDown)
			m.logger.Error("pipeline stopped with error", "pipeline", name, "error", runErr)
		} else {
			mp.status = StatusStopped
			mp.lastErr = ""
			m.health.SetStatus("pipeline:"+name, health.StatusDown)
			m.logger.Info("pipeline stopped", "pipeline", name)
		}
		mp.cancel = nil
		mp.startedAt = nil
	}()

	return nil
}

// Stop gracefully stops a running or paused pipeline.
func (m *Manager) Stop(name string) error {
	m.mu.RLock()
	mp, ok := m.pipelines[name]
	if !ok {
		m.mu.RUnlock()
		return fmt.Errorf("pipeline %q not found", name)
	}
	if mp.status != StatusRunning && mp.status != StatusPaused {
		m.mu.RUnlock()
		return fmt.Errorf("pipeline %q is not running (status: %s)", name, mp.status)
	}
	wasPaused := mp.status == StatusPaused
	cancel := mp.cancel
	done := mp.done
	m.mu.RUnlock()

	if cancel != nil {
		cancel()
	}

	// Wait for pipeline goroutine to finish.
	if done != nil {
		<-done
	}

	// A paused pipeline has no running goroutine to update the status,
	// so transition to stopped explicitly.
	if wasPaused {
		m.mu.Lock()
		mp.status = StatusStopped
		mp.lastErr = ""
		m.health.SetStatus("pipeline:"+name, health.StatusDown)
		m.mu.Unlock()
		m.logger.Info("pipeline stopped", "pipeline", name)
	}
	return nil
}

// List returns info for all pipelines.
func (m *Manager) List() []PipelineInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]PipelineInfo, 0, len(m.pipelines))
	for _, mp := range m.pipelines {
		result = append(result, mp.info())
	}
	return result
}

// Get returns info for a single pipeline.
func (m *Manager) Get(name string) (PipelineInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	mp, ok := m.pipelines[name]
	if !ok {
		return PipelineInfo{}, fmt.Errorf("pipeline %q not found", name)
	}
	return mp.info(), nil
}

// StartAll starts all stopped pipelines.
func (m *Manager) StartAll(ctx context.Context) error {
	m.mu.RLock()
	names := make([]string, 0, len(m.pipelines))
	for name, mp := range m.pipelines {
		if mp.status == StatusStopped {
			names = append(names, name)
		}
	}
	m.mu.RUnlock()

	for _, name := range names {
		if err := m.Start(ctx, name); err != nil {
			return fmt.Errorf("start pipeline %q: %w", name, err)
		}
	}
	return nil
}

// StopAll gracefully stops all running pipelines.
func (m *Manager) StopAll() {
	m.mu.RLock()
	names := make([]string, 0, len(m.pipelines))
	for name, mp := range m.pipelines {
		if mp.status == StatusRunning || mp.status == StatusPaused {
			names = append(names, name)
		}
	}
	m.mu.RUnlock()

	for _, name := range names {
		if err := m.Stop(name); err != nil {
			m.logger.Error("stop pipeline error", "pipeline", name, "error", err)
		}
	}
}

// Pause pauses a running pipeline. The pipeline context is cancelled but the
// managed pipeline entry is kept with status "paused" so it can be resumed.
func (m *Manager) Pause(name string) error {
	m.mu.RLock()
	mp, ok := m.pipelines[name]
	if !ok {
		m.mu.RUnlock()
		return fmt.Errorf("pipeline %q not found", name)
	}
	if mp.status != StatusRunning {
		m.mu.RUnlock()
		return fmt.Errorf("pipeline %q is not running (status: %s)", name, mp.status)
	}
	cancel := mp.cancel
	done := mp.done
	m.mu.RUnlock()

	if cancel != nil {
		cancel()
	}

	// Wait for the pipeline goroutine to finish and release its lock.
	if done != nil {
		<-done
	}

	// The background goroutine has completed and set status to stopped/error.
	// Override to paused.
	m.mu.Lock()
	mp.status = StatusPaused
	mp.lastErr = ""
	m.health.SetStatus("pipeline:"+name, health.StatusDown)
	m.mu.Unlock()

	m.logger.Info("pipeline paused", "pipeline", name)
	return nil
}

// Resume restarts a paused pipeline from its existing config.
func (m *Manager) Resume(ctx context.Context, name string) error {
	m.mu.RLock()
	mp, ok := m.pipelines[name]
	if !ok {
		m.mu.RUnlock()
		return fmt.Errorf("pipeline %q not found", name)
	}
	if mp.status != StatusPaused {
		m.mu.RUnlock()
		return fmt.Errorf("pipeline %q is not paused (status: %s)", name, mp.status)
	}
	m.mu.RUnlock()

	return m.Start(ctx, name)
}

func (mp *ManagedPipeline) info() PipelineInfo {
	return PipelineInfo{
		Name:      mp.config.Name,
		Status:    mp.status,
		StartedAt: mp.startedAt,
		Error:     mp.lastErr,
		Config:    mp.config,
	}
}

// MarshalJSON implements json.Marshaler for PipelineInfo.
func (pi PipelineInfo) MarshalJSON() ([]byte, error) {
	type Alias PipelineInfo
	return json.Marshal(struct{ Alias }{Alias(pi)})
}
