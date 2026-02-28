package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/florinutz/pgcdc"
	"github.com/florinutz/pgcdc/adapter/stdout"
	"github.com/florinutz/pgcdc/detector/listennotify"
	"github.com/florinutz/pgcdc/event"
)

// mockBuilder creates a pipeline with a mock detector that blocks until context cancellation.
func mockBuilder(cfg PipelineConfig, logger *slog.Logger) (*pgcdc.Pipeline, error) {
	if cfg.Name == "fail-build" {
		return nil, fmt.Errorf("intentional build failure")
	}
	// Use a listennotify detector with an invalid DSN — it won't actually connect
	// but the Pipeline object is valid.
	det := listennotify.New("postgres://invalid:5432/test", nil, 0, 0, nil)
	p := pgcdc.NewPipeline(det,
		pgcdc.WithAdapter(stdout.New(nil, nil)),
		pgcdc.WithSkipValidation(true),
	)
	return p, nil
}

// blockingDetector is a detector that blocks until its context is cancelled.
type blockingDetector struct{}

func (d *blockingDetector) Start(ctx context.Context, events chan<- event.Event) error {
	<-ctx.Done()
	return ctx.Err()
}

func (d *blockingDetector) Name() string { return "blocking" }

// blockingAdapter is an adapter that blocks until its context is cancelled.
type blockingAdapter struct{}

func (a *blockingAdapter) Start(ctx context.Context, events <-chan event.Event) error {
	<-ctx.Done()
	return ctx.Err()
}

func (a *blockingAdapter) Name() string { return "blocking" }

// blockingBuilder creates a pipeline with a blocking detector and adapter that
// stay alive until context cancellation. This lets tests exercise the full
// start/pause/resume/stop lifecycle.
func blockingBuilder(cfg PipelineConfig, logger *slog.Logger) (*pgcdc.Pipeline, error) {
	if cfg.Name == "fail-build" {
		return nil, fmt.Errorf("intentional build failure")
	}
	p := pgcdc.NewPipeline(&blockingDetector{},
		pgcdc.WithAdapter(&blockingAdapter{}),
		pgcdc.WithSkipValidation(true),
	)
	return p, nil
}

func newTestManager() *Manager {
	return NewManager(mockBuilder, nil)
}

func TestManager_AddAndList(t *testing.T) {
	mgr := newTestManager()

	err := mgr.Add(PipelineConfig{Name: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	err = mgr.Add(PipelineConfig{Name: "users"})
	if err != nil {
		t.Fatal(err)
	}

	list := mgr.List()
	if len(list) != 2 {
		t.Fatalf("expected 2 pipelines, got %d", len(list))
	}

	// Verify statuses are stopped.
	for _, info := range list {
		if info.Status != StatusStopped {
			t.Errorf("pipeline %s: expected stopped, got %s", info.Name, info.Status)
		}
	}
}

func TestManager_AddDuplicate(t *testing.T) {
	mgr := newTestManager()
	_ = mgr.Add(PipelineConfig{Name: "orders"})

	err := mgr.Add(PipelineConfig{Name: "orders"})
	if err == nil {
		t.Fatal("expected error for duplicate pipeline name")
	}
}

func TestManager_AddEmptyName(t *testing.T) {
	mgr := newTestManager()

	err := mgr.Add(PipelineConfig{})
	if err == nil {
		t.Fatal("expected error for empty pipeline name")
	}
}

func TestManager_Get(t *testing.T) {
	mgr := newTestManager()
	_ = mgr.Add(PipelineConfig{Name: "orders"})

	info, err := mgr.Get("orders")
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != "orders" {
		t.Errorf("expected name 'orders', got %q", info.Name)
	}
	if info.Status != StatusStopped {
		t.Errorf("expected stopped, got %s", info.Status)
	}
}

func TestManager_GetNotFound(t *testing.T) {
	mgr := newTestManager()

	_, err := mgr.Get("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent pipeline")
	}
}

func TestManager_RemoveStopped(t *testing.T) {
	mgr := newTestManager()
	_ = mgr.Add(PipelineConfig{Name: "orders"})

	err := mgr.Remove("orders")
	if err != nil {
		t.Fatal(err)
	}

	list := mgr.List()
	if len(list) != 0 {
		t.Fatalf("expected 0 pipelines after remove, got %d", len(list))
	}
}

func TestManager_RemoveNotFound(t *testing.T) {
	mgr := newTestManager()

	err := mgr.Remove("nonexistent")
	if err == nil {
		t.Fatal("expected error for removing nonexistent pipeline")
	}
}

func TestManager_StopNotRunning(t *testing.T) {
	mgr := newTestManager()
	_ = mgr.Add(PipelineConfig{Name: "orders"})

	err := mgr.Stop("orders")
	if err == nil {
		t.Fatal("expected error for stopping non-running pipeline")
	}
}

func TestManager_StopNotFound(t *testing.T) {
	mgr := newTestManager()

	err := mgr.Stop("nonexistent")
	if err == nil {
		t.Fatal("expected error for stopping nonexistent pipeline")
	}
}

func TestManager_StartNotFound(t *testing.T) {
	mgr := newTestManager()

	err := mgr.Start(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error for starting nonexistent pipeline")
	}
}

func TestManager_BuildFailure(t *testing.T) {
	mgr := NewManager(mockBuilder, nil)

	_ = mgr.Add(PipelineConfig{Name: "fail-build"})
	err := mgr.Start(context.Background(), "fail-build")
	if err == nil {
		t.Fatal("expected error for build failure")
	}

	info, _ := mgr.Get("fail-build")
	if info.Status != StatusError {
		t.Errorf("expected error status, got %s", info.Status)
	}
}

func TestManager_ConcurrentAccess(t *testing.T) {
	mgr := newTestManager()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			name := fmt.Sprintf("pipeline-%d", n)
			_ = mgr.Add(PipelineConfig{Name: name})
			_ = mgr.List()
			_, _ = mgr.Get(name)
		}(i)
	}
	wg.Wait()

	list := mgr.List()
	if len(list) != 10 {
		t.Errorf("expected 10 pipelines, got %d", len(list))
	}
}

// --- API handler tests ---

func TestAPI_ListPipelines(t *testing.T) {
	mgr := newTestManager()
	_ = mgr.Add(PipelineConfig{Name: "orders"})
	_ = mgr.Add(PipelineConfig{Name: "users"})

	handler := APIHandler(mgr)
	req := httptest.NewRequest("GET", "/api/v1/pipelines", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var body map[string]any
	json.NewDecoder(rec.Body).Decode(&body)
	count := int(body["count"].(float64))
	if count != 2 {
		t.Errorf("expected count 2, got %d", count)
	}
}

func TestAPI_GetPipeline(t *testing.T) {
	mgr := newTestManager()
	_ = mgr.Add(PipelineConfig{Name: "orders"})

	handler := APIHandler(mgr)
	req := httptest.NewRequest("GET", "/api/v1/pipelines/orders", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestAPI_GetPipelineNotFound(t *testing.T) {
	mgr := newTestManager()

	handler := APIHandler(mgr)
	req := httptest.NewRequest("GET", "/api/v1/pipelines/nonexistent", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestAPI_CreatePipeline(t *testing.T) {
	mgr := newTestManager()

	handler := APIHandler(mgr)
	body := `{"name":"orders","detector":{"type":"wal"},"adapters":[{"name":"stdout","type":"stdout"}]}`
	req := httptest.NewRequest("POST", "/api/v1/pipelines", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}

	list := mgr.List()
	if len(list) != 1 {
		t.Fatalf("expected 1 pipeline, got %d", len(list))
	}
}

func TestAPI_CreatePipelineBadJSON(t *testing.T) {
	mgr := newTestManager()

	handler := APIHandler(mgr)
	req := httptest.NewRequest("POST", "/api/v1/pipelines", strings.NewReader("not json"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestAPI_DeletePipeline(t *testing.T) {
	mgr := newTestManager()
	_ = mgr.Add(PipelineConfig{Name: "orders"})

	handler := APIHandler(mgr)
	req := httptest.NewRequest("DELETE", "/api/v1/pipelines/orders", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	list := mgr.List()
	if len(list) != 0 {
		t.Fatalf("expected 0 pipelines after delete, got %d", len(list))
	}
}

func TestAPI_StopNotRunning(t *testing.T) {
	mgr := newTestManager()
	_ = mgr.Add(PipelineConfig{Name: "orders"})

	handler := APIHandler(mgr)
	req := httptest.NewRequest("POST", "/api/v1/pipelines/orders/stop", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rec.Code)
	}
}

func TestPipelineInfo_JSON(t *testing.T) {
	now := time.Now().UTC()
	info := PipelineInfo{
		Name:      "orders",
		Status:    StatusRunning,
		StartedAt: &now,
		Config: PipelineConfig{
			Name:     "orders",
			Detector: DetectorSpec{Type: "wal"},
		},
	}

	data, err := json.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}

	var decoded map[string]any
	json.Unmarshal(data, &decoded)
	if decoded["name"] != "orders" {
		t.Errorf("expected name 'orders', got %v", decoded["name"])
	}
	if decoded["status"] != "running" {
		t.Errorf("expected status 'running', got %v", decoded["status"])
	}
}

// --- Pause/Resume tests ---

func TestManager_PauseRunningPipeline(t *testing.T) {
	mgr := NewManager(blockingBuilder, nil)

	_ = mgr.Add(PipelineConfig{Name: "orders"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := mgr.Start(ctx, "orders"); err != nil {
		t.Fatal(err)
	}

	// Give the pipeline goroutine a moment to transition to running.
	time.Sleep(50 * time.Millisecond)

	if err := mgr.Pause("orders"); err != nil {
		t.Fatal(err)
	}

	info, err := mgr.Get("orders")
	if err != nil {
		t.Fatal(err)
	}
	if info.Status != StatusPaused {
		t.Errorf("expected status paused, got %s", info.Status)
	}
}

func TestManager_PauseNotRunning(t *testing.T) {
	mgr := NewManager(blockingBuilder, nil)
	_ = mgr.Add(PipelineConfig{Name: "orders"})

	err := mgr.Pause("orders")
	if err == nil {
		t.Fatal("expected error when pausing a stopped pipeline")
	}
}

func TestManager_PauseNotFound(t *testing.T) {
	mgr := NewManager(blockingBuilder, nil)

	err := mgr.Pause("nonexistent")
	if err == nil {
		t.Fatal("expected error when pausing nonexistent pipeline")
	}
}

func TestManager_ResumeAfterPause(t *testing.T) {
	mgr := NewManager(blockingBuilder, nil)

	_ = mgr.Add(PipelineConfig{Name: "orders"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := mgr.Start(ctx, "orders"); err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	if err := mgr.Pause("orders"); err != nil {
		t.Fatal(err)
	}

	info, _ := mgr.Get("orders")
	if info.Status != StatusPaused {
		t.Fatalf("expected paused, got %s", info.Status)
	}

	if err := mgr.Resume(ctx, "orders"); err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	info, _ = mgr.Get("orders")
	if info.Status != StatusRunning {
		t.Errorf("expected running after resume, got %s", info.Status)
	}

	// Clean up: stop the resumed pipeline.
	if err := mgr.Stop("orders"); err != nil {
		t.Fatal(err)
	}
}

func TestManager_ResumeNotPaused(t *testing.T) {
	mgr := NewManager(blockingBuilder, nil)
	_ = mgr.Add(PipelineConfig{Name: "orders"})

	err := mgr.Resume(context.Background(), "orders")
	if err == nil {
		t.Fatal("expected error when resuming a non-paused pipeline")
	}
}

func TestManager_ResumeNotFound(t *testing.T) {
	mgr := NewManager(blockingBuilder, nil)

	err := mgr.Resume(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error when resuming nonexistent pipeline")
	}
}

func TestManager_PauseThenStop(t *testing.T) {
	mgr := NewManager(blockingBuilder, nil)

	_ = mgr.Add(PipelineConfig{Name: "orders"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := mgr.Start(ctx, "orders"); err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	if err := mgr.Pause("orders"); err != nil {
		t.Fatal(err)
	}

	// Stop should work on a paused pipeline (existing behavior).
	if err := mgr.Stop("orders"); err != nil {
		t.Fatal(err)
	}

	info, _ := mgr.Get("orders")
	if info.Status != StatusStopped {
		t.Errorf("expected stopped, got %s", info.Status)
	}
}

func TestManager_RemovePaused(t *testing.T) {
	mgr := NewManager(blockingBuilder, nil)

	_ = mgr.Add(PipelineConfig{Name: "orders"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := mgr.Start(ctx, "orders"); err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	if err := mgr.Pause("orders"); err != nil {
		t.Fatal(err)
	}

	// Paused pipelines cannot be removed (same guard as running).
	err := mgr.Remove("orders")
	if err == nil {
		t.Fatal("expected error when removing a paused pipeline")
	}
}

// --- API pause/resume tests ---

func TestAPI_PausePipeline(t *testing.T) {
	mgr := NewManager(blockingBuilder, nil)
	_ = mgr.Add(PipelineConfig{Name: "orders"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := mgr.Start(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	handler := APIHandler(mgr)
	req := httptest.NewRequest("POST", "/api/v1/pipelines/orders/pause", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	info, _ := mgr.Get("orders")
	if info.Status != StatusPaused {
		t.Errorf("expected paused, got %s", info.Status)
	}
}

func TestAPI_PauseNotRunning(t *testing.T) {
	mgr := NewManager(blockingBuilder, nil)
	_ = mgr.Add(PipelineConfig{Name: "orders"})

	handler := APIHandler(mgr)
	req := httptest.NewRequest("POST", "/api/v1/pipelines/orders/pause", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestAPI_ResumePipeline(t *testing.T) {
	mgr := NewManager(blockingBuilder, nil)
	_ = mgr.Add(PipelineConfig{Name: "orders"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := mgr.Start(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	if err := mgr.Pause("orders"); err != nil {
		t.Fatal(err)
	}

	handler := APIHandler(mgr)
	req := httptest.NewRequest("POST", "/api/v1/pipelines/orders/resume", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	time.Sleep(50 * time.Millisecond)

	info, _ := mgr.Get("orders")
	if info.Status != StatusRunning {
		t.Errorf("expected running after resume, got %s", info.Status)
	}

	// Clean up.
	mgr.Stop("orders")
}

func TestAPI_ResumeNotPaused(t *testing.T) {
	mgr := NewManager(blockingBuilder, nil)
	_ = mgr.Add(PipelineConfig{Name: "orders"})

	handler := APIHandler(mgr)
	req := httptest.NewRequest("POST", "/api/v1/pipelines/orders/resume", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", rec.Code, rec.Body.String())
	}
}
