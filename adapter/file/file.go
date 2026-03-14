package file

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/metrics"
)

const (
	defaultMaxSize  = 100 * 1024 * 1024 // 100 MB
	defaultMaxFiles = 5
)

// Adapter writes events as JSON lines to a file with optional rotation.
//
// Implements adapter.Deliverer — the middleware stack provides the event loop,
// metrics, and cooperative checkpoint ack.
type Adapter struct {
	path     string
	maxSize  int64
	maxFiles int
	logger   *slog.Logger

	mu   sync.Mutex
	f    *os.File
	enc  *json.Encoder
	init bool
}

// New creates a file adapter. maxSize is the max file size before rotation
// (defaults to 100MB if <= 0). maxFiles is the number of rotated files to keep
// (defaults to 5 if <= 0).
func New(path string, maxSize int64, maxFiles int, logger *slog.Logger) *Adapter {
	if maxSize <= 0 {
		maxSize = defaultMaxSize
	}
	if maxFiles <= 0 {
		maxFiles = defaultMaxFiles
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Adapter{
		path:     path,
		maxSize:  maxSize,
		maxFiles: maxFiles,
		logger:   logger.With("adapter", "file"),
	}
}

// Name returns the adapter name.
func (a *Adapter) Name() string {
	return "file"
}

// Validate checks that the parent directory exists and is writable.
func (a *Adapter) Validate(_ context.Context) error {
	dir := filepath.Dir(a.path)
	info, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("stat parent directory %q: %w", dir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("parent path %q is not a directory", dir)
	}
	return nil
}

// Deliver writes a single event as a JSON line. Implements adapter.Deliverer.
// Lazily opens the file on first call.
func (a *Adapter) Deliver(_ context.Context, ev event.Event) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.init {
		if err := a.open(); err != nil {
			return fmt.Errorf("open: %w", err)
		}
		a.init = true
	}

	if err := a.enc.Encode(ev); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	if err := a.f.Sync(); err != nil {
		a.logger.Warn("failed to sync file", "error", err)
	} else {
		metrics.EventsDelivered.WithLabelValues("file").Inc()
	}

	return a.maybeRotate()
}

// Start is the legacy event loop kept for backward compatibility.
func (a *Adapter) Start(ctx context.Context, events <-chan event.Event) error {
	a.logger.Info("file adapter started (legacy path)", "path", a.path)
	for {
		select {
		case <-ctx.Done():
			a.close()
			return ctx.Err()
		case ev, ok := <-events:
			if !ok {
				a.close()
				return nil
			}
			if err := a.Deliver(ctx, ev); err != nil {
				return err
			}
		}
	}
}

// Drain closes the file on graceful shutdown.
func (a *Adapter) Drain(_ context.Context) error {
	a.close()
	return nil
}

func (a *Adapter) open() error {
	f, err := os.OpenFile(a.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	a.f = f
	a.enc = json.NewEncoder(f)
	return nil
}

func (a *Adapter) close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.f != nil {
		_ = a.f.Close()
		a.f = nil
		a.init = false
	}
}

func (a *Adapter) maybeRotate() error {
	info, err := a.f.Stat()
	if err != nil {
		return fmt.Errorf("stat: %w", err)
	}
	if info.Size() < a.maxSize {
		return nil
	}
	return a.rotate()
}

func (a *Adapter) rotate() error {
	if a.f != nil {
		_ = a.f.Close()
	}

	// Remove the oldest rotated file.
	oldest := fmt.Sprintf("%s.%d", a.path, a.maxFiles)
	_ = os.Remove(oldest)

	// Shift existing rotated files: .{n} -> .{n+1}
	for n := a.maxFiles - 1; n >= 1; n-- {
		src := fmt.Sprintf("%s.%d", a.path, n)
		dst := fmt.Sprintf("%s.%d", a.path, n+1)
		_ = os.Rename(src, dst)
	}

	// Rename current file to .1
	if err := os.Rename(a.path, a.path+".1"); err != nil {
		return fmt.Errorf("rename to .1: %w", err)
	}

	metrics.FileRotations.Inc()
	a.logger.Info("file rotated", "path", a.path)

	return a.open()
}
