package file

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/florinutz/pgpipe/event"
	"github.com/florinutz/pgpipe/metrics"
)

const (
	defaultMaxSize  = 100 * 1024 * 1024 // 100 MB
	defaultMaxFiles = 5
)

// Adapter writes events as JSON lines to a file with optional rotation.
type Adapter struct {
	path     string
	maxSize  int64
	maxFiles int
	logger   *slog.Logger
	f        *os.File
	enc      *json.Encoder
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

// Start blocks, consuming events and writing them as JSON lines to the file.
func (a *Adapter) Start(ctx context.Context, events <-chan event.Event) error {
	a.logger.Info("file adapter started", "path", a.path)

	if err := a.open(); err != nil {
		return fmt.Errorf("file adapter open: %w", err)
	}
	defer a.close()

	for {
		select {
		case <-ctx.Done():
			a.logger.Info("file adapter stopping", "reason", "context cancelled")
			return ctx.Err()

		case ev, ok := <-events:
			if !ok {
				a.logger.Info("file adapter stopping", "reason", "channel closed")
				return nil
			}
			if err := a.enc.Encode(ev); err != nil {
				a.logger.Error("failed to encode event", "event_id", ev.ID, "error", err)
				continue
			}
			if err := a.f.Sync(); err != nil {
				a.logger.Warn("failed to sync file", "error", err)
			}
			metrics.EventsDelivered.WithLabelValues("file").Inc()

			if err := a.maybeRotate(); err != nil {
				a.logger.Error("rotation failed", "error", err)
			}
		}
	}
}

func (a *Adapter) open() error {
	f, err := os.OpenFile(a.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	a.f = f
	a.enc = json.NewEncoder(f)
	return nil
}

func (a *Adapter) close() {
	if a.f != nil {
		_ = a.f.Close()
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
	a.close()

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
