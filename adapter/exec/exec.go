package exec

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/exec"
	"time"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/backoff"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/pgcdcerr"
)

const (
	defaultBackoffBase = 1 * time.Second
	defaultBackoffCap  = 30 * time.Second
)

// Adapter pipes events as JSON lines to a long-running subprocess's stdin.
// If the process exits, it is restarted with exponential backoff.
type Adapter struct {
	command     string
	backoffBase time.Duration
	backoffCap  time.Duration
	logger      *slog.Logger
}

// New creates an exec adapter. The command is run via "sh -c".
// Duration parameters default to sensible values when zero.
func New(command string, backoffBase, backoffCap time.Duration, logger *slog.Logger) *Adapter {
	if backoffBase <= 0 {
		backoffBase = defaultBackoffBase
	}
	if backoffCap <= 0 {
		backoffCap = defaultBackoffCap
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Adapter{
		command:     command,
		backoffBase: backoffBase,
		backoffCap:  backoffCap,
		logger:      logger.With("adapter", "exec"),
	}
}

// Name returns the adapter name.
func (a *Adapter) Name() string {
	return "exec"
}

// Validate implements adapter.Validator: checks that the command is resolvable.
func (a *Adapter) Validate(_ context.Context) error {
	if a.command == "" {
		return fmt.Errorf("exec: command is required")
	}
	// Check that the shell is available (command is run via "sh -c").
	if _, err := exec.LookPath("sh"); err != nil {
		return fmt.Errorf("exec: shell not found: %w", err)
	}
	return nil
}

// Start blocks, consuming events and writing them as JSON lines to the
// subprocess stdin. The process is restarted on failure with backoff.
func (a *Adapter) Start(ctx context.Context, events <-chan event.Event) error {
	a.logger.Info("exec adapter started", "command", a.command)

	var attempt int
	var pending *event.Event
	for {
		runErr := a.run(ctx, events, &pending)
		if ctx.Err() != nil {
			return ctx.Err()
		}

		delay := backoff.Jitter(attempt, a.backoffBase, a.backoffCap)
		a.logger.Error("process exited, restarting",
			"error", runErr,
			"attempt", attempt+1,
			"delay", delay,
		)
		metrics.ExecRestarts.Inc()
		attempt++

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

// run starts one process and streams events to it. If pending is non-nil, it
// is sent first (re-delivery after a broken pipe). On write error, the failed
// event is stored in pending for re-delivery on the next process.
func (a *Adapter) run(ctx context.Context, events <-chan event.Event, pending **event.Event) error {
	cmd := exec.CommandContext(ctx, "sh", "-c", a.command)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return &pgcdcerr.ExecProcessError{Command: a.command, Err: fmt.Errorf("stdin pipe: %w", err)}
	}

	if err := cmd.Start(); err != nil {
		return &pgcdcerr.ExecProcessError{Command: a.command, Err: fmt.Errorf("start: %w", err)}
	}

	enc := json.NewEncoder(stdin)

	// Re-send pending event from previous process crash.
	if *pending != nil {
		if err := enc.Encode(*pending); err != nil {
			_ = stdin.Close()
			_ = cmd.Wait()
			return &pgcdcerr.ExecProcessError{Command: a.command, Err: fmt.Errorf("write pending: %w", err)}
		}
		metrics.EventsDelivered.WithLabelValues("exec").Inc()
		*pending = nil
	}

	// Wait for process exit in the background.
	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()

	for {
		select {
		case <-ctx.Done():
			_ = stdin.Close()
			<-waitCh
			return ctx.Err()

		case waitErr := <-waitCh:
			// Process exited on its own.
			return &pgcdcerr.ExecProcessError{Command: a.command, Err: fmt.Errorf("exited: %w", waitErr)}

		case ev, ok := <-events:
			if !ok {
				_ = stdin.Close()
				<-waitCh
				return nil
			}
			if err := enc.Encode(ev); err != nil {
				*pending = &ev
				_ = stdin.Close()
				<-waitCh
				return &pgcdcerr.ExecProcessError{Command: a.command, Err: fmt.Errorf("write: %w", err)}
			}
			metrics.EventsDelivered.WithLabelValues("exec").Inc()
		}
	}
}
