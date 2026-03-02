package logcolor

import (
	"context"
	"io"
	"log/slog"
	"os"
	"runtime"

	"golang.org/x/term"
)

// ANSI color codes.
const (
	reset   = "\033[0m"
	dim     = "\033[2m"
	cyan    = "\033[36m"
	yellow  = "\033[33m"
	red     = "\033[31m"
	redBold = "\033[1;31m"
)

// levelColor returns the ANSI prefix for a given log level.
func levelColor(level slog.Level) string {
	switch {
	case level < slog.LevelInfo:
		return dim
	case level < slog.LevelWarn:
		return cyan
	case level < slog.LevelError:
		return yellow
	default:
		return redBold
	}
}

// Handler is an slog.Handler that adds ANSI color to level labels.
// It wraps an slog.TextHandler and prepends color codes.
type Handler struct {
	inner slog.Handler
}

// NewHandler creates a colored slog handler writing to w.
func NewHandler(w io.Writer, opts *slog.HandlerOptions) *Handler {
	if opts == nil {
		opts = &slog.HandlerOptions{}
	}

	// Wrap the ReplaceAttr to inject color codes around the level.
	origReplace := opts.ReplaceAttr
	opts.ReplaceAttr = func(groups []string, a slog.Attr) slog.Attr {
		if origReplace != nil {
			a = origReplace(groups, a)
		}
		if a.Key == slog.LevelKey {
			level := a.Value.Any().(slog.Level)
			a.Value = slog.StringValue(levelColor(level) + level.String() + reset)
		}
		return a
	}

	return &Handler{
		inner: slog.NewTextHandler(w, opts),
	}
}

// Enabled reports whether the handler handles records at the given level.
func (h *Handler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

// Handle formats a Record with colored level labels.
func (h *Handler) Handle(ctx context.Context, r slog.Record) error {
	return h.inner.Handle(ctx, r)
}

// WithAttrs returns a new Handler with the given attributes.
func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &Handler{inner: h.inner.WithAttrs(attrs)}
}

// WithGroup returns a new Handler with the given group name.
func (h *Handler) WithGroup(name string) slog.Handler {
	return &Handler{inner: h.inner.WithGroup(name)}
}

// ShouldColor returns true if colored output should be used.
// Returns false if NO_COLOR is set, the format is explicitly "text" or "json",
// the writer is not a TTY, or the platform is Windows (no reliable ANSI support detection).
func ShouldColor(fd uintptr, format string) bool {
	if format == "text" || format == "json" {
		return false
	}
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	if runtime.GOOS == "windows" {
		return false
	}
	return term.IsTerminal(int(fd))
}
