package logcolor

import (
	"io"
	"log/slog"
	"os"
	"runtime"
	"strings"

	"golang.org/x/term"
)

// ANSI color codes.
const (
	Reset   = "\033[0m"
	Dim     = "\033[2m"
	Cyan    = "\033[36m"
	Yellow  = "\033[33m"
	RedBold = "\033[1;31m"
)

// colorWriter wraps an io.Writer and replaces slog level labels with colored versions.
type colorWriter struct {
	w io.Writer
}

func (cw *colorWriter) Write(p []byte) (int, error) {
	s := string(p)
	// Replace level labels with colored versions. Only the first match
	// is replaced since each log line has exactly one level field.
	switch {
	case strings.Contains(s, "level=DEBUG"):
		s = strings.Replace(s, "level=DEBUG", "level="+Dim+"DEBUG"+Reset, 1)
	case strings.Contains(s, "level=INFO"):
		s = strings.Replace(s, "level=INFO", "level="+Cyan+"INFO"+Reset, 1)
	case strings.Contains(s, "level=WARN"):
		s = strings.Replace(s, "level=WARN", "level="+Yellow+"WARN"+Reset, 1)
	case strings.Contains(s, "level=ERROR"):
		s = strings.Replace(s, "level=ERROR", "level="+RedBold+"ERROR"+Reset, 1)
	}
	_, err := cw.w.Write([]byte(s))
	return len(p), err
}

// NewHandler creates a colored slog handler writing to w.
// It wraps slog.TextHandler and post-processes the output to add ANSI color
// codes to level labels, avoiding issues with TextHandler quoting.
func NewHandler(w io.Writer, opts *slog.HandlerOptions) slog.Handler {
	return slog.NewTextHandler(&colorWriter{w: w}, opts)
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
