package logcolor

import (
	"bytes"
	"log/slog"
	"os"
	"strings"
	"testing"
)

func TestColorHandler_LevelColors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		level    slog.Level
		contains string
	}{
		{slog.LevelDebug, Dim},
		{slog.LevelInfo, Cyan},
		{slog.LevelWarn, Yellow},
		{slog.LevelError, RedBold},
	}

	for _, tt := range tests {
		t.Run(tt.level.String(), func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			h := NewHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
			logger := slog.New(h)

			logger.Log(nil, tt.level, "test message")

			out := buf.String()
			if !strings.Contains(out, tt.contains) {
				t.Errorf("expected output to contain ANSI code %q for level %s, got:\n%s",
					tt.contains, tt.level, out)
			}
			if !strings.Contains(out, Reset) {
				t.Errorf("expected output to contain reset code for level %s", tt.level)
			}
		})
	}
}

func TestColorHandler_WithAttrs(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	h := NewHandler(&buf, nil)
	h2 := h.WithAttrs([]slog.Attr{slog.String("component", "test")})
	logger := slog.New(h2)

	logger.Info("hello")

	out := buf.String()
	if !strings.Contains(out, "component=test") {
		t.Errorf("expected WithAttrs attr in output, got:\n%s", out)
	}
}

func TestColorHandler_WithGroup(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	h := NewHandler(&buf, nil)
	h2 := h.WithGroup("mygroup")
	logger := slog.New(h2)

	logger.Info("hello", "key", "val")

	out := buf.String()
	if !strings.Contains(out, "mygroup.key=val") {
		t.Errorf("expected group prefix in output, got:\n%s", out)
	}
}

func TestShouldColor_NoColor(t *testing.T) {
	t.Setenv("NO_COLOR", "1")

	if ShouldColor(os.Stderr.Fd(), "auto") {
		t.Error("expected ShouldColor to return false when NO_COLOR is set")
	}
}

func TestShouldColor_ExplicitFormats(t *testing.T) {
	t.Parallel()

	if ShouldColor(os.Stderr.Fd(), "text") {
		t.Error("expected ShouldColor to return false for explicit 'text' format")
	}
	if ShouldColor(os.Stderr.Fd(), "json") {
		t.Error("expected ShouldColor to return false for explicit 'json' format")
	}
}
