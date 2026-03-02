package output

import (
	"bytes"
	"strings"
	"testing"

	"github.com/florinutz/pgcdc/internal/config"
)

func TestPrintSummary_MinimalConfig(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	cfg := &config.Config{
		Detector: config.DetectorConfig{Type: "wal"},
	}
	PrintSummary(&buf, cfg, false)

	out := buf.String()
	if !strings.Contains(out, "Detector:") {
		t.Errorf("expected Detector line, got:\n%s", out)
	}
	if !strings.Contains(out, "wal") {
		t.Errorf("expected 'wal' in output, got:\n%s", out)
	}
	// Box borders.
	if !strings.Contains(out, "+--") {
		t.Errorf("expected box border, got:\n%s", out)
	}
}

func TestPrintSummary_FullConfig(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	cfg := &config.Config{
		Detector: config.DetectorConfig{
			Type:                  "wal",
			CooperativeCheckpoint: true,
		},
		Adapters: []string{"stdout", "sse", "webhook"},
		Channels: []string{"orders", "users"},
		Bus:      config.BusConfig{Mode: "reliable", BufferSize: 2048},
		Backpressure: config.BackpressureConfig{
			Enabled: true,
		},
		SSE: config.SSEConfig{Addr: ":9090"},
	}
	PrintSummary(&buf, cfg, false)

	out := buf.String()

	checks := []string{
		"Detector:", "wal", "cooperative-ckpt",
		"Adapters:", "stdout", "sse", "webhook",
		"Channels:", "orders", "users",
		"Bus:", "reliable", "2048",
		"Features:", "backpressure",
		"Endpoints:", ":9090",
	}
	for _, s := range checks {
		if !strings.Contains(out, s) {
			t.Errorf("expected %q in output, got:\n%s", s, out)
		}
	}
}

func TestPrintSummary_SchemaStore(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	cfg := &config.Config{
		Schema: config.SchemaConfig{Enabled: true},
	}
	PrintSummary(&buf, cfg, false)

	out := buf.String()
	if !strings.Contains(out, "schema-store") {
		t.Errorf("expected 'schema-store' feature, got:\n%s", out)
	}
}

func TestPrintSummary_NilConfig(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	PrintSummary(&buf, nil, false)

	if buf.Len() != 0 {
		t.Errorf("expected no output for nil config, got:\n%s", buf.String())
	}
}

func TestPrintSummary_ColorOn(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	cfg := &config.Config{
		Detector: config.DetectorConfig{Type: "listen_notify"},
	}
	PrintSummary(&buf, cfg, true)

	out := buf.String()
	if !strings.Contains(out, "\033[36m") {
		t.Errorf("expected ANSI cyan code with color on, got:\n%s", out)
	}
	if !strings.Contains(out, "\033[0m") {
		t.Errorf("expected ANSI reset code with color on, got:\n%s", out)
	}
}

func TestPrintSummary_ColorOff(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	cfg := &config.Config{
		Detector: config.DetectorConfig{Type: "listen_notify"},
	}
	PrintSummary(&buf, cfg, false)

	out := buf.String()
	if strings.Contains(out, "\033[") {
		t.Errorf("expected no ANSI codes with color off, got:\n%s", out)
	}
}

func TestPrintSummary_DefaultValues(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	cfg := &config.Config{}
	PrintSummary(&buf, cfg, false)

	out := buf.String()
	// Default detector.
	if !strings.Contains(out, "listen_notify") {
		t.Errorf("expected default detector 'listen_notify', got:\n%s", out)
	}
	// Default bus mode.
	if !strings.Contains(out, "fast") {
		t.Errorf("expected default bus mode 'fast', got:\n%s", out)
	}
	// Default buffer size.
	if !strings.Contains(out, "1024") {
		t.Errorf("expected default buffer size '1024', got:\n%s", out)
	}
}

func TestPrintSummary_MetricsEndpoint(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	cfg := &config.Config{
		MetricsAddr: ":2112",
	}
	PrintSummary(&buf, cfg, false)

	out := buf.String()
	if !strings.Contains(out, ":2112") {
		t.Errorf("expected metrics addr in output, got:\n%s", out)
	}
	if !strings.Contains(out, "metrics") {
		t.Errorf("expected 'metrics' label, got:\n%s", out)
	}
}
