//go:build integration

package scenarios

import (
	"strings"
	"testing"
)

func TestScenario_CLIValidation(t *testing.T) {
	t.Run("missing --db", func(t *testing.T) {
		output, err := runPGPipe("listen", "--channel", "orders")
		if err == nil {
			t.Fatal("expected error for missing --db")
		}
		if !strings.Contains(output, "no database URL") {
			t.Errorf("unexpected output: %s", output)
		}
	})

	t.Run("missing --channel", func(t *testing.T) {
		output, err := runPGPipe("listen", "--db", "postgres://localhost/test")
		if err == nil {
			t.Fatal("expected error for missing --channel")
		}
		if !strings.Contains(output, "no channels") {
			t.Errorf("unexpected output: %s", output)
		}
	})

	t.Run("unknown adapter", func(t *testing.T) {
		output, err := runPGPipe("listen", "--db", "postgres://localhost/test", "--channel", "orders", "--adapter", "kafka")
		if err == nil {
			t.Fatal("expected error for unknown adapter")
		}
		if !strings.Contains(output, "unknown adapter") {
			t.Errorf("unexpected output: %s", output)
		}
	})

	t.Run("tx-metadata requires WAL detector", func(t *testing.T) {
		output, err := runPGPipe("listen", "--db", "postgres://localhost/test", "--channel", "orders", "--tx-metadata")
		if err == nil {
			t.Fatal("expected error for --tx-metadata without WAL")
		}
		if !strings.Contains(output, "--tx-metadata and --tx-markers require --detector wal") {
			t.Errorf("unexpected output: %s", output)
		}
	})

	t.Run("tx-markers requires WAL detector", func(t *testing.T) {
		output, err := runPGPipe("listen", "--db", "postgres://localhost/test", "--channel", "orders", "--tx-markers")
		if err == nil {
			t.Fatal("expected error for --tx-markers without WAL")
		}
		if !strings.Contains(output, "--tx-metadata and --tx-markers require --detector wal") {
			t.Errorf("unexpected output: %s", output)
		}
	})

	t.Run("webhook without --url", func(t *testing.T) {
		output, err := runPGPipe("listen", "--db", "postgres://localhost/test", "--channel", "orders", "--adapter", "webhook")
		if err == nil {
			t.Fatal("expected error for webhook without URL")
		}
		if !strings.Contains(output, "webhook adapter requires a URL") {
			t.Errorf("unexpected output: %s", output)
		}
	})
}
