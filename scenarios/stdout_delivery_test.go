//go:build integration

package scenarios

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/adapter/stdout"
	"github.com/florinutz/pgcdc/event"
)

func TestScenario_StdoutDelivery(t *testing.T) {
	connStr := startPostgres(t)
	capture := newLineCapture()
	startPipeline(t, connStr, []string{"stdout_test"}, stdout.New(capture, testLogger()))

	// Wait for detector to connect and start listening.
	waitForDetector(t, connStr, "stdout_test", capture)

	t.Run("happy path", func(t *testing.T) {
		sendNotify(t, connStr, "stdout_test", `{"op":"INSERT","table":"orders","row":{"id":1}}`)

		line := capture.waitLine(t, 5*time.Second)

		var ev event.Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("invalid JSON output: %v\nraw: %s", err, line)
		}
		if ev.Channel != "stdout_test" {
			t.Errorf("channel = %q, want %q", ev.Channel, "stdout_test")
		}
		if ev.Operation != "INSERT" {
			t.Errorf("operation = %q, want %q", ev.Operation, "INSERT")
		}
		if ev.Source != "listen_notify" {
			t.Errorf("source = %q, want %q", ev.Source, "listen_notify")
		}
		if ev.ID == "" {
			t.Error("event ID is empty")
		}
	})

	t.Run("malformed payload handled gracefully", func(t *testing.T) {
		sendNotify(t, connStr, "stdout_test", "not-json-at-all")

		line := capture.waitLine(t, 5*time.Second)

		var ev event.Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("invalid JSON output: %v\nraw: %s", err, line)
		}
		if ev.Operation != "NOTIFY" {
			t.Errorf("operation = %q, want NOTIFY for non-JSON payload", ev.Operation)
		}
	})
}
