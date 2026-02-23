//go:build integration

package scenarios

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/florinutz/pgpipe/adapter/stdout"
	"github.com/florinutz/pgpipe/event"
)

func TestScenario_Reconnection(t *testing.T) {
	connStr := startPostgres(t)
	capture := newLineCapture()
	startPipeline(t, connStr, []string{"reconnect_test"}, stdout.New(capture, testLogger()))

	// Wait for detector to connect.
	time.Sleep(1 * time.Second)

	t.Run("happy path", func(t *testing.T) {
		sendNotify(t, connStr, "reconnect_test", `{"op":"INSERT","table":"orders","row":{"id":1}}`)

		line := capture.waitLine(t, 5*time.Second)

		var ev event.Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("invalid JSON: %v\nraw: %s", err, line)
		}
		if ev.Operation != "INSERT" {
			t.Errorf("operation = %q, want INSERT", ev.Operation)
		}
	})

	t.Run("events resume after reconnect", func(t *testing.T) {
		// Kill the detector's PG connection.
		terminateBackends(t, connStr)

		// Drain any leftover lines from previous subtest.
		capture.drain()

		// Wait for detector to reconnect. The backoff starts at [100ms, 5s).
		// Retry sending notifications until one arrives through the pipeline.
		deadline := time.After(20 * time.Second)
		for {
			sendNotify(t, connStr, "reconnect_test", `{"op":"UPDATE","table":"orders","row":{"id":2}}`)

			select {
			case line := <-capture.lines:
				var ev event.Event
				if err := json.Unmarshal([]byte(line), &ev); err != nil {
					t.Fatalf("invalid JSON after reconnect: %v", err)
				}
				if ev.Operation != "UPDATE" {
					t.Errorf("operation = %q, want UPDATE", ev.Operation)
				}
				return
			case <-time.After(2 * time.Second):
				// Not reconnected yet, retry.
			case <-deadline:
				t.Fatal("detector did not reconnect within 20s")
			}
		}
	})
}
