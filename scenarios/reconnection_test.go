//go:build integration

package scenarios

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/adapter/stdout"
	"github.com/florinutz/pgcdc/event"
)

func TestScenario_Reconnection(t *testing.T) {
	t.Parallel()
	connStr := startPostgres(t)
	capture := newLineCapture()
	startPipeline(t, connStr, []string{"reconnect_test"}, stdout.New(capture, testLogger()))

	// Wait for detector to connect.
	waitForDetector(t, connStr, "reconnect_test", capture)

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
		// Kill the detector's LISTEN connection.
		terminateListenBackend(t, connStr, "reconnect_test")

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

func TestScenario_WALReconnection(t *testing.T) {
	t.Parallel()
	connStr := startPostgres(t)

	table := "wal_reconnect_orders"
	pubName := "pgcdc_wal_reconnect"
	slotName := "pgcdc_wal_reconn_slot"

	createTable(t, connStr, table)
	createPublication(t, connStr, pubName, table)
	t.Cleanup(func() { dropReplicationSlot(t, connStr, slotName) })

	capture := newLineCapture()
	startWALPipelineWithSlot(t, connStr, pubName, slotName, stdout.New(capture, testLogger()))

	// Wait for replication slot setup.
	waitForWALDetector(t, connStr, table, capture)

	t.Run("happy path", func(t *testing.T) {
		insertRow(t, connStr, table, map[string]any{"key": "value"})

		line := capture.waitLine(t, 10*time.Second)

		var ev event.Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("invalid JSON: %v\nraw: %s", err, line)
		}
		if ev.Operation != "INSERT" {
			t.Errorf("operation = %q, want INSERT", ev.Operation)
		}
		if ev.Source != "wal_replication" {
			t.Errorf("source = %q, want wal_replication", ev.Source)
		}
	})

	t.Run("events resume after WAL reconnect", func(t *testing.T) {
		terminateSlotBackend(t, connStr, slotName)
		capture.drain()

		deadline := time.After(30 * time.Second)
		for {
			insertRow(t, connStr, "wal_reconnect_orders", map[string]any{"after": "reconnect"})

			select {
			case line := <-capture.lines:
				var ev event.Event
				if err := json.Unmarshal([]byte(line), &ev); err != nil {
					t.Fatalf("invalid JSON after reconnect: %v", err)
				}
				if ev.Operation != "INSERT" {
					t.Errorf("operation = %q, want INSERT", ev.Operation)
				}
				return
			case <-time.After(2 * time.Second):
				// Not reconnected yet, retry.
			case <-deadline:
				t.Fatal("WAL detector did not reconnect within 30s")
			}
		}
	})
}
