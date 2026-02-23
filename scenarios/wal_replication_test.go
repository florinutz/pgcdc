//go:build integration

package scenarios

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/florinutz/pgpipe/adapter/stdout"
	"github.com/florinutz/pgpipe/event"
)

func TestScenario_WALReplication(t *testing.T) {
	connStr := startPostgres(t)

	// Create table and publication for WAL replication.
	createTable(t, connStr, "wal_orders")
	createPublication(t, connStr, "pgpipe_wal_orders", "wal_orders")

	capture := newLineCapture()
	startWALPipeline(t, connStr, "pgpipe_wal_orders", stdout.New(capture, testLogger()))

	// Wait for replication slot setup and initial sync.
	time.Sleep(3 * time.Second)

	t.Run("happy path", func(t *testing.T) {
		// INSERT a row directly — no trigger needed.
		insertRow(t, connStr, "wal_orders", map[string]any{"key": "value"})

		line := capture.waitLine(t, 10*time.Second)

		var ev event.Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("invalid JSON output: %v\nraw: %s", err, line)
		}
		if ev.Channel != "pgpipe:wal_orders" {
			t.Errorf("channel = %q, want %q", ev.Channel, "pgpipe:wal_orders")
		}
		if ev.Operation != "INSERT" {
			t.Errorf("operation = %q, want %q", ev.Operation, "INSERT")
		}
		if ev.Source != "wal_replication" {
			t.Errorf("source = %q, want %q", ev.Source, "wal_replication")
		}
		if ev.ID == "" {
			t.Error("event ID is empty")
		}

		// Verify payload structure matches LISTEN/NOTIFY format.
		var payload map[string]any
		if err := json.Unmarshal(ev.Payload, &payload); err != nil {
			t.Fatalf("invalid payload JSON: %v", err)
		}
		if payload["op"] != "INSERT" {
			t.Errorf("payload.op = %v, want INSERT", payload["op"])
		}
		if payload["table"] != "wal_orders" {
			t.Errorf("payload.table = %v, want wal_orders", payload["table"])
		}
		if payload["row"] == nil {
			t.Error("payload.row is nil, expected row data")
		}
	})

	t.Run("update and delete captured", func(t *testing.T) {
		capture.drain()

		// INSERT a row we can update/delete.
		insertRow(t, connStr, "wal_orders", map[string]any{"status": "pending"})
		insertLine := capture.waitLine(t, 10*time.Second)

		var insertEv event.Event
		if err := json.Unmarshal([]byte(insertLine), &insertEv); err != nil {
			t.Fatalf("invalid JSON for insert: %v", err)
		}

		// Get the row ID from the INSERT event payload.
		var insertPayload map[string]any
		json.Unmarshal(insertEv.Payload, &insertPayload)
		row := insertPayload["row"].(map[string]any)
		// The id comes as a string from WAL text format; parse it.
		idStr, ok := row["id"].(string)
		if !ok {
			t.Fatalf("row id is not a string: %T", row["id"])
		}
		var rowID int
		for _, c := range idStr {
			rowID = rowID*10 + int(c-'0')
		}

		// UPDATE the row.
		updateRow(t, connStr, "wal_orders", rowID, map[string]any{"status": "shipped"})
		updateLine := capture.waitLine(t, 10*time.Second)

		var updateEv event.Event
		if err := json.Unmarshal([]byte(updateLine), &updateEv); err != nil {
			t.Fatalf("invalid JSON for update: %v", err)
		}
		if updateEv.Operation != "UPDATE" {
			t.Errorf("update operation = %q, want UPDATE", updateEv.Operation)
		}

		var updatePayload map[string]any
		json.Unmarshal(updateEv.Payload, &updatePayload)
		if updatePayload["old"] == nil {
			t.Error("update payload.old is nil, expected old row data")
		}

		// DELETE the row.
		deleteRow(t, connStr, "wal_orders", rowID)
		deleteLine := capture.waitLine(t, 10*time.Second)

		var deleteEv event.Event
		if err := json.Unmarshal([]byte(deleteLine), &deleteEv); err != nil {
			t.Fatalf("invalid JSON for delete: %v", err)
		}
		if deleteEv.Operation != "DELETE" {
			t.Errorf("delete operation = %q, want DELETE", deleteEv.Operation)
		}

		var deletePayload map[string]any
		json.Unmarshal(deleteEv.Payload, &deletePayload)
		if deletePayload["row"] == nil {
			t.Error("delete payload.row is nil, expected old row data")
		}
	})
}
