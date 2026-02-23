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

	t.Run("happy path", func(t *testing.T) {
		createTable(t, connStr, "wal_orders")
		createPublication(t, connStr, "pgpipe_wal_orders", "wal_orders")

		capture := newLineCapture()
		startWALPipeline(t, connStr, "pgpipe_wal_orders", stdout.New(capture, testLogger()))
		time.Sleep(3 * time.Second)

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

	t.Run("no transaction metadata by default", func(t *testing.T) {
		createTable(t, connStr, "wal_notx_orders")
		createPublication(t, connStr, "pgpipe_wal_notx_orders", "wal_notx_orders")

		capture := newLineCapture()
		startWALPipeline(t, connStr, "pgpipe_wal_notx_orders", stdout.New(capture, testLogger()))
		time.Sleep(3 * time.Second)

		insertRow(t, connStr, "wal_notx_orders", map[string]any{"check": "no_tx"})
		line := capture.waitLine(t, 10*time.Second)

		var ev event.Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("invalid JSON: %v", err)
		}
		if ev.Transaction != nil {
			t.Errorf("expected nil transaction metadata, got %+v", ev.Transaction)
		}

		var raw map[string]json.RawMessage
		json.Unmarshal([]byte(line), &raw)
		if _, ok := raw["transaction"]; ok {
			t.Error("expected 'transaction' key absent from JSON, but it was present")
		}
	})

	t.Run("update and delete captured", func(t *testing.T) {
		createTable(t, connStr, "wal_ud_orders")
		createPublication(t, connStr, "pgpipe_wal_ud_orders", "wal_ud_orders")

		capture := newLineCapture()
		startWALPipeline(t, connStr, "pgpipe_wal_ud_orders", stdout.New(capture, testLogger()))
		time.Sleep(3 * time.Second)

		insertRow(t, connStr, "wal_ud_orders", map[string]any{"status": "pending"})
		insertLine := capture.waitLine(t, 10*time.Second)

		var insertEv event.Event
		if err := json.Unmarshal([]byte(insertLine), &insertEv); err != nil {
			t.Fatalf("invalid JSON for insert: %v", err)
		}

		var insertPayload map[string]any
		json.Unmarshal(insertEv.Payload, &insertPayload)
		row := insertPayload["row"].(map[string]any)
		idStr, ok := row["id"].(string)
		if !ok {
			t.Fatalf("row id is not a string: %T", row["id"])
		}
		var rowID int
		for _, c := range idStr {
			rowID = rowID*10 + int(c-'0')
		}

		updateRow(t, connStr, "wal_ud_orders", rowID, map[string]any{"status": "shipped"})
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

		deleteRow(t, connStr, "wal_ud_orders", rowID)
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

	t.Run("transaction metadata present", func(t *testing.T) {
		createTable(t, connStr, "wal_tx_orders")
		createPublication(t, connStr, "pgpipe_wal_tx_orders", "wal_tx_orders")

		capture := newLineCapture()
		startWALPipelineWithTxMetadata(t, connStr, "pgpipe_wal_tx_orders", stdout.New(capture, testLogger()))
		time.Sleep(3 * time.Second)

		insertRowsInTx(t, connStr, "wal_tx_orders", []map[string]any{
			{"item": "alpha"},
			{"item": "beta"},
		})

		line1 := capture.waitLine(t, 10*time.Second)
		line2 := capture.waitLine(t, 10*time.Second)

		var ev1, ev2 event.Event
		if err := json.Unmarshal([]byte(line1), &ev1); err != nil {
			t.Fatalf("invalid JSON for event 1: %v", err)
		}
		if err := json.Unmarshal([]byte(line2), &ev2); err != nil {
			t.Fatalf("invalid JSON for event 2: %v", err)
		}

		if ev1.Transaction == nil {
			t.Fatal("event 1 missing transaction metadata")
		}
		if ev2.Transaction == nil {
			t.Fatal("event 2 missing transaction metadata")
		}

		if ev1.Transaction.Xid != ev2.Transaction.Xid {
			t.Errorf("xid mismatch: event1=%d, event2=%d", ev1.Transaction.Xid, ev2.Transaction.Xid)
		}
		if !ev1.Transaction.CommitTime.Equal(ev2.Transaction.CommitTime) {
			t.Errorf("commit_time mismatch: event1=%v, event2=%v",
				ev1.Transaction.CommitTime, ev2.Transaction.CommitTime)
		}

		if ev1.Transaction.Seq != 1 {
			t.Errorf("event 1 seq = %d, want 1", ev1.Transaction.Seq)
		}
		if ev2.Transaction.Seq != 2 {
			t.Errorf("event 2 seq = %d, want 2", ev2.Transaction.Seq)
		}

		if ev1.Transaction.Xid == 0 {
			t.Error("transaction xid is 0")
		}

		if time.Since(ev1.Transaction.CommitTime) > time.Minute {
			t.Errorf("commit_time too old: %v", ev1.Transaction.CommitTime)
		}
	})

	t.Run("begin and commit markers wrap DML events", func(t *testing.T) {
		createTable(t, connStr, "wal_marker_orders")
		createPublication(t, connStr, "pgpipe_wal_marker_orders", "wal_marker_orders")

		capture := newLineCapture()
		startWALPipelineWithTxMarkers(t, connStr, "pgpipe_wal_marker_orders", stdout.New(capture, testLogger()))
		time.Sleep(3 * time.Second)

		insertRowsInTx(t, connStr, "wal_marker_orders", []map[string]any{
			{"item": "alpha"},
			{"item": "beta"},
		})

		line1 := capture.waitLine(t, 10*time.Second)
		line2 := capture.waitLine(t, 10*time.Second)
		line3 := capture.waitLine(t, 10*time.Second)
		line4 := capture.waitLine(t, 10*time.Second)

		var ev1, ev2, ev3, ev4 event.Event
		if err := json.Unmarshal([]byte(line1), &ev1); err != nil {
			t.Fatalf("invalid JSON for event 1: %v", err)
		}
		if err := json.Unmarshal([]byte(line2), &ev2); err != nil {
			t.Fatalf("invalid JSON for event 2: %v", err)
		}
		if err := json.Unmarshal([]byte(line3), &ev3); err != nil {
			t.Fatalf("invalid JSON for event 3: %v", err)
		}
		if err := json.Unmarshal([]byte(line4), &ev4); err != nil {
			t.Fatalf("invalid JSON for event 4: %v", err)
		}

		// BEGIN marker.
		if ev1.Operation != "BEGIN" {
			t.Errorf("event 1 operation = %q, want BEGIN", ev1.Operation)
		}
		if ev1.Channel != "pgpipe:_txn" {
			t.Errorf("event 1 channel = %q, want pgpipe:_txn", ev1.Channel)
		}
		if ev1.Transaction != nil {
			t.Errorf("BEGIN marker should have nil Transaction, got %+v", ev1.Transaction)
		}

		// DML events with transaction metadata.
		if ev2.Operation != "INSERT" {
			t.Errorf("event 2 operation = %q, want INSERT", ev2.Operation)
		}
		if ev2.Transaction == nil {
			t.Fatal("event 2 missing transaction metadata")
		}
		if ev2.Transaction.Seq != 1 {
			t.Errorf("event 2 seq = %d, want 1", ev2.Transaction.Seq)
		}

		if ev3.Operation != "INSERT" {
			t.Errorf("event 3 operation = %q, want INSERT", ev3.Operation)
		}
		if ev3.Transaction == nil {
			t.Fatal("event 3 missing transaction metadata")
		}
		if ev3.Transaction.Seq != 2 {
			t.Errorf("event 3 seq = %d, want 2", ev3.Transaction.Seq)
		}

		// COMMIT marker.
		if ev4.Operation != "COMMIT" {
			t.Errorf("event 4 operation = %q, want COMMIT", ev4.Operation)
		}
		if ev4.Channel != "pgpipe:_txn" {
			t.Errorf("event 4 channel = %q, want pgpipe:_txn", ev4.Channel)
		}
		if ev4.Transaction != nil {
			t.Errorf("COMMIT marker should have nil Transaction, got %+v", ev4.Transaction)
		}

		// BEGIN payload contains xid matching DML xid.
		var beginPayload map[string]any
		if err := json.Unmarshal(ev1.Payload, &beginPayload); err != nil {
			t.Fatalf("invalid BEGIN payload: %v", err)
		}
		beginXid := uint32(beginPayload["xid"].(float64))
		if beginXid != ev2.Transaction.Xid {
			t.Errorf("BEGIN xid = %d, DML xid = %d", beginXid, ev2.Transaction.Xid)
		}

		if beginPayload["commit_time"] == nil {
			t.Error("BEGIN payload missing commit_time")
		}

		// COMMIT payload contains xid and event_count.
		var commitPayload map[string]any
		if err := json.Unmarshal(ev4.Payload, &commitPayload); err != nil {
			t.Fatalf("invalid COMMIT payload: %v", err)
		}
		commitXid := uint32(commitPayload["xid"].(float64))
		if commitXid != ev2.Transaction.Xid {
			t.Errorf("COMMIT xid = %d, DML xid = %d", commitXid, ev2.Transaction.Xid)
		}
		eventCount := int(commitPayload["event_count"].(float64))
		if eventCount != 2 {
			t.Errorf("COMMIT event_count = %d, want 2", eventCount)
		}
	})
}
