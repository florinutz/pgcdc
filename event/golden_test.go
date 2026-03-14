package event_test

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/event"
)

var update = flag.Bool("update", false, "update golden files")

// fixedTime is a deterministic timestamp for golden tests.
var fixedTime = time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)

func TestEventJSON_Golden(t *testing.T) {
	t.Run("insert", func(t *testing.T) {
		ev := event.Event{
			ID:        "019576a0-0000-7000-8000-000000000001",
			Channel:   "pgcdc:orders",
			Operation: "INSERT",
			Payload:   json.RawMessage(`{"op":"INSERT","table":"orders","row":{"id":1,"status":"new"},"old":null}`),
			Source:    "wal_replication",
			CreatedAt: fixedTime,
		}
		got, err := json.MarshalIndent(ev, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		checkGolden(t, got)
	})

	t.Run("update", func(t *testing.T) {
		ev := event.Event{
			ID:        "019576a0-0000-7000-8000-000000000002",
			Channel:   "pgcdc:orders",
			Operation: "UPDATE",
			Payload:   json.RawMessage(`{"op":"UPDATE","table":"orders","row":{"id":1,"status":"shipped"},"old":{"id":1,"status":"new"}}`),
			Source:    "wal_replication",
			CreatedAt: fixedTime,
		}
		got, err := json.MarshalIndent(ev, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		checkGolden(t, got)
	})

	t.Run("delete", func(t *testing.T) {
		ev := event.Event{
			ID:        "019576a0-0000-7000-8000-000000000003",
			Channel:   "pgcdc:orders",
			Operation: "DELETE",
			Payload:   json.RawMessage(`{"op":"DELETE","table":"orders","row":{"id":1},"old":null}`),
			Source:    "wal_replication",
			CreatedAt: fixedTime,
		}
		got, err := json.MarshalIndent(ev, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		checkGolden(t, got)
	})

	t.Run("with_transaction", func(t *testing.T) {
		ev := event.Event{
			ID:        "019576a0-0000-7000-8000-000000000004",
			Channel:   "pgcdc:orders",
			Operation: "INSERT",
			Payload:   json.RawMessage(`{"op":"INSERT","table":"orders","row":{"id":1},"old":null}`),
			Source:    "wal_replication",
			CreatedAt: fixedTime,
			Transaction: &event.TransactionInfo{
				Xid:        12345,
				CommitTime: fixedTime,
				Seq:        1,
			},
		}
		got, err := json.MarshalIndent(ev, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		checkGolden(t, got)
	})

	t.Run("from_record", func(t *testing.T) {
		r := &event.Record{
			Position:  event.NewPosition(5000),
			Operation: event.OperationCreate,
			Metadata:  event.Metadata{event.MetaTable: "users"},
			Change: event.Change{
				After: event.NewStructuredDataFromMap(map[string]any{
					"id":   float64(42),
					"name": "alice",
				}),
			},
		}
		ev := event.Event{
			ID:        "019576a0-0000-7000-8000-000000000005",
			Channel:   "pgcdc:users",
			Operation: "INSERT",
			Source:    "wal_replication",
			CreatedAt: fixedTime,
		}
		ev.SetRecord(r)

		got, err := json.MarshalIndent(ev, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		checkGolden(t, got)
	})
}

func checkGolden(t *testing.T, got []byte) {
	t.Helper()
	golden := filepath.Join("testdata", "golden", t.Name()+".json")
	if *update {
		if err := os.MkdirAll(filepath.Dir(golden), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(golden, got, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("golden file missing; run with -update to create: %v", err)
	}
	if !bytes.Equal(want, got) {
		t.Errorf("event JSON changed.\nWant:\n%s\n\nGot:\n%s", want, got)
	}
}
