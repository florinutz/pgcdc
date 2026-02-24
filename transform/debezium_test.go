package transform

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/event"
)

func TestDebezium_INSERT(t *testing.T) {
	ev := event.Event{
		ID:        "test-1",
		Channel:   "orders",
		Operation: "INSERT",
		Payload:   json.RawMessage(`{"op":"INSERT","table":"orders","row":{"id":1,"name":"alice"},"old":null}`),
		Source:    "wal",
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		LSN:       12345,
	}

	fn := Debezium()
	got, err := fn(ev)
	if err != nil {
		t.Fatal(err)
	}

	var envelope map[string]any
	if err := json.Unmarshal(got.Payload, &envelope); err != nil {
		t.Fatal(err)
	}

	if envelope["op"] != "c" {
		t.Errorf("op = %v, want c", envelope["op"])
	}
	if envelope["before"] != nil {
		t.Errorf("before = %v, want nil", envelope["before"])
	}

	after, ok := envelope["after"].(map[string]any)
	if !ok {
		t.Fatalf("after is not a map: %T", envelope["after"])
	}
	if after["name"] != "alice" {
		t.Errorf("after.name = %v, want alice", after["name"])
	}

	src := envelope["source"].(map[string]any)
	if src["lsn"] != float64(12345) {
		t.Errorf("source.lsn = %v, want 12345", src["lsn"])
	}
	if src["table"] != "orders" {
		t.Errorf("source.table = %v, want orders", src["table"])
	}

	// Event envelope fields preserved.
	if got.ID != "test-1" {
		t.Errorf("event ID = %v, want test-1", got.ID)
	}
	if got.Operation != "INSERT" {
		t.Errorf("operation = %v, want INSERT", got.Operation)
	}
}

func TestDebezium_UPDATE(t *testing.T) {
	ev := event.Event{
		Operation: "UPDATE",
		Payload:   json.RawMessage(`{"op":"UPDATE","table":"orders","row":{"id":1,"name":"bob"},"old":{"id":1,"name":"alice"}}`),
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	fn := Debezium()
	got, err := fn(ev)
	if err != nil {
		t.Fatal(err)
	}

	var envelope map[string]any
	if err := json.Unmarshal(got.Payload, &envelope); err != nil {
		t.Fatal(err)
	}

	if envelope["op"] != "u" {
		t.Errorf("op = %v, want u", envelope["op"])
	}

	before := envelope["before"].(map[string]any)
	if before["name"] != "alice" {
		t.Errorf("before.name = %v, want alice", before["name"])
	}

	after := envelope["after"].(map[string]any)
	if after["name"] != "bob" {
		t.Errorf("after.name = %v, want bob", after["name"])
	}
}

func TestDebezium_DELETE(t *testing.T) {
	// pgcdc swaps old→row for DELETE, so row has the old data.
	ev := event.Event{
		Operation: "DELETE",
		Payload:   json.RawMessage(`{"op":"DELETE","table":"orders","row":{"id":1,"name":"alice"},"old":null}`),
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	fn := Debezium()
	got, err := fn(ev)
	if err != nil {
		t.Fatal(err)
	}

	var envelope map[string]any
	if err := json.Unmarshal(got.Payload, &envelope); err != nil {
		t.Fatal(err)
	}

	if envelope["op"] != "d" {
		t.Errorf("op = %v, want d", envelope["op"])
	}

	before := envelope["before"].(map[string]any)
	if before["name"] != "alice" {
		t.Errorf("before.name = %v, want alice", before["name"])
	}

	if envelope["after"] != nil {
		t.Errorf("after = %v, want nil", envelope["after"])
	}
}

func TestDebezium_SNAPSHOT(t *testing.T) {
	ev := event.Event{
		Operation: "SNAPSHOT",
		Payload:   json.RawMessage(`{"op":"SNAPSHOT","table":"orders","row":{"id":1},"old":null}`),
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	fn := Debezium()
	got, err := fn(ev)
	if err != nil {
		t.Fatal(err)
	}

	var envelope map[string]any
	if err := json.Unmarshal(got.Payload, &envelope); err != nil {
		t.Fatal(err)
	}

	if envelope["op"] != "r" {
		t.Errorf("op = %v, want r", envelope["op"])
	}

	src := envelope["source"].(map[string]any)
	if src["snapshot"] != true {
		t.Errorf("source.snapshot = %v, want true", src["snapshot"])
	}

	if envelope["before"] != nil {
		t.Errorf("before = %v, want nil", envelope["before"])
	}
	if envelope["after"] == nil {
		t.Fatal("after is nil, want map")
	}
}

func TestDebezium_WithTransaction(t *testing.T) {
	commitTime := time.Date(2026, 2, 15, 10, 30, 0, 0, time.UTC)
	ev := event.Event{
		Operation: "INSERT",
		Payload:   json.RawMessage(`{"op":"INSERT","table":"t","row":{"id":1},"old":null}`),
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Transaction: &event.TransactionInfo{
			Xid:        42,
			CommitTime: commitTime,
			Seq:        3,
		},
		LSN: 99999,
	}

	fn := Debezium()
	got, err := fn(ev)
	if err != nil {
		t.Fatal(err)
	}

	var envelope map[string]any
	if err := json.Unmarshal(got.Payload, &envelope); err != nil {
		t.Fatal(err)
	}

	// Transaction block present.
	txn, ok := envelope["transaction"].(map[string]any)
	if !ok {
		t.Fatalf("transaction block missing or wrong type: %T", envelope["transaction"])
	}
	if txn["id"] != "42:99999" {
		t.Errorf("transaction.id = %v, want 42:99999", txn["id"])
	}
	if txn["total_order"] != float64(3) {
		t.Errorf("transaction.total_order = %v, want 3", txn["total_order"])
	}

	// Timestamp from transaction commit time.
	src := envelope["source"].(map[string]any)
	if src["ts_ms"] != float64(commitTime.UnixMilli()) {
		t.Errorf("source.ts_ms = %v, want %v", src["ts_ms"], commitTime.UnixMilli())
	}
}

func TestDebezium_WithoutTransaction(t *testing.T) {
	ev := event.Event{
		Operation: "INSERT",
		Payload:   json.RawMessage(`{"op":"INSERT","table":"t","row":{"id":1},"old":null}`),
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	fn := Debezium()
	got, err := fn(ev)
	if err != nil {
		t.Fatal(err)
	}

	var envelope map[string]any
	if err := json.Unmarshal(got.Payload, &envelope); err != nil {
		t.Fatal(err)
	}

	if _, ok := envelope["transaction"]; ok {
		t.Errorf("transaction block should be absent when Transaction is nil")
	}

	src := envelope["source"].(map[string]any)
	if src["ts_ms"] != float64(ev.CreatedAt.UnixMilli()) {
		t.Errorf("source.ts_ms = %v, want %v (CreatedAt)", src["ts_ms"], ev.CreatedAt.UnixMilli())
	}
}

func TestDebezium_Options(t *testing.T) {
	ev := event.Event{
		Operation: "INSERT",
		Payload:   json.RawMessage(`{"op":"INSERT","table":"t","row":{"id":1},"old":null}`),
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	fn := Debezium(WithConnectorName("my-source"), WithDatabase("mydb"))
	got, err := fn(ev)
	if err != nil {
		t.Fatal(err)
	}

	var envelope map[string]any
	if err := json.Unmarshal(got.Payload, &envelope); err != nil {
		t.Fatal(err)
	}

	src := envelope["source"].(map[string]any)
	if src["name"] != "my-source" {
		t.Errorf("source.name = %v, want my-source", src["name"])
	}
	if src["db"] != "mydb" {
		t.Errorf("source.db = %v, want mydb", src["db"])
	}
}

func TestDebezium_UnstructuredPayload(t *testing.T) {
	ev := event.Event{
		Operation: "NOTIFY",
		Payload:   json.RawMessage(`"hello world"`),
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	fn := Debezium()
	got, err := fn(ev)
	if err != nil {
		t.Fatal(err)
	}

	var envelope map[string]any
	if err := json.Unmarshal(got.Payload, &envelope); err != nil {
		t.Fatal(err)
	}

	if envelope["op"] != "n" {
		t.Errorf("op = %v, want n", envelope["op"])
	}
	if envelope["after"] != "hello world" {
		t.Errorf("after = %v, want hello world", envelope["after"])
	}
}

func TestDebezium_NilPayload(t *testing.T) {
	ev := event.Event{
		Operation: "INSERT",
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	fn := Debezium()
	got, err := fn(ev)
	if err != nil {
		t.Fatal(err)
	}

	var envelope map[string]any
	if err := json.Unmarshal(got.Payload, &envelope); err != nil {
		t.Fatal(err)
	}

	if envelope["before"] != nil {
		t.Errorf("before = %v, want nil", envelope["before"])
	}
	if envelope["after"] != nil {
		t.Errorf("after = %v, want nil", envelope["after"])
	}
}

func TestDebezium_MissingSchema(t *testing.T) {
	// Payload without "schema" key — should default to "public".
	ev := event.Event{
		Operation: "INSERT",
		Payload:   json.RawMessage(`{"op":"INSERT","table":"orders","row":{"id":1},"old":null}`),
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	fn := Debezium()
	got, err := fn(ev)
	if err != nil {
		t.Fatal(err)
	}

	var envelope map[string]any
	if err := json.Unmarshal(got.Payload, &envelope); err != nil {
		t.Fatal(err)
	}

	src := envelope["source"].(map[string]any)
	if src["schema"] != "public" {
		t.Errorf("source.schema = %v, want public", src["schema"])
	}
}

func TestDebezium_EmptyOperation(t *testing.T) {
	ev := event.Event{
		Operation: "",
		Payload:   json.RawMessage(`{"op":"","table":"t","row":null,"old":null}`),
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	fn := Debezium()
	got, err := fn(ev)
	if err != nil {
		t.Fatal(err)
	}

	var envelope map[string]any
	if err := json.Unmarshal(got.Payload, &envelope); err != nil {
		t.Fatal(err)
	}

	if envelope["op"] != "" {
		t.Errorf("op = %v, want empty string", envelope["op"])
	}
}

func TestDebezium_UnknownOperation(t *testing.T) {
	ev := event.Event{
		Operation: "TRUNCATE",
		Payload:   json.RawMessage(`{"op":"TRUNCATE","table":"t","row":null,"old":null}`),
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	fn := Debezium()
	got, err := fn(ev)
	if err != nil {
		t.Fatal(err)
	}

	var envelope map[string]any
	if err := json.Unmarshal(got.Payload, &envelope); err != nil {
		t.Fatal(err)
	}

	if envelope["op"] != "t" {
		t.Errorf("op = %v, want t (first char lowercase)", envelope["op"])
	}
}

func TestDebezium_LSNPropagation(t *testing.T) {
	ev := event.Event{
		Operation: "INSERT",
		Payload:   json.RawMessage(`{"op":"INSERT","table":"t","row":{"id":1},"old":null}`),
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		LSN:       777888,
	}

	fn := Debezium()
	got, err := fn(ev)
	if err != nil {
		t.Fatal(err)
	}

	// LSN preserved on event struct.
	if got.LSN != 777888 {
		t.Errorf("event.LSN = %d, want 777888", got.LSN)
	}

	var envelope map[string]any
	if err := json.Unmarshal(got.Payload, &envelope); err != nil {
		t.Fatal(err)
	}

	src := envelope["source"].(map[string]any)
	if src["lsn"] != float64(777888) {
		t.Errorf("source.lsn = %v, want 777888", src["lsn"])
	}
}

func TestDebezium_WithSchemaInPayload(t *testing.T) {
	ev := event.Event{
		Operation: "INSERT",
		Payload:   json.RawMessage(`{"op":"INSERT","table":"orders","schema":"sales","row":{"id":1},"old":null}`),
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	fn := Debezium()
	got, err := fn(ev)
	if err != nil {
		t.Fatal(err)
	}

	var envelope map[string]any
	if err := json.Unmarshal(got.Payload, &envelope); err != nil {
		t.Fatal(err)
	}

	src := envelope["source"].(map[string]any)
	if src["schema"] != "sales" {
		t.Errorf("source.schema = %v, want sales", src["schema"])
	}
}
