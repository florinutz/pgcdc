package event

import (
	"encoding/json"
	"testing"
)

func TestOperation_String(t *testing.T) {
	tests := []struct {
		op   Operation
		want string
	}{
		{OperationCreate, "INSERT"},
		{OperationUpdate, "UPDATE"},
		{OperationDelete, "DELETE"},
		{OperationTruncate, "TRUNCATE"},
		{OperationSnapshot, "SNAPSHOT"},
		{0, ""},
	}
	for _, tt := range tests {
		if got := tt.op.String(); got != tt.want {
			t.Errorf("Operation(%d).String() = %q, want %q", tt.op, got, tt.want)
		}
	}
}

func TestParseOperation(t *testing.T) {
	tests := []struct {
		s    string
		want Operation
	}{
		{"INSERT", OperationCreate},
		{"UPDATE", OperationUpdate},
		{"DELETE", OperationDelete},
		{"TRUNCATE", OperationTruncate},
		{"SNAPSHOT", OperationSnapshot},
		{"NOTIFY", 0},
		{"BEGIN", 0},
		{"", 0},
	}
	for _, tt := range tests {
		if got := ParseOperation(tt.s); got != tt.want {
			t.Errorf("ParseOperation(%q) = %d, want %d", tt.s, got, tt.want)
		}
	}
}

func TestOperation_RoundTrip(t *testing.T) {
	ops := []Operation{OperationCreate, OperationUpdate, OperationDelete, OperationTruncate, OperationSnapshot}
	for _, op := range ops {
		if ParseOperation(op.String()) != op {
			t.Errorf("ParseOperation(%q) != %d", op.String(), op)
		}
	}
}

func TestPosition(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		p := Position{}
		if !p.IsZero() {
			t.Error("empty position should be zero")
		}
		if p.Uint64() != 0 {
			t.Error("empty position Uint64 should be 0")
		}
		if p.Raw() != nil {
			t.Error("empty position Raw should be nil")
		}
	})

	t.Run("uint64 only", func(t *testing.T) {
		p := NewPosition(42)
		if p.IsZero() {
			t.Error("should not be zero")
		}
		if p.Uint64() != 42 {
			t.Errorf("Uint64() = %d, want 42", p.Uint64())
		}
	})

	t.Run("with raw", func(t *testing.T) {
		raw := []byte("resume-token-data")
		p := NewPositionWithRaw(100, raw)
		if p.Uint64() != 100 {
			t.Errorf("Uint64() = %d, want 100", p.Uint64())
		}
		if string(p.Raw()) != "resume-token-data" {
			t.Errorf("Raw() = %q, want %q", p.Raw(), "resume-token-data")
		}
	})
}

func TestStructuredData(t *testing.T) {
	t.Run("from map", func(t *testing.T) {
		m := map[string]any{"id": float64(1), "name": "alice"}
		sd := NewStructuredDataFromMap(m)
		if sd.Len() != 2 {
			t.Errorf("Len() = %d, want 2", sd.Len())
		}
		v, ok := sd.Get("id")
		if !ok || v != float64(1) {
			t.Errorf("Get(id) = %v, %v", v, ok)
		}
		v, ok = sd.Get("name")
		if !ok || v != "alice" {
			t.Errorf("Get(name) = %v, %v", v, ok)
		}
		if sd.Has("missing") {
			t.Error("Has(missing) should be false")
		}
	})

	t.Run("nil map", func(t *testing.T) {
		sd := NewStructuredDataFromMap(nil)
		if sd != nil {
			t.Error("nil map should return nil")
		}
	})

	t.Run("set and delete", func(t *testing.T) {
		sd := NewStructuredData([]Field{
			{Name: "a", Value: 1},
			{Name: "b", Value: 2},
		})
		sd.Set("c", 3)
		if sd.Len() != 3 {
			t.Errorf("after Set, Len() = %d, want 3", sd.Len())
		}
		sd.Set("a", 10) // overwrite
		v, _ := sd.Get("a")
		if v != 10 {
			t.Errorf("after Set(a,10), Get(a) = %v", v)
		}
		if !sd.Delete("b") {
			t.Error("Delete(b) should return true")
		}
		if sd.Len() != 2 {
			t.Errorf("after Delete, Len() = %d, want 2", sd.Len())
		}
		if sd.Delete("missing") {
			t.Error("Delete(missing) should return false")
		}
	})

	t.Run("ToMap", func(t *testing.T) {
		sd := NewStructuredData([]Field{
			{Name: "x", Value: "hello"},
			{Name: "y", Value: float64(42)},
		})
		m := sd.ToMap()
		if m["x"] != "hello" || m["y"] != float64(42) {
			t.Errorf("ToMap() = %v", m)
		}
	})

	t.Run("Fields copy", func(t *testing.T) {
		sd := NewStructuredData([]Field{{Name: "a", Value: 1}})
		fields := sd.Fields()
		fields[0].Value = 999 // mutate copy
		v, _ := sd.Get("a")
		if v != 1 {
			t.Error("Fields() should return a copy")
		}
	})
}

func TestRecord_MarshalCompatPayload_INSERT(t *testing.T) {
	r := &Record{
		Operation: OperationCreate,
		Metadata:  Metadata{MetaTable: "orders"},
		Change: Change{
			After: NewStructuredDataFromMap(map[string]any{
				"id":     float64(1),
				"status": "new",
			}),
		},
	}

	payload := r.MarshalCompatPayload()

	var m map[string]any
	if err := json.Unmarshal(payload, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if m["op"] != "INSERT" {
		t.Errorf("op = %v, want INSERT", m["op"])
	}
	if m["table"] != "orders" {
		t.Errorf("table = %v, want orders", m["table"])
	}
	row, ok := m["row"].(map[string]any)
	if !ok {
		t.Fatal("row is not a map")
	}
	if row["id"] != float64(1) || row["status"] != "new" {
		t.Errorf("row = %v", row)
	}
	if m["old"] != nil {
		t.Errorf("old = %v, want nil", m["old"])
	}
}

func TestRecord_MarshalCompatPayload_UPDATE(t *testing.T) {
	r := &Record{
		Operation: OperationUpdate,
		Metadata:  Metadata{MetaTable: "orders"},
		Change: Change{
			Before: NewStructuredDataFromMap(map[string]any{"status": "new"}),
			After:  NewStructuredDataFromMap(map[string]any{"status": "shipped"}),
		},
	}

	payload := r.MarshalCompatPayload()

	var m map[string]any
	if err := json.Unmarshal(payload, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	row := m["row"].(map[string]any)
	old := m["old"].(map[string]any)
	if row["status"] != "shipped" {
		t.Errorf("row.status = %v", row["status"])
	}
	if old["status"] != "new" {
		t.Errorf("old.status = %v", old["status"])
	}
}

func TestRecord_MarshalCompatPayload_DELETE(t *testing.T) {
	r := &Record{
		Operation: OperationDelete,
		Metadata:  Metadata{MetaTable: "orders"},
		Change: Change{
			Before: NewStructuredDataFromMap(map[string]any{"id": float64(1)}),
		},
	}

	payload := r.MarshalCompatPayload()

	var m map[string]any
	if err := json.Unmarshal(payload, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if m["op"] != "DELETE" {
		t.Errorf("op = %v", m["op"])
	}
	// DELETE legacy: row = old data, old = nil
	row := m["row"].(map[string]any)
	if row["id"] != float64(1) {
		t.Errorf("row.id = %v", row["id"])
	}
	if m["old"] != nil {
		t.Errorf("old = %v, want nil", m["old"])
	}
}

func TestRecord_RoundTrip(t *testing.T) {
	// Create a legacy-format payload.
	original := map[string]any{
		"op":    "INSERT",
		"table": "users",
		"row":   map[string]any{"id": float64(1), "name": "alice"},
		"old":   nil,
	}
	originalJSON, _ := json.Marshal(original)

	// Parse into an Event.
	ev, err := New("pgcdc:users", "INSERT", originalJSON, "wal_replication")
	if err != nil {
		t.Fatal(err)
	}

	// Access Record (lazy parse).
	rec := ev.Record()
	if rec == nil {
		t.Fatal("Record() returned nil")
	}
	if rec.Operation != OperationCreate {
		t.Errorf("Operation = %v, want Create", rec.Operation)
	}
	if rec.Metadata[MetaTable] != "users" {
		t.Errorf("table = %q", rec.Metadata[MetaTable])
	}
	if rec.Change.After == nil {
		t.Fatal("After is nil")
	}
	name, ok := rec.Change.After.Get("name")
	if !ok || name != "alice" {
		t.Errorf("After.name = %v", name)
	}

	// Re-marshal via MarshalCompatPayload and verify key fields match.
	reconstituted := rec.MarshalCompatPayload()
	var recon map[string]any
	if err := json.Unmarshal(reconstituted, &recon); err != nil {
		t.Fatal(err)
	}
	if recon["op"] != "INSERT" {
		t.Errorf("reconstituted op = %v", recon["op"])
	}
	if recon["table"] != "users" {
		t.Errorf("reconstituted table = %v", recon["table"])
	}
	reconRow, ok := recon["row"].(map[string]any)
	if !ok {
		t.Fatal("reconstituted row not a map")
	}
	if reconRow["name"] != "alice" {
		t.Errorf("reconstituted row.name = %v", reconRow["name"])
	}
}

func TestRecord_RoundTrip_DELETE(t *testing.T) {
	// DELETE legacy format: row = old data, old = nil.
	original := map[string]any{
		"op":    "DELETE",
		"table": "users",
		"row":   map[string]any{"id": float64(1)},
		"old":   nil,
	}
	originalJSON, _ := json.Marshal(original)

	ev, err := New("pgcdc:users", "DELETE", originalJSON, "wal_replication")
	if err != nil {
		t.Fatal(err)
	}

	rec := ev.Record()
	if rec == nil {
		t.Fatal("Record() returned nil")
	}
	// Structured: DELETE → Before has the data, After is nil.
	if rec.Change.Before == nil {
		t.Fatal("Before is nil for DELETE")
	}
	if rec.Change.After != nil {
		t.Error("After should be nil for DELETE")
	}

	// Round-trip: MarshalCompatPayload should produce row = old data, old = nil.
	reconstituted := rec.MarshalCompatPayload()
	var recon map[string]any
	if err := json.Unmarshal(reconstituted, &recon); err != nil {
		t.Fatal(err)
	}
	row := recon["row"].(map[string]any)
	if row["id"] != float64(1) {
		t.Errorf("row.id = %v", row["id"])
	}
	if recon["old"] != nil {
		t.Errorf("old = %v", recon["old"])
	}
}

func TestEvent_NewFromRecord(t *testing.T) {
	r := &Record{
		Position:  NewPosition(12345),
		Operation: OperationCreate,
		Metadata:  Metadata{MetaTable: "orders"},
		Change: Change{
			After: NewStructuredDataFromMap(map[string]any{"id": float64(1)}),
		},
	}

	ev, err := NewFromRecord("pgcdc:orders", r, "wal_replication")
	if err != nil {
		t.Fatal(err)
	}
	if ev.Operation != "INSERT" {
		t.Errorf("Operation = %q", ev.Operation)
	}
	if ev.LSN != 12345 {
		t.Errorf("LSN = %d", ev.LSN)
	}
	if ev.Channel != "pgcdc:orders" {
		t.Errorf("Channel = %q", ev.Channel)
	}

	// Record should be directly accessible without parse.
	if ev.Record() != r {
		t.Error("Record() should return the same pointer")
	}
}

func TestEvent_MarshalJSON_WithRecord(t *testing.T) {
	r := &Record{
		Position:  NewPosition(100),
		Operation: OperationCreate,
		Metadata:  Metadata{MetaTable: "orders"},
		Change: Change{
			After: NewStructuredDataFromMap(map[string]any{
				"id":     float64(1),
				"status": "new",
			}),
		},
	}

	ev, err := NewFromRecord("pgcdc:orders", r, "wal_replication")
	if err != nil {
		t.Fatal(err)
	}

	data, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if m["operation"] != "INSERT" {
		t.Errorf("operation = %v", m["operation"])
	}
	payload, ok := m["payload"].(map[string]any)
	if !ok {
		t.Fatalf("payload is not a map: %T", m["payload"])
	}
	if payload["op"] != "INSERT" {
		t.Errorf("payload.op = %v", payload["op"])
	}
	if payload["table"] != "orders" {
		t.Errorf("payload.table = %v", payload["table"])
	}
	row, ok := payload["row"].(map[string]any)
	if !ok {
		t.Fatal("payload.row is not a map")
	}
	if row["status"] != "new" {
		t.Errorf("row.status = %v", row["status"])
	}
}

func TestEvent_EnsurePayload(t *testing.T) {
	r := &Record{
		Operation: OperationCreate,
		Metadata:  Metadata{MetaTable: "t"},
		Change: Change{
			After: NewStructuredDataFromMap(map[string]any{"a": float64(1)}),
		},
	}

	ev, err := NewFromRecord("pgcdc:t", r, "test")
	if err != nil {
		t.Fatal(err)
	}

	// Before EnsurePayload, Payload should be nil (lazy).
	if ev.Payload != nil {
		t.Error("Payload should be nil before EnsurePayload")
	}

	ev.EnsurePayload()

	if ev.Payload == nil {
		t.Fatal("Payload should be non-nil after EnsurePayload")
	}
	var m map[string]any
	if err := json.Unmarshal(ev.Payload, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if m["op"] != "INSERT" {
		t.Errorf("op = %v", m["op"])
	}
}

func TestRecord_Extras(t *testing.T) {
	r := &Record{
		Operation: OperationUpdate,
		Metadata: Metadata{
			MetaDatabase:   "mydb",
			MetaCollection: "users",
		},
		Change: Change{
			After: NewStructuredDataFromMap(map[string]any{"name": "alice"}),
		},
	}
	r.SetExtra("update_description", map[string]any{
		"updated_fields": map[string]any{"name": "alice"},
		"removed_fields": []any{},
	})

	payload := r.MarshalCompatPayload()
	var m map[string]any
	if err := json.Unmarshal(payload, &m); err != nil {
		t.Fatal(err)
	}
	if m["database"] != "mydb" {
		t.Errorf("database = %v", m["database"])
	}
	if m["collection"] != "users" {
		t.Errorf("collection = %v", m["collection"])
	}
	if _, ok := m["update_description"]; !ok {
		t.Error("update_description missing from payload")
	}
}
