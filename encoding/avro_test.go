package encoding

import (
	"encoding/json"
	"testing"

	"github.com/florinutz/pgcdc/event"
)

func TestAvroEncoder_WithOptions(t *testing.T) {
	enc := NewAvroEncoder(nil, WithNamespace("custom.ns"))
	if enc.namespace != "custom.ns" {
		t.Errorf("namespace = %q, want custom.ns", enc.namespace)
	}
	if enc.ContentType() != "application/avro" {
		t.Errorf("ContentType = %q", enc.ContentType())
	}
	if err := enc.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestCoerceDebeziumForAvro(t *testing.T) {
	columns := []ColumnInfo{{Name: "id", TypeName: "int4"}}

	t.Run("insert", func(t *testing.T) {
		payload := map[string]any{
			"before": nil,
			"after":  map[string]any{"id": float64(1)},
			"op":     "c",
			"ts_ms":  float64(1700000000000),
			"source": map[string]any{"version": "2.0", "connector": "pgcdc", "name": "test", "ts_ms": float64(100), "db": "mydb", "schema": "public", "table": "users"},
		}
		result := coerceDebeziumForAvro(payload, columns)
		if result["before"] != nil {
			t.Errorf("before = %v, want nil", result["before"])
		}
		after, ok := result["after"].(map[string]any)
		if !ok {
			t.Fatalf("after type = %T, want map", result["after"])
		}
		if after["id"] != 1 {
			t.Errorf("after.id = %v, want 1", after["id"])
		}
		if result["op"] != "c" {
			t.Errorf("op = %v, want c", result["op"])
		}
		if result["ts_ms"] != int64(1700000000000) {
			t.Errorf("ts_ms = %v, want 1700000000000", result["ts_ms"])
		}
		src, ok := result["source"].(map[string]any)
		if !ok {
			t.Fatalf("source type = %T, want map", result["source"])
		}
		if src["version"] != "2.0" {
			t.Errorf("source.version = %v", src["version"])
		}
	})

	t.Run("update_with_transaction", func(t *testing.T) {
		payload := map[string]any{
			"before": map[string]any{"id": float64(1)},
			"after":  map[string]any{"id": float64(2)},
			"op":     "u",
			"ts_ms":  float64(100),
			"source": map[string]any{},
			"transaction": map[string]any{
				"id":                    "tx-1",
				"total_order":           float64(5),
				"data_collection_order": float64(2),
			},
		}
		result := coerceDebeziumForAvro(payload, columns)
		before, ok := result["before"].(map[string]any)
		if !ok {
			t.Fatalf("before type = %T", result["before"])
		}
		if before["id"] != 1 {
			t.Errorf("before.id = %v, want 1", before["id"])
		}
		tx, ok := result["transaction"].(map[string]any)
		if !ok {
			t.Fatalf("transaction type = %T", result["transaction"])
		}
		if tx["id"] != "tx-1" {
			t.Errorf("transaction.id = %v", tx["id"])
		}
		if tx["total_order"] != int64(5) {
			t.Errorf("transaction.total_order = %v", tx["total_order"])
		}
	})

	t.Run("no_transaction", func(t *testing.T) {
		payload := map[string]any{
			"before": nil,
			"after":  nil,
			"op":     "d",
			"source": map[string]any{},
		}
		result := coerceDebeziumForAvro(payload, columns)
		if result["transaction"] != nil {
			t.Errorf("transaction = %v, want nil", result["transaction"])
		}
		if result["ts_ms"] != int64(0) {
			t.Errorf("ts_ms = %v, want 0 (default)", result["ts_ms"])
		}
	})

	t.Run("non_map_before_after", func(t *testing.T) {
		payload := map[string]any{
			"before": "not-a-map",
			"after":  "not-a-map",
			"op":     "r",
			"source": "not-a-map",
		}
		result := coerceDebeziumForAvro(payload, columns)
		if result["before"] != nil {
			t.Errorf("before = %v, want nil", result["before"])
		}
		if result["after"] != nil {
			t.Errorf("after = %v, want nil", result["after"])
		}
		// source should get default values.
		src, ok := result["source"].(map[string]any)
		if !ok {
			t.Fatalf("source type = %T", result["source"])
		}
		if src["version"] != "" {
			t.Errorf("source.version = %v, want empty", src["version"])
		}
	})
}

func TestAvroEncoder_DebeziumNoColumns(t *testing.T) {
	enc := NewAvroEncoder(nil)
	ev := event.Event{ID: "ev-1", Channel: "pgcdc:orders"}

	// Debezium envelope without column metadata → opaque fallback.
	payload := map[string]any{
		"before": nil,
		"after":  map[string]any{"id": float64(1)},
		"op":     "c",
		"source": map[string]any{},
	}
	data, _ := json.Marshal(payload)

	encoded, err := enc.Encode(ev, data)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) == 0 {
		t.Error("expected opaque encoding fallback")
	}
}

func TestAvroEncoder_NonJSON(t *testing.T) {
	enc := NewAvroEncoder(nil)
	ev := event.Event{ID: "ev-1", Channel: "pgcdc:ch"}

	// Non-JSON data → opaque mode.
	encoded, err := enc.Encode(ev, []byte("not json"))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) == 0 {
		t.Error("expected opaque encoding for non-JSON")
	}
}

func TestCoerceValue(t *testing.T) {
	tests := []struct {
		name   string
		val    any
		pgType string
		check  func(t *testing.T, got any)
	}{
		{"bool_true", true, "bool", func(t *testing.T, got any) {
			if got != true {
				t.Errorf("got %v", got)
			}
		}},
		{"int2_float64", float64(42), "int2", func(t *testing.T, got any) {
			if got != 42 {
				t.Errorf("got %v", got)
			}
		}},
		{"int4_json_number", json.Number("99"), "int4", func(t *testing.T, got any) {
			if got != 99 {
				t.Errorf("got %v", got)
			}
		}},
		{"int8_int64", int64(123456), "int8", func(t *testing.T, got any) {
			if got != int64(123456) {
				t.Errorf("got %v", got)
			}
		}},
		{"int8_int", int(77), "int8", func(t *testing.T, got any) {
			if got != int64(77) {
				t.Errorf("got %v", got)
			}
		}},
		{"float4", float64(3.14), "float4", func(t *testing.T, got any) {
			if got != float32(3.14) {
				t.Errorf("got %v", got)
			}
		}},
		{"float4_from_float32", float32(2.5), "float4", func(t *testing.T, got any) {
			if got != float32(2.5) {
				t.Errorf("got %v", got)
			}
		}},
		{"float8", float64(2.718), "float8", func(t *testing.T, got any) {
			if got != float64(2.718) {
				t.Errorf("got %v", got)
			}
		}},
		{"float8_from_float32", float32(1.5), "float8", func(t *testing.T, got any) {
			if got != float64(1.5) {
				t.Errorf("got %v", got)
			}
		}},
		{"bytea", "\\x0102", "bytea", func(t *testing.T, got any) {
			b, ok := got.([]byte)
			if !ok {
				t.Fatalf("type = %T, want []byte", got)
			}
			if string(b) != "\\x0102" {
				t.Errorf("got %q", b)
			}
		}},
		{"text_string", "hello", "text", func(t *testing.T, got any) {
			if got != "hello" {
				t.Errorf("got %v", got)
			}
		}},
		{"text_number", float64(42), "text", func(t *testing.T, got any) {
			if got != "42" {
				t.Errorf("got %v", got)
			}
		}},
		{"int_default", "not_a_number", "int4", func(t *testing.T, got any) {
			if got != 0 {
				t.Errorf("got %v, want 0 (default)", got)
			}
		}},
		{"float4_default", "not_a_number", "float4", func(t *testing.T, got any) {
			if got != float32(0) {
				t.Errorf("got %v", got)
			}
		}},
		{"float8_default", "not_a_number", "float8", func(t *testing.T, got any) {
			if got != float64(0) {
				t.Errorf("got %v", got)
			}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := coerceValue(tt.val, tt.pgType)
			tt.check(t, got)
		})
	}
}

func TestCoerceToLong(t *testing.T) {
	tests := []struct {
		name string
		val  any
		want int64
	}{
		{"float64", float64(42), 42},
		{"int", int(99), 99},
		{"int64", int64(100), 100},
		{"json_number", json.Number("77"), 77},
		{"string_default", "bad", 0},
		{"nil_default", nil, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := coerceToLong(tt.val); got != tt.want {
				t.Errorf("coerceToLong(%v) = %d, want %d", tt.val, got, tt.want)
			}
		})
	}
}

func TestCoerceToInt(t *testing.T) {
	tests := []struct {
		name string
		val  any
		want int
	}{
		{"float64", float64(42), 42},
		{"int", int(99), 99},
		{"int64", int64(100), 100},
		{"json_number", json.Number("77"), 77},
		{"default", "bad", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := coerceToInt(tt.val); got != tt.want {
				t.Errorf("coerceToInt(%v) = %d, want %d", tt.val, got, tt.want)
			}
		})
	}
}

func TestIsDebeziumEnvelope(t *testing.T) {
	if !isDebeziumEnvelope(map[string]any{"op": "c", "source": map[string]any{}}) {
		t.Error("should detect debezium")
	}
	if isDebeziumEnvelope(map[string]any{"id": 1}) {
		t.Error("should not detect non-debezium")
	}
}

func TestExtractColumnsFromDebezium(t *testing.T) {
	// Top-level columns.
	payload := map[string]any{
		"op":     "c",
		"source": map[string]any{},
		"columns": []any{
			map[string]any{"name": "id", "type_oid": float64(23), "type_name": "int4"},
		},
	}
	cols := extractColumnsFromDebezium(payload)
	if len(cols) != 1 || cols[0].Name != "id" {
		t.Errorf("extractColumnsFromDebezium = %v", cols)
	}

	// Source-level columns.
	payload2 := map[string]any{
		"op": "c",
		"source": map[string]any{
			"columns": []any{
				map[string]any{"name": "name", "type_oid": float64(25), "type_name": "text"},
			},
		},
	}
	cols2 := extractColumnsFromDebezium(payload2)
	if len(cols2) != 1 || cols2[0].Name != "name" {
		t.Errorf("extractColumnsFromDebezium (source) = %v", cols2)
	}

	// No columns.
	cols3 := extractColumnsFromDebezium(map[string]any{"op": "c", "source": map[string]any{}})
	if len(cols3) != 0 {
		t.Errorf("expected no columns, got %v", cols3)
	}
}

func TestTopicForEvent(t *testing.T) {
	ev := event.Event{Channel: "pgcdc:orders"}
	if got := topicForEvent(ev); got != "pgcdc.orders" {
		t.Errorf("topicForEvent = %q, want pgcdc.orders", got)
	}
}

func TestStringOr(t *testing.T) {
	m := map[string]any{"key": "val", "num": float64(42)}
	if got := stringOr(m, "key", "def"); got != "val" {
		t.Errorf("got %q, want val", got)
	}
	if got := stringOr(m, "num", "def"); got != "42" {
		t.Errorf("got %q, want 42", got)
	}
	if got := stringOr(m, "missing", "def"); got != "def" {
		t.Errorf("got %q, want def", got)
	}
}

func TestExtractTableInfo(t *testing.T) {
	tests := []struct {
		channel    string
		wantTable  string
		wantSchema string
	}{
		{"pgcdc:public.orders", "orders", "public"},
		{"pgcdc:orders", "orders", ""},
		{"orders", "orders", ""},
	}
	for _, tt := range tests {
		ev := event.Event{Channel: tt.channel}
		table, schema := extractTableInfo(ev)
		if table != tt.wantTable || schema != tt.wantSchema {
			t.Errorf("extractTableInfo(%q) = (%q, %q), want (%q, %q)", tt.channel, table, schema, tt.wantTable, tt.wantSchema)
		}
	}
}

func TestExtractRowData(t *testing.T) {
	payload := map[string]any{"id": 1, "name": "test", "columns": []any{}, "schema": "public", "table": "users"}
	row := extractRowData(payload)
	if _, ok := row["columns"]; ok {
		t.Error("columns should be stripped")
	}
	if _, ok := row["schema"]; ok {
		t.Error("schema should be stripped")
	}
	if row["id"] != 1 {
		t.Error("id should be preserved")
	}
}

func TestCoerceForAvro_NilValue(t *testing.T) {
	columns := []ColumnInfo{{Name: "id", TypeName: "int4"}, {Name: "name", TypeName: "text"}}
	data := map[string]any{"id": float64(1)}
	result := coerceForAvro(data, columns)
	if result["name"] != nil {
		t.Errorf("missing field should be nil, got %v", result["name"])
	}
	if result["id"] != 1 {
		t.Errorf("id = %v, want 1", result["id"])
	}
}

func TestProtobufEncoder_Close(t *testing.T) {
	enc := NewProtobufEncoder(nil, nil)
	if err := enc.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestProtobufEncoder_NoTransaction(t *testing.T) {
	enc := NewProtobufEncoder(nil, nil)
	ev := event.Event{
		ID:        "ev-1",
		Channel:   "pgcdc:orders",
		Operation: "INSERT",
		Payload:   json.RawMessage(`{"id":1}`),
		Source:    "wal",
	}
	encoded, err := enc.Encode(ev, ev.Payload)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) == 0 {
		t.Error("encoded should not be empty")
	}
}

func TestJSONEncoder_Close(t *testing.T) {
	enc := JSONEncoder{}
	if err := enc.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}
