package cel

import (
	"encoding/json"
	"testing"

	"github.com/florinutz/pgcdc/event"
)

func TestCompile_ValidExpression(t *testing.T) {
	prg, err := Compile("operation == 'INSERT'")
	if err != nil {
		t.Fatalf("unexpected compile error: %v", err)
	}
	if prg == nil {
		t.Fatal("expected non-nil program")
	}
}

func TestCompile_InvalidExpression(t *testing.T) {
	_, err := Compile("operation ==== 'INSERT'")
	if err == nil {
		t.Fatal("expected error for invalid expression")
	}
}

func TestCompile_NonBoolResult(t *testing.T) {
	_, err := Compile("channel")
	if err == nil {
		t.Fatal("expected error for non-bool expression")
	}
}

func TestEval_OperationFilter(t *testing.T) {
	prg, err := Compile("operation == 'INSERT'")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	tests := []struct {
		name string
		op   string
		want bool
	}{
		{"match", "INSERT", true},
		{"no match", "DELETE", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev := event.Event{
				ID:        "test-id",
				Channel:   "pgcdc:orders",
				Operation: tt.op,
				Source:    "test",
			}
			got, err := prg.Eval(ev)
			if err != nil {
				t.Fatalf("eval: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEval_ChannelFilter(t *testing.T) {
	prg, err := Compile("channel == 'pgcdc:orders'")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	tests := []struct {
		name    string
		channel string
		want    bool
	}{
		{"match", "pgcdc:orders", true},
		{"no match", "pgcdc:users", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev := event.Event{
				ID:        "test-id",
				Channel:   tt.channel,
				Operation: "INSERT",
				Source:    "test",
			}
			got, err := prg.Eval(ev)
			if err != nil {
				t.Fatalf("eval: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEval_ComplexExpression(t *testing.T) {
	prg, err := Compile("operation == 'INSERT' && channel == 'pgcdc:orders'")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	tests := []struct {
		name    string
		op      string
		channel string
		want    bool
	}{
		{"both match", "INSERT", "pgcdc:orders", true},
		{"op mismatch", "DELETE", "pgcdc:orders", false},
		{"channel mismatch", "INSERT", "pgcdc:users", false},
		{"both mismatch", "DELETE", "pgcdc:users", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev := event.Event{
				ID:        "test-id",
				Channel:   tt.channel,
				Operation: tt.op,
				Source:    "test",
			}
			got, err := prg.Eval(ev)
			if err != nil {
				t.Fatalf("eval: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEval_TableMetadata(t *testing.T) {
	prg, err := Compile("table == 'orders'")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	// Event with record metadata containing table name.
	payload := json.RawMessage(`{"op":"INSERT","table":"orders","row":{"id":1},"old":null}`)
	ev := event.Event{
		ID:        "test-id",
		Channel:   "pgcdc:orders",
		Operation: "INSERT",
		Payload:   payload,
		Source:    "test",
	}

	got, err := prg.Eval(ev)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if !got {
		t.Error("expected true for table == 'orders'")
	}

	// Event without matching table.
	payload2 := json.RawMessage(`{"op":"INSERT","table":"users","row":{"id":1},"old":null}`)
	ev2 := event.Event{
		ID:        "test-id-2",
		Channel:   "pgcdc:users",
		Operation: "INSERT",
		Payload:   payload2,
		Source:    "test",
	}

	got2, err := prg.Eval(ev2)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if got2 {
		t.Error("expected false for table == 'orders' when table is 'users'")
	}
}

func TestEval_SchemaMetadata(t *testing.T) {
	prg, err := Compile("schema_name == 'public'")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	payload := json.RawMessage(`{"op":"INSERT","table":"orders","schema":"public","row":{"id":1},"old":null}`)
	ev := event.Event{
		ID:        "test-id",
		Channel:   "pgcdc:orders",
		Operation: "INSERT",
		Payload:   payload,
		Source:    "test",
	}

	got, err := prg.Eval(ev)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if !got {
		t.Error("expected true for schema_name == 'public'")
	}
}

func TestEval_HasRecord(t *testing.T) {
	prg, err := Compile("has_record")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	// Event without any payload -> no record.
	ev := event.Event{
		ID:        "test-id",
		Channel:   "pgcdc:test",
		Operation: "INSERT",
		Source:    "test",
	}

	got, err := prg.Eval(ev)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if got {
		t.Error("expected false for has_record on event without payload")
	}
}

func TestEval_EmptyTableWhenNoRecord(t *testing.T) {
	prg, err := Compile("table == ''")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	// Event without payload -> table should be empty string.
	ev := event.Event{
		ID:        "test-id",
		Channel:   "pgcdc:test",
		Operation: "INSERT",
		Source:    "test",
	}

	got, err := prg.Eval(ev)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if !got {
		t.Error("expected true for table == '' on event without record")
	}
}

func TestEval_SourceAndID(t *testing.T) {
	prg, err := Compile("source == 'wal' && id == 'evt-123'")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ev := event.Event{
		ID:        "evt-123",
		Channel:   "pgcdc:test",
		Operation: "INSERT",
		Source:    "wal",
	}

	got, err := prg.Eval(ev)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if !got {
		t.Error("expected true")
	}
}

func TestEval_StringContains(t *testing.T) {
	prg, err := Compile("channel.contains('orders')")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ev := event.Event{
		ID:        "test-id",
		Channel:   "pgcdc:orders",
		Operation: "INSERT",
		Source:    "test",
	}

	got, err := prg.Eval(ev)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if !got {
		t.Error("expected true for channel containing 'orders'")
	}
}

func TestEval_OperationIn(t *testing.T) {
	prg, err := Compile("operation in ['INSERT', 'UPDATE']")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	tests := []struct {
		name string
		op   string
		want bool
	}{
		{"INSERT matches", "INSERT", true},
		{"UPDATE matches", "UPDATE", true},
		{"DELETE no match", "DELETE", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev := event.Event{
				ID:        "test-id",
				Channel:   "pgcdc:test",
				Operation: tt.op,
				Source:    "test",
			}
			got, err := prg.Eval(ev)
			if err != nil {
				t.Fatalf("eval: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}
