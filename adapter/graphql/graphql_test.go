package graphql

import (
	"encoding/json"
	"testing"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/schema"
)

func TestParseQueryFields(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		wantRoot       string
		wantSelections []string
	}{
		{
			name:           "basic subscription",
			query:          `subscription { events { id channel payload } }`,
			wantRoot:       "events",
			wantSelections: []string{"id", "channel", "payload"},
		},
		{
			name:           "named subscription",
			query:          `subscription OnChange { events { id operation } }`,
			wantRoot:       "events",
			wantSelections: []string{"id", "operation"},
		},
		{
			name:           "no selection set",
			query:          `subscription { events }`,
			wantRoot:       "events",
			wantSelections: nil,
		},
		{
			name:           "with arguments",
			query:          `subscription { events(channel: "orders") { id } }`,
			wantRoot:       "events",
			wantSelections: []string{"id"},
		},
		{
			name:     "empty query",
			query:    ``,
			wantRoot: "",
		},
		{
			name:           "shorthand query",
			query:          `{ events { id } }`,
			wantRoot:       "events",
			wantSelections: []string{"id"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root, selections := parseQueryFields(tt.query)
			if root != tt.wantRoot {
				t.Errorf("root = %q, want %q", root, tt.wantRoot)
			}
			if len(selections) != len(tt.wantSelections) {
				t.Errorf("selections = %v, want %v", selections, tt.wantSelections)
				return
			}
			for i := range selections {
				if selections[i] != tt.wantSelections[i] {
					t.Errorf("selections[%d] = %q, want %q", i, selections[i], tt.wantSelections[i])
				}
			}
		})
	}
}

func TestParseSubscription(t *testing.T) {
	payload := SubscribePayload{
		Query:     `subscription { events { id channel } }`,
		Variables: map[string]any{"channel": "orders"},
	}
	field, selections, vars := parseSubscription(payload)

	if field != "events" {
		t.Errorf("field = %q, want %q", field, "events")
	}
	if len(selections) != 2 {
		t.Errorf("selections = %v, want [id channel]", selections)
	}
	if vars["channel"] != "orders" {
		t.Errorf("vars[channel] = %v, want %q", vars["channel"], "orders")
	}
}

func TestFilterMatches(t *testing.T) {
	ev := event.Event{
		ID:        "test-1",
		Channel:   "pgcdc:orders",
		Operation: "INSERT",
		Payload:   json.RawMessage(`{"id": 1, "status": "active"}`),
	}

	tests := []struct {
		name   string
		filter *Filter
		want   bool
	}{
		{
			name:   "nil filter matches all",
			filter: nil,
			want:   true,
		},
		{
			name:   "empty filter matches all",
			filter: &Filter{},
			want:   true,
		},
		{
			name:   "matching channel",
			filter: &Filter{Channel: "pgcdc:orders"},
			want:   true,
		},
		{
			name:   "non-matching channel",
			filter: &Filter{Channel: "pgcdc:users"},
			want:   false,
		},
		{
			name:   "matching operation",
			filter: &Filter{Operation: "INSERT"},
			want:   true,
		},
		{
			name:   "non-matching operation",
			filter: &Filter{Operation: "DELETE"},
			want:   false,
		},
		{
			name:   "matching field filter",
			filter: &Filter{FieldFilters: map[string]any{"status": "active"}},
			want:   true,
		},
		{
			name:   "non-matching field filter",
			filter: &Filter{FieldFilters: map[string]any{"status": "inactive"}},
			want:   false,
		},
		{
			name:   "missing field filter",
			filter: &Filter{FieldFilters: map[string]any{"nonexistent": "value"}},
			want:   false,
		},
		{
			name: "combined filters all match",
			filter: &Filter{
				Channel:      "pgcdc:orders",
				Operation:    "INSERT",
				FieldFilters: map[string]any{"status": "active"},
			},
			want: true,
		},
		{
			name: "combined filters one fails",
			filter: &Filter{
				Channel:   "pgcdc:orders",
				Operation: "DELETE",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.filter.Matches(ev)
			if got != tt.want {
				t.Errorf("Matches() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseFilter(t *testing.T) {
	tests := []struct {
		name      string
		variables map[string]any
		want      *Filter
	}{
		{
			name:      "nil variables",
			variables: nil,
			want:      &Filter{},
		},
		{
			name:      "channel only",
			variables: map[string]any{"channel": "orders"},
			want:      &Filter{Channel: "orders"},
		},
		{
			name:      "operation only",
			variables: map[string]any{"operation": "INSERT"},
			want:      &Filter{Operation: "INSERT"},
		},
		{
			name: "with where clause",
			variables: map[string]any{
				"where": map[string]any{"status": "active"},
			},
			want: &Filter{FieldFilters: map[string]any{"status": "active"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseFilter(tt.variables)
			if got.Channel != tt.want.Channel {
				t.Errorf("Channel = %q, want %q", got.Channel, tt.want.Channel)
			}
			if got.Operation != tt.want.Operation {
				t.Errorf("Operation = %q, want %q", got.Operation, tt.want.Operation)
			}
		})
	}
}

func TestOIDToGraphQLType(t *testing.T) {
	tests := []struct {
		name string
		oid  uint32
		want string
	}{
		{"int2", 21, "Int"},
		{"int4", 23, "Int"},
		{"int8", 20, "Int"},
		{"oid", 26, "Int"},
		{"float4", 700, "Float"},
		{"float8", 701, "Float"},
		{"numeric", 1700, "Float"},
		{"bool", 16, "Boolean"},
		{"text", 25, "String"},
		{"uuid", 2950, "String"},
		{"unknown", 0, "String"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := OIDToGraphQLType(tt.oid)
			if got != tt.want {
				t.Errorf("OIDToGraphQLType(%d) = %q, want %q", tt.oid, got, tt.want)
			}
		})
	}
}

func TestGenerateSchema(t *testing.T) {
	// Test with nil store — should produce base schema.
	s := GenerateSchema(nil)
	if s == "" {
		t.Fatal("GenerateSchema(nil) returned empty string")
	}
	if !contains(s, "type Event") {
		t.Error("schema missing Event type")
	}
	if !contains(s, "type Subscription") {
		t.Error("schema missing Subscription type")
	}
	if !contains(s, "events(channel: String, operation: String): Event") {
		t.Error("schema missing events subscription field")
	}
}

func TestGenerateTypeForSubject(t *testing.T) {
	s := &schema.Schema{
		Subject: "public.orders",
		Version: 1,
		Columns: []schema.ColumnDef{
			{Name: "id", TypeOID: 23, TypeName: "int4"},
			{Name: "total", TypeOID: 1700, TypeName: "numeric"},
			{Name: "status", TypeOID: 25, TypeName: "text", Nullable: true},
		},
	}

	result := GenerateTypeForSubject(s)
	if !contains(result, "type PublicOrders") {
		t.Errorf("expected type PublicOrders, got: %s", result)
	}
	if !contains(result, "id: Int!") {
		t.Errorf("expected id: Int!, got: %s", result)
	}
	if !contains(result, "total: Float!") {
		t.Errorf("expected total: Float!, got: %s", result)
	}
	if !contains(result, "status: String") {
		t.Errorf("expected status: String, got: %s", result)
	}
}

func TestGenerateTypeForSubject_nil(t *testing.T) {
	result := GenerateTypeForSubject(nil)
	if result != "" {
		t.Errorf("expected empty string for nil schema, got: %s", result)
	}
}

func TestMessageTypes(t *testing.T) {
	// Verify message type constants match the spec.
	if MsgConnectionInit != "connection_init" {
		t.Errorf("MsgConnectionInit = %q", MsgConnectionInit)
	}
	if MsgConnectionAck != "connection_ack" {
		t.Errorf("MsgConnectionAck = %q", MsgConnectionAck)
	}
	if MsgSubscribe != "subscribe" {
		t.Errorf("MsgSubscribe = %q", MsgSubscribe)
	}
	if MsgNext != "next" {
		t.Errorf("MsgNext = %q", MsgNext)
	}
	if MsgComplete != "complete" {
		t.Errorf("MsgComplete = %q", MsgComplete)
	}
	if MsgPing != "ping" {
		t.Errorf("MsgPing = %q", MsgPing)
	}
	if MsgPong != "pong" {
		t.Errorf("MsgPong = %q", MsgPong)
	}
	if MsgError != "error" {
		t.Errorf("MsgError = %q", MsgError)
	}
	if Subprotocol != "graphql-transport-ws" {
		t.Errorf("Subprotocol = %q", Subprotocol)
	}
}

func TestMessageMarshalUnmarshal(t *testing.T) {
	msg := Message{
		ID:   "1",
		Type: MsgSubscribe,
		Payload: json.RawMessage(`{
			"query": "subscription { events { id } }"
		}`),
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded Message
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.ID != msg.ID {
		t.Errorf("ID = %q, want %q", decoded.ID, msg.ID)
	}
	if decoded.Type != msg.Type {
		t.Errorf("Type = %q, want %q", decoded.Type, msg.Type)
	}
}

func TestAdapterName(t *testing.T) {
	a := New("/graphql", false, 0, 0, 0, nil, nil)
	if a.Name() != "graphql" {
		t.Errorf("Name() = %q, want %q", a.Name(), "graphql")
	}
}

func TestNewDefaults(t *testing.T) {
	a := New("", false, 0, 0, 0, nil, nil)
	if a.path != "/graphql" {
		t.Errorf("path = %q, want %q", a.path, "/graphql")
	}
	if a.bufferSize != 256 {
		t.Errorf("bufferSize = %d, want %d", a.bufferSize, 256)
	}
	if a.keepaliveInterval != 15*1e9 { // 15s in nanoseconds
		t.Errorf("keepaliveInterval = %v, want 15s", a.keepaliveInterval)
	}
}

func TestSubjectToTypeName(t *testing.T) {
	tests := []struct {
		subject string
		want    string
	}{
		{"public.orders", "PublicOrders"},
		{"myschema.users", "MyschemaUsers"},
		{"orders", "Orders"},
	}

	for _, tt := range tests {
		t.Run(tt.subject, func(t *testing.T) {
			got := subjectToTypeName(tt.subject)
			if got != tt.want {
				t.Errorf("subjectToTypeName(%q) = %q, want %q", tt.subject, got, tt.want)
			}
		})
	}
}

func TestValuesEqual(t *testing.T) {
	tests := []struct {
		name string
		a, b any
		want bool
	}{
		{"equal strings", "foo", "foo", true},
		{"unequal strings", "foo", "bar", false},
		{"equal floats", 1.0, 1.0, true},
		{"unequal floats", 1.0, 2.0, false},
		{"equal bools", true, true, true},
		{"unequal bools", true, false, false},
		{"both nil", nil, nil, true},
		{"nil vs string", nil, "foo", false},
		{"type mismatch", "1", 1.0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := valuesEqual(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("valuesEqual(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
