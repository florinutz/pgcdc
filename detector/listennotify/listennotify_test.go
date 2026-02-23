package listennotify

import (
	"encoding/json"
	"testing"
)

func TestParsePayload(t *testing.T) {
	tests := []struct {
		name     string
		raw      string
		wantOp   string
		wantJSON string // expected payload as a string for comparison
	}{
		{
			name:     "JSON with op field",
			raw:      `{"op":"INSERT","table":"users","id":42}`,
			wantOp:   "INSERT",
			wantJSON: `{"op":"INSERT","table":"users","id":42}`,
		},
		{
			name:     "JSON with op DELETE",
			raw:      `{"op":"DELETE","id":1}`,
			wantOp:   "DELETE",
			wantJSON: `{"op":"DELETE","id":1}`,
		},
		{
			name:     "JSON without op field",
			raw:      `{"table":"users","id":42}`,
			wantOp:   "NOTIFY",
			wantJSON: `{"table":"users","id":42}`,
		},
		{
			name:     "JSON with empty op field",
			raw:      `{"op":"","table":"users"}`,
			wantOp:   "NOTIFY",
			wantJSON: `{"op":"","table":"users"}`,
		},
		{
			name:     "JSON with non-string op field",
			raw:      `{"op":123}`,
			wantOp:   "NOTIFY",
			wantJSON: `{"op":123}`,
		},
		{
			name:     "plain text payload",
			raw:      `hello world`,
			wantOp:   "NOTIFY",
			wantJSON: `"hello world"`,
		},
		{
			name:     "empty payload",
			raw:      ``,
			wantOp:   "NOTIFY",
			wantJSON: `""`,
		},
		{
			name:     "JSON array (not object)",
			raw:      `[1,2,3]`,
			wantOp:   "NOTIFY",
			wantJSON: `"[1,2,3]"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotOp, gotPayload := parsePayload(tt.raw)

			if gotOp != tt.wantOp {
				t.Errorf("operation: got %q, want %q", gotOp, tt.wantOp)
			}

			// Compare as compacted JSON to ignore whitespace differences.
			var wantBuf, gotBuf json.RawMessage
			if err := json.Unmarshal([]byte(tt.wantJSON), &wantBuf); err != nil {
				t.Fatalf("invalid wantJSON %q: %v", tt.wantJSON, err)
			}
			if err := json.Unmarshal(gotPayload, &gotBuf); err != nil {
				t.Fatalf("invalid gotPayload %q: %v", string(gotPayload), err)
			}

			wantCompact, _ := json.Marshal(wantBuf)
			gotCompact, _ := json.Marshal(gotBuf)
			if string(gotCompact) != string(wantCompact) {
				t.Errorf("payload:\n  got  %s\n  want %s", gotCompact, wantCompact)
			}
		})
	}
}

func TestBackoff(t *testing.T) {
	const (
		base     = 5_000_000_000  // 5s in nanoseconds
		maxDly   = 60_000_000_000 // 60s in nanoseconds
		minFloor = 100_000_000    // 100ms minimum
	)

	// Attempt 0: max = base * 2^0 = 5s, so delay must be in [100ms, 5s).
	for range 100 {
		d := backoff(0, base, maxDly)
		if d < minFloor || d >= base {
			t.Fatalf("attempt 0: got %v, want [%v, %v)", d, minFloor, base)
		}
	}

	// High attempt: should be capped at 60s.
	for range 100 {
		d := backoff(100, base, maxDly)
		if d < minFloor || d >= maxDly {
			t.Fatalf("attempt 100: got %v, want [%v, %v)", d, minFloor, maxDly)
		}
	}
}

func TestName(t *testing.T) {
	d := New("postgres://localhost/test", []string{"events"}, 0, 0, nil)
	if got := d.Name(); got != "listen_notify" {
		t.Errorf("Name() = %q, want %q", got, "listen_notify")
	}
}
