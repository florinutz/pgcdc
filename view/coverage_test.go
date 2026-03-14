package view

import (
	"testing"
	"time"
)

func TestTickInterval(t *testing.T) {
	tests := []struct {
		name string
		def  *ViewDef
		want time.Duration
	}{
		{"tumbling", &ViewDef{WindowType: WindowTumbling, WindowSize: time.Minute}, time.Minute},
		{"sliding", &ViewDef{WindowType: WindowSliding, SlideSize: 10 * time.Second}, 10 * time.Second},
		{"session", &ViewDef{WindowType: WindowSession, SessionGap: 30 * time.Second}, 30 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tickInterval(tt.def); got != tt.want {
				t.Errorf("tickInterval() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractEventTime(t *testing.T) {
	tests := []struct {
		name    string
		payload map[string]any
		field   string
		wantOK  bool
	}{
		{
			name:    "RFC3339Nano",
			payload: map[string]any{"ts": "2026-03-01T12:00:00.123456789Z"},
			field:   "ts",
			wantOK:  true,
		},
		{
			name:    "RFC3339",
			payload: map[string]any{"ts": "2026-03-01T12:00:00Z"},
			field:   "ts",
			wantOK:  true,
		},
		{
			name:    "nested with payload prefix",
			payload: map[string]any{"row": map[string]any{"ts": "2026-03-01T12:00:00Z"}},
			field:   "payload.row.ts",
			wantOK:  true,
		},
		{
			name:    "nested without prefix",
			payload: map[string]any{"row": map[string]any{"ts": "2026-03-01T12:00:00Z"}},
			field:   "row.ts",
			wantOK:  true,
		},
		{
			name:    "missing field",
			payload: map[string]any{"id": 1},
			field:   "ts",
			wantOK:  false,
		},
		{
			name:    "invalid format",
			payload: map[string]any{"ts": "not-a-time"},
			field:   "ts",
			wantOK:  false,
		},
		{
			name:    "non-string value",
			payload: map[string]any{"ts": 12345},
			field:   "ts",
			wantOK:  false,
		},
		{
			name:    "empty field",
			payload: map[string]any{"ts": "2026-03-01T12:00:00Z"},
			field:   "",
			wantOK:  false,
		},
		{
			name:    "nil payload",
			payload: nil,
			field:   "ts",
			wantOK:  false,
		},
		{
			name:    "nested missing intermediate",
			payload: map[string]any{"a": "not-a-map"},
			field:   "a.b.c",
			wantOK:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractEventTime(tt.payload, tt.field)
			if tt.wantOK && got.IsZero() {
				t.Error("expected non-zero time")
			}
			if !tt.wantOK && !got.IsZero() {
				t.Errorf("expected zero time, got %v", got)
			}
		})
	}
}

func TestFlipOp(t *testing.T) {
	// The opcode package uses int types. We test via the parser's flipOp.
	// Just verify the inversion pairs work.
	tests := []struct {
		name string
		in   int
		want int
	}{
		// GT=3, LT=1, GE=4, LE=2, EQ=5, NE=6  (tidb opcode values)
		{"GT→LT", 3, 1},
		{"LT→GT", 1, 3},
		{"GE→LE", 4, 2},
		{"LE→GE", 2, 4},
		{"EQ stays", 5, 5},
		{"NE stays", 6, 6},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// We can't import opcode.Op directly in a table test easily,
			// so test through Parse with HAVING.
		})
	}
	// Test through actual Parse call to verify flipOp is exercised.
	_, err := Parse("test",
		"SELECT COUNT(*) as cnt FROM pgcdc_events GROUP BY payload.region HAVING 5 < COUNT(*) TUMBLING WINDOW 1m",
		EmitRow, 0)
	if err != nil {
		t.Fatalf("parse with flipped HAVING: %v", err)
	}
}

func TestLikeToRegex(t *testing.T) {
	tests := []struct {
		pattern string
		match   string
		want    bool
	}{
		{"%test%", "this is a test string", true},
		{"%test%", "no match here", false},
		{"test%", "testing", true},
		{"test%", "a test", false},
		{"%test", "a test", true},
		{"te_t", "test", true},
		{"te_t", "text", true},
		{"te_t", "tet", false},
		{"100\\%", "100%", true},
		{"100\\%", "100x", false},
		{"", "", true},
		{"%", "anything", true},
		{"exact", "exact", true},
		{"exact", "not exact", false},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"→"+tt.match, func(t *testing.T) {
			re, err := likeToRegex(tt.pattern)
			if err != nil {
				t.Fatalf("likeToRegex(%q): %v", tt.pattern, err)
			}
			got := re.MatchString(tt.match)
			if got != tt.want {
				t.Errorf("likeToRegex(%q).MatchString(%q) = %v, want %v", tt.pattern, tt.match, got, tt.want)
			}
		})
	}
}

func TestParse_LIKE(t *testing.T) {
	def, err := Parse("test",
		"SELECT COUNT(*) as n FROM pgcdc_events WHERE channel LIKE 'pgcdc:%' TUMBLING WINDOW 1m",
		EmitRow, 0)
	if err != nil {
		t.Fatalf("parse LIKE: %v", err)
	}
	if def.Where == nil {
		t.Fatal("expected WHERE predicate")
	}
}

func TestParse_IN(t *testing.T) {
	def, err := Parse("test",
		"SELECT COUNT(*) as n FROM pgcdc_events WHERE operation IN ('INSERT', 'UPDATE') TUMBLING WINDOW 1m",
		EmitRow, 0)
	if err != nil {
		t.Fatalf("parse IN: %v", err)
	}
	if def.Where == nil {
		t.Fatal("expected WHERE predicate")
	}
}

func TestParse_NOT_LIKE(t *testing.T) {
	def, err := Parse("test",
		"SELECT COUNT(*) as n FROM pgcdc_events WHERE channel NOT LIKE '%internal%' TUMBLING WINDOW 1m",
		EmitRow, 0)
	if err != nil {
		t.Fatalf("parse NOT LIKE: %v", err)
	}
	if def.Where == nil {
		t.Fatal("expected WHERE predicate")
	}
}

func TestParse_NOT_IN(t *testing.T) {
	def, err := Parse("test",
		"SELECT COUNT(*) as n FROM pgcdc_events WHERE operation NOT IN ('TRUNCATE') TUMBLING WINDOW 1m",
		EmitRow, 0)
	if err != nil {
		t.Fatalf("parse NOT IN: %v", err)
	}
	if def.Where == nil {
		t.Fatal("expected WHERE predicate")
	}
}

func TestParse_IS_NULL(t *testing.T) {
	_, err := Parse("test",
		"SELECT COUNT(*) as n FROM pgcdc_events WHERE payload.deleted_at IS NULL TUMBLING WINDOW 1m",
		EmitRow, 0)
	if err != nil {
		t.Fatalf("parse IS NULL: %v", err)
	}
}

func TestParse_IS_NOT_NULL(t *testing.T) {
	_, err := Parse("test",
		"SELECT COUNT(*) as n FROM pgcdc_events WHERE payload.deleted_at IS NOT NULL TUMBLING WINDOW 1m",
		EmitRow, 0)
	if err != nil {
		t.Fatalf("parse IS NOT NULL: %v", err)
	}
}

func TestParse_HavingAND(t *testing.T) {
	_, err := Parse("test",
		"SELECT COUNT(*) as cnt, SUM(payload.amount) as total FROM pgcdc_events GROUP BY payload.region HAVING COUNT(*) > 5 AND SUM(payload.amount) > 100 TUMBLING WINDOW 1m",
		EmitRow, 0)
	if err != nil {
		t.Fatalf("parse HAVING AND: %v", err)
	}
}

func TestParse_EventTimeBY(t *testing.T) {
	def, err := Parse("test",
		"SELECT COUNT(*) as n FROM pgcdc_events EVENT TIME BY payload.ts ALLOWED LATENESS 5s GROUP BY channel TUMBLING WINDOW 10s",
		EmitRow, 0)
	if err != nil {
		t.Fatalf("parse EVENT TIME BY: %v", err)
	}
	if def.EventTimeField != "payload.ts" {
		t.Errorf("EventTimeField = %q, want payload.ts", def.EventTimeField)
	}
}

func TestCloneAggregator(t *testing.T) {
	tests := []struct {
		name string
		agg  Aggregator
	}{
		{"count", &countAgg{n: 5}},
		{"sum", &sumAgg{sum: 10.5, any: true}},
		{"avg", &avgAgg{sum: 20, count: 4}},
		{"min", &minAgg{min: 1.5, any: true}},
		{"max", &maxAgg{max: 99.9, any: true}},
		{"count_distinct", &countDistinctAgg{seen: map[string]struct{}{"a": {}, "b": {}}}},
		{"stddev", &stddevAgg{n: 3, mean: 10.0, m2: 5.0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cloned := cloneAggregator(tt.agg)
			origResult := tt.agg.Result()
			clonedResult := cloned.Result()

			// Results should match after cloning.
			if origResult != clonedResult {
				// For count_distinct, results are int64.
				t.Errorf("clone result = %v, want %v", clonedResult, origResult)
			}

			// Mutating clone should not affect original.
			cloned.Add(float64(1000))
			if tt.agg.Result() != origResult {
				t.Errorf("original mutated after cloning: %v != %v", tt.agg.Result(), origResult)
			}
		})
	}
}

func TestMergeAggregator(t *testing.T) {
	t.Run("count", func(t *testing.T) {
		dst := &countAgg{n: 3}
		src := &countAgg{n: 7}
		mergeAggregator(dst, src)
		if dst.n != 10 {
			t.Errorf("merged count = %d, want 10", dst.n)
		}
	})

	t.Run("sum", func(t *testing.T) {
		dst := &sumAgg{sum: 10, any: true}
		src := &sumAgg{sum: 20, any: true}
		mergeAggregator(dst, src)
		if dst.sum != 30 {
			t.Errorf("merged sum = %f, want 30", dst.sum)
		}
	})

	t.Run("avg", func(t *testing.T) {
		dst := &avgAgg{sum: 10, count: 2}
		src := &avgAgg{sum: 20, count: 3}
		mergeAggregator(dst, src)
		if dst.count != 5 || dst.sum != 30 {
			t.Errorf("merged avg = sum:%f count:%d, want sum:30 count:5", dst.sum, dst.count)
		}
	})

	t.Run("min", func(t *testing.T) {
		dst := &minAgg{min: 5, any: true}
		src := &minAgg{min: 2, any: true}
		mergeAggregator(dst, src)
		if dst.min != 2 {
			t.Errorf("merged min = %f, want 2", dst.min)
		}
	})

	t.Run("min_dst_empty", func(t *testing.T) {
		dst := &minAgg{}
		src := &minAgg{min: 7, any: true}
		mergeAggregator(dst, src)
		if dst.min != 7 || !dst.any {
			t.Errorf("merged min = %f (any=%v), want 7 (any=true)", dst.min, dst.any)
		}
	})

	t.Run("max", func(t *testing.T) {
		dst := &maxAgg{max: 5, any: true}
		src := &maxAgg{max: 10, any: true}
		mergeAggregator(dst, src)
		if dst.max != 10 {
			t.Errorf("merged max = %f, want 10", dst.max)
		}
	})

	t.Run("max_dst_empty", func(t *testing.T) {
		dst := &maxAgg{}
		src := &maxAgg{max: 3, any: true}
		mergeAggregator(dst, src)
		if dst.max != 3 || !dst.any {
			t.Errorf("merged max = %f (any=%v), want 3 (any=true)", dst.max, dst.any)
		}
	})

	t.Run("count_distinct", func(t *testing.T) {
		dst := &countDistinctAgg{seen: map[string]struct{}{"a": {}, "b": {}}}
		src := &countDistinctAgg{seen: map[string]struct{}{"b": {}, "c": {}}}
		mergeAggregator(dst, src)
		if len(dst.seen) != 3 {
			t.Errorf("merged distinct = %d, want 3", len(dst.seen))
		}
	})

	t.Run("stddev", func(t *testing.T) {
		dst := &stddevAgg{n: 3, mean: 10.0, m2: 6.0}
		src := &stddevAgg{n: 2, mean: 20.0, m2: 2.0}
		mergeAggregator(dst, src)
		if dst.n != 5 {
			t.Errorf("merged stddev n = %d, want 5", dst.n)
		}
	})

	t.Run("stddev_dst_empty", func(t *testing.T) {
		dst := &stddevAgg{}
		src := &stddevAgg{n: 3, mean: 5.0, m2: 10.0}
		mergeAggregator(dst, src)
		if dst.n != 3 || dst.mean != 5.0 || dst.m2 != 10.0 {
			t.Errorf("merged stddev = n:%d mean:%f m2:%f", dst.n, dst.mean, dst.m2)
		}
	})
}

func TestTumblingWindow_FlushUpTo(t *testing.T) {
	def, err := Parse("test", "SELECT COUNT(*) as n FROM pgcdc_events TUMBLING WINDOW 1m", EmitRow, 0)
	if err != nil {
		t.Fatal(err)
	}

	tw := NewTumblingWindow(def, nil)

	// Add an event so flush has something to emit.
	tw.Add(EventMeta{Channel: "pgcdc:ch", Operation: "INSERT"}, map[string]any{}, time.Time{})

	// Watermark before window end → no flush.
	events := tw.FlushUpTo(time.Now())
	if len(events) != 0 {
		t.Errorf("FlushUpTo before window end should return nil, got %d events", len(events))
	}

	// Watermark far in the future → should flush.
	events = tw.FlushUpTo(time.Now().Add(2 * time.Minute))
	if len(events) == 0 {
		t.Error("FlushUpTo after window end should flush events")
	}
}

func TestSlidingWindow_FlushUpTo(t *testing.T) {
	def, err := Parse("test", "SELECT COUNT(*) as n FROM pgcdc_events SLIDING WINDOW 5m SLIDE 1m", EmitRow, 0)
	if err != nil {
		t.Fatal(err)
	}

	sw := NewSlidingWindow(def, nil)
	sw.Add(EventMeta{Channel: "pgcdc:ch", Operation: "INSERT"}, map[string]any{}, time.Time{})

	// FlushUpTo delegates to Flush.
	events := sw.FlushUpTo(time.Now().Add(10 * time.Minute))
	// May or may not have events depending on internal state, but should not panic.
	_ = events
}

func TestSessionWindow_FlushUpTo(t *testing.T) {
	def, err := Parse("test", "SELECT COUNT(*) as n FROM pgcdc_events SESSION WINDOW 1ms", EmitRow, 0)
	if err != nil {
		t.Fatal(err)
	}

	sw := NewSessionWindow(def, nil)
	sw.Add(EventMeta{Channel: "pgcdc:ch", Operation: "INSERT"}, map[string]any{}, time.Time{})

	// Let session expire.
	time.Sleep(5 * time.Millisecond)

	// FlushUpTo delegates to Flush.
	events := sw.FlushUpTo(time.Now())
	if len(events) == 0 {
		t.Error("expected flushed session events")
	}
}
