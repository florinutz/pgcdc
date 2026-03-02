package cmd

import (
	"strings"
	"testing"
)

func TestDiffMaps_Identical(t *testing.T) {
	t.Parallel()

	m := map[string]any{"a": "1", "b": "2"}
	diffs := diffMaps("", m, m)
	if len(diffs) != 0 {
		t.Errorf("expected no diffs for identical maps, got %v", diffs)
	}
}

func TestDiffMaps_AddedKey(t *testing.T) {
	t.Parallel()

	old := map[string]any{"a": "1"}
	new := map[string]any{"a": "1", "b": "2"}
	diffs := diffMaps("", old, new)

	if len(diffs) != 1 {
		t.Fatalf("expected 1 diff, got %d: %v", len(diffs), diffs)
	}
	if !strings.HasPrefix(diffs[0], "+ b:") {
		t.Errorf("expected '+ b:' prefix, got %q", diffs[0])
	}
}

func TestDiffMaps_RemovedKey(t *testing.T) {
	t.Parallel()

	old := map[string]any{"a": "1", "b": "2"}
	new := map[string]any{"a": "1"}
	diffs := diffMaps("", old, new)

	if len(diffs) != 1 {
		t.Fatalf("expected 1 diff, got %d: %v", len(diffs), diffs)
	}
	if !strings.HasPrefix(diffs[0], "- b:") {
		t.Errorf("expected '- b:' prefix, got %q", diffs[0])
	}
}

func TestDiffMaps_ChangedScalar(t *testing.T) {
	t.Parallel()

	old := map[string]any{"mode": "fast"}
	new := map[string]any{"mode": "reliable"}
	diffs := diffMaps("", old, new)

	if len(diffs) != 1 {
		t.Fatalf("expected 1 diff, got %d: %v", len(diffs), diffs)
	}
	if !strings.Contains(diffs[0], "fast -> reliable") {
		t.Errorf("expected 'fast -> reliable', got %q", diffs[0])
	}
}

func TestDiffMaps_NestedChanges(t *testing.T) {
	t.Parallel()

	old := map[string]any{
		"bus": map[string]any{"mode": "fast", "buffer_size": 1024},
	}
	new := map[string]any{
		"bus": map[string]any{"mode": "reliable", "buffer_size": 1024},
	}
	diffs := diffMaps("", old, new)

	if len(diffs) != 1 {
		t.Fatalf("expected 1 diff, got %d: %v", len(diffs), diffs)
	}
	if !strings.Contains(diffs[0], "bus.mode") {
		t.Errorf("expected 'bus.mode' path, got %q", diffs[0])
	}
}

func TestDiffMaps_WithPrefix(t *testing.T) {
	t.Parallel()

	old := map[string]any{"x": "1"}
	new := map[string]any{"x": "2"}
	diffs := diffMaps("root", old, new)

	if len(diffs) != 1 {
		t.Fatalf("expected 1 diff, got %d: %v", len(diffs), diffs)
	}
	if !strings.Contains(diffs[0], "root.x") {
		t.Errorf("expected 'root.x' path, got %q", diffs[0])
	}
}

func TestDiffSlices_Identical(t *testing.T) {
	t.Parallel()

	s := []any{"a", "b", "c"}
	diffs := diffSlices("items", s, s)
	if len(diffs) != 0 {
		t.Errorf("expected no diffs for identical slices, got %v", diffs)
	}
}

func TestDiffSlices_Added(t *testing.T) {
	t.Parallel()

	old := []any{"a"}
	new := []any{"a", "b"}
	diffs := diffSlices("items", old, new)

	found := false
	for _, d := range diffs {
		if strings.Contains(d, "+ items[1]") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected '+ items[1]' diff, got %v", diffs)
	}
}

func TestDiffSlices_Removed(t *testing.T) {
	t.Parallel()

	old := []any{"a", "b"}
	new := []any{"a"}
	diffs := diffSlices("items", old, new)

	found := false
	for _, d := range diffs {
		if strings.Contains(d, "- items[1]") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected '- items[1]' diff, got %v", diffs)
	}
}

func TestDiffSlices_Changed(t *testing.T) {
	t.Parallel()

	old := []any{"a", "b"}
	new := []any{"a", "c"}
	diffs := diffSlices("items", old, new)

	if len(diffs) != 1 {
		t.Fatalf("expected 1 diff, got %d: %v", len(diffs), diffs)
	}
	if !strings.Contains(diffs[0], "b -> c") {
		t.Errorf("expected 'b -> c', got %q", diffs[0])
	}
}

func TestFormatValue_String(t *testing.T) {
	t.Parallel()
	if got := formatValue("hello"); got != "hello" {
		t.Errorf("formatValue(string) = %q, want %q", got, "hello")
	}
}

func TestFormatValue_Map(t *testing.T) {
	t.Parallel()
	v := map[string]any{"b": "2", "a": "1"}
	got := formatValue(v)
	// Keys should be sorted.
	if got != "{a: 1, b: 2}" {
		t.Errorf("formatValue(map) = %q, want %q", got, "{a: 1, b: 2}")
	}
}

func TestFormatValue_Slice(t *testing.T) {
	t.Parallel()
	v := []any{"x", "y"}
	if got := formatValue(v); got != "[x, y]" {
		t.Errorf("formatValue(slice) = %q, want %q", got, "[x, y]")
	}
}

func TestFormatValue_Int(t *testing.T) {
	t.Parallel()
	if got := formatValue(42); got != "42" {
		t.Errorf("formatValue(int) = %q, want %q", got, "42")
	}
}
