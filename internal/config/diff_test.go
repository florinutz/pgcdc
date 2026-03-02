package config

import (
	"strings"
	"testing"
)

func TestBuildReloadSummary(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Transforms: TransformConfig{
			Global: []TransformSpec{
				{Type: "cel_filter"},
				{Type: "mask"},
			},
			Adapter: map[string][]TransformSpec{
				"webhook": {{Type: "drop_columns"}},
			},
		},
		Routes: map[string][]string{
			"kafka": {"orders", "users"},
		},
	}

	s := BuildReloadSummary(cfg)

	if len(s.GlobalTransforms) != 2 || s.GlobalTransforms[0] != "cel_filter" {
		t.Errorf("unexpected global transforms: %v", s.GlobalTransforms)
	}
	if len(s.AdapterTransforms["webhook"]) != 1 || s.AdapterTransforms["webhook"][0] != "drop_columns" {
		t.Errorf("unexpected adapter transforms: %v", s.AdapterTransforms)
	}
	if len(s.Routes["kafka"]) != 2 {
		t.Errorf("unexpected routes: %v", s.Routes)
	}
}

func TestBuildReloadSummary_SkipsEmptyType(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Transforms: TransformConfig{
			Global: []TransformSpec{{Type: ""}, {Type: "mask"}},
		},
	}

	s := BuildReloadSummary(cfg)
	if len(s.GlobalTransforms) != 1 || s.GlobalTransforms[0] != "mask" {
		t.Errorf("expected only 'mask', got %v", s.GlobalTransforms)
	}
}

func TestDiffReload_NoChanges(t *testing.T) {
	t.Parallel()

	s := ReloadSummary{
		GlobalTransforms:  []string{"mask"},
		AdapterTransforms: map[string][]string{},
		Routes:            map[string][]string{},
	}

	if got := DiffReload(s, s); got != "no changes" {
		t.Errorf("expected 'no changes', got %q", got)
	}
}

func TestDiffReload_AddedGlobalTransform(t *testing.T) {
	t.Parallel()

	old := ReloadSummary{
		GlobalTransforms:  []string{"mask"},
		AdapterTransforms: map[string][]string{},
		Routes:            map[string][]string{},
	}
	new := ReloadSummary{
		GlobalTransforms:  []string{"mask", "cel_filter"},
		AdapterTransforms: map[string][]string{},
		Routes:            map[string][]string{},
	}

	got := DiffReload(old, new)
	if !strings.Contains(got, "transforms: +cel_filter") {
		t.Errorf("expected '+cel_filter', got %q", got)
	}
}

func TestDiffReload_RemovedAdapterTransform(t *testing.T) {
	t.Parallel()

	old := ReloadSummary{
		GlobalTransforms:  nil,
		AdapterTransforms: map[string][]string{"webhook": {"mask", "drop_columns"}},
		Routes:            map[string][]string{},
	}
	new := ReloadSummary{
		GlobalTransforms:  nil,
		AdapterTransforms: map[string][]string{"webhook": {"drop_columns"}},
		Routes:            map[string][]string{},
	}

	got := DiffReload(old, new)
	if !strings.Contains(got, "transforms[webhook]: -mask") {
		t.Errorf("expected '-mask', got %q", got)
	}
}

func TestDiffReload_ChangedRoutes(t *testing.T) {
	t.Parallel()

	old := ReloadSummary{
		GlobalTransforms:  nil,
		AdapterTransforms: map[string][]string{},
		Routes:            map[string][]string{"kafka": {"orders"}},
	}
	new := ReloadSummary{
		GlobalTransforms:  nil,
		AdapterTransforms: map[string][]string{},
		Routes:            map[string][]string{"kafka": {"payments"}},
	}

	got := DiffReload(old, new)
	if !strings.Contains(got, "routes[kafka]: +payments -orders") {
		t.Errorf("expected route diff, got %q", got)
	}
}

func TestDiffReload_MultipleSections(t *testing.T) {
	t.Parallel()

	old := ReloadSummary{
		GlobalTransforms:  []string{"mask"},
		AdapterTransforms: map[string][]string{},
		Routes:            map[string][]string{"kafka": {"orders"}},
	}
	new := ReloadSummary{
		GlobalTransforms:  []string{"cel_filter"},
		AdapterTransforms: map[string][]string{},
		Routes:            map[string][]string{"kafka": {"orders", "users"}},
	}

	got := DiffReload(old, new)
	if !strings.Contains(got, "transforms:") {
		t.Errorf("expected transforms section, got %q", got)
	}
	if !strings.Contains(got, "routes[kafka]:") {
		t.Errorf("expected routes section, got %q", got)
	}
	// Multiple sections separated by "; ".
	if !strings.Contains(got, "; ") {
		t.Errorf("expected sections joined by '; ', got %q", got)
	}
}

func TestDiffReload_NewAdapter(t *testing.T) {
	t.Parallel()

	old := ReloadSummary{
		AdapterTransforms: map[string][]string{},
		Routes:            map[string][]string{},
	}
	new := ReloadSummary{
		AdapterTransforms: map[string][]string{"webhook": {"mask"}},
		Routes:            map[string][]string{},
	}

	got := DiffReload(old, new)
	if !strings.Contains(got, "transforms[webhook]: +mask") {
		t.Errorf("expected new adapter transform, got %q", got)
	}
}
