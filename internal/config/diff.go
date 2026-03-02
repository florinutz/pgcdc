package config

import (
	"fmt"
	"sort"
	"strings"
)

// ReloadSummary captures the reloadable parts of config for diffing.
type ReloadSummary struct {
	GlobalTransforms  []string            // transform type names
	AdapterTransforms map[string][]string // adapter -> transform type names
	Routes            map[string][]string // adapter -> channels
}

// BuildReloadSummary creates a ReloadSummary from config.
func BuildReloadSummary(cfg Config) ReloadSummary {
	s := ReloadSummary{
		AdapterTransforms: make(map[string][]string),
		Routes:            make(map[string][]string),
	}

	for _, spec := range cfg.Transforms.Global {
		if spec.Type != "" {
			s.GlobalTransforms = append(s.GlobalTransforms, spec.Type)
		}
	}

	for adapterName, specs := range cfg.Transforms.Adapter {
		for _, spec := range specs {
			if spec.Type != "" {
				s.AdapterTransforms[adapterName] = append(s.AdapterTransforms[adapterName], spec.Type)
			}
		}
	}

	for k, v := range cfg.Routes {
		s.Routes[k] = v
	}

	return s
}

// DiffReload compares old and new ReloadSummary and returns a human-readable diff string.
// Returns "no changes" if nothing changed.
func DiffReload(old, new ReloadSummary) string {
	var sections []string

	// Diff global transforms.
	if diff := diffStringSlice("transforms", old.GlobalTransforms, new.GlobalTransforms); diff != "" {
		sections = append(sections, diff)
	}

	// Diff per-adapter transforms.
	allAdapterTransforms := mergeKeys(old.AdapterTransforms, new.AdapterTransforms)
	sort.Strings(allAdapterTransforms)
	for _, adapter := range allAdapterTransforms {
		label := fmt.Sprintf("transforms[%s]", adapter)
		if diff := diffStringSlice(label, old.AdapterTransforms[adapter], new.AdapterTransforms[adapter]); diff != "" {
			sections = append(sections, diff)
		}
	}

	// Diff routes.
	allRouteAdapters := mergeKeys(old.Routes, new.Routes)
	sort.Strings(allRouteAdapters)
	for _, adapter := range allRouteAdapters {
		label := fmt.Sprintf("routes[%s]", adapter)
		if diff := diffStringSlice(label, old.Routes[adapter], new.Routes[adapter]); diff != "" {
			sections = append(sections, diff)
		}
	}

	if len(sections) == 0 {
		return "no changes"
	}
	return strings.Join(sections, "; ")
}

// diffStringSlice compares two slices and returns a label with +added -removed items.
func diffStringSlice(label string, old, new []string) string {
	oldSet := make(map[string]bool, len(old))
	for _, v := range old {
		oldSet[v] = true
	}
	newSet := make(map[string]bool, len(new))
	for _, v := range new {
		newSet[v] = true
	}

	var changes []string
	for _, v := range new {
		if !oldSet[v] {
			changes = append(changes, "+"+v)
		}
	}
	for _, v := range old {
		if !newSet[v] {
			changes = append(changes, "-"+v)
		}
	}

	if len(changes) == 0 {
		return ""
	}
	return label + ": " + strings.Join(changes, " ")
}

// mergeKeys returns the union of keys from two maps.
func mergeKeys(a, b map[string][]string) []string {
	seen := make(map[string]bool)
	for k := range a {
		seen[k] = true
	}
	for k := range b {
		seen[k] = true
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	return keys
}
