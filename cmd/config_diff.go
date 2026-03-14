package cmd

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

var configDiffCmd = &cobra.Command{
	Use:   "diff <old.yaml> <new.yaml>",
	Short: "Show differences between two pgcdc config files",
	Long:  `Compares two YAML configuration files and shows added, removed, and changed values with dotted key paths.`,
	Args:  cobra.ExactArgs(2),
	RunE:  runConfigDiff,
}

func init() {
	configCmd.AddCommand(configDiffCmd)
}

func runConfigDiff(cmd *cobra.Command, args []string) error {
	oldData, err := os.ReadFile(args[0])
	if err != nil {
		return fmt.Errorf("read %s: %w", args[0], err)
	}
	newData, err := os.ReadFile(args[1])
	if err != nil {
		return fmt.Errorf("read %s: %w", args[1], err)
	}

	var oldMap, newMap map[string]any
	if err := yaml.Unmarshal(oldData, &oldMap); err != nil {
		return fmt.Errorf("parse %s: %w", args[0], err)
	}
	if err := yaml.Unmarshal(newData, &newMap); err != nil {
		return fmt.Errorf("parse %s: %w", args[1], err)
	}

	diffs := diffMaps("", oldMap, newMap)
	if len(diffs) == 0 {
		_, _ = fmt.Fprintln(cmd.OutOrStdout(), "No differences found.")
		return nil
	}
	for _, d := range diffs {
		_, _ = fmt.Fprintln(cmd.OutOrStdout(), d)
	}
	return nil
}

// diffMaps recursively compares two maps and returns diff lines.
func diffMaps(prefix string, old, new map[string]any) []string {
	var diffs []string

	allKeys := make(map[string]bool)
	for k := range old {
		allKeys[k] = true
	}
	for k := range new {
		allKeys[k] = true
	}

	keys := make([]string, 0, len(allKeys))
	for k := range allKeys {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		path := k
		if prefix != "" {
			path = prefix + "." + k
		}

		oldVal, inOld := old[k]
		newVal, inNew := new[k]

		if !inOld {
			diffs = append(diffs, fmt.Sprintf("+ %s: %s", path, formatValue(newVal)))
			continue
		}
		if !inNew {
			diffs = append(diffs, fmt.Sprintf("- %s: %s", path, formatValue(oldVal)))
			continue
		}

		// Both exist, compare by type.
		oldMap, oldIsMap := oldVal.(map[string]any)
		newMap, newIsMap := newVal.(map[string]any)
		if oldIsMap && newIsMap {
			diffs = append(diffs, diffMaps(path, oldMap, newMap)...)
			continue
		}

		oldSlice, oldIsSlice := toSlice(oldVal)
		newSlice, newIsSlice := toSlice(newVal)
		if oldIsSlice && newIsSlice {
			diffs = append(diffs, diffSlices(path, oldSlice, newSlice)...)
			continue
		}

		// Scalar comparison.
		if fmt.Sprintf("%v", oldVal) != fmt.Sprintf("%v", newVal) {
			diffs = append(diffs, fmt.Sprintf("  %s: %s -> %s", path, formatValue(oldVal), formatValue(newVal)))
		}
	}
	return diffs
}

// diffSlices compares two slices of any values.
func diffSlices(path string, old, new []any) []string {
	// If both are slices of maps (e.g. transforms), diff element by element.
	oldStrs := sliceToStrings(old)
	newStrs := sliceToStrings(new)

	if strings.Join(oldStrs, ",") == strings.Join(newStrs, ",") {
		return nil
	}

	// Check for element-level changes.
	var diffs []string
	maxLen := len(old)
	if len(new) > maxLen {
		maxLen = len(new)
	}

	for i := 0; i < maxLen; i++ {
		elemPath := fmt.Sprintf("%s[%d]", path, i)
		switch {
		case i >= len(old):
			diffs = append(diffs, fmt.Sprintf("+ %s: %s", elemPath, formatValue(new[i])))
		case i >= len(new):
			diffs = append(diffs, fmt.Sprintf("- %s: %s", elemPath, formatValue(old[i])))
		default:
			oldElem := fmt.Sprintf("%v", old[i])
			newElem := fmt.Sprintf("%v", new[i])
			if oldElem != newElem {
				// Check if both are maps for recursive diff.
				oldMap, oldIsMap := old[i].(map[string]any)
				newMap, newIsMap := new[i].(map[string]any)
				if oldIsMap && newIsMap {
					diffs = append(diffs, diffMaps(elemPath, oldMap, newMap)...)
				} else {
					diffs = append(diffs, fmt.Sprintf("  %s: %s -> %s", elemPath, formatValue(old[i]), formatValue(new[i])))
				}
			}
		}
	}
	return diffs
}

// formatValue formats a value for display.
func formatValue(v any) string {
	switch val := v.(type) {
	case map[string]any:
		parts := make([]string, 0, len(val))
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			parts = append(parts, fmt.Sprintf("%s: %s", k, formatValue(val[k])))
		}
		return "{" + strings.Join(parts, ", ") + "}"
	case []any:
		parts := make([]string, len(val))
		for i, item := range val {
			parts[i] = formatValue(item)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case string:
		return val
	default:
		return fmt.Sprintf("%v", v)
	}
}

// toSlice attempts to convert an interface to a slice of any.
func toSlice(v any) ([]any, bool) {
	switch val := v.(type) {
	case []any:
		return val, true
	case []string:
		result := make([]any, len(val))
		for i, s := range val {
			result[i] = s
		}
		return result, true
	default:
		return nil, false
	}
}

// sliceToStrings converts a slice of any to a slice of formatted strings.
func sliceToStrings(s []any) []string {
	result := make([]string, len(s))
	for i, v := range s {
		result[i] = fmt.Sprintf("%v", v)
	}
	return result
}
