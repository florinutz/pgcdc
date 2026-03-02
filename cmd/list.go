package cmd

import (
	"fmt"

	"github.com/florinutz/pgcdc/internal/output"
	"github.com/florinutz/pgcdc/registry"
	"github.com/spf13/cobra"
)

type componentInfo struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type listResult struct {
	Adapters   []componentInfo `json:"adapters,omitempty"`
	Detectors  []componentInfo `json:"detectors,omitempty"`
	Transforms []componentInfo `json:"transforms,omitempty"`
}

var listCmd = &cobra.Command{
	Use:   "list [adapters|detectors|transforms]",
	Short: "List available components",
	Long:  `List all registered adapters, detectors, or transforms.`,
	Args:  cobra.MaximumNArgs(1),
	RunE:  runList,
}

func init() {
	rootCmd.AddCommand(listCmd)
	output.AddOutputFlag(listCmd)
}

func runList(cmd *cobra.Command, args []string) error {
	kind := "all"
	if len(args) > 0 {
		kind = args[0]
	}

	printer := output.FromCommand(cmd)

	switch kind {
	case "adapters", "adapter":
		entries := registry.Adapters()
		if printer.IsJSON() {
			items := make([]componentInfo, len(entries))
			for i, e := range entries {
				items[i] = componentInfo{Name: e.Name, Description: e.Description}
			}
			return printer.JSON(listResult{Adapters: items})
		}
		headers := []string{"NAME", "DESCRIPTION"}
		rows := make([][]string, len(entries))
		for i, e := range entries {
			rows[i] = []string{e.Name, e.Description}
		}
		return printer.Table(headers, rows)

	case "detectors", "detector":
		entries := registry.Detectors()
		if printer.IsJSON() {
			items := make([]componentInfo, len(entries))
			for i, e := range entries {
				items[i] = componentInfo{Name: e.Name, Description: e.Description}
			}
			return printer.JSON(listResult{Detectors: items})
		}
		headers := []string{"NAME", "DESCRIPTION"}
		rows := make([][]string, len(entries))
		for i, e := range entries {
			rows[i] = []string{e.Name, e.Description}
		}
		return printer.Table(headers, rows)

	case "transforms", "transform":
		entries := registry.Transforms()
		if printer.IsJSON() {
			items := make([]componentInfo, len(entries))
			for i, e := range entries {
				items[i] = componentInfo{Name: e.Name, Description: e.Description}
			}
			return printer.JSON(listResult{Transforms: items})
		}
		headers := []string{"NAME", "DESCRIPTION"}
		rows := make([][]string, len(entries))
		for i, e := range entries {
			rows[i] = []string{e.Name, e.Description}
		}
		return printer.Table(headers, rows)

	case "all":
		adapters := registry.Adapters()
		detectors := registry.Detectors()
		transforms := registry.Transforms()

		if printer.IsJSON() {
			result := listResult{
				Adapters:   make([]componentInfo, len(adapters)),
				Detectors:  make([]componentInfo, len(detectors)),
				Transforms: make([]componentInfo, len(transforms)),
			}
			for i, e := range adapters {
				result.Adapters[i] = componentInfo{Name: e.Name, Description: e.Description}
			}
			for i, e := range detectors {
				result.Detectors[i] = componentInfo{Name: e.Name, Description: e.Description}
			}
			for i, e := range transforms {
				result.Transforms[i] = componentInfo{Name: e.Name, Description: e.Description}
			}
			return printer.JSON(result)
		}

		w := printer.Writer()
		_, _ = fmt.Fprintln(w, "ADAPTERS")
		adapterRows := make([][]string, len(adapters))
		for i, e := range adapters {
			adapterRows[i] = []string{e.Name, e.Description}
		}
		if err := printer.Table([]string{"NAME", "DESCRIPTION"}, adapterRows); err != nil {
			return err
		}
		_, _ = fmt.Fprintln(w)

		_, _ = fmt.Fprintln(w, "DETECTORS")
		detectorRows := make([][]string, len(detectors))
		for i, e := range detectors {
			detectorRows[i] = []string{e.Name, e.Description}
		}
		if err := printer.Table([]string{"NAME", "DESCRIPTION"}, detectorRows); err != nil {
			return err
		}
		_, _ = fmt.Fprintln(w)

		_, _ = fmt.Fprintln(w, "TRANSFORMS")
		transformRows := make([][]string, len(transforms))
		for i, e := range transforms {
			transformRows[i] = []string{e.Name, e.Description}
		}
		return printer.Table([]string{"NAME", "DESCRIPTION"}, transformRows)

	default:
		return fmt.Errorf("unknown component type %q: expected adapters, detectors, or transforms", kind)
	}
}
