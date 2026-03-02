package cmd

import (
	"fmt"
	"strings"

	"github.com/florinutz/pgcdc/internal/output"
	"github.com/florinutz/pgcdc/registry"
	"github.com/spf13/cobra"
)

type describeResult struct {
	Name        string        `json:"name"`
	Type        string        `json:"type"`
	Description string        `json:"description,omitempty"`
	Params      []paramResult `json:"params,omitempty"`
}

type paramResult struct {
	Name        string   `json:"name"`
	Type        string   `json:"type"`
	Default     string   `json:"default"`
	Required    bool     `json:"required"`
	Validations []string `json:"validations,omitempty"`
	Description string   `json:"description"`
}

var describeCmd = &cobra.Command{
	Use:   "describe <connector>",
	Short: "Describe a connector's configuration parameters",
	Long:  `Prints the typed specification for an adapter or detector, including parameter names, types, defaults, validation rules, and descriptions.`,
	Args:  cobra.ExactArgs(1),
	RunE:  runDescribe,
}

func init() {
	rootCmd.AddCommand(describeCmd)
	output.AddOutputFlag(describeCmd)
}

func runDescribe(cmd *cobra.Command, args []string) error {
	name := args[0]
	spec := registry.GetConnectorSpec(name)
	if spec == nil {
		return fmt.Errorf("unknown connector %q: not found in adapters or detectors", name)
	}

	printer := output.FromCommand(cmd)

	result := describeResult{
		Name:        spec.Name,
		Type:        spec.Type,
		Description: spec.Description,
	}
	for _, p := range spec.Params {
		def := registry.FormatDefault(p.Default)
		if def == "" {
			def = "-"
		}
		result.Params = append(result.Params, paramResult{
			Name:        p.Name,
			Type:        p.Type,
			Default:     def,
			Required:    p.Required,
			Validations: p.Validations,
			Description: p.Description,
		})
	}

	if printer.IsJSON() {
		return printer.JSON(result)
	}

	out := printer.Writer()
	_, _ = fmt.Fprintf(out, "%s (%s)\n", spec.Name, spec.Type)
	if spec.Description != "" {
		_, _ = fmt.Fprintf(out, "%s\n", spec.Description)
	}
	_, _ = fmt.Fprintln(out)

	if len(spec.Params) == 0 {
		_, _ = fmt.Fprintln(out, "No parameters defined.")
		return nil
	}

	headers := []string{"NAME", "TYPE", "DEFAULT", "REQUIRED", "VALIDATIONS", "DESCRIPTION"}
	var rows [][]string
	for _, p := range result.Params {
		req := "no"
		if p.Required {
			req = "yes"
		}
		validations := "-"
		if len(p.Validations) > 0 {
			validations = strings.Join(p.Validations, ", ")
		}
		rows = append(rows, []string{p.Name, p.Type, p.Default, req, validations, p.Description})
	}
	return printer.Table(headers, rows)
}
