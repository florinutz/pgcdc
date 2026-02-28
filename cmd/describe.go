package cmd

import (
	"fmt"
	"strings"
	"text/tabwriter"

	"github.com/florinutz/pgcdc/registry"
	"github.com/spf13/cobra"
)

var describeCmd = &cobra.Command{
	Use:   "describe <connector>",
	Short: "Describe a connector's configuration parameters",
	Long:  `Prints the typed specification for an adapter or detector, including parameter names, types, defaults, validation rules, and descriptions.`,
	Args:  cobra.ExactArgs(1),
	RunE:  runDescribe,
}

func init() {
	rootCmd.AddCommand(describeCmd)
}

func runDescribe(cmd *cobra.Command, args []string) error {
	name := args[0]
	spec := registry.GetConnectorSpec(name)
	if spec == nil {
		return fmt.Errorf("unknown connector %q: not found in adapters or detectors", name)
	}

	out := cmd.OutOrStdout()

	_, _ = fmt.Fprintf(out, "%s (%s)\n", spec.Name, spec.Type)
	if spec.Description != "" {
		_, _ = fmt.Fprintf(out, "%s\n", spec.Description)
	}
	_, _ = fmt.Fprintln(out)

	if len(spec.Params) == 0 {
		_, _ = fmt.Fprintln(out, "No parameters defined.")
		return nil
	}

	w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "NAME\tTYPE\tDEFAULT\tREQUIRED\tVALIDATIONS\tDESCRIPTION")
	_, _ = fmt.Fprintln(w, "----\t----\t-------\t--------\t-----------\t-----------")
	for _, p := range spec.Params {
		req := "no"
		if p.Required {
			req = "yes"
		}
		def := registry.FormatDefault(p.Default)
		if def == "" {
			def = "-"
		}
		validations := "-"
		if len(p.Validations) > 0 {
			validations = strings.Join(p.Validations, ", ")
		}
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", p.Name, p.Type, def, req, validations, p.Description)
	}
	return w.Flush()
}
