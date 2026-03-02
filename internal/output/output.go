package output

import (
	"io"
	"os"

	"github.com/spf13/cobra"
)

// Format represents the output format for CLI commands.
type Format string

const (
	FormatTable Format = "table"
	FormatJSON  Format = "json"
)

// Printer handles formatted output for CLI commands.
type Printer struct {
	w      io.Writer
	format Format
}

// NewPrinter creates a Printer with the given writer and format.
func NewPrinter(w io.Writer, format Format) *Printer {
	return &Printer{w: w, format: format}
}

// Writer returns the underlying writer.
func (p *Printer) Writer() io.Writer { return p.w }

// Format returns the configured output format.
func (p *Printer) Format() Format { return p.format }

// IsJSON returns true if the output format is JSON.
func (p *Printer) IsJSON() bool { return p.format == FormatJSON }

// AddOutputFlag adds the --output/-o flag to a cobra command.
func AddOutputFlag(cmd *cobra.Command) {
	cmd.Flags().StringP("output", "o", "table", "output format: table or json")
}

// FromCommand reads the --output flag and creates a Printer.
func FromCommand(cmd *cobra.Command) *Printer {
	format, _ := cmd.Flags().GetString("output")
	f := FormatTable
	if format == "json" {
		f = FormatJSON
	}
	return NewPrinter(cmd.OutOrStdout(), f)
}

// FromWriter creates a table-format Printer writing to the given writer.
func FromWriter(w io.Writer) *Printer {
	return NewPrinter(w, FormatTable)
}

// Stdout creates a table-format Printer writing to os.Stdout.
func Stdout() *Printer {
	return NewPrinter(os.Stdout, FormatTable)
}
