package output

import (
	"fmt"
	"text/tabwriter"
)

// Table writes a formatted table with the given headers and rows.
func (p *Printer) Table(headers []string, rows [][]string) error {
	w := tabwriter.NewWriter(p.w, 0, 4, 2, ' ', 0)

	// Print header row.
	for i, h := range headers {
		if i > 0 {
			fmt.Fprint(w, "\t")
		}
		fmt.Fprint(w, h)
	}
	fmt.Fprintln(w)

	// Print separator row.
	for i, h := range headers {
		if i > 0 {
			fmt.Fprint(w, "\t")
		}
		for range len(h) {
			fmt.Fprint(w, "-")
		}
	}
	fmt.Fprintln(w)

	// Print data rows.
	for _, row := range rows {
		for i, cell := range row {
			if i > 0 {
				fmt.Fprint(w, "\t")
			}
			fmt.Fprint(w, cell)
		}
		fmt.Fprintln(w)
	}

	return w.Flush()
}
