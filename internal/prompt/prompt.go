package prompt

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Prompter handles interactive terminal prompts.
type Prompter struct {
	r *bufio.Scanner
	w io.Writer
}

// New creates a Prompter that reads from r and writes prompts to w.
func New(r io.Reader, w io.Writer) *Prompter {
	return &Prompter{r: bufio.NewScanner(r), w: w}
}

// Select prints numbered options and reads a single choice. Returns the
// zero-based index of the selected option. def is the default index used
// when the user presses Enter without typing a number.
func (p *Prompter) Select(label string, opts []string, def int) (int, error) {
	fmt.Fprintf(p.w, "%s:\n", label)
	for i, opt := range opts {
		defMark := ""
		if i == def {
			defMark = " (default)"
		}
		fmt.Fprintf(p.w, "  %d) %s%s\n", i+1, opt, defMark)
	}
	fmt.Fprintf(p.w, "Enter 1-%d: ", len(opts))

	if !p.r.Scan() {
		if err := p.r.Err(); err != nil {
			return def, fmt.Errorf("read input: %w", err)
		}
		return def, nil
	}

	line := strings.TrimSpace(p.r.Text())
	if line == "" {
		return def, nil
	}

	n, err := strconv.Atoi(line)
	if err != nil || n < 1 || n > len(opts) {
		return def, fmt.Errorf("invalid choice %q: expected 1-%d", line, len(opts))
	}
	return n - 1, nil
}

// MultiSelect prints numbered options and reads space-separated choices.
// Returns zero-based indices of selected options.
func (p *Prompter) MultiSelect(label string, opts []string) ([]int, error) {
	fmt.Fprintf(p.w, "%s (space-separated numbers):\n", label)
	for i, opt := range opts {
		fmt.Fprintf(p.w, "  %d) %s\n", i+1, opt)
	}
	fmt.Fprintf(p.w, "Enter choices: ")

	if !p.r.Scan() {
		if err := p.r.Err(); err != nil {
			return nil, fmt.Errorf("read input: %w", err)
		}
		return nil, nil
	}

	line := strings.TrimSpace(p.r.Text())
	if line == "" {
		return nil, nil
	}

	parts := strings.Fields(line)
	var selected []int
	for _, part := range parts {
		n, err := strconv.Atoi(part)
		if err != nil || n < 1 || n > len(opts) {
			return nil, fmt.Errorf("invalid choice %q: expected 1-%d", part, len(opts))
		}
		selected = append(selected, n-1)
	}
	return selected, nil
}

// Confirm prints a y/n prompt and returns the boolean answer.
// def is the default when the user presses Enter without typing.
func (p *Prompter) Confirm(label string, def bool) (bool, error) {
	hint := "[y/N]"
	if def {
		hint = "[Y/n]"
	}
	fmt.Fprintf(p.w, "%s %s: ", label, hint)

	if !p.r.Scan() {
		if err := p.r.Err(); err != nil {
			return def, fmt.Errorf("read input: %w", err)
		}
		return def, nil
	}

	line := strings.TrimSpace(strings.ToLower(p.r.Text()))
	if line == "" {
		return def, nil
	}

	switch line {
	case "y", "yes":
		return true, nil
	case "n", "no":
		return false, nil
	default:
		return def, fmt.Errorf("invalid input %q: expected y or n", line)
	}
}
