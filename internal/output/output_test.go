package output

import (
	"bytes"
	"strings"
	"testing"
)

func TestPrinterTable(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	p := NewPrinter(&buf, FormatTable)

	headers := []string{"NAME", "STATUS", "DETAIL"}
	rows := [][]string{
		{"database", "OK", "connected"},
		{"config", "FAIL", "missing required field"},
	}

	if err := p.Table(headers, rows); err != nil {
		t.Fatal(err)
	}

	out := buf.String()

	// Check headers present.
	if !strings.Contains(out, "NAME") || !strings.Contains(out, "STATUS") || !strings.Contains(out, "DETAIL") {
		t.Errorf("missing headers in output:\n%s", out)
	}

	// Check separator row.
	if !strings.Contains(out, "----") {
		t.Errorf("missing separator in output:\n%s", out)
	}

	// Check data rows.
	if !strings.Contains(out, "database") || !strings.Contains(out, "OK") || !strings.Contains(out, "connected") {
		t.Errorf("missing first row in output:\n%s", out)
	}
	if !strings.Contains(out, "config") || !strings.Contains(out, "FAIL") {
		t.Errorf("missing second row in output:\n%s", out)
	}
}

func TestPrinterJSON(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	p := NewPrinter(&buf, FormatJSON)

	data := map[string]string{"name": "test", "status": "ok"}
	if err := p.JSON(data); err != nil {
		t.Fatal(err)
	}

	out := buf.String()

	if !strings.Contains(out, `"name": "test"`) {
		t.Errorf("missing field in JSON output:\n%s", out)
	}
	if !strings.Contains(out, `"status": "ok"`) {
		t.Errorf("missing field in JSON output:\n%s", out)
	}
}

func TestPrinterIsJSON(t *testing.T) {
	t.Parallel()

	json := NewPrinter(nil, FormatJSON)
	table := NewPrinter(nil, FormatTable)

	if !json.IsJSON() {
		t.Error("expected IsJSON() to return true for JSON format")
	}
	if table.IsJSON() {
		t.Error("expected IsJSON() to return false for table format")
	}
}

func TestTableEmptyRows(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	p := NewPrinter(&buf, FormatTable)

	if err := p.Table([]string{"A", "B"}, nil); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 { // header + separator only
		t.Errorf("expected 2 lines for empty table, got %d:\n%s", len(lines), out)
	}
}
