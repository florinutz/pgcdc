package prompt

import (
	"bytes"
	"strings"
	"testing"
)

func TestSelect_ValidChoice(t *testing.T) {
	t.Parallel()
	p := New(strings.NewReader("2\n"), &bytes.Buffer{})
	idx, err := p.Select("Pick", []string{"a", "b", "c"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if idx != 1 {
		t.Errorf("expected index 1, got %d", idx)
	}
}

func TestSelect_EmptyUsesDefault(t *testing.T) {
	t.Parallel()
	p := New(strings.NewReader("\n"), &bytes.Buffer{})
	idx, err := p.Select("Pick", []string{"a", "b"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if idx != 1 {
		t.Errorf("expected default index 1, got %d", idx)
	}
}

func TestSelect_OutOfRange(t *testing.T) {
	t.Parallel()
	p := New(strings.NewReader("5\n"), &bytes.Buffer{})
	_, err := p.Select("Pick", []string{"a", "b"}, 0)
	if err == nil {
		t.Fatal("expected error for out-of-range choice")
	}
	if !strings.Contains(err.Error(), "invalid choice") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSelect_NonNumeric(t *testing.T) {
	t.Parallel()
	p := New(strings.NewReader("abc\n"), &bytes.Buffer{})
	_, err := p.Select("Pick", []string{"a", "b"}, 0)
	if err == nil {
		t.Fatal("expected error for non-numeric input")
	}
	if !strings.Contains(err.Error(), "invalid choice") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSelect_EOF(t *testing.T) {
	t.Parallel()
	p := New(strings.NewReader(""), &bytes.Buffer{})
	idx, err := p.Select("Pick", []string{"a", "b"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if idx != 0 {
		t.Errorf("expected default index 0 on EOF, got %d", idx)
	}
}

func TestSelect_PromptsRendered(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	p := New(strings.NewReader("1\n"), &buf)
	_, _ = p.Select("Color", []string{"red", "blue"}, 0)

	out := buf.String()
	if !strings.Contains(out, "Color:") {
		t.Errorf("expected label in output: %s", out)
	}
	if !strings.Contains(out, "1) red (default)") {
		t.Errorf("expected default marker: %s", out)
	}
	if !strings.Contains(out, "2) blue") {
		t.Errorf("expected option 2: %s", out)
	}
}

func TestMultiSelect_ValidChoices(t *testing.T) {
	t.Parallel()
	p := New(strings.NewReader("1 3\n"), &bytes.Buffer{})
	selected, err := p.MultiSelect("Pick", []string{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 2 || selected[0] != 0 || selected[1] != 2 {
		t.Errorf("expected [0,2], got %v", selected)
	}
}

func TestMultiSelect_Empty(t *testing.T) {
	t.Parallel()
	p := New(strings.NewReader("\n"), &bytes.Buffer{})
	selected, err := p.MultiSelect("Pick", []string{"a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	if selected != nil {
		t.Errorf("expected nil for empty input, got %v", selected)
	}
}

func TestMultiSelect_InvalidNumber(t *testing.T) {
	t.Parallel()
	p := New(strings.NewReader("1 x\n"), &bytes.Buffer{})
	_, err := p.MultiSelect("Pick", []string{"a", "b"})
	if err == nil {
		t.Fatal("expected error for invalid number")
	}
	if !strings.Contains(err.Error(), "invalid choice") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestMultiSelect_EOF(t *testing.T) {
	t.Parallel()
	p := New(strings.NewReader(""), &bytes.Buffer{})
	selected, err := p.MultiSelect("Pick", []string{"a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	if selected != nil {
		t.Errorf("expected nil on EOF, got %v", selected)
	}
}

func TestConfirm_Yes(t *testing.T) {
	t.Parallel()
	for _, input := range []string{"y\n", "yes\n"} {
		p := New(strings.NewReader(input), &bytes.Buffer{})
		result, err := p.Confirm("Continue?", false)
		if err != nil {
			t.Fatal(err)
		}
		if !result {
			t.Errorf("expected true for input %q", input)
		}
	}
}

func TestConfirm_No(t *testing.T) {
	t.Parallel()
	for _, input := range []string{"n\n", "no\n"} {
		p := New(strings.NewReader(input), &bytes.Buffer{})
		result, err := p.Confirm("Continue?", true)
		if err != nil {
			t.Fatal(err)
		}
		if result {
			t.Errorf("expected false for input %q", input)
		}
	}
}

func TestConfirm_EmptyDefaultTrue(t *testing.T) {
	t.Parallel()
	p := New(strings.NewReader("\n"), &bytes.Buffer{})
	result, err := p.Confirm("Continue?", true)
	if err != nil {
		t.Fatal(err)
	}
	if !result {
		t.Error("expected default true on empty input")
	}
}

func TestConfirm_EmptyDefaultFalse(t *testing.T) {
	t.Parallel()
	p := New(strings.NewReader("\n"), &bytes.Buffer{})
	result, err := p.Confirm("Continue?", false)
	if err != nil {
		t.Fatal(err)
	}
	if result {
		t.Error("expected default false on empty input")
	}
}

func TestConfirm_Invalid(t *testing.T) {
	t.Parallel()
	p := New(strings.NewReader("maybe\n"), &bytes.Buffer{})
	_, err := p.Confirm("Continue?", false)
	if err == nil {
		t.Fatal("expected error for invalid input")
	}
	if !strings.Contains(err.Error(), "invalid input") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestConfirm_HintRendered(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	p := New(strings.NewReader("y\n"), &buf)
	_, _ = p.Confirm("OK?", true)
	if !strings.Contains(buf.String(), "[Y/n]") {
		t.Errorf("expected [Y/n] hint, got: %s", buf.String())
	}

	buf.Reset()
	p = New(strings.NewReader("n\n"), &buf)
	_, _ = p.Confirm("OK?", false)
	if !strings.Contains(buf.String(), "[y/N]") {
		t.Errorf("expected [y/N] hint, got: %s", buf.String())
	}
}
