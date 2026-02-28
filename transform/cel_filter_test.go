package transform

import (
	"errors"
	"testing"
)

func TestFilterCEL_MatchingExpression(t *testing.T) {
	fn, err := FilterCEL(`operation == "INSERT"`)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ev := makeEvent(t, "INSERT", map[string]any{"name": "Alice"})
	result, err := fn(ev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ID != ev.ID {
		t.Errorf("expected event to pass through unchanged")
	}
}

func TestFilterCEL_NonMatchingExpression(t *testing.T) {
	fn, err := FilterCEL(`operation == "DELETE"`)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ev := makeEvent(t, "INSERT", map[string]any{"name": "Alice"})
	_, err = fn(ev)
	if !errors.Is(err, ErrDropEvent) {
		t.Errorf("expected ErrDropEvent, got %v", err)
	}
}

func TestFilterCEL_ChannelFilter(t *testing.T) {
	fn, err := FilterCEL(`channel == "test"`)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ev := makeEvent(t, "INSERT", map[string]any{"name": "Alice"})
	_, err = fn(ev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFilterCEL_CompoundExpression(t *testing.T) {
	fn, err := FilterCEL(`operation == "INSERT" && channel == "test"`)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ev := makeEvent(t, "INSERT", map[string]any{"name": "Alice"})
	_, err = fn(ev)
	if err != nil {
		t.Fatalf("expected pass, got: %v", err)
	}

	ev.Channel = "other"
	_, err = fn(ev)
	if !errors.Is(err, ErrDropEvent) {
		t.Errorf("expected ErrDropEvent for non-matching channel, got %v", err)
	}
}

func TestFilterCEL_InvalidExpression(t *testing.T) {
	_, err := FilterCEL(`this is not valid CEL !!!`)
	if err == nil {
		t.Fatal("expected error for invalid expression")
	}
}

func TestFilterCEL_NonBoolExpression(t *testing.T) {
	_, err := FilterCEL(`channel`)
	if err == nil {
		t.Fatal("expected error for non-bool expression")
	}
}
