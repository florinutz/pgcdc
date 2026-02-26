package safegoroutine

import (
	"errors"
	"strings"
	"testing"

	"golang.org/x/sync/errgroup"
)

func TestGo_NormalExecution(t *testing.T) {
	var g errgroup.Group
	Go(&g, nil, "test", func() error {
		return nil
	})
	if err := g.Wait(); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestGo_PanicRecovery(t *testing.T) {
	var g errgroup.Group
	Go(&g, nil, "panicker", func() error {
		panic("something went wrong")
	})
	err := g.Wait()
	if err == nil {
		t.Fatal("expected error from panic recovery")
	}
	if !strings.Contains(err.Error(), "panic in panicker") {
		t.Fatalf("expected panic message in error, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "something went wrong") {
		t.Fatalf("expected panic value in error, got %q", err.Error())
	}
}

func TestGo_NilPanicRecovery(t *testing.T) {
	var g errgroup.Group
	Go(&g, nil, "nil-panicker", func() error {
		panic(nil)
	})
	err := g.Wait()
	if err == nil {
		t.Fatal("expected error from nil panic recovery")
	}
	if !strings.Contains(err.Error(), "panic in nil-panicker") {
		t.Fatalf("expected panic message in error, got %q", err.Error())
	}
}

func TestGo_ErrorPropagation(t *testing.T) {
	var g errgroup.Group
	expected := errors.New("task failed")
	Go(&g, nil, "failing", func() error {
		return expected
	})
	err := g.Wait()
	if !errors.Is(err, expected) {
		t.Fatalf("expected %v, got %v", expected, err)
	}
}
