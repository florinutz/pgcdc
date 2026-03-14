package middleware

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/testutil"
)

func TestTimeout_Success(t *testing.T) {
	inner := func(ctx context.Context, ev event.Event) error {
		return nil
	}

	chain := Timeout(100 * time.Millisecond)(inner)
	err := chain(context.Background(), testutil.MakeEvent("pgcdc:test", `{"row":{"id":"1"}}`))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestTimeout_Exceeded(t *testing.T) {
	inner := func(ctx context.Context, ev event.Event) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
			return nil
		}
	}

	chain := Timeout(10 * time.Millisecond)(inner)
	err := chain(context.Background(), testutil.MakeEvent("pgcdc:test", `{"row":{"id":"1"}}`))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "delivery timeout") {
		t.Fatalf("expected delivery timeout error, got %v", err)
	}
}

func TestTimeout_ParentCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	inner := func(ctx context.Context, ev event.Event) error {
		return ctx.Err()
	}

	chain := Timeout(100 * time.Millisecond)(inner)
	err := chain(ctx, testutil.MakeEvent("pgcdc:test", `{"row":{"id":"1"}}`))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
