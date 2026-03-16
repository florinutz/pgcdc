package ws_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/florinutz/pgcdc/adapter/adaptertest"
	"github.com/florinutz/pgcdc/adapter/ws"
	"github.com/florinutz/pgcdc/event"
)

func TestWSAdapter_Contract(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	adaptertest.RunContractTests(t, "ws", func(ctx context.Context, ch <-chan event.Event) error {
		return ws.New(100, 0, 0, logger).Start(ctx, ch)
	})
}
