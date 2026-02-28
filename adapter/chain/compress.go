package chain

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"

	"github.com/florinutz/pgcdc/event"
)

// Compress is a Link that gzip-compresses the event payload.
type Compress struct {
	level int
}

// NewCompress creates a gzip compression link. Level defaults to
// gzip.DefaultCompression when <= 0.
func NewCompress(level int) *Compress {
	if level <= 0 {
		level = gzip.DefaultCompression
	}
	return &Compress{level: level}
}

func (c *Compress) Name() string { return "compress" }

func (c *Compress) Process(_ context.Context, ev event.Event) (event.Event, error) {
	if len(ev.Payload) == 0 {
		return ev, nil
	}

	ev.EnsurePayload()

	var buf bytes.Buffer
	w, err := gzip.NewWriterLevel(&buf, c.level)
	if err != nil {
		return ev, fmt.Errorf("create gzip writer: %w", err)
	}
	if _, err := w.Write(ev.Payload); err != nil {
		return ev, fmt.Errorf("gzip write: %w", err)
	}
	if err := w.Close(); err != nil {
		return ev, fmt.Errorf("gzip close: %w", err)
	}

	ev.Payload = buf.Bytes()
	return ev, nil
}
