package adapter

import (
	"context"

	"github.com/florinutz/pgpipe/event"
)

type Adapter interface {
	Start(ctx context.Context, events <-chan event.Event) error
	Name() string
}
