package detector

import (
	"context"

	"github.com/florinutz/pgpipe/event"
)

type Detector interface {
	Start(ctx context.Context, events chan<- event.Event) error
	Name() string
}
