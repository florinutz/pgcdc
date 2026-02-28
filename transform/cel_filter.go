package transform

import (
	"fmt"

	pgcel "github.com/florinutz/pgcdc/cel"
	"github.com/florinutz/pgcdc/event"
)

// FilterCEL returns a transform that drops events where the CEL expression
// evaluates to false. Events that cause evaluation errors are passed through
// unchanged.
func FilterCEL(expr string) (TransformFunc, error) {
	prg, err := pgcel.Compile(expr)
	if err != nil {
		return nil, fmt.Errorf("compile filter expression: %w", err)
	}
	return func(ev event.Event) (event.Event, error) {
		ok, err := prg.Eval(ev)
		if err != nil {
			return ev, nil // pass through on error
		}
		if !ok {
			return ev, ErrDropEvent
		}
		return ev, nil
	}, nil
}
