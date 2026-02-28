package cel

import (
	"fmt"

	"github.com/google/cel-go/cel"

	"github.com/florinutz/pgcdc/event"
)

// Program is a compiled CEL expression ready for evaluation.
type Program struct {
	program cel.Program
}

// Compile compiles a CEL expression string into a Program.
// Available variables in expressions:
//   - channel (string): event channel
//   - operation (string): event operation (INSERT, UPDATE, DELETE, etc.)
//   - source (string): event source
//   - id (string): event ID
//   - has_record (bool): whether event has structured record
//   - table (string): table name from metadata (empty if not available)
//   - schema_name (string): schema name from metadata (empty if not available)
func Compile(expr string) (*Program, error) {
	env, err := cel.NewEnv(
		cel.Variable("channel", cel.StringType),
		cel.Variable("operation", cel.StringType),
		cel.Variable("source", cel.StringType),
		cel.Variable("id", cel.StringType),
		cel.Variable("has_record", cel.BoolType),
		cel.Variable("table", cel.StringType),
		cel.Variable("schema_name", cel.StringType),
	)
	if err != nil {
		return nil, fmt.Errorf("create cel env: %w", err)
	}

	ast, issues := env.Compile(expr)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("compile cel expression: %w", issues.Err())
	}

	// Verify the result type is bool.
	if ast.OutputType() != cel.BoolType {
		return nil, fmt.Errorf("cel expression must return bool, got %v", ast.OutputType())
	}

	prg, err := env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("program cel expression: %w", err)
	}

	return &Program{program: prg}, nil
}

// Eval evaluates the expression against an event. Returns true if the
// expression evaluates to true, false otherwise (including errors).
func (p *Program) Eval(ev event.Event) (bool, error) {
	vars := map[string]any{
		"channel":     ev.Channel,
		"operation":   ev.Operation,
		"source":      ev.Source,
		"id":          ev.ID,
		"has_record":  ev.HasRecord(),
		"table":       "",
		"schema_name": "",
	}

	// Extract metadata if Record is available.
	if rec := ev.Record(); rec != nil {
		if t, ok := rec.Metadata[event.MetaTable]; ok {
			vars["table"] = t
		}
		if s, ok := rec.Metadata[event.MetaSchema]; ok {
			vars["schema_name"] = s
		}
	}

	out, _, err := p.program.Eval(vars)
	if err != nil {
		return false, fmt.Errorf("eval cel: %w", err)
	}

	b, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("cel result is not bool: %T", out.Value())
	}
	return b, nil
}
