//go:build !integration

package cel

import (
	"testing"
)

func FuzzCompile(f *testing.F) {
	// Seed corpus: valid CEL expressions.
	seeds := []string{
		`channel == "orders"`,
		`operation == "INSERT"`,
		`channel == "orders" && operation == "INSERT"`,
		`has_record == true`,
		`table == "users"`,
		`channel.startsWith("pg")`,
		`operation == "UPDATE" || operation == "DELETE"`,
		`schema_name == "public" && table == "events"`,
		`id != ""`,
		`source == "wal"`,
		// Invalid and edge-case inputs.
		"",
		"not valid cel",
		"channel ==",
		"(((",
		"1/0",
		"channel + 1",
		"unknown_var == true",
		`channel == "x" &&`,
		`"unclosed string`,
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, expr string) {
		// Must never panic; errors are expected for bad input.
		_, _ = Compile(expr)
	})
}
