//go:build !integration

package view

import (
	"testing"
)

func FuzzParse(f *testing.F) {
	// Seed corpus: valid queries from existing tests.
	seeds := []string{
		"SELECT COUNT(*) as order_count, payload.region FROM pgcdc_events WHERE channel = 'pgcdc:orders' GROUP BY payload.region TUMBLING WINDOW 1m",
		"SELECT COUNT(*) as n FROM pgcdc_events TUMBLING WINDOW 30s",
		"SELECT COUNT(*) as cnt, SUM(payload.amount) as total, AVG(payload.amount) as avg_amount FROM pgcdc_events WHERE operation = 'INSERT' GROUP BY payload.region TUMBLING WINDOW 5m",
		"SELECT COUNT(*) as cnt FROM pgcdc_events GROUP BY payload.region HAVING COUNT(*) > 5 TUMBLING WINDOW 1m",
		"SELECT COUNT(*) as n FROM pgcdc_events SLIDING WINDOW 5m SLIDE 1m",
		"SELECT COUNT(*) as n FROM pgcdc_events SESSION WINDOW 30s",
		"SELECT COUNT(*) as n FROM pgcdc_events ALLOWED LATENESS 10s TUMBLING WINDOW 1m",
		"SELECT COUNT(DISTINCT payload.user_id) as unique_users FROM pgcdc_events TUMBLING WINDOW 1m",
		"SELECT STDDEV(payload.amount) as amount_stddev FROM pgcdc_events TUMBLING WINDOW 1m",
		"SELECT MIN(payload.price) as min_price, MAX(payload.price) as max_price FROM pgcdc_events TUMBLING WINDOW 1m",
		"SELECT COUNT(*) as n FROM pgcdc_events WHERE channel = 'pgcdc:orders' AND (operation = 'INSERT' OR operation = 'UPDATE') TUMBLING WINDOW 1m",
		"SELECT a.payload, b.payload FROM pgcdc_events a JOIN pgcdc_events b ON a.channel = b.channel WITHIN 30s WHERE a.channel = 'left' AND b.channel = 'right'",
		"SELECT COUNT(*) as n FROM pgcdc_events EVENT TIME BY payload.ts ALLOWED LATENESS 5s TUMBLING WINDOW 10s GROUP BY channel",
		"SELECT AVG(payload.value) FROM pgcdc_events SLIDING WINDOW 1m SLIDE 30s GROUP BY channel",
		// Invalid and edge-case inputs.
		"",
		"not a sql query",
		"SELECT * FROM other_table",
		"SELECT COUNT(*) FROM pgcdc_events",
		"DROP TABLE pgcdc_events",
		"SELECT COUNT(*) FROM pgcdc_events TUMBLING WINDOW",
		"INSERT INTO pgcdc_events VALUES (1) TUMBLING WINDOW 1m",
		"SELECT COUNT(*) FROM pgcdc_events TUMBLING WINDOW xyz",
		"SELECT COUNT(*) FROM pgcdc_events TUMBLING WINDOW -1s",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, query string) {
		// Must never panic; errors are expected for bad input.
		_, _ = Parse("fuzz", query, EmitRow, 0)
	})
}
