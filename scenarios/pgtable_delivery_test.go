//go:build integration

package scenarios

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/adapter/pgtable"
	"github.com/jackc/pgx/v5"
)

func TestScenario_PGTableDelivery(t *testing.T) {
	connStr := startPostgres(t)

	t.Run("happy path", func(t *testing.T) {
		tableName := "pgcdc_events_happy"
		createEventsTable(t, connStr, tableName)

		pa := pgtable.New(connStr, tableName, 0, 0, testLogger())
		startPipeline(t, connStr, []string{"pgtable_test_happy"}, pa)

		// Wait for detector by sending probes until a row appears.
		waitFor(t, 10*time.Second, func() bool {
			sendNotify(t, connStr, "pgtable_test_happy", `{"__probe":true}`)
			time.Sleep(100 * time.Millisecond)
			return countEventsRows(t, connStr, tableName) > 0
		})

		sendNotify(t, connStr, "pgtable_test_happy", `{"order_id":42}`)

		// Poll for the real row (at least 2: probe + real).
		var count int
		waitFor(t, 5*time.Second, func() bool {
			count = countEventsRows(t, connStr, tableName)
			return count >= 2
		})

		if count < 2 {
			t.Fatal("expected at least 2 rows (probe + real) in events table")
		}

		// Verify a row has correct content.
		row := queryFirstEventRow(t, connStr, tableName)
		if row.channel != "pgtable_test_happy" {
			t.Errorf("channel = %q, want pgtable_test_happy", row.channel)
		}
		if row.source != "listen_notify" {
			t.Errorf("source = %q, want listen_notify", row.source)
		}
	})

	t.Run("constraint violation skipped", func(t *testing.T) {
		tableName := "pgcdc_events_check"
		createEventsTable(t, connStr, tableName)

		// Add a CHECK constraint that rejects a specific channel.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		conn, err := pgx.Connect(ctx, connStr)
		if err != nil {
			t.Fatalf("connect for ALTER: %v", err)
		}
		alterSQL := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT reject_check CHECK (channel != 'pgtable_reject')",
			pgx.Identifier{tableName}.Sanitize())
		if _, err := conn.Exec(ctx, alterSQL); err != nil {
			t.Fatalf("ALTER TABLE: %v", err)
		}
		conn.Close(ctx)

		pa := pgtable.New(connStr, tableName, 0, 0, testLogger())
		startPipeline(t, connStr, []string{"pgtable_reject", "pgtable_ok"}, pa)

		// Wait for detector by sending a probe on the OK channel.
		waitFor(t, 10*time.Second, func() bool {
			sendNotify(t, connStr, "pgtable_ok", `{"__probe":true}`)
			time.Sleep(100 * time.Millisecond)
			return countEventsRows(t, connStr, tableName) > 0
		})
		probeCount := countEventsRows(t, connStr, tableName)

		// Event on rejected channel: INSERT will fail (CHECK violation), adapter should skip.
		sendNotify(t, connStr, "pgtable_reject", `{"n":1}`)

		// Event on OK channel: INSERT should succeed.
		sendNotify(t, connStr, "pgtable_ok", `{"n":2}`)

		// Poll for the new row.
		var count int
		waitFor(t, 5*time.Second, func() bool {
			count = countEventsRows(t, connStr, tableName)
			return count > probeCount
		})

		// Only the OK event should have been added (rejected one skipped).
		if count != probeCount+1 {
			t.Fatalf("expected %d rows (probes + 1 OK), got %d", probeCount+1, count)
		}

		row := queryFirstEventRow(t, connStr, tableName)
		if row.channel != "pgtable_ok" {
			t.Errorf("channel = %q, want pgtable_ok", row.channel)
		}
	})

	t.Run("reconnect after disconnect", func(t *testing.T) {
		tableName := "pgcdc_events_reconn"
		createEventsTable(t, connStr, tableName)

		pa := pgtable.New(connStr, tableName, 100*time.Millisecond, 500*time.Millisecond, testLogger())
		startPipeline(t, connStr, []string{"pgtable_test_reconn"}, pa)

		// Wait for detector by sending probes until a row appears.
		waitFor(t, 10*time.Second, func() bool {
			sendNotify(t, connStr, "pgtable_test_reconn", `{"n":1}`)
			time.Sleep(100 * time.Millisecond)
			return countEventsRows(t, connStr, tableName) > 0
		})

		// Terminate all backends (simulates connection drop for both detector and adapter).
		terminateBackends(t, connStr)

		// Wait for both detector and adapter to reconnect.
		// The detector reconnects with backoff (default 5s base), so we need to
		// keep retrying the second notify until it's picked up.
		deadline := time.Now().Add(15 * time.Second)
		var count int
		for time.Now().Before(deadline) {
			// Keep sending notifies — only one received after reconnect is enough.
			sendNotify(t, connStr, "pgtable_test_reconn", `{"n":2}`)
			time.Sleep(500 * time.Millisecond)

			count = countEventsRows(t, connStr, tableName)
			if count >= 2 {
				break
			}
		}

		if count < 2 {
			t.Fatalf("expected at least 2 rows after reconnect, got %d", count)
		}
	})
}

// ─── PG table helpers ────────────────────────────────────────────────────────

func createEventsTable(t *testing.T, connStr, tableName string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("createEventsTable connect: %v", err)
	}
	defer conn.Close(ctx)

	sql := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id TEXT PRIMARY KEY,
			channel TEXT NOT NULL,
			operation TEXT NOT NULL,
			payload JSONB NOT NULL,
			source TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL
		)
	`, pgx.Identifier{tableName}.Sanitize())
	if _, err := conn.Exec(ctx, sql); err != nil {
		t.Fatalf("createEventsTable: %v", err)
	}
}

type eventRow struct {
	id        string
	channel   string
	operation string
	source    string
}

func countEventsRows(t *testing.T, connStr, tableName string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("countEventsRows connect: %v", err)
	}
	defer conn.Close(ctx)

	var count int
	err = conn.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", pgx.Identifier{tableName}.Sanitize())).Scan(&count)
	if err != nil {
		t.Fatalf("countEventsRows: %v", err)
	}
	return count
}

func queryFirstEventRow(t *testing.T, connStr, tableName string) eventRow {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("queryFirstEventRow connect: %v", err)
	}
	defer conn.Close(ctx)

	var row eventRow
	err = conn.QueryRow(ctx, fmt.Sprintf("SELECT id, channel, operation, source FROM %s LIMIT 1", pgx.Identifier{tableName}.Sanitize())).
		Scan(&row.id, &row.channel, &row.operation, &row.source)
	if err != nil {
		t.Fatalf("queryFirstEventRow: %v", err)
	}
	return row
}
