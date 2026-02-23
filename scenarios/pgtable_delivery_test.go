//go:build integration

package scenarios

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/florinutz/pgpipe/adapter/pgtable"
	"github.com/jackc/pgx/v5"
)

func TestScenario_PGTableDelivery(t *testing.T) {
	connStr := startPostgres(t)

	t.Run("happy path", func(t *testing.T) {
		tableName := "pgpipe_events_happy"
		createEventsTable(t, connStr, tableName)

		pa := pgtable.New(connStr, tableName, 0, 0, testLogger())
		startPipeline(t, connStr, []string{"pgtable_test_happy"}, pa)

		time.Sleep(500 * time.Millisecond)

		sendNotify(t, connStr, "pgtable_test_happy", `{"order_id":42}`)

		// Poll for the row.
		var count int
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			count = countEventsRows(t, connStr, tableName)
			if count > 0 {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}

		if count == 0 {
			t.Fatal("no rows inserted into events table")
		}

		// Verify row content.
		row := queryFirstEventRow(t, connStr, tableName)
		if row.channel != "pgtable_test_happy" {
			t.Errorf("channel = %q, want pgtable_test_happy", row.channel)
		}
		if row.source != "listen_notify" {
			t.Errorf("source = %q, want listen_notify", row.source)
		}
	})

	t.Run("reconnect after disconnect", func(t *testing.T) {
		tableName := "pgpipe_events_reconn"
		createEventsTable(t, connStr, tableName)

		pa := pgtable.New(connStr, tableName, 100*time.Millisecond, 500*time.Millisecond, testLogger())
		startPipeline(t, connStr, []string{"pgtable_test_reconn"}, pa)

		time.Sleep(500 * time.Millisecond)

		// Send first event.
		sendNotify(t, connStr, "pgtable_test_reconn", `{"n":1}`)
		time.Sleep(500 * time.Millisecond)

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
	`, tableName)
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
	err = conn.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count)
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
	err = conn.QueryRow(ctx, fmt.Sprintf("SELECT id, channel, operation, source FROM %s LIMIT 1", tableName)).
		Scan(&row.id, &row.channel, &row.operation, &row.source)
	if err != nil {
		t.Fatalf("queryFirstEventRow: %v", err)
	}
	return row
}
