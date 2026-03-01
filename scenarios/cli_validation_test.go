//go:build integration

package scenarios

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestScenario_CLIValidation(t *testing.T) {
	t.Run("missing --db", func(t *testing.T) {
		output, err := runPGCDC("listen", "--channel", "orders")
		if err == nil {
			t.Fatal("expected error for missing --db")
		}
		if !strings.Contains(output, "database_url is required") {
			t.Errorf("unexpected output: %s", output)
		}
	})

	t.Run("missing --channel", func(t *testing.T) {
		output, err := runPGCDC("listen", "--db", "postgres://localhost/test")
		if err == nil {
			t.Fatal("expected error for missing --channel")
		}
		if !strings.Contains(output, "no channels") {
			t.Errorf("unexpected output: %s", output)
		}
	})

	t.Run("unknown adapter", func(t *testing.T) {
		output, err := runPGCDC("listen", "--db", "postgres://localhost/test", "--channel", "orders", "--adapter", "nonexistent")
		if err == nil {
			t.Fatal("expected error for unknown adapter")
		}
		if !strings.Contains(output, "unknown adapter") {
			t.Errorf("unexpected output: %s", output)
		}
	})

	t.Run("tx-metadata requires WAL detector", func(t *testing.T) {
		output, err := runPGCDC("listen", "--db", "postgres://localhost/test", "--channel", "orders", "--tx-metadata")
		if err == nil {
			t.Fatal("expected error for --tx-metadata without WAL")
		}
		if !strings.Contains(output, "tx-metadata requires --detector wal") {
			t.Errorf("unexpected output: %s", output)
		}
	})

	t.Run("tx-markers requires WAL detector", func(t *testing.T) {
		output, err := runPGCDC("listen", "--db", "postgres://localhost/test", "--channel", "orders", "--tx-markers")
		if err == nil {
			t.Fatal("expected error for --tx-markers without WAL")
		}
		if !strings.Contains(output, "tx-markers requires --detector wal") {
			t.Errorf("unexpected output: %s", output)
		}
	})

	t.Run("webhook without --url", func(t *testing.T) {
		output, err := runPGCDC("listen", "--db", "postgres://localhost/test", "--channel", "orders", "--adapter", "webhook")
		if err == nil {
			t.Fatal("expected error for webhook without URL")
		}
		if !strings.Contains(output, "webhook.url is required") {
			t.Errorf("unexpected output: %s", output)
		}
	})

	t.Run("snapshot-first requires WAL detector", func(t *testing.T) {
		output, err := runPGCDC("listen", "--db", "postgres://localhost/test", "--channel", "orders", "--snapshot-first", "--snapshot-table", "orders")
		if err == nil {
			t.Fatal("expected error for --snapshot-first without WAL")
		}
		if !strings.Contains(output, "snapshot-first requires --detector wal") {
			t.Errorf("unexpected output: %s", output)
		}
	})

	t.Run("snapshot-first requires --snapshot-table", func(t *testing.T) {
		output, err := runPGCDC("listen", "--db", "postgres://localhost/test", "--detector", "wal", "--publication", "p", "--snapshot-first")
		if err == nil {
			t.Fatal("expected error for --snapshot-first without --snapshot-table")
		}
		if !strings.Contains(output, "snapshot-first requires a snapshot table") {
			t.Errorf("unexpected output: %s", output)
		}
	})

	t.Run("trigger SQL generates valid SQL", func(t *testing.T) {
		connStr := startPostgres(t)

		output, err := runPGCDC("init", "--table", "trigger_test_orders")
		if err != nil {
			t.Fatalf("pgcdc init: %v\noutput: %s", err, output)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		conn, err := pgx.Connect(ctx, connStr)
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		defer conn.Close(ctx)

		_, err = conn.Exec(ctx, "CREATE TABLE trigger_test_orders (id SERIAL PRIMARY KEY, data JSONB DEFAULT '{}')")
		if err != nil {
			t.Fatalf("create table: %v", err)
		}

		_, err = conn.Exec(ctx, output)
		if err != nil {
			t.Fatalf("execute trigger SQL: %v\nSQL: %s", err, output)
		}

		_, err = conn.Exec(ctx, `LISTEN "pgcdc:trigger_test_orders"`)
		if err != nil {
			t.Fatalf("listen: %v", err)
		}

		_, err = conn.Exec(ctx, `INSERT INTO trigger_test_orders (data) VALUES ('{"item":"widget"}')`)
		if err != nil {
			t.Fatalf("insert: %v", err)
		}

		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			t.Fatalf("wait for notification: %v", err)
		}

		if notification.Channel != "pgcdc:trigger_test_orders" {
			t.Errorf("channel = %q, want %q", notification.Channel, "pgcdc:trigger_test_orders")
		}

		var payload map[string]json.RawMessage
		if err := json.Unmarshal([]byte(notification.Payload), &payload); err != nil {
			t.Fatalf("payload not JSON: %v\nraw: %s", err, notification.Payload)
		}
		var op string
		json.Unmarshal(payload["op"], &op)
		if op != "INSERT" {
			t.Errorf("op = %q, want INSERT", op)
		}
	})

	t.Run("init rejects invalid table name", func(t *testing.T) {
		output, err := runPGCDC("init", "--table", "123-invalid!")
		if err == nil {
			t.Fatal("expected error for invalid table name")
		}
		if !strings.Contains(output, "invalid table name") {
			t.Errorf("unexpected output: %s", output)
		}
	})

	t.Run("init rejects invalid channel name", func(t *testing.T) {
		output, err := runPGCDC("init", "--table", "orders", "--channel", "bad channel!")
		if err == nil {
			t.Fatal("expected error for invalid channel name")
		}
		if !strings.Contains(output, "invalid channel name") {
			t.Errorf("unexpected output: %s", output)
		}
	})
}
