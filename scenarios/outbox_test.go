//go:build integration

package scenarios

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/adapter/stdout"
	"github.com/florinutz/pgcdc/bus"
	"github.com/florinutz/pgcdc/detector/outbox"
	"github.com/florinutz/pgcdc/event"
	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"
)

func TestScenario_Outbox(t *testing.T) {
	connStr := startPostgres(t)

	t.Run("happy path", func(t *testing.T) {
		table := "outbox_happy_" + fmt.Sprintf("%d", time.Now().UnixNano())
		logger := testLogger()

		// Create the outbox table.
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			conn, err := pgx.Connect(ctx, connStr)
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			defer conn.Close(ctx)

			safeTable := pgx.Identifier{table}.Sanitize()
			sql := fmt.Sprintf(`CREATE TABLE %s (
				id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
				channel TEXT NOT NULL,
				operation TEXT NOT NULL DEFAULT 'NOTIFY',
				payload JSONB NOT NULL,
				created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
				processed_at TIMESTAMPTZ
			)`, safeTable)
			if _, err := conn.Exec(ctx, sql); err != nil {
				t.Fatalf("create outbox table: %v", err)
			}
		}()

		// Wire outbox detector -> bus -> stdout adapter.
		ctx, cancel := context.WithCancel(context.Background())
		det := outbox.New(connStr, table, 200*time.Millisecond, 10, false, 0, 0, logger)
		b := bus.New(64, logger)
		lc := newLineCapture()
		stdoutAdapter := stdout.New(lc, logger)

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error { return b.Start(gCtx) })
		g.Go(func() error { return det.Start(gCtx, b.Ingest()) })

		sub, err := b.Subscribe(stdoutAdapter.Name())
		if err != nil {
			cancel()
			t.Fatalf("subscribe: %v", err)
		}
		g.Go(func() error { return stdoutAdapter.Start(gCtx, sub) })

		t.Cleanup(func() {
			cancel()
			g.Wait()
		})

		// Wait for the detector to connect by inserting a probe row and waiting for it.
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, err := pgx.Connect(ctx, connStr)
			if err != nil {
				t.Fatalf("connect for probe: %v", err)
			}
			defer conn.Close(ctx)
			safeTable := pgx.Identifier{table}.Sanitize()
			_, err = conn.Exec(ctx,
				fmt.Sprintf(`INSERT INTO %s (channel, operation, payload) VALUES ($1, $2, $3)`, safeTable),
				"orders", "NOTIFY", `{"__probe":true}`,
			)
			if err != nil {
				t.Fatalf("insert probe row: %v", err)
			}
		}()
		lc.waitLine(t, 10*time.Second)

		// Insert rows into the outbox table.
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, err := pgx.Connect(ctx, connStr)
			if err != nil {
				t.Fatalf("connect for insert: %v", err)
			}
			defer conn.Close(ctx)

			safeTable := pgx.Identifier{table}.Sanitize()
			_, err = conn.Exec(ctx,
				fmt.Sprintf(`INSERT INTO %s (channel, operation, payload) VALUES ($1, $2, $3)`, safeTable),
				"orders", "INSERT", `{"id":1,"product":"widget"}`,
			)
			if err != nil {
				t.Fatalf("insert outbox row: %v", err)
			}
			_, err = conn.Exec(ctx,
				fmt.Sprintf(`INSERT INTO %s (channel, operation, payload) VALUES ($1, $2, $3)`, safeTable),
				"orders", "UPDATE", `{"id":1,"product":"gadget"}`,
			)
			if err != nil {
				t.Fatalf("insert outbox row 2: %v", err)
			}
		}()

		// Wait for both events to arrive at the stdout adapter.
		line1 := lc.waitLine(t, 10*time.Second)
		line2 := lc.waitLine(t, 10*time.Second)

		var ev1, ev2 event.Event
		if err := json.Unmarshal([]byte(line1), &ev1); err != nil {
			t.Fatalf("unmarshal event 1: %v\nraw: %s", err, line1)
		}
		if err := json.Unmarshal([]byte(line2), &ev2); err != nil {
			t.Fatalf("unmarshal event 2: %v\nraw: %s", err, line2)
		}

		// Verify first event.
		if ev1.Channel != "orders" {
			t.Errorf("event 1 channel = %q, want %q", ev1.Channel, "orders")
		}
		if ev1.Operation != "INSERT" {
			t.Errorf("event 1 operation = %q, want %q", ev1.Operation, "INSERT")
		}
		if ev1.Source != "outbox" {
			t.Errorf("event 1 source = %q, want %q", ev1.Source, "outbox")
		}
		if ev1.ID == "" {
			t.Error("event 1 ID is empty")
		}

		// Verify second event.
		if ev2.Channel != "orders" {
			t.Errorf("event 2 channel = %q, want %q", ev2.Channel, "orders")
		}
		if ev2.Operation != "UPDATE" {
			t.Errorf("event 2 operation = %q, want %q", ev2.Operation, "UPDATE")
		}
		if ev2.Source != "outbox" {
			t.Errorf("event 2 source = %q, want %q", ev2.Source, "outbox")
		}

		// Verify payload contents.
		var p1 map[string]any
		if err := json.Unmarshal(ev1.Payload, &p1); err != nil {
			t.Fatalf("unmarshal payload 1: %v", err)
		}
		if p1["product"] != "widget" {
			t.Errorf("payload 1 product = %v, want %q", p1["product"], "widget")
		}

		// Verify the outbox table is empty (rows deleted after processing).
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, err := pgx.Connect(ctx, connStr)
			if err != nil {
				t.Fatalf("connect for verify: %v", err)
			}
			defer conn.Close(ctx)

			var count int
			err = conn.QueryRow(ctx,
				fmt.Sprintf("SELECT count(*) FROM %s", pgx.Identifier{table}.Sanitize()),
			).Scan(&count)
			if err != nil {
				t.Fatalf("count outbox rows: %v", err)
			}
			if count != 0 {
				t.Errorf("outbox row count = %d, want 0 (rows should be deleted after processing)", count)
			}
		}()
	})

	t.Run("keep processed", func(t *testing.T) {
		table := "outbox_keep_" + fmt.Sprintf("%d", time.Now().UnixNano())
		logger := testLogger()

		// Create the outbox table.
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			conn, err := pgx.Connect(ctx, connStr)
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			defer conn.Close(ctx)

			safeTable := pgx.Identifier{table}.Sanitize()
			sql := fmt.Sprintf(`CREATE TABLE %s (
				id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
				channel TEXT NOT NULL,
				operation TEXT NOT NULL DEFAULT 'NOTIFY',
				payload JSONB NOT NULL,
				created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
				processed_at TIMESTAMPTZ
			)`, safeTable)
			if _, err := conn.Exec(ctx, sql); err != nil {
				t.Fatalf("create outbox table: %v", err)
			}
		}()

		// Wire outbox detector with keepProcessed=true.
		ctx, cancel := context.WithCancel(context.Background())
		det := outbox.New(connStr, table, 200*time.Millisecond, 10, true, 0, 0, logger)
		b := bus.New(64, logger)
		lc := newLineCapture()
		stdoutAdapter := stdout.New(lc, logger)

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error { return b.Start(gCtx) })
		g.Go(func() error { return det.Start(gCtx, b.Ingest()) })

		sub, err := b.Subscribe(stdoutAdapter.Name())
		if err != nil {
			cancel()
			t.Fatalf("subscribe: %v", err)
		}
		g.Go(func() error { return stdoutAdapter.Start(gCtx, sub) })

		t.Cleanup(func() {
			cancel()
			g.Wait()
		})

		// Wait for the detector to connect by inserting a probe row and waiting for it.
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, err := pgx.Connect(ctx, connStr)
			if err != nil {
				t.Fatalf("connect for probe: %v", err)
			}
			defer conn.Close(ctx)
			safeTable := pgx.Identifier{table}.Sanitize()
			_, err = conn.Exec(ctx,
				fmt.Sprintf(`INSERT INTO %s (channel, operation, payload) VALUES ($1, $2, $3)`, safeTable),
				"users", "NOTIFY", `{"__probe":true}`,
			)
			if err != nil {
				t.Fatalf("insert probe row: %v", err)
			}
		}()
		lc.waitLine(t, 10*time.Second)

		// Insert a row into the outbox table.
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, err := pgx.Connect(ctx, connStr)
			if err != nil {
				t.Fatalf("connect for insert: %v", err)
			}
			defer conn.Close(ctx)

			safeTable := pgx.Identifier{table}.Sanitize()
			_, err = conn.Exec(ctx,
				fmt.Sprintf(`INSERT INTO %s (channel, operation, payload) VALUES ($1, $2, $3)`, safeTable),
				"users", "INSERT", `{"id":42,"name":"alice"}`,
			)
			if err != nil {
				t.Fatalf("insert outbox row: %v", err)
			}
		}()

		// Wait for the event to arrive.
		line := lc.waitLine(t, 10*time.Second)

		var ev event.Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("unmarshal event: %v\nraw: %s", err, line)
		}
		if ev.Channel != "users" {
			t.Errorf("channel = %q, want %q", ev.Channel, "users")
		}
		if ev.Operation != "INSERT" {
			t.Errorf("operation = %q, want %q", ev.Operation, "INSERT")
		}
		if ev.Source != "outbox" {
			t.Errorf("source = %q, want %q", ev.Source, "outbox")
		}

		// Verify the outbox table row still exists but has processed_at set.
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, err := pgx.Connect(ctx, connStr)
			if err != nil {
				t.Fatalf("connect for verify: %v", err)
			}
			defer conn.Close(ctx)

			safeTable := pgx.Identifier{table}.Sanitize()

			var count int
			err = conn.QueryRow(ctx,
				fmt.Sprintf("SELECT count(*) FROM %s", safeTable),
			).Scan(&count)
			if err != nil {
				t.Fatalf("count outbox rows: %v", err)
			}
			if count < 1 {
				t.Errorf("outbox row count = %d, want >= 1 (rows should be kept)", count)
			}

			var processedAt *time.Time
			err = conn.QueryRow(ctx,
				fmt.Sprintf("SELECT processed_at FROM %s LIMIT 1", safeTable),
			).Scan(&processedAt)
			if err != nil {
				t.Fatalf("query processed_at: %v", err)
			}
			if processedAt == nil {
				t.Error("processed_at is nil, want a timestamp")
			}
		}()
	})
}
