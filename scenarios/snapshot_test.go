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
	"github.com/florinutz/pgcdc/detector/walreplication"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/snapshot"
	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"
)

func TestScenario_Snapshot(t *testing.T) {
	connStr := startPostgres(t)

	t.Run("happy path", func(t *testing.T) {
		table := "snapshot_test_orders"

		// Create and populate the table.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		conn, err := pgx.Connect(ctx, connStr)
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		defer conn.Close(ctx)

		safeTable := pgx.Identifier{table}.Sanitize()
		_, err = conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, data JSONB DEFAULT '{}')`, safeTable))
		if err != nil {
			t.Fatalf("create table: %v", err)
		}

		// Insert 100 rows.
		for i := range 100 {
			_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s (data) VALUES ($1)", safeTable), fmt.Sprintf(`{"item": %d}`, i))
			if err != nil {
				t.Fatalf("insert row %d: %v", i, err)
			}
		}
		conn.Close(ctx)

		// Run snapshot through stdout adapter.
		capture := newLineCapture()
		logger := testLogger()

		snap := snapshot.New(connStr, table, "", 50, logger)
		b := bus.New(256, logger)
		stdoutAdapter := stdout.New(capture, logger)

		sub, err := b.Subscribe(stdoutAdapter.Name())
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}

		snapCtx, snapCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer snapCancel()

		g, gCtx := errgroup.WithContext(snapCtx)

		g.Go(func() error { return b.Start(gCtx) })
		g.Go(func() error { return stdoutAdapter.Start(gCtx, sub) })
		g.Go(func() error {
			err := snap.Run(gCtx, b.Ingest())
			snapCancel()
			return err
		})

		_ = g.Wait()

		// Collect all lines.
		var events []event.Event
		for {
			select {
			case line := <-capture.lines:
				var ev event.Event
				if err := json.Unmarshal([]byte(line), &ev); err != nil {
					t.Fatalf("invalid JSON: %v\nraw: %s", err, line)
				}
				events = append(events, ev)
			default:
				goto done
			}
		}
	done:
		if len(events) != 100 {
			t.Fatalf("expected 100 events, got %d", len(events))
		}

		// Verify event properties.
		for i, ev := range events {
			if ev.Channel != "pgcdc:"+table {
				t.Errorf("event %d: channel = %q, want %q", i, ev.Channel, "pgcdc:"+table)
			}
			if ev.Operation != "SNAPSHOT" {
				t.Errorf("event %d: operation = %q, want SNAPSHOT", i, ev.Operation)
			}
			if ev.Source != "snapshot" {
				t.Errorf("event %d: source = %q, want snapshot", i, ev.Source)
			}
			if ev.ID == "" {
				t.Errorf("event %d: ID is empty", i)
			}
		}
	})

	t.Run("non-existent table returns error", func(t *testing.T) {
		snap := snapshot.New(connStr, "this_table_does_not_exist", "", 0, testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		events := make(chan event.Event, 100)
		err := snap.Run(ctx, events)
		if err == nil {
			t.Fatal("expected error for non-existent table")
		}
	})

	t.Run("snapshot then live WAL streaming", func(t *testing.T) {
		table := "snapshot_first_orders"
		pubName := "pgcdc_snapshot_first_orders"

		createTable(t, connStr, table)
		createPublication(t, connStr, pubName, table)

		// Insert rows before starting — these should arrive as SNAPSHOT events.
		for i := range 5 {
			insertRow(t, connStr, table, map[string]any{"item": fmt.Sprintf("existing_%d", i)})
		}

		capture := newLineCapture()
		logger := testLogger()

		// Create WAL detector with snapshot-first enabled.
		walDet := walreplication.New(connStr, pubName, 0, 0, false, false, logger)
		walDet.SetSnapshotFirst(table, "", 100)

		b := bus.New(256, logger)
		stdoutAdapter := stdout.New(capture, logger)

		sub, err := b.Subscribe(stdoutAdapter.Name())
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error { return b.Start(gCtx) })
		g.Go(func() error { return stdoutAdapter.Start(gCtx, sub) })
		g.Go(func() error { return walDet.Start(gCtx, b.Ingest()) })

		// Collect 5 SNAPSHOT events.
		var snapshotEvents []event.Event
		for range 5 {
			line := capture.waitLine(t, 15*time.Second)
			var ev event.Event
			if err := json.Unmarshal([]byte(line), &ev); err != nil {
				t.Fatalf("invalid JSON: %v\nraw: %s", err, line)
			}
			snapshotEvents = append(snapshotEvents, ev)
		}

		// Verify all snapshot events.
		for i, ev := range snapshotEvents {
			if ev.Operation != "SNAPSHOT" {
				t.Errorf("snapshot event %d: operation = %q, want SNAPSHOT", i, ev.Operation)
			}
			if ev.Source != "snapshot" {
				t.Errorf("snapshot event %d: source = %q, want snapshot", i, ev.Source)
			}
			if ev.Channel != "pgcdc:"+table {
				t.Errorf("snapshot event %d: channel = %q, want %q", i, ev.Channel, "pgcdc:"+table)
			}
			if ev.ID == "" {
				t.Errorf("snapshot event %d: ID is empty", i)
			}
		}

		// Insert a new row — should arrive as a live WAL INSERT event.
		time.Sleep(2 * time.Second)
		insertRow(t, connStr, table, map[string]any{"item": "live_insert"})

		line := capture.waitLine(t, 15*time.Second)
		var liveEv event.Event
		if err := json.Unmarshal([]byte(line), &liveEv); err != nil {
			t.Fatalf("invalid JSON for live event: %v\nraw: %s", err, line)
		}

		if liveEv.Operation != "INSERT" {
			t.Errorf("live event: operation = %q, want INSERT", liveEv.Operation)
		}
		if liveEv.Source != "wal_replication" {
			t.Errorf("live event: source = %q, want wal_replication", liveEv.Source)
		}
		if liveEv.Channel != "pgcdc:"+table {
			t.Errorf("live event: channel = %q, want %q", liveEv.Channel, "pgcdc:"+table)
		}

		cancel()
		_ = g.Wait()
	})
}
