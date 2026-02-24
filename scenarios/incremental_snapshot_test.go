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

func TestScenario_IncrementalSnapshot(t *testing.T) {
	connStr := startPostgres(t)

	t.Run("signal-triggered snapshot alongside WAL", func(t *testing.T) {
		table := "incr_snap_orders"
		signalTable := "incr_snap_signals"
		pubName := "pgcdc_incr_snap"

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		// Create tables and publication.
		setupConn, err := pgx.Connect(ctx, connStr)
		if err != nil {
			t.Fatalf("setup connect: %v", err)
		}

		_, err = setupConn.Exec(ctx, fmt.Sprintf(`
			DROP TABLE IF EXISTS %s CASCADE;
			DROP TABLE IF EXISTS %s CASCADE;
			DROP PUBLICATION IF EXISTS %s;

			CREATE TABLE %s (id SERIAL PRIMARY KEY, name TEXT NOT NULL);
			ALTER TABLE %s REPLICA IDENTITY FULL;

			CREATE TABLE %s (
				id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
				signal TEXT NOT NULL,
				payload JSONB NOT NULL DEFAULT '{}',
				created_at TIMESTAMPTZ NOT NULL DEFAULT now()
			);

			CREATE PUBLICATION %s FOR TABLE %s, %s;
		`,
			pgx.Identifier{table}.Sanitize(),
			pgx.Identifier{signalTable}.Sanitize(),
			pgx.Identifier{pubName}.Sanitize(),
			pgx.Identifier{table}.Sanitize(),
			pgx.Identifier{table}.Sanitize(),
			pgx.Identifier{signalTable}.Sanitize(),
			pgx.Identifier{pubName}.Sanitize(),
			pgx.Identifier{table}.Sanitize(),
			pgx.Identifier{signalTable}.Sanitize(),
		))
		setupConn.Close(ctx)
		if err != nil {
			t.Fatalf("setup: %v", err)
		}

		// Insert rows to be snapshot.
		for i := range 10 {
			func() {
				c, err := pgx.Connect(ctx, connStr)
				if err != nil {
					t.Fatalf("insert connect: %v", err)
				}
				defer c.Close(ctx)
				_, err = c.Exec(ctx, fmt.Sprintf("INSERT INTO %s (name) VALUES ($1)", pgx.Identifier{table}.Sanitize()), fmt.Sprintf("existing_%d", i))
				if err != nil {
					t.Fatalf("insert existing: %v", err)
				}
			}()
		}

		// Set up progress store.
		progStore, err := snapshot.NewPGProgressStore(ctx, connStr, testLogger())
		if err != nil {
			t.Fatalf("create progress store: %v", err)
		}
		defer progStore.Close()

		// Create WAL detector with incremental snapshots enabled.
		capture := newLineCapture()
		logger := testLogger()

		walDet := walreplication.New(connStr, pubName, 0, 0, false, false, logger)
		walDet.SetIncrementalSnapshot(signalTable, 5, 0)
		walDet.SetProgressStore(progStore)

		b := bus.New(256, logger)
		stdoutAdapter := stdout.New(capture, logger)

		sub, err := b.Subscribe(stdoutAdapter.Name())
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error { return b.Start(gCtx) })
		g.Go(func() error { return stdoutAdapter.Start(gCtx, sub) })
		g.Go(func() error { return walDet.Start(gCtx, b.Ingest()) })

		// Give WAL streaming time to start.
		time.Sleep(3 * time.Second)

		// Insert a live row — verify WAL event arrives.
		func() {
			c, err := pgx.Connect(ctx, connStr)
			if err != nil {
				t.Fatalf("live insert connect: %v", err)
			}
			defer c.Close(ctx)
			_, err = c.Exec(ctx, fmt.Sprintf("INSERT INTO %s (name) VALUES ($1)", pgx.Identifier{table}.Sanitize()), "live_before_snapshot")
			if err != nil {
				t.Fatalf("live insert: %v", err)
			}
		}()

		liveEvent := waitForEvent(t, capture, 10*time.Second, func(ev event.Event) bool {
			return ev.Operation == "INSERT" && ev.Channel == "pgcdc:"+table
		})
		if liveEvent.Source != "wal_replication" {
			t.Errorf("live event source = %q, want wal_replication", liveEvent.Source)
		}

		// Drain the signal table INSERT event that WAL will pick up.
		capture.drain()

		// Trigger incremental snapshot via signal table.
		func() {
			c, err := pgx.Connect(ctx, connStr)
			if err != nil {
				t.Fatalf("signal connect: %v", err)
			}
			defer c.Close(ctx)
			_, err = c.Exec(ctx, fmt.Sprintf(
				"INSERT INTO %s (signal, payload) VALUES ('execute-snapshot', $1)",
				pgx.Identifier{signalTable}.Sanitize(),
			), fmt.Sprintf(`{"table": "%s"}`, table))
			if err != nil {
				t.Fatalf("insert signal: %v", err)
			}
		}()

		// Expect: signal INSERT event on WAL, then SNAPSHOT_STARTED, then data events, then SNAPSHOT_COMPLETED.
		// Collect events until we see SNAPSHOT_COMPLETED or timeout.
		var snapshotStarted bool
		var snapshotCompleted bool
		var snapshotDataCount int
		deadline := time.After(30 * time.Second)

		for !snapshotCompleted {
			select {
			case line := <-capture.lines:
				var ev event.Event
				if err := json.Unmarshal([]byte(line), &ev); err != nil {
					continue
				}

				switch ev.Operation {
				case "SNAPSHOT_STARTED":
					snapshotStarted = true
				case "SNAPSHOT":
					if ev.Source == "incremental_snapshot" {
						snapshotDataCount++
					}
				case "SNAPSHOT_COMPLETED":
					snapshotCompleted = true
				case "INSERT":
					// Signal table INSERT or live INSERT — skip.
				}
			case <-deadline:
				t.Fatalf("timed out waiting for snapshot events (started=%v, data=%d, completed=%v)",
					snapshotStarted, snapshotDataCount, snapshotCompleted)
			}
		}

		if !snapshotStarted {
			t.Error("never received SNAPSHOT_STARTED event")
		}
		// We inserted 10 rows + 1 live row = 11 rows in the table at snapshot time.
		if snapshotDataCount < 10 {
			t.Errorf("snapshot data events = %d, want >= 10", snapshotDataCount)
		}

		// Verify WAL still works after snapshot.
		capture.drain()
		func() {
			c, err := pgx.Connect(ctx, connStr)
			if err != nil {
				t.Fatalf("post-snapshot insert connect: %v", err)
			}
			defer c.Close(ctx)
			_, err = c.Exec(ctx, fmt.Sprintf("INSERT INTO %s (name) VALUES ($1)", pgx.Identifier{table}.Sanitize()), "live_after_snapshot")
			if err != nil {
				t.Fatalf("post-snapshot insert: %v", err)
			}
		}()

		postEv := waitForEvent(t, capture, 10*time.Second, func(ev event.Event) bool {
			return ev.Operation == "INSERT" && ev.Channel == "pgcdc:"+table
		})
		if postEv.Source != "wal_replication" {
			t.Errorf("post-snapshot event source = %q, want wal_replication", postEv.Source)
		}

		cancel()
		_ = g.Wait()
	})

	t.Run("crash recovery resumes from last chunk", func(t *testing.T) {
		table := "incr_snap_resume"
		signalTable := "incr_snap_resume_signals"
		pubName := "pgcdc_incr_snap_resume"

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		// Create tables and publication.
		setupConn, err := pgx.Connect(ctx, connStr)
		if err != nil {
			t.Fatalf("setup connect: %v", err)
		}

		_, err = setupConn.Exec(ctx, fmt.Sprintf(`
			DROP TABLE IF EXISTS %s CASCADE;
			DROP TABLE IF EXISTS %s CASCADE;
			DROP PUBLICATION IF EXISTS %s;

			CREATE TABLE %s (id SERIAL PRIMARY KEY, name TEXT NOT NULL);
			ALTER TABLE %s REPLICA IDENTITY FULL;

			CREATE TABLE %s (
				id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
				signal TEXT NOT NULL,
				payload JSONB NOT NULL DEFAULT '{}',
				created_at TIMESTAMPTZ NOT NULL DEFAULT now()
			);

			CREATE PUBLICATION %s FOR TABLE %s, %s;
		`,
			pgx.Identifier{table}.Sanitize(),
			pgx.Identifier{signalTable}.Sanitize(),
			pgx.Identifier{pubName}.Sanitize(),
			pgx.Identifier{table}.Sanitize(),
			pgx.Identifier{table}.Sanitize(),
			pgx.Identifier{signalTable}.Sanitize(),
			pgx.Identifier{pubName}.Sanitize(),
			pgx.Identifier{table}.Sanitize(),
			pgx.Identifier{signalTable}.Sanitize(),
		))
		setupConn.Close(ctx)
		if err != nil {
			t.Fatalf("setup: %v", err)
		}

		// Insert 20 rows.
		for i := range 20 {
			func() {
				c, err := pgx.Connect(ctx, connStr)
				if err != nil {
					t.Fatalf("insert connect: %v", err)
				}
				defer c.Close(ctx)
				_, err = c.Exec(ctx, fmt.Sprintf("INSERT INTO %s (name) VALUES ($1)", pgx.Identifier{table}.Sanitize()), fmt.Sprintf("row_%d", i))
				if err != nil {
					t.Fatalf("insert: %v", err)
				}
			}()
		}

		// Set up progress store.
		progStore, err := snapshot.NewPGProgressStore(ctx, connStr, testLogger())
		if err != nil {
			t.Fatalf("create progress store: %v", err)
		}
		defer progStore.Close()

		// Phase 1: Start snapshot with chunk size 5, cancel after first chunk.
		capture1 := newLineCapture()
		logger := testLogger()

		walDet1 := walreplication.New(connStr, pubName, 0, 0, false, false, logger)
		walDet1.SetIncrementalSnapshot(signalTable, 5, 100*time.Millisecond)
		walDet1.SetProgressStore(progStore)

		b1 := bus.New(256, logger)
		stdoutAdapter1 := stdout.New(capture1, logger)

		sub1, err := b1.Subscribe(stdoutAdapter1.Name())
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}

		ctx1, cancel1 := context.WithCancel(ctx)
		g1, gCtx1 := errgroup.WithContext(ctx1)
		g1.Go(func() error { return b1.Start(gCtx1) })
		g1.Go(func() error { return stdoutAdapter1.Start(gCtx1, sub1) })
		g1.Go(func() error { return walDet1.Start(gCtx1, b1.Ingest()) })

		time.Sleep(3 * time.Second)

		// Trigger snapshot.
		func() {
			c, err := pgx.Connect(ctx, connStr)
			if err != nil {
				t.Fatalf("signal connect: %v", err)
			}
			defer c.Close(ctx)
			_, err = c.Exec(ctx, fmt.Sprintf(
				"INSERT INTO %s (signal, payload) VALUES ('execute-snapshot', $1)",
				pgx.Identifier{signalTable}.Sanitize(),
			), fmt.Sprintf(`{"table": "%s", "snapshot_id": "resume-test-1"}`, table))
			if err != nil {
				t.Fatalf("insert signal: %v", err)
			}
		}()

		// Wait for at least one chunk of SNAPSHOT events, then cancel.
		var firstRunCount int
		snapshotStarted := false
		earlyCancel := time.After(8 * time.Second)

	collectLoop:
		for {
			select {
			case line := <-capture1.lines:
				var ev event.Event
				if err := json.Unmarshal([]byte(line), &ev); err != nil {
					continue
				}
				if ev.Operation == "SNAPSHOT_STARTED" {
					snapshotStarted = true
				}
				if ev.Operation == "SNAPSHOT" && ev.Source == "incremental_snapshot" {
					firstRunCount++
					// After getting some events but not all 20, cancel to simulate crash.
					if firstRunCount >= 5 {
						cancel1()
						break collectLoop
					}
				}
			case <-earlyCancel:
				// Cancel anyway after timeout.
				cancel1()
				break collectLoop
			}
		}
		_ = g1.Wait()

		// Brief pause for the bare snapshot goroutine (not in errgroup) to finish
		// its cleanup and final progress save.
		time.Sleep(2 * time.Second)

		if !snapshotStarted {
			t.Fatal("first run: never received SNAPSHOT_STARTED")
		}
		if firstRunCount < 5 {
			t.Fatalf("first run: expected >= 5 events, got %d", firstRunCount)
		}

		// Verify progress was saved.
		rec, err := progStore.Load(ctx, "resume-test-1")
		if err != nil {
			t.Fatalf("load progress: %v", err)
		}
		if rec == nil {
			t.Fatal("no progress record found after first run")
		}
		if rec.RowsProcessed < 5 {
			t.Errorf("progress rows = %d, want >= 5", rec.RowsProcessed)
		}
		savedRows := rec.RowsProcessed

		// Phase 2: Restart — snapshot should resume from last checkpoint.
		capture2 := newLineCapture()

		walDet2 := walreplication.New(connStr, pubName, 0, 0, false, false, logger)
		walDet2.SetIncrementalSnapshot(signalTable, 5, 0)
		walDet2.SetProgressStore(progStore)

		b2 := bus.New(256, logger)
		stdoutAdapter2 := stdout.New(capture2, logger)

		sub2, err := b2.Subscribe(stdoutAdapter2.Name())
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}

		ctx2, cancel2 := context.WithCancel(ctx)
		g2, gCtx2 := errgroup.WithContext(ctx2)
		g2.Go(func() error { return b2.Start(gCtx2) })
		g2.Go(func() error { return stdoutAdapter2.Start(gCtx2, sub2) })
		g2.Go(func() error { return walDet2.Start(gCtx2, b2.Ingest()) })

		// The detector should auto-resume the snapshot. Wait for SNAPSHOT_COMPLETED.
		var secondRunSnapEvents int
		completedSeen := false
		resumeDeadline := time.After(30 * time.Second)

	resumeLoop:
		for {
			select {
			case line := <-capture2.lines:
				var ev event.Event
				if err := json.Unmarshal([]byte(line), &ev); err != nil {
					continue
				}
				if ev.Operation == "SNAPSHOT" && ev.Source == "incremental_snapshot" {
					secondRunSnapEvents++
				}
				if ev.Operation == "SNAPSHOT_COMPLETED" {
					completedSeen = true
					break resumeLoop
				}
			case <-resumeDeadline:
				t.Fatalf("timed out waiting for resume completion (events=%d, completed=%v)", secondRunSnapEvents, completedSeen)
			}
		}

		if !completedSeen {
			t.Error("second run: never received SNAPSHOT_COMPLETED")
		}

		// The second run should have fewer events than total (it resumed).
		if int64(secondRunSnapEvents) >= 20 {
			t.Errorf("second run processed %d events, expected fewer than 20 (should have resumed from row %d)", secondRunSnapEvents, savedRows)
		}

		cancel2()
		_ = g2.Wait()
	})
}

// waitForEvent drains lines from capture until predicate matches or timeout.
func waitForEvent(t *testing.T, capture *lineCapture, timeout time.Duration, pred func(event.Event) bool) event.Event {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case line := <-capture.lines:
			var ev event.Event
			if err := json.Unmarshal([]byte(line), &ev); err != nil {
				continue
			}
			if pred(ev) {
				return ev
			}
		case <-deadline:
			t.Fatal("timed out waiting for matching event")
			return event.Event{}
		}
	}
}
