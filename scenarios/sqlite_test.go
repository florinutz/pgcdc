//go:build integration

package scenarios

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/adapter/stdout"
	"github.com/florinutz/pgcdc/bus"
	sqlitedet "github.com/florinutz/pgcdc/detector/sqlite"
	"github.com/florinutz/pgcdc/event"
	"golang.org/x/sync/errgroup"
)

func TestScenario_SQLiteDetector(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")

		db, err := sql.Open("sqlite", dbPath)
		if err != nil {
			t.Fatalf("open sqlite: %v", err)
		}
		_, err = db.Exec(`CREATE TABLE pgcdc_changes (
			table_name TEXT NOT NULL,
			operation  TEXT NOT NULL,
			row_data   TEXT,
			old_data   TEXT,
			created_at TEXT NOT NULL DEFAULT (datetime('now')),
			processed  INTEGER NOT NULL DEFAULT 0
		)`)
		if err != nil {
			t.Fatalf("create table: %v", err)
		}
		_, err = db.Exec(`INSERT INTO pgcdc_changes (table_name, operation, row_data)
			VALUES ('orders', 'INSERT', '{"id":1,"item":"widget"}')`)
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		db.Close()

		det := sqlitedet.New(dbPath, 100*time.Millisecond, 0, false, testLogger())

		capture := newLineCapture()
		logger := testLogger()
		b := bus.New(64, logger)
		stdoutAdapter := stdout.New(capture, logger)

		sub, err := b.Subscribe(stdoutAdapter.Name())
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error { return b.Start(gCtx) })
		g.Go(func() error { return det.Start(gCtx, b.Ingest()) })
		g.Go(func() error { return stdoutAdapter.Start(gCtx, sub) })
		t.Cleanup(func() {
			cancel()
			_ = g.Wait()
		})

		line := capture.waitLine(t, 5*time.Second)
		var ev event.Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("invalid JSON: %v\nraw: %s", err, line)
		}
		if ev.Channel != "pgcdc:orders" {
			t.Errorf("channel = %q, want pgcdc:orders", ev.Channel)
		}
		if ev.Operation != "INSERT" {
			t.Errorf("operation = %q, want INSERT", ev.Operation)
		}
		if ev.Source != "sqlite" {
			t.Errorf("source = %q, want sqlite", ev.Source)
		}
		if ev.ID == "" {
			t.Error("event ID is empty")
		}

		// Verify row was deleted (default mode).
		db2, err := sql.Open("sqlite", dbPath)
		if err != nil {
			t.Fatalf("reopen sqlite: %v", err)
		}
		defer db2.Close()
		var count int
		db2.QueryRow("SELECT COUNT(*) FROM pgcdc_changes").Scan(&count)
		if count != 0 {
			t.Errorf("expected 0 remaining rows, got %d", count)
		}
	})

	t.Run("keep-processed marks instead of deleting", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")

		db, err := sql.Open("sqlite", dbPath)
		if err != nil {
			t.Fatalf("open sqlite: %v", err)
		}
		_, err = db.Exec(`CREATE TABLE pgcdc_changes (
			table_name TEXT NOT NULL,
			operation  TEXT NOT NULL,
			row_data   TEXT,
			old_data   TEXT,
			created_at TEXT NOT NULL DEFAULT (datetime('now')),
			processed  INTEGER NOT NULL DEFAULT 0
		)`)
		if err != nil {
			t.Fatalf("create table: %v", err)
		}
		_, err = db.Exec(`INSERT INTO pgcdc_changes (table_name, operation, row_data)
			VALUES ('users', 'UPDATE', '{"id":1,"name":"alice"}')`)
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		db.Close()

		det := sqlitedet.New(dbPath, 100*time.Millisecond, 0, true, testLogger())

		capture := newLineCapture()
		logger := testLogger()
		b := bus.New(64, logger)
		stdoutAdapter := stdout.New(capture, logger)

		sub, err := b.Subscribe(stdoutAdapter.Name())
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error { return b.Start(gCtx) })
		g.Go(func() error { return det.Start(gCtx, b.Ingest()) })
		g.Go(func() error { return stdoutAdapter.Start(gCtx, sub) })
		t.Cleanup(func() {
			cancel()
			_ = g.Wait()
		})

		_ = capture.waitLine(t, 5*time.Second)

		db2, err := sql.Open("sqlite", dbPath)
		if err != nil {
			t.Fatalf("reopen sqlite: %v", err)
		}
		defer db2.Close()
		var processed int
		db2.QueryRow("SELECT processed FROM pgcdc_changes WHERE table_name = 'users'").Scan(&processed)
		if processed != 1 {
			t.Errorf("expected processed=1, got %d", processed)
		}
	})
}
