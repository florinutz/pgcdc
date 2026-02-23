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
	"golang.org/x/sync/errgroup"
)

func TestScenario_SnapshotFirst(t *testing.T) {
	connStr := startPostgres(t)

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
		// Small delay to ensure WAL streaming has started.
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
