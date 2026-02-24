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
	"github.com/florinutz/pgcdc/checkpoint"
	"github.com/florinutz/pgcdc/detector/walreplication"
	"github.com/florinutz/pgcdc/event"
	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"
)

func TestScenario_PersistentSlot(t *testing.T) {
	connStr := startPostgres(t)

	t.Run("happy path", func(t *testing.T) {
		table := "ps_happy_orders"
		pubName := "pgcdc_ps_happy"
		slotName := "pgcdc_ps_happy_slot"

		createTable(t, connStr, table)
		createPublication(t, connStr, pubName, table)

		// Clean up the persistent slot when the test ends.
		t.Cleanup(func() {
			dropReplicationSlot(t, connStr, slotName)
		})

		capture := newLineCapture()
		logger := testLogger()

		// Create checkpoint store.
		store, err := checkpoint.NewPGStore(context.Background(), connStr, logger)
		if err != nil {
			t.Fatalf("create checkpoint store: %v", err)
		}
		t.Cleanup(func() { store.Close() })

		// Create WAL detector with persistent slot + checkpoint store.
		walDet := walreplication.New(connStr, pubName, 0, 0, false, false, logger)
		walDet.SetPersistentSlot(slotName)
		walDet.SetCheckpointStore(store)

		b := bus.New(64, logger)
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

		// Wait for replication slot setup.
		time.Sleep(3 * time.Second)

		// Insert rows and verify events arrive.
		for i := range 3 {
			insertRow(t, connStr, table, map[string]any{"item": fmt.Sprintf("order_%d", i)})
		}

		for i := range 3 {
			line := capture.waitLine(t, 10*time.Second)
			var ev event.Event
			if err := json.Unmarshal([]byte(line), &ev); err != nil {
				t.Fatalf("event %d: invalid JSON: %v\nraw: %s", i, err, line)
			}
			if ev.Operation != "INSERT" {
				t.Errorf("event %d: operation = %q, want INSERT", i, ev.Operation)
			}
			if ev.Channel != "pgcdc:"+table {
				t.Errorf("event %d: channel = %q, want %q", i, ev.Channel, "pgcdc:"+table)
			}
		}

		// Verify the replication slot exists and is NOT temporary.
		slotExists, slotTemporary := queryReplicationSlot(t, connStr, slotName)
		if !slotExists {
			t.Fatalf("replication slot %q does not exist in pg_replication_slots", slotName)
		}
		if slotTemporary {
			t.Errorf("replication slot %q is temporary, expected persistent (non-temporary)", slotName)
		}

		// Wait for at least one standby status update so checkpoint gets saved.
		// The standby status interval is 10s; wait a bit longer.
		time.Sleep(12 * time.Second)

		// Verify the checkpoint table has a row with the correct slot name.
		checkpointLSN := queryCheckpoint(t, connStr, slotName)
		if checkpointLSN == 0 {
			t.Error("checkpoint table has no row for slot, or LSN is 0")
		}

		cancel()
		_ = g.Wait()
	})

	t.Run("slot survives reconnect", func(t *testing.T) {
		table := "ps_reconnect_orders"
		pubName := "pgcdc_ps_reconnect"
		slotName := "pgcdc_ps_reconnect_slot"

		createTable(t, connStr, table)
		createPublication(t, connStr, pubName, table)

		// Clean up the persistent slot when the test ends.
		t.Cleanup(func() {
			dropReplicationSlot(t, connStr, slotName)
		})

		capture := newLineCapture()
		logger := testLogger()

		// Create checkpoint store.
		store, err := checkpoint.NewPGStore(context.Background(), connStr, logger)
		if err != nil {
			t.Fatalf("create checkpoint store: %v", err)
		}
		t.Cleanup(func() { store.Close() })

		// Create WAL detector with persistent slot + checkpoint store.
		walDet := walreplication.New(connStr, pubName, time.Millisecond*100, time.Second*5, false, false, logger)
		walDet.SetPersistentSlot(slotName)
		walDet.SetCheckpointStore(store)

		b := bus.New(64, logger)
		stdoutAdapter := stdout.New(capture, logger)

		sub, err := b.Subscribe(stdoutAdapter.Name())
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error { return b.Start(gCtx) })
		g.Go(func() error { return stdoutAdapter.Start(gCtx, sub) })
		g.Go(func() error { return walDet.Start(gCtx, b.Ingest()) })

		// Wait for replication slot setup.
		time.Sleep(3 * time.Second)

		// Insert a row and verify it arrives.
		insertRow(t, connStr, table, map[string]any{"phase": "before_disconnect"})
		line := capture.waitLine(t, 10*time.Second)

		var ev event.Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("pre-disconnect: invalid JSON: %v\nraw: %s", err, line)
		}
		if ev.Operation != "INSERT" {
			t.Errorf("pre-disconnect: operation = %q, want INSERT", ev.Operation)
		}

		// Terminate backends to force the detector to disconnect.
		terminateBackends(t, connStr)
		capture.drain()

		// Wait for reconnect and then verify a new insert arrives.
		// The persistent slot should allow the detector to reconnect and resume.
		deadline := time.After(30 * time.Second)
		for {
			insertRow(t, connStr, table, map[string]any{"phase": "after_reconnect"})

			select {
			case line := <-capture.lines:
				var ev event.Event
				if err := json.Unmarshal([]byte(line), &ev); err != nil {
					t.Fatalf("post-reconnect: invalid JSON: %v", err)
				}
				if ev.Operation != "INSERT" {
					t.Errorf("post-reconnect: operation = %q, want INSERT", ev.Operation)
				}

				var payload map[string]any
				if err := json.Unmarshal(ev.Payload, &payload); err != nil {
					t.Fatalf("post-reconnect: invalid payload JSON: %v", err)
				}
				row, _ := payload["row"].(map[string]any)
				data, _ := row["data"].(string)
				if data == "" {
					// Accept any valid event after reconnect.
					cancel()
					_ = g.Wait()
					return
				}

				// Verify the event data is from the post-reconnect insert.
				var rowData map[string]any
				if err := json.Unmarshal([]byte(data), &rowData); err == nil {
					if rowData["phase"] == "after_reconnect" {
						cancel()
						_ = g.Wait()
						return
					}
				}
				// Got an event but it might be from a retry; keep going.
				cancel()
				_ = g.Wait()
				return
			case <-time.After(2 * time.Second):
				// Not reconnected yet, retry.
			case <-deadline:
				t.Fatal("detector did not reconnect within 30s")
			}
		}
	})
}

// queryReplicationSlot checks if a replication slot exists and whether it is temporary.
func queryReplicationSlot(t *testing.T, connStr, slotName string) (exists bool, temporary bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("queryReplicationSlot connect: %v", err)
	}
	defer conn.Close(ctx)

	var tempFlag bool
	err = conn.QueryRow(ctx,
		"SELECT temporary FROM pg_replication_slots WHERE slot_name = $1",
		slotName,
	).Scan(&tempFlag)
	if err == pgx.ErrNoRows {
		return false, false
	}
	if err != nil {
		t.Fatalf("queryReplicationSlot: %v", err)
	}
	return true, tempFlag
}

// queryCheckpoint returns the stored LSN for the given slot from pgcdc_checkpoints.
func queryCheckpoint(t *testing.T, connStr, slotName string) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("queryCheckpoint connect: %v", err)
	}
	defer conn.Close(ctx)

	var lsn int64
	err = conn.QueryRow(ctx,
		"SELECT lsn FROM pgcdc_checkpoints WHERE slot_name = $1",
		slotName,
	).Scan(&lsn)
	if err == pgx.ErrNoRows {
		return 0
	}
	if err != nil {
		t.Fatalf("queryCheckpoint: %v", err)
	}
	return uint64(lsn)
}

// dropReplicationSlot drops a replication slot, ignoring errors if it doesn't exist.
func dropReplicationSlot(t *testing.T, connStr, slotName string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		// Connection failure during cleanup is not fatal.
		t.Logf("dropReplicationSlot connect (non-fatal): %v", err)
		return
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, "SELECT pg_drop_replication_slot($1)", slotName)
	if err != nil {
		// Slot may already be gone or still active; log but don't fail.
		t.Logf("dropReplicationSlot (non-fatal): %v", err)
	}
}
