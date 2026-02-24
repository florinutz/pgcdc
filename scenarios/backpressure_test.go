//go:build integration

package scenarios

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	pgcdc "github.com/florinutz/pgcdc"
	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/adapter/stdout"
	"github.com/florinutz/pgcdc/backpressure"
	"github.com/florinutz/pgcdc/bus"
	"github.com/florinutz/pgcdc/checkpoint"
	"github.com/florinutz/pgcdc/detector/walreplication"
	"github.com/florinutz/pgcdc/event"
)

// TestScenario_Backpressure verifies source-aware backpressure:
// zone transitions, throttling, and load shedding.
func TestScenario_Backpressure(t *testing.T) {
	connStr := startPostgres(t)

	t.Run("happy path", func(t *testing.T) {
		table := "bp_happy_orders"
		pubName := "pgcdc_bp_happy"
		slotName := "pgcdc_bp_happy_slot"

		createTable(t, connStr, table)
		createPublication(t, connStr, pubName, table)
		t.Cleanup(func() { dropReplicationSlot(t, connStr, slotName) })

		capture := newLineCapture()
		logger := testLogger()

		store, err := checkpoint.NewPGStore(context.Background(), connStr, logger)
		if err != nil {
			t.Fatalf("create checkpoint store: %v", err)
		}
		t.Cleanup(func() { _ = store.Close() })

		walDet := walreplication.New(connStr, pubName, 0, 0, false, false, logger)
		walDet.SetPersistentSlot(slotName)
		walDet.SetCheckpointStore(store)
		walDet.SetSlotLagWarn(1) // very low threshold to trigger monitoring

		stdoutAdapter := stdout.New(capture, logger)

		// Low thresholds: 1KB warn, 5KB critical — realistic for test data volumes.
		bpCtrl := backpressure.New(
			1024,   // 1KB warn threshold
			5*1024, // 5KB critical threshold
			100*time.Millisecond,
			2*time.Second, // fast poll for test
			nil,
			logger,
		)

		p := pgcdc.NewPipeline(walDet,
			pgcdc.WithAdapter(stdoutAdapter),
			pgcdc.WithBusBuffer(64),
			pgcdc.WithBusMode(bus.BusModeReliable),
			pgcdc.WithCooperativeCheckpoint(true),
			pgcdc.WithCheckpointStore(store),
			pgcdc.WithBackpressure(bpCtrl),
			pgcdc.WithLogger(logger),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		pipelineErr := make(chan error, 1)
		go func() { pipelineErr <- p.Run(ctx) }()

		// Wait for replication to start.
		time.Sleep(3 * time.Second)

		// Insert rows — with low thresholds the controller should detect lag
		// and transition zones. Even if throttled, events must arrive.
		for i := range 5 {
			insertRow(t, connStr, table, map[string]any{"item": fmt.Sprintf("bp_order_%d", i)})
		}

		// Collect events — all 5 must arrive (throttled, not lost).
		for i := range 5 {
			line := capture.waitLine(t, 15*time.Second)
			var ev event.Event
			if err := json.Unmarshal([]byte(line), &ev); err != nil {
				t.Fatalf("event %d: invalid JSON: %v\nraw: %s", i, err, line)
			}
			if ev.Operation != "INSERT" {
				t.Errorf("event %d: expected INSERT, got %q", i, ev.Operation)
			}
		}

		// Verify controller is accessible and has a valid zone value.
		zone := bpCtrl.Zone()
		if zone < backpressure.ZoneGreen || zone > backpressure.ZoneRed {
			t.Errorf("unexpected zone value: %d", zone)
		}

		cancel()
		select {
		case err := <-pipelineErr:
			_ = err
		case <-time.After(5 * time.Second):
			t.Error("pipeline did not shut down in time")
		}
	})

	t.Run("load shedding", func(t *testing.T) {
		table := "bp_shed_orders"
		pubName := "pgcdc_bp_shed"
		slotName := "pgcdc_bp_shed_slot"

		createTable(t, connStr, table)
		createPublication(t, connStr, pubName, table)
		t.Cleanup(func() { dropReplicationSlot(t, connStr, slotName) })

		logger := testLogger()

		store, err := checkpoint.NewPGStore(context.Background(), connStr, logger)
		if err != nil {
			t.Fatalf("create checkpoint store: %v", err)
		}
		t.Cleanup(func() { _ = store.Close() })

		walDet := walreplication.New(connStr, pubName, 0, 0, false, false, logger)
		walDet.SetPersistentSlot(slotName)
		walDet.SetCheckpointStore(store)

		criticalCapture := newLineCapture()
		criticalAdapter := stdout.New(criticalCapture, logger)

		// bestEffortAdapter counts events received.
		var bestEffortCount atomic.Int32
		bestEffortAdapter := &countingAdapter{
			name:    "best_effort",
			counter: &bestEffortCount,
		}

		// Controller with artificial lag injection. We set fakeLag before
		// creating the pipeline so that SetBackpressureController (which
		// overwrites lagFn) runs first, and we override it afterward during
		// the 3s startup wait.
		var fakeLag atomic.Int64
		fakeLag.Store(0) // start in green

		bpCtrl := backpressure.New(
			100, // 100 byte warn
			500, // 500 byte critical
			100*time.Millisecond,
			500*time.Millisecond, // fast poll
			nil,
			logger,
		)
		bpCtrl.SetAdapterPriority("stdout", backpressure.PriorityCritical)
		bpCtrl.SetAdapterPriority("best_effort", backpressure.PriorityBestEffort)

		p := pgcdc.NewPipeline(walDet,
			pgcdc.WithAdapter(criticalAdapter),
			pgcdc.WithAdapter(bestEffortAdapter),
			pgcdc.WithBusBuffer(64),
			pgcdc.WithBusMode(bus.BusModeReliable),
			pgcdc.WithCooperativeCheckpoint(true),
			pgcdc.WithCheckpointStore(store),
			pgcdc.WithBackpressure(bpCtrl),
			pgcdc.WithLogger(logger),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		pipelineErr := make(chan error, 1)
		go func() { pipelineErr <- p.Run(ctx) }()

		// Wait for replication to start. After p.Run wires
		// SetBackpressureController, we override lagFn with our fake.
		time.Sleep(3 * time.Second)

		// Override lagFn AFTER pipeline wiring to simulate high lag.
		bpCtrl.SetLagFunc(func() int64 { return fakeLag.Load() })
		fakeLag.Store(200) // above warn (100), below critical (500) → yellow

		// Give controller time to detect and enter yellow zone.
		time.Sleep(2 * time.Second)

		if bpCtrl.Zone() != backpressure.ZoneYellow {
			t.Fatalf("expected yellow zone, got %v", bpCtrl.Zone())
		}

		// Record best-effort count before inserts.
		beBefore := bestEffortCount.Load()

		// Insert events while in yellow zone.
		for i := range 5 {
			insertRow(t, connStr, table, map[string]any{"item": fmt.Sprintf("shed_%d", i)})
		}

		// Critical adapter (stdout) should get all events.
		for i := range 5 {
			line := criticalCapture.waitLine(t, 15*time.Second)
			var ev event.Event
			if err := json.Unmarshal([]byte(line), &ev); err != nil {
				t.Fatalf("event %d: invalid JSON: %v\nraw: %s", i, err, line)
			}
		}

		// Give time for potential shed events to be processed.
		time.Sleep(2 * time.Second)

		// In yellow zone, best-effort should be shed — it should receive
		// at most 1-2 events (grace for zone transition timing), not all 5.
		beAfter := bestEffortCount.Load()
		beReceived := beAfter - beBefore
		if beReceived > 2 {
			t.Errorf("best-effort adapter received %d/5 events in yellow zone; expected at most 2 due to shedding", beReceived)
		}

		// Exit yellow zone.
		fakeLag.Store(0)
		time.Sleep(2 * time.Second)

		if bpCtrl.Zone() != backpressure.ZoneGreen {
			t.Errorf("expected green zone after lag drop, got %v", bpCtrl.Zone())
		}

		cancel()
		select {
		case err := <-pipelineErr:
			_ = err
		case <-time.After(5 * time.Second):
			t.Error("pipeline did not shut down in time")
		}
	})
}

// countingAdapter implements adapter.Adapter and adapter.Acknowledger.
// It counts events received and acks them immediately.
type countingAdapter struct {
	name    string
	counter *atomic.Int32
	ackFn   adapter.AckFunc
}

func (a *countingAdapter) Name() string                  { return a.name }
func (a *countingAdapter) SetAckFunc(fn adapter.AckFunc) { a.ackFn = fn }

func (a *countingAdapter) Start(ctx context.Context, events <-chan event.Event) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-events:
			if !ok {
				return nil
			}
			a.counter.Add(1)
			if a.ackFn != nil && ev.LSN > 0 {
				a.ackFn(ev.LSN)
			}
		}
	}
}
