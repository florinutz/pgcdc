//go:build integration

package scenarios

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/adapter/stdout"
	viewadapter "github.com/florinutz/pgcdc/adapter/view"
	"github.com/florinutz/pgcdc/bus"
	"github.com/florinutz/pgcdc/detector/walreplication"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/view"
	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"
)

func TestScenario_View(t *testing.T) {
	t.Parallel()
	connStr := startPostgres(t)

	t.Run("happy path: count and sum with group by", func(t *testing.T) {
		table := "view_orders"
		pubName := "pgcdc_view_orders"

		// Create table with typed columns (not JSONB) so WAL row values are directly accessible.
		createViewTable(t, connStr, table)
		createPublication(t, connStr, pubName, table)

		// Parse view definition.
		// WAL payload is: {"op":"INSERT","table":"view_orders","row":{"id":"1","region":"us-east","amount":"100"},"old":null}
		// So we use payload.row.region and payload.row.amount.
		def, err := view.Parse("orders_per_window",
			"SELECT COUNT(*) as order_count, SUM(payload.row.amount) as total_revenue, payload.row.region "+
				"FROM pgcdc_events "+
				"WHERE channel = 'pgcdc:view_orders' AND operation = 'INSERT' "+
				"GROUP BY payload.row.region "+
				"TUMBLING WINDOW 2s",
			view.EmitRow, 0)
		if err != nil {
			t.Fatalf("parse view: %v", err)
		}

		logger := testLogger()
		engine := view.NewEngine([]*view.ViewDef{def}, logger)
		va := viewadapter.New(engine, logger)

		capture := newLineCapture()
		stdoutAdapter := stdout.New(capture, logger)

		det := walreplication.New(connStr, pubName, 0, 0, false, false, logger)
		b := bus.New(64, logger)

		ctx, cancel := context.WithCancel(context.Background())
		va.SetIngestChan(b.Ingest())

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error { return b.Start(gCtx) })
		g.Go(func() error { return det.Start(gCtx, b.Ingest()) })

		viewSub, err := b.Subscribe("view")
		if err != nil {
			cancel()
			t.Fatalf("subscribe view: %v", err)
		}
		g.Go(func() error { return va.Start(gCtx, viewSub) })

		stdoutSub, err := b.Subscribe("stdout")
		if err != nil {
			cancel()
			t.Fatalf("subscribe stdout: %v", err)
		}
		g.Go(func() error { return stdoutAdapter.Start(gCtx, stdoutSub) })

		t.Cleanup(func() {
			cancel()
			g.Wait()
		})

		// Wait for WAL detector readiness by inserting probe rows.
		waitViewDetector(t, connStr, table, capture, func() {
			insertViewOrderRow(t, connStr, table, "__probe", 0)
		})
		// Wait for the probe tumbling window to flush (VIEW_RESULT) so probe
		// data doesn't contaminate real results.
		waitForViewResult(t, capture, 10*time.Second)
		time.Sleep(200 * time.Millisecond)
		capture.drain()

		// Insert test data.
		insertViewOrderRow(t, connStr, table, "us-east", 100)
		insertViewOrderRow(t, connStr, table, "us-east", 200)
		insertViewOrderRow(t, connStr, table, "eu-west", 50)

		// Consume the 3 INSERT events, skipping any stale VIEW_RESULTs from
		// probe windows that may arrive after the drain.
		inserts := 0
		for inserts < 3 {
			line := capture.waitLine(t, 15*time.Second)
			var ev event.Event
			if err := json.Unmarshal([]byte(line), &ev); err != nil {
				t.Fatalf("unmarshal insert event %d: %v\nraw: %s", inserts, err, line)
			}
			if ev.Operation == "VIEW_RESULT" {
				continue // stale probe window result
			}
			if ev.Operation != "INSERT" {
				t.Fatalf("expected INSERT, got %s", ev.Operation)
			}
			inserts++
		}

		// Wait for the 2s tumbling window to fire and emit VIEW_RESULT events.
		var viewResults []event.Event
		deadline := time.After(15 * time.Second)
		for len(viewResults) < 2 {
			select {
			case line := <-capture.lines:
				var ev event.Event
				if err := json.Unmarshal([]byte(line), &ev); err != nil {
					t.Logf("DEBUG: unparseable line: %s", line)
					continue
				}
				t.Logf("DEBUG: got event op=%s ch=%s payload=%s", ev.Operation, ev.Channel, string(ev.Payload))
				if ev.Operation == "VIEW_RESULT" {
					viewResults = append(viewResults, ev)
				}
			case <-deadline:
				t.Fatalf("timed out waiting for VIEW_RESULT events (got %d)", len(viewResults))
			}
		}

		// Verify results by region.
		byRegion := make(map[string]map[string]any)
		for _, ev := range viewResults {
			if ev.Channel != "pgcdc:_view:orders_per_window" {
				t.Errorf("channel = %q, want pgcdc:_view:orders_per_window", ev.Channel)
			}
			var p map[string]any
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Fatalf("unmarshal view result: %v", err)
			}
			region := p["region"].(string)
			byRegion[region] = p
		}

		usEast := byRegion["us-east"]
		if usEast == nil {
			t.Fatal("missing us-east group")
		}
		if int64(usEast["order_count"].(float64)) != 2 {
			t.Errorf("us-east order_count = %v, want 2", usEast["order_count"])
		}
		if usEast["total_revenue"].(float64) != 300 {
			t.Errorf("us-east total_revenue = %v, want 300", usEast["total_revenue"])
		}

		euWest := byRegion["eu-west"]
		if euWest == nil {
			t.Fatal("missing eu-west group")
		}
		if int64(euWest["order_count"].(float64)) != 1 {
			t.Errorf("eu-west order_count = %v, want 1", euWest["order_count"])
		}
		if euWest["total_revenue"].(float64) != 50 {
			t.Errorf("eu-west total_revenue = %v, want 50", euWest["total_revenue"])
		}

		if usEast["_window"] == nil {
			t.Error("missing _window in view result")
		}
	})

	t.Run("batch emit mode", func(t *testing.T) {
		table := "view_batch_orders"
		pubName := "pgcdc_view_batch_orders"
		createViewTable(t, connStr, table)
		createPublication(t, connStr, pubName, table)

		def, err := view.Parse("batch_view",
			"SELECT COUNT(*) as cnt, payload.row.region "+
				"FROM pgcdc_events "+
				"WHERE channel = 'pgcdc:view_batch_orders' AND operation = 'INSERT' "+
				"GROUP BY payload.row.region "+
				"TUMBLING WINDOW 2s",
			view.EmitBatch, 0)
		if err != nil {
			t.Fatalf("parse view: %v", err)
		}

		logger := testLogger()
		engine := view.NewEngine([]*view.ViewDef{def}, logger)
		va := viewadapter.New(engine, logger)
		capture := newLineCapture()
		stdoutAdapter := stdout.New(capture, logger)
		det := walreplication.New(connStr, pubName, 0, 0, false, false, logger)
		b := bus.New(64, logger)

		ctx, cancel := context.WithCancel(context.Background())
		va.SetIngestChan(b.Ingest())

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error { return b.Start(gCtx) })
		g.Go(func() error { return det.Start(gCtx, b.Ingest()) })

		viewSub, _ := b.Subscribe("view")
		g.Go(func() error { return va.Start(gCtx, viewSub) })
		stdoutSub, _ := b.Subscribe("stdout")
		g.Go(func() error { return stdoutAdapter.Start(gCtx, stdoutSub) })

		t.Cleanup(func() {
			cancel()
			g.Wait()
		})

		// Wait for WAL detector readiness by inserting probe rows.
		waitViewDetector(t, connStr, table, capture, func() {
			insertViewOrderRow(t, connStr, table, "__probe", 0)
		})
		// Wait for the probe tumbling window to flush (VIEW_RESULT), confirming
		// a window boundary has passed. This replaces a fixed sleep and ensures
		// both real inserts land in the same fresh window.
		waitForViewResult(t, capture, 10*time.Second)
		time.Sleep(200 * time.Millisecond)
		capture.drain()

		insertViewOrderRow(t, connStr, table, "us-east", 10)
		insertViewOrderRow(t, connStr, table, "eu-west", 20)

		// Consume INSERT events (skip any stale VIEW_RESULTs from probes).
		inserts := 0
		for inserts < 2 {
			line := capture.waitLine(t, 15*time.Second)
			var ev event.Event
			if err := json.Unmarshal([]byte(line), &ev); err == nil && ev.Operation == "VIEW_RESULT" {
				continue
			}
			inserts++
		}

		// Wait for batch VIEW_RESULT.
		deadline := time.After(15 * time.Second)
		for {
			select {
			case line := <-capture.lines:
				var ev event.Event
				if err := json.Unmarshal([]byte(line), &ev); err != nil {
					continue
				}
				if ev.Operation != "VIEW_RESULT" {
					continue
				}

				var p map[string]any
				json.Unmarshal(ev.Payload, &p)
				rows, ok := p["rows"].([]any)
				if !ok {
					t.Fatalf("batch result missing rows array, got %T", p["rows"])
				}
				if len(rows) != 2 {
					t.Errorf("batch rows = %d, want 2", len(rows))
				}
				if p["_window"] == nil {
					t.Error("batch result missing _window")
				}
				return

			case <-deadline:
				t.Fatal("timed out waiting for batch VIEW_RESULT")
			}
		}
	})

	t.Run("watermark-driven window", func(t *testing.T) {
		table := "view_wm_events"
		pubName := "pgcdc_view_wm_events"
		createViewTableWithTS(t, connStr, table)
		createPublication(t, connStr, pubName, table)

		def, err := view.Parse("wm_window",
			"SELECT COUNT(*) as cnt, payload.row.region "+
				"FROM pgcdc_events "+
				"WHERE channel = 'pgcdc:"+table+"' AND operation = 'INSERT' "+
				"GROUP BY payload.row.region "+
				"EVENT TIME BY payload.row.ts "+
				"TUMBLING WINDOW 2s",
			view.EmitRow, 0)
		if err != nil {
			t.Fatalf("parse view: %v", err)
		}

		logger := testLogger()
		engine := view.NewEngine([]*view.ViewDef{def}, logger)
		va := viewadapter.New(engine, logger)
		capture := newLineCapture()
		stdoutAdapter := stdout.New(capture, logger)
		det := walreplication.New(connStr, pubName, 0, 0, false, false, logger)
		b := bus.New(64, logger)

		ctx, cancel := context.WithCancel(context.Background())
		va.SetIngestChan(b.Ingest())

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error { return b.Start(gCtx) })
		g.Go(func() error { return det.Start(gCtx, b.Ingest()) })

		viewSub, _ := b.Subscribe("view")
		g.Go(func() error { return va.Start(gCtx, viewSub) })
		stdoutSub, _ := b.Subscribe("stdout")
		g.Go(func() error { return stdoutAdapter.Start(gCtx, stdoutSub) })

		t.Cleanup(func() {
			cancel()
			g.Wait()
		})

		// Wait for WAL detector readiness by inserting probe rows.
		waitViewDetector(t, connStr, table, capture, func() {
			insertViewWMRow(t, connStr, table, "__probe", 0, time.Now().UTC().Format(time.RFC3339Nano))
		})
		time.Sleep(500 * time.Millisecond)
		capture.drain()

		// Insert three events. The third event's ts is 10s ahead — its watermark
		// (base+10s) will advance past the 2s window end, triggering a flush.
		base := time.Now().UTC()
		insertViewWMRow(t, connStr, table, "east", 100, base.Format(time.RFC3339Nano))
		insertViewWMRow(t, connStr, table, "east", 200, base.Add(time.Second).Format(time.RFC3339Nano))
		insertViewWMRow(t, connStr, table, "east", 300, base.Add(10*time.Second).Format(time.RFC3339Nano))

		// Collect VIEW_RESULT events until the total count for 'east' reaches 3.
		// Events may be split across multiple flush cycles; we just verify the total.
		totalCnt := 0
		deadline := time.After(10 * time.Second)
		for totalCnt < 3 {
			select {
			case line := <-capture.lines:
				var ev event.Event
				if err := json.Unmarshal([]byte(line), &ev); err != nil {
					continue
				}
				if ev.Operation != "VIEW_RESULT" {
					continue
				}
				var p map[string]any
				if err := json.Unmarshal(ev.Payload, &p); err != nil {
					continue
				}
				if p["region"] != "east" {
					continue
				}
				if cnt, ok := p["cnt"].(float64); ok {
					totalCnt += int(cnt)
				}
			case <-deadline:
				t.Fatalf("timed out waiting for watermark-driven VIEW_RESULT (got total cnt=%d, want 3)", totalCnt)
			}
		}
	})

	t.Run("interval join matches correlated events", func(t *testing.T) {
		ordersTable := "join_orders"
		paymentsTable := "join_payments"
		pubName := "pgcdc_join_test"

		createJoinTable(t, connStr, ordersTable, "order_id TEXT, product TEXT")
		createJoinTable(t, connStr, paymentsTable, "order_id TEXT, amount TEXT")
		createMultiTablePublication(t, connStr, pubName, ordersTable, paymentsTable)

		def, err := view.Parse("order_payment_join",
			"SELECT a.payload.row.product, b.payload.row.amount "+
				"FROM pgcdc_events a "+
				"JOIN pgcdc_events b "+
				"ON a.payload.row.order_id = b.payload.row.order_id "+
				"WITHIN 5s "+
				"WHERE a.channel = 'pgcdc:"+ordersTable+"' AND b.channel = 'pgcdc:"+paymentsTable+"'",
			view.EmitRow, 0)
		if err != nil {
			t.Fatalf("parse join view: %v", err)
		}

		logger := testLogger()
		engine := view.NewEngine([]*view.ViewDef{def}, logger)
		va := viewadapter.New(engine, logger)
		capture := newLineCapture()
		stdoutAdapter := stdout.New(capture, logger)
		det := walreplication.New(connStr, pubName, 0, 0, false, false, logger)
		b := bus.New(64, logger)

		ctx, cancel := context.WithCancel(context.Background())
		va.SetIngestChan(b.Ingest())

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error { return b.Start(gCtx) })
		g.Go(func() error { return det.Start(gCtx, b.Ingest()) })

		viewSub, _ := b.Subscribe("view")
		g.Go(func() error { return va.Start(gCtx, viewSub) })
		stdoutSub, _ := b.Subscribe("stdout")
		g.Go(func() error { return stdoutAdapter.Start(gCtx, stdoutSub) })

		t.Cleanup(func() {
			cancel()
			g.Wait()
		})

		// Wait for WAL detector readiness by inserting a probe row.
		waitViewDetector(t, connStr, ordersTable, capture, func() {
			insertJoinRow(t, connStr, ordersTable, "order_id, product", "__probe", "0")
		})
		time.Sleep(500 * time.Millisecond)
		capture.drain()

		// Insert a matching order + payment pair with the same order_id.
		insertJoinRow(t, connStr, ordersTable, "order_id, product", "ORD-001", "widget")
		insertJoinRow(t, connStr, paymentsTable, "order_id, amount", "ORD-001", "9.99")

		// Wait for the interval join to emit a VIEW_RESULT.
		deadline := time.After(10 * time.Second)
		for {
			select {
			case line := <-capture.lines:
				var ev event.Event
				if err := json.Unmarshal([]byte(line), &ev); err != nil {
					continue
				}
				if ev.Operation != "VIEW_RESULT" {
					continue
				}
				if ev.Channel != "pgcdc:_view:order_payment_join" {
					continue
				}
				var p map[string]any
				if err := json.Unmarshal(ev.Payload, &p); err != nil {
					t.Fatalf("unmarshal join result: %v", err)
				}
				if p["a_product"] != "widget" {
					t.Errorf("a_product = %v, want widget", p["a_product"])
				}
				if p["b_amount"] != "9.99" {
					t.Errorf("b_amount = %v, want 9.99", p["b_amount"])
				}
				return // Success.
			case <-deadline:
				t.Fatal("timed out waiting for interval join VIEW_RESULT")
			}
		}
	})
}

// waitViewDetector waits until the WAL replication detector is streaming by
// repeatedly calling insertFn (which inserts a probe row) and waiting for
// any event to arrive on capture.
func waitViewDetector(t *testing.T, connStr, table string, capture *lineCapture, insertFn func()) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		insertFn()
		select {
		case <-capture.lines:
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
	t.Fatalf("waitViewDetector: WAL detector for %s did not become ready within 30s", table)
}

// waitForViewResult waits until a VIEW_RESULT event arrives on capture,
// discarding non-VIEW_RESULT events. Used to synchronize with tumbling window
// boundaries so that subsequent inserts land in a fresh window.
func waitForViewResult(t *testing.T, capture *lineCapture, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case line := <-capture.lines:
			var ev event.Event
			if err := json.Unmarshal([]byte(line), &ev); err == nil && ev.Operation == "VIEW_RESULT" {
				return
			}
		case <-deadline:
			t.Fatal("waitForViewResult: timed out waiting for VIEW_RESULT")
		}
	}
}

// createViewTable creates a table with explicit region and amount columns.
func createViewTable(t *testing.T, connStr, table string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("createViewTable connect: %v", err)
	}
	defer conn.Close(ctx)

	safeTable := pgx.Identifier{table}.Sanitize()
	sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, region TEXT, amount NUMERIC)`, safeTable)
	if _, err := conn.Exec(ctx, sql); err != nil {
		t.Fatalf("createViewTable: %v", err)
	}
	identity := fmt.Sprintf(`ALTER TABLE %s REPLICA IDENTITY FULL`, safeTable)
	if _, err := conn.Exec(ctx, identity); err != nil {
		t.Fatalf("createViewTable set replica identity: %v", err)
	}
}

// insertViewOrderRow inserts a row with region and amount into a view test table.
func insertViewOrderRow(t *testing.T, connStr, table, region string, amount float64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("insertViewOrderRow connect: %v", err)
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s (region, amount) VALUES ($1, $2)", pgx.Identifier{table}.Sanitize()), region, amount)
	if err != nil {
		t.Fatalf("insertViewOrderRow: %v", err)
	}
}

// createViewTableWithTS creates a table with region, amount, and a TEXT ts column for watermark tests.
func createViewTableWithTS(t *testing.T, connStr, table string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("createViewTableWithTS connect: %v", err)
	}
	defer conn.Close(ctx)

	safeTable := pgx.Identifier{table}.Sanitize()
	if _, err := conn.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, region TEXT, amount NUMERIC, ts TEXT)`, safeTable,
	)); err != nil {
		t.Fatalf("createViewTableWithTS: %v", err)
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s REPLICA IDENTITY FULL`, safeTable)); err != nil {
		t.Fatalf("createViewTableWithTS set replica identity: %v", err)
	}
}

// insertViewWMRow inserts a watermark-test row with an explicit RFC3339 timestamp string.
func insertViewWMRow(t *testing.T, connStr, table, region string, amount float64, ts string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("insertViewWMRow connect: %v", err)
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx,
		fmt.Sprintf("INSERT INTO %s (region, amount, ts) VALUES ($1, $2, $3)", pgx.Identifier{table}.Sanitize()),
		region, amount, ts,
	)
	if err != nil {
		t.Fatalf("insertViewWMRow: %v", err)
	}
}

// createJoinTable creates a generic join test table with the given extra columns.
func createJoinTable(t *testing.T, connStr, table, extraCols string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("createJoinTable connect: %v", err)
	}
	defer conn.Close(ctx)

	safeTable := pgx.Identifier{table}.Sanitize()
	if _, err := conn.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, %s)`, safeTable, extraCols,
	)); err != nil {
		t.Fatalf("createJoinTable %s: %v", table, err)
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s REPLICA IDENTITY FULL`, safeTable)); err != nil {
		t.Fatalf("createJoinTable set replica identity: %v", err)
	}
}

// createMultiTablePublication creates a publication covering multiple tables.
func createMultiTablePublication(t *testing.T, connStr, pubName string, tables ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("createMultiTablePublication connect: %v", err)
	}
	defer conn.Close(ctx)

	safeNames := make([]string, len(tables))
	for i, tbl := range tables {
		safeNames[i] = pgx.Identifier{tbl}.Sanitize()
	}
	sql := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s",
		pgx.Identifier{pubName}.Sanitize(),
		strings.Join(safeNames, ", "),
	)
	if _, err := conn.Exec(ctx, sql); err != nil {
		t.Fatalf("createMultiTablePublication: %v", err)
	}
}

// insertJoinRow inserts a row into a join test table with two TEXT value columns.
func insertJoinRow(t *testing.T, connStr, table, cols, val1, val2 string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("insertJoinRow connect: %v", err)
	}
	defer conn.Close(ctx)
	parts := strings.SplitN(cols, ", ", 2)
	_, err = conn.Exec(ctx,
		fmt.Sprintf("INSERT INTO %s (%s, %s) VALUES ($1, $2)",
			pgx.Identifier{table}.Sanitize(), parts[0], parts[1]),
		val1, val2,
	)
	if err != nil {
		t.Fatalf("insertJoinRow: %v", err)
	}
}
