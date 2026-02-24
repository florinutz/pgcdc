//go:build integration

package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/bus"
	"github.com/florinutz/pgcdc/detector/walreplication"
	"github.com/florinutz/pgcdc/event"
	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"
)

// latencyAdapter records the delivery time for each event.
type latencyAdapter struct {
	mu       sync.Mutex
	received []time.Time
}

func (l *latencyAdapter) Start(ctx context.Context, events <-chan event.Event) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-events:
			if !ok {
				return nil
			}
			l.mu.Lock()
			l.received = append(l.received, time.Now())
			l.mu.Unlock()
		}
	}
}

func (l *latencyAdapter) Name() string { return "bench_latency" }

var _ adapter.Adapter = (*latencyAdapter)(nil)

func BenchmarkLatency_WAL(b *testing.B) {
	ctx := context.Background()
	table := "bench_latency"
	pubName := "bench_pub_latency"

	conn, err := pgx.Connect(ctx, benchConnStr)
	if err != nil {
		b.Fatal(err)
	}
	safeTable := pgx.Identifier{table}.Sanitize()
	conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", safeTable))
	conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id SERIAL PRIMARY KEY, data JSONB DEFAULT '{}')", safeTable))
	conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", safeTable))
	conn.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pgx.Identifier{pubName}.Sanitize()))
	conn.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pgx.Identifier{pubName}.Sanitize(), safeTable))
	conn.Close(ctx)

	samples := 1000
	if b.N > 1 {
		samples = b.N * 100
	}

	runCtx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	lat := &latencyAdapter{}

	det := walreplication.New(benchConnStr, pubName, 0, 0, false, false, logger)
	b2 := bus.New(65536, logger)

	g, gCtx := errgroup.WithContext(runCtx)
	g.Go(func() error { return b2.Start(gCtx) })
	g.Go(func() error { return det.Start(gCtx, b2.Ingest()) })

	sub, err := b2.Subscribe(lat.Name())
	if err != nil {
		b.Fatal(err)
	}
	g.Go(func() error { return lat.Start(gCtx, sub) })

	time.Sleep(2 * time.Second)

	// Insert one at a time, recording the send time.
	insertConn, err := pgx.Connect(runCtx, benchConnStr)
	if err != nil {
		b.Fatal(err)
	}

	sendTimes := make([]time.Time, samples)
	insertTable := pgx.Identifier{table}.Sanitize()
	for i := 0; i < samples; i++ {
		data, _ := json.Marshal(map[string]interface{}{"i": i})
		sendTimes[i] = time.Now()
		insertConn.Exec(runCtx, fmt.Sprintf("INSERT INTO %s (data) VALUES ($1)", insertTable), data)
		time.Sleep(5 * time.Millisecond)
	}
	insertConn.Close(runCtx)

	// Wait for all events.
	deadline := time.After(30 * time.Second)
	for {
		lat.mu.Lock()
		got := len(lat.received)
		lat.mu.Unlock()
		if got >= samples {
			break
		}
		select {
		case <-deadline:
			b.Fatalf("timeout: only received %d/%d events", got, samples)
		case <-time.After(100 * time.Millisecond):
		}
	}

	cancel()
	g.Wait()

	// Calculate latencies.
	lat.mu.Lock()
	defer lat.mu.Unlock()

	latencies := make([]time.Duration, 0, samples)
	for i := 0; i < samples && i < len(lat.received); i++ {
		d := lat.received[i].Sub(sendTimes[i])
		if d > 0 {
			latencies = append(latencies, d)
		}
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	p50 := latencies[len(latencies)*50/100]
	p95 := latencies[len(latencies)*95/100]
	p99 := latencies[len(latencies)*99/100]

	b.ReportMetric(float64(p50.Microseconds()), "p50_us")
	b.ReportMetric(float64(p95.Microseconds()), "p95_us")
	b.ReportMetric(float64(p99.Microseconds()), "p99_us")
	b.Logf("latency p50=%v p95=%v p99=%v (n=%d)", p50, p95, p99, len(latencies))

	// Cleanup.
	cleanConn, _ := pgx.Connect(context.Background(), benchConnStr)
	cleanConn.Exec(context.Background(), fmt.Sprintf("TRUNCATE %s", pgx.Identifier{table}.Sanitize()))
	cleanConn.Close(context.Background())
}
