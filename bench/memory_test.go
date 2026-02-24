//go:build integration

package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/bus"
	"github.com/florinutz/pgcdc/detector/walreplication"
	"github.com/florinutz/pgcdc/event"
	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"
)

// devNullAdapter discards all events.
type devNullAdapter struct{}

func (d devNullAdapter) Start(ctx context.Context, events <-chan event.Event) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-events:
			if !ok {
				return nil
			}
		}
	}
}

func (d devNullAdapter) Name() string { return "bench_devnull" }

func BenchmarkMemory_Idle(b *testing.B) {
	for i := 0; i < b.N; i++ {
		measureIdleMemory(b)
	}
}

func measureIdleMemory(b *testing.B) {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	table := "bench_mem_idle"
	pubName := "bench_pub_mem_idle"

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

	det := walreplication.New(benchConnStr, pubName, 0, 0, false, false, logger)
	b2 := bus.New(1024, logger)
	null := devNullAdapter{}

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error { return b2.Start(gCtx) })
	g.Go(func() error { return det.Start(gCtx, b2.Ingest()) })
	sub, err := b2.Subscribe(null.Name())
	if err != nil {
		b.Fatal(err)
	}
	g.Go(func() error { return null.Start(gCtx, sub) })

	time.Sleep(3 * time.Second)

	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	b.ReportMetric(float64(m.Alloc)/1024/1024, "alloc_MB")
	b.ReportMetric(float64(m.Sys)/1024/1024, "sys_MB")
	b.Logf("idle: alloc=%.1f MB, sys=%.1f MB, goroutines=%d", float64(m.Alloc)/1024/1024, float64(m.Sys)/1024/1024, runtime.NumGoroutine())

	cancel()
	g.Wait()
}

func BenchmarkMemory_Sustained(b *testing.B) {
	for i := 0; i < b.N; i++ {
		measureSustainedMemory(b, 10_000)
	}
}

func measureSustainedMemory(b *testing.B, eventsPerSec int) {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	table := "bench_mem_sustained"
	pubName := "bench_pub_mem_sustained"

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

	det := walreplication.New(benchConnStr, pubName, 0, 0, false, false, logger)
	b2 := bus.New(65536, logger)
	null := devNullAdapter{}

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error { return b2.Start(gCtx) })
	g.Go(func() error { return det.Start(gCtx, b2.Ingest()) })
	sub, err := b2.Subscribe(null.Name())
	if err != nil {
		b.Fatal(err)
	}
	g.Go(func() error { return null.Start(gCtx, sub) })

	time.Sleep(2 * time.Second)

	// Sustained insert load for 10 seconds.
	insertConn, err := pgx.Connect(ctx, benchConnStr)
	if err != nil {
		b.Fatal(err)
	}

	batchSize := eventsPerSec / 10 // Insert in 100ms batches
	if batchSize < 1 {
		batchSize = 1
	}
	safeTable2 := pgx.Identifier{table}.Sanitize()

	for sec := 0; sec < 10; sec++ {
		for batch := 0; batch < 10; batch++ {
			tx, err := insertConn.Begin(ctx)
			if err != nil {
				b.Fatal(err)
			}
			for j := 0; j < batchSize; j++ {
				data, _ := json.Marshal(map[string]interface{}{"i": sec*eventsPerSec + batch*batchSize + j})
				tx.Exec(ctx, fmt.Sprintf("INSERT INTO %s (data) VALUES ($1)", safeTable2), data)
			}
			if err := tx.Commit(ctx); err != nil {
				b.Fatal(err)
			}
			time.Sleep(90 * time.Millisecond) // ~100ms between batches
		}
	}
	insertConn.Close(ctx)

	time.Sleep(5 * time.Second) // Let pipeline catch up.

	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	b.ReportMetric(float64(m.Alloc)/1024/1024, "alloc_MB")
	b.ReportMetric(float64(m.Sys)/1024/1024, "sys_MB")
	b.Logf("sustained (%d events/sec): alloc=%.1f MB, sys=%.1f MB, goroutines=%d",
		eventsPerSec, float64(m.Alloc)/1024/1024, float64(m.Sys)/1024/1024, runtime.NumGoroutine())

	cancel()
	g.Wait()
}
