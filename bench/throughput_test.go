//go:build integration

package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/bus"
	"github.com/florinutz/pgcdc/detector/walreplication"
	"github.com/florinutz/pgcdc/event"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/sync/errgroup"
)

var benchConnStr string

func TestMain(m *testing.M) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "postgres:16",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "pgcdc",
			"POSTGRES_PASSWORD": "pgcdc",
			"POSTGRES_DB":       "pgcdc_bench",
		},
		Cmd: []string{
			"postgres",
			"-c", "wal_level=logical",
			"-c", "max_replication_slots=4",
			"-c", "max_wal_senders=4",
			"-c", "shared_buffers=256MB",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "start postgres: %v\n", err)
		os.Exit(1)
	}

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "5432")
	benchConnStr = fmt.Sprintf("postgres://pgcdc:pgcdc@%s:%s/pgcdc_bench?sslmode=disable", host, port.Port())

	code := m.Run()
	_ = container.Terminate(context.Background())
	os.Exit(code)
}

// countingAdapter counts events delivered and tracks the last delivery time.
type countingAdapter struct {
	count   atomic.Int64
	lastAt  atomic.Int64
	firstAt atomic.Int64
}

func (c *countingAdapter) Start(ctx context.Context, events <-chan event.Event) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-events:
			if !ok {
				return nil
			}
			now := time.Now().UnixNano()
			c.firstAt.CompareAndSwap(0, now)
			c.lastAt.Store(now)
			c.count.Add(1)
		}
	}
}

func (c *countingAdapter) Name() string { return "bench_counter" }

func BenchmarkThroughput_WAL_10K(b *testing.B) {
	benchmarkThroughput(b, 10_000)
}

func BenchmarkThroughput_WAL_100K(b *testing.B) {
	benchmarkThroughput(b, 100_000)
}

func benchmarkThroughput(b *testing.B, rows int) {
	b.Helper()
	ctx := context.Background()

	// Create table + publication.
	table := fmt.Sprintf("bench_%d", rows)
	pubName := fmt.Sprintf("bench_pub_%d", rows)
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

	for i := 0; i < b.N; i++ {
		runThroughput(b, table, pubName, rows)
	}
}

func runThroughput(b *testing.B, table, pubName string, rows int) {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	counter := &countingAdapter{}

	det := walreplication.New(benchConnStr, pubName, 0, 0, false, false, logger)
	b2 := bus.New(65536, logger)

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error { return b2.Start(gCtx) })
	g.Go(func() error { return det.Start(gCtx, b2.Ingest()) })

	sub, err := b2.Subscribe(counter.Name())
	if err != nil {
		b.Fatal(err)
	}
	g.Go(func() error { return counter.Start(gCtx, sub) })

	// Wait for detector to be ready.
	time.Sleep(2 * time.Second)

	// Bulk insert.
	conn, err := pgx.Connect(ctx, benchConnStr)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close(ctx)

	safeTable := pgx.Identifier{table}.Sanitize()
	insertStart := time.Now()
	tx, err := conn.Begin(ctx)
	if err != nil {
		b.Fatal(err)
	}
	for j := 0; j < rows; j++ {
		data, _ := json.Marshal(map[string]interface{}{"i": j, "name": fmt.Sprintf("row-%d", j)})
		tx.Exec(ctx, fmt.Sprintf("INSERT INTO %s (data) VALUES ($1)", safeTable), data)
	}
	if err := tx.Commit(ctx); err != nil {
		b.Fatal(err)
	}
	b.Logf("inserted %d rows in %v", rows, time.Since(insertStart))

	// Wait for all events to arrive.
	deadline := time.After(3 * time.Minute)
	for counter.count.Load() < int64(rows) {
		select {
		case <-deadline:
			b.Fatalf("timeout: only received %d/%d events", counter.count.Load(), rows)
		case <-time.After(100 * time.Millisecond):
		}
	}

	firstNano := counter.firstAt.Load()
	lastNano := counter.lastAt.Load()
	duration := time.Duration(lastNano - firstNano)
	eventsPerSec := float64(rows) / duration.Seconds()

	b.ReportMetric(eventsPerSec, "events/sec")
	b.ReportMetric(duration.Seconds(), "total_sec")
	b.Logf("throughput: %.0f events/sec (%d events in %v)", eventsPerSec, rows, duration)

	cancel()
	g.Wait()

	// Cleanup.
	cleanConn, _ := pgx.Connect(context.Background(), benchConnStr)
	cleanConn.Exec(context.Background(), fmt.Sprintf("TRUNCATE %s", safeTable))
	cleanConn.Close(context.Background())
}

// Ensure our counter implements the adapter interface.
var _ adapter.Adapter = (*countingAdapter)(nil)
