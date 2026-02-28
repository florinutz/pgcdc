//go:build debezium

package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/adapter/webhook"
	"github.com/florinutz/pgcdc/bus"
	"github.com/florinutz/pgcdc/detector/walreplication"
	"github.com/florinutz/pgcdc/event"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/sync/errgroup"
)

// dbzConnStr is the PG connection string for the host side (mapped port).
var dbzConnStr string

// dbzPGAlias is the hostname the PG container is reachable at inside the Docker network.
const dbzPGAlias = "pghost"

// dbzNetwork holds the shared Docker network for PG + Debezium containers.
var dbzNetwork *testcontainers.DockerNetwork

// dbzPGContainer holds the PG container so Debezium tests can reference it.
var dbzPGContainer testcontainers.Container

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Create a shared Docker network so Debezium can reach PG by alias.
	nw, err := network.New(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create docker network: %v\n", err)
		os.Exit(1)
	}
	dbzNetwork = nw

	// Start PG container on the shared network.
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
			"-c", "max_replication_slots=10",
			"-c", "max_wal_senders=10",
			"-c", "shared_buffers=256MB",
		},
		Networks:       []string{nw.Name},
		NetworkAliases: map[string][]string{nw.Name: {dbzPGAlias}},
		WaitingFor:     wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "start postgres: %v\n", err)
		os.Exit(1)
	}
	dbzPGContainer = container

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "5432")
	dbzConnStr = fmt.Sprintf("postgres://pgcdc:pgcdc@%s:%s/pgcdc_bench?sslmode=disable", host, port.Port())

	code := m.Run()

	_ = container.Terminate(context.Background())
	_ = nw.Remove(context.Background())
	os.Exit(code)
}

// ---------------------------------------------------------------------------
// HTTP event counter — receives POST callbacks from Debezium HTTP sink.
// ---------------------------------------------------------------------------

type httpEventCounter struct {
	count   atomic.Int64
	firstAt atomic.Int64 // UnixNano of first event
	lastAt  atomic.Int64 // UnixNano of last event

	mu    sync.Mutex
	times []time.Time // per-event arrival times

	server   *http.Server
	listener net.Listener
	addr     string // host:port
}

func newHTTPEventCounter() (*httpEventCounter, error) {
	ln, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}

	c := &httpEventCounter{
		listener: ln,
		addr:     ln.Addr().String(),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", c.handleEvent)
	c.server = &http.Server{Handler: mux}

	go func() {
		if err := c.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			slog.Error("http event counter serve error", "error", err)
		}
	}()

	return c, nil
}

func (c *httpEventCounter) handleEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	// Drain body so Debezium doesn't get a broken pipe.
	_, _ = io.Copy(io.Discard, r.Body)
	_ = r.Body.Close()

	now := time.Now()
	nowNano := now.UnixNano()

	c.firstAt.CompareAndSwap(0, nowNano)
	c.lastAt.Store(nowNano)
	c.count.Add(1)

	c.mu.Lock()
	c.times = append(c.times, now)
	c.mu.Unlock()

	w.WriteHeader(http.StatusOK)
}

func (c *httpEventCounter) close() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = c.server.Shutdown(ctx)
}

// hostIP returns the IP address of the host as seen from Docker containers.
// On Docker Desktop (macOS/Windows) this is "host.docker.internal".
// On Linux, we resolve the host gateway from the Docker bridge.
func hostIP() string {
	// Docker Desktop provides host.docker.internal automatically.
	// For Linux, testcontainers-go sets the extra host, but Debezium Server
	// (a generic container) needs an explicit IP. Try host.docker.internal first;
	// fall back to the default bridge gateway.
	return "host.docker.internal"
}

// ---------------------------------------------------------------------------
// Debezium Server container helper
// ---------------------------------------------------------------------------

func startDebeziumServer(ctx context.Context, t testing.TB, sinkURL string) testcontainers.Container {
	t.Helper()

	req := testcontainers.ContainerRequest{
		Image:        "quay.io/debezium/server:2.5",
		ExposedPorts: []string{"8080/tcp"},
		Env: map[string]string{
			// Source connector.
			"DEBEZIUM_SOURCE_CONNECTOR_CLASS":                       "io.debezium.connector.postgresql.PostgresConnector",
			"DEBEZIUM_SOURCE_OFFSET_STORAGE_FILE_FILENAME":          "/tmp/offsets.dat",
			"DEBEZIUM_SOURCE_SCHEMA_HISTORY_INTERNAL_FILE_FILENAME": "/tmp/schema.dat",
			"DEBEZIUM_SOURCE_DATABASE_HOSTNAME":                     dbzPGAlias,
			"DEBEZIUM_SOURCE_DATABASE_PORT":                         "5432",
			"DEBEZIUM_SOURCE_DATABASE_USER":                         "pgcdc",
			"DEBEZIUM_SOURCE_DATABASE_PASSWORD":                     "pgcdc",
			"DEBEZIUM_SOURCE_DATABASE_DBNAME":                       "pgcdc_bench",
			"DEBEZIUM_SOURCE_TOPIC_PREFIX":                          "dbz",
			"DEBEZIUM_SOURCE_PLUGIN_NAME":                           "pgoutput",
			"DEBEZIUM_SOURCE_SLOT_NAME":                             "debezium_bench",
			"DEBEZIUM_SOURCE_TABLE_INCLUDE_LIST":                    "public.dbz_bench",

			// HTTP sink.
			"DEBEZIUM_SINK_TYPE":     "http",
			"DEBEZIUM_SINK_HTTP_URL": sinkURL,

			// Value format.
			"DEBEZIUM_FORMAT_VALUE": "json",
		},
		Networks:       []string{dbzNetwork.Name},
		NetworkAliases: map[string][]string{dbzNetwork.Name: {"debezium"}},
		WaitingFor:     wait.ForLog("Starting streaming").WithStartupTimeout(120 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start debezium server: %v", err)
	}
	return container
}

// ---------------------------------------------------------------------------
// pgcdc counting adapter (same pattern as throughput_test.go)
// ---------------------------------------------------------------------------

type dbzCountingAdapter struct {
	count   atomic.Int64
	firstAt atomic.Int64
	lastAt  atomic.Int64
}

func (c *dbzCountingAdapter) Start(ctx context.Context, events <-chan event.Event) error {
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

func (c *dbzCountingAdapter) Name() string { return "dbz_bench_counter" }

var _ adapter.Adapter = (*dbzCountingAdapter)(nil)

// dbzLatencyAdapter records per-event arrival times.
type dbzLatencyAdapter struct {
	mu       sync.Mutex
	received []time.Time
}

func (l *dbzLatencyAdapter) Start(ctx context.Context, events <-chan event.Event) error {
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

func (l *dbzLatencyAdapter) Name() string { return "dbz_bench_latency" }

var _ adapter.Adapter = (*dbzLatencyAdapter)(nil)

// ---------------------------------------------------------------------------
// Helper: create table + publication for a benchmark run.
// ---------------------------------------------------------------------------

func dbzSetupTable(ctx context.Context, t testing.TB, table, pubName string) {
	t.Helper()
	conn, err := pgx.Connect(ctx, dbzConnStr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	safeTable := pgx.Identifier{table}.Sanitize()
	safePub := pgx.Identifier{pubName}.Sanitize()

	for _, sql := range []string{
		fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", safeTable),
		fmt.Sprintf("CREATE TABLE %s (id SERIAL PRIMARY KEY, data JSONB DEFAULT '{}')", safeTable),
		fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", safeTable),
		fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", safePub),
		fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", safePub, safeTable),
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("setup sql %q: %v", sql, err)
		}
	}
}

// dbzCleanupSlot terminates any active backend using the slot, then drops it.
func dbzCleanupSlot(ctx context.Context, slotName string) {
	conn, err := pgx.Connect(ctx, dbzConnStr)
	if err != nil {
		return
	}
	defer conn.Close(ctx)
	// Terminate any backend holding the slot active.
	conn.Exec(ctx, `SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1 AND active`, slotName)
	time.Sleep(500 * time.Millisecond)
	conn.Exec(ctx, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1", slotName)
}

// ---------------------------------------------------------------------------
// BenchmarkComparison_Startup
// ---------------------------------------------------------------------------

func BenchmarkComparison_Startup(b *testing.B) {
	table := "dbz_bench"
	pubName := "dbz_bench_pub"

	ctx := context.Background()
	dbzSetupTable(ctx, b, table, pubName)

	b.Run("pgcdc", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			benchStartupPgcdc(b, table, pubName)
		}
	})

	b.Run("debezium", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			benchStartupDebezium(b, table)
		}
	})
}

func benchStartupPgcdc(b *testing.B, table, pubName string) {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	counter := &dbzCountingAdapter{}

	det := walreplication.New(dbzConnStr, pubName, 0, 0, false, false, logger)
	b2 := bus.New(1024, logger)

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error { return b2.Start(gCtx) })
	g.Go(func() error { return det.Start(gCtx, b2.Ingest()) })

	sub, err := b2.Subscribe(counter.Name())
	if err != nil {
		b.Fatal(err)
	}
	g.Go(func() error { return counter.Start(gCtx, sub) })

	start := time.Now()

	// Wait for detector to be ready, then insert one row.
	time.Sleep(2 * time.Second)

	conn, err := pgx.Connect(ctx, dbzConnStr)
	if err != nil {
		b.Fatal(err)
	}
	safeTable := pgx.Identifier{table}.Sanitize()
	conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s (data) VALUES ('{\"startup\": true}')", safeTable))
	conn.Close(ctx)

	// Wait for first event.
	deadline := time.After(30 * time.Second)
	for counter.count.Load() < 1 {
		select {
		case <-deadline:
			b.Fatal("timeout waiting for first pgcdc event")
		case <-time.After(50 * time.Millisecond):
		}
	}

	startupDuration := time.Since(start)
	b.ReportMetric(float64(startupDuration.Milliseconds()), "startup_ms")
	b.Logf("pgcdc startup to first event: %v", startupDuration)

	cancel()
	g.Wait()

	// Cleanup.
	cleanCtx := context.Background()
	cleanConn, _ := pgx.Connect(cleanCtx, dbzConnStr)
	cleanConn.Exec(cleanCtx, fmt.Sprintf("TRUNCATE %s", pgx.Identifier{table}.Sanitize()))
	cleanConn.Close(cleanCtx)
}

func benchStartupDebezium(b *testing.B, table string) {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Clean up any leftover Debezium replication slot from previous runs.
	dbzCleanupSlot(ctx, "debezium_bench")

	counter, err := newHTTPEventCounter()
	if err != nil {
		b.Fatal(err)
	}
	defer counter.close()

	// Debezium connects to PG via the Docker network alias. The HTTP sink
	// needs to reach the test process on the host. Use host.docker.internal.
	_, sinkPort, _ := net.SplitHostPort(counter.addr)
	sinkURL := fmt.Sprintf("http://%s:%s/", hostIP(), sinkPort)

	start := time.Now()
	dbzContainer := startDebeziumServer(ctx, b, sinkURL)
	defer func() {
		_ = dbzContainer.Terminate(context.Background())
		dbzCleanupSlot(context.Background(), "debezium_bench")
	}()

	// Insert one row after Debezium is started.
	conn, err := pgx.Connect(ctx, dbzConnStr)
	if err != nil {
		b.Fatal(err)
	}
	safeTable := pgx.Identifier{table}.Sanitize()
	conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s (data) VALUES ('{\"startup\": true}')", safeTable))
	conn.Close(ctx)

	// Wait for first event.
	deadline := time.After(120 * time.Second)
	for counter.count.Load() < 1 {
		select {
		case <-deadline:
			b.Fatal("timeout waiting for first Debezium event")
		case <-time.After(100 * time.Millisecond):
		}
	}

	startupDuration := time.Since(start)
	b.ReportMetric(float64(startupDuration.Milliseconds()), "startup_ms")
	b.Logf("debezium startup to first event: %v", startupDuration)

	// Cleanup.
	cleanCtx := context.Background()
	cleanConn, _ := pgx.Connect(cleanCtx, dbzConnStr)
	cleanConn.Exec(cleanCtx, fmt.Sprintf("TRUNCATE %s", pgx.Identifier{table}.Sanitize()))
	cleanConn.Close(cleanCtx)
}

// ---------------------------------------------------------------------------
// BenchmarkComparison_Memory
// ---------------------------------------------------------------------------

func BenchmarkComparison_Memory(b *testing.B) {
	table := "dbz_bench"
	pubName := "dbz_bench_pub"

	ctx := context.Background()
	dbzSetupTable(ctx, b, table, pubName)

	b.Run("pgcdc", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			benchMemoryPgcdc(b, pubName)
		}
	})

	b.Run("debezium", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			benchMemoryDebezium(b)
		}
	})
}

func benchMemoryPgcdc(b *testing.B, pubName string) {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	det := walreplication.New(dbzConnStr, pubName, 0, 0, false, false, logger)
	b2 := bus.New(1024, logger)
	null := &dbzCountingAdapter{}

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error { return b2.Start(gCtx) })
	g.Go(func() error { return det.Start(gCtx, b2.Ingest()) })
	sub, err := b2.Subscribe(null.Name())
	if err != nil {
		b.Fatal(err)
	}
	g.Go(func() error { return null.Start(gCtx, sub) })

	// Let pipeline stabilize.
	time.Sleep(5 * time.Second)

	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	allocMB := float64(m.Alloc) / 1024 / 1024
	sysMB := float64(m.Sys) / 1024 / 1024

	b.ReportMetric(allocMB, "alloc_MB")
	b.ReportMetric(sysMB, "sys_MB")
	b.Logf("pgcdc idle: alloc=%.1f MB, sys=%.1f MB, goroutines=%d", allocMB, sysMB, runtime.NumGoroutine())

	cancel()
	g.Wait()
}

func benchMemoryDebezium(b *testing.B) {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Clean up any leftover Debezium replication slot.
	dbzCleanupSlot(ctx, "debezium_bench")

	counter, err := newHTTPEventCounter()
	if err != nil {
		b.Fatal(err)
	}
	defer counter.close()

	_, sinkPort, _ := net.SplitHostPort(counter.addr)
	sinkURL := fmt.Sprintf("http://%s:%s/", hostIP(), sinkPort)

	dbzContainer := startDebeziumServer(ctx, b, sinkURL)
	defer func() {
		_ = dbzContainer.Terminate(context.Background())
		dbzCleanupSlot(context.Background(), "debezium_bench")
	}()

	// Let Debezium stabilize.
	time.Sleep(10 * time.Second)

	// Read container memory usage via Docker stats API.
	// The testcontainers-go library does not expose a direct Stats method,
	// so we use the underlying Docker client.
	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		b.Fatalf("create docker provider: %v", err)
	}
	defer provider.Close()

	containerID := dbzContainer.GetContainerID()
	statsResp, err := provider.Client().ContainerStats(ctx, containerID, false)
	if err != nil {
		b.Fatalf("container stats: %v", err)
	}
	defer statsResp.Body.Close()

	var stats struct {
		MemoryStats struct {
			Usage uint64 `json:"usage"`
			Limit uint64 `json:"limit"`
		} `json:"memory_stats"`
	}
	if err := json.NewDecoder(statsResp.Body).Decode(&stats); err != nil {
		b.Fatalf("decode container stats: %v", err)
	}

	memMB := float64(stats.MemoryStats.Usage) / 1024 / 1024
	b.ReportMetric(memMB, "container_MB")
	b.Logf("debezium idle memory: %.1f MB", memMB)
}

// ---------------------------------------------------------------------------
// BenchmarkComparison_Throughput
// ---------------------------------------------------------------------------

func BenchmarkComparison_Throughput(b *testing.B) {
	const rows = 10_000
	table := "dbz_bench"
	pubName := "dbz_bench_pub"

	ctx := context.Background()
	dbzSetupTable(ctx, b, table, pubName)

	b.Run("pgcdc", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			benchThroughputPgcdc(b, table, pubName, rows)
		}
	})

	b.Run("pgcdc_webhook", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			benchThroughputPgcdcWebhook(b, table, pubName, rows)
		}
	})

	b.Run("debezium", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			benchThroughputDebezium(b, table, rows)
		}
	})
}

func benchThroughputPgcdc(b *testing.B, table, pubName string, rows int) {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	counter := &dbzCountingAdapter{}

	det := walreplication.New(dbzConnStr, pubName, 0, 0, false, false, logger)
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
	conn, err := pgx.Connect(ctx, dbzConnStr)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close(ctx)

	safeTable := pgx.Identifier{table}.Sanitize()
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

	// Wait for all events.
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
	b.Logf("pgcdc throughput: %.0f events/sec (%d events in %v)", eventsPerSec, rows, duration)

	cancel()
	g.Wait()

	// Cleanup.
	cleanCtx := context.Background()
	cleanConn, _ := pgx.Connect(cleanCtx, dbzConnStr)
	cleanConn.Exec(cleanCtx, fmt.Sprintf("TRUNCATE %s", pgx.Identifier{table}.Sanitize()))
	cleanConn.Close(cleanCtx)
}

func benchThroughputPgcdcWebhook(b *testing.B, table, pubName string, rows int) {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	counter, err := newHTTPEventCounter()
	if err != nil {
		b.Fatal(err)
	}
	defer counter.close()

	_, sinkPort, _ := net.SplitHostPort(counter.addr)
	sinkURL := fmt.Sprintf("http://127.0.0.1:%s/", sinkPort)

	wh := webhook.New(sinkURL, nil, "", 3, 30*time.Second, 100*time.Millisecond, 5*time.Second, logger)
	det := walreplication.New(dbzConnStr, pubName, 0, 0, false, false, logger)
	b2 := bus.New(65536, logger)

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error { return b2.Start(gCtx) })
	g.Go(func() error { return det.Start(gCtx, b2.Ingest()) })

	sub, err := b2.Subscribe(wh.Name())
	if err != nil {
		b.Fatal(err)
	}
	g.Go(func() error { return wh.Start(gCtx, sub) })

	// Wait for detector to be ready.
	time.Sleep(2 * time.Second)

	// Bulk insert.
	conn, err := pgx.Connect(ctx, dbzConnStr)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close(ctx)

	safeTable := pgx.Identifier{table}.Sanitize()
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

	// Wait for all events.
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
	b.Logf("pgcdc_webhook throughput: %.0f events/sec (%d events in %v)", eventsPerSec, rows, duration)

	cancel()
	g.Wait()

	// Cleanup.
	cleanCtx := context.Background()
	cleanConn, _ := pgx.Connect(cleanCtx, dbzConnStr)
	cleanConn.Exec(cleanCtx, fmt.Sprintf("TRUNCATE %s", pgx.Identifier{table}.Sanitize()))
	cleanConn.Close(cleanCtx)
}

func benchThroughputDebezium(b *testing.B, table string, rows int) {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Clean up any leftover Debezium replication slot.
	dbzCleanupSlot(ctx, "debezium_bench")

	counter, err := newHTTPEventCounter()
	if err != nil {
		b.Fatal(err)
	}
	defer counter.close()

	_, sinkPort, _ := net.SplitHostPort(counter.addr)
	sinkURL := fmt.Sprintf("http://%s:%s/", hostIP(), sinkPort)

	dbzContainer := startDebeziumServer(ctx, b, sinkURL)
	defer func() {
		_ = dbzContainer.Terminate(context.Background())
		dbzCleanupSlot(context.Background(), "debezium_bench")
	}()

	// Give Debezium a moment to be fully ready after "Starting streaming".
	time.Sleep(5 * time.Second)

	// Bulk insert.
	conn, err := pgx.Connect(ctx, dbzConnStr)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close(ctx)

	safeTable := pgx.Identifier{table}.Sanitize()
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

	// Wait for all events (Debezium is slower, use longer timeout).
	deadline := time.After(5 * time.Minute)
	for counter.count.Load() < int64(rows) {
		select {
		case <-deadline:
			b.Fatalf("timeout: only received %d/%d events", counter.count.Load(), rows)
		case <-time.After(200 * time.Millisecond):
		}
	}

	firstNano := counter.firstAt.Load()
	lastNano := counter.lastAt.Load()
	duration := time.Duration(lastNano - firstNano)
	eventsPerSec := float64(rows) / duration.Seconds()

	b.ReportMetric(eventsPerSec, "events/sec")
	b.ReportMetric(duration.Seconds(), "total_sec")
	b.Logf("debezium throughput: %.0f events/sec (%d events in %v)", eventsPerSec, rows, duration)

	// Cleanup.
	cleanCtx := context.Background()
	cleanConn, _ := pgx.Connect(cleanCtx, dbzConnStr)
	cleanConn.Exec(cleanCtx, fmt.Sprintf("TRUNCATE %s", pgx.Identifier{table}.Sanitize()))
	cleanConn.Close(cleanCtx)
}

// ---------------------------------------------------------------------------
// BenchmarkComparison_Latency
// ---------------------------------------------------------------------------

func BenchmarkComparison_Latency(b *testing.B) {
	table := "dbz_bench"
	pubName := "dbz_bench_pub"

	ctx := context.Background()
	dbzSetupTable(ctx, b, table, pubName)

	b.Run("pgcdc", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			benchLatencyPgcdc(b, table, pubName)
		}
	})

	b.Run("pgcdc_webhook", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			benchLatencyPgcdcWebhook(b, table, pubName)
		}
	})

	b.Run("debezium", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			benchLatencyDebezium(b, table)
		}
	})
}

func benchLatencyPgcdc(b *testing.B, table, pubName string) {
	b.Helper()
	const samples = 200

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	lat := &dbzLatencyAdapter{}

	det := walreplication.New(dbzConnStr, pubName, 0, 0, false, false, logger)
	b2 := bus.New(65536, logger)

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error { return b2.Start(gCtx) })
	g.Go(func() error { return det.Start(gCtx, b2.Ingest()) })

	sub, err := b2.Subscribe(lat.Name())
	if err != nil {
		b.Fatal(err)
	}
	g.Go(func() error { return lat.Start(gCtx, sub) })

	time.Sleep(2 * time.Second)

	// Insert one at a time, recording send times.
	conn, err := pgx.Connect(ctx, dbzConnStr)
	if err != nil {
		b.Fatal(err)
	}

	safeTable := pgx.Identifier{table}.Sanitize()
	sendTimes := make([]time.Time, samples)
	for i := 0; i < samples; i++ {
		data, _ := json.Marshal(map[string]interface{}{"i": i})
		sendTimes[i] = time.Now()
		conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s (data) VALUES ($1)", safeTable), data)
		time.Sleep(10 * time.Millisecond)
	}
	conn.Close(ctx)

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
			lat.mu.Lock()
			b.Fatalf("timeout: only received %d/%d events", len(lat.received), samples)
			lat.mu.Unlock()
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

	if len(latencies) == 0 {
		b.Fatal("no valid latency samples")
	}

	p50 := latencies[len(latencies)*50/100]
	p99 := latencies[len(latencies)*99/100]

	b.ReportMetric(float64(p50.Microseconds()), "p50_us")
	b.ReportMetric(float64(p99.Microseconds()), "p99_us")
	b.Logf("pgcdc latency p50=%v p99=%v (n=%d)", p50, p99, len(latencies))

	// Cleanup.
	cleanCtx := context.Background()
	cleanConn, _ := pgx.Connect(cleanCtx, dbzConnStr)
	cleanConn.Exec(cleanCtx, fmt.Sprintf("TRUNCATE %s", pgx.Identifier{table}.Sanitize()))
	cleanConn.Close(cleanCtx)
}

func benchLatencyPgcdcWebhook(b *testing.B, table, pubName string) {
	b.Helper()
	const samples = 200

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	counter, err := newHTTPEventCounter()
	if err != nil {
		b.Fatal(err)
	}
	defer counter.close()

	_, sinkPort, _ := net.SplitHostPort(counter.addr)
	sinkURL := fmt.Sprintf("http://127.0.0.1:%s/", sinkPort)

	wh := webhook.New(sinkURL, nil, "", 3, 30*time.Second, 100*time.Millisecond, 5*time.Second, logger)
	det := walreplication.New(dbzConnStr, pubName, 0, 0, false, false, logger)
	b2 := bus.New(65536, logger)

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error { return b2.Start(gCtx) })
	g.Go(func() error { return det.Start(gCtx, b2.Ingest()) })

	sub, err := b2.Subscribe(wh.Name())
	if err != nil {
		b.Fatal(err)
	}
	g.Go(func() error { return wh.Start(gCtx, sub) })

	time.Sleep(2 * time.Second)

	// Insert one at a time, recording send times.
	conn, err := pgx.Connect(ctx, dbzConnStr)
	if err != nil {
		b.Fatal(err)
	}

	safeTable := pgx.Identifier{table}.Sanitize()
	sendTimes := make([]time.Time, samples)
	for i := 0; i < samples; i++ {
		data, _ := json.Marshal(map[string]interface{}{"i": i})
		sendTimes[i] = time.Now()
		conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s (data) VALUES ($1)", safeTable), data)
		time.Sleep(10 * time.Millisecond)
	}
	conn.Close(ctx)

	// Wait for all events.
	deadline := time.After(30 * time.Second)
	for counter.count.Load() < int64(samples) {
		select {
		case <-deadline:
			b.Fatalf("timeout: only received %d/%d events", counter.count.Load(), samples)
		case <-time.After(100 * time.Millisecond):
		}
	}

	cancel()
	g.Wait()

	// Calculate latencies.
	counter.mu.Lock()
	defer counter.mu.Unlock()

	latencies := make([]time.Duration, 0, samples)
	for i := 0; i < samples && i < len(counter.times); i++ {
		d := counter.times[i].Sub(sendTimes[i])
		if d > 0 {
			latencies = append(latencies, d)
		}
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	if len(latencies) == 0 {
		b.Fatal("no valid latency samples")
	}

	p50 := latencies[len(latencies)*50/100]
	p99 := latencies[len(latencies)*99/100]

	b.ReportMetric(float64(p50.Microseconds()), "p50_us")
	b.ReportMetric(float64(p99.Microseconds()), "p99_us")
	b.Logf("pgcdc_webhook latency p50=%v p99=%v (n=%d)", p50, p99, len(latencies))

	// Cleanup.
	cleanCtx := context.Background()
	cleanConn, _ := pgx.Connect(cleanCtx, dbzConnStr)
	cleanConn.Exec(cleanCtx, fmt.Sprintf("TRUNCATE %s", pgx.Identifier{table}.Sanitize()))
	cleanConn.Close(cleanCtx)
}

func benchLatencyDebezium(b *testing.B, table string) {
	b.Helper()
	const samples = 200

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Clean up any leftover Debezium replication slot.
	dbzCleanupSlot(ctx, "debezium_bench")

	counter, err := newHTTPEventCounter()
	if err != nil {
		b.Fatal(err)
	}
	defer counter.close()

	_, sinkPort, _ := net.SplitHostPort(counter.addr)
	sinkURL := fmt.Sprintf("http://%s:%s/", hostIP(), sinkPort)

	dbzContainer := startDebeziumServer(ctx, b, sinkURL)
	defer func() {
		_ = dbzContainer.Terminate(context.Background())
		dbzCleanupSlot(context.Background(), "debezium_bench")
	}()

	// Wait for Debezium to be fully ready.
	time.Sleep(5 * time.Second)

	// Insert one at a time, recording send times.
	conn, err := pgx.Connect(ctx, dbzConnStr)
	if err != nil {
		b.Fatal(err)
	}

	safeTable := pgx.Identifier{table}.Sanitize()
	sendTimes := make([]time.Time, samples)
	for i := 0; i < samples; i++ {
		data, _ := json.Marshal(map[string]interface{}{"i": i})
		sendTimes[i] = time.Now()
		conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s (data) VALUES ($1)", safeTable), data)
		time.Sleep(20 * time.Millisecond) // Slightly slower cadence for Debezium.
	}
	conn.Close(ctx)

	// Wait for all events.
	deadline := time.After(120 * time.Second)
	for counter.count.Load() < int64(samples) {
		select {
		case <-deadline:
			b.Fatalf("timeout: only received %d/%d events", counter.count.Load(), samples)
		case <-time.After(200 * time.Millisecond):
		}
	}

	// Calculate latencies from send times vs arrival times.
	counter.mu.Lock()
	defer counter.mu.Unlock()

	latencies := make([]time.Duration, 0, samples)
	for i := 0; i < samples && i < len(counter.times); i++ {
		d := counter.times[i].Sub(sendTimes[i])
		if d > 0 {
			latencies = append(latencies, d)
		}
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	if len(latencies) == 0 {
		b.Fatal("no valid latency samples")
	}

	p50 := latencies[len(latencies)*50/100]
	p99 := latencies[len(latencies)*99/100]

	b.ReportMetric(float64(p50.Microseconds()), "p50_us")
	b.ReportMetric(float64(p99.Microseconds()), "p99_us")
	b.Logf("debezium latency p50=%v p99=%v (n=%d)", p50, p99, len(latencies))

	// Cleanup.
	cleanCtx := context.Background()
	cleanConn, _ := pgx.Connect(cleanCtx, dbzConnStr)
	cleanConn.Exec(cleanCtx, fmt.Sprintf("TRUNCATE %s", pgx.Identifier{table}.Sanitize()))
	cleanConn.Close(cleanCtx)
}
