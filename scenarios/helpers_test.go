//go:build integration

package scenarios

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/bus"
	"github.com/florinutz/pgcdc/detector/listennotify"
	"github.com/florinutz/pgcdc/detector/walreplication"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/sync/errgroup"
)

// Shared state set up once in TestMain.
var (
	testConnStr   string
	pgcdcBinary   string
	testContainer testcontainers.Container
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Start a shared PostgreSQL container for all scenarios.
	req := testcontainers.ContainerRequest{
		Image:        "pgvector/pgvector:pg16",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "pgcdc",
			"POSTGRES_PASSWORD": "pgcdc",
			"POSTGRES_DB":       "pgcdc_test",
		},
		Cmd: []string{
			"postgres",
			"-c", "wal_level=logical",
			"-c", "max_replication_slots=4",
			"-c", "max_wal_senders=4",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "start postgres container: %v\n", err)
		os.Exit(1)
	}
	testContainer = container

	host, err := container.Host(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "get container host: %v\n", err)
		os.Exit(1)
	}

	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		fmt.Fprintf(os.Stderr, "get mapped port: %v\n", err)
		os.Exit(1)
	}

	testConnStr = fmt.Sprintf("postgres://pgcdc:pgcdc@%s:%s/pgcdc_test?sslmode=disable", host, port.Port())

	// Verify connectivity.
	conn, err := pgx.Connect(ctx, testConnStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect to postgres: %v\n", err)
		os.Exit(1)
	}
	conn.Close(ctx)

	// Build the pgcdc binary for CLI-based scenarios.
	tmpDir, err := os.MkdirTemp("", "pgcdc-test-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "create temp dir: %v\n", err)
		os.Exit(1)
	}

	pgcdcBinary = filepath.Join(tmpDir, "pgcdc")
	buildCmd := exec.Command("go", "build", "-o", pgcdcBinary, "github.com/florinutz/pgcdc/cmd/pgcdc")
	buildCmd.Stderr = os.Stderr
	if err := buildCmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "build pgcdc: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()

	_ = container.Terminate(context.Background())
	os.RemoveAll(tmpDir)
	os.Exit(code)
}

// ─── Common helpers ─────────────────────────────────────────────────────────

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func startPostgres(t *testing.T) string {
	t.Helper()
	if testConnStr == "" {
		t.Fatal("shared postgres not available (TestMain failed?)")
	}
	return testConnStr
}

func sendNotify(t *testing.T, connStr, channel, payload string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("sendNotify connect: %v", err)
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, "SELECT pg_notify($1, $2)", channel, payload)
	if err != nil {
		t.Fatalf("sendNotify: %v", err)
	}
}

func createTrigger(t *testing.T, connStr, table string) string {
	t.Helper()
	channel := "pgcdc:" + table

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("createTrigger connect: %v", err)
	}
	defer conn.Close(ctx)

	safeTable := pgx.Identifier{table}.Sanitize()
	safeFuncName := pgx.Identifier{"pgcdc_notify_" + table}.Sanitize()
	safeTriggerName := pgx.Identifier{"pgcdc_" + table + "_trigger"}.Sanitize()

	sql := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, data JSONB DEFAULT '{}');

		CREATE OR REPLACE FUNCTION %s() RETURNS trigger AS $fn$
		BEGIN
		  -- channel is a PL/pgSQL string literal, not a SQL identifier
		  PERFORM pg_notify('%s', json_build_object(
		    'op', TG_OP,
		    'table', TG_TABLE_NAME,
		    'row', CASE WHEN TG_OP = 'DELETE' THEN row_to_json(OLD) ELSE row_to_json(NEW) END,
		    'old', CASE WHEN TG_OP = 'UPDATE' THEN row_to_json(OLD) ELSE NULL END
		  )::text);
		  RETURN COALESCE(NEW, OLD);
		END;
		$fn$ LANGUAGE plpgsql;

		CREATE OR REPLACE TRIGGER %s
		  AFTER INSERT OR UPDATE OR DELETE ON %s
		  FOR EACH ROW EXECUTE FUNCTION %s();
	`, safeTable, safeFuncName, channel, safeTriggerName, safeTable, safeFuncName)

	_, err = conn.Exec(ctx, sql)
	if err != nil {
		t.Fatalf("createTrigger: %v", err)
	}
	return channel
}

func insertRow(t *testing.T, connStr, table string, data map[string]any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("insertRow connect: %v", err)
	}
	defer conn.Close(ctx)

	jsonData, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("insertRow marshal: %v", err)
	}
	_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s (data) VALUES ($1)", pgx.Identifier{table}.Sanitize()), jsonData)
	if err != nil {
		t.Fatalf("insertRow: %v", err)
	}
}

func terminateBackends(t *testing.T, connStr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("terminateBackends connect: %v", err)
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx,
		"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND pid != pg_backend_pid()")
	if err != nil {
		t.Fatalf("terminateBackends: %v", err)
	}
}

func runPGCDC(args ...string) (string, error) {
	cmd := exec.Command(pgcdcBinary, args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// ─── lineCapture ────────────────────────────────────────────────────────────

// lineCapture is an io.Writer that buffers input and sends each complete line
// to a channel. Used to capture stdout adapter output without polling.
type lineCapture struct {
	mu    sync.Mutex
	buf   []byte
	lines chan string
}

func newLineCapture() *lineCapture {
	return &lineCapture{lines: make(chan string, 100)}
}

func (lc *lineCapture) Write(p []byte) (int, error) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.buf = append(lc.buf, p...)
	for {
		idx := bytes.IndexByte(lc.buf, '\n')
		if idx < 0 {
			break
		}
		line := string(lc.buf[:idx])
		lc.buf = lc.buf[idx+1:]
		if line != "" {
			select {
			case lc.lines <- line:
			default:
			}
		}
	}
	return len(p), nil
}

func (lc *lineCapture) waitLine(t *testing.T, timeout time.Duration) string {
	t.Helper()
	select {
	case line := <-lc.lines:
		return line
	case <-time.After(timeout):
		t.Fatal("timed out waiting for output line")
		return ""
	}
}

func (lc *lineCapture) drain() {
	for {
		select {
		case <-lc.lines:
		default:
			return
		}
	}
}

// ─── webhookReceiver ────────────────────────────────────────────────────────

type capturedRequest struct {
	Method  string
	Headers http.Header
	Body    []byte
}

type webhookReceiver struct {
	Server   *httptest.Server
	requests chan *capturedRequest
}

// newWebhookReceiver starts an httptest server that captures incoming requests.
// statusFn is called with the 1-indexed request number and returns the HTTP
// status code to respond with.
func newWebhookReceiver(t *testing.T, statusFn func(n int) int) *webhookReceiver {
	t.Helper()
	wr := &webhookReceiver{
		requests: make(chan *capturedRequest, 100),
	}
	var count atomic.Int32
	wr.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		wr.requests <- &capturedRequest{
			Method:  r.Method,
			Headers: r.Header.Clone(),
			Body:    body,
		}
		n := int(count.Add(1))
		w.WriteHeader(statusFn(n))
	}))
	t.Cleanup(func() { wr.Server.Close() })
	return wr
}

func (wr *webhookReceiver) waitRequest(t *testing.T, timeout time.Duration) *capturedRequest {
	t.Helper()
	select {
	case req := <-wr.requests:
		return req
	case <-time.After(timeout):
		t.Fatal("timed out waiting for webhook request")
		return nil
	}
}

func alwaysOK(_ int) int { return http.StatusOK }

// ─── SSE client ─────────────────────────────────────────────────────────────

type sseEvent struct {
	ID    string
	Event string
	Data  string
}

// connectSSE opens an SSE connection and returns a channel of parsed events.
// The connection is closed when ctx is cancelled.
func connectSSE(ctx context.Context, t *testing.T, url string) <-chan sseEvent {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("connectSSE: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("connectSSE: %v", err)
	}

	events := make(chan sseEvent, 100)
	go func() {
		defer resp.Body.Close()
		defer close(events)

		scanner := bufio.NewScanner(resp.Body)
		var current sseEvent
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				if current.Data != "" || current.ID != "" {
					events <- current
					current = sseEvent{}
				}
				continue
			}
			switch {
			case strings.HasPrefix(line, "id: "):
				current.ID = strings.TrimPrefix(line, "id: ")
			case strings.HasPrefix(line, "event: "):
				current.Event = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				current.Data = strings.TrimPrefix(line, "data: ")
			}
		}
	}()

	return events
}

// ─── Pipeline wiring ────────────────────────────────────────────────────────

// startPipeline wires detector -> bus -> adapters and starts them in an errgroup.
// The pipeline is automatically shut down when the test ends via t.Cleanup.
func startPipeline(t *testing.T, connStr string, channels []string, adapters ...adapter.Adapter) {
	t.Helper()

	logger := testLogger()
	ctx, cancel := context.WithCancel(context.Background())

	det := listennotify.New(connStr, channels, 0, 0, logger)
	b := bus.New(64, logger)

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error { return b.Start(gCtx) })
	g.Go(func() error { return det.Start(gCtx, b.Ingest()) })

	for _, a := range adapters {
		sub, err := b.Subscribe(a.Name())
		if err != nil {
			cancel()
			t.Fatalf("subscribe %s: %v", a.Name(), err)
		}
		a := a
		g.Go(func() error { return a.Start(gCtx, sub) })
	}

	t.Cleanup(func() {
		cancel()
		g.Wait()
	})
}

// ─── WAL replication helpers ────────────────────────────────────────────────

func createTable(t *testing.T, connStr, table string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("createTable connect: %v", err)
	}
	defer conn.Close(ctx)

	safeTable := pgx.Identifier{table}.Sanitize()
	sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, data JSONB DEFAULT '{}')`, safeTable)
	if _, err := conn.Exec(ctx, sql); err != nil {
		t.Fatalf("createTable: %v", err)
	}

	// Set REPLICA IDENTITY to FULL so UPDATE/DELETE include old row data.
	identity := fmt.Sprintf(`ALTER TABLE %s REPLICA IDENTITY FULL`, safeTable)
	if _, err := conn.Exec(ctx, identity); err != nil {
		t.Fatalf("createTable set replica identity: %v", err)
	}
}

func createPublication(t *testing.T, connStr, pubName, table string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("createPublication connect: %v", err)
	}
	defer conn.Close(ctx)

	sql := fmt.Sprintf(`CREATE PUBLICATION %s FOR TABLE %s`, pgx.Identifier{pubName}.Sanitize(), pgx.Identifier{table}.Sanitize())
	if _, err := conn.Exec(ctx, sql); err != nil {
		t.Fatalf("createPublication: %v", err)
	}
}

func createPublicationAllTables(t *testing.T, connStr, pubName string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("createPublicationAllTables connect: %v", err)
	}
	defer conn.Close(ctx)

	sql := fmt.Sprintf(`CREATE PUBLICATION %s FOR ALL TABLES`, pgx.Identifier{pubName}.Sanitize())
	if _, err := conn.Exec(ctx, sql); err != nil {
		t.Fatalf("createPublicationAllTables: %v", err)
	}
	t.Cleanup(func() {
		ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel2()
		conn2, err := pgx.Connect(ctx2, connStr)
		if err != nil {
			return
		}
		defer conn2.Close(ctx2)
		conn2.Exec(ctx2, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pgx.Identifier{pubName}.Sanitize()))
	})
}

func updateRow(t *testing.T, connStr, table string, id int, data map[string]any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("updateRow connect: %v", err)
	}
	defer conn.Close(ctx)

	jsonData, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("updateRow marshal: %v", err)
	}
	_, err = conn.Exec(ctx, fmt.Sprintf("UPDATE %s SET data = $1 WHERE id = $2", pgx.Identifier{table}.Sanitize()), jsonData, id)
	if err != nil {
		t.Fatalf("updateRow: %v", err)
	}
}

// insertRowsInTx inserts multiple rows in a single transaction.
func insertRowsInTx(t *testing.T, connStr, table string, rows []map[string]any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("insertRowsInTx connect: %v", err)
	}
	defer conn.Close(ctx)

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("insertRowsInTx begin: %v", err)
	}
	for _, data := range rows {
		jsonData, err := json.Marshal(data)
		if err != nil {
			t.Fatalf("insertRowsInTx marshal: %v", err)
		}
		_, err = tx.Exec(ctx, fmt.Sprintf("INSERT INTO %s (data) VALUES ($1)", pgx.Identifier{table}.Sanitize()), jsonData)
		if err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("insertRowsInTx exec: %v", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("insertRowsInTx commit: %v", err)
	}
}

func deleteRow(t *testing.T, connStr, table string, id int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("deleteRow connect: %v", err)
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE id = $1", pgx.Identifier{table}.Sanitize()), id)
	if err != nil {
		t.Fatalf("deleteRow: %v", err)
	}
}

func truncateTable(t *testing.T, connStr, table string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("truncateTable connect: %v", err)
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, fmt.Sprintf("TRUNCATE %s", pgx.Identifier{table}.Sanitize()))
	if err != nil {
		t.Fatalf("truncateTable: %v", err)
	}
}

// startWALPipeline wires WAL detector -> bus -> adapters and starts them in an errgroup.
func startWALPipeline(t *testing.T, connStr string, publication string, adapters ...adapter.Adapter) {
	t.Helper()
	startWALPipelineWithOpts(t, connStr, publication, false, false, adapters...)
}

// startWALPipelineWithTxMetadata is like startWALPipeline but enables transaction metadata.
func startWALPipelineWithTxMetadata(t *testing.T, connStr string, publication string, adapters ...adapter.Adapter) {
	t.Helper()
	startWALPipelineWithOpts(t, connStr, publication, true, false, adapters...)
}

// startWALPipelineWithTxMarkers is like startWALPipeline but enables transaction markers (implies metadata).
func startWALPipelineWithTxMarkers(t *testing.T, connStr string, publication string, adapters ...adapter.Adapter) {
	t.Helper()
	startWALPipelineWithOpts(t, connStr, publication, true, true, adapters...)
}

func startWALPipelineWithOpts(t *testing.T, connStr string, publication string, txMetadata, txMarkers bool, adapters ...adapter.Adapter) {
	t.Helper()

	logger := testLogger()
	ctx, cancel := context.WithCancel(context.Background())

	det := walreplication.New(connStr, publication, 0, 0, txMetadata, txMarkers, logger)
	b := bus.New(64, logger)

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error { return b.Start(gCtx) })
	g.Go(func() error { return det.Start(gCtx, b.Ingest()) })

	for _, a := range adapters {
		sub, err := b.Subscribe(a.Name())
		if err != nil {
			cancel()
			t.Fatalf("subscribe %s: %v", a.Name(), err)
		}
		a := a
		g.Go(func() error { return a.Start(gCtx, sub) })
	}

	t.Cleanup(func() {
		cancel()
		g.Wait()
	})
}
