//go:build integration

package scenarios

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	embeddingadapter "github.com/florinutz/pgcdc/adapter/embedding"
	"github.com/jackc/pgx/v5"
)

func TestScenario_Embedding(t *testing.T) {
	connStr := startPostgres(t)

	t.Run("happy path: insert update delete lifecycle", func(t *testing.T) {
		table := "embedding_test_articles"
		embTable := "embedding_test_vectors"

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		conn, err := pgx.Connect(ctx, connStr)
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		defer conn.Close(ctx)

		// Create source table with trigger.
		channel := createTrigger(t, connStr, table)

		// Create pgvector embeddings table.
		_, err = conn.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS vector`)
		if err != nil {
			t.Fatalf("create vector extension: %v", err)
		}
		safeEmb := pgx.Identifier{embTable}.Sanitize()
		_, err = conn.Exec(ctx, fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				source_id    TEXT PRIMARY KEY,
				source_table TEXT NOT NULL,
				content      TEXT NOT NULL,
				embedding    vector(4),
				event_id     TEXT NOT NULL,
				created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
				updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
			)`, safeEmb))
		if err != nil {
			t.Fatalf("create embeddings table: %v", err)
		}
		conn.Close(ctx)

		// Mock embedding API: returns a fixed 4-dimensional vector.
		fixedVec := []float64{0.1, 0.2, 0.3, 0.4}
		apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data":  []map[string]any{{"embedding": fixedVec, "index": 0}},
				"usage": map[string]any{"total_tokens": 5},
			})
		}))
		defer apiServer.Close()

		// Wire: LISTEN/NOTIFY → bus → embedding adapter.
		adapter := embeddingadapter.New(
			apiServer.URL,
			"",
			"test-model",
			[]string{"data"}, // embed the "data" column
			"id",
			connStr,
			embTable,
			4, // dimension
			3, // maxRetries
			0, // timeout (default)
			0, // backoffBase (default)
			0, // backoffCap (default)
			false,
			testLogger(),
		)
		startPipeline(t, connStr, []string{channel}, adapter)

		// Give the pipeline a moment to start.
		time.Sleep(200 * time.Millisecond)

		// INSERT a row: triggers NOTIFY with {"op":"INSERT","table":"...","row":{"id":1,"data":{"text":"hello"}}}
		insertRow(t, connStr, table, map[string]any{"text": "hello"})

		// Wait for vector to appear.
		waitForEmbedding(t, connStr, embTable, "1", 10*time.Second)

		// Verify embedding row content.
		conn2, _ := pgx.Connect(context.Background(), connStr)
		defer conn2.Close(context.Background())
		var sourceTable, content string
		err = conn2.QueryRow(context.Background(),
			fmt.Sprintf(`SELECT source_table, content FROM %s WHERE source_id = $1`, safeEmb), "1").
			Scan(&sourceTable, &content)
		if err != nil {
			t.Fatalf("query embedding row: %v", err)
		}
		if sourceTable != table {
			t.Errorf("source_table = %q, want %q", sourceTable, table)
		}
		if content == "" {
			t.Error("content is empty")
		}

		// UPDATE the row: should re-embed and update the row.
		updateRow(t, connStr, table, 1, map[string]any{"text": "hello world updated"})

		// Wait for updated_at to change (allow a second for the upsert to land).
		time.Sleep(500 * time.Millisecond)
		var count int
		_ = conn2.QueryRow(context.Background(),
			fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE source_id = '1'`, safeEmb)).Scan(&count)
		if count != 1 {
			t.Errorf("expected 1 embedding row after UPDATE, got %d", count)
		}

		// DELETE the row: vector should be removed.
		deleteRow(t, connStr, table, 1)
		time.Sleep(500 * time.Millisecond)
		_ = conn2.QueryRow(context.Background(),
			fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE source_id = '1'`, safeEmb)).Scan(&count)
		if count != 0 {
			t.Errorf("expected 0 embedding rows after DELETE, got %d", count)
		}
	})

	t.Run("api retry: 500 then success", func(t *testing.T) {
		table := "embedding_retry_articles"
		embTable := "embedding_retry_vectors"

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		conn, err := pgx.Connect(ctx, connStr)
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		defer conn.Close(ctx)

		channel := createTrigger(t, connStr, table)

		_, err = conn.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS vector`)
		if err != nil {
			t.Fatalf("create vector extension: %v", err)
		}
		safeEmb := pgx.Identifier{embTable}.Sanitize()
		_, err = conn.Exec(ctx, fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				source_id    TEXT PRIMARY KEY,
				source_table TEXT NOT NULL,
				content      TEXT NOT NULL,
				embedding    vector(4),
				event_id     TEXT NOT NULL,
				created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
				updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
			)`, safeEmb))
		if err != nil {
			t.Fatalf("create embeddings table: %v", err)
		}
		conn.Close(ctx)

		// Mock API: fail twice with 500, then succeed.
		var callCount atomic.Int32
		apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n := callCount.Add(1)
			if n <= 2 {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data":  []map[string]any{{"embedding": []float64{0.1, 0.2, 0.3, 0.4}, "index": 0}},
				"usage": map[string]any{"total_tokens": 3},
			})
		}))
		defer apiServer.Close()

		adapter := embeddingadapter.New(
			apiServer.URL, "", "test-model",
			[]string{"data"}, "id",
			connStr, embTable, 4,
			5, 0, 100*time.Millisecond, 500*time.Millisecond,
			false,
			testLogger(),
		)
		startPipeline(t, connStr, []string{channel}, adapter)
		time.Sleep(200 * time.Millisecond)

		insertRow(t, connStr, table, map[string]any{"text": "retry test"})

		// Wait for embedding to appear (retry takes time due to backoff).
		waitForEmbedding(t, connStr, embTable, "1", 30*time.Second)

		if n := callCount.Load(); n < 3 {
			t.Errorf("expected at least 3 API calls (2 failures + 1 success), got %d", n)
		}
	})
}

// waitForEmbedding polls until a row with the given source_id appears in the embeddings table.
func waitForEmbedding(t *testing.T, connStr, table, sourceID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	safeTable := pgx.Identifier{table}.Sanitize()
	for time.Now().Before(deadline) {
		conn, err := pgx.Connect(context.Background(), connStr)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		var count int
		_ = conn.QueryRow(context.Background(),
			fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE source_id = $1`, safeTable), sourceID).Scan(&count)
		conn.Close(context.Background())
		if count > 0 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for embedding row source_id=%q in table %q", sourceID, table)
}
