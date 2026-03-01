//go:build integration && !no_duckdb

package scenarios

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/adapter/duckdb"
	"github.com/go-chi/chi/v5"
)

func TestScenario_DuckDB(t *testing.T) {
	connStr := startPostgres(t)

	t.Run("happy path", func(t *testing.T) {
		table := "duckdb_test_orders"
		channel := createTrigger(t, connStr, table)

		// In-memory DuckDB, fast flush interval.
		adapter := duckdb.New(":memory:", 0, 500*time.Millisecond, 0, testLogger())

		r := chi.NewRouter()
		adapter.MountHTTP(r)
		srv := httptest.NewServer(r)
		defer srv.Close()

		startPipeline(t, connStr, []string{channel}, adapter)

		// Wait for LISTEN/NOTIFY detector + insert row.
		time.Sleep(3 * time.Second)
		insertRow(t, connStr, table, map[string]any{"item": "widget"})

		// Poll query endpoint until event appears in DuckDB.
		var results []map[string]any
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			time.Sleep(500 * time.Millisecond)
			results = queryDuckDBEndpoint(t, srv.URL, "SELECT id, channel, operation, source FROM cdc_events")
			if len(results) > 0 {
				break
			}
			// Re-insert in case the detector wasn't ready for the first NOTIFY.
			insertRow(t, connStr, table, map[string]any{"item": "retry"})
		}

		if len(results) == 0 {
			t.Fatal("no events found in DuckDB after waiting")
		}

		// Verify first result.
		row := results[0]
		if row["channel"] != channel {
			t.Errorf("channel = %v, want %s", row["channel"], channel)
		}
		if row["operation"] != "INSERT" {
			t.Errorf("operation = %v, want INSERT", row["operation"])
		}
		if row["source"] != "listen_notify" {
			t.Errorf("source = %v, want listen_notify", row["source"])
		}
		if row["id"] == nil || row["id"] == "" {
			t.Error("event id is empty")
		}
	})

	t.Run("tables endpoint returns channel counts", func(t *testing.T) {
		table := "duckdb_tables_orders"
		channel := createTrigger(t, connStr, table)

		adapter := duckdb.New(":memory:", 0, 500*time.Millisecond, 0, testLogger())

		r := chi.NewRouter()
		adapter.MountHTTP(r)
		srv := httptest.NewServer(r)
		defer srv.Close()

		startPipeline(t, connStr, []string{channel}, adapter)

		time.Sleep(3 * time.Second)
		insertRow(t, connStr, table, map[string]any{"item": "alpha"})
		insertRow(t, connStr, table, map[string]any{"item": "beta"})

		// Poll until events appear.
		var tables []map[string]any
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			time.Sleep(500 * time.Millisecond)

			resp, err := http.Get(srv.URL + "/query/tables")
			if err != nil {
				continue
			}
			json.NewDecoder(resp.Body).Decode(&tables)
			resp.Body.Close()

			// Look for our channel with count >= 2.
			for _, tbl := range tables {
				ch, _ := tbl["channel"].(string)
				cnt, _ := tbl["count"].(float64)
				if ch == channel && cnt >= 2 {
					return // success
				}
			}

			insertRow(t, connStr, table, map[string]any{"item": "retry"})
		}

		t.Fatalf("expected channel %q with count >= 2 in tables endpoint, got %v", channel, tables)
	})
}

// queryDuckDBEndpoint performs a SQL query via the DuckDB HTTP endpoint.
func queryDuckDBEndpoint(t *testing.T, baseURL, sql string) []map[string]any {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"sql": sql})
	resp, err := http.Post(baseURL+"/query", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	var results []map[string]any
	json.NewDecoder(resp.Body).Decode(&results)
	return results
}
