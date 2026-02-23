//go:build integration

package scenarios

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	icebergadapter "github.com/florinutz/pgcdc/adapter/iceberg"
	"github.com/parquet-go/parquet-go"
)

func TestScenario_Iceberg(t *testing.T) {
	connStr := startPostgres(t)
	channel := createTrigger(t, connStr, "iceberg_events")

	t.Run("happy path append raw", func(t *testing.T) {
		warehouse := t.TempDir()

		a := icebergadapter.New(
			"hadoop",      // catalog type
			"",            // catalog URI (not needed for hadoop)
			warehouse,     // warehouse path
			"pgcdc",       // namespace
			"orders_cdc",  // table
			"append",      // mode
			"raw",         // schema mode
			nil,           // primary keys (not needed for append)
			1*time.Second, // flush interval (fast for test)
			100,           // flush size
			0, 0,          // backoff defaults
			testLogger(),
		)

		startPipeline(t, connStr, []string{channel}, a)

		// Allow pipeline to settle.
		time.Sleep(500 * time.Millisecond)

		// Insert 3 rows.
		for i := range 3 {
			insertRow(t, connStr, "iceberg_events", map[string]any{"item": i + 1})
		}

		// Wait for flush (interval=1s + some margin).
		time.Sleep(3 * time.Second)

		// Verify Parquet data files exist.
		dataDir := filepath.Join(warehouse, "pgcdc", "orders_cdc", "data")
		entries, err := os.ReadDir(dataDir)
		if err != nil {
			t.Fatalf("read data dir: %v", err)
		}
		if len(entries) == 0 {
			t.Fatal("expected at least one parquet data file")
		}

		// Read the Parquet file and verify row count.
		var totalRows int64
		for _, entry := range entries {
			if filepath.Ext(entry.Name()) != ".parquet" {
				continue
			}
			pqPath := filepath.Join(dataDir, entry.Name())
			f, err := os.Open(pqPath)
			if err != nil {
				t.Fatalf("open parquet: %v", err)
			}
			fi, err := f.Stat()
			if err != nil {
				f.Close()
				t.Fatalf("stat parquet: %v", err)
			}
			pf, err := parquet.OpenFile(f, fi.Size())
			if err != nil {
				f.Close()
				t.Fatalf("open parquet file: %v", err)
			}
			totalRows += pf.NumRows()
			f.Close()
		}

		if totalRows != 3 {
			t.Fatalf("expected 3 rows in parquet, got %d", totalRows)
		}

		// Verify metadata.json exists and is valid Iceberg v2.
		// v1.metadata.json is the initial table creation (no snapshots).
		// v2.metadata.json is written after the first flush (with snapshot).
		metaDir := filepath.Join(warehouse, "pgcdc", "orders_cdc", "metadata")

		// Find the highest version metadata file.
		var latestMeta string
		for v := 100; v >= 1; v-- {
			candidate := filepath.Join(metaDir, "v"+strconv.Itoa(v)+".metadata.json")
			if _, err := os.Stat(candidate); err == nil {
				latestMeta = candidate
				break
			}
		}
		if latestMeta == "" {
			t.Fatal("no versioned metadata.json found")
		}

		data, err := os.ReadFile(latestMeta)
		if err != nil {
			t.Fatalf("read metadata: %v", err)
		}
		var meta map[string]any
		if err := json.Unmarshal(data, &meta); err != nil {
			t.Fatalf("parse metadata: %v", err)
		}
		if fv, ok := meta["format-version"]; !ok {
			t.Fatal("missing format-version in metadata")
		} else if fv != float64(2) {
			t.Fatalf("expected format-version 2, got %v", fv)
		}
		snapshots, ok := meta["snapshots"].([]any)
		if !ok || len(snapshots) == 0 {
			t.Fatalf("expected at least one snapshot in metadata at %s", latestMeta)
		}

		// Verify version-hint.text exists.
		hintPath := filepath.Join(metaDir, "version-hint.text")
		if _, err := os.Stat(hintPath); err != nil {
			t.Fatalf("version-hint.text not found: %v", err)
		}
	})

	t.Run("flush failure and retry", func(t *testing.T) {
		warehouse := t.TempDir()

		a := icebergadapter.New(
			"hadoop",
			"",
			warehouse,
			"pgcdc",
			"retry_test",
			"append",
			"raw",
			nil,
			500*time.Millisecond, // very fast flush
			100,
			0, 0,
			testLogger(),
		)

		startPipeline(t, connStr, []string{channel}, a)
		time.Sleep(500 * time.Millisecond)

		// Insert a row to trigger initial table creation.
		insertRow(t, connStr, "iceberg_events", map[string]any{"item": "first"})
		time.Sleep(2 * time.Second)

		// Make the data directory read-only to force write failures.
		dataDir := filepath.Join(warehouse, "pgcdc", "retry_test", "data")
		if err := os.Chmod(dataDir, 0o444); err != nil {
			t.Fatalf("chmod data dir: %v", err)
		}

		// Insert more rows — these should fail to flush.
		insertRow(t, connStr, "iceberg_events", map[string]any{"item": "blocked"})
		time.Sleep(2 * time.Second)

		// Restore write permissions — next flush should succeed.
		if err := os.Chmod(dataDir, 0o755); err != nil {
			t.Fatalf("restore chmod: %v", err)
		}

		time.Sleep(2 * time.Second)

		// Verify the blocked event was eventually written.
		entries, err := os.ReadDir(dataDir)
		if err != nil {
			t.Fatalf("read data dir: %v", err)
		}

		var totalRows int64
		for _, entry := range entries {
			if filepath.Ext(entry.Name()) != ".parquet" {
				continue
			}
			pqPath := filepath.Join(dataDir, entry.Name())
			f, err := os.Open(pqPath)
			if err != nil {
				t.Fatalf("open parquet: %v", err)
			}
			fi, err := f.Stat()
			if err != nil {
				f.Close()
				t.Fatalf("stat parquet: %v", err)
			}
			pf, err := parquet.OpenFile(f, fi.Size())
			if err != nil {
				f.Close()
				t.Fatalf("open parquet file: %v", err)
			}
			totalRows += pf.NumRows()
			f.Close()
		}

		if totalRows < 2 {
			t.Fatalf("expected at least 2 rows (initial + retried), got %d", totalRows)
		}
	})
}
