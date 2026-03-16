//go:build !no_duckdb

package duckdb

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/florinutz/pgcdc/metrics"
)

const defaultQueryTimeout = 30 * time.Second

type queryRequest struct {
	SQL     string   `json:"sql"`
	Params  []string `json:"params"`
	Timeout string   `json:"timeout"`
}

type tableInfo struct {
	Channel string `json:"channel"`
	Count   int64  `json:"count"`
}

// QueryHandler handles POST /query — executes a SQL query against the DuckDB
// database and returns results as a JSON array.
func (a *Adapter) QueryHandler(w http.ResponseWriter, r *http.Request) {
	db := a.getDB()
	if db == nil {
		http.Error(w, "database not initialized", http.StatusServiceUnavailable)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 10<<20) // 10 MB

	var req queryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		return
	}
	if req.SQL == "" {
		http.Error(w, "sql field is required", http.StatusBadRequest)
		return
	}

	timeout := defaultQueryTimeout
	if req.Timeout != "" {
		parsed, err := time.ParseDuration(req.Timeout)
		if err != nil {
			http.Error(w, fmt.Sprintf("invalid timeout: %v", err), http.StatusBadRequest)
			return
		}
		timeout = parsed
	}

	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	start := time.Now()

	// Convert params to []any for db.QueryContext.
	args := make([]any, len(req.Params))
	for i, p := range req.Params {
		args[i] = p
	}

	rows, err := db.QueryContext(ctx, req.SQL, args...)
	if err != nil {
		metrics.DuckDBQueries.WithLabelValues("error").Inc()
		metrics.DuckDBQueryDuration.Observe(time.Since(start).Seconds())
		http.Error(w, fmt.Sprintf("query error: %v", err), http.StatusBadRequest)
		return
	}
	defer func() { _ = rows.Close() }()

	columns, err := rows.Columns()
	if err != nil {
		metrics.DuckDBQueries.WithLabelValues("error").Inc()
		http.Error(w, fmt.Sprintf("columns error: %v", err), http.StatusInternalServerError)
		return
	}

	var results []map[string]any
	for rows.Next() {
		values := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			metrics.DuckDBQueries.WithLabelValues("error").Inc()
			http.Error(w, fmt.Sprintf("scan error: %v", err), http.StatusInternalServerError)
			return
		}
		row := make(map[string]any, len(columns))
		for i, col := range columns {
			row[col] = values[i]
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		metrics.DuckDBQueries.WithLabelValues("error").Inc()
		http.Error(w, fmt.Sprintf("rows error: %v", err), http.StatusInternalServerError)
		return
	}

	metrics.DuckDBQueries.WithLabelValues("success").Inc()
	metrics.DuckDBQueryDuration.Observe(time.Since(start).Seconds())

	w.Header().Set("Content-Type", "application/json")
	if results == nil {
		results = []map[string]any{}
	}
	_ = json.NewEncoder(w).Encode(results)
}

// TablesHandler handles GET /query/tables — returns channels with row counts.
func (a *Adapter) TablesHandler(w http.ResponseWriter, r *http.Request) {
	db := a.getDB()
	if db == nil {
		http.Error(w, "database not initialized", http.StatusServiceUnavailable)
		return
	}

	rows, err := db.QueryContext(r.Context(),
		"SELECT channel, COUNT(*) as count FROM cdc_events GROUP BY channel ORDER BY channel")
	if err != nil {
		http.Error(w, fmt.Sprintf("query error: %v", err), http.StatusInternalServerError)
		return
	}
	defer func() { _ = rows.Close() }()

	var tables []tableInfo
	for rows.Next() {
		var t tableInfo
		if err := rows.Scan(&t.Channel, &t.Count); err != nil {
			http.Error(w, fmt.Sprintf("scan error: %v", err), http.StatusInternalServerError)
			return
		}
		tables = append(tables, t)
	}

	w.Header().Set("Content-Type", "application/json")
	if tables == nil {
		tables = []tableInfo{}
	}
	_ = json.NewEncoder(w).Encode(tables)
}
