//go:build !no_duckdb

package duckdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/go-chi/chi/v5"
)

// Adapter ingests CDC events into an in-process DuckDB database and exposes
// a SQL query HTTP endpoint. Events are buffered and flushed in batches.
// Old events are cleaned up based on the retention duration.
type Adapter struct {
	path          string
	retention     time.Duration
	flushInterval time.Duration
	flushSize     int
	queryToken    string
	logger        *slog.Logger

	db    atomic.Pointer[sql.DB]
	ready chan struct{} // closed by Start after the DB is initialised

	mu     sync.Mutex
	buffer []event.Event
}

// New creates a DuckDB analytics adapter.
func New(path string, retention, flushInterval time.Duration, flushSize int, queryToken string, logger *slog.Logger) *Adapter {
	if path == "" {
		path = ":memory:"
	}
	if retention <= 0 {
		retention = 1 * time.Hour
	}
	if flushInterval <= 0 {
		flushInterval = 5 * time.Second
	}
	if flushSize <= 0 {
		flushSize = 1000
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Adapter{
		path:          path,
		retention:     retention,
		flushInterval: flushInterval,
		flushSize:     flushSize,
		queryToken:    queryToken,
		logger:        logger.With("adapter", "duckdb"),
		ready:         make(chan struct{}),
	}
}

func (a *Adapter) Name() string { return "duckdb" }

// Start consumes events from the bus, buffering and flushing them into DuckDB.
// Blocks until ctx is cancelled.
func (a *Adapter) Start(ctx context.Context, events <-chan event.Event) error {
	a.logger.Info("duckdb adapter started",
		"path", a.path,
		"retention", a.retention,
		"flush_interval", a.flushInterval,
		"flush_size", a.flushSize,
	)

	db, err := sql.Open("duckdb", a.path)
	if err != nil {
		return fmt.Errorf("open duckdb: %w", err)
	}
	a.db.Store(db)

	if err := a.createTable(ctx); err != nil {
		_ = db.Close()
		a.db.Store(nil)
		return fmt.Errorf("create table: %w", err)
	}
	close(a.ready)

	flushTicker := time.NewTicker(a.flushInterval)
	defer flushTicker.Stop()

	cleanupInterval := a.retention / 10
	if cleanupInterval < 1*time.Second {
		cleanupInterval = 1 * time.Second
	}
	cleanupTicker := time.NewTicker(cleanupInterval)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			a.flushBuffer(ctx)
			return ctx.Err()
		case ev, ok := <-events:
			if !ok {
				a.flushBuffer(ctx)
				return nil
			}
			a.mu.Lock()
			a.buffer = append(a.buffer, ev)
			shouldFlush := len(a.buffer) >= a.flushSize
			a.mu.Unlock()
			if shouldFlush {
				a.flushBuffer(ctx)
			}
		case <-flushTicker.C:
			a.flushBuffer(ctx)
		case <-cleanupTicker.C:
			a.cleanup(ctx)
		}
	}
}

// MountHTTP registers the adapter's HTTP routes on the given router.
func (a *Adapter) MountHTTP(r chi.Router) {
	if a.queryToken == "" {
		r.Post("/query", func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "set --duckdb-query-token to enable the query endpoint", http.StatusForbidden)
		})
		r.Get("/query/tables", func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "set --duckdb-query-token to enable the query endpoint", http.StatusForbidden)
		})
		return
	}
	r.Post("/query", a.requireToken(a.QueryHandler))
	r.Get("/query/tables", a.requireToken(a.TablesHandler))
}

func (a *Adapter) requireToken(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get("Authorization")
		if token != "Bearer "+a.queryToken {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

// Drain flushes remaining buffered events on shutdown.
func (a *Adapter) Drain(ctx context.Context) error {
	a.flushBuffer(ctx)
	if db := a.db.Load(); db != nil {
		return db.Close()
	}
	return nil
}

// getDB returns the current database handle or nil if not initialized.
func (a *Adapter) getDB() *sql.DB {
	return a.db.Load()
}

func (a *Adapter) createTable(ctx context.Context) error {
	db := a.getDB()
	if db == nil {
		return fmt.Errorf("database not initialized")
	}
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS cdc_events (
		id VARCHAR PRIMARY KEY,
		channel VARCHAR NOT NULL,
		operation VARCHAR NOT NULL,
		payload JSON,
		source VARCHAR,
		created_at TIMESTAMP NOT NULL
	)`)
	return err
}

func (a *Adapter) flushBuffer(ctx context.Context) {
	a.mu.Lock()
	if len(a.buffer) == 0 {
		a.mu.Unlock()
		return
	}
	batch := a.buffer
	a.buffer = nil
	a.mu.Unlock()

	if err := a.ingest(ctx, batch); err != nil {
		a.logger.Error("flush failed", "error", err, "batch_size", len(batch))
		metrics.DuckDBFlushes.WithLabelValues("error").Inc()
		return
	}
	metrics.DuckDBFlushes.WithLabelValues("success").Inc()
	metrics.EventsDelivered.WithLabelValues("duckdb").Add(float64(len(batch)))
}

func (a *Adapter) ingest(ctx context.Context, events []event.Event) error {
	db := a.getDB()
	if db == nil {
		return fmt.Errorf("database not initialized")
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	stmt, err := tx.PrepareContext(ctx,
		"INSERT OR REPLACE INTO cdc_events (id, channel, operation, payload, source, created_at) VALUES (?, ?, ?, ?, ?, ?)")
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("prepare: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for _, ev := range events {
		ev.EnsurePayload()
		payloadStr := string(ev.Payload)
		if !json.Valid(ev.Payload) {
			payloadStr = "null"
		}
		if _, err := stmt.ExecContext(ctx, ev.ID, ev.Channel, ev.Operation, payloadStr, ev.Source, ev.CreatedAt); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("insert: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	metrics.DuckDBRows.Add(float64(len(events)))
	return nil
}

func (a *Adapter) cleanup(ctx context.Context) {
	db := a.getDB()
	if db == nil {
		return
	}
	cutoff := time.Now().UTC().Add(-a.retention)
	result, err := db.ExecContext(ctx, "DELETE FROM cdc_events WHERE created_at < ?", cutoff)
	if err != nil {
		a.logger.Warn("cleanup failed", "error", err)
		return
	}
	if n, _ := result.RowsAffected(); n > 0 {
		metrics.DuckDBRows.Sub(float64(n))
		a.logger.Debug("cleaned up expired events", "deleted", n)
	}
}
