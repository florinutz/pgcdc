package dlq

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/jackc/pgx/v5"
)

// Record stores a failed event for later inspection/replay.
type Record struct {
	Event     event.Event `json:"event"`
	Adapter   string      `json:"adapter"`
	Error     string      `json:"error"`
	Timestamp time.Time   `json:"timestamp"`
}

// DLQ receives events that failed delivery after retry exhaustion.
type DLQ interface {
	Record(ctx context.Context, ev event.Event, adapter string, err error) error
	Close() error
}

// ── StderrDLQ ───────────────────────────────────────────────────────────────

// StderrDLQ writes failed events as JSON lines to stderr.
type StderrDLQ struct {
	mu     sync.Mutex
	w      io.Writer
	enc    *json.Encoder
	logger *slog.Logger
}

// NewStderrDLQ creates a DLQ that writes JSON lines to stderr.
func NewStderrDLQ(logger *slog.Logger) *StderrDLQ {
	if logger == nil {
		logger = slog.Default()
	}
	w := os.Stderr
	return &StderrDLQ{
		w:      w,
		enc:    json.NewEncoder(w),
		logger: logger,
	}
}

func (d *StderrDLQ) Record(ctx context.Context, ev event.Event, adapter string, err error) error {
	rec := Record{
		Event:     ev,
		Adapter:   adapter,
		Error:     err.Error(),
		Timestamp: time.Now().UTC(),
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if encErr := d.enc.Encode(rec); encErr != nil {
		metrics.DLQErrors.Inc()
		return fmt.Errorf("encode dlq record: %w", encErr)
	}
	metrics.DLQRecords.WithLabelValues(adapter).Inc()
	return nil
}

func (d *StderrDLQ) Close() error { return nil }

// ── PGTableDLQ ──────────────────────────────────────────────────────────────

const defaultTable = "pgcdc_dead_letters"

// PGTableDLQ writes failed events to a PostgreSQL table.
// The pgcdc_dead_letters table is managed by the migration system
// (internal/migrate/sql/001_initial.sql and 003_dlq_replayed_at.sql).
// Callers must run migrations before using this type.
type PGTableDLQ struct {
	dbURL     string
	table     string
	conn      *pgx.Conn
	logger    *slog.Logger
	mu        sync.Mutex
	insertSQL string // cached INSERT statement
}

// NewPGTableDLQ creates a DLQ backed by a PostgreSQL table.
// The pgcdc_dead_letters table must already exist (created by the migration system).
func NewPGTableDLQ(dbURL, table string, logger *slog.Logger) *PGTableDLQ {
	if table == "" {
		table = defaultTable
	}
	if logger == nil {
		logger = slog.Default()
	}
	safeTable := pgx.Identifier{table}.Sanitize()
	return &PGTableDLQ{
		dbURL:     dbURL,
		table:     table,
		logger:    logger,
		insertSQL: fmt.Sprintf(`INSERT INTO %s (event_id, adapter, error, payload) VALUES ($1, $2, $3, $4)`, safeTable),
	}
}

func (d *PGTableDLQ) ensureConn(ctx context.Context) error {
	if d.conn != nil && !d.conn.IsClosed() {
		return nil
	}
	conn, err := pgx.Connect(ctx, d.dbURL)
	if err != nil {
		return fmt.Errorf("dlq connect: %w", err)
	}
	d.conn = conn
	return nil
}

func (d *PGTableDLQ) Record(ctx context.Context, ev event.Event, adapter string, err error) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if connErr := d.ensureConn(ctx); connErr != nil {
		metrics.DLQErrors.Inc()
		return connErr
	}

	payload, marshalErr := json.Marshal(ev)
	if marshalErr != nil {
		metrics.DLQErrors.Inc()
		return fmt.Errorf("marshal event: %w", marshalErr)
	}

	if _, execErr := d.conn.Exec(ctx, d.insertSQL, ev.ID, adapter, err.Error(), payload); execErr != nil {
		metrics.DLQErrors.Inc()
		return fmt.Errorf("insert dlq record: %w", execErr)
	}

	metrics.DLQRecords.WithLabelValues(adapter).Inc()
	d.logger.Warn("event recorded to dead letter queue",
		"event_id", ev.ID,
		"adapter", adapter,
		"error", err.Error(),
	)
	return nil
}

func (d *PGTableDLQ) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.conn != nil && !d.conn.IsClosed() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return d.conn.Close(ctx)
	}
	return nil
}

// ── NopDLQ ──────────────────────────────────────────────────────────────────

// NopDLQ discards all failed events (--dlq none).
type NopDLQ struct{}

func (NopDLQ) Record(context.Context, event.Event, string, error) error { return nil }
func (NopDLQ) Close() error                                             { return nil }
