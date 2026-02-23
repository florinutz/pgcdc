package pgtable

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/florinutz/pgpipe/event"
	"github.com/florinutz/pgpipe/internal/backoff"
	"github.com/florinutz/pgpipe/metrics"
	"github.com/jackc/pgx/v5"
)

const (
	defaultTable       = "pgpipe_events"
	defaultBackoffBase = 1 * time.Second
	defaultBackoffCap  = 30 * time.Second
)

// Adapter inserts events into a PostgreSQL table.
type Adapter struct {
	dbURL       string
	table       string
	backoffBase time.Duration
	backoffCap  time.Duration
	logger      *slog.Logger
}

// New creates a pg_table adapter. The table must already exist.
// Duration parameters default to sensible values when zero.
func New(dbURL, table string, backoffBase, backoffCap time.Duration, logger *slog.Logger) *Adapter {
	if table == "" {
		table = defaultTable
	}
	if backoffBase <= 0 {
		backoffBase = defaultBackoffBase
	}
	if backoffCap <= 0 {
		backoffCap = defaultBackoffCap
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Adapter{
		dbURL:       dbURL,
		table:       table,
		backoffBase: backoffBase,
		backoffCap:  backoffCap,
		logger:      logger.With("adapter", "pg_table"),
	}
}

// Name returns the adapter name.
func (a *Adapter) Name() string {
	return "pg_table"
}

// Start blocks, consuming events and inserting them into the PostgreSQL table.
// It reconnects with backoff on connection errors.
func (a *Adapter) Start(ctx context.Context, events <-chan event.Event) error {
	a.logger.Info("pg_table adapter started", "table", a.table)

	var attempt int
	for {
		runErr := a.run(ctx, events)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Channel closed means clean shutdown.
		if runErr == nil {
			return nil
		}

		delay := backoff.Jitter(attempt, a.backoffBase, a.backoffCap)
		a.logger.Error("connection lost, reconnecting",
			"error", runErr,
			"attempt", attempt+1,
			"delay", delay,
		)
		attempt++

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

func (a *Adapter) run(ctx context.Context, events <-chan event.Event) error {
	conn, err := pgx.Connect(ctx, a.dbURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	safeTable := pgx.Identifier{a.table}.Sanitize()
	insertSQL := fmt.Sprintf(
		"INSERT INTO %s (id, channel, operation, payload, source, created_at) VALUES ($1, $2, $3, $4, $5, $6)",
		safeTable,
	)

	a.logger.Info("connected to database", "table", a.table)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case ev, ok := <-events:
			if !ok {
				return nil
			}

			payloadStr := string(ev.Payload)
			if !json.Valid(ev.Payload) {
				payloadStr = `"` + payloadStr + `"`
			}

			_, err := conn.Exec(ctx, insertSQL,
				ev.ID,
				ev.Channel,
				ev.Operation,
				payloadStr,
				ev.Source,
				ev.CreatedAt,
			)
			if err != nil {
				// Check if this is a connection-level error (worth reconnecting for).
				if conn.IsClosed() {
					return fmt.Errorf("insert (connection lost): %w", err)
				}
				// Non-connection error (e.g., constraint violation): log and skip.
				a.logger.Warn("insert failed, skipping event",
					"event_id", ev.ID,
					"error", err,
				)
				continue
			}
			metrics.EventsDelivered.WithLabelValues("pg_table").Inc()
		}
	}
}
