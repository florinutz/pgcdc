package snapshot

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/jackc/pgx/v5"
)

var validSnapshotName = regexp.MustCompile(`^[0-9A-Fa-f/:\-]+$`)

const source = "snapshot"

// Snapshot exports existing rows from a table as SNAPSHOT events.
type Snapshot struct {
	dbURL        string
	table        string
	where        string
	batchSize    int
	snapshotName string // if set, use SET TRANSACTION SNAPSHOT for consistent reads
	logger       *slog.Logger
}

// New creates a Snapshot. batchSize controls how many rows are buffered
// before flushing to the events channel (defaults to 1000 if <= 0).
func New(dbURL, table, where string, batchSize int, logger *slog.Logger) *Snapshot {
	if batchSize <= 0 {
		batchSize = 1000
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Snapshot{
		dbURL:     dbURL,
		table:     table,
		where:     where,
		batchSize: batchSize,
		logger:    logger.With("component", "snapshot"),
	}
}

// SetSnapshotName sets an exported snapshot name (from CREATE_REPLICATION_SLOT)
// to use with SET TRANSACTION SNAPSHOT for zero-gap snapshot-first workflows.
// Returns an error if the name contains characters outside the expected
// PostgreSQL snapshot name format (hex digits, hyphens, forward slashes, colons).
func (s *Snapshot) SetSnapshotName(name string) error {
	if !validSnapshotName.MatchString(name) {
		return fmt.Errorf("invalid snapshot name: %q", name)
	}
	s.snapshotName = name
	return nil
}

// Run exports all rows from the table as SNAPSHOT events to the events channel.
// It uses a REPEATABLE READ transaction to get a consistent view of the table.
// The caller owns the events channel; Run does NOT close it.
func (s *Snapshot) Run(ctx context.Context, events chan<- event.Event) error {
	start := time.Now()

	conn, err := pgx.Connect(ctx, s.dbURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	// Start a REPEATABLE READ transaction for a consistent snapshot.
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead,
	})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// If a snapshot name is provided (from a replication slot for snapshot-first),
	// import it to see exactly the same data state as the slot's consistent point.
	// SET TRANSACTION SNAPSHOT requires a string literal, not a parameter.
	// The name comes from PostgreSQL's CREATE_REPLICATION_SLOT, not user input.
	if s.snapshotName != "" {
		if _, err := tx.Exec(ctx, "SET TRANSACTION SNAPSHOT '"+s.snapshotName+"'"); err != nil {
			return fmt.Errorf("set transaction snapshot: %w", err)
		}
	}

	channel := "pgcdc:" + s.table

	// Build the query.
	safeTable := pgx.Identifier{s.table}.Sanitize()
	query := "SELECT * FROM " + safeTable
	if s.where != "" {
		query += " WHERE " + s.where
	}

	s.logger.Info("starting snapshot", "table", s.table, "where", s.where)

	rows, err := tx.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	fieldDescs := rows.FieldDescriptions()
	var count int64

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return fmt.Errorf("read row: %w", err)
		}

		row := make(map[string]any, len(fieldDescs))
		for i, fd := range fieldDescs {
			row[fd.Name] = values[i]
		}

		rec := &event.Record{
			Operation: event.OperationSnapshot,
			Metadata:  event.Metadata{event.MetaTable: s.table},
			Change: event.Change{
				After: event.NewStructuredDataFromMap(row),
			},
		}

		ev, err := event.NewFromRecord(channel, rec, source)
		if err != nil {
			s.logger.Error("create event failed", "error", err)
			continue
		}

		select {
		case events <- ev:
			count++
			metrics.SnapshotRowsExported.Inc()
		case <-ctx.Done():
			return ctx.Err()
		}

		if count%int64(s.batchSize) == 0 {
			s.logger.Info("snapshot progress", "rows", count, "elapsed", time.Since(start))
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows iteration: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	elapsed := time.Since(start)
	metrics.SnapshotDuration.Observe(elapsed.Seconds())
	s.logger.Info("snapshot complete", "table", s.table, "rows", count, "elapsed", elapsed)

	return nil
}
