package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// SnapshotStatus represents the state of an incremental snapshot.
type SnapshotStatus string

const (
	StatusPending   SnapshotStatus = "pending"
	StatusRunning   SnapshotStatus = "running"
	StatusPaused    SnapshotStatus = "paused"
	StatusCompleted SnapshotStatus = "completed"
	StatusAborted   SnapshotStatus = "aborted"
)

// ProgressRecord tracks the state of an incremental snapshot.
type ProgressRecord struct {
	SnapshotID    string          `json:"snapshot_id"`
	TableName     string          `json:"table_name"`
	LastPK        json.RawMessage `json:"last_pk"`
	RowsProcessed int64           `json:"rows_processed"`
	Status        SnapshotStatus  `json:"status"`
	UpdatedAt     time.Time       `json:"updated_at"`
}

// ProgressStore persists incremental snapshot progress for crash recovery.
type ProgressStore interface {
	Load(ctx context.Context, snapshotID string) (*ProgressRecord, error)
	Save(ctx context.Context, rec ProgressRecord) error
	List(ctx context.Context, status *SnapshotStatus) ([]ProgressRecord, error)
	Delete(ctx context.Context, snapshotID string) error
	Close() error
}

// PGProgressStore implements ProgressStore using a PostgreSQL table.
// All methods are goroutine-safe — a mutex serializes access to the underlying
// pgx.Conn (which is not safe for concurrent use).
type PGProgressStore struct {
	mu           sync.Mutex
	conn         *pgx.Conn
	tableCreated bool
	logger       *slog.Logger
}

// NewPGProgressStore connects to PostgreSQL and returns a progress store.
// The pgcdc_snapshot_progress table is auto-created on the first operation.
func NewPGProgressStore(ctx context.Context, connStr string, logger *slog.Logger) (*PGProgressStore, error) {
	if logger == nil {
		logger = slog.Default()
	}

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("progress store connect: %w", err)
	}

	return &PGProgressStore{
		conn:   conn,
		logger: logger.With("component", "snapshot_progress"),
	}, nil
}

func (s *PGProgressStore) ensureTable(ctx context.Context) error {
	if s.tableCreated {
		return nil
	}

	_, err := s.conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS pgcdc_snapshot_progress (
			snapshot_id    TEXT PRIMARY KEY,
			table_name     TEXT NOT NULL,
			last_pk        JSONB,
			rows_processed BIGINT NOT NULL DEFAULT 0,
			status         TEXT NOT NULL DEFAULT 'pending',
			updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`)
	if err != nil {
		return fmt.Errorf("create progress table: %w", err)
	}

	s.tableCreated = true
	return nil
}

// Load returns the progress record for the given snapshot ID, or nil if not found.
func (s *PGProgressStore) Load(ctx context.Context, snapshotID string) (*ProgressRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureTable(ctx); err != nil {
		return nil, err
	}

	var rec ProgressRecord
	err := s.conn.QueryRow(ctx,
		"SELECT snapshot_id, table_name, last_pk, rows_processed, status, updated_at FROM pgcdc_snapshot_progress WHERE snapshot_id = $1",
		snapshotID,
	).Scan(&rec.SnapshotID, &rec.TableName, &rec.LastPK, &rec.RowsProcessed, &rec.Status, &rec.UpdatedAt)

	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load progress: %w", err)
	}

	return &rec, nil
}

// Save persists or updates a progress record using an upsert.
func (s *PGProgressStore) Save(ctx context.Context, rec ProgressRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureTable(ctx); err != nil {
		return err
	}

	_, err := s.conn.Exec(ctx, `
		INSERT INTO pgcdc_snapshot_progress (snapshot_id, table_name, last_pk, rows_processed, status, updated_at)
		VALUES ($1, $2, $3, $4, $5, now())
		ON CONFLICT (snapshot_id)
		DO UPDATE SET last_pk = EXCLUDED.last_pk, rows_processed = EXCLUDED.rows_processed, status = EXCLUDED.status, updated_at = EXCLUDED.updated_at
	`, rec.SnapshotID, rec.TableName, rec.LastPK, rec.RowsProcessed, string(rec.Status))
	if err != nil {
		return fmt.Errorf("save progress: %w", err)
	}

	return nil
}

// List returns progress records, optionally filtered by status.
func (s *PGProgressStore) List(ctx context.Context, status *SnapshotStatus) ([]ProgressRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureTable(ctx); err != nil {
		return nil, err
	}

	var rows pgx.Rows
	var err error

	if status != nil {
		rows, err = s.conn.Query(ctx,
			"SELECT snapshot_id, table_name, last_pk, rows_processed, status, updated_at FROM pgcdc_snapshot_progress WHERE status = $1 ORDER BY updated_at",
			string(*status),
		)
	} else {
		rows, err = s.conn.Query(ctx,
			"SELECT snapshot_id, table_name, last_pk, rows_processed, status, updated_at FROM pgcdc_snapshot_progress ORDER BY updated_at",
		)
	}
	if err != nil {
		return nil, fmt.Errorf("list progress: %w", err)
	}
	defer rows.Close()

	var records []ProgressRecord
	for rows.Next() {
		var rec ProgressRecord
		if err := rows.Scan(&rec.SnapshotID, &rec.TableName, &rec.LastPK, &rec.RowsProcessed, &rec.Status, &rec.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan progress: %w", err)
		}
		records = append(records, rec)
	}

	return records, rows.Err()
}

// Delete removes a progress record.
func (s *PGProgressStore) Delete(ctx context.Context, snapshotID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureTable(ctx); err != nil {
		return err
	}

	_, err := s.conn.Exec(ctx, "DELETE FROM pgcdc_snapshot_progress WHERE snapshot_id = $1", snapshotID)
	if err != nil {
		return fmt.Errorf("delete progress: %w", err)
	}

	return nil
}

// Close releases the underlying connection.
func (s *PGProgressStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.conn.Close(ctx)
}
