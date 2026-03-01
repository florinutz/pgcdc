package checkpoint

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

// Store persists replication slot LSN positions so pgcdc can resume from the
// last confirmed position after a crash or restart.
type Store interface {
	// Load returns the last persisted LSN for the given slot. Returns 0 if no
	// checkpoint exists.
	Load(ctx context.Context, slotName string) (lsn uint64, err error)

	// Save persists the current LSN for the given slot.
	Save(ctx context.Context, slotName string, lsn uint64) error

	// Close releases the underlying connection.
	Close() error
}

// PGStore implements Store using a PostgreSQL table.
// The pgcdc_checkpoints table is managed by the migration system
// (internal/migrate/sql/001_initial.sql). Callers must run migrations
// before using this store.
type PGStore struct {
	conn          *pgx.Conn
	logger        *slog.Logger
	lastSavedLSN  uint64
	lastSavedTime time.Time
}

// NewPGStore connects to PostgreSQL and returns a checkpoint store.
// The pgcdc_checkpoints table must already exist (created by the migration system).
func NewPGStore(ctx context.Context, connStr string, logger *slog.Logger) (*PGStore, error) {
	if logger == nil {
		logger = slog.Default()
	}

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("checkpoint connect: %w", err)
	}

	return &PGStore{
		conn:   conn,
		logger: logger.With("component", "checkpoint"),
	}, nil
}

// Load returns the last persisted LSN for the given slot. Returns 0 if no
// checkpoint exists.
func (s *PGStore) Load(ctx context.Context, slotName string) (uint64, error) {
	var lsn int64
	err := s.conn.QueryRow(ctx,
		"SELECT lsn FROM pgcdc_checkpoints WHERE slot_name = $1",
		slotName,
	).Scan(&lsn)

	if err == pgx.ErrNoRows {
		s.logger.Info("no checkpoint found, starting from scratch", "slot", slotName)
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("load checkpoint: %w", err)
	}

	s.logger.Info("checkpoint loaded", "slot", slotName, "lsn", lsn)
	return uint64(lsn), nil
}

// Save persists the current LSN for the given slot using an upsert.
// Skips the write if the LSN is unchanged since the last successful save.
func (s *PGStore) Save(ctx context.Context, slotName string, lsn uint64) error {
	if lsn == s.lastSavedLSN {
		return nil
	}

	_, err := s.conn.Exec(ctx, `
		INSERT INTO pgcdc_checkpoints (slot_name, lsn, updated_at)
		VALUES ($1, $2, now())
		ON CONFLICT (slot_name)
		DO UPDATE SET lsn = EXCLUDED.lsn, updated_at = EXCLUDED.updated_at
	`, slotName, int64(lsn))
	if err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}

	s.lastSavedLSN = lsn
	s.lastSavedTime = time.Now()
	return nil
}

// LastSavedLSN returns the most recently saved LSN and when it was saved.
func (s *PGStore) LastSavedLSN() (uint64, time.Time) {
	return s.lastSavedLSN, s.lastSavedTime
}

// Close releases the underlying connection.
func (s *PGStore) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.conn.Close(ctx)
}
