package dlq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// StoredRecord represents a single DLQ record read from the database.
type StoredRecord struct {
	ID         string          `json:"id"`
	EventID    string          `json:"event_id"`
	Adapter    string          `json:"adapter"`
	Error      string          `json:"error"`
	Payload    json.RawMessage `json:"payload"`
	CreatedAt  time.Time       `json:"created_at"`
	ReplayedAt *time.Time      `json:"replayed_at,omitempty"`
}

// ListFilter controls which DLQ records are returned by List/Count/Purge.
type ListFilter struct {
	Adapter      string
	Since        *time.Time
	Until        *time.Time
	ID           string
	Limit        int
	Pending      bool // only records with replayed_at IS NULL
	ReplayedOnly bool // only records with replayed_at IS NOT NULL
}

// Store provides read-side operations on the DLQ table.
type Store struct {
	dbURL  string
	table  string
	logger *slog.Logger
}

// NewStore creates a DLQ store for CLI read operations.
func NewStore(dbURL, table string, logger *slog.Logger) *Store {
	if table == "" {
		table = defaultTable
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Store{dbURL: dbURL, table: table, logger: logger}
}

func (s *Store) connect(ctx context.Context) (*pgx.Conn, error) {
	conn, err := pgx.Connect(ctx, s.dbURL)
	if err != nil {
		return nil, fmt.Errorf("dlq store connect: %w", err)
	}
	return conn, nil
}

// EnsureSchema is a no-op. The pgcdc_dead_letters table and its replayed_at
// column are managed by the migration system (internal/migrate/sql/003_dlq_replayed_at.sql).
// This method is retained for backwards compatibility with CLI callers.
func (s *Store) EnsureSchema(_ context.Context) error {
	return nil
}

// buildWhere constructs a WHERE clause and parameter list from a ListFilter.
func (s *Store) buildWhere(f ListFilter) (string, []any) {
	var conds []string
	var params []any
	idx := 1

	if f.Adapter != "" {
		conds = append(conds, fmt.Sprintf("adapter = $%d", idx))
		params = append(params, f.Adapter)
		idx++
	}
	if f.Since != nil {
		conds = append(conds, fmt.Sprintf("created_at >= $%d", idx))
		params = append(params, *f.Since)
		idx++
	}
	if f.Until != nil {
		conds = append(conds, fmt.Sprintf("created_at <= $%d", idx))
		params = append(params, *f.Until)
		idx++
	}
	if f.ID != "" {
		conds = append(conds, fmt.Sprintf("id::text = $%d", idx))
		params = append(params, f.ID)
		// idx++ omitted: last parameterized condition
	}
	if f.Pending {
		conds = append(conds, "replayed_at IS NULL")
	}
	if f.ReplayedOnly {
		conds = append(conds, "replayed_at IS NOT NULL")
	}

	where := ""
	if len(conds) > 0 {
		where = " WHERE " + strings.Join(conds, " AND ")
	}
	return where, params
}

// List returns DLQ records matching the filter.
func (s *Store) List(ctx context.Context, f ListFilter) ([]StoredRecord, error) {
	conn, err := s.connect(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(ctx) }()

	safeTable := pgx.Identifier{s.table}.Sanitize()
	where, params := s.buildWhere(f)

	query := fmt.Sprintf(
		`SELECT id, event_id, adapter, error, payload, created_at, replayed_at FROM %s%s ORDER BY created_at DESC`,
		safeTable, where,
	)

	if f.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", f.Limit)
	}

	rows, err := conn.Query(ctx, query, params...)
	if err != nil {
		return nil, fmt.Errorf("list dlq records: %w", err)
	}
	defer rows.Close()

	var records []StoredRecord
	for rows.Next() {
		var r StoredRecord
		if err := rows.Scan(&r.ID, &r.EventID, &r.Adapter, &r.Error, &r.Payload, &r.CreatedAt, &r.ReplayedAt); err != nil {
			return nil, fmt.Errorf("scan dlq record: %w", err)
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

// Count returns the number of DLQ records matching the filter.
func (s *Store) Count(ctx context.Context, f ListFilter) (int64, error) {
	conn, err := s.connect(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close(ctx) }()

	safeTable := pgx.Identifier{s.table}.Sanitize()
	where, params := s.buildWhere(f)

	var count int64
	query := fmt.Sprintf(`SELECT COUNT(*) FROM %s%s`, safeTable, where)
	if err := conn.QueryRow(ctx, query, params...).Scan(&count); err != nil {
		return 0, fmt.Errorf("count dlq records: %w", err)
	}
	return count, nil
}

// MarkReplayed sets replayed_at = NOW() for the given record IDs.
func (s *Store) MarkReplayed(ctx context.Context, ids []string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}

	conn, err := s.connect(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close(ctx) }()

	safeTable := pgx.Identifier{s.table}.Sanitize()
	query := fmt.Sprintf(`UPDATE %s SET replayed_at = NOW() WHERE id::text = ANY($1)`, safeTable)
	tag, err := conn.Exec(ctx, query, ids)
	if err != nil {
		return 0, fmt.Errorf("mark replayed: %w", err)
	}
	return tag.RowsAffected(), nil
}

// Purge deletes DLQ records matching the filter and returns rows affected.
func (s *Store) Purge(ctx context.Context, f ListFilter) (int64, error) {
	conn, err := s.connect(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close(ctx) }()

	safeTable := pgx.Identifier{s.table}.Sanitize()
	where, params := s.buildWhere(f)

	query := fmt.Sprintf(`DELETE FROM %s%s`, safeTable, where)
	tag, err := conn.Exec(ctx, query, params...)
	if err != nil {
		return 0, fmt.Errorf("purge dlq records: %w", err)
	}
	return tag.RowsAffected(), nil
}
