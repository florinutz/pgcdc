package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// PGStore is a PostgreSQL-backed schema store. It uses the pgcdc_schemas table
// to persist schema versions. An in-memory LRU cache avoids repeated DB lookups.
type PGStore struct {
	connString string
	logger     *slog.Logger

	mu     sync.RWMutex
	cache  map[string]*Schema // keyed by "subject:version"
	latest map[string]*Schema // keyed by subject
}

// NewPGStore creates a PG-backed schema store.
func NewPGStore(connString string, logger *slog.Logger) *PGStore {
	if logger == nil {
		logger = slog.Default()
	}
	return &PGStore{
		connString: connString,
		logger:     logger.With("component", "schema-store"),
		cache:      make(map[string]*Schema),
		latest:     make(map[string]*Schema),
	}
}

// Init creates the pgcdc_schemas table if it doesn't exist.
func (s *PGStore) Init(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, s.connString)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS pgcdc_schemas (
			subject    TEXT NOT NULL,
			version    INT NOT NULL,
			columns    JSONB NOT NULL,
			hash       TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (subject, version)
		)
	`)
	if err != nil {
		return fmt.Errorf("create schemas table: %w", err)
	}

	// Index for latest-version lookups.
	_, err = conn.Exec(ctx, `
		CREATE INDEX IF NOT EXISTS idx_pgcdc_schemas_subject
		ON pgcdc_schemas (subject, version DESC)
	`)
	if err != nil {
		return fmt.Errorf("create schemas index: %w", err)
	}

	return nil
}

func (s *PGStore) Register(ctx context.Context, subject string, columns []ColumnDef) (*Schema, bool, error) {
	hash := ComputeHash(columns)

	// Check cache for latest version with same hash.
	s.mu.RLock()
	if latest, ok := s.latest[subject]; ok && latest.Hash == hash {
		s.mu.RUnlock()
		return latest, false, nil
	}
	s.mu.RUnlock()

	conn, err := pgx.Connect(ctx, s.connString)
	if err != nil {
		return nil, false, fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	// Get latest version from DB.
	var latestVersion int
	var latestHash string
	err = conn.QueryRow(ctx,
		"SELECT version, hash FROM pgcdc_schemas WHERE subject = $1 ORDER BY version DESC LIMIT 1",
		subject,
	).Scan(&latestVersion, &latestHash)

	if err == nil && latestHash == hash {
		// Schema unchanged — load from DB and cache.
		schema, err := s.loadFromDB(ctx, conn, subject, latestVersion)
		if err != nil {
			return nil, false, err
		}
		s.cacheSchema(schema)
		return schema, false, nil
	}
	// err == pgx.ErrNoRows or hash differs — register new version.

	newVersion := latestVersion + 1
	columnsJSON, err := json.Marshal(columns)
	if err != nil {
		return nil, false, fmt.Errorf("marshal columns: %w", err)
	}

	var createdAt time.Time
	err = conn.QueryRow(ctx,
		`INSERT INTO pgcdc_schemas (subject, version, columns, hash)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (subject, version) DO NOTHING
		 RETURNING created_at`,
		subject, newVersion, columnsJSON, hash,
	).Scan(&createdAt)
	if err != nil {
		return nil, false, fmt.Errorf("insert schema: %w", err)
	}

	schema := &Schema{
		Subject:   subject,
		Version:   newVersion,
		Columns:   columns,
		Hash:      hash,
		CreatedAt: createdAt,
	}
	s.cacheSchema(schema)
	s.logger.Info("schema registered",
		"subject", subject,
		"version", newVersion,
		"columns", len(columns),
	)
	return schema, true, nil
}

func (s *PGStore) Get(ctx context.Context, subject string, version int) (*Schema, error) {
	// Check cache.
	key := fmt.Sprintf("%s:%d", subject, version)
	s.mu.RLock()
	if cached, ok := s.cache[key]; ok {
		s.mu.RUnlock()
		return cached, nil
	}
	s.mu.RUnlock()

	conn, err := pgx.Connect(ctx, s.connString)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	schema, err := s.loadFromDB(ctx, conn, subject, version)
	if err != nil {
		return nil, err
	}
	s.cacheSchema(schema)
	return schema, nil
}

func (s *PGStore) Latest(ctx context.Context, subject string) (*Schema, error) {
	// Check cache.
	s.mu.RLock()
	if cached, ok := s.latest[subject]; ok {
		s.mu.RUnlock()
		return cached, nil
	}
	s.mu.RUnlock()

	conn, err := pgx.Connect(ctx, s.connString)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	var version int
	err = conn.QueryRow(ctx,
		"SELECT version FROM pgcdc_schemas WHERE subject = $1 ORDER BY version DESC LIMIT 1",
		subject,
	).Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("get latest version: %w", err)
	}

	schema, err := s.loadFromDB(ctx, conn, subject, version)
	if err != nil {
		return nil, err
	}
	s.cacheSchema(schema)
	return schema, nil
}

func (s *PGStore) List(ctx context.Context, subject string) ([]*Schema, error) {
	conn, err := pgx.Connect(ctx, s.connString)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	rows, err := conn.Query(ctx,
		"SELECT version, columns, hash, created_at FROM pgcdc_schemas WHERE subject = $1 ORDER BY version",
		subject,
	)
	if err != nil {
		return nil, fmt.Errorf("list schemas: %w", err)
	}
	defer rows.Close()

	var schemas []*Schema
	for rows.Next() {
		var version int
		var columnsJSON []byte
		var hash string
		var createdAt time.Time

		if err := rows.Scan(&version, &columnsJSON, &hash, &createdAt); err != nil {
			return nil, fmt.Errorf("scan schema: %w", err)
		}

		var columns []ColumnDef
		if err := json.Unmarshal(columnsJSON, &columns); err != nil {
			return nil, fmt.Errorf("unmarshal columns: %w", err)
		}

		schemas = append(schemas, &Schema{
			Subject:   subject,
			Version:   version,
			Columns:   columns,
			Hash:      hash,
			CreatedAt: createdAt,
		})
	}

	return schemas, rows.Err()
}

func (s *PGStore) loadFromDB(ctx context.Context, conn *pgx.Conn, subject string, version int) (*Schema, error) {
	var columnsJSON []byte
	var hash string
	var createdAt time.Time

	err := conn.QueryRow(ctx,
		"SELECT columns, hash, created_at FROM pgcdc_schemas WHERE subject = $1 AND version = $2",
		subject, version,
	).Scan(&columnsJSON, &hash, &createdAt)
	if err != nil {
		return nil, fmt.Errorf("get schema %s v%d: %w", subject, version, err)
	}

	var columns []ColumnDef
	if err := json.Unmarshal(columnsJSON, &columns); err != nil {
		return nil, fmt.Errorf("unmarshal columns: %w", err)
	}

	return &Schema{
		Subject:   subject,
		Version:   version,
		Columns:   columns,
		Hash:      hash,
		CreatedAt: createdAt,
	}, nil
}

func (s *PGStore) cacheSchema(schema *Schema) {
	key := fmt.Sprintf("%s:%d", schema.Subject, schema.Version)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cache[key] = schema
	if existing, ok := s.latest[schema.Subject]; !ok || schema.Version > existing.Version {
		s.latest[schema.Subject] = schema
	}
}
