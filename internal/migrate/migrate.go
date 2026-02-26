package migrate

import (
	"context"
	"embed"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

//go:embed sql/*.sql
var migrations embed.FS

const migrationsTable = `CREATE TABLE IF NOT EXISTS pgcdc_migrations (
	version INT PRIMARY KEY,
	applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`

// Run executes all pending migrations against the database.
func Run(ctx context.Context, connString string, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}

	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	if _, err := conn.Exec(ctx, migrationsTable); err != nil {
		return fmt.Errorf("create migrations table: %w", err)
	}

	files, err := listMigrations()
	if err != nil {
		return fmt.Errorf("list migrations: %w", err)
	}

	for _, mig := range files {
		applied, err := isApplied(ctx, conn, mig.version)
		if err != nil {
			return fmt.Errorf("check migration %d: %w", mig.version, err)
		}
		if applied {
			continue
		}

		sql, err := migrations.ReadFile(mig.path)
		if err != nil {
			return fmt.Errorf("read migration %d: %w", mig.version, err)
		}

		start := time.Now()
		if _, err := conn.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("execute migration %d: %w", mig.version, err)
		}

		if _, err := conn.Exec(ctx, "INSERT INTO pgcdc_migrations (version) VALUES ($1)", mig.version); err != nil {
			return fmt.Errorf("record migration %d: %w", mig.version, err)
		}

		logger.Info("migration applied", "version", mig.version, "file", mig.path, "duration", time.Since(start))
	}

	return nil
}

type migration struct {
	version int
	path    string
}

func listMigrations() ([]migration, error) {
	entries, err := migrations.ReadDir("sql")
	if err != nil {
		return nil, fmt.Errorf("read sql dir: %w", err)
	}

	var migs []migration
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		v, err := parseMigrationVersion(e.Name())
		if err != nil {
			continue
		}
		migs = append(migs, migration{version: v, path: "sql/" + e.Name()})
	}

	sort.Slice(migs, func(i, j int) bool {
		return migs[i].version < migs[j].version
	})

	return migs, nil
}

// parseMigrationVersion extracts the version number from a filename like "001_initial.sql".
func parseMigrationVersion(name string) (int, error) {
	parts := strings.SplitN(name, "_", 2)
	if len(parts) < 2 {
		return 0, fmt.Errorf("invalid migration filename: %s", name)
	}
	v, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, fmt.Errorf("parse version from %s: %w", name, err)
	}
	return v, nil
}

func isApplied(ctx context.Context, conn *pgx.Conn, version int) (bool, error) {
	var count int
	err := conn.QueryRow(ctx, "SELECT COUNT(*) FROM pgcdc_migrations WHERE version = $1", version).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}
