package iceberg

import "context"

// Catalog manages Iceberg table metadata lifecycle.
type Catalog interface {
	// LoadTable loads the current table metadata. Returns nil, nil if the table does not exist.
	LoadTable(ctx context.Context, ns []string, table string) (*TableMetadata, error)

	// CreateTable creates a new table with the given metadata.
	CreateTable(ctx context.Context, ns []string, table string, meta *TableMetadata) error

	// CommitTable atomically updates the table metadata pointer.
	// oldMeta is the metadata that was read; the commit fails if the table has been modified since.
	CommitTable(ctx context.Context, ns []string, table string, oldMeta, newMeta *TableMetadata) error
}
