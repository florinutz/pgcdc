package iceberg

import "context"

// Storage abstracts file I/O for Iceberg data, manifest, and metadata files.
type Storage interface {
	Write(ctx context.Context, path string, data []byte) error
	Read(ctx context.Context, path string) ([]byte, error)
	Exists(ctx context.Context, path string) (bool, error)
	Delete(ctx context.Context, path string) error
}
