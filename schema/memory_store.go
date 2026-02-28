package schema

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MemoryStore is an in-memory schema store for testing and lightweight use.
type MemoryStore struct {
	mu      sync.RWMutex
	schemas map[string][]*Schema // keyed by subject, ordered by version
}

// NewMemoryStore creates an in-memory schema store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		schemas: make(map[string][]*Schema),
	}
}

func (s *MemoryStore) Register(_ context.Context, subject string, columns []ColumnDef) (*Schema, bool, error) {
	hash := ComputeHash(columns)

	s.mu.Lock()
	defer s.mu.Unlock()

	versions := s.schemas[subject]
	if len(versions) > 0 {
		latest := versions[len(versions)-1]
		if latest.Hash == hash {
			return latest, false, nil
		}
	}

	schema := &Schema{
		Subject:   subject,
		Version:   len(versions) + 1,
		Columns:   columns,
		Hash:      hash,
		CreatedAt: time.Now().UTC(),
	}
	s.schemas[subject] = append(versions, schema)
	return schema, true, nil
}

func (s *MemoryStore) Get(_ context.Context, subject string, version int) (*Schema, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	versions := s.schemas[subject]
	for _, schema := range versions {
		if schema.Version == version {
			return schema, nil
		}
	}
	return nil, fmt.Errorf("schema %s v%d not found", subject, version)
}

func (s *MemoryStore) Latest(_ context.Context, subject string) (*Schema, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	versions := s.schemas[subject]
	if len(versions) == 0 {
		return nil, fmt.Errorf("no schemas for subject %s", subject)
	}
	return versions[len(versions)-1], nil
}

func (s *MemoryStore) List(_ context.Context, subject string) ([]*Schema, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	versions := s.schemas[subject]
	out := make([]*Schema, len(versions))
	copy(out, versions)
	return out, nil
}
