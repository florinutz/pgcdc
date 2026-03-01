// Package schema tracks versioned column definitions detected from WAL
// replication events. It stores the column names, types, and nullability
// for each table (subject) and detects schema changes across versions.
//
// This is distinct from encoding/registry, which is a Confluent Schema
// Registry client for Avro/Protobuf wire format negotiation.
package schema

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"
)

// Schema represents a versioned schema for a subject (typically table).
type Schema struct {
	Subject   string      `json:"subject"`
	Version   int         `json:"version"`
	Columns   []ColumnDef `json:"columns"`
	Hash      string      `json:"hash"` // SHA-256 of canonical column definitions
	CreatedAt time.Time   `json:"created_at"`
}

// ColumnDef describes a column within a schema.
type ColumnDef struct {
	Name     string `json:"name"`
	TypeOID  uint32 `json:"type_oid"`
	TypeName string `json:"type_name"`
	Nullable bool   `json:"nullable,omitempty"`
}

// SchemaRef is a lightweight reference to a schema, attached to events.
type SchemaRef struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

// Store is the interface for schema storage and retrieval.
type Store interface {
	// Register stores a new schema version for the subject if the column
	// definitions differ from the latest version. Returns the schema
	// (existing if unchanged, new if different) and whether it was newly created.
	Register(ctx context.Context, subject string, columns []ColumnDef) (*Schema, bool, error)

	// Get retrieves a specific schema version for a subject.
	Get(ctx context.Context, subject string, version int) (*Schema, error)

	// Latest retrieves the most recent schema version for a subject.
	Latest(ctx context.Context, subject string) (*Schema, error)

	// List retrieves all schema versions for a subject, ordered by version.
	List(ctx context.Context, subject string) ([]*Schema, error)
}

// ComputeHash returns a deterministic SHA-256 hash of the column definitions.
// Two schemas with the same columns (in order) produce the same hash.
func ComputeHash(columns []ColumnDef) string {
	data, _ := json.Marshal(columns)
	h := sha256.Sum256(data)
	return fmt.Sprintf("%x", h)
}

// SubjectName builds a schema subject from schema and table names.
// Format: "<schema>.<table>" (e.g., "public.orders").
func SubjectName(schemaName, tableName string) string {
	if schemaName == "" {
		schemaName = "public"
	}
	return schemaName + "." + tableName
}
