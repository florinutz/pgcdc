package iceberg

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
)

// newTableMetadata creates initial Iceberg v2 table metadata.
func newTableMetadata(location string, schema *Schema, partitionSpec *PartitionSpec) *TableMetadata {
	now := time.Now().UnixMilli()
	meta := &TableMetadata{
		FormatVersion:    2,
		TableUUID:        uuid.New().String(),
		Location:         location,
		LastSeqNumber:    0,
		LastUpdatedMS:    now,
		LastColumnID:     lastFieldID(schema),
		Schemas:          []Schema{*schema},
		CurrentSchemaID:  schema.SchemaID,
		PartitionSpecs:   []PartitionSpec{*partitionSpec},
		DefaultSpecID:    partitionSpec.SpecID,
		LastPartitionID:  lastPartFieldID(partitionSpec),
		CurrentSnapshot:  -1,
		Snapshots:        []Snapshot{},
		SnapshotLog:      []SnapshotLogEntry{},
		SortOrders:       []SortOrder{{OrderID: 0, Fields: []SortField{}}},
		DefaultSortOrder: 0,
		Properties:       map[string]string{},
	}
	return meta
}

// writeMetadata serializes table metadata to JSON.
func writeMetadata(meta *TableMetadata) ([]byte, error) {
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal metadata: %w", err)
	}
	return data, nil
}

// readMetadata deserializes table metadata from JSON.
func readMetadata(data []byte) (*TableMetadata, error) {
	var meta TableMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("unmarshal metadata: %w", err)
	}
	return &meta, nil
}

// generateSnapshotID produces a random positive int64 for snapshot IDs.
func generateSnapshotID() int64 {
	return rand.Int64N(1<<62) + 1
}

// lastPartFieldID returns the highest partition field ID in the spec.
func lastPartFieldID(spec *PartitionSpec) int {
	max := 999 // Iceberg reserves 1000+ for partition fields
	for _, f := range spec.Fields {
		if f.FieldID > max {
			max = f.FieldID
		}
	}
	return max
}
