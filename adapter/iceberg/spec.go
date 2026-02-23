package iceberg

// Iceberg table format v2 type definitions.
// See: https://iceberg.apache.org/spec/

// TableMetadata is the top-level Iceberg table metadata (format-version 2).
type TableMetadata struct {
	FormatVersion    int                `json:"format-version"`
	TableUUID        string             `json:"table-uuid"`
	Location         string             `json:"location"`
	LastSeqNumber    int64              `json:"last-sequence-number"`
	LastUpdatedMS    int64              `json:"last-updated-ms"`
	LastColumnID     int                `json:"last-column-id"`
	Schemas          []Schema           `json:"schemas"`
	CurrentSchemaID  int                `json:"current-schema-id"`
	PartitionSpecs   []PartitionSpec    `json:"partition-specs"`
	DefaultSpecID    int                `json:"default-spec-id"`
	LastPartitionID  int                `json:"last-partition-id"`
	CurrentSnapshot  int64              `json:"current-snapshot-id"`
	Snapshots        []Snapshot         `json:"snapshots"`
	SnapshotLog      []SnapshotLogEntry `json:"snapshot-log"`
	SortOrders       []SortOrder        `json:"sort-orders"`
	DefaultSortOrder int                `json:"default-sort-order-id"`
	Properties       map[string]string  `json:"properties,omitempty"`
}

// Schema defines the columns of an Iceberg table.
type Schema struct {
	SchemaID int     `json:"schema-id"`
	Fields   []Field `json:"fields"`
}

// Field is a single column in an Iceberg schema.
type Field struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Type     string `json:"type"` // "string", "long", "boolean", "double", "timestamptz"
	Required bool   `json:"required"`
	Doc      string `json:"doc,omitempty"`
}

// PartitionSpec defines how data is partitioned.
type PartitionSpec struct {
	SpecID int              `json:"spec-id"`
	Fields []PartitionField `json:"fields"`
}

// PartitionField maps a source column to a partition transform.
type PartitionField struct {
	SourceID  int    `json:"source-id"`
	FieldID   int    `json:"field-id"`
	Name      string `json:"name"`
	Transform string `json:"transform"` // "identity", "day", "month", "year", "hour"
}

// Snapshot records a point-in-time view of the table.
type Snapshot struct {
	SnapshotID       int64             `json:"snapshot-id"`
	ParentSnapshotID *int64            `json:"parent-snapshot-id,omitempty"`
	SequenceNumber   int64             `json:"sequence-number"`
	TimestampMS      int64             `json:"timestamp-ms"`
	ManifestList     string            `json:"manifest-list"`
	Summary          map[string]string `json:"summary"`
	SchemaID         int               `json:"schema-id"`
}

// SnapshotLogEntry records when a snapshot was made current.
type SnapshotLogEntry struct {
	TimestampMS int64 `json:"timestamp-ms"`
	SnapshotID  int64 `json:"snapshot-id"`
}

// SortOrder defines how data is sorted within files.
type SortOrder struct {
	OrderID int         `json:"order-id"`
	Fields  []SortField `json:"fields"`
}

// SortField is a single sort column.
type SortField struct {
	SourceID  int    `json:"source-id"`
	Transform string `json:"transform"`
	Direction string `json:"direction"`  // "asc" or "desc"
	NullOrder string `json:"null-order"` // "nulls-first" or "nulls-last"
}

// DataFile describes a single Parquet data file in the table.
type DataFile struct {
	ContentType     int               `json:"content"` // 0 = data
	FilePath        string            `json:"file-path"`
	FileFormat      string            `json:"file-format"` // "PARQUET"
	PartitionData   map[string]string `json:"partition,omitempty"`
	RecordCount     int64             `json:"record-count"`
	FileSizeBytes   int64             `json:"file-size-in-bytes"`
	ColumnSizes     map[int]int64     `json:"column-sizes,omitempty"`
	ValueCounts     map[int]int64     `json:"value-counts,omitempty"`
	NullValueCounts map[int]int64     `json:"null-value-counts,omitempty"`
	LowerBounds     map[int][]byte    `json:"lower-bounds,omitempty"`
	UpperBounds     map[int][]byte    `json:"upper-bounds,omitempty"`
	SortOrderID     *int              `json:"sort-order-id,omitempty"`
}

// ManifestEntry status constants.
const (
	ManifestEntryStatusAdded    = 1
	ManifestEntryStatusExisting = 0
	ManifestEntryStatusDeleted  = 2
)

// ManifestEntry is a row in a manifest file (Avro).
type ManifestEntry struct {
	Status     int      `avro:"status"`
	SnapshotID int64    `avro:"snapshot_id"`
	DataFile   DataFile `avro:"-"` // serialized manually
}

// ManifestFile describes a manifest in the manifest list (Avro).
type ManifestFile struct {
	ManifestPath        string `avro:"manifest_path"`
	ManifestLength      int64  `avro:"manifest_length"`
	PartitionSpecID     int    `avro:"partition_spec_id"`
	ContentType         int    `avro:"content"` // 0 = data
	SequenceNumber      int64  `avro:"sequence_number"`
	MinSequenceNumber   int64  `avro:"min_sequence_number"`
	AddedSnapshotID     int64  `avro:"added_snapshot_id"`
	AddedDataFilesCount int    `avro:"added_data_files_count"`
	AddedRowsCount      int64  `avro:"added_rows_count"`
	ExistingDataFiles   int    `avro:"existing_data_files_count"`
	ExistingRowsCount   int64  `avro:"existing_rows_count"`
	DeletedDataFiles    int    `avro:"deleted_data_files_count"`
	DeletedRowsCount    int64  `avro:"deleted_rows_count"`
}

// Mode determines how events are written to the Iceberg table.
type Mode int

const (
	ModeAppend Mode = iota
	ModeUpsert
)

// SchemaMode determines how the Iceberg schema is derived.
type SchemaMode int

const (
	SchemaModeAuto SchemaMode = iota
	SchemaModeRaw
)
