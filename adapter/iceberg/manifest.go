package iceberg

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/hamba/avro/v2/ocf"
)

// Avro schema for Iceberg manifest entries (format v2).
// This is a simplified version focused on the fields we need.
const manifestEntryAvroSchema = `{
	"type": "record",
	"name": "manifest_entry",
	"fields": [
		{"name": "status", "type": "int"},
		{"name": "snapshot_id", "type": ["null", "long"], "default": null},
		{"name": "sequence_number", "type": ["null", "long"], "default": null},
		{"name": "file_sequence_number", "type": ["null", "long"], "default": null},
		{"name": "data_file", "type": {
			"type": "record",
			"name": "r2",
			"fields": [
				{"name": "content", "type": "int"},
				{"name": "file_path", "type": "string"},
				{"name": "file_format", "type": "string"},
				{"name": "partition", "type": {"type": "record", "name": "r102", "fields": []}},
				{"name": "record_count", "type": "long"},
				{"name": "file_size_in_bytes", "type": "long"},
				{"name": "column_sizes", "type": ["null", {"type": "array", "items": {
					"type": "record", "name": "k117_v118",
					"fields": [
						{"name": "key", "type": "int"},
						{"name": "value", "type": "long"}
					]
				}, "logicalType": "map"}], "default": null},
				{"name": "value_counts", "type": ["null", {"type": "array", "items": {
					"type": "record", "name": "k119_v120",
					"fields": [
						{"name": "key", "type": "int"},
						{"name": "value", "type": "long"}
					]
				}, "logicalType": "map"}], "default": null},
				{"name": "null_value_counts", "type": ["null", {"type": "array", "items": {
					"type": "record", "name": "k121_v122",
					"fields": [
						{"name": "key", "type": "int"},
						{"name": "value", "type": "long"}
					]
				}, "logicalType": "map"}], "default": null},
				{"name": "nan_value_counts", "type": ["null", {"type": "array", "items": {
					"type": "record", "name": "k138_v139",
					"fields": [
						{"name": "key", "type": "int"},
						{"name": "value", "type": "long"}
					]
				}, "logicalType": "map"}], "default": null},
				{"name": "lower_bounds", "type": ["null", {"type": "array", "items": {
					"type": "record", "name": "k126_v127",
					"fields": [
						{"name": "key", "type": "int"},
						{"name": "value", "type": "bytes"}
					]
				}, "logicalType": "map"}], "default": null},
				{"name": "upper_bounds", "type": ["null", {"type": "array", "items": {
					"type": "record", "name": "k128_v129",
					"fields": [
						{"name": "key", "type": "int"},
						{"name": "value", "type": "bytes"}
					]
				}, "logicalType": "map"}], "default": null},
				{"name": "key_metadata", "type": ["null", "bytes"], "default": null},
				{"name": "split_offsets", "type": ["null", {"type": "array", "items": "long"}], "default": null},
				{"name": "equality_ids", "type": ["null", {"type": "array", "items": "int"}], "default": null},
				{"name": "sort_order_id", "type": ["null", "int"], "default": null}
			]
		}}
	]
}`

// manifestEntryAvro is the Avro-serializable form of a manifest entry.
type manifestEntryAvro struct {
	Status             int                  `avro:"status"`
	SnapshotID         *int64               `avro:"snapshot_id"`
	SequenceNumber     *int64               `avro:"sequence_number"`
	FileSequenceNumber *int64               `avro:"file_sequence_number"`
	DataFile           manifestDataFileAvro `avro:"data_file"`
}

type manifestDataFileAvro struct {
	Content         int          `avro:"content"`
	FilePath        string       `avro:"file_path"`
	FileFormat      string       `avro:"file_format"`
	Partition       struct{}     `avro:"partition"`
	RecordCount     int64        `avro:"record_count"`
	FileSizeBytes   int64        `avro:"file_size_in_bytes"`
	ColumnSizes     []intLongKV  `avro:"column_sizes"`
	ValueCounts     []intLongKV  `avro:"value_counts"`
	NullValueCounts []intLongKV  `avro:"null_value_counts"`
	NanValueCounts  []intLongKV  `avro:"nan_value_counts"`
	LowerBounds     []intBytesKV `avro:"lower_bounds"`
	UpperBounds     []intBytesKV `avro:"upper_bounds"`
	KeyMetadata     []byte       `avro:"key_metadata"`
	SplitOffsets    []int64      `avro:"split_offsets"`
	EqualityIDs     []int        `avro:"equality_ids"`
	SortOrderID     *int         `avro:"sort_order_id"`
}

type intLongKV struct {
	Key   int   `avro:"key"`
	Value int64 `avro:"value"`
}

type intBytesKV struct {
	Key   int    `avro:"key"`
	Value []byte `avro:"value"`
}

// writeManifest writes manifest entries as an Avro OCF file.
// Returns the serialized bytes.
func writeManifest(entries []ManifestEntry, dataFiles []*DataFile, schema *Schema, snapshotID, seqNum int64, specID int) ([]byte, error) {
	// Build schema JSON for the manifest metadata.
	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		return nil, fmt.Errorf("marshal schema: %w", err)
	}

	var buf bytes.Buffer
	enc, err := ocf.NewEncoder(manifestEntryAvroSchema, &buf,
		ocf.WithMetadata(map[string][]byte{
			"schema":            schemaJSON,
			"schema-id":         encodeIntBytes(schema.SchemaID),
			"partition-spec":    []byte("[]"),
			"partition-spec-id": encodeIntBytes(specID),
			"format-version":    []byte("2"),
			"content":           []byte("data"),
		}),
		ocf.WithCodec(ocf.Deflate),
	)
	if err != nil {
		return nil, fmt.Errorf("create manifest encoder: %w", err)
	}

	for i, entry := range entries {
		df := dataFiles[i]
		avroEntry := toManifestEntryAvro(entry, df, seqNum)
		if err := enc.Encode(avroEntry); err != nil {
			return nil, fmt.Errorf("encode manifest entry: %w", err)
		}
	}

	if err := enc.Close(); err != nil {
		return nil, fmt.Errorf("close manifest encoder: %w", err)
	}

	return buf.Bytes(), nil
}

func toManifestEntryAvro(entry ManifestEntry, df *DataFile, seqNum int64) manifestEntryAvro {
	snapID := entry.SnapshotID
	return manifestEntryAvro{
		Status:             entry.Status,
		SnapshotID:         &snapID,
		SequenceNumber:     &seqNum,
		FileSequenceNumber: &seqNum,
		DataFile: manifestDataFileAvro{
			Content:         df.ContentType,
			FilePath:        df.FilePath,
			FileFormat:      df.FileFormat,
			RecordCount:     df.RecordCount,
			FileSizeBytes:   df.FileSizeBytes,
			ColumnSizes:     mapToIntLongKV(df.ColumnSizes),
			ValueCounts:     mapToIntLongKV(df.ValueCounts),
			NullValueCounts: mapToIntLongKV(df.NullValueCounts),
			LowerBounds:     mapToIntBytesKV(df.LowerBounds),
			UpperBounds:     mapToIntBytesKV(df.UpperBounds),
		},
	}
}

func mapToIntLongKV(m map[int]int64) []intLongKV {
	if len(m) == 0 {
		return nil
	}
	out := make([]intLongKV, 0, len(m))
	for k, v := range m {
		out = append(out, intLongKV{Key: k, Value: v})
	}
	return out
}

func mapToIntBytesKV(m map[int][]byte) []intBytesKV {
	if len(m) == 0 {
		return nil
	}
	out := make([]intBytesKV, 0, len(m))
	for k, v := range m {
		out = append(out, intBytesKV{Key: k, Value: v})
	}
	return out
}

func encodeIntBytes(v int) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(v))
	return b
}

// Avro schema for manifest list (format v2).
const manifestListAvroSchema = `{
	"type": "record",
	"name": "manifest_file",
	"fields": [
		{"name": "manifest_path", "type": "string"},
		{"name": "manifest_length", "type": "long"},
		{"name": "partition_spec_id", "type": "int"},
		{"name": "content", "type": "int"},
		{"name": "sequence_number", "type": "long"},
		{"name": "min_sequence_number", "type": "long"},
		{"name": "added_snapshot_id", "type": "long"},
		{"name": "added_data_files_count", "type": "int"},
		{"name": "added_rows_count", "type": "long"},
		{"name": "existing_data_files_count", "type": "int"},
		{"name": "existing_rows_count", "type": "long"},
		{"name": "deleted_data_files_count", "type": "int"},
		{"name": "deleted_rows_count", "type": "long"}
	]
}`

// writeManifestList writes manifest file entries as an Avro OCF file.
func writeManifestList(manifests []ManifestFile) ([]byte, error) {
	var buf bytes.Buffer
	enc, err := ocf.NewEncoder(manifestListAvroSchema, &buf,
		ocf.WithMetadata(map[string][]byte{
			"format-version": []byte("2"),
		}),
		ocf.WithCodec(ocf.Deflate),
	)
	if err != nil {
		return nil, fmt.Errorf("create manifest list encoder: %w", err)
	}

	for _, mf := range manifests {
		if err := enc.Encode(mf); err != nil {
			return nil, fmt.Errorf("encode manifest list entry: %w", err)
		}
	}

	if err := enc.Close(); err != nil {
		return nil, fmt.Errorf("close manifest list encoder: %w", err)
	}

	return buf.Bytes(), nil
}
