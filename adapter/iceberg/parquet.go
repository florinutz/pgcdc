package iceberg

import (
	"bytes"
	"fmt"
	"time"

	"github.com/florinutz/pgcdc/event"
	"github.com/parquet-go/parquet-go"
)

// rawRow is the Parquet row struct for raw schema mode.
type rawRow struct {
	EventID   string `parquet:"event_id"`
	Operation string `parquet:"operation"`
	Timestamp int64  `parquet:"timestamp,timestamp(microsecond)"`
	Channel   string `parquet:"channel"`
	Source    string `parquet:"source"`
	Payload   string `parquet:"payload"`
}

// writeDataFile writes events as a Parquet data file and returns the DataFile metadata.
// The caller provides the full file path and the schema mode.
func writeDataFile(events []event.Event, schemaMode SchemaMode) ([]byte, *DataFile, error) {
	if schemaMode != SchemaModeRaw {
		return nil, nil, fmt.Errorf("only raw schema mode is supported in phase 1")
	}

	rows := make([]rawRow, 0, len(events))
	for _, ev := range events {
		rows = append(rows, rawRow{
			EventID:   ev.ID,
			Operation: ev.Operation,
			Timestamp: ev.CreatedAt.UnixMicro(),
			Channel:   ev.Channel,
			Source:    ev.Source,
			Payload:   string(ev.Payload),
		})
	}

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[rawRow](&buf, parquet.Compression(&parquet.Snappy))

	if _, err := w.Write(rows); err != nil {
		return nil, nil, fmt.Errorf("write parquet rows: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, nil, fmt.Errorf("close parquet writer: %w", err)
	}

	data := buf.Bytes()

	// Compute column stats for the raw schema.
	stats := computeRawStats(events)

	df := &DataFile{
		ContentType:     0, // data
		FileFormat:      "PARQUET",
		RecordCount:     int64(len(events)),
		FileSizeBytes:   int64(len(data)),
		ColumnSizes:     stats.columnSizes,
		ValueCounts:     stats.valueCounts,
		NullValueCounts: stats.nullValueCounts,
		LowerBounds:     stats.lowerBounds,
		UpperBounds:     stats.upperBounds,
	}

	return data, df, nil
}

// columnStats holds aggregated column statistics.
type columnStats struct {
	columnSizes     map[int]int64
	valueCounts     map[int]int64
	nullValueCounts map[int]int64
	lowerBounds     map[int][]byte
	upperBounds     map[int][]byte
}

// computeRawStats computes column-level stats for the raw schema.
func computeRawStats(events []event.Event) columnStats {
	n := int64(len(events))
	stats := columnStats{
		columnSizes:     make(map[int]int64),
		valueCounts:     make(map[int]int64),
		nullValueCounts: make(map[int]int64),
		lowerBounds:     make(map[int][]byte),
		upperBounds:     make(map[int][]byte),
	}

	if n == 0 {
		return stats
	}

	// Track min/max for string fields and timestamp.
	var minEventID, maxEventID string
	var minOp, maxOp string
	var minTS, maxTS time.Time
	var minChannel, maxChannel string
	var minSource, maxSource string

	for i, ev := range events {
		if i == 0 {
			minEventID, maxEventID = ev.ID, ev.ID
			minOp, maxOp = ev.Operation, ev.Operation
			minTS, maxTS = ev.CreatedAt, ev.CreatedAt
			minChannel, maxChannel = ev.Channel, ev.Channel
			minSource, maxSource = ev.Source, ev.Source
		} else {
			if ev.ID < minEventID {
				minEventID = ev.ID
			}
			if ev.ID > maxEventID {
				maxEventID = ev.ID
			}
			if ev.Operation < minOp {
				minOp = ev.Operation
			}
			if ev.Operation > maxOp {
				maxOp = ev.Operation
			}
			if ev.CreatedAt.Before(minTS) {
				minTS = ev.CreatedAt
			}
			if ev.CreatedAt.After(maxTS) {
				maxTS = ev.CreatedAt
			}
			if ev.Channel < minChannel {
				minChannel = ev.Channel
			}
			if ev.Channel > maxChannel {
				maxChannel = ev.Channel
			}
			if ev.Source < minSource {
				minSource = ev.Source
			}
			if ev.Source > maxSource {
				maxSource = ev.Source
			}
		}

		// Accumulate sizes.
		stats.columnSizes[1] += int64(len(ev.ID))
		stats.columnSizes[2] += int64(len(ev.Operation))
		stats.columnSizes[3] += 8 // timestamp = 8 bytes
		stats.columnSizes[4] += int64(len(ev.Channel))
		stats.columnSizes[5] += int64(len(ev.Source))
		stats.columnSizes[6] += int64(len(ev.Payload))
	}

	// Value counts (all non-null for raw schema).
	for id := 1; id <= 6; id++ {
		stats.valueCounts[id] = n
		stats.nullValueCounts[id] = 0
	}

	// Bounds as Iceberg single-value serialization (UTF-8 for strings, 8-byte LE for timestamp).
	stats.lowerBounds[1] = []byte(minEventID)
	stats.upperBounds[1] = []byte(maxEventID)
	stats.lowerBounds[2] = []byte(minOp)
	stats.upperBounds[2] = []byte(maxOp)
	stats.lowerBounds[3] = encodeTimestampMicros(minTS)
	stats.upperBounds[3] = encodeTimestampMicros(maxTS)
	stats.lowerBounds[4] = []byte(minChannel)
	stats.upperBounds[4] = []byte(maxChannel)
	stats.lowerBounds[5] = []byte(minSource)
	stats.upperBounds[5] = []byte(maxSource)
	// Skip bounds for payload (field 6) — unbounded string content.

	return stats
}

// encodeTimestampMicros encodes a time as 8-byte little-endian microseconds since epoch.
func encodeTimestampMicros(t time.Time) []byte {
	micros := t.UnixMicro()
	b := make([]byte, 8)
	b[0] = byte(micros)
	b[1] = byte(micros >> 8)
	b[2] = byte(micros >> 16)
	b[3] = byte(micros >> 24)
	b[4] = byte(micros >> 32)
	b[5] = byte(micros >> 40)
	b[6] = byte(micros >> 48)
	b[7] = byte(micros >> 56)
	return b
}
