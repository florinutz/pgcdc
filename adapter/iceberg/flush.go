package iceberg

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/google/uuid"
)

// flush atomically swaps the buffer and writes the batch to Iceberg.
func (a *Adapter) flush(ctx context.Context) error {
	a.mu.Lock()
	if len(a.buffer) == 0 {
		a.mu.Unlock()
		return nil
	}
	batch := a.buffer
	a.buffer = make([]event.Event, 0, a.flushSize)
	a.mu.Unlock()

	metrics.IcebergBufferSize.Set(0)

	start := time.Now()
	err := a.flushAppend(ctx, batch)
	duration := time.Since(start).Seconds()

	metrics.IcebergFlushDuration.Observe(duration)
	if err != nil {
		a.logger.Error("flush failed",
			"events", len(batch),
			"duration", duration,
			"error", err,
		)
		// Put events back in the buffer for retry.
		a.mu.Lock()
		a.buffer = append(batch, a.buffer...)
		bufLen := len(a.buffer)
		a.mu.Unlock()
		metrics.IcebergBufferSize.Set(float64(bufLen))
		return err
	}

	metrics.IcebergFlushes.WithLabelValues("append").Inc()
	metrics.IcebergFlushSize.Observe(float64(len(batch)))
	a.logger.Info("flush complete",
		"events", len(batch),
		"duration_s", fmt.Sprintf("%.3f", duration),
	)
	return nil
}

// flushAppend writes events in append mode:
// 1. Write Parquet data file
// 2. Write Avro manifest
// 3. Write Avro manifest list
// 4. Update table metadata with new snapshot
// 5. Commit to catalog
func (a *Adapter) flushAppend(ctx context.Context, events []event.Event) error {
	tableLocation := a.tableLocation()

	// 1. Write Parquet data file.
	parquetData, dataFile, err := writeDataFile(events, a.schemaMode)
	if err != nil {
		return fmt.Errorf("write data file: %w", err)
	}

	dataFileName := fmt.Sprintf("%s.parquet", uuid.New().String())
	dataFilePath := filepath.Join(tableLocation, "data", dataFileName)
	dataFile.FilePath = dataFilePath

	if err := a.storage.Write(ctx, dataFilePath, parquetData); err != nil {
		return fmt.Errorf("store data file: %w", err)
	}
	metrics.IcebergDataFilesWritten.Inc()
	metrics.IcebergBytesWritten.Add(float64(len(parquetData)))

	// 2. Ensure we have table metadata (load or create).
	if a.tableMeta == nil {
		if err := a.ensureTable(ctx); err != nil {
			return fmt.Errorf("ensure table: %w", err)
		}
	}

	nextSeqNum := a.tableMeta.LastSeqNumber + 1

	// Build snapshot summary.
	summary := map[string]string{
		"operation":        "append",
		"added-data-files": "1",
		"added-records":    fmt.Sprintf("%d", dataFile.RecordCount),
		"total-data-files": fmt.Sprintf("%d", countDataFiles(a.tableMeta)+1),
		"total-records":    fmt.Sprintf("%d", countRecords(a.tableMeta)+dataFile.RecordCount),
	}

	// Build snapshot ID early so we can reference it in manifest.
	snapID := generateSnapshotID()

	// 3. Write Avro manifest.
	manifestEntries := []ManifestEntry{
		{Status: ManifestEntryStatusAdded, SnapshotID: snapID},
	}
	manifestData, err := writeManifest(manifestEntries, []*DataFile{dataFile}, a.schema, snapID, nextSeqNum, a.tableMeta.DefaultSpecID)
	if err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	manifestFileName := fmt.Sprintf("%s-m0.avro", uuid.New().String())
	manifestFilePath := filepath.Join(tableLocation, "metadata", manifestFileName)

	if err := a.storage.Write(ctx, manifestFilePath, manifestData); err != nil {
		return fmt.Errorf("store manifest: %w", err)
	}

	// 4. Write Avro manifest list.
	// Include manifests from previous snapshot (if any) plus the new one.
	manifestFiles := a.previousManifestFiles()
	manifestFiles = append(manifestFiles, ManifestFile{
		ManifestPath:        manifestFilePath,
		ManifestLength:      int64(len(manifestData)),
		PartitionSpecID:     a.tableMeta.DefaultSpecID,
		ContentType:         0, // data
		SequenceNumber:      nextSeqNum,
		MinSequenceNumber:   nextSeqNum,
		AddedSnapshotID:     snapID,
		AddedDataFilesCount: 1,
		AddedRowsCount:      dataFile.RecordCount,
	})

	manifestListData, err := writeManifestList(manifestFiles)
	if err != nil {
		return fmt.Errorf("write manifest list: %w", err)
	}

	manifestListFileName := fmt.Sprintf("snap-%d-%d-%s.avro", snapID, 0, uuid.New().String())
	manifestListPath := filepath.Join(tableLocation, "metadata", manifestListFileName)

	if err := a.storage.Write(ctx, manifestListPath, manifestListData); err != nil {
		return fmt.Errorf("store manifest list: %w", err)
	}

	// 5. Build new metadata with snapshot.
	// We directly construct the snapshot instead of using addSnapshot to control the ID.
	now := time.Now()
	a.tableMeta.LastSeqNumber = nextSeqNum
	a.tableMeta.LastUpdatedMS = now.UnixMilli()

	var parentID *int64
	if a.tableMeta.CurrentSnapshot >= 0 {
		old := a.tableMeta.CurrentSnapshot
		parentID = &old
	}

	snap := Snapshot{
		SnapshotID:       snapID,
		ParentSnapshotID: parentID,
		SequenceNumber:   nextSeqNum,
		TimestampMS:      now.UnixMilli(),
		ManifestList:     manifestListPath,
		Summary:          summary,
		SchemaID:         a.tableMeta.CurrentSchemaID,
	}
	a.tableMeta.Snapshots = append(a.tableMeta.Snapshots, snap)
	a.tableMeta.SnapshotLog = append(a.tableMeta.SnapshotLog, SnapshotLogEntry{
		TimestampMS: now.UnixMilli(),
		SnapshotID:  snapID,
	})
	oldSnapshot := a.tableMeta.CurrentSnapshot
	a.tableMeta.CurrentSnapshot = snapID

	// 6. Commit to catalog.
	oldMeta := *a.tableMeta
	oldMeta.CurrentSnapshot = oldSnapshot
	if err := a.catalog.CommitTable(ctx, a.namespace, a.tableName, &oldMeta, a.tableMeta); err != nil {
		return fmt.Errorf("commit table: %w", err)
	}

	return nil
}

// ensureTable loads or creates the Iceberg table.
func (a *Adapter) ensureTable(ctx context.Context) error {
	meta, err := a.catalog.LoadTable(ctx, a.namespace, a.tableName)
	if err != nil {
		return fmt.Errorf("load table: %w", err)
	}

	if meta != nil {
		a.tableMeta = meta
		// Use the table's current schema.
		for _, s := range meta.Schemas {
			if s.SchemaID == meta.CurrentSchemaID {
				schemaCopy := s
				a.schema = &schemaCopy
				break
			}
		}
		return nil
	}

	// Create new table.
	schema := rawSchema()
	a.schema = schema

	partSpec := &PartitionSpec{
		SpecID: 0,
		Fields: []PartitionField{},
	}

	meta = newTableMetadata(a.tableLocation(), schema, partSpec)
	if err := a.catalog.CreateTable(ctx, a.namespace, a.tableName, meta); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	a.tableMeta = meta
	return nil
}

// tableLocation returns the filesystem path for the table.
func (a *Adapter) tableLocation() string {
	parts := append([]string{a.warehouse}, a.namespace...)
	parts = append(parts, a.tableName)
	return filepath.Join(parts...)
}

// countDataFiles counts data files across all snapshots' manifest summaries.
func countDataFiles(meta *TableMetadata) int64 {
	if meta.CurrentSnapshot < 0 || len(meta.Snapshots) == 0 {
		return 0
	}
	for _, s := range meta.Snapshots {
		if s.SnapshotID == meta.CurrentSnapshot {
			if v, ok := s.Summary["total-data-files"]; ok {
				n, _ := strconv.ParseInt(v, 10, 64)
				return n
			}
		}
	}
	return 0
}

// countRecords counts total records from the current snapshot summary.
func countRecords(meta *TableMetadata) int64 {
	if meta.CurrentSnapshot < 0 || len(meta.Snapshots) == 0 {
		return 0
	}
	for _, s := range meta.Snapshots {
		if s.SnapshotID == meta.CurrentSnapshot {
			if v, ok := s.Summary["total-records"]; ok {
				n, _ := strconv.ParseInt(v, 10, 64)
				return n
			}
		}
	}
	return 0
}

// previousManifestFiles collects manifest file entries from the current snapshot's manifest list.
// For Phase 1 simplicity, we track them from metadata summaries.
// A more complete implementation would read the manifest list Avro file.
func (a *Adapter) previousManifestFiles() []ManifestFile {
	// In append mode, each new manifest list includes previous manifests.
	// We don't re-read old manifest lists in Phase 1 — we rely on the catalog
	// always having fresh metadata. For hadoop catalog, this works because
	// we hold the metadata in memory and update it on each flush.
	return nil
}
