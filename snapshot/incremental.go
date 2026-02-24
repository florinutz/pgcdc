package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/metrics"
)

const (
	incrementalSource  = "incremental_snapshot"
	snapshotCtlChannel = "pgcdc:_snapshot"
	chunkTimeout       = 60 * time.Second
)

// IncrementalSnapshot reads a table in primary-key-ordered chunks, pushing
// SNAPSHOT events to the shared events channel. Each chunk is a short-lived
// transaction, avoiding the long-running transaction problems of full-table
// snapshots.
type IncrementalSnapshot struct {
	dbURL      string
	table      string
	chunkSize  int
	chunkDelay time.Duration
	progress   ProgressStore
	snapshotID string
	logger     *slog.Logger
}

// NewIncremental creates an IncrementalSnapshot. Defaults: chunkSize=1000, chunkDelay=0.
func NewIncremental(dbURL, table string, chunkSize int, chunkDelay time.Duration, progress ProgressStore, snapshotID string, logger *slog.Logger) *IncrementalSnapshot {
	if chunkSize <= 0 {
		chunkSize = 1000
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &IncrementalSnapshot{
		dbURL:      dbURL,
		table:      table,
		chunkSize:  chunkSize,
		chunkDelay: chunkDelay,
		progress:   progress,
		snapshotID: snapshotID,
		logger:     logger.With("component", "incremental_snapshot", "snapshot_id", snapshotID, "table", table),
	}
}

// Run executes the chunked snapshot, sending SNAPSHOT events to the events channel.
// It emits SNAPSHOT_STARTED and SNAPSHOT_COMPLETED control events on pgcdc:_snapshot.
// On context cancellation, progress is saved as StatusPaused for later resume.
func (s *IncrementalSnapshot) Run(ctx context.Context, events chan<- event.Event) error {
	start := time.Now()
	metrics.IncrementalSnapshotsActive.Inc()
	defer metrics.IncrementalSnapshotsActive.Dec()

	// Detect primary key columns.
	pkCols, err := s.detectPrimaryKey(ctx)
	if err != nil {
		return fmt.Errorf("detect primary key: %w", err)
	}

	s.logger.Info("starting incremental snapshot", "pk_columns", pkCols, "chunk_size", s.chunkSize)

	// Load resume state if available.
	var afterPK []any
	var totalRows int64
	if s.progress != nil {
		rec, loadErr := s.progress.Load(ctx, s.snapshotID)
		if loadErr != nil {
			s.logger.Warn("failed to load progress, starting from beginning", "error", loadErr)
		} else if rec != nil && rec.LastPK != nil {
			var pk []any
			if err := json.Unmarshal(rec.LastPK, &pk); err == nil {
				afterPK = pk
				totalRows = rec.RowsProcessed
				s.logger.Info("resuming from checkpoint", "last_pk", afterPK, "rows_processed", totalRows)
			}
		}
	}

	// Save initial running state.
	// All progress saves use a short-lived background context. Using the parent
	// ctx could poison the pgx.Conn if the context gets cancelled mid-query.
	if s.progress != nil {
		_ = s.saveProgress(ProgressRecord{
			SnapshotID:    s.snapshotID,
			TableName:     s.table,
			LastPK:        marshalPK(afterPK),
			RowsProcessed: totalRows,
			Status:        StatusRunning,
		})
	}

	// Emit SNAPSHOT_STARTED.
	if err := s.emitControl(ctx, events, event.OpSnapshotStarted, map[string]any{
		"table":       s.table,
		"chunk_size":  s.chunkSize,
		"snapshot_id": s.snapshotID,
	}); err != nil {
		return err
	}

	// Chunk loop.
	for {
		count, lastPK, chunkErr := s.readChunk(ctx, pkCols, afterPK, events)
		if chunkErr != nil {
			// On context cancellation, save as paused.
			if ctx.Err() != nil {
				if s.progress != nil {
					_ = s.saveProgress(ProgressRecord{
						SnapshotID:    s.snapshotID,
						TableName:     s.table,
						LastPK:        marshalPK(afterPK),
						RowsProcessed: totalRows,
						Status:        StatusPaused,
					})
				}
				return ctx.Err()
			}
			return chunkErr
		}

		totalRows += int64(count)
		metrics.IncrementalSnapshotChunks.Inc()

		// Save progress after each chunk.
		if s.progress != nil && lastPK != nil {
			if saveErr := s.saveProgress(ProgressRecord{
				SnapshotID:    s.snapshotID,
				TableName:     s.table,
				LastPK:        marshalPK(lastPK),
				RowsProcessed: totalRows,
				Status:        StatusRunning,
			}); saveErr != nil {
				s.logger.Warn("failed to save progress", "error", saveErr)
			}
		}

		if lastPK != nil {
			afterPK = lastPK
		}

		s.logger.Info("chunk complete", "chunk_rows", count, "total_rows", totalRows)

		// If chunk returned fewer rows than chunkSize, we've reached the end.
		if count < s.chunkSize {
			break
		}

		// Optional delay between chunks.
		if s.chunkDelay > 0 {
			select {
			case <-ctx.Done():
				if s.progress != nil {
					_ = s.saveProgress(ProgressRecord{
						SnapshotID:    s.snapshotID,
						TableName:     s.table,
						LastPK:        marshalPK(afterPK),
						RowsProcessed: totalRows,
						Status:        StatusPaused,
					})
				}
				return ctx.Err()
			case <-time.After(s.chunkDelay):
			}
		}
	}

	// Emit SNAPSHOT_COMPLETED.
	if err := s.emitControl(ctx, events, event.OpSnapshotCompleted, map[string]any{
		"table":       s.table,
		"rows":        totalRows,
		"snapshot_id": s.snapshotID,
		"duration_ms": time.Since(start).Milliseconds(),
	}); err != nil {
		return err
	}

	// Mark completed.
	if s.progress != nil {
		_ = s.saveProgress(ProgressRecord{
			SnapshotID:    s.snapshotID,
			TableName:     s.table,
			LastPK:        marshalPK(afterPK),
			RowsProcessed: totalRows,
			Status:        StatusCompleted,
		})
	}

	elapsed := time.Since(start)
	metrics.IncrementalSnapshotDuration.Observe(elapsed.Seconds())
	s.logger.Info("incremental snapshot complete", "rows", totalRows, "elapsed", elapsed)

	return nil
}

// detectPrimaryKey queries pg_index/pg_attribute for the table's primary key columns.
func (s *IncrementalSnapshot) detectPrimaryKey(ctx context.Context) ([]string, error) {
	connCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	conn, err := pgx.Connect(connCtx, s.dbURL)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(connCtx) }()

	rows, err := conn.Query(connCtx, `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = $1::regclass AND i.indisprimary
		ORDER BY array_position(i.indkey, a.attnum)
	`, s.table)
	if err != nil {
		return nil, fmt.Errorf("query pk: %w", err)
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("scan pk column: %w", err)
		}
		cols = append(cols, col)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pk columns: %w", err)
	}

	if len(cols) == 0 {
		return nil, fmt.Errorf("table %q has no primary key", s.table)
	}

	return cols, nil
}

// readChunk reads one chunk of rows from the table, starting after afterPK.
// Returns the number of rows read, the last PK values for the next chunk, and any error.
func (s *IncrementalSnapshot) readChunk(ctx context.Context, pkCols []string, afterPK []any, events chan<- event.Event) (int, []any, error) {
	chunkCtx, cancel := context.WithTimeout(ctx, chunkTimeout)
	defer cancel()

	conn, err := pgx.Connect(chunkCtx, s.dbURL)
	if err != nil {
		return 0, nil, fmt.Errorf("chunk connect: %w", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = conn.Close(closeCtx)
	}()

	safeTable := pgx.Identifier{s.table}.Sanitize()
	query, args := buildChunkQuery(safeTable, pkCols, afterPK, s.chunkSize)

	rows, err := conn.Query(chunkCtx, query, args...)
	if err != nil {
		return 0, nil, fmt.Errorf("chunk query: %w", err)
	}
	defer rows.Close()

	fieldDescs := rows.FieldDescriptions()
	channel := "pgcdc:" + s.table
	var count int
	var lastPK []any

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return count, lastPK, fmt.Errorf("read row: %w", err)
		}

		row := make(map[string]any, len(fieldDescs))
		for i, fd := range fieldDescs {
			row[fd.Name] = values[i]
		}

		// Extract PK values for next chunk cursor.
		pk := make([]any, len(pkCols))
		for i, col := range pkCols {
			pk[i] = row[col]
		}
		lastPK = pk

		payload := map[string]any{
			"op":          event.OpSnapshot,
			"table":       s.table,
			"row":         row,
			"old":         nil,
			"snapshot_id": s.snapshotID,
		}

		payloadJSON, err := json.Marshal(payload)
		if err != nil {
			s.logger.Error("marshal payload failed", "error", err)
			continue
		}

		ev, err := event.New(channel, event.OpSnapshot, payloadJSON, incrementalSource)
		if err != nil {
			s.logger.Error("create event failed", "error", err)
			continue
		}

		select {
		case events <- ev:
			count++
			metrics.IncrementalSnapshotRowsExported.Inc()
		case <-ctx.Done():
			return count, lastPK, ctx.Err()
		}
	}

	if err := rows.Err(); err != nil {
		return count, lastPK, fmt.Errorf("rows iteration: %w", err)
	}

	return count, lastPK, nil
}

// buildChunkQuery builds a SELECT query for one chunk.
// For the first chunk (afterPK is nil): SELECT * FROM table ORDER BY pk LIMIT N
// For subsequent chunks: SELECT * FROM table WHERE (pk1, pk2) > ($1, $2) ORDER BY pk1, pk2 LIMIT N
func buildChunkQuery(safeTable string, pkCols []string, afterPK []any, chunkSize int) (string, []any) {
	safePKCols := make([]string, len(pkCols))
	for i, col := range pkCols {
		safePKCols[i] = pgx.Identifier{col}.Sanitize()
	}
	orderBy := strings.Join(safePKCols, ", ")

	if len(afterPK) == 0 {
		query := fmt.Sprintf("SELECT * FROM %s ORDER BY %s LIMIT %d", safeTable, orderBy, chunkSize)
		return query, nil
	}

	// Build tuple comparison: (col1, col2) > ($1, $2)
	params := make([]string, len(pkCols))
	args := make([]any, len(afterPK))
	for i := range pkCols {
		params[i] = fmt.Sprintf("$%d", i+1)
		args[i] = afterPK[i]
	}

	pkTuple := "(" + strings.Join(safePKCols, ", ") + ")"
	paramTuple := "(" + strings.Join(params, ", ") + ")"

	query := fmt.Sprintf("SELECT * FROM %s WHERE %s > %s ORDER BY %s LIMIT %d",
		safeTable, pkTuple, paramTuple, orderBy, chunkSize)
	return query, args
}

// emitControl sends a control event (SNAPSHOT_STARTED/SNAPSHOT_COMPLETED) on pgcdc:_snapshot.
func (s *IncrementalSnapshot) emitControl(ctx context.Context, events chan<- event.Event, operation string, extra map[string]any) error {
	payloadJSON, err := json.Marshal(extra)
	if err != nil {
		s.logger.Error("marshal control payload failed", "error", err)
		return nil
	}

	ev, err := event.New(snapshotCtlChannel, operation, payloadJSON, incrementalSource)
	if err != nil {
		s.logger.Error("create control event failed", "error", err)
		return nil
	}

	select {
	case events <- ev:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// saveProgress saves a progress record using a short-lived background context.
// This avoids poisoning the pgx.Conn when the parent context is cancelled.
func (s *IncrementalSnapshot) saveProgress(rec ProgressRecord) error {
	saveCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return s.progress.Save(saveCtx, rec)
}

// marshalPK serializes PK values to JSON for storage.
func marshalPK(pk []any) json.RawMessage {
	if pk == nil {
		return nil
	}
	data, err := json.Marshal(pk)
	if err != nil {
		return nil
	}
	return data
}
