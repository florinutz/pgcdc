package walreplication

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/florinutz/pgcdc/checkpoint"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/backoff"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/pgcdcerr"
	"github.com/florinutz/pgcdc/snapshot"
)

const (
	source     = "wal_replication"
	txnChannel = "pgcdc:_txn"
)

const (
	defaultBackoffBase    = 5 * time.Second
	defaultBackoffCap     = 60 * time.Second
	standbyStatusInterval = 10 * time.Second
)

// Detector implements detector.Detector using PostgreSQL WAL logical replication.
// It uses the pgoutput plugin to decode changes and emits events for INSERT,
// UPDATE, and DELETE operations. No triggers required — changes are captured
// directly from the write-ahead log.
type Detector struct {
	dbURL       string
	publication string
	backoffBase time.Duration
	backoffCap  time.Duration
	txMetadata  bool
	txMarkers   bool
	logger      *slog.Logger

	// snapshot-first fields: when snapshotTable is set, the detector runs a
	// table snapshot using the replication slot's exported snapshot before
	// transitioning to live WAL streaming.
	snapshotTable     string
	snapshotWhere     string
	snapshotBatchSize int
	snapshotDone      bool

	// Persistent slot fields: when persistentSlot is true, the detector uses
	// a named, non-temporary replication slot that survives disconnects.
	persistentSlot  bool
	slotName        string
	checkpointStore checkpoint.Store

	// Schema inclusion: when includeSchema is true, events include column
	// type metadata from RelationMessages.
	includeSchema bool

	// Schema events: when schemaEvents is true, the detector emits
	// SCHEMA_CHANGE events when relation metadata changes.
	schemaEvents bool

	// Heartbeat fields: periodic writes to keep the replication slot advancing.
	heartbeatInterval time.Duration
	heartbeatTable    string
	heartbeatDBURL    string

	// Slot lag warning threshold in bytes.
	slotLagWarn int64

	// Incremental snapshot fields.
	incrementalEnabled bool
	signalTable        string
	snapshotChunkSize  int
	snapshotChunkDelay time.Duration
	progressStore      snapshot.ProgressStore
	activeSnapshots    map[string]context.CancelFunc // table -> cancel
	snapshotMu         sync.Mutex
}

// txState tracks the current transaction when txMetadata is enabled.
type txState struct {
	xid        uint32
	commitTime time.Time
	seq        int
}

// New creates a WAL logical replication detector for the given publication.
// Duration parameters default to sensible values when zero.
// When txMetadata is true, events include transaction info (xid, commit_time, seq).
// When txMarkers is true, synthetic BEGIN/COMMIT events are emitted (implies txMetadata).
func New(dbURL string, publication string, backoffBase, backoffCap time.Duration, txMetadata, txMarkers bool, logger *slog.Logger) *Detector {
	if txMarkers {
		txMetadata = true
	}
	if backoffBase <= 0 {
		backoffBase = defaultBackoffBase
	}
	if backoffCap <= 0 {
		backoffCap = defaultBackoffCap
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Detector{
		dbURL:       dbURL,
		publication: publication,
		backoffBase: backoffBase,
		backoffCap:  backoffCap,
		txMetadata:  txMetadata,
		txMarkers:   txMarkers,
		logger:      logger.With("detector", source),
	}
}

// Name returns the detector name.
func (d *Detector) Name() string {
	return source
}

// SetSnapshotFirst configures the detector to run a table snapshot before
// starting live WAL streaming. The snapshot uses the replication slot's
// exported snapshot for zero-gap delivery.
func (d *Detector) SetSnapshotFirst(table, where string, batchSize int) {
	d.snapshotTable = table
	d.snapshotWhere = where
	d.snapshotBatchSize = batchSize
}

// SetPersistentSlot configures the detector to use a named, non-temporary
// replication slot that survives disconnects.
func (d *Detector) SetPersistentSlot(name string) {
	d.persistentSlot = true
	d.slotName = name
}

// SetCheckpointStore sets the checkpoint store for LSN persistence.
func (d *Detector) SetCheckpointStore(store checkpoint.Store) {
	d.checkpointStore = store
}

// SetIncludeSchema enables column type metadata in events.
func (d *Detector) SetIncludeSchema(include bool) {
	d.includeSchema = include
}

// SetSchemaEvents enables SCHEMA_CHANGE event emission.
func (d *Detector) SetSchemaEvents(enabled bool) {
	d.schemaEvents = enabled
}

// SetHeartbeat configures periodic writes to keep the replication slot advancing.
func (d *Detector) SetHeartbeat(interval time.Duration, table, dbURL string) {
	d.heartbeatInterval = interval
	d.heartbeatTable = table
	d.heartbeatDBURL = dbURL
}

// SetSlotLagWarn sets the slot lag warning threshold in bytes.
func (d *Detector) SetSlotLagWarn(bytes int64) {
	d.slotLagWarn = bytes
}

// SetIncrementalSnapshot enables signal-triggered incremental snapshots.
func (d *Detector) SetIncrementalSnapshot(signalTable string, chunkSize int, chunkDelay time.Duration) {
	d.incrementalEnabled = true
	d.signalTable = signalTable
	if chunkSize <= 0 {
		chunkSize = 1000
	}
	d.snapshotChunkSize = chunkSize
	d.snapshotChunkDelay = chunkDelay
	d.activeSnapshots = make(map[string]context.CancelFunc)
}

// SetProgressStore sets the progress store for incremental snapshot persistence.
func (d *Detector) SetProgressStore(store snapshot.ProgressStore) {
	d.progressStore = store
}

// Start connects to PostgreSQL, creates a temporary replication slot, and
// streams WAL changes as events. It blocks until ctx is cancelled.
// The caller owns the events channel; Start does NOT close it.
func (d *Detector) Start(ctx context.Context, events chan<- event.Event) error {
	var attempt int
	for {
		runErr := d.run(ctx, events)
		if ctx.Err() != nil {
			return ctx.Err()
		}

		disconnErr := &pgcdcerr.DetectorDisconnectedError{
			Source: source,
			Err:    runErr,
		}

		delay := backoff.Jitter(attempt, d.backoffBase, d.backoffCap)
		d.logger.Error("connection lost, reconnecting",
			"error", disconnErr,
			"attempt", attempt+1,
			"delay", delay,
		)
		attempt++

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

// run performs a single connect-replicate cycle.
func (d *Detector) run(ctx context.Context, events chan<- event.Event) error {
	connStr := ensureReplicationParam(d.dbURL)

	conn, err := pgconn.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	var slotName string
	var consistentPoint pglogrepl.LSN
	var result pglogrepl.CreateReplicationSlotResult

	if d.persistentSlot && d.slotName != "" {
		slotName = d.slotName

		// Try to load checkpoint LSN for resume.
		if d.checkpointStore != nil {
			savedLSN, loadErr := d.checkpointStore.Load(ctx, slotName)
			if loadErr != nil {
				d.logger.Warn("failed to load checkpoint, will start fresh", "error", loadErr)
			} else if savedLSN > 0 {
				consistentPoint = pglogrepl.LSN(savedLSN)
				d.logger.Info("resuming from checkpoint",
					"slot", slotName,
					"lsn", consistentPoint.String(),
				)
			}
		}

		// Create slot if it doesn't exist (persistent, non-temporary).
		result, err = pglogrepl.CreateReplicationSlot(ctx, conn, slotName, "pgoutput",
			pglogrepl.CreateReplicationSlotOptions{Temporary: false})
		if err != nil {
			// Slot already exists — expected for persistent slots on reconnect.
			if isSlotAlreadyExists(err) {
				d.logger.Info("persistent slot already exists, reusing",
					"slot", slotName,
				)
			} else {
				return fmt.Errorf("create replication slot: %w", err)
			}
		} else {
			d.logger.Info("persistent replication slot created",
				"slot", slotName,
				"consistent_point", result.ConsistentPoint,
			)
			// Only use consistent point from slot creation if we don't have a checkpoint.
			if consistentPoint == 0 {
				parsed, parseErr := pglogrepl.ParseLSN(result.ConsistentPoint)
				if parseErr != nil {
					return fmt.Errorf("parse consistent point: %w", parseErr)
				}
				consistentPoint = parsed
			}
		}
	} else {
		// Temporary slot (original behavior).
		slotName, err = randomSlotName()
		if err != nil {
			return fmt.Errorf("generate slot name: %w", err)
		}

		result, err = pglogrepl.CreateReplicationSlot(ctx, conn, slotName, "pgoutput",
			pglogrepl.CreateReplicationSlotOptions{Temporary: true})
		if err != nil {
			return fmt.Errorf("create replication slot: %w", err)
		}

		d.logger.Info("replication slot created",
			"slot", slotName,
			"consistent_point", result.ConsistentPoint,
		)

		parsed, parseErr := pglogrepl.ParseLSN(result.ConsistentPoint)
		if parseErr != nil {
			return fmt.Errorf("parse consistent point: %w", parseErr)
		}
		consistentPoint = parsed
	}

	// If snapshot-first is configured, export existing rows using the slot's
	// snapshot before starting replication. The snapshot runs on a separate
	// regular connection with SET TRANSACTION SNAPSHOT to see exactly the
	// data state at the slot's consistent point — zero gap.
	if d.snapshotTable != "" && !d.snapshotDone {
		if result.SnapshotName == "" {
			return fmt.Errorf("snapshot-first: replication slot did not export a snapshot name")
		}

		snap := snapshot.New(d.dbURL, d.snapshotTable, d.snapshotWhere, d.snapshotBatchSize, d.logger)
		snap.SetSnapshotName(result.SnapshotName)

		d.logger.Info("snapshot-first: exporting existing rows",
			"table", d.snapshotTable,
			"snapshot_name", result.SnapshotName,
		)

		if err := snap.Run(ctx, events); err != nil {
			return fmt.Errorf("snapshot-first: %w", err)
		}

		d.snapshotDone = true
		d.logger.Info("snapshot-first complete, transitioning to live WAL streaming")
	}

	// Start replication.
	err = pglogrepl.StartReplication(ctx, conn, slotName, consistentPoint,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '1'",
				fmt.Sprintf("publication_names '%s'", d.publication),
			},
		})
	if err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	d.logger.Info("replication started",
		"publication", d.publication,
		"slot", slotName,
	)

	// Start heartbeat goroutine if configured.
	if d.heartbeatInterval > 0 && d.heartbeatTable != "" {
		heartbeatCtx, heartbeatCancel := context.WithCancel(ctx)
		defer heartbeatCancel()
		go d.runHeartbeat(heartbeatCtx)
	}

	// Start slot lag monitor goroutine if persistent slot.
	if d.persistentSlot {
		go d.runSlotLagMonitor(ctx, slotName)
	}

	// Resume incomplete incremental snapshots from progress store.
	if d.incrementalEnabled && d.progressStore != nil {
		d.resumeIncompleteSnapshots(ctx, events)
	}

	// Message loop.
	clientXLogPos := consistentPoint
	nextStatusDeadline := time.Now().Add(standbyStatusInterval)
	relations := make(map[uint32]*pglogrepl.RelationMessage)
	var currentTx *txState // non-nil during a transaction when txMetadata is enabled

	for {
		if time.Now().After(nextStatusDeadline) {
			err := pglogrepl.SendStandbyStatusUpdate(ctx, conn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				return fmt.Errorf("send standby status: %w", err)
			}

			// Checkpoint after successful standby status update.
			if d.checkpointStore != nil && clientXLogPos > 0 {
				if saveErr := d.checkpointStore.Save(ctx, slotName, uint64(clientXLogPos)); saveErr != nil {
					d.logger.Warn("failed to save checkpoint", "error", saveErr)
				} else {
					metrics.CheckpointLSN.Set(float64(clientXLogPos))
					metrics.CheckpointTotal.Inc()
				}
			}

			nextStatusDeadline = time.Now().Add(standbyStatusInterval)
		}

		receiveCtx, cancel := context.WithDeadline(ctx, nextStatusDeadline)
		rawMsg, err := conn.ReceiveMessage(receiveCtx)
		cancel()
		if err != nil {
			if receiveCtx.Err() != nil && ctx.Err() == nil {
				// Deadline hit for status update, not a real error.
				continue
			}
			return fmt.Errorf("receive message: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("postgres error: %s (code %s)", errMsg.Message, errMsg.Code)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				d.logger.Warn("parse keepalive failed", "error", err)
				continue
			}
			if pkm.ReplyRequested {
				nextStatusDeadline = time.Time{} // force immediate status on next iteration
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				d.logger.Warn("parse xlog data failed", "error", err)
				continue
			}

			newPos := xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			if newPos > clientXLogPos {
				clientXLogPos = newPos
			}

			logicalMsg, err := pglogrepl.Parse(xld.WALData)
			if err != nil {
				d.logger.Warn("parse logical message failed", "error", err)
				continue
			}

			switch m := logicalMsg.(type) {
			case *pglogrepl.RelationMessage:
				if d.schemaEvents {
					if oldRel, exists := relations[m.RelationID]; exists {
						if changes := diffRelation(oldRel, m); len(changes) > 0 {
							if err := d.emitSchemaChange(ctx, events, m, changes); err != nil {
								return fmt.Errorf("emit schema change: %w", err)
							}
						}
					}
				}
				relations[m.RelationID] = m

			case *pglogrepl.BeginMessage:
				if d.txMetadata {
					currentTx = &txState{xid: m.Xid, commitTime: m.CommitTime}
				}
				if d.txMarkers {
					if err := d.emitMarker(ctx, events, "BEGIN", m.Xid, m.CommitTime, 0); err != nil {
						return err
					}
				}

			case *pglogrepl.CommitMessage:
				if d.txMarkers && currentTx != nil {
					if err := d.emitMarker(ctx, events, "COMMIT", currentTx.xid, currentTx.commitTime, currentTx.seq); err != nil {
						return err
					}
				}
				currentTx = nil

			case *pglogrepl.InsertMessage:
				if currentTx != nil {
					currentTx.seq++
				}
				if err := d.emitEvent(ctx, events, relations, m.RelationID, "INSERT", m.Tuple, nil, currentTx); err != nil {
					return err
				}
				if d.incrementalEnabled {
					if rel, ok := relations[m.RelationID]; ok && rel.RelationName == d.signalTable {
						d.handleSignal(ctx, events, rel, m.Tuple)
					}
				}

			case *pglogrepl.UpdateMessage:
				if currentTx != nil {
					currentTx.seq++
				}
				if err := d.emitEvent(ctx, events, relations, m.RelationID, "UPDATE", m.NewTuple, m.OldTuple, currentTx); err != nil {
					return err
				}

			case *pglogrepl.DeleteMessage:
				if currentTx != nil {
					currentTx.seq++
				}
				if err := d.emitEvent(ctx, events, relations, m.RelationID, "DELETE", nil, m.OldTuple, currentTx); err != nil {
					return err
				}

			case *pglogrepl.TruncateMessage, *pglogrepl.TypeMessage, *pglogrepl.OriginMessage:
				// Ignored.
			}
		}
	}
}

// emitEvent builds an event from WAL data and sends it to the events channel.
func (d *Detector) emitEvent(
	ctx context.Context,
	events chan<- event.Event,
	relations map[uint32]*pglogrepl.RelationMessage,
	relationID uint32,
	op string,
	newTuple *pglogrepl.TupleData,
	oldTuple *pglogrepl.TupleData,
	tx *txState,
) error {
	rel, ok := relations[relationID]
	if !ok {
		d.logger.Warn("unknown relation, skipping event", "relation_id", relationID)
		return nil
	}

	channel := channelName(rel)

	var row map[string]any
	if newTuple != nil {
		row = tupleToMap(rel, newTuple)
	}
	var old map[string]any
	if oldTuple != nil {
		old = tupleToMap(rel, oldTuple)
	}

	// For DELETE, "row" is the old row (matches LISTEN/NOTIFY trigger format).
	if op == "DELETE" && row == nil && old != nil {
		row = old
		old = nil
	}

	payload := map[string]any{
		"op":    op,
		"table": rel.RelationName,
		"row":   row,
		"old":   old,
	}

	if d.includeSchema {
		payload["schema"] = rel.Namespace
		columns := make([]ColumnSchema, 0, len(rel.Columns))
		for _, col := range rel.Columns {
			columns = append(columns, ColumnSchema{
				Name:     col.Name,
				TypeOID:  col.DataType,
				TypeName: TypeNameForOID(col.DataType),
			})
		}
		payload["columns"] = columns
	}

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		d.logger.Error("marshal payload failed", "error", err)
		return nil
	}

	ev, err := event.New(channel, op, payloadJSON, source)
	if err != nil {
		d.logger.Error("create event failed", "error", err)
		return nil
	}

	if tx != nil {
		ev.Transaction = &event.TransactionInfo{
			Xid:        tx.xid,
			CommitTime: tx.commitTime,
			Seq:        tx.seq,
		}
	}

	select {
	case events <- ev:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// emitMarker sends a synthetic BEGIN or COMMIT marker event.
func (d *Detector) emitMarker(ctx context.Context, events chan<- event.Event, op string, xid uint32, commitTime time.Time, eventCount int) error {
	payload := map[string]any{
		"xid":         xid,
		"commit_time": commitTime,
	}
	if op == "COMMIT" {
		payload["event_count"] = eventCount
	}

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		d.logger.Error("marshal marker payload failed", "error", err)
		return nil
	}

	ev, err := event.New(txnChannel, op, payloadJSON, source)
	if err != nil {
		d.logger.Error("create marker event failed", "error", err)
		return nil
	}

	select {
	case events <- ev:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// channelName returns the pgcdc channel name for a relation.
// For non-public schemas: "pgcdc:<schema>.<table>", otherwise "pgcdc:<table>".
func channelName(rel *pglogrepl.RelationMessage) string {
	if rel.Namespace != "" && rel.Namespace != "public" {
		return "pgcdc:" + rel.Namespace + "." + rel.RelationName
	}
	return "pgcdc:" + rel.RelationName
}

// tupleToMap converts a WAL tuple into a map keyed by column name.
func tupleToMap(rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) map[string]any {
	m := make(map[string]any, len(tuple.Columns))
	for i, col := range tuple.Columns {
		if i >= len(rel.Columns) {
			break
		}
		colName := rel.Columns[i].Name
		switch col.DataType {
		case 'n': // null
			m[colName] = nil
		case 'u': // unchanged TOAST
			m[colName] = "(unchanged)"
		case 't': // text
			m[colName] = string(col.Data)
		}
	}
	return m
}

// ensureReplicationParam appends replication=database to the connection string
// if not already present.
func ensureReplicationParam(connStr string) string {
	u, err := url.Parse(connStr)
	if err != nil {
		// If we can't parse it, just append as query param.
		if len(connStr) > 0 && connStr[len(connStr)-1] == '?' {
			return connStr + "replication=database"
		}
		return connStr + "?replication=database"
	}
	q := u.Query()
	if q.Get("replication") == "" {
		q.Set("replication", "database")
		u.RawQuery = q.Encode()
	}
	return u.String()
}

// randomSlotName generates a unique replication slot name.
func randomSlotName() (string, error) {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "pgcdc_" + hex.EncodeToString(b), nil
}

const schemaChannel = "pgcdc:_schema"

// schemaChange describes a single column-level change detected from RelationMessage diffs.
type schemaChange struct {
	Type     string `json:"type"`
	Column   string `json:"column"`
	TypeName string `json:"type_name,omitempty"`
	OldType  string `json:"old_type,omitempty"`
	NewType  string `json:"new_type,omitempty"`
}

// diffRelation compares old and new RelationMessages and returns detected changes.
func diffRelation(oldRel, newRel *pglogrepl.RelationMessage) []schemaChange {
	var changes []schemaChange

	oldCols := make(map[string]*pglogrepl.RelationMessageColumn, len(oldRel.Columns))
	for _, col := range oldRel.Columns {
		oldCols[col.Name] = col
	}

	newCols := make(map[string]*pglogrepl.RelationMessageColumn, len(newRel.Columns))
	for _, col := range newRel.Columns {
		newCols[col.Name] = col
	}

	// Check for added or changed columns.
	for name, newCol := range newCols {
		if oldCol, exists := oldCols[name]; !exists {
			changes = append(changes, schemaChange{
				Type:     "column_added",
				Column:   name,
				TypeName: TypeNameForOID(newCol.DataType),
			})
		} else if oldCol.DataType != newCol.DataType {
			changes = append(changes, schemaChange{
				Type:    "column_type_changed",
				Column:  name,
				OldType: TypeNameForOID(oldCol.DataType),
				NewType: TypeNameForOID(newCol.DataType),
			})
		}
	}

	// Check for removed columns.
	for name := range oldCols {
		if _, exists := newCols[name]; !exists {
			changes = append(changes, schemaChange{
				Type:   "column_removed",
				Column: name,
			})
		}
	}

	return changes
}

// runSlotLagMonitor runs monitorSlotLag on a ticker for the lifetime of ctx.
func (d *Detector) runSlotLagMonitor(ctx context.Context, slotName string) {
	ticker := time.NewTicker(standbyStatusInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.monitorSlotLag(ctx, slotName)
		}
	}
}

// emitSchemaChange sends a synthetic SCHEMA_CHANGE event.
func (d *Detector) emitSchemaChange(ctx context.Context, events chan<- event.Event, rel *pglogrepl.RelationMessage, changes []schemaChange) error {
	d.logger.Warn("schema change detected",
		"table", rel.RelationName,
		"schema", rel.Namespace,
		"changes", changes,
	)

	payload := map[string]any{
		"op":      "SCHEMA_CHANGE",
		"table":   rel.RelationName,
		"schema":  rel.Namespace,
		"changes": changes,
	}

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		d.logger.Error("marshal schema change payload failed", "error", err)
		return nil
	}

	ev, err := event.New(schemaChannel, "SCHEMA_CHANGE", payloadJSON, source)
	if err != nil {
		d.logger.Error("create schema change event failed", "error", err)
		return nil
	}

	select {
	case events <- ev:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// isSlotAlreadyExists checks if the error indicates the replication slot already exists.
func isSlotAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	// PostgreSQL error code 42710 = duplicate_object (slot already exists).
	var pgErr *pgconn.PgError
	if ok := errors.As(err, &pgErr); ok {
		return pgErr.Code == "42710"
	}
	return false
}

// runHeartbeat periodically updates the heartbeat table to keep the replication
// slot advancing on idle databases. Uses a separate connection.
func (d *Detector) runHeartbeat(ctx context.Context) {
	dbURL := d.heartbeatDBURL
	if dbURL == "" {
		dbURL = d.dbURL
	}

	ticker := time.NewTicker(d.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := d.heartbeatOnce(ctx, dbURL); err != nil {
				d.logger.Warn("heartbeat failed", "error", err)
			}
		}
	}
}

func (d *Detector) heartbeatOnce(ctx context.Context, dbURL string) error {
	hbCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	conn, err := pgx.Connect(hbCtx, dbURL)
	if err != nil {
		return fmt.Errorf("heartbeat connect: %w", err)
	}
	defer func() { _ = conn.Close(hbCtx) }()

	table := pgx.Identifier{d.heartbeatTable}.Sanitize()

	// Auto-create heartbeat table.
	_, err = conn.Exec(hbCtx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INTEGER PRIMARY KEY DEFAULT 1,
			last_beat TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`, table))
	if err != nil {
		return fmt.Errorf("create heartbeat table: %w", err)
	}

	// Upsert heartbeat row.
	_, err = conn.Exec(hbCtx, fmt.Sprintf(`
		INSERT INTO %s (id, last_beat) VALUES (1, now())
		ON CONFLICT (id) DO UPDATE SET last_beat = now()
	`, table))
	if err != nil {
		return fmt.Errorf("heartbeat upsert: %w", err)
	}

	return nil
}

// handleSignal parses a signal table INSERT and dispatches the appropriate action.
func (d *Detector) handleSignal(ctx context.Context, events chan<- event.Event, rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) {
	row := tupleToMap(rel, tuple)

	signal, _ := row["signal"].(string)
	payloadStr, _ := row["payload"].(string)

	var payload map[string]any
	if payloadStr != "" {
		if err := json.Unmarshal([]byte(payloadStr), &payload); err != nil {
			d.logger.Warn("failed to parse signal payload", "signal", signal, "error", err)
			return
		}
	}

	table, _ := payload["table"].(string)
	if table == "" {
		d.logger.Warn("signal missing table in payload", "signal", signal)
		return
	}

	switch signal {
	case "execute-snapshot":
		snapshotID, _ := payload["snapshot_id"].(string)
		if snapshotID != "" {
			d.startIncrementalSnapshotWithID(ctx, events, table, snapshotID)
		} else {
			d.startIncrementalSnapshot(ctx, events, table)
		}
	case "stop-snapshot":
		d.stopIncrementalSnapshot(table)
	default:
		d.logger.Warn("unknown signal type", "signal", signal)
	}
}

// startIncrementalSnapshot starts a new incremental snapshot goroutine for the given table.
func (d *Detector) startIncrementalSnapshot(ctx context.Context, events chan<- event.Event, table string) {
	id, err := uuid.NewV7()
	if err != nil {
		d.logger.Error("generate snapshot id", "error", err)
		return
	}
	d.startIncrementalSnapshotWithID(ctx, events, table, id.String())
}

// startIncrementalSnapshotWithID starts an incremental snapshot with a specific ID.
func (d *Detector) startIncrementalSnapshotWithID(ctx context.Context, events chan<- event.Event, table, snapshotID string) {
	d.snapshotMu.Lock()
	if _, active := d.activeSnapshots[table]; active {
		d.snapshotMu.Unlock()
		d.logger.Warn("incremental snapshot already active for table, ignoring", "table", table)
		return
	}

	snapCtx, cancel := context.WithCancel(ctx)
	d.activeSnapshots[table] = cancel
	d.snapshotMu.Unlock()

	d.logger.Info("starting incremental snapshot",
		"table", table,
		"snapshot_id", snapshotID,
	)

	go func() {
		defer func() {
			d.snapshotMu.Lock()
			delete(d.activeSnapshots, table)
			d.snapshotMu.Unlock()
		}()

		snap := snapshot.NewIncremental(d.dbURL, table, d.snapshotChunkSize, d.snapshotChunkDelay, d.progressStore, snapshotID, d.logger)
		if err := snap.Run(snapCtx, events); err != nil {
			if snapCtx.Err() == nil {
				d.logger.Error("incremental snapshot failed",
					"table", table,
					"snapshot_id", snapshotID,
					"error", &pgcdcerr.IncrementalSnapshotError{
						SnapshotID: snapshotID,
						Table:      table,
						Err:        err,
					},
				)
			}
		}
	}()
}

// stopIncrementalSnapshot cancels a running incremental snapshot for the given table.
func (d *Detector) stopIncrementalSnapshot(table string) {
	d.snapshotMu.Lock()
	cancel, ok := d.activeSnapshots[table]
	d.snapshotMu.Unlock()

	if !ok {
		d.logger.Warn("no active snapshot to stop", "table", table)
		return
	}

	d.logger.Info("stopping incremental snapshot", "table", table)
	cancel()
}

// resumeIncompleteSnapshots restarts any snapshots that were running or paused
// when the detector last shut down.
func (d *Detector) resumeIncompleteSnapshots(ctx context.Context, events chan<- event.Event) {
	for _, status := range []snapshot.SnapshotStatus{snapshot.StatusRunning, snapshot.StatusPaused} {
		records, err := d.progressStore.List(ctx, &status)
		if err != nil {
			d.logger.Warn("failed to list incomplete snapshots", "status", status, "error", err)
			continue
		}
		for _, rec := range records {
			d.logger.Info("resuming incomplete snapshot",
				"snapshot_id", rec.SnapshotID,
				"table", rec.TableName,
				"status", rec.Status,
				"rows_processed", rec.RowsProcessed,
			)
			d.startIncrementalSnapshotWithID(ctx, events, rec.TableName, rec.SnapshotID)
		}
	}
}

// monitorSlotLag queries the replication slot lag and updates metrics.
func (d *Detector) monitorSlotLag(ctx context.Context, slotName string) {
	dbURL := d.heartbeatDBURL
	if dbURL == "" {
		dbURL = d.dbURL
	}

	lagCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	conn, err := pgx.Connect(lagCtx, dbURL)
	if err != nil {
		d.logger.Debug("slot lag monitor connect failed", "error", err)
		return
	}
	defer func() { _ = conn.Close(lagCtx) }()

	var lagBytes *int64
	err = conn.QueryRow(lagCtx,
		"SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) FROM pg_replication_slots WHERE slot_name = $1",
		slotName,
	).Scan(&lagBytes)
	if err != nil || lagBytes == nil {
		return
	}

	metrics.SlotLagBytes.Set(float64(*lagBytes))

	if d.slotLagWarn > 0 && *lagBytes > d.slotLagWarn {
		d.logger.Warn("replication slot lag exceeds threshold",
			"slot", slotName,
			"lag_bytes", *lagBytes,
			"threshold", d.slotLagWarn,
		)
	}
}
