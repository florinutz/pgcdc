package walreplication

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/florinutz/pgpipe/event"
	"github.com/florinutz/pgpipe/internal/backoff"
	"github.com/florinutz/pgpipe/pgpipeerr"
)

const (
	source     = "wal_replication"
	txnChannel = "pgpipe:_txn"
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

		disconnErr := &pgpipeerr.DetectorDisconnectedError{
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

	// Generate a unique slot name.
	slotName, err := randomSlotName()
	if err != nil {
		return fmt.Errorf("generate slot name: %w", err)
	}

	// Create a temporary replication slot (auto-dropped on disconnect).
	result, err := pglogrepl.CreateReplicationSlot(ctx, conn, slotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Temporary: true})
	if err != nil {
		return fmt.Errorf("create replication slot: %w", err)
	}

	d.logger.Info("replication slot created",
		"slot", slotName,
		"consistent_point", result.ConsistentPoint,
	)

	consistentPoint, err := pglogrepl.ParseLSN(result.ConsistentPoint)
	if err != nil {
		return fmt.Errorf("parse consistent point: %w", err)
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

// channelName returns the pgpipe channel name for a relation.
// For non-public schemas: "pgpipe:<schema>.<table>", otherwise "pgpipe:<table>".
func channelName(rel *pglogrepl.RelationMessage) string {
	if rel.Namespace != "" && rel.Namespace != "public" {
		return "pgpipe:" + rel.Namespace + "." + rel.RelationName
	}
	return "pgpipe:" + rel.RelationName
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
	return "pgpipe_" + hex.EncodeToString(b), nil
}
