package walreplication

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/url"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/metrics"
)

const schemaChannel = event.SchemaChangeChannel

// schemaChange describes a single column-level change detected from RelationMessage diffs.
type schemaChange struct {
	Type     string `json:"type"`
	Column   string `json:"column"`
	TypeName string `json:"type_name,omitempty"`
	OldType  string `json:"old_type,omitempty"`
	NewType  string `json:"new_type,omitempty"`
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
// It returns the map and a list of column names that were unchanged TOAST
// columns (DataType 'u'). These columns are set to nil in the map.
func tupleToMap(rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) (map[string]any, []string) {
	m := make(map[string]any, len(tuple.Columns))
	var unchanged []string
	for i, col := range tuple.Columns {
		if i >= len(rel.Columns) {
			break
		}
		colName := rel.Columns[i].Name
		switch col.DataType {
		case 'n': // null
			m[colName] = nil
		case 'u': // unchanged TOAST
			m[colName] = nil
			unchanged = append(unchanged, colName)
		case 't': // text
			m[colName] = string(col.Data)
		case 'b': // binary
			m[colName] = string(col.Data)
		}
	}
	return m, unchanged
}

// copyMap creates a shallow copy of the map.
func copyMap(m map[string]any) map[string]any {
	cp := make(map[string]any, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
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
	d.lastLagBytes.Store(*lagBytes)

	if d.slotLagWarn > 0 && *lagBytes > d.slotLagWarn {
		d.logger.Warn("replication slot lag exceeds threshold",
			"slot", slotName,
			"lag_bytes", *lagBytes,
			"threshold", d.slotLagWarn,
		)
	}
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
