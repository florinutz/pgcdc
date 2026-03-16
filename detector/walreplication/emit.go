package walreplication

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/florinutz/pgcdc/detector/walreplication/toastcache"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/schema"
)

// emitEvent builds an event from WAL data and sends it to the events channel.
// currentLSN is the WAL position at which this change was received; it is
// stored on the event for cooperative checkpointing. tc may be nil.
// channel is the pre-computed channel name from channelNames cache.
func (d *Detector) emitEvent(
	ctx context.Context,
	events chan<- event.Event,
	relations map[uint32]*pglogrepl.RelationMessage,
	relationID uint32,
	channel string,
	op string,
	newTuple *pglogrepl.TupleData,
	oldTuple *pglogrepl.TupleData,
	tx *txState,
	currentLSN pglogrepl.LSN,
	tc *toastcache.Cache,
) error {
	rel, ok := relations[relationID]
	if !ok {
		d.logger.Warn("unknown relation, skipping event", "relation_id", relationID)
		return nil
	}

	var newRow map[string]any
	var unchangedCols []string
	if newTuple != nil {
		newRow, unchangedCols = tupleToMap(rel, newTuple)
	}
	var oldRow map[string]any
	if oldTuple != nil {
		oldRow, _ = tupleToMap(rel, oldTuple)
	}

	// Build structured Record with proper Before/After semantics.
	rec := &event.Record{
		Position:  event.NewPosition(uint64(currentLSN)),
		Operation: event.ParseOperation(op),
		Metadata:  event.Metadata{event.MetaTable: rel.RelationName},
	}

	// Map WAL tuples to Change.Before/After based on operation.
	switch op {
	case "DELETE":
		// DELETE: old tuple is the deleted row. No new tuple.
		if oldRow != nil {
			rec.Change.Before = event.NewStructuredDataFromMap(oldRow)
		} else if newRow != nil {
			// Fallback: some configs may not have old tuple.
			rec.Change.Before = event.NewStructuredDataFromMap(newRow)
		}
	case "UPDATE":
		if newRow != nil {
			rec.Change.After = event.NewStructuredDataFromMap(newRow)
		}
		if oldRow != nil {
			rec.Change.Before = event.NewStructuredDataFromMap(oldRow)
		}
	default:
		// INSERT, TRUNCATE
		if newRow != nil {
			rec.Change.After = event.NewStructuredDataFromMap(newRow)
		}
	}

	// For TOAST cache operations, we need the "row" map (the primary data map).
	// For DELETE, "row" is the old data; for others, "row" is the new data.
	var row map[string]any
	if op == "DELETE" {
		if oldRow != nil {
			row = oldRow
		} else {
			row = newRow
		}
	} else {
		row = newRow
	}

	// TOAST cache: backfill unchanged columns from cache, then update cache.
	if tc != nil && row != nil {
		pk := toastcache.BuildPK(rel, row)

		switch op {
		case "INSERT":
			if pk != "" {
				tc.Put(relationID, pk, copyMap(row))
			}
			unchangedCols = nil

		case "UPDATE":
			if len(unchangedCols) > 0 && pk != "" {
				if cached, ok := tc.Get(relationID, pk); ok {
					for _, col := range unchangedCols {
						row[col] = cached[col]
					}
					// Update the Record's After with backfilled data.
					rec.Change.After = event.NewStructuredDataFromMap(row)
					unchangedCols = nil
					metrics.ToastCacheHits.Inc()
				} else {
					metrics.ToastCacheMisses.Inc()
				}
			}
			if pk != "" {
				tc.Put(relationID, pk, copyMap(row))
			}

		case "DELETE":
			if pk != "" {
				tc.Delete(relationID, pk)
			}
		}
	}

	// Store unchanged TOAST columns in metadata.
	if len(unchangedCols) > 0 {
		data, _ := json.Marshal(unchangedCols)
		rec.Metadata[event.MetaUnchangedToastCols] = string(data)
	}

	// Schema info.
	if d.includeSchema {
		rec.Metadata[event.MetaSchema] = rel.Namespace
		columns := make([]event.ColumnInfo, 0, len(rel.Columns))
		for _, col := range rel.Columns {
			columns = append(columns, event.ColumnInfo{
				Name:     col.Name,
				TypeOID:  col.DataType,
				TypeName: TypeNameForOID(col.DataType),
			})
		}
		rec.Schema = &event.SchemaInfo{Columns: columns}
	}

	// Schema registry: auto-register schema versions from RelationMessages.
	if d.schemaStore != nil {
		schemaName := rel.Namespace
		if schemaName == "" {
			schemaName = "public"
		}
		subject := schema.SubjectName(schemaName, rel.RelationName)
		cols := make([]schema.ColumnDef, len(rel.Columns))
		for i, col := range rel.Columns {
			cols[i] = schema.ColumnDef{
				Name:     col.Name,
				TypeOID:  col.DataType,
				TypeName: TypeNameForOID(col.DataType),
			}
		}
		if s, created, err := d.schemaStore.Register(ctx, subject, cols); err != nil {
			d.logger.Warn("schema registration failed",
				"subject", subject,
				"error", err,
			)
		} else {
			rec.Metadata[event.MetaSchemaSubject] = s.Subject
			rec.Metadata[event.MetaSchemaVersion] = fmt.Sprintf("%d", s.Version)
			if created {
				d.logger.Info("schema registered",
					"subject", s.Subject,
					"version", s.Version,
				)
			}
		}
	}

	ev, err := event.NewFromRecord(channel, rec, source)
	if err != nil {
		d.logger.Error("create event failed", "error", err)
		return err
	}

	if tx != nil {
		ev.Transaction = &event.TransactionInfo{
			Xid:        tx.xid,
			CommitTime: tx.commitTime,
			Seq:        tx.seq,
		}
	}

	if d.tracer != nil {
		_, span := d.tracer.Start(ctx, "pgcdc.detect",
			trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(
				attribute.String("pgcdc.event.id", ev.ID),
				attribute.String("pgcdc.channel", ev.Channel),
				attribute.String("pgcdc.operation", op),
				attribute.String("pgcdc.source", source),
				attribute.Int64("pgcdc.lsn", int64(currentLSN)),
				attribute.String("pgcdc.table", rel.RelationName),
			),
		)
		ev.SpanContext = span.SpanContext()
		span.End()
	}

	select {
	case events <- ev:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// emitMarker sends a synthetic BEGIN or COMMIT marker event.
// currentLSN is stored on the event for cooperative checkpointing.
func (d *Detector) emitMarker(ctx context.Context, events chan<- event.Event, op string, xid uint32, commitTime time.Time, eventCount int, currentLSN pglogrepl.LSN) error {
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
		return err
	}

	ev, err := event.New(txnChannel, op, payloadJSON, source)
	if err != nil {
		d.logger.Error("create marker event failed", "error", err)
		return err
	}

	ev.LSN = uint64(currentLSN)

	if d.tracer != nil {
		_, span := d.tracer.Start(ctx, "pgcdc.detect",
			trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(
				attribute.String("pgcdc.event.id", ev.ID),
				attribute.String("pgcdc.channel", txnChannel),
				attribute.String("pgcdc.operation", op),
				attribute.String("pgcdc.source", source),
				attribute.Int64("pgcdc.lsn", int64(currentLSN)),
			),
		)
		ev.SpanContext = span.SpanContext()
		span.End()
	}

	select {
	case events <- ev:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// emitSchemaChange sends a synthetic SCHEMA_CHANGE event.
// currentLSN is stored on the event for cooperative checkpointing.
func (d *Detector) emitSchemaChange(ctx context.Context, events chan<- event.Event, rel *pglogrepl.RelationMessage, changes []schemaChange, currentLSN pglogrepl.LSN) error {
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
		return err
	}

	ev, err := event.New(schemaChannel, "SCHEMA_CHANGE", payloadJSON, source)
	if err != nil {
		d.logger.Error("create schema change event failed", "error", err)
		return err
	}

	ev.LSN = uint64(currentLSN)

	if d.tracer != nil {
		_, span := d.tracer.Start(ctx, "pgcdc.detect",
			trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(
				attribute.String("pgcdc.event.id", ev.ID),
				attribute.String("pgcdc.channel", schemaChannel),
				attribute.String("pgcdc.operation", "SCHEMA_CHANGE"),
				attribute.String("pgcdc.source", source),
				attribute.Int64("pgcdc.lsn", int64(currentLSN)),
				attribute.String("pgcdc.table", rel.RelationName),
			),
		)
		ev.SpanContext = span.SpanContext()
		span.End()
	}

	select {
	case events <- ev:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
