package event

import "encoding/json"

// Record is a structured representation of a CDC change event. It replaces
// the opaque json.RawMessage payload with typed fields for direct access to
// change data, position, metadata, and schema information.
type Record struct {
	Position  Position
	Operation Operation
	Key       *StructuredData
	Change    Change
	Metadata  Metadata
	Schema    *SchemaInfo

	// extras holds source-specific fields that don't map to standard Record
	// fields (e.g., MongoDB's document_key, update_description). These are
	// preserved for MarshalCompatPayload round-trip fidelity.
	extras map[string]any
}

// SetExtra stores a source-specific key-value pair that will be included
// in the legacy payload produced by MarshalCompatPayload.
func (r *Record) SetExtra(key string, value any) {
	if r.extras == nil {
		r.extras = make(map[string]any)
	}
	r.extras[key] = value
}

// Extra returns a source-specific value by key.
func (r *Record) Extra(key string) (any, bool) {
	if r.extras == nil {
		return nil, false
	}
	v, ok := r.extras[key]
	return v, ok
}

// MarshalCompatPayload produces the legacy JSON payload format:
//
//	{"op": "INSERT", "table": "...", "row": {...}, "old": {...}}
//
// This ensures json.Marshal(event) produces identical output to the
// pre-structured-records format.
func (r *Record) MarshalCompatPayload() json.RawMessage {
	payload := make(map[string]any, 8)

	// Start with extras so standard fields can override.
	for k, v := range r.extras {
		payload[k] = v
	}

	payload["op"] = r.Operation.String()

	// Table metadata.
	if table, ok := r.Metadata[MetaTable]; ok {
		payload["table"] = table
	}
	if schema, ok := r.Metadata[MetaSchema]; ok {
		payload["schema"] = schema
	}

	// MongoDB metadata.
	if db, ok := r.Metadata[MetaDatabase]; ok {
		payload["database"] = db
	}
	if coll, ok := r.Metadata[MetaCollection]; ok {
		payload["collection"] = coll
	}

	// Map structured Change to legacy row/old fields.
	// DELETE legacy format: row = old data, old = nil (matches LISTEN/NOTIFY).
	switch r.Operation {
	case OperationDelete:
		if r.Change.Before != nil {
			payload["row"] = r.Change.Before.ToMap()
		} else {
			payload["row"] = nil
		}
		payload["old"] = nil
	default:
		if r.Change.After != nil {
			payload["row"] = r.Change.After.ToMap()
		} else {
			payload["row"] = nil
		}
		if r.Change.Before != nil {
			payload["old"] = r.Change.Before.ToMap()
		} else {
			payload["old"] = nil
		}
	}

	// Unchanged TOAST columns.
	if cols, ok := r.Metadata[MetaUnchangedToastCols]; ok && cols != "" {
		var colList []string
		if json.Unmarshal([]byte(cols), &colList) == nil {
			payload["_unchanged_toast_columns"] = colList
		}
	}

	// Schema info.
	if r.Schema != nil {
		columns := make([]map[string]any, len(r.Schema.Columns))
		for i, col := range r.Schema.Columns {
			columns[i] = map[string]any{
				"name":      col.Name,
				"type_oid":  col.TypeOID,
				"type_name": col.TypeName,
			}
		}
		payload["columns"] = columns
	}

	// Snapshot ID.
	if id, ok := r.Metadata[MetaSnapshotID]; ok {
		payload["snapshot_id"] = id
	}

	// Key as document_key (MongoDB).
	if r.Key != nil {
		if _, hasDocKey := payload["document_key"]; !hasDocKey {
			payload["document_key"] = r.Key.ToMap()
		}
	}

	data, _ := json.Marshal(payload)
	return data
}

// parseRecordFromEvent creates a Record by parsing an Event's legacy payload.
// Returns nil if the payload is empty or cannot be parsed as a JSON object.
func parseRecordFromEvent(ev *Event) *Record {
	if len(ev.Payload) == 0 {
		return nil
	}

	var payload map[string]any
	if err := json.Unmarshal(ev.Payload, &payload); err != nil {
		return nil
	}

	r := &Record{
		Position: NewPosition(ev.LSN),
		Metadata: make(Metadata),
	}
	r.Operation = ParseOperation(ev.Operation)

	// Known top-level keys that map to Record fields.
	knownKeys := map[string]bool{
		"op": true, "table": true, "schema": true,
		"row": true, "old": true,
		"database": true, "collection": true,
		"_unchanged_toast_columns": true,
		"columns":                  true, "snapshot_id": true,
		"document_key": true, "update_description": true,
	}

	// Extract standard metadata.
	if table, ok := payload["table"].(string); ok {
		r.Metadata[MetaTable] = table
	}
	if schema, ok := payload["schema"].(string); ok {
		r.Metadata[MetaSchema] = schema
	}
	if db, ok := payload["database"].(string); ok {
		r.Metadata[MetaDatabase] = db
	}
	if coll, ok := payload["collection"].(string); ok {
		r.Metadata[MetaCollection] = coll
	}
	if id, ok := payload["snapshot_id"].(string); ok {
		r.Metadata[MetaSnapshotID] = id
	}

	// Parse row and old → Change.
	var rowData, oldData *StructuredData
	if row, ok := payload["row"].(map[string]any); ok {
		rowData = NewStructuredDataFromMap(row)
	}
	if old, ok := payload["old"].(map[string]any); ok {
		oldData = NewStructuredDataFromMap(old)
	}

	// Map legacy row/old to structured Before/After.
	// DELETE legacy format: row = old data, old = nil → Before = row, After = nil.
	switch r.Operation {
	case OperationDelete:
		r.Change.Before = rowData
		r.Change.After = nil
	default:
		r.Change.After = rowData
		r.Change.Before = oldData
	}

	// Unchanged TOAST columns.
	if cols, ok := payload["_unchanged_toast_columns"].([]any); ok {
		var colNames []string
		for _, c := range cols {
			if s, ok := c.(string); ok {
				colNames = append(colNames, s)
			}
		}
		if len(colNames) > 0 {
			data, _ := json.Marshal(colNames)
			r.Metadata[MetaUnchangedToastCols] = string(data)
		}
	}

	// Schema info from columns array.
	if cols, ok := payload["columns"].([]any); ok {
		var columns []ColumnInfo
		for _, c := range cols {
			cm, ok := c.(map[string]any)
			if !ok {
				continue
			}
			ci := ColumnInfo{}
			if name, ok := cm["name"].(string); ok {
				ci.Name = name
			}
			if oid, ok := cm["type_oid"].(float64); ok {
				ci.TypeOID = uint32(oid)
			}
			if tn, ok := cm["type_name"].(string); ok {
				ci.TypeName = tn
			}
			columns = append(columns, ci)
		}
		if len(columns) > 0 {
			r.Schema = &SchemaInfo{Columns: columns}
		}
	}

	// Document key (MongoDB) → Key.
	if dk, ok := payload["document_key"].(map[string]any); ok {
		r.Key = NewStructuredDataFromMap(dk)
	}

	// Preserve unknown fields as extras.
	for k, v := range payload {
		if !knownKeys[k] {
			r.SetExtra(k, v)
		}
	}

	// Preserve known source-specific fields as extras for round-trip.
	if ud, ok := payload["update_description"]; ok {
		r.SetExtra("update_description", ud)
	}

	return r
}
