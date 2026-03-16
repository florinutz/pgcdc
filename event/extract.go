package event

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ExtractedRow holds the result of extracting structured data from an event.
type ExtractedRow struct {
	Row            map[string]any
	Old            map[string]any // non-nil for UPDATE/DELETE with Before image
	ID             string
	Table          string
	IsDelete       bool
	UnchangedToast []string
}

// ExtractRow extracts structured row data from an event using the Record path
// (zero JSON parsing) with fallback to legacy JSON payload parsing.
// idColumn is the field name used to extract the row's primary key.
func ExtractRow(ev Event, idColumn string) ExtractedRow {
	// Structured record path: zero JSON parsing.
	if rec := ev.Record(); rec != nil && rec.Operation != 0 &&
		(rec.Change.After != nil || rec.Change.Before != nil) {
		var result ExtractedRow
		result.IsDelete = rec.Operation == OperationDelete
		result.Table = rec.Metadata[MetaTable]

		if rec.Change.After != nil {
			result.Row = rec.Change.After.ToMap()
		}
		if rec.Change.Before != nil {
			result.Old = rec.Change.Before.ToMap()
		}

		// For DELETE, primary row data is in Before.
		if result.IsDelete && result.Row == nil {
			result.Row = result.Old
			result.Old = nil
		}

		if result.Row != nil {
			result.ID = extractID(result.Row, idColumn)
		}

		// Unchanged TOAST columns from metadata.
		if toastCSV, ok := rec.Metadata[MetaUnchangedToastCols]; ok && toastCSV != "" {
			result.UnchangedToast = strings.Split(toastCSV, ",")
		}

		return result
	}

	// Legacy path: parse payload JSON.
	var p struct {
		Op             string         `json:"op"`
		Table          string         `json:"table"`
		Row            map[string]any `json:"row"`
		Old            map[string]any `json:"old"`
		UnchangedToast []string       `json:"_unchanged_toast_columns"`
	}
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		return ExtractedRow{}
	}

	result := ExtractedRow{
		Row:            p.Row,
		Old:            p.Old,
		Table:          p.Table,
		IsDelete:       p.Op == "DELETE",
		UnchangedToast: p.UnchangedToast,
	}

	if result.Row != nil {
		result.ID = extractID(result.Row, idColumn)
	}

	return result
}

// extractID extracts a string ID from a row map.
func extractID(row map[string]any, idColumn string) string {
	v, ok := row[idColumn]
	if !ok || v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}
