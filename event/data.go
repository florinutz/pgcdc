package event

import "encoding/json"

// Well-known metadata keys.
const (
	MetaTable              = "pgcdc.table"
	MetaSchema             = "pgcdc.schema"
	MetaUnchangedToastCols = "pgcdc.unchanged_toast_columns"
	MetaSnapshotID         = "pgcdc.snapshot_id"
	MetaDatabase           = "pgcdc.database"
	MetaCollection         = "pgcdc.collection"
	MetaSchemaSubject      = "pgcdc.schema_subject"
	MetaSchemaVersion      = "pgcdc.schema_version"
)

// Field is a named value in a StructuredData record. Fields preserve
// insertion order, which matches column order from the source database.
type Field struct {
	Name  string
	Value any
}

// StructuredData is an ordered collection of named fields with lazy O(1)
// access by name. It wraps []Field with a lazily-built index map.
type StructuredData struct {
	fields []Field
	index  map[string]int // lazily built on first lookup
}

// NewStructuredData creates a StructuredData from an ordered field list.
func NewStructuredData(fields []Field) *StructuredData {
	return &StructuredData{fields: fields}
}

// NewStructuredDataFromMap creates a StructuredData from a map.
// Field order is non-deterministic (map iteration order).
// Returns nil if m is nil.
func NewStructuredDataFromMap(m map[string]any) *StructuredData {
	if m == nil {
		return nil
	}
	fields := make([]Field, 0, len(m))
	for k, v := range m {
		fields = append(fields, Field{Name: k, Value: v})
	}
	return &StructuredData{fields: fields}
}

// buildIndex lazily constructs the field-name → index map.
func (sd *StructuredData) buildIndex() {
	if sd.index != nil {
		return
	}
	sd.index = make(map[string]int, len(sd.fields))
	for i, f := range sd.fields {
		sd.index[f.Name] = i
	}
}

// Get returns the value of the named field and whether it exists.
func (sd *StructuredData) Get(name string) (any, bool) {
	sd.buildIndex()
	i, ok := sd.index[name]
	if !ok {
		return nil, false
	}
	return sd.fields[i].Value, true
}

// Set sets the value of the named field. If the field exists, its value is
// updated in place. If not, a new field is appended.
func (sd *StructuredData) Set(name string, value any) {
	sd.buildIndex()
	if i, ok := sd.index[name]; ok {
		sd.fields[i].Value = value
		return
	}
	sd.index[name] = len(sd.fields)
	sd.fields = append(sd.fields, Field{Name: name, Value: value})
}

// Delete removes the named field. Returns true if the field existed.
func (sd *StructuredData) Delete(name string) bool {
	sd.buildIndex()
	i, ok := sd.index[name]
	if !ok {
		return false
	}
	sd.fields = append(sd.fields[:i], sd.fields[i+1:]...)
	// Rebuild index after shift.
	sd.index = nil
	return true
}

// Fields returns a copy of the ordered field list.
func (sd *StructuredData) Fields() []Field {
	out := make([]Field, len(sd.fields))
	copy(out, sd.fields)
	return out
}

// Len returns the number of fields.
func (sd *StructuredData) Len() int {
	return len(sd.fields)
}

// Has reports whether a field with the given name exists.
func (sd *StructuredData) Has(name string) bool {
	sd.buildIndex()
	_, ok := sd.index[name]
	return ok
}

// ToMap copies all fields into a new map[string]any.
func (sd *StructuredData) ToMap() map[string]any {
	m := make(map[string]any, len(sd.fields))
	for _, f := range sd.fields {
		m[f.Name] = f.Value
	}
	return m
}

// MarshalJSON serializes StructuredData as a JSON object preserving
// field order.
func (sd *StructuredData) MarshalJSON() ([]byte, error) {
	return json.Marshal(sd.ToMap())
}

// Change holds the before and after states of a database record.
// For INSERT: Before is nil, After is the new row.
// For UPDATE: Before is the old row, After is the new row.
// For DELETE: Before is the old row, After is nil.
// For TRUNCATE: both are nil.
type Change struct {
	Before *StructuredData
	After  *StructuredData
}

// Metadata is a typed string map for event metadata. Well-known keys are
// defined as Meta* constants.
type Metadata map[string]string

// ColumnInfo describes a single column's type information.
type ColumnInfo struct {
	Name     string `json:"name"`
	TypeOID  uint32 `json:"type_oid"`
	TypeName string `json:"type_name"`
}

// SchemaInfo holds column type metadata for a database table.
type SchemaInfo struct {
	Columns []ColumnInfo
}
