package iceberg

// rawSchema returns the fixed 6-column schema for raw mode.
// All CDC metadata + payload as a JSON string.
func rawSchema() *Schema {
	return &Schema{
		SchemaID: 0,
		Fields: []Field{
			{ID: 1, Name: "event_id", Type: "string", Required: true, Doc: "pgcdc event UUIDv7"},
			{ID: 2, Name: "operation", Type: "string", Required: true, Doc: "INSERT, UPDATE, DELETE, or SNAPSHOT"},
			{ID: 3, Name: "timestamp", Type: "timestamptz", Required: true, Doc: "event creation time"},
			{ID: 4, Name: "channel", Type: "string", Required: true, Doc: "pgcdc channel name"},
			{ID: 5, Name: "source", Type: "string", Required: true, Doc: "event source (listen_notify, wal, snapshot)"},
			{ID: 6, Name: "payload", Type: "string", Required: true, Doc: "JSON payload"},
		},
	}
}

// lastFieldID returns the highest field ID in the schema.
func lastFieldID(s *Schema) int {
	max := 0
	for _, f := range s.Fields {
		if f.ID > max {
			max = f.ID
		}
	}
	return max
}
