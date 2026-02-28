package transform

import "github.com/florinutz/pgcdc/event"

// DropColumns returns a transform that deletes the named columns from the
// event's row data. For structured records, operates directly on
// Change.Before/After. For legacy events, operates on the payload map.
// Missing keys are silently ignored.
func DropColumns(columns ...string) TransformFunc {
	return func(ev event.Event) (event.Event, error) {
		if ev.HasRecord() {
			return withRecordData(ev, func(sd *event.StructuredData) {
				for _, col := range columns {
					sd.Delete(col)
				}
			})
		}
		return withPayload(ev, func(m map[string]any) {
			for _, col := range columns {
				delete(m, col)
			}
		})
	}
}
