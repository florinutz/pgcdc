package transform

import "github.com/florinutz/pgcdc/event"

// RenameFields returns a transform that renames columns in the event's row
// data according to the mapping (old → new). For structured records, operates
// directly on Change.Before/After. Missing source keys are ignored. Existing
// target keys are overwritten.
func RenameFields(mapping map[string]string) TransformFunc {
	return func(ev event.Event) (event.Event, error) {
		if ev.HasRecord() {
			return withRecordData(ev, func(sd *event.StructuredData) {
				for old, new_ := range mapping {
					val, ok := sd.Get(old)
					if !ok {
						continue
					}
					sd.Delete(old)
					sd.Set(new_, val)
				}
			})
		}
		return withPayload(ev, func(m map[string]any) {
			for old, new_ := range mapping {
				val, ok := m[old]
				if !ok {
					continue
				}
				delete(m, old)
				m[new_] = val
			}
		})
	}
}
