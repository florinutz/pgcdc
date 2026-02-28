package transform

import (
	"crypto/sha256"
	"fmt"
	"strings"

	"github.com/florinutz/pgcdc/event"
)

// MaskMode controls how a field value is masked.
type MaskMode string

const (
	MaskHash    MaskMode = "hash"    // SHA-256 hex digest
	MaskRedact  MaskMode = "redact"  // ***REDACTED***
	MaskPartial MaskMode = "partial" // keep first/last char, asterisks in middle
)

// MaskField pairs a payload field name with a masking mode.
type MaskField struct {
	Field string
	Mode  MaskMode
}

// Mask returns a transform that masks the specified columns in the event's
// row data. For structured records, operates directly on Change.Before/After.
// Missing fields are silently ignored. Values are converted to string via
// fmt.Sprintf before masking.
func Mask(fields ...MaskField) TransformFunc {
	maskValue := func(val any, mode MaskMode) any {
		s := fmt.Sprintf("%v", val)
		switch mode {
		case MaskHash:
			h := sha256.Sum256([]byte(s))
			return fmt.Sprintf("%x", h)
		case MaskRedact:
			return "***REDACTED***"
		case MaskPartial:
			return partialMask(s)
		default:
			return "***REDACTED***"
		}
	}

	return func(ev event.Event) (event.Event, error) {
		if ev.HasRecord() {
			return withRecordData(ev, func(sd *event.StructuredData) {
				for _, f := range fields {
					val, ok := sd.Get(f.Field)
					if !ok {
						continue
					}
					sd.Set(f.Field, maskValue(val, f.Mode))
				}
			})
		}
		return withPayload(ev, func(m map[string]any) {
			for _, f := range fields {
				val, ok := m[f.Field]
				if !ok {
					continue
				}
				m[f.Field] = maskValue(val, f.Mode)
			}
		})
	}
}

// partialMask keeps the first and last character, replacing the middle with
// asterisks. Strings shorter than 3 characters are fully redacted.
func partialMask(s string) string {
	runes := []rune(s)
	if len(runes) < 3 {
		return strings.Repeat("*", len(runes))
	}
	return string(runes[0]) + strings.Repeat("*", len(runes)-2) + string(runes[len(runes)-1])
}
