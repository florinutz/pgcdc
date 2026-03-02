package output

import "encoding/json"

// JSON writes the value as indented JSON.
func (p *Printer) JSON(v any) error {
	enc := json.NewEncoder(p.w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
