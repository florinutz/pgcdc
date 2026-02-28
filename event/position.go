package event

// Position abstracts a source-specific event position. It provides a uint64
// representation for checkpoint compatibility (WAL LSN, MySQL binlog encoded
// position) and a raw byte representation for full-fidelity positions
// (MongoDB resume tokens, composite positions).
type Position struct {
	uint64Val uint64
	raw       []byte
}

// NewPosition creates a Position from a uint64 value (WAL LSN or encoded position).
func NewPosition(val uint64) Position {
	return Position{uint64Val: val}
}

// NewPositionWithRaw creates a Position from both a uint64 and raw bytes.
func NewPositionWithRaw(val uint64, raw []byte) Position {
	return Position{uint64Val: val, raw: raw}
}

// Uint64 returns the checkpoint-compatible uint64 representation.
// For WAL: the LSN value. For MySQL: the encoded binlog position.
// For MongoDB: 0 (no uint64 equivalent).
func (p Position) Uint64() uint64 {
	return p.uint64Val
}

// Raw returns the full-fidelity position bytes, or nil if not set.
func (p Position) Raw() []byte {
	return p.raw
}

// IsZero reports whether the position represents a zero/unset value.
func (p Position) IsZero() bool {
	return p.uint64Val == 0 && len(p.raw) == 0
}
