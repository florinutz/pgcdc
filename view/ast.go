package view

import "time"

// EmitMode controls how view results are emitted per window.
type EmitMode int

const (
	// EmitRow emits one event per group key.
	EmitRow EmitMode = iota
	// EmitBatch emits a single event containing all group results.
	EmitBatch
)

// AggFunc identifies a supported aggregate function.
type AggFunc int

const (
	AggCount AggFunc = iota
	AggSum
	AggAvg
	AggMin
	AggMax
	AggCountDistinct
	AggStddev
)

// WindowType identifies the type of window.
type WindowType int

const (
	WindowTumbling WindowType = iota
	WindowSliding
	WindowSession
)

// SelectItem represents one item in the SELECT list.
type SelectItem struct {
	// Aggregate is non-nil for aggregate expressions (COUNT, SUM, ...).
	Aggregate *AggFunc
	// Field is the column reference inside the aggregate (e.g. "payload.amount").
	// Empty for COUNT(*).
	Field string
	// Alias is the output name (e.g. "order_count").
	Alias string
	// IsGroupKey is true when this select item is a plain group-by column reference.
	IsGroupKey bool
}

// ViewDef is the fully parsed definition of a streaming SQL view.
type ViewDef struct {
	Name      string
	Query     string
	Emit      EmitMode
	MaxGroups int

	// Parsed fields.
	SelectItems     []SelectItem
	FromTable       string // must be "pgcdc_events"
	Where           Predicate
	GroupBy         []string // field names
	Having          Predicate
	WindowSize      time.Duration
	WindowType      WindowType
	SlideSize       time.Duration // for sliding windows
	SessionGap      time.Duration // for session windows
	AllowedLateness time.Duration // for late event handling
}

// Predicate is a compiled filter function over an event's metadata and payload.
// Returns true if the event/row passes the filter.
// A nil Predicate always passes.
type Predicate func(meta EventMeta, payload map[string]any) bool

// EventMeta contains event-level metadata accessible in WHERE clauses.
type EventMeta struct {
	Channel   string
	Operation string
	Source    string
}
