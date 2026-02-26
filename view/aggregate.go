package view

import (
	"fmt"
	"math"
)

// Aggregator accumulates values for a single aggregate function.
type Aggregator interface {
	// Add adds a value. Returns false if the value was skipped (e.g. non-numeric for SUM).
	Add(v any) bool
	// Result returns the current aggregate value.
	Result() any
	// Reset resets the aggregator for a new window.
	Reset()
}

// NewAggregator creates an aggregator for the given function type.
func NewAggregator(fn AggFunc) Aggregator {
	switch fn {
	case AggCount:
		return &countAgg{}
	case AggSum:
		return &sumAgg{}
	case AggAvg:
		return &avgAgg{}
	case AggMin:
		return &minAgg{}
	case AggMax:
		return &maxAgg{}
	case AggCountDistinct:
		return &countDistinctAgg{seen: make(map[string]struct{})}
	case AggStddev:
		return &stddevAgg{}
	default:
		return &countAgg{}
	}
}

// countAgg counts non-nil values. For COUNT(*), the caller passes a non-nil
// sentinel; for COUNT(field), the caller passes the resolved value (nil when
// the field is absent or NULL, matching SQL semantics).
type countAgg struct {
	n int64
}

func (a *countAgg) Add(v any) bool {
	if v == nil {
		return false
	}
	a.n++
	return true
}

func (a *countAgg) Result() any { return a.n }

func (a *countAgg) Reset() { a.n = 0 }

// sumAgg sums numeric values.
type sumAgg struct {
	sum float64
	any bool // true if at least one value was added
}

func (a *sumAgg) Add(v any) bool {
	f, ok := toFloat64(v)
	if !ok {
		return false
	}
	a.sum += f
	a.any = true
	return true
}

func (a *sumAgg) Result() any {
	if !a.any {
		return float64(0)
	}
	return a.sum
}

func (a *sumAgg) Reset() {
	a.sum = 0
	a.any = false
}

// avgAgg computes a running average.
type avgAgg struct {
	sum   float64
	count int64
}

func (a *avgAgg) Add(v any) bool {
	f, ok := toFloat64(v)
	if !ok {
		return false
	}
	a.sum += f
	a.count++
	return true
}

func (a *avgAgg) Result() any {
	if a.count == 0 {
		return float64(0)
	}
	return a.sum / float64(a.count)
}

func (a *avgAgg) Reset() {
	a.sum = 0
	a.count = 0
}

// minAgg tracks the minimum value.
type minAgg struct {
	min float64
	any bool
}

func (a *minAgg) Add(v any) bool {
	f, ok := toFloat64(v)
	if !ok {
		return false
	}
	if !a.any || f < a.min {
		a.min = f
	}
	a.any = true
	return true
}

func (a *minAgg) Result() any {
	if !a.any {
		return nil
	}
	return a.min
}

func (a *minAgg) Reset() {
	a.min = 0
	a.any = false
}

// maxAgg tracks the maximum value.
type maxAgg struct {
	max float64
	any bool
}

func (a *maxAgg) Add(v any) bool {
	f, ok := toFloat64(v)
	if !ok {
		return false
	}
	if !a.any || f > a.max {
		a.max = f
	}
	a.any = true
	return true
}

func (a *maxAgg) Result() any {
	if !a.any {
		return nil
	}
	return a.max
}

func (a *maxAgg) Reset() {
	a.max = 0
	a.any = false
}

// countDistinctAgg counts distinct non-nil values.
type countDistinctAgg struct {
	seen map[string]struct{}
}

func (a *countDistinctAgg) Add(v any) bool {
	if v == nil {
		return false
	}
	key := fmt.Sprintf("%v", v)
	a.seen[key] = struct{}{}
	return true
}

func (a *countDistinctAgg) Result() any { return int64(len(a.seen)) }
func (a *countDistinctAgg) Reset()      { a.seen = make(map[string]struct{}) }

// stddevAgg computes population standard deviation using Welford's online algorithm.
type stddevAgg struct {
	n    int64
	mean float64
	m2   float64
}

func (a *stddevAgg) Add(v any) bool {
	f, ok := toFloat64(v)
	if !ok {
		return false
	}
	a.n++
	delta := f - a.mean
	a.mean += delta / float64(a.n)
	delta2 := f - a.mean
	a.m2 += delta * delta2
	return true
}

func (a *stddevAgg) Result() any {
	if a.n < 2 {
		return float64(0)
	}
	return math.Sqrt(a.m2 / float64(a.n))
}

func (a *stddevAgg) Reset() { a.n = 0; a.mean = 0; a.m2 = 0 }
