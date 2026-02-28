package inspect

import (
	"sync"

	"github.com/florinutz/pgcdc/event"
)

// TapPoint identifies where in the pipeline events are captured.
type TapPoint string

const (
	TapPostDetector  TapPoint = "post-detector"
	TapPostTransform TapPoint = "post-transform"
	TapPreAdapter    TapPoint = "pre-adapter"
)

// Inspector provides ring-buffer event sampling at configurable tap points.
// Zero performance impact when nobody is reading.
type Inspector struct {
	mu      sync.RWMutex
	buffers map[TapPoint]*ringBuffer
	size    int
}

// New creates an Inspector with ring buffers of the given size per tap point.
func New(bufferSize int) *Inspector {
	if bufferSize <= 0 {
		bufferSize = 100
	}
	return &Inspector{
		buffers: map[TapPoint]*ringBuffer{
			TapPostDetector:  newRingBuffer(bufferSize),
			TapPostTransform: newRingBuffer(bufferSize),
			TapPreAdapter:    newRingBuffer(bufferSize),
		},
		size: bufferSize,
	}
}

// Record stores an event at the given tap point.
func (i *Inspector) Record(point TapPoint, ev event.Event) {
	i.mu.RLock()
	buf, ok := i.buffers[point]
	i.mu.RUnlock()
	if !ok {
		return
	}
	buf.add(ev)
}

// Snapshot returns the last N events from the given tap point.
// If limit <= 0, returns all buffered events.
func (i *Inspector) Snapshot(point TapPoint, limit int) []event.Event {
	i.mu.RLock()
	buf, ok := i.buffers[point]
	i.mu.RUnlock()
	if !ok {
		return nil
	}
	return buf.snapshot(limit)
}

// Subscribe returns a channel that receives events at the given tap point
// in real-time. Call the returned cancel function to unsubscribe.
func (i *Inspector) Subscribe(point TapPoint) (<-chan event.Event, func()) {
	i.mu.RLock()
	buf, ok := i.buffers[point]
	i.mu.RUnlock()
	if !ok {
		ch := make(chan event.Event)
		close(ch)
		return ch, func() {}
	}
	return buf.subscribe()
}
