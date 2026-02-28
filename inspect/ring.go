package inspect

import (
	"sync"

	"github.com/florinutz/pgcdc/event"
)

type ringBuffer struct {
	mu    sync.Mutex
	items []event.Event
	size  int
	head  int
	count int

	subsMu sync.RWMutex
	subs   map[*subscriber]struct{}
}

type subscriber struct {
	ch chan event.Event
}

func newRingBuffer(size int) *ringBuffer {
	return &ringBuffer{
		items: make([]event.Event, size),
		size:  size,
		subs:  make(map[*subscriber]struct{}),
	}
}

func (rb *ringBuffer) add(ev event.Event) {
	rb.mu.Lock()
	idx := (rb.head + rb.count) % rb.size
	if rb.count >= rb.size {
		rb.head = (rb.head + 1) % rb.size
	} else {
		rb.count++
	}
	rb.items[idx] = ev
	rb.mu.Unlock()

	// Notify subscribers (non-blocking).
	rb.subsMu.RLock()
	for sub := range rb.subs {
		select {
		case sub.ch <- ev:
		default:
			// Slow subscriber — drop event.
		}
	}
	rb.subsMu.RUnlock()
}

func (rb *ringBuffer) snapshot(limit int) []event.Event {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	n := rb.count
	if limit > 0 && limit < n {
		n = limit
	}

	result := make([]event.Event, n)
	// Read the last N events (most recent).
	start := rb.count - n
	for i := 0; i < n; i++ {
		idx := (rb.head + start + i) % rb.size
		result[i] = rb.items[idx]
	}
	return result
}

func (rb *ringBuffer) subscribe() (<-chan event.Event, func()) {
	sub := &subscriber{ch: make(chan event.Event, 64)}
	rb.subsMu.Lock()
	rb.subs[sub] = struct{}{}
	rb.subsMu.Unlock()

	cancel := func() {
		rb.subsMu.Lock()
		delete(rb.subs, sub)
		rb.subsMu.Unlock()
		close(sub.ch)
	}
	return sub.ch, cancel
}
