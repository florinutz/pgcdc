package dlq

import "sync"

// NackWindow tracks delivery outcomes (ack/nack) in a sliding window.
// When the nack count exceeds the threshold, the adapter should be paused.
type NackWindow struct {
	mu        sync.Mutex
	outcomes  []bool // ring buffer: true=ack, false=nack
	size      int
	head      int // index of oldest entry
	count     int // total outcomes in buffer (0..size)
	nackCount int // count of nacks in buffer
	threshold int // nack count to trigger pause
}

// NewNackWindow creates a window with the given size and nack threshold.
func NewNackWindow(size, threshold int) *NackWindow {
	if size <= 0 {
		size = 100
	}
	if threshold <= 0 {
		threshold = size / 2
	}
	return &NackWindow{
		outcomes:  make([]bool, size),
		size:      size,
		threshold: threshold,
	}
}

// RecordAck records a successful delivery.
// Returns true if the nack threshold is currently exceeded.
func (w *NackWindow) RecordAck() bool { return w.record(true) }

// RecordNack records a failed delivery.
// Returns true if the nack threshold is now exceeded.
func (w *NackWindow) RecordNack() bool { return w.record(false) }

// record adds an outcome and returns true if threshold exceeded.
func (w *NackWindow) record(ack bool) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.count < w.size {
		// Buffer not yet full — append at tail.
		idx := (w.head + w.count) % w.size
		w.outcomes[idx] = ack
		w.count++
	} else {
		// Buffer full — evict oldest at head, write new entry there.
		if !w.outcomes[w.head] {
			w.nackCount--
		}
		w.outcomes[w.head] = ack
		w.head = (w.head + 1) % w.size
	}

	if !ack {
		w.nackCount++
	}

	return w.nackCount >= w.threshold
}

// Exceeded reports whether the nack threshold is currently exceeded.
func (w *NackWindow) Exceeded() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.nackCount >= w.threshold
}

// Reset clears the window.
func (w *NackWindow) Reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.head = 0
	w.count = 0
	w.nackCount = 0
}

// Stats returns current window statistics.
func (w *NackWindow) Stats() (total, nacks, threshold int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.count, w.nackCount, w.threshold
}
