package transform

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/metrics"
)

const defaultDedupMaxKeys = 100000

type dedupEntry struct {
	key     string
	expires time.Time
	prev    *dedupEntry
	next    *dedupEntry
}

type lruDedup struct {
	mu      sync.Mutex
	items   map[string]*dedupEntry
	head    *dedupEntry // most recently used
	tail    *dedupEntry // least recently used
	maxKeys int
	window  time.Duration
}

func newLRUDedup(window time.Duration, maxKeys int) *lruDedup {
	if maxKeys <= 0 {
		maxKeys = defaultDedupMaxKeys
	}
	return &lruDedup{
		items:   make(map[string]*dedupEntry, maxKeys),
		maxKeys: maxKeys,
		window:  window,
	}
}

// seen returns true if the key was already seen within the TTL window (duplicate).
// It is safe for concurrent use.
func (l *lruDedup) seen(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()

	// Check if key exists.
	if e, ok := l.items[key]; ok {
		if now.Before(e.expires) {
			// Not expired — duplicate.
			l.moveToFront(e)
			return true
		}
		// Expired — remove and treat as new.
		l.remove(e)
	}

	// Evict expired entries from tail, or evict LRU if at capacity.
	for l.tail != nil && (now.After(l.tail.expires) || !now.Before(l.tail.expires) && len(l.items) >= l.maxKeys) {
		l.remove(l.tail)
	}
	for len(l.items) >= l.maxKeys {
		l.remove(l.tail)
	}

	// Add new entry.
	e := &dedupEntry{
		key:     key,
		expires: now.Add(l.window),
	}
	l.items[key] = e
	l.pushFront(e)

	metrics.DedupCacheSize.Set(float64(len(l.items)))
	return false
}

func (l *lruDedup) pushFront(e *dedupEntry) {
	e.prev = nil
	e.next = l.head
	if l.head != nil {
		l.head.prev = e
	}
	l.head = e
	if l.tail == nil {
		l.tail = e
	}
}

func (l *lruDedup) unlink(e *dedupEntry) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		l.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		l.tail = e.prev
	}
	e.prev = nil
	e.next = nil
}

func (l *lruDedup) moveToFront(e *dedupEntry) {
	if l.head == e {
		return
	}
	l.unlink(e)
	l.pushFront(e)
}

func (l *lruDedup) remove(e *dedupEntry) {
	l.unlink(e)
	delete(l.items, e.key)
}

// Dedup returns a TransformFunc that drops duplicate events by key within a
// time window. When keyPath is empty, the event ID is used as the dedup key.
// When non-empty, keyPath is a dot-separated path into the event payload
// (e.g. "row.id"). maxKeys limits the LRU cache size; 0 defaults to 100000.
func Dedup(keyPath string, window time.Duration, maxKeys int) TransformFunc {
	cache := newLRUDedup(window, maxKeys)
	parts := splitKeyPath(keyPath)

	return func(ev event.Event) (event.Event, error) {
		key := extractDedupKey(ev, parts)
		if cache.seen(key) {
			metrics.DedupDropped.Inc()
			return ev, ErrDropEvent
		}
		return ev, nil
	}
}

// splitKeyPath splits a dot-separated key path into segments.
// Returns nil for empty paths (meaning: use event ID).
func splitKeyPath(keyPath string) []string {
	if keyPath == "" {
		return nil
	}
	return strings.Split(keyPath, ".")
}

// extractDedupKey extracts the dedup key from an event.
// When parts is nil, uses the event ID. Otherwise navigates the payload
// using the dot-path segments.
func extractDedupKey(ev event.Event, parts []string) string {
	if len(parts) == 0 {
		return ev.ID
	}

	m, ok := readPayload(ev)
	if !ok {
		return ev.ID
	}

	var current any = m
	for _, p := range parts {
		cm, ok := current.(map[string]any)
		if !ok {
			return ev.ID
		}
		current, ok = cm[p]
		if !ok {
			return ev.ID
		}
	}

	return fmt.Sprintf("%v", current)
}
