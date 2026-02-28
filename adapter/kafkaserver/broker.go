package kafkaserver

import (
	"strings"
	"sync"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/metrics"
)

// topic holds the partitions for a single Kafka topic.
type topic struct {
	name       string
	partitions []*partition
}

// broker manages topics and routes events from the bus into partitions.
type broker struct {
	mu             sync.RWMutex
	topics         map[string]*topic
	partitionCount int
	bufferSize     int
	keyColumn      string
}

func newBroker(partitionCount, bufferSize int, keyColumn string) *broker {
	if partitionCount <= 0 {
		partitionCount = 8
	}
	if bufferSize <= 0 {
		bufferSize = 10000
	}
	if keyColumn == "" {
		keyColumn = "id"
	}
	return &broker{
		topics:         make(map[string]*topic),
		partitionCount: partitionCount,
		bufferSize:     bufferSize,
		keyColumn:      keyColumn,
	}
}

// channelToTopic converts a pgcdc channel name to a Kafka topic name.
// Same convention as adapter/kafka: "pgcdc:orders" -> "pgcdc.orders".
func channelToTopic(channel string) string {
	return strings.ReplaceAll(channel, ":", ".")
}

// getOrCreateTopic returns an existing topic or creates a new one.
func (b *broker) getOrCreateTopic(name string) *topic {
	b.mu.RLock()
	t, ok := b.topics[name]
	b.mu.RUnlock()
	if ok {
		return t
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	// Double-check after acquiring write lock.
	if t, ok := b.topics[name]; ok {
		return t
	}

	t = &topic{
		name:       name,
		partitions: make([]*partition, b.partitionCount),
	}
	for i := range t.partitions {
		t.partitions[i] = newPartition(b.bufferSize)
	}
	b.topics[name] = t
	return t
}

// getTopic returns a topic by name, or nil if it doesn't exist.
func (b *broker) getTopic(name string) *topic {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.topics[name]
}

// topicNames returns all known topic names.
func (b *broker) topicNames() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	names := make([]string, 0, len(b.topics))
	for name := range b.topics {
		names = append(names, name)
	}
	return names
}

// ingest processes an event from the bus: maps channel to topic, hashes the
// key, appends to the correct partition.
func (b *broker) ingest(ev event.Event) {
	topicName := channelToTopic(ev.Channel)
	t := b.getOrCreateTopic(topicName)

	// Try structured record first for zero-parse key extraction.
	var key string
	if rec := ev.Record(); rec != nil && rec.Operation != 0 &&
		(rec.Change.After != nil || rec.Change.Before != nil || rec.Key != nil) {
		key = extractKeyFromRecord(rec, b.keyColumn, ev.ID)
	} else {
		key = extractKey(ev.Payload, b.keyColumn, ev.ID)
	}
	partIdx := partitionForKey(key, len(t.partitions))

	rec := record{
		Key:       []byte(key),
		Value:     ev.Payload,
		Timestamp: ev.CreatedAt.UnixMilli(),
		LSN:       ev.LSN,
		Headers: []recordHeader{
			{Key: "pgcdc-channel", Value: []byte(ev.Channel)},
			{Key: "pgcdc-operation", Value: []byte(ev.Operation)},
			{Key: "pgcdc-event-id", Value: []byte(ev.ID)},
		},
	}

	t.partitions[partIdx].append(rec)
	metrics.EventsDelivered.WithLabelValues("kafkaserver").Inc()
}
