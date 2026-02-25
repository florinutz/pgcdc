package tracing

import (
	kafkago "github.com/segmentio/kafka-go"
)

// KafkaCarrier adapts a slice of kafka-go headers to the
// propagation.TextMapCarrier interface for W3C trace context injection.
type KafkaCarrier struct {
	Headers *[]kafkago.Header
}

// Get returns the value for the given key, or empty string if not found.
func (c KafkaCarrier) Get(key string) string {
	for _, h := range *c.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// Set stores a key-value pair, replacing any existing header with the same key.
func (c KafkaCarrier) Set(key, value string) {
	for i, h := range *c.Headers {
		if h.Key == key {
			(*c.Headers)[i].Value = []byte(value)
			return
		}
	}
	*c.Headers = append(*c.Headers, kafkago.Header{Key: key, Value: []byte(value)})
}

// Keys returns all header keys.
func (c KafkaCarrier) Keys() []string {
	keys := make([]string, len(*c.Headers))
	for i, h := range *c.Headers {
		keys[i] = h.Key
	}
	return keys
}
