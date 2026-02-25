package tracing

import (
	"testing"

	kafkago "github.com/segmentio/kafka-go"
)

func TestKafkaCarrier_SetGet(t *testing.T) {
	headers := []kafkago.Header{
		{Key: "existing", Value: []byte("value")},
	}
	carrier := KafkaCarrier{Headers: &headers}

	// Set new key.
	carrier.Set("traceparent", "00-abc-def-01")
	got := carrier.Get("traceparent")
	if got != "00-abc-def-01" {
		t.Fatalf("expected '00-abc-def-01', got %q", got)
	}

	// Overwrite existing key.
	carrier.Set("traceparent", "00-xyz-uvw-00")
	got = carrier.Get("traceparent")
	if got != "00-xyz-uvw-00" {
		t.Fatalf("expected '00-xyz-uvw-00', got %q", got)
	}

	// Get non-existent key.
	got = carrier.Get("missing")
	if got != "" {
		t.Fatalf("expected empty string for missing key, got %q", got)
	}

	// Existing key preserved.
	got = carrier.Get("existing")
	if got != "value" {
		t.Fatalf("expected 'value', got %q", got)
	}
}

func TestKafkaCarrier_Keys(t *testing.T) {
	headers := []kafkago.Header{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
	}
	carrier := KafkaCarrier{Headers: &headers}
	keys := carrier.Keys()
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}
	if keys[0] != "a" || keys[1] != "b" {
		t.Fatalf("unexpected keys: %v", keys)
	}
}
