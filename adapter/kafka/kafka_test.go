package kafka_test

import (
	"testing"

	"github.com/florinutz/pgcdc/adapter/kafka"
)

func TestKafkaAdapter_Name(t *testing.T) {
	a := kafka.New(kafka.Config{}, nil)
	if got := a.Name(); got != "kafka" {
		t.Errorf("Name() = %q, want %q", got, "kafka")
	}
}
