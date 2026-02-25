//go:build integration

package scenarios

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	kafkaadapter "github.com/florinutz/pgcdc/adapter/kafka"
	"github.com/florinutz/pgcdc/bus"
	"github.com/florinutz/pgcdc/detector/listennotify"
	"github.com/florinutz/pgcdc/dlq"
	"github.com/florinutz/pgcdc/event"
	kafkatc "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

// captureDLQ collects DLQ records in memory for test assertions.
type captureDLQ struct {
	mu      sync.Mutex
	records []dlq.Record
}

func (c *captureDLQ) Record(_ context.Context, ev event.Event, adapterName string, err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, dlq.Record{
		Event:     ev,
		Adapter:   adapterName,
		Error:     err.Error(),
		Timestamp: time.Now().UTC(),
	})
	return nil
}

func (c *captureDLQ) Close() error { return nil }

func (c *captureDLQ) captured() []dlq.Record {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]dlq.Record, len(c.records))
	copy(out, c.records)
	return out
}

// startKafka starts a Kafka container and returns the broker address.
func startKafka(t *testing.T) []string {
	t.Helper()
	ctx := context.Background()

	kc, err := kafkatc.Run(ctx, "confluentinc/confluent-local:7.5.0")
	if err != nil {
		t.Fatalf("start kafka container: %v", err)
	}
	t.Cleanup(func() { _ = kc.Terminate(context.Background()) })

	brokers, err := kc.Brokers(ctx)
	if err != nil {
		t.Fatalf("get kafka brokers: %v", err)
	}
	return brokers
}

// readOneRecord reads a single record from the given Kafka topic within the timeout.
func readOneRecord(t *testing.T, brokers []string, topic string, timeout time.Duration) *kgo.Record {
	t.Helper()

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("create kafka consumer: %v", err)
	}
	defer consumer.Close()

	readCtx, readCancel := context.WithTimeout(context.Background(), timeout)
	defer readCancel()

	var received *kgo.Record
	for received == nil && readCtx.Err() == nil {
		fetches := consumer.PollFetches(readCtx)
		fetches.EachRecord(func(r *kgo.Record) {
			if received == nil {
				received = r
			}
		})
	}
	if received == nil {
		t.Fatal("no kafka message received within timeout")
	}
	return received
}

func TestScenario_KafkaAdapter(t *testing.T) {
	connStr := startPostgres(t)

	t.Run("happy path", func(t *testing.T) {
		brokers := startKafka(t)

		logger := testLogger()
		a := kafkaadapter.New(brokers, "", "", "", "", "", false, 0, 0, nil, logger, "")

		// Pre-create the topic to avoid "Unknown Topic Or Partition" race.
		channel := "kafka_test"
		ensureKafkaTopic(t, brokers, channel)

		// Wire pipeline: LISTEN/NOTIFY detector → bus → Kafka adapter.
		pipelineCtx, pipelineCancel := context.WithCancel(context.Background())
		det := listennotify.New(connStr, []string{channel}, 0, 0, logger)
		b := bus.New(64, logger)

		g, gCtx := errgroup.WithContext(pipelineCtx)
		g.Go(func() error { return b.Start(gCtx) })
		g.Go(func() error { return det.Start(gCtx, b.Ingest()) })

		sub, err := b.Subscribe(a.Name())
		if err != nil {
			pipelineCancel()
			t.Fatalf("subscribe kafka: %v", err)
		}
		g.Go(func() error { return a.Start(gCtx, sub) })

		t.Cleanup(func() {
			pipelineCancel()
			_ = g.Wait()
		})

		// Wait for adapter to establish connection.
		time.Sleep(3 * time.Second)

		// Send a NOTIFY event through PostgreSQL.
		payload := `{"op":"INSERT","table":"orders","row":{"id":1,"item":"widget"}}`
		sendNotify(t, connStr, channel, payload)

		// Read back from the Kafka topic.
		// Topic mapping: "kafka_test" → "kafka_test" (no colon, no substitution).
		msg := readOneRecord(t, brokers, channel, 15*time.Second)

		// The adapter sends ev.Payload as the message value (raw inner JSON),
		// with metadata in headers and message key.
		var raw map[string]any
		if err := json.Unmarshal(msg.Value, &raw); err != nil {
			t.Fatalf("unmarshal payload: %v\nraw: %s", err, string(msg.Value))
		}
		if raw["op"] != "INSERT" {
			t.Errorf("payload op = %v, want INSERT", raw["op"])
		}
		if raw["table"] != "orders" {
			t.Errorf("payload table = %v, want orders", raw["table"])
		}

		// Verify the message key is a non-empty event ID.
		eventID := string(msg.Key)
		if eventID == "" {
			t.Error("message key (event ID) is empty")
		}

		// Verify headers.
		headers := make(map[string]string)
		for _, h := range msg.Headers {
			headers[h.Key] = string(h.Value)
		}
		if headers["pgcdc-channel"] != channel {
			t.Errorf("pgcdc-channel header = %q, want %q", headers["pgcdc-channel"], channel)
		}
		if headers["pgcdc-operation"] != "INSERT" {
			t.Errorf("pgcdc-operation header = %q, want INSERT", headers["pgcdc-operation"])
		}
		if headers["pgcdc-event-id"] != eventID {
			t.Errorf("pgcdc-event-id header = %q, want event ID %q", headers["pgcdc-event-id"], eventID)
		}
		if headers["content-type"] != "application/json" {
			t.Errorf("content-type header = %q, want application/json", headers["content-type"])
		}

		fmt.Fprintf(os.Stderr, "Kafka message received: topic=%s key=%s event_id=%s\n",
			msg.Topic, eventID, eventID)
	})

	t.Run("terminal error goes to DLQ", func(t *testing.T) {
		brokers := startKafka(t)

		// Create a topic with max.message.bytes=100 via the admin client.
		// Messages larger than 100 bytes will trigger MessageSizeTooLarge (terminal error).
		tinyTopic := "kafka_dlq_test"
		cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
		if err != nil {
			t.Fatalf("create admin client: %v", err)
		}
		admin := kadm.NewClient(cl)
		createCtx, createCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer createCancel()
		maxBytes := "100"
		resps, err := admin.CreateTopics(createCtx, 1, 1, map[string]*string{
			"max.message.bytes": &maxBytes,
		}, tinyTopic)
		cl.Close()
		if err != nil {
			t.Fatalf("create topic: %v", err)
		}
		for _, resp := range resps.Sorted() {
			if resp.Err != nil {
				t.Fatalf("create topic %s: %v", resp.Topic, resp.Err)
			}
		}

		capDLQ := &captureDLQ{}
		logger := testLogger()
		a := kafkaadapter.New(brokers, tinyTopic, "", "", "", "", false, 0, 0, nil, logger, "")
		a.SetDLQ(capDLQ)

		// Wire a minimal pipeline.
		pipelineCtx, pipelineCancel := context.WithCancel(context.Background())
		channel := "kafka_dlq_chan"
		det := listennotify.New(connStr, []string{channel}, 0, 0, logger)
		b := bus.New(64, logger)

		g, gCtx := errgroup.WithContext(pipelineCtx)
		g.Go(func() error { return b.Start(gCtx) })
		g.Go(func() error { return det.Start(gCtx, b.Ingest()) })

		sub, subErr := b.Subscribe(a.Name())
		if subErr != nil {
			pipelineCancel()
			t.Fatalf("subscribe kafka: %v", subErr)
		}
		g.Go(func() error { return a.Start(gCtx, sub) })

		t.Cleanup(func() {
			pipelineCancel()
			_ = g.Wait()
		})

		// Wait for adapter to be ready.
		time.Sleep(3 * time.Second)

		// Send a NOTIFY with a payload larger than 100 bytes.
		// This triggers MessageSizeTooLarge when kafka writes it.
		largeValue := make([]byte, 200)
		for i := range largeValue {
			largeValue[i] = 'x'
		}
		bigPayload := fmt.Sprintf(`{"op":"INSERT","table":"orders","data":%q}`, string(largeValue))
		sendNotify(t, connStr, channel, bigPayload)

		// Wait for the event to be processed and recorded to DLQ.
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if recs := capDLQ.captured(); len(recs) > 0 {
				// Verify the DLQ record.
				rec := recs[0]
				if rec.Adapter != "kafka" {
					t.Errorf("DLQ adapter = %q, want kafka", rec.Adapter)
				}
				if rec.Error == "" {
					t.Error("DLQ record error is empty")
				}
				fmt.Fprintf(os.Stderr, "DLQ record captured: adapter=%s error=%s\n", rec.Adapter, rec.Error)
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
		t.Fatal("expected event to be recorded to DLQ, but none received within timeout")
	})

	t.Run("transactional exactly-once", func(t *testing.T) {
		brokers := startKafka(t)

		logger := testLogger()
		channel := "kafka_txn_test"
		ensureKafkaTopic(t, brokers, channel)

		// Create adapter with transactional ID.
		a := kafkaadapter.New(brokers, "", "", "", "", "", false, 0, 0, nil, logger, "pgcdc-test-txn")

		// Wire pipeline.
		pipelineCtx, pipelineCancel := context.WithCancel(context.Background())
		det := listennotify.New(connStr, []string{channel}, 0, 0, logger)
		b := bus.New(64, logger)

		g, gCtx := errgroup.WithContext(pipelineCtx)
		g.Go(func() error { return b.Start(gCtx) })
		g.Go(func() error { return det.Start(gCtx, b.Ingest()) })

		sub, err := b.Subscribe(a.Name())
		if err != nil {
			pipelineCancel()
			t.Fatalf("subscribe kafka: %v", err)
		}
		g.Go(func() error { return a.Start(gCtx, sub) })

		t.Cleanup(func() {
			pipelineCancel()
			_ = g.Wait()
		})

		// Wait for adapter to establish connection.
		time.Sleep(3 * time.Second)

		// Send a NOTIFY event.
		payload := `{"op":"INSERT","table":"orders","row":{"id":99,"item":"txn-widget"}}`
		sendNotify(t, connStr, channel, payload)

		// Read from Kafka — transactional events are committed atomically.
		// Use read_committed isolation via consumer option.
		consumer, err := kgo.NewClient(
			kgo.SeedBrokers(brokers...),
			kgo.ConsumeTopics(channel),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		)
		if err != nil {
			t.Fatalf("create kafka consumer: %v", err)
		}
		defer consumer.Close()

		readCtx, readCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer readCancel()

		var msg *kgo.Record
		for msg == nil && readCtx.Err() == nil {
			fetches := consumer.PollFetches(readCtx)
			fetches.EachRecord(func(r *kgo.Record) {
				if msg == nil {
					msg = r
				}
			})
		}
		if msg == nil {
			t.Fatal("no kafka message received within timeout")
		}

		// Verify the payload.
		var raw map[string]any
		if err := json.Unmarshal(msg.Value, &raw); err != nil {
			t.Fatalf("unmarshal payload: %v\nraw: %s", err, string(msg.Value))
		}
		if raw["op"] != "INSERT" {
			t.Errorf("payload op = %v, want INSERT", raw["op"])
		}
		if raw["table"] != "orders" {
			t.Errorf("payload table = %v, want orders", raw["table"])
		}

		fmt.Fprintf(os.Stderr, "Transactional Kafka message received: topic=%s key=%s\n",
			msg.Topic, string(msg.Key))
	})
}
