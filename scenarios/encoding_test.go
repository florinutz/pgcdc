//go:build integration

package scenarios

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/hamba/avro/v2"

	"github.com/florinutz/pgcdc/bus"
	"github.com/florinutz/pgcdc/detector/listennotify"
	"github.com/florinutz/pgcdc/encoding"
	"github.com/florinutz/pgcdc/encoding/registry"

	kafkaadapter "github.com/florinutz/pgcdc/adapter/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

// ensureKafkaTopic pre-creates a Kafka topic so the producer doesn't hit
// "Unknown Topic Or Partition" on the first produce.
func ensureKafkaTopic(t *testing.T, brokers []string, topic string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("create kafka client: %v", err)
	}
	defer cl.Close()

	admin := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resps, err := admin.CreateTopics(ctx, 1, 1, nil, topic)
	if err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}
	for _, resp := range resps.Sorted() {
		if resp.Err != nil {
			t.Fatalf("create topic %s: %v", resp.Topic, resp.Err)
		}
	}
}

func TestScenario_Encoding(t *testing.T) {
	connStr := startPostgres(t)

	t.Run("kafka avro encoding", func(t *testing.T) {
		brokers := startKafka(t)

		logger := testLogger()
		enc := encoding.NewAvroEncoder(logger)
		capDLQ := &captureDLQ{}
		a := kafkaadapter.New(brokers, "", "", "", "", "", false, 0, 0, enc, logger, "")
		a.SetDLQ(capDLQ)

		// Pre-create the topic to avoid "Unknown Topic Or Partition" race.
		channel := "enc_avro_test"
		ensureKafkaTopic(t, brokers, channel)

		// Wire pipeline: LISTEN/NOTIFY detector → bus → Kafka adapter (Avro).
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

		// Wait for adapter connection.
		time.Sleep(3 * time.Second)

		// Send a NOTIFY event.
		payload := `{"op":"INSERT","table":"orders","row":{"id":1,"item":"widget"}}`
		sendNotify(t, connStr, channel, payload)

		// Read from Kafka.
		msg := readOneRecord(t, brokers, channel, 15*time.Second)
		if msg == nil {
			if recs := capDLQ.captured(); len(recs) > 0 {
				t.Fatalf("no kafka message (DLQ error: %s)", recs[0].Error)
			}
			t.Fatal("no kafka message received")
		}

		// Verify the message is NOT plain JSON (it's Avro-encoded).
		var jsonCheck map[string]any
		if json.Unmarshal(msg.Value, &jsonCheck) == nil {
			if _, hasID := jsonCheck["id"]; hasID {
				t.Error("message appears to be plain JSON, expected Avro binary encoding")
			}
		}

		// Verify the content-type header is set.
		headers := make(map[string]string)
		for _, h := range msg.Headers {
			headers[h.Key] = string(h.Value)
		}
		if headers["content-type"] != "application/avro" {
			t.Errorf("content-type header = %q, want application/avro", headers["content-type"])
		}

		// The message is Avro-encoded using the opaque schema (no --include-schema columns).
		// Decode it to verify the round-trip.
		opaqueSchemaJSON := `{"type":"record","name":"OpaqueEvent","namespace":"pgcdc","fields":[{"name":"payload","type":"bytes"}]}`
		opaqueSchema, err := avro.Parse(opaqueSchemaJSON)
		if err != nil {
			t.Fatalf("parse opaque schema: %v", err)
		}

		var decoded map[string]any
		if err := avro.Unmarshal(opaqueSchema, msg.Value, &decoded); err != nil {
			t.Fatalf("avro unmarshal: %v", err)
		}

		payloadBytes, ok := decoded["payload"].([]byte)
		if !ok {
			t.Fatalf("decoded payload is not bytes: %T", decoded["payload"])
		}
		if len(payloadBytes) == 0 {
			t.Error("decoded payload is empty")
		}

		fmt.Fprintf(os.Stderr, "Avro-encoded Kafka message received: topic=%s key=%s payload_bytes=%d\n",
			msg.Topic, string(msg.Key), len(payloadBytes))
	})

	t.Run("kafka avro with schema registry", func(t *testing.T) {
		brokers := startKafka(t)

		// Start a mock Schema Registry server.
		mockReg := newMockSchemaRegistry(t)

		logger := testLogger()
		regClient := registry.New(mockReg.URL(), "", "")
		enc := encoding.NewAvroEncoder(logger, encoding.WithRegistry(regClient))
		capDLQ := &captureDLQ{}
		a := kafkaadapter.New(brokers, "", "", "", "", "", false, 0, 0, enc, logger, "")
		a.SetDLQ(capDLQ)

		// Pre-create the topic.
		channel := "enc_reg_test"
		ensureKafkaTopic(t, brokers, channel)

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

		time.Sleep(3 * time.Second)

		// Send event.
		payload := `{"op":"INSERT","table":"items","row":{"id":42}}`
		sendNotify(t, connStr, channel, payload)

		// Read from Kafka.
		msg := readOneRecord(t, brokers, channel, 15*time.Second)
		if msg == nil {
			if recs := capDLQ.captured(); len(recs) > 0 {
				t.Fatalf("no kafka message (DLQ error: %s)", recs[0].Error)
			}
			t.Fatal("no kafka message received")
		}

		// Verify the schema was registered.
		regCount := mockReg.RegistrationCount()
		if regCount == 0 {
			t.Error("expected at least one schema registration, got none")
		}

		// Verify the message has Confluent wire format header: [0x00][4-byte schema ID][payload].
		if len(msg.Value) < 5 {
			t.Fatalf("message too short for wire format: %d bytes", len(msg.Value))
		}
		if msg.Value[0] != 0x00 {
			t.Errorf("wire format magic byte = 0x%02x, want 0x00", msg.Value[0])
		}

		// Decode the wire-format payload.
		schemaID, avroPayload, err := registry.WireDecode(msg.Value)
		if err != nil {
			t.Fatalf("wire decode: %v", err)
		}
		if schemaID <= 0 {
			t.Errorf("schema ID = %d, want > 0", schemaID)
		}
		if len(avroPayload) == 0 {
			t.Error("avro payload after wire decode is empty")
		}

		fmt.Fprintf(os.Stderr, "Schema Registry Avro message: topic=%s schema_id=%d registrations=%d avro_bytes=%d\n",
			msg.Topic, schemaID, regCount, len(avroPayload))
	})

	t.Run("schema registry unavailable", func(t *testing.T) {
		brokers := startKafka(t)

		logger := testLogger()
		// Point to a registry URL that will refuse connections.
		regClient := registry.New("http://127.0.0.1:1", "", "")
		enc := encoding.NewAvroEncoder(logger, encoding.WithRegistry(regClient))

		capDLQ := &captureDLQ{}
		a := kafkaadapter.New(brokers, "", "", "", "", "", false, 0, 0, enc, logger, "")
		a.SetDLQ(capDLQ)

		// Wire pipeline.
		pipelineCtx, pipelineCancel := context.WithCancel(context.Background())
		channel := "enc_regfail_test"
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

		time.Sleep(3 * time.Second)

		// Send event — encoding will fail because registry is unreachable.
		payload := `{"op":"INSERT","table":"orders","row":{"id":99}}`
		sendNotify(t, connStr, channel, payload)

		// Wait for the event to be recorded to DLQ.
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if recs := capDLQ.captured(); len(recs) > 0 {
				rec := recs[0]
				if rec.Adapter != "kafka" {
					t.Errorf("DLQ adapter = %q, want kafka", rec.Adapter)
				}
				if rec.Error == "" {
					t.Error("DLQ record error is empty")
				}
				fmt.Fprintf(os.Stderr, "Encoding failure DLQ record: adapter=%s error=%s\n", rec.Adapter, rec.Error)
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
		t.Fatal("expected encoding failure to be recorded to DLQ, but none received within timeout")
	})
}

// mockSchemaRegistry is a minimal Schema Registry mock for testing.
type mockSchemaRegistry struct {
	server *httptest.Server
	mu     sync.Mutex
	nextID int
	count  int
}

func newMockSchemaRegistry(t *testing.T) *mockSchemaRegistry {
	t.Helper()
	m := &mockSchemaRegistry{nextID: 1}
	m.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost:
			// Register schema → return ID.
			m.mu.Lock()
			id := m.nextID
			m.nextID++
			m.count++
			m.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"id":%d}`, id)
		case r.Method == http.MethodGet:
			// Get schema by ID → return dummy.
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"schema":"{}","schemaType":"AVRO","id":1}`)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(func() { m.server.Close() })
	return m
}

func (m *mockSchemaRegistry) URL() string {
	return m.server.URL
}

func (m *mockSchemaRegistry) RegistrationCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.count
}
