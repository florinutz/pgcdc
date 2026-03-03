//go:build integration

package scenarios

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	kafkaserveradapter "github.com/florinutz/pgcdc/adapter/kafkaserver"
	"github.com/florinutz/pgcdc/bus"
	"github.com/florinutz/pgcdc/detector/listennotify"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

func TestScenario_KafkaServer(t *testing.T) {
	t.Parallel()
	connStr := startPostgres(t)

	t.Run("happy path", func(t *testing.T) {
		// Use a unique port to avoid conflicts.
		addr := "127.0.0.1:19092"
		channel := "ks_orders"

		logger := testLogger()
		a := kafkaserveradapter.New(addr, 4, 1000, 10*time.Second, "id", nil, logger)

		// Wire pipeline: LISTEN/NOTIFY detector -> bus -> kafkaserver adapter.
		pipelineCtx, pipelineCancel := context.WithCancel(context.Background())

		det := listennotify.New(connStr, []string{channel}, 0, 0, logger)
		b := bus.New(64, logger)

		g, gCtx := errgroup.WithContext(pipelineCtx)
		g.Go(func() error { return b.Start(gCtx) })
		g.Go(func() error { return det.Start(gCtx, b.Ingest()) })

		sub, err := b.Subscribe(a.Name())
		if err != nil {
			pipelineCancel()
			t.Fatalf("subscribe kafkaserver: %v", err)
		}
		g.Go(func() error { return a.Start(gCtx, sub) })

		t.Cleanup(func() {
			pipelineCancel()
			_ = g.Wait()
		})

		// Wait for the TCP server to start and the LISTEN/NOTIFY detector to connect.
		time.Sleep(2 * time.Second)

		// Connect a franz-go consumer to our Kafka protocol server.
		// The topic should be "ks_orders" (channel has no colon, so no replacement).
		consumer, err := kgo.NewClient(
			kgo.SeedBrokers(addr),
			kgo.ConsumeTopics(channel),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			// Disable consumer group features for simple consumption.
		)
		if err != nil {
			t.Fatalf("create franz-go consumer: %v", err)
		}
		defer consumer.Close()

		// Send a NOTIFY event through PostgreSQL.
		payload := `{"id":"order-1","status":"shipped","item":"widget"}`
		sendNotify(t, connStr, channel, payload)

		// Poll for the message.
		readCtx, readCancel := context.WithTimeout(context.Background(), 10*time.Second)
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
			t.Fatal("no message received from kafkaserver within timeout")
		}

		// Verify the message payload.
		var raw map[string]any
		if err := json.Unmarshal(received.Value, &raw); err != nil {
			t.Fatalf("unmarshal payload: %v\nraw: %s", err, string(received.Value))
		}
		if raw["id"] != "order-1" {
			t.Errorf("payload id = %v, want order-1", raw["id"])
		}
		if raw["status"] != "shipped" {
			t.Errorf("payload status = %v, want shipped", raw["status"])
		}

		// Verify the key is the extracted "id" field.
		key := string(received.Key)
		if key != "order-1" {
			t.Errorf("message key = %q, want %q", key, "order-1")
		}

		// Verify headers.
		headers := make(map[string]string)
		for _, h := range received.Headers {
			headers[h.Key] = string(h.Value)
		}
		if headers["pgcdc-channel"] != channel {
			t.Errorf("pgcdc-channel header = %q, want %q", headers["pgcdc-channel"], channel)
		}
		if headers["pgcdc-event-id"] == "" {
			t.Error("pgcdc-event-id header is empty")
		}

		fmt.Fprintf(os.Stderr, "KafkaServer message received: topic=%s key=%s\n",
			received.Topic, key)
	})

	t.Run("consumer group rebalance", func(t *testing.T) {
		addr := "127.0.0.1:19093"
		channel := "ks_rebalance"

		logger := testLogger()
		a := kafkaserveradapter.New(addr, 4, 1000, 5*time.Second, "id", nil, logger)

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
			t.Fatalf("subscribe kafkaserver: %v", err)
		}
		g.Go(func() error { return a.Start(gCtx, sub) })

		t.Cleanup(func() {
			pipelineCancel()
			_ = g.Wait()
		})

		time.Sleep(500 * time.Millisecond)

		// Pre-create the topic by sending an event.
		sendNotify(t, connStr, channel, `{"id":"setup","data":"init"}`)
		time.Sleep(200 * time.Millisecond)

		// Connect consumer 1.
		c1, err := kgo.NewClient(
			kgo.SeedBrokers(addr),
			kgo.ConsumeTopics(channel),
			kgo.ConsumerGroup("test-group"),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		)
		if err != nil {
			t.Fatalf("create consumer 1: %v", err)
		}

		// Let consumer 1 join the group.
		time.Sleep(1 * time.Second)

		// Connect consumer 2.
		c2, err := kgo.NewClient(
			kgo.SeedBrokers(addr),
			kgo.ConsumeTopics(channel),
			kgo.ConsumerGroup("test-group"),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		)
		if err != nil {
			c1.Close()
			t.Fatalf("create consumer 2: %v", err)
		}

		// Let rebalance happen.
		time.Sleep(2 * time.Second)

		// Both consumers should be connected (no crash).
		// Send an event and verify at least one consumer receives it.
		sendNotify(t, connStr, channel, `{"id":"rebalance-test","data":"after-rebalance"}`)

		readCtx, readCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer readCancel()

		var got bool
		for !got && readCtx.Err() == nil {
			// Poll from either consumer.
			fetches := c1.PollFetches(readCtx)
			fetches.EachRecord(func(r *kgo.Record) {
				var raw map[string]any
				if json.Unmarshal(r.Value, &raw) == nil {
					if raw["id"] == "rebalance-test" {
						got = true
					}
				}
			})
			if !got {
				fetches = c2.PollFetches(readCtx)
				fetches.EachRecord(func(r *kgo.Record) {
					var raw map[string]any
					if json.Unmarshal(r.Value, &raw) == nil {
						if raw["id"] == "rebalance-test" {
							got = true
						}
					}
				})
			}
		}

		c1.Close()
		c2.Close()

		if !got {
			t.Fatal("no consumer received the event after rebalance")
		}

		fmt.Fprintf(os.Stderr, "KafkaServer consumer group rebalance test passed\n")
	})
}
