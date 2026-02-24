//go:build integration

package scenarios

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	natsadapter "github.com/florinutz/pgcdc/adapter/nats"
	"github.com/florinutz/pgcdc/bus"
	"github.com/florinutz/pgcdc/detector/listennotify"
	"github.com/florinutz/pgcdc/event"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/sync/errgroup"
)

func TestScenario_Nats(t *testing.T) {
	connStr := startPostgres(t)

	t.Run("happy path", func(t *testing.T) {
		ctx := context.Background()

		// Start a NATS server with JetStream enabled.
		req := testcontainers.ContainerRequest{
			Image:        "nats:latest",
			ExposedPorts: []string{"4222/tcp"},
			Cmd:          []string{"-js"},
			WaitingFor:   wait.ForLog("Server is ready"),
		}
		natsContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			t.Fatalf("start nats container: %v", err)
		}
		t.Cleanup(func() { _ = natsContainer.Terminate(context.Background()) })

		host, err := natsContainer.Host(ctx)
		if err != nil {
			t.Fatalf("get nats host: %v", err)
		}
		port, err := natsContainer.MappedPort(ctx, "4222")
		if err != nil {
			t.Fatalf("get nats mapped port: %v", err)
		}
		natsURL := fmt.Sprintf("nats://%s:%s", host, port.Port())

		// Create the NATS adapter.
		logger := testLogger()
		a := natsadapter.New(natsURL, "pgcdc", "pgcdc", "", 0, 0, 0, logger)

		// Wire pipeline manually: detector -> bus -> NATS adapter.
		pipelineCtx, pipelineCancel := context.WithCancel(context.Background())
		channel := "nats_test"
		det := listennotify.New(connStr, []string{channel}, 0, 0, logger)
		b := bus.New(64, logger)

		g, gCtx := errgroup.WithContext(pipelineCtx)
		g.Go(func() error { return b.Start(gCtx) })
		g.Go(func() error { return det.Start(gCtx, b.Ingest()) })

		sub, err := b.Subscribe(a.Name())
		if err != nil {
			pipelineCancel()
			t.Fatalf("subscribe nats: %v", err)
		}
		g.Go(func() error { return a.Start(gCtx, sub) })

		t.Cleanup(func() {
			pipelineCancel()
			g.Wait()
		})

		// Wait for detector + NATS adapter to connect.
		time.Sleep(2 * time.Second)

		// Send a NOTIFY event through PostgreSQL.
		payload := `{"op":"INSERT","table":"orders","row":{"id":42,"item":"widget"}}`
		sendNotify(t, connStr, channel, payload)

		// Connect to NATS as a consumer and verify the message arrived.
		nc, err := nats.Connect(natsURL)
		if err != nil {
			t.Fatalf("consumer connect to nats: %v", err)
		}
		defer nc.Close()

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("consumer jetstream init: %v", err)
		}

		// Create an ephemeral consumer on the "pgcdc" stream.
		consCtx, consCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer consCancel()

		cons, err := js.CreateOrUpdateConsumer(consCtx, "pgcdc", jetstream.ConsumerConfig{
			DeliverPolicy: jetstream.DeliverAllPolicy,
			AckPolicy:     jetstream.AckExplicitPolicy,
		})
		if err != nil {
			t.Fatalf("create consumer: %v", err)
		}

		// Fetch messages with a timeout.
		msgs, err := cons.Fetch(1, jetstream.FetchMaxWait(10*time.Second))
		if err != nil {
			t.Fatalf("fetch messages: %v", err)
		}

		var received []jetstream.Msg
		for msg := range msgs.Messages() {
			received = append(received, msg)
			_ = msg.Ack()
		}
		if msgs.Error() != nil {
			// Fetch can return context deadline exceeded if no messages arrived.
			t.Fatalf("fetch error: %v", msgs.Error())
		}

		if len(received) == 0 {
			t.Fatal("expected at least one message from NATS JetStream, got none")
		}

		msg := received[0]

		// Verify the message is a valid event.Event.
		var ev event.Event
		if err := json.Unmarshal(msg.Data(), &ev); err != nil {
			t.Fatalf("unmarshal event: %v\nraw: %s", err, string(msg.Data()))
		}

		if ev.Channel != channel {
			t.Errorf("channel = %q, want %q", ev.Channel, channel)
		}
		if ev.Operation != "INSERT" {
			t.Errorf("operation = %q, want %q", ev.Operation, "INSERT")
		}
		if ev.Source != "listen_notify" {
			t.Errorf("source = %q, want %q", ev.Source, "listen_notify")
		}
		if ev.ID == "" {
			t.Error("event ID is empty")
		}

		// Verify the dedup ID (Nats-Msg-Id header) matches the event ID.
		dedupID := msg.Headers().Get("Nats-Msg-Id")
		if dedupID == "" {
			t.Error("Nats-Msg-Id header is empty (dedup ID missing)")
		}
		if dedupID != ev.ID {
			t.Errorf("Nats-Msg-Id = %q, want event ID %q", dedupID, ev.ID)
		}

		// Verify the NATS subject follows the expected pattern.
		if msg.Subject() != "pgcdc.nats_test" {
			t.Errorf("subject = %q, want %q", msg.Subject(), "pgcdc.nats_test")
		}

		// Verify the payload content round-trips correctly.
		var payloadData map[string]any
		if err := json.Unmarshal(ev.Payload, &payloadData); err != nil {
			t.Fatalf("unmarshal payload: %v", err)
		}
		if payloadData["op"] != "INSERT" {
			t.Errorf("payload op = %v, want INSERT", payloadData["op"])
		}

		// Print to test log for debugging when run with -v.
		fmt.Fprintf(os.Stderr, "NATS message received: subject=%s dedup_id=%s event_id=%s\n",
			msg.Subject(), dedupID, ev.ID)
	})
}
