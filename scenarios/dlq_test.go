//go:build integration

package scenarios

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/adapter/webhook"
	"github.com/florinutz/pgcdc/bus"
	"github.com/florinutz/pgcdc/detector/listennotify"
	"github.com/florinutz/pgcdc/dlq"
	"github.com/florinutz/pgcdc/event"
	"golang.org/x/sync/errgroup"
)

func TestScenario_DLQCommands(t *testing.T) {
	connStr := startPostgres(t)

	t.Run("happy path: list, replay, purge", func(t *testing.T) {
		// 1. Start a pipeline with webhook adapter (always 500) + DLQ pg_table.
		channel := "dlq_happy"
		dlqTable := "pgcdc_dlq_test_happy"

		logger := testLogger()
		pipelineCtx, pipelineCancel := context.WithCancel(context.Background())

		receiver := newWebhookReceiver(t, func(_ int) int {
			return http.StatusInternalServerError
		})
		// maxRetries=1, fast backoff so retries complete quickly.
		a := webhook.New(receiver.Server.URL, nil, "", 1, 0, 50*time.Millisecond, 100*time.Millisecond, logger)

		pgDLQ := dlq.NewPGTableDLQ(connStr, dlqTable, logger)
		a.SetDLQ(pgDLQ)
		t.Cleanup(func() { _ = pgDLQ.Close() })

		det := listennotify.New(connStr, []string{channel}, 0, 0, logger)
		b := bus.New(64, logger)

		g, gCtx := errgroup.WithContext(pipelineCtx)
		g.Go(func() error { return b.Start(gCtx) })
		g.Go(func() error { return det.Start(gCtx, b.Ingest()) })

		sub, err := b.Subscribe("webhook")
		if err != nil {
			pipelineCancel()
			t.Fatalf("subscribe: %v", err)
		}
		g.Go(func() error { return a.Start(gCtx, sub) })

		time.Sleep(1 * time.Second)

		// 2. NOTIFY → webhook fails → event in DLQ table.
		sendNotify(t, connStr, channel, `{"op":"INSERT","table":"orders","row":{"id":42}}`)

		// Wait for DLQ record.
		store := dlq.NewStore(connStr, dlqTable, logger)
		var records []dlq.StoredRecord
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			records, err = store.List(ctx, dlq.ListFilter{})
			cancel()
			if err == nil && len(records) > 0 {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if len(records) == 0 {
			pipelineCancel()
			_ = g.Wait()
			t.Fatal("expected DLQ record, got none")
		}

		// Stop pipeline so we can replay.
		pipelineCancel()
		_ = g.Wait()

		// 3. pgcdc dlq list → record visible.
		listOut, listErr := runPGCDC("dlq", "list", "--db", connStr, "--dlq-table", dlqTable, "--format", "json")
		if listErr != nil {
			t.Fatalf("dlq list: %v\n%s", listErr, listOut)
		}
		var listed []dlq.StoredRecord
		if err := json.Unmarshal([]byte(listOut), &listed); err != nil {
			t.Fatalf("unmarshal dlq list output: %v\n%s", err, listOut)
		}
		if len(listed) == 0 {
			t.Fatal("dlq list returned no records")
		}
		if listed[0].Adapter != "webhook" {
			t.Errorf("adapter = %q, want webhook", listed[0].Adapter)
		}

		// 4. Start new webhook receiver (returning 200).
		okReceiver := newWebhookReceiver(t, alwaysOK)

		// 5. pgcdc dlq replay --webhook-url <new> → event delivered, replayed_at set.
		replayOut, replayErr := runPGCDC("dlq", "replay",
			"--db", connStr,
			"--dlq-table", dlqTable,
			"--webhook-url", okReceiver.Server.URL,
		)
		if replayErr != nil {
			t.Fatalf("dlq replay: %v\n%s", replayErr, replayOut)
		}

		// Verify webhook received the replayed event.
		req := okReceiver.waitRequest(t, 5*time.Second)
		var ev event.Event
		if err := json.Unmarshal(req.Body, &ev); err != nil {
			t.Fatalf("unmarshal replayed event: %v", err)
		}
		if ev.Operation != "INSERT" {
			t.Errorf("replayed operation = %q, want INSERT", ev.Operation)
		}

		// 6. pgcdc dlq list --pending → no records.
		pendingOut, pendingErr := runPGCDC("dlq", "list", "--db", connStr, "--dlq-table", dlqTable, "--pending", "--format", "json")
		if pendingErr != nil {
			t.Fatalf("dlq list --pending: %v\n%s", pendingErr, pendingOut)
		}
		var pending []dlq.StoredRecord
		if err := json.Unmarshal([]byte(pendingOut), &pending); err != nil {
			// If output is "null" that's also fine (empty array).
			if pendingOut != "null\n" {
				t.Fatalf("unmarshal pending list: %v\n%s", err, pendingOut)
			}
		}
		if len(pending) > 0 {
			t.Errorf("expected 0 pending records, got %d", len(pending))
		}

		// 7. pgcdc dlq purge --replayed --force → table empty.
		purgeOut, purgeErr := runPGCDC("dlq", "purge",
			"--db", connStr,
			"--dlq-table", dlqTable,
			"--replayed", "--force",
		)
		if purgeErr != nil {
			t.Fatalf("dlq purge: %v\n%s", purgeErr, purgeOut)
		}

		// Verify table is empty.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		count, err := store.Count(ctx, dlq.ListFilter{})
		if err != nil {
			t.Fatalf("count after purge: %v", err)
		}
		if count != 0 {
			t.Errorf("expected 0 records after purge, got %d", count)
		}

		fmt.Fprintf(os.Stderr, "DLQ happy path: list=%d replay=%s purge=%d\n",
			len(listed), "ok", count)
	})

	t.Run("replay failure: adapter still failing", func(t *testing.T) {
		dlqTable := "pgcdc_dlq_test_fail"

		// Seed a DLQ record directly.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		pgDLQ := dlq.NewPGTableDLQ(connStr, dlqTable, nil)
		ev, err := event.New("dlq_fail_chan", "INSERT", json.RawMessage(`{"op":"INSERT","table":"orders","row":{"id":99}}`), "test")
		if err != nil {
			t.Fatalf("create event: %v", err)
		}
		if err := pgDLQ.Record(ctx, ev, "webhook", fmt.Errorf("original failure")); err != nil {
			t.Fatalf("seed DLQ record: %v", err)
		}
		_ = pgDLQ.Close()

		// Start a still-failing receiver.
		failReceiver := newWebhookReceiver(t, func(_ int) int {
			return http.StatusInternalServerError
		})

		// Replay to still-failing adapter.
		replayOut, _ := runPGCDC("dlq", "replay",
			"--db", connStr,
			"--dlq-table", dlqTable,
			"--webhook-url", failReceiver.Server.URL,
		)

		// Record should NOT be marked as replayed.
		store := dlq.NewStore(connStr, dlqTable, nil)
		records, err := store.List(ctx, dlq.ListFilter{Pending: true})
		if err != nil {
			t.Fatalf("list pending: %v", err)
		}
		if len(records) == 0 {
			t.Errorf("expected pending record to remain after failed replay, got 0")
		}

		fmt.Fprintf(os.Stderr, "DLQ replay failure: output=%s pending=%d\n", replayOut, len(records))
	})
}
