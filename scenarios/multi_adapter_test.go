//go:build integration

package scenarios

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/adapter/stdout"
	"github.com/florinutz/pgcdc/adapter/webhook"
	"github.com/florinutz/pgcdc/event"
)

func TestScenario_MultiAdapterFanOut(t *testing.T) {
	connStr := startPostgres(t)

	t.Run("happy path", func(t *testing.T) {
		capture := newLineCapture()
		receiver := newWebhookReceiver(t, alwaysOK)

		startPipeline(t, connStr, []string{"fanout_happy"},
			stdout.New(capture, testLogger()),
			webhook.New(receiver.Server.URL, nil, "", 1, 0, 0, 0, testLogger()),
		)
		waitForDetector(t, connStr, "fanout_happy", capture)

		sendNotify(t, connStr, "fanout_happy", `{"op":"INSERT","table":"orders","row":{"id":1}}`)

		// Both adapters receive the same event.
		line := capture.waitLine(t, 5*time.Second)
		req := receiver.waitRequest(t, 5*time.Second)

		var stdoutEv event.Event
		if err := json.Unmarshal([]byte(line), &stdoutEv); err != nil {
			t.Fatalf("stdout: invalid JSON: %v", err)
		}
		if stdoutEv.Operation != "INSERT" {
			t.Errorf("stdout: operation = %q, want INSERT", stdoutEv.Operation)
		}

		var webhookEv event.Event
		if err := json.Unmarshal(req.Body, &webhookEv); err != nil {
			t.Fatalf("webhook: invalid JSON: %v", err)
		}
		if webhookEv.Operation != "INSERT" {
			t.Errorf("webhook: operation = %q, want INSERT", webhookEv.Operation)
		}

		// Both adapters received the exact same event.
		if stdoutEv.ID != webhookEv.ID {
			t.Errorf("event IDs differ: stdout=%q webhook=%q", stdoutEv.ID, webhookEv.ID)
		}
	})

	t.Run("slow webhook doesn't block stdout", func(t *testing.T) {
		capture := newLineCapture()

		// Webhook server that takes 10s per request — simulates a slow consumer.
		slowServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.ReadAll(r.Body)
			time.Sleep(10 * time.Second)
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(func() { slowServer.Close() })

		startPipeline(t, connStr, []string{"fanout_slow"},
			stdout.New(capture, testLogger()),
			webhook.New(slowServer.URL, nil, "", 1, 0, 0, 0, testLogger()),
		)
		waitForDetector(t, connStr, "fanout_slow", capture)

		// Send 3 events rapidly.
		for i := 0; i < 3; i++ {
			sendNotify(t, connStr, "fanout_slow", fmt.Sprintf(`{"op":"INSERT","seq":%d}`, i))
		}

		// Stdout receives all events promptly despite slow webhook.
		for i := 0; i < 3; i++ {
			capture.waitLine(t, 3*time.Second)
		}
	})
}
