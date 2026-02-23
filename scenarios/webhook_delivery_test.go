//go:build integration

package scenarios

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/florinutz/pgpipe/adapter/webhook"
	"github.com/florinutz/pgpipe/event"
)

func TestScenario_WebhookDelivery(t *testing.T) {
	connStr := startPostgres(t)

	t.Run("happy path", func(t *testing.T) {
		signingKey := "test-secret-key"
		receiver := newWebhookReceiver(t, alwaysOK)
		a := webhook.New(receiver.Server.URL, nil, signingKey, 3, 0, 0, 0, testLogger())

		startPipeline(t, connStr, []string{"webhook_happy"}, a)
		time.Sleep(1 * time.Second)

		sendNotify(t, connStr, "webhook_happy", `{"op":"INSERT","table":"orders","row":{"id":1}}`)

		req := receiver.waitRequest(t, 5*time.Second)

		if req.Method != http.MethodPost {
			t.Errorf("method = %q, want POST", req.Method)
		}
		if ct := req.Headers.Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", ct)
		}
		if ua := req.Headers.Get("User-Agent"); ua != "pgpipe/1.0" {
			t.Errorf("User-Agent = %q, want pgpipe/1.0", ua)
		}

		// Verify HMAC signature.
		sig := req.Headers.Get("X-PGPipe-Signature")
		mac := hmac.New(sha256.New, []byte(signingKey))
		mac.Write(req.Body)
		expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
		if sig != expected {
			t.Errorf("X-PGPipe-Signature = %q, want %q", sig, expected)
		}

		// Verify body is a valid event.
		var ev event.Event
		if err := json.Unmarshal(req.Body, &ev); err != nil {
			t.Fatalf("invalid JSON body: %v", err)
		}
		if ev.Operation != "INSERT" {
			t.Errorf("operation = %q, want INSERT", ev.Operation)
		}
	})

	t.Run("retry on 500", func(t *testing.T) {
		receiver := newWebhookReceiver(t, func(n int) int {
			if n == 1 {
				return http.StatusInternalServerError
			}
			return http.StatusOK
		})
		a := webhook.New(receiver.Server.URL, nil, "", 3, 0, 0, 0, testLogger())

		startPipeline(t, connStr, []string{"webhook_retry"}, a)
		time.Sleep(1 * time.Second)

		sendNotify(t, connStr, "webhook_retry", `{"op":"INSERT","table":"orders","row":{"id":2}}`)

		// First request returns 500 (retryable).
		_ = receiver.waitRequest(t, 5*time.Second)

		// Second request succeeds after retry.
		req2 := receiver.waitRequest(t, 10*time.Second)

		var ev event.Event
		if err := json.Unmarshal(req2.Body, &ev); err != nil {
			t.Fatalf("invalid JSON body on retry: %v", err)
		}
		if ev.Operation != "INSERT" {
			t.Errorf("operation = %q, want INSERT", ev.Operation)
		}
	})
}
