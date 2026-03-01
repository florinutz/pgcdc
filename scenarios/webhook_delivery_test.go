//go:build integration

package scenarios

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/adapter/webhook"
	"github.com/florinutz/pgcdc/event"
)

func TestScenario_WebhookDelivery(t *testing.T) {
	connStr := startPostgres(t)

	t.Run("happy path", func(t *testing.T) {
		signingKey := "test-secret-key"
		receiver := newWebhookReceiver(t, alwaysOK)
		a := webhook.New(receiver.Server.URL, nil, signingKey, 3, 0, 0, 0, testLogger())

		startPipeline(t, connStr, []string{"webhook_happy"}, a)
		waitForDetectorWebhook(t, connStr, "webhook_happy", receiver)

		sendNotify(t, connStr, "webhook_happy", `{"op":"INSERT","table":"orders","row":{"id":1}}`)

		req := receiver.waitRequest(t, 5*time.Second)

		if req.Method != http.MethodPost {
			t.Errorf("method = %q, want POST", req.Method)
		}
		if ct := req.Headers.Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", ct)
		}
		if ua := req.Headers.Get("User-Agent"); ua != "pgcdc/1.0" {
			t.Errorf("User-Agent = %q, want pgcdc/1.0", ua)
		}

		// Verify HMAC signature.
		sig := req.Headers.Get("X-PGCDC-Signature")
		mac := hmac.New(sha256.New, []byte(signingKey))
		mac.Write(req.Body)
		expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
		if sig != expected {
			t.Errorf("X-PGCDC-Signature = %q, want %q", sig, expected)
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
		waitForDetectorWebhook(t, connStr, "webhook_retry", receiver)

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

	t.Run("all retries exhausted", func(t *testing.T) {
		// Always-500 receiver; use atomic flag to flip to 200 later.
		var allow atomic.Bool
		receiver := newWebhookReceiver(t, func(_ int) int {
			if allow.Load() {
				return http.StatusOK
			}
			return http.StatusInternalServerError
		})
		// maxRetries=2 with fast backoff so retries complete quickly.
		a := webhook.New(receiver.Server.URL, nil, "", 2, 0, 50*time.Millisecond, 100*time.Millisecond, testLogger())

		startPipeline(t, connStr, []string{"webhook_exhaust"}, a)
		waitForDetectorWebhook(t, connStr, "webhook_exhaust", receiver)

		// Event 1: all retries will fail (2 attempts total for maxRetries=2).
		sendNotify(t, connStr, "webhook_exhaust", `{"op":"INSERT","table":"orders","row":{"id":10}}`)

		// Wait for 2 failed attempts.
		_ = receiver.waitRequest(t, 5*time.Second)
		_ = receiver.waitRequest(t, 5*time.Second)

		// Flip receiver to 200 and send event 2.
		allow.Store(true)
		sendNotify(t, connStr, "webhook_exhaust", `{"op":"INSERT","table":"orders","row":{"id":11}}`)

		// Event 2 should arrive successfully — adapter survived the exhaustion.
		req := receiver.waitRequest(t, 10*time.Second)
		var ev event.Event
		if err := json.Unmarshal(req.Body, &ev); err != nil {
			t.Fatalf("invalid JSON body after exhaustion: %v", err)
		}
		if ev.Operation != "INSERT" {
			t.Errorf("operation = %q, want INSERT", ev.Operation)
		}
	})
}
