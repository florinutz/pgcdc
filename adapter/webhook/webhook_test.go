package webhook_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/florinutz/pgpipe/adapter/webhook"
	"github.com/florinutz/pgpipe/event"
)

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestEvent() event.Event {
	return event.Event{
		ID:        "evt-test-1",
		Channel:   "orders",
		Operation: "INSERT",
		Payload:   json.RawMessage(`{"order_id":42}`),
		Source:    "test",
		CreatedAt: time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC),
	}
}

func TestHMACSigning(t *testing.T) {
	signingKey := "my-secret-key"
	var gotSig string
	var gotBody []byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSig = r.Header.Get("X-PGPipe-Signature")
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	a := webhook.New(srv.URL, nil, signingKey, 1, 0, 0, 0, newTestLogger())

	events := make(chan event.Event, 1)
	events <- newTestEvent()
	close(events)

	err := a.Start(context.Background(), events)
	if err != nil {
		t.Fatalf("Start returned unexpected error: %v", err)
	}

	// Verify the signature.
	mac := hmac.New(sha256.New, []byte(signingKey))
	mac.Write(gotBody)
	expectedSig := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	if gotSig != expectedSig {
		t.Errorf("X-PGPipe-Signature = %q, want %q", gotSig, expectedSig)
	}
}

func TestHMACSigning_NotSetWhenNoKey(t *testing.T) {
	var gotSig string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSig = r.Header.Get("X-PGPipe-Signature")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	a := webhook.New(srv.URL, nil, "", 1, 0, 0, 0, newTestLogger())

	events := make(chan event.Event, 1)
	events <- newTestEvent()
	close(events)

	err := a.Start(context.Background(), events)
	if err != nil {
		t.Fatalf("Start returned unexpected error: %v", err)
	}

	if gotSig != "" {
		t.Errorf("X-PGPipe-Signature should be empty when no signing key, got %q", gotSig)
	}
}

func TestBackoffWithinBounds(t *testing.T) {
	// Run many iterations to exercise the jitter range.
	for attempt := range 10 {
		for range 100 {
			d := webhook.Backoff(attempt, 0, 0)
			if d < 0 {
				t.Errorf("Backoff(%d) = %v, want >= 0", attempt, d)
			}
			// Cap is 32s.
			if d > 32*time.Second {
				t.Errorf("Backoff(%d) = %v, want <= 32s", attempt, d)
			}
		}
	}
}

func TestBackoffExponentialGrowth(t *testing.T) {
	// Collect many samples per attempt and verify the max envelope grows.
	// attempt 0: max ~1s, attempt 1: max ~2s, attempt 5: max ~32s (capped)
	samples := 5000
	for _, tc := range []struct {
		attempt int
		maxCap  time.Duration
	}{
		{0, 1 * time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 16 * time.Second},
		{5, 32 * time.Second},
		{6, 32 * time.Second}, // Should be capped.
		{10, 32 * time.Second},
	} {
		for range samples {
			d := webhook.Backoff(tc.attempt, 0, 0)
			if d > tc.maxCap {
				t.Errorf("Backoff(%d) = %v, exceeds expected cap %v",
					tc.attempt, d, tc.maxCap)
			}
		}
	}
}
