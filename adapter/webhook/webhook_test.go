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
