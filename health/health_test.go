package health

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestChecker_AllUp(t *testing.T) {
	c := NewChecker()
	c.Register("detector")
	c.Register("bus")
	c.SetStatus("detector", StatusUp)
	c.SetStatus("bus", StatusUp)

	rec := httptest.NewRecorder()
	c.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp response
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Status != StatusUp {
		t.Fatalf("expected status %q, got %q", StatusUp, resp.Status)
	}
}

func TestChecker_AnyDown(t *testing.T) {
	c := NewChecker()
	c.Register("detector")
	c.Register("bus")
	c.SetStatus("detector", StatusUp)
	c.SetStatus("bus", StatusDown)

	rec := httptest.NewRecorder()
	c.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}

	var resp response
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Status != StatusDown {
		t.Fatalf("expected status %q, got %q", StatusDown, resp.Status)
	}
	if resp.Components["bus"].Status != StatusDown {
		t.Fatalf("expected bus component %q, got %q", StatusDown, resp.Components["bus"].Status)
	}
}

func TestChecker_RegisterStartsDown(t *testing.T) {
	c := NewChecker()
	c.Register("detector")

	rec := httptest.NewRecorder()
	c.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}

	var resp response
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Status != StatusDown {
		t.Fatalf("expected status %q, got %q", StatusDown, resp.Status)
	}
}

func TestChecker_SetStatusDetail(t *testing.T) {
	c := NewChecker()
	c.Register("detector")
	c.SetStatusDetail("detector", StatusDegraded, "connection timeout")

	rec := httptest.NewRecorder()
	c.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	var resp response
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	det := resp.Components["detector"]
	if det.Status != StatusDegraded {
		t.Fatalf("expected status %q, got %q", StatusDegraded, det.Status)
	}
	if det.LastError != "connection timeout" {
		t.Fatalf("expected last_error %q, got %q", "connection timeout", det.LastError)
	}
	if det.LastSeen.IsZero() {
		t.Fatal("expected last_seen to be set")
	}
	if time.Since(det.LastSeen) > 5*time.Second {
		t.Fatalf("last_seen too old: %v", det.LastSeen)
	}
}

func TestChecker_SetStatus_ClearsError(t *testing.T) {
	c := NewChecker()
	c.Register("bus")
	c.SetStatusDetail("bus", StatusDown, "broker unreachable")
	c.SetStatus("bus", StatusUp)

	rec := httptest.NewRecorder()
	c.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	var resp response
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	bus := resp.Components["bus"]
	if bus.Status != StatusUp {
		t.Fatalf("expected status %q, got %q", StatusUp, bus.Status)
	}
	if bus.LastError != "" {
		t.Fatalf("expected last_error to be cleared, got %q", bus.LastError)
	}
	if bus.LastSeen.IsZero() {
		t.Fatal("expected last_seen to be set after SetStatus")
	}
}
