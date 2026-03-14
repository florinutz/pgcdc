package registry

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/florinutz/pgcdc/pgcdcerr"
)

func TestClient_Register_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/vnd.schemaregistry.v1+json" {
			t.Errorf("Content-Type = %q", ct)
		}

		var req registerRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		if req.SchemaType != "AVRO" {
			t.Errorf("schemaType = %q, want AVRO", req.SchemaType)
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id": 42}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "", "")
	id, err := c.Register(context.Background(), "test-value", `{"type":"string"}`, SchemaTypeAvro)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if id != 42 {
		t.Errorf("id = %d, want 42", id)
	}
}

func TestClient_Register_Cache(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id": 7}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "", "")
	ctx := context.Background()

	id1, err := c.Register(ctx, "sub", "schema", SchemaTypeAvro)
	if err != nil {
		t.Fatal(err)
	}
	id2, err := c.Register(ctx, "sub", "schema", SchemaTypeAvro)
	if err != nil {
		t.Fatal(err)
	}

	if id1 != id2 {
		t.Errorf("cached id mismatch: %d vs %d", id1, id2)
	}
	if calls != 1 {
		t.Errorf("HTTP calls = %d, want 1 (second should be cached)", calls)
	}
}

func TestClient_Register_BasicAuth(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "myuser" || pass != "mypass" {
			t.Errorf("basic auth = (%q, %q, %v), want (myuser, mypass, true)", user, pass, ok)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id": 1}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "myuser", "mypass")
	if _, err := c.Register(context.Background(), "s", "schema", SchemaTypeAvro); err != nil {
		t.Fatal(err)
	}
}

func TestClient_Register_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error_code":409,"message":"conflict"}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "", "")
	_, err := c.Register(context.Background(), "sub", "schema", SchemaTypeAvro)
	if err == nil {
		t.Fatal("expected error")
	}
	var sre *pgcdcerr.SchemaRegistryError
	if !errors.As(err, &sre) {
		t.Fatalf("expected SchemaRegistryError, got %T", err)
	}
	if sre.StatusCode != http.StatusConflict {
		t.Errorf("status = %d, want 409", sre.StatusCode)
	}
}

func TestClient_Register_NetworkError(t *testing.T) {
	c := New("http://127.0.0.1:1", "", "") // nothing listening
	_, err := c.Register(context.Background(), "sub", "schema", SchemaTypeAvro)
	if err == nil {
		t.Fatal("expected error")
	}
	var sre *pgcdcerr.SchemaRegistryError
	if !errors.As(err, &sre) {
		t.Fatalf("expected SchemaRegistryError, got %T", err)
	}
}

func TestClient_GetByID_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		if r.URL.Path != "/schemas/ids/42" {
			t.Errorf("path = %s, want /schemas/ids/42", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":42,"schema":"{\"type\":\"string\"}","schemaType":"AVRO"}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "", "")
	schema, schemaType, err := c.GetByID(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if schema != `{"type":"string"}` {
		t.Errorf("schema = %q", schema)
	}
	if schemaType != SchemaTypeAvro {
		t.Errorf("type = %q, want AVRO", schemaType)
	}
}

func TestClient_GetByID_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error_code":40403,"message":"not found"}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "", "")
	_, _, err := c.GetByID(context.Background(), 999)
	if err == nil {
		t.Fatal("expected error")
	}
	var sre *pgcdcerr.SchemaRegistryError
	if !errors.As(err, &sre) {
		t.Fatalf("expected SchemaRegistryError, got %T", err)
	}
	if sre.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", sre.StatusCode)
	}
}

func TestClient_GetByID_NetworkError(t *testing.T) {
	c := New("http://127.0.0.1:1", "", "")
	_, _, err := c.GetByID(context.Background(), 1)
	if err == nil {
		t.Fatal("expected error")
	}
	var sre *pgcdcerr.SchemaRegistryError
	if !errors.As(err, &sre) {
		t.Fatalf("expected SchemaRegistryError, got %T", err)
	}
}

func TestClient_GetByID_BasicAuth(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "u" || pass != "p" {
			t.Errorf("basic auth = (%q, %q, %v)", user, pass, ok)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":1,"schema":"s","schemaType":"PROTOBUF"}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "u", "p")
	_, st, err := c.GetByID(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if st != SchemaTypeProtobuf {
		t.Errorf("type = %q, want PROTOBUF", st)
	}
}
