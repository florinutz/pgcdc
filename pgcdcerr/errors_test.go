package pgcdcerr_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/florinutz/pgcdc/pgcdcerr"
)

func TestErrBusClosed(t *testing.T) {
	err := fmt.Errorf("subscribe: %w", pgcdcerr.ErrBusClosed)
	if !errors.Is(err, pgcdcerr.ErrBusClosed) {
		t.Error("errors.Is should match ErrBusClosed")
	}
}

func TestWebhookDeliveryError(t *testing.T) {
	cause := fmt.Errorf("connection refused")
	err := &pgcdcerr.WebhookDeliveryError{
		EventID: "evt-1",
		URL:     "https://example.com/hook",
		Retries: 5,
		Err:     cause,
	}

	// errors.As
	var target *pgcdcerr.WebhookDeliveryError
	if !errors.As(err, &target) {
		t.Fatal("errors.As should match WebhookDeliveryError")
	}
	if target.EventID != "evt-1" {
		t.Errorf("EventID = %q, want evt-1", target.EventID)
	}

	// Unwrap
	if !errors.Is(err, cause) {
		t.Error("errors.Is should match underlying cause via Unwrap")
	}

	// Wrapped
	wrapped := fmt.Errorf("deliver: %w", err)
	if !errors.As(wrapped, &target) {
		t.Fatal("errors.As should match through wrapping")
	}
}

func TestWebhookDeliveryError_StatusCode(t *testing.T) {
	err := &pgcdcerr.WebhookDeliveryError{
		EventID:    "evt-2",
		URL:        "https://example.com/hook",
		StatusCode: 500,
		Retries:    3,
	}
	msg := err.Error()
	if msg == "" {
		t.Error("Error() should return non-empty string")
	}
}

func TestDetectorDisconnectedError(t *testing.T) {
	cause := fmt.Errorf("connection reset")
	err := &pgcdcerr.DetectorDisconnectedError{
		Source: "listen_notify",
		Err:    cause,
	}

	var target *pgcdcerr.DetectorDisconnectedError
	if !errors.As(err, &target) {
		t.Fatal("errors.As should match DetectorDisconnectedError")
	}
	if target.Source != "listen_notify" {
		t.Errorf("Source = %q, want listen_notify", target.Source)
	}

	if !errors.Is(err, cause) {
		t.Error("errors.Is should match underlying cause via Unwrap")
	}
}

// TestTypedErrors verifies errors.As, Unwrap, and Error() for all typed error types.
func TestTypedErrors(t *testing.T) {
	cause := fmt.Errorf("underlying cause")

	tests := []struct {
		name   string
		err    error
		target any
		hasErr bool // has Unwrap (wraps cause)
	}{
		{
			name:   "ExecProcessError",
			err:    &pgcdcerr.ExecProcessError{Command: "cat", Err: cause},
			target: new(*pgcdcerr.ExecProcessError),
			hasErr: true,
		},
		{
			name:   "EmbeddingDeliveryError/with_err",
			err:    &pgcdcerr.EmbeddingDeliveryError{EventID: "e1", Model: "m1", Retries: 2, Err: cause},
			target: new(*pgcdcerr.EmbeddingDeliveryError),
			hasErr: true,
		},
		{
			name:   "EmbeddingDeliveryError/with_status",
			err:    &pgcdcerr.EmbeddingDeliveryError{EventID: "e1", Model: "m1", StatusCode: 429, Retries: 2},
			target: new(*pgcdcerr.EmbeddingDeliveryError),
			hasErr: false,
		},
		{
			name:   "NatsPublishError",
			err:    &pgcdcerr.NatsPublishError{EventID: "e1", Subject: "pgcdc.events", Err: cause},
			target: new(*pgcdcerr.NatsPublishError),
			hasErr: true,
		},
		{
			name:   "OutboxProcessError",
			err:    &pgcdcerr.OutboxProcessError{Table: "outbox", Err: cause},
			target: new(*pgcdcerr.OutboxProcessError),
			hasErr: true,
		},
		{
			name:   "IncrementalSnapshotError",
			err:    &pgcdcerr.IncrementalSnapshotError{SnapshotID: "snap-1", Table: "users", Err: cause},
			target: new(*pgcdcerr.IncrementalSnapshotError),
			hasErr: true,
		},
		{
			name:   "IcebergFlushError",
			err:    &pgcdcerr.IcebergFlushError{Attempts: 3, Err: cause},
			target: new(*pgcdcerr.IcebergFlushError),
			hasErr: true,
		},
		{
			name:   "SchemaRegistryError/with_status",
			err:    &pgcdcerr.SchemaRegistryError{Subject: "users-value", Operation: "register", StatusCode: 409, Err: cause},
			target: new(*pgcdcerr.SchemaRegistryError),
			hasErr: true,
		},
		{
			name:   "SchemaRegistryError/without_status",
			err:    &pgcdcerr.SchemaRegistryError{Subject: "users-value", Operation: "register", Err: cause},
			target: new(*pgcdcerr.SchemaRegistryError),
			hasErr: true,
		},
		{
			name:   "S3UploadError",
			err:    &pgcdcerr.S3UploadError{Attempts: 5, Err: cause},
			target: new(*pgcdcerr.S3UploadError),
			hasErr: true,
		},
		{
			name:   "MySQLReplicationError",
			err:    &pgcdcerr.MySQLReplicationError{Addr: "localhost:3306", Err: cause},
			target: new(*pgcdcerr.MySQLReplicationError),
			hasErr: true,
		},
		{
			name:   "MongoDBChangeStreamError",
			err:    &pgcdcerr.MongoDBChangeStreamError{URI: "mongodb://localhost", Err: cause},
			target: new(*pgcdcerr.MongoDBChangeStreamError),
			hasErr: true,
		},
		{
			name:   "KafkaServerError",
			err:    &pgcdcerr.KafkaServerError{Addr: ":9092", Err: cause},
			target: new(*pgcdcerr.KafkaServerError),
			hasErr: true,
		},
		{
			name:   "ViewError",
			err:    &pgcdcerr.ViewError{View: "user_counts", Err: cause},
			target: new(*pgcdcerr.ViewError),
			hasErr: true,
		},
		{
			name:   "PluginError",
			err:    &pgcdcerr.PluginError{Plugin: "my-plugin", Type: "transform", Err: cause},
			target: new(*pgcdcerr.PluginError),
			hasErr: true,
		},
		{
			name:   "CircuitBreakerOpenError",
			err:    &pgcdcerr.CircuitBreakerOpenError{Adapter: "webhook"},
			target: new(*pgcdcerr.CircuitBreakerOpenError),
			hasErr: false,
		},
		{
			name:   "ValidationError",
			err:    &pgcdcerr.ValidationError{Adapter: "redis", Err: cause},
			target: new(*pgcdcerr.ValidationError),
			hasErr: true,
		},
		{
			name:   "RateLimitExceededError",
			err:    &pgcdcerr.RateLimitExceededError{Adapter: "webhook"},
			target: new(*pgcdcerr.RateLimitExceededError),
			hasErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Error() returns non-empty string.
			msg := tt.err.Error()
			if msg == "" {
				t.Error("Error() returned empty string")
			}

			// errors.As matches the concrete type.
			if !errors.As(tt.err, tt.target) {
				t.Error("errors.As should match the error type")
			}

			// errors.As matches through wrapping.
			wrapped := fmt.Errorf("outer: %w", tt.err)
			if !errors.As(wrapped, tt.target) {
				t.Error("errors.As should match through wrapping")
			}

			// Unwrap returns cause (if applicable).
			if tt.hasErr {
				if !errors.Is(tt.err, cause) {
					t.Error("errors.Is should match underlying cause via Unwrap")
				}
			}
		})
	}
}
