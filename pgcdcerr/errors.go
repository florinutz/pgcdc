package pgcdcerr

import (
	"errors"
	"fmt"
)

// ErrBusClosed is returned when subscribing to a bus that has already stopped.
var ErrBusClosed = errors.New("bus: already stopped")

// WebhookDeliveryError indicates that a webhook delivery failed after exhausting retries.
type WebhookDeliveryError struct {
	EventID    string
	URL        string
	StatusCode int
	Retries    int
	Err        error
}

func (e *WebhookDeliveryError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("webhook delivery failed for event %s to %s after %d retries: %v", e.EventID, e.URL, e.Retries, e.Err)
	}
	return fmt.Sprintf("webhook delivery failed for event %s to %s after %d retries (last status %d)", e.EventID, e.URL, e.Retries, e.StatusCode)
}

func (e *WebhookDeliveryError) Unwrap() error {
	return e.Err
}

// DetectorDisconnectedError indicates that a detector lost its connection.
type DetectorDisconnectedError struct {
	Source string
	Err    error
}

func (e *DetectorDisconnectedError) Error() string {
	return fmt.Sprintf("detector %s disconnected: %v", e.Source, e.Err)
}

func (e *DetectorDisconnectedError) Unwrap() error {
	return e.Err
}

// ExecProcessError indicates that an exec adapter subprocess failed.
type ExecProcessError struct {
	Command string
	Err     error
}

func (e *ExecProcessError) Error() string {
	return fmt.Sprintf("exec process %q failed: %v", e.Command, e.Err)
}

func (e *ExecProcessError) Unwrap() error {
	return e.Err
}

// EmbeddingDeliveryError indicates that an embedding API call failed after exhausting retries.
type EmbeddingDeliveryError struct {
	EventID    string
	Model      string
	StatusCode int
	Retries    int
	Err        error
}

func (e *EmbeddingDeliveryError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("embedding delivery failed for event %s (model %s) after %d retries: %v", e.EventID, e.Model, e.Retries, e.Err)
	}
	return fmt.Sprintf("embedding delivery failed for event %s (model %s) after %d retries (last status %d)", e.EventID, e.Model, e.Retries, e.StatusCode)
}

func (e *EmbeddingDeliveryError) Unwrap() error {
	return e.Err
}

// NatsPublishError indicates that a NATS publish failed.
type NatsPublishError struct {
	EventID string
	Subject string
	Err     error
}

func (e *NatsPublishError) Error() string {
	return fmt.Sprintf("nats publish failed for event %s to subject %s: %v", e.EventID, e.Subject, e.Err)
}

func (e *NatsPublishError) Unwrap() error {
	return e.Err
}

// OutboxProcessError indicates that an outbox poll cycle failed.
type OutboxProcessError struct {
	Table string
	Err   error
}

func (e *OutboxProcessError) Error() string {
	return fmt.Sprintf("outbox process failed for table %s: %v", e.Table, e.Err)
}

func (e *OutboxProcessError) Unwrap() error {
	return e.Err
}

// IncrementalSnapshotError indicates that an incremental snapshot failed.
type IncrementalSnapshotError struct {
	SnapshotID string
	Table      string
	Err        error
}

func (e *IncrementalSnapshotError) Error() string {
	return fmt.Sprintf("incremental snapshot %s for table %s failed: %v", e.SnapshotID, e.Table, e.Err)
}

func (e *IncrementalSnapshotError) Unwrap() error {
	return e.Err
}

// IcebergFlushError indicates that the Iceberg adapter failed to flush after consecutive attempts.
type IcebergFlushError struct {
	Attempts int
	Err      error
}

func (e *IcebergFlushError) Error() string {
	return fmt.Sprintf("iceberg flush failed after %d consecutive attempts: %v", e.Attempts, e.Err)
}

func (e *IcebergFlushError) Unwrap() error {
	return e.Err
}

// SchemaRegistryError indicates a Schema Registry operation failed.
type SchemaRegistryError struct {
	Subject    string
	Operation  string
	StatusCode int
	Err        error
}

func (e *SchemaRegistryError) Error() string {
	if e.StatusCode > 0 {
		return fmt.Sprintf("schema registry %s for subject %q failed (HTTP %d): %v", e.Operation, e.Subject, e.StatusCode, e.Err)
	}
	return fmt.Sprintf("schema registry %s for subject %q failed: %v", e.Operation, e.Subject, e.Err)
}

func (e *SchemaRegistryError) Unwrap() error { return e.Err }

// S3UploadError indicates that the S3 adapter failed to upload after consecutive attempts.
type S3UploadError struct {
	Attempts int
	Err      error
}

func (e *S3UploadError) Error() string {
	return fmt.Sprintf("s3 upload failed after %d consecutive attempts: %v", e.Attempts, e.Err)
}

func (e *S3UploadError) Unwrap() error {
	return e.Err
}

// MySQLReplicationError indicates that the MySQL binlog detector encountered an error.
type MySQLReplicationError struct {
	Addr string
	Err  error
}

func (e *MySQLReplicationError) Error() string {
	return fmt.Sprintf("mysql replication error (addr %s): %v", e.Addr, e.Err)
}

func (e *MySQLReplicationError) Unwrap() error {
	return e.Err
}

// MongoDBChangeStreamError indicates that the MongoDB change stream detector encountered an error.
type MongoDBChangeStreamError struct {
	URI string
	Err error
}

func (e *MongoDBChangeStreamError) Error() string {
	return fmt.Sprintf("mongodb change stream error (uri %s): %v", e.URI, e.Err)
}

func (e *MongoDBChangeStreamError) Unwrap() error {
	return e.Err
}

// KafkaServerError indicates that the Kafka protocol server adapter encountered an error.
type KafkaServerError struct {
	Addr string
	Err  error
}

func (e *KafkaServerError) Error() string {
	return fmt.Sprintf("kafkaserver error (addr %s): %v", e.Addr, e.Err)
}

func (e *KafkaServerError) Unwrap() error {
	return e.Err
}

// ViewError indicates that a view engine operation failed.
type ViewError struct {
	View string
	Err  error
}

func (e *ViewError) Error() string {
	return fmt.Sprintf("view %s: %v", e.View, e.Err)
}

func (e *ViewError) Unwrap() error { return e.Err }

// PluginError indicates that a Wasm plugin call failed.
type PluginError struct {
	Plugin string
	Type   string // "transform", "adapter", "dlq", "checkpoint"
	Err    error
}

func (e *PluginError) Error() string {
	return fmt.Sprintf("plugin %s (%s): %s", e.Plugin, e.Type, e.Err)
}

func (e *PluginError) Unwrap() error { return e.Err }

// CircuitBreakerOpenError indicates that a delivery was skipped because the
// circuit breaker is in open state.
type CircuitBreakerOpenError struct {
	Adapter string
}

func (e *CircuitBreakerOpenError) Error() string {
	return fmt.Sprintf("circuit breaker open for adapter %s", e.Adapter)
}

// ValidationError indicates that an adapter's startup validation failed.
type ValidationError struct {
	Adapter string
	Err     error
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation failed for adapter %s: %v", e.Adapter, e.Err)
}

func (e *ValidationError) Unwrap() error { return e.Err }

// RateLimitExceededError indicates that a rate limiter wait was cancelled.
type RateLimitExceededError struct {
	Adapter string
}

func (e *RateLimitExceededError) Error() string {
	return fmt.Sprintf("rate limit exceeded for adapter %s", e.Adapter)
}
