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
