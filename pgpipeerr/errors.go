package pgpipeerr

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
