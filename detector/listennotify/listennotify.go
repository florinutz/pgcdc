package listennotify

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/backoff"
	"github.com/florinutz/pgcdc/pgcdcerr"
	"github.com/jackc/pgx/v5"
)

const source = "listen_notify"

const (
	defaultBackoffBase = 5 * time.Second
	defaultBackoffCap  = 60 * time.Second
)

// Detector implements detector.Detector using PostgreSQL LISTEN/NOTIFY.
// It maintains a dedicated (non-pooled) connection and automatically
// reconnects with exponential backoff when the connection is lost.
type Detector struct {
	dbURL       string
	channels    []string
	backoffBase time.Duration
	backoffCap  time.Duration
	logger      *slog.Logger
	tracer      trace.Tracer
}

// SetTracer sets the OpenTelemetry tracer for creating per-event spans.
func (d *Detector) SetTracer(t trace.Tracer) {
	d.tracer = t
}

// New creates a LISTEN/NOTIFY detector for the given channels.
// Duration parameters default to sensible values when zero.
func New(dbURL string, channels []string, backoffBase, backoffCap time.Duration, logger *slog.Logger) *Detector {
	if backoffBase <= 0 {
		backoffBase = defaultBackoffBase
	}
	if backoffCap <= 0 {
		backoffCap = defaultBackoffCap
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Detector{
		dbURL:       dbURL,
		channels:    channels,
		backoffBase: backoffBase,
		backoffCap:  backoffCap,
		logger:      logger.With("detector", source),
	}
}

// Name returns the detector name.
func (d *Detector) Name() string {
	return source
}

// Start connects to PostgreSQL, subscribes to the configured channels, and
// forwards notifications as events. It blocks until ctx is cancelled.
// The caller owns the events channel; Start does NOT close it.
func (d *Detector) Start(ctx context.Context, events chan<- event.Event) error {
	if len(d.channels) == 0 {
		return fmt.Errorf("listennotify: no channels configured")
	}

	var attempt int
	for {
		runErr := d.run(ctx, events)
		if ctx.Err() != nil {
			return ctx.Err()
		}

		disconnErr := &pgcdcerr.DetectorDisconnectedError{
			Source: source,
			Err:    runErr,
		}

		delay := backoff.Jitter(attempt, d.backoffBase, d.backoffCap)
		d.logger.Error("connection lost, reconnecting",
			"error", disconnErr,
			"attempt", attempt+1,
			"delay", delay,
		)
		attempt++

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

// run performs a single connect-listen-receive cycle.
// It returns when the connection drops or ctx is cancelled.
func (d *Detector) run(ctx context.Context, events chan<- event.Event) error {
	conn, err := pgx.Connect(ctx, d.dbURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	for _, ch := range d.channels {
		safe := pgx.Identifier{ch}.Sanitize()
		if _, err := conn.Exec(ctx, "LISTEN "+safe); err != nil {
			return fmt.Errorf("listen %s: %w", safe, err)
		}
		d.logger.Info("subscribed", "channel", ch)
	}

	d.logger.Info("listening for notifications")
	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			return fmt.Errorf("wait: %w", err)
		}

		op, payload := parsePayload(notification.Payload)

		ev, err := event.New(notification.Channel, op, payload, source)
		if err != nil {
			d.logger.Error("failed to create event", "error", err)
			continue
		}

		if d.tracer != nil {
			_, span := d.tracer.Start(ctx, "pgcdc.detect",
				trace.WithSpanKind(trace.SpanKindProducer),
				trace.WithAttributes(
					attribute.String("pgcdc.event.id", ev.ID),
					attribute.String("pgcdc.channel", ev.Channel),
					attribute.String("pgcdc.operation", op),
					attribute.String("pgcdc.source", source),
				),
			)
			ev.SpanContext = span.SpanContext()
			span.End()
		}

		select {
		case events <- ev:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// parsePayload inspects the notification payload.
// If it is valid JSON containing an "op" field, the value is used as the
// operation and the full payload is preserved. Otherwise the operation
// defaults to "NOTIFY" and the raw text is kept as a JSON string.
func parsePayload(raw string) (operation string, payload json.RawMessage) {
	data := []byte(raw)

	var obj map[string]json.RawMessage
	if json.Unmarshal(data, &obj) == nil {
		if opVal, ok := obj["op"]; ok {
			var op string
			if json.Unmarshal(opVal, &op) == nil && op != "" {
				return op, data
			}
		}
		// Valid JSON but no usable "op" field.
		return "NOTIFY", data
	}

	// Not JSON; wrap the raw text as a JSON string.
	quoted, _ := json.Marshal(raw)
	return "NOTIFY", quoted
}
