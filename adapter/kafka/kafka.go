package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/dlq"
	"github.com/florinutz/pgcdc/encoding"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/backoff"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/tracing"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

const adapterName = "kafka"

// Adapter publishes events to a Kafka topic.
type Adapter struct {
	brokers     []string
	topic       string // fixed topic; empty = per-channel mapping
	encoder     encoding.Encoder
	saslMech    sasl.Mechanism
	tlsConfig   *tls.Config
	dlqInstance dlq.DLQ
	ackFn       adapter.AckFunc
	backoffBase time.Duration
	backoffCap  time.Duration
	logger      *slog.Logger
	tracer      trace.Tracer
}

// SetTracer implements adapter.Traceable.
func (a *Adapter) SetTracer(t trace.Tracer) { a.tracer = t }

// SetDLQ implements pgcdc.DLQAware.
func (a *Adapter) SetDLQ(d dlq.DLQ) { a.dlqInstance = d }

// SetAckFunc implements adapter.Acknowledger.
func (a *Adapter) SetAckFunc(fn adapter.AckFunc) { a.ackFn = fn }

// Name returns the adapter name.
func (a *Adapter) Name() string { return adapterName }

// New creates a Kafka adapter. Duration parameters default to sensible values
// when zero. If encoder is nil, events are sent as raw JSON (current behavior).
func New(
	brokers []string,
	topic, saslMechanism, saslUser, saslPass, tlsCAFile string,
	tlsEnabled bool,
	backoffBase, backoffCap time.Duration,
	encoder encoding.Encoder,
	logger *slog.Logger,
) *Adapter {
	if logger == nil {
		logger = slog.Default()
	}
	if backoffBase <= 0 {
		backoffBase = 1 * time.Second
	}
	if backoffCap <= 0 {
		backoffCap = 30 * time.Second
	}
	if len(brokers) == 0 {
		brokers = []string{"localhost:9092"}
	}

	var saslMech sasl.Mechanism
	switch saslMechanism {
	case "plain":
		saslMech = plain.Mechanism{Username: saslUser, Password: saslPass}
	case "scram-sha-256":
		if m, err := scram.Mechanism(scram.SHA256, saslUser, saslPass); err == nil {
			saslMech = m
		}
	case "scram-sha-512":
		if m, err := scram.Mechanism(scram.SHA512, saslUser, saslPass); err == nil {
			saslMech = m
		}
	}

	var tlsCfg *tls.Config
	if tlsEnabled {
		tlsCfg = &tls.Config{}
		if tlsCAFile != "" {
			pem, err := os.ReadFile(tlsCAFile)
			if err == nil {
				pool := x509.NewCertPool()
				if pool.AppendCertsFromPEM(pem) {
					tlsCfg.RootCAs = pool
				}
			}
		}
	}

	return &Adapter{
		brokers:     brokers,
		topic:       topic,
		encoder:     encoder,
		saslMech:    saslMech,
		tlsConfig:   tlsCfg,
		backoffBase: backoffBase,
		backoffCap:  backoffCap,
		logger:      logger.With("adapter", adapterName),
	}
}

// Start connects to Kafka and publishes events from the channel. It blocks
// until ctx is cancelled. On connection error, it reconnects with exponential
// backoff.
func (a *Adapter) Start(ctx context.Context, events <-chan event.Event) error {
	var attempt int
	for {
		err := a.run(ctx, events)
		if ctx.Err() != nil {
			return ctx.Err()
		}

		delay := backoff.Jitter(attempt, a.backoffBase, a.backoffCap)
		a.logger.Error("kafka connection lost, reconnecting",
			"error", err,
			"attempt", attempt+1,
			"delay", delay,
		)
		metrics.KafkaErrors.Add(1)
		attempt++

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

func (a *Adapter) run(ctx context.Context, events <-chan event.Event) error {
	transport := &kafkago.Transport{}
	if a.saslMech != nil {
		transport.SASL = a.saslMech
	}
	if a.tlsConfig != nil {
		transport.TLS = a.tlsConfig
	}

	w := &kafkago.Writer{
		Addr:         kafkago.TCP(a.brokers...),
		RequiredAcks: kafkago.RequireAll,
		Transport:    transport,
		Logger: kafkago.LoggerFunc(func(msg string, args ...interface{}) {
			a.logger.Debug(fmt.Sprintf(msg, args...))
		}),
		ErrorLogger: kafkago.LoggerFunc(func(msg string, args ...interface{}) {
			a.logger.Warn(fmt.Sprintf(msg, args...))
		}),
	}
	defer func() { _ = w.Close() }()

	a.logger.Info("kafka writer ready", "brokers", a.brokers)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-events:
			if !ok {
				return nil
			}

			topic := a.topicForEvent(ev)

			value := ev.Payload
			contentType := "application/json"
			if a.encoder != nil {
				encoded, encErr := a.encoder.Encode(ev, ev.Payload)
				if encErr != nil {
					metrics.EncodingErrors.Add(1)
					a.logger.Warn("encoding failed, recording to DLQ",
						"error", encErr,
						"event_id", ev.ID,
						"topic", topic,
					)
					if a.dlqInstance != nil {
						_ = a.dlqInstance.Record(ctx, ev, adapterName, encErr)
					}
					if a.ackFn != nil && ev.LSN > 0 {
						a.ackFn(ev.LSN)
					}
					continue
				}
				value = encoded
				contentType = a.encoder.ContentType()
			}

			headers := []kafkago.Header{
				{Key: "pgcdc-channel", Value: []byte(ev.Channel)},
				{Key: "pgcdc-operation", Value: []byte(ev.Operation)},
				{Key: "pgcdc-event-id", Value: []byte(ev.ID)},
				{Key: "content-type", Value: []byte(contentType)},
			}

			// Create delivery span and inject trace context into Kafka headers.
			var span trace.Span
			writeCtx := ctx
			if a.tracer != nil {
				var opts []trace.SpanStartOption
				opts = append(opts,
					trace.WithSpanKind(trace.SpanKindConsumer),
					trace.WithAttributes(
						attribute.String("pgcdc.adapter", adapterName),
						attribute.String("pgcdc.event.id", ev.ID),
						attribute.String("pgcdc.channel", ev.Channel),
						attribute.String("pgcdc.operation", ev.Operation),
					),
				)
				if ev.SpanContext.IsValid() {
					opts = append(opts, trace.WithLinks(trace.Link{SpanContext: ev.SpanContext}))
					writeCtx = trace.ContextWithRemoteSpanContext(ctx, ev.SpanContext)
				}
				writeCtx, span = a.tracer.Start(writeCtx, "pgcdc.adapter.deliver", opts...)
				otel.GetTextMapPropagator().Inject(writeCtx, propagation.TextMapCarrier(tracing.KafkaCarrier{Headers: &headers}))
			}

			msg := kafkago.Message{
				Topic:   topic,
				Key:     []byte(ev.ID),
				Value:   value,
				Headers: headers,
			}

			start := time.Now()
			err := w.WriteMessages(ctx, msg)
			metrics.KafkaPublishDuration.Observe(time.Since(start).Seconds())

			if err != nil {
				metrics.KafkaErrors.Add(1)
				if span != nil {
					span.RecordError(err)
					span.SetStatus(codes.Error, err.Error())
					span.End()
				}
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if isTerminalError(err) {
					a.logger.Warn("kafka terminal write error, recording to DLQ",
						"error", err,
						"event_id", ev.ID,
						"topic", topic,
					)
					if a.dlqInstance != nil {
						_ = a.dlqInstance.Record(ctx, ev, adapterName, err)
					}
					if a.ackFn != nil && ev.LSN > 0 {
						a.ackFn(ev.LSN)
					}
					continue
				}
				return fmt.Errorf("write message: %w", err)
			}

			if span != nil {
				span.End()
			}
			metrics.KafkaPublished.Add(1)
			metrics.EventsDelivered.WithLabelValues(adapterName).Inc()
			if a.ackFn != nil && ev.LSN > 0 {
				a.ackFn(ev.LSN)
			}
		}
	}
}

// topicForEvent returns the Kafka topic for an event.
// If a fixed topic is configured, it is always used.
// Otherwise the channel is mapped: "pgcdc:orders" → "pgcdc.orders".
func (a *Adapter) topicForEvent(ev event.Event) string {
	if a.topic != "" {
		return a.topic
	}
	return strings.ReplaceAll(ev.Channel, ":", ".")
}

// isTerminalError returns true if the error indicates a permanent failure that
// reconnecting won't fix (e.g. authorization denied, message too large).
// These events are sent to the DLQ rather than triggering a reconnect loop.
func isTerminalError(err error) bool {
	// WriteErrors wraps per-message errors; unwrap and check each one.
	if we, ok := err.(kafkago.WriteErrors); ok {
		for _, e := range we {
			if e != nil && isTerminalError(e) {
				return true
			}
		}
		return false
	}
	var ke kafkago.Error
	if errors.As(err, &ke) {
		return !ke.Temporary()
	}
	return false
}
