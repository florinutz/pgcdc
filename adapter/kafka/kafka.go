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
	"github.com/florinutz/pgcdc/pgcdcerr"
	"github.com/florinutz/pgcdc/tracing"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

const adapterName = "kafka"

// Adapter publishes events to a Kafka topic.
type Adapter struct {
	opts          []kgo.Opt
	topic         string // fixed topic; empty = per-channel mapping
	transactional bool   // true when TransactionalID is set
	encoder       encoding.Encoder
	dlqInstance   dlq.DLQ
	ackFn         adapter.AckFunc
	backoffBase   time.Duration
	backoffCap    time.Duration
	logger        *slog.Logger
	tracer        trace.Tracer
}

// SetTracer implements adapter.Traceable.
func (a *Adapter) SetTracer(t trace.Tracer) { a.tracer = t }

// SetDLQ implements pgcdc.DLQAware.
func (a *Adapter) SetDLQ(d dlq.DLQ) { a.dlqInstance = d }

// SetAckFunc implements adapter.Acknowledger.
func (a *Adapter) SetAckFunc(fn adapter.AckFunc) { a.ackFn = fn }

// Name returns the adapter name.
func (a *Adapter) Name() string { return adapterName }

// Validate checks Kafka broker connectivity by creating a temporary client
// and calling Metadata.
func (a *Adapter) Validate(ctx context.Context) error {
	client, err := kgo.NewClient(a.opts...)
	if err != nil {
		return fmt.Errorf("create kafka client: %w", err)
	}
	defer client.Close()
	if err := client.Ping(ctx); err != nil {
		return fmt.Errorf("kafka ping: %w", err)
	}
	return nil
}

// Drain flushes any buffered Kafka records.
func (a *Adapter) Drain(ctx context.Context) error {
	// franz-go client is per-run; nothing to drain at pipeline level.
	// The run loop handles its own flushing on context cancel.
	return nil
}

// New creates a Kafka adapter. Duration parameters default to sensible values
// when zero. If encoder is nil, events are sent as raw JSON (current behavior).
// When transactionalID is non-empty, each event is produced inside its own
// Kafka transaction for exactly-once delivery.
func New(
	brokers []string,
	topic, saslMechanism, saslUser, saslPass, tlsCAFile string,
	tlsEnabled bool,
	backoffBase, backoffCap time.Duration,
	encoder encoding.Encoder,
	logger *slog.Logger,
	transactionalID string,
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

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}

	switch saslMechanism {
	case "plain":
		opts = append(opts, kgo.SASL(plain.Auth{User: saslUser, Pass: saslPass}.AsMechanism()))
	case "scram-sha-256":
		opts = append(opts, kgo.SASL(scram.Auth{User: saslUser, Pass: saslPass}.AsSha256Mechanism()))
	case "scram-sha-512":
		opts = append(opts, kgo.SASL(scram.Auth{User: saslUser, Pass: saslPass}.AsSha512Mechanism()))
	}

	if tlsEnabled {
		tlsCfg := &tls.Config{}
		if tlsCAFile != "" {
			pem, err := os.ReadFile(tlsCAFile)
			if err == nil {
				pool := x509.NewCertPool()
				if pool.AppendCertsFromPEM(pem) {
					tlsCfg.RootCAs = pool
				}
			}
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	transactional := transactionalID != ""
	if transactional {
		opts = append(opts, kgo.TransactionalID(transactionalID))
	}

	return &Adapter{
		opts:          opts,
		topic:         topic,
		transactional: transactional,
		encoder:       encoder,
		backoffBase:   backoffBase,
		backoffCap:    backoffCap,
		logger:        logger.With("adapter", adapterName),
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
	client, err := kgo.NewClient(a.opts...)
	if err != nil {
		return fmt.Errorf("create kafka client: %w", err)
	}
	defer client.Close()

	a.logger.Info("kafka client ready")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-events:
			if !ok {
				return nil
			}

			if err := a.handleEvent(ctx, client, ev); err != nil {
				return err
			}
		}
	}
}

func (a *Adapter) handleEvent(ctx context.Context, client *kgo.Client, ev event.Event) error {
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
			return nil
		}
		value = encoded
		contentType = a.encoder.ContentType()
	}

	headers := []kgo.RecordHeader{
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

	record := &kgo.Record{
		Topic:   topic,
		Key:     []byte(ev.ID),
		Value:   value,
		Headers: headers,
	}

	start := time.Now()

	var produceErr error
	if a.transactional {
		produceErr = a.produceTransactional(ctx, client, record)
	} else {
		produceErr = client.ProduceSync(ctx, record).FirstErr()
	}
	metrics.KafkaPublishDuration.Observe(time.Since(start).Seconds())

	if produceErr != nil {
		metrics.KafkaErrors.Add(1)
		if span != nil {
			span.RecordError(produceErr)
			span.SetStatus(codes.Error, produceErr.Error())
			span.End()
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if isTerminalError(produceErr) {
			a.logger.Warn("kafka terminal write error, recording to DLQ",
				"error", produceErr,
				"event_id", ev.ID,
				"topic", topic,
			)
			if a.dlqInstance != nil {
				_ = a.dlqInstance.Record(ctx, ev, adapterName, produceErr)
			}
			if a.ackFn != nil && ev.LSN > 0 {
				a.ackFn(ev.LSN)
			}
			return nil
		}
		return pgcdcerr.WrapEvent(produceErr, adapterName, ev)
	}

	if span != nil {
		span.End()
	}
	metrics.KafkaPublished.Add(1)
	metrics.EventsDelivered.WithLabelValues(adapterName).Inc()
	if a.transactional {
		metrics.KafkaTransactions.Add(1)
	}
	if a.ackFn != nil && ev.LSN > 0 {
		a.ackFn(ev.LSN)
	}
	return nil
}

// produceTransactional produces a single record inside a Kafka transaction.
func (a *Adapter) produceTransactional(ctx context.Context, client *kgo.Client, record *kgo.Record) error {
	if err := client.BeginTransaction(); err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	if err := client.ProduceSync(ctx, record).FirstErr(); err != nil {
		// Abort the transaction using a background context since the
		// original ctx may already be cancelled.
		abortCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.EndTransaction(abortCtx, kgo.TryAbort)
		metrics.KafkaTransactionErrors.Add(1)
		return err
	}

	if err := client.EndTransaction(ctx, kgo.TryCommit); err != nil {
		metrics.KafkaTransactionErrors.Add(1)
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
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
	var ke *kerr.Error
	if errors.As(err, &ke) {
		return !ke.Retriable
	}
	return false
}
