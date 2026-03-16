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
	"github.com/florinutz/pgcdc/internal/circuitbreaker"
	"github.com/florinutz/pgcdc/internal/ratelimit"
	"github.com/florinutz/pgcdc/internal/reconnect"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/pgcdcerr"
	"github.com/florinutz/pgcdc/tracing"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

const adapterName = "kafka"

// Pre-allocated header key constants to avoid per-event string construction.
const (
	headerKeyChannel     = "pgcdc-channel"
	headerKeyOperation   = "pgcdc-operation"
	headerKeyEventID     = "pgcdc-event-id"
	headerKeyContentType = "content-type"
)

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
	cb            *circuitbreaker.CircuitBreaker
	limiter       *ratelimit.Limiter
	topicCache    map[string]string
}

// SetTracer sets the OpenTelemetry tracer for per-event spans.
func (a *Adapter) SetTracer(t trace.Tracer) { a.tracer = t }

// SetDLQ sets the dead letter queue for failed deliveries.
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

// Config holds all parameters for the Kafka adapter.
type Config struct {
	Brokers         []string
	Topic           string
	SASLMechanism   string
	SASLUsername    string
	SASLPassword    string
	TLSCAFile       string
	TLSEnabled      bool
	BackoffBase     time.Duration
	BackoffCap      time.Duration
	Encoder         encoding.Encoder
	TransactionalID string
	CBMaxFailures   int
	CBResetTimeout  time.Duration
	RateLimit       float64
	RateLimitBurst  int
}

// New creates a Kafka adapter. Duration parameters default to sensible values
// when zero. If Encoder is nil, events are sent as raw JSON (current behavior).
// When TransactionalID is non-empty, each event is produced inside its own
// Kafka transaction for exactly-once delivery.
func New(cfg Config, logger *slog.Logger) *Adapter {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = 1 * time.Second
	}
	if cfg.BackoffCap <= 0 {
		cfg.BackoffCap = 30 * time.Second
	}
	if len(cfg.Brokers) == 0 {
		cfg.Brokers = []string{"localhost:9092"}
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}

	switch cfg.SASLMechanism {
	case "plain":
		opts = append(opts, kgo.SASL(plain.Auth{User: cfg.SASLUsername, Pass: cfg.SASLPassword}.AsMechanism()))
	case "scram-sha-256":
		opts = append(opts, kgo.SASL(scram.Auth{User: cfg.SASLUsername, Pass: cfg.SASLPassword}.AsSha256Mechanism()))
	case "scram-sha-512":
		opts = append(opts, kgo.SASL(scram.Auth{User: cfg.SASLUsername, Pass: cfg.SASLPassword}.AsSha512Mechanism()))
	}

	if cfg.TLSEnabled {
		tlsCfg := &tls.Config{}
		tlsCfg.MinVersion = tls.VersionTLS12
		if cfg.TLSCAFile != "" {
			pem, err := os.ReadFile(cfg.TLSCAFile)
			if err == nil {
				pool := x509.NewCertPool()
				if pool.AppendCertsFromPEM(pem) {
					tlsCfg.RootCAs = pool
				}
			}
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	transactional := cfg.TransactionalID != ""
	if transactional {
		opts = append(opts, kgo.TransactionalID(cfg.TransactionalID))
	}

	a := &Adapter{
		opts:          opts,
		topic:         cfg.Topic,
		transactional: transactional,
		encoder:       cfg.Encoder,
		backoffBase:   cfg.BackoffBase,
		backoffCap:    cfg.BackoffCap,
		logger:        logger.With("adapter", adapterName),
		limiter:       ratelimit.New(cfg.RateLimit, cfg.RateLimitBurst, adapterName, logger),
		topicCache:    make(map[string]string),
	}
	if cfg.CBMaxFailures > 0 {
		a.cb = circuitbreaker.New(cfg.CBMaxFailures, cfg.CBResetTimeout, logger)
	}
	return a
}

// Start connects to Kafka and publishes events from the channel. It blocks
// until ctx is cancelled. On connection error, it reconnects with exponential
// backoff.
func (a *Adapter) Start(ctx context.Context, events <-chan event.Event) error {
	return reconnect.Loop(ctx, adapterName, a.backoffBase, a.backoffCap,
		a.logger, metrics.KafkaErrors,
		func(ctx context.Context) error {
			return a.run(ctx, events)
		})
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
	// Circuit breaker check.
	if a.cb != nil && !a.cb.Allow() {
		metrics.CircuitBreakerState.WithLabelValues(adapterName).Set(1) // open
		if a.dlqInstance != nil {
			_ = a.dlqInstance.Record(ctx, ev, adapterName, &pgcdcerr.CircuitBreakerOpenError{Adapter: adapterName})
		}
		if a.ackFn != nil && ev.LSN > 0 {
			a.ackFn(ev.LSN)
		}
		return nil
	}

	// Rate limiter.
	if err := a.limiter.Wait(ctx); err != nil {
		return ctx.Err()
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
			return nil
		}
		value = encoded
		contentType = a.encoder.ContentType()
	}

	headers := []kgo.RecordHeader{
		{Key: headerKeyChannel, Value: []byte(ev.Channel)},
		{Key: headerKeyOperation, Value: []byte(ev.Operation)},
		{Key: headerKeyEventID, Value: []byte(ev.ID)},
		{Key: headerKeyContentType, Value: []byte(contentType)},
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
		if a.cb != nil {
			a.cb.RecordFailure()
		}
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
		return &pgcdcerr.KafkaPublishError{Topic: topic, Err: produceErr}
	}

	if a.cb != nil {
		a.cb.RecordSuccess()
	}
	if span != nil {
		span.End()
	}
	metrics.KafkaPublished.Add(1)
	metrics.EventsDelivered.WithLabelValues(adapterName).Inc()
	if !ev.CreatedAt.IsZero() {
		metrics.EventDeliveryLag.WithLabelValues(adapterName).Observe(time.Since(ev.CreatedAt).Seconds())
	}
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
// Results are cached since channel-to-topic mappings are deterministic.
func (a *Adapter) topicForEvent(ev event.Event) string {
	if a.topic != "" {
		return a.topic
	}
	if t, ok := a.topicCache[ev.Channel]; ok {
		return t
	}
	t := strings.ReplaceAll(ev.Channel, ":", ".")
	a.topicCache[ev.Channel] = t
	return t
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
