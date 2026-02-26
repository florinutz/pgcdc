package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	natsclient "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/encoding"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/backoff"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/pgcdcerr"
)

const adapterName = "nats"

// Adapter publishes events to a NATS JetStream stream.
type Adapter struct {
	url           string
	subjectPrefix string
	streamName    string
	credFile      string
	encoder       encoding.Encoder
	maxAge        time.Duration
	backoffBase   time.Duration
	backoffCap    time.Duration
	logger        *slog.Logger
	ackFn         adapter.AckFunc
	tracer        trace.Tracer
}

// SetTracer implements adapter.Traceable.
func (a *Adapter) SetTracer(t trace.Tracer) { a.tracer = t }

// SetAckFunc implements adapter.Acknowledger.
func (a *Adapter) SetAckFunc(fn adapter.AckFunc) { a.ackFn = fn }

// New creates a NATS JetStream adapter. Duration parameters default to sensible
// values when zero. If encoder is nil, events are sent as JSON (current behavior).
func New(
	url, subjectPrefix, streamName, credFile string,
	maxAge, backoffBase, backoffCap time.Duration,
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
	if maxAge <= 0 {
		maxAge = 24 * time.Hour
	}
	if subjectPrefix == "" {
		subjectPrefix = "pgcdc"
	}
	if streamName == "" {
		streamName = "pgcdc"
	}
	return &Adapter{
		url:           url,
		subjectPrefix: subjectPrefix,
		streamName:    streamName,
		credFile:      credFile,
		encoder:       encoder,
		maxAge:        maxAge,
		backoffBase:   backoffBase,
		backoffCap:    backoffCap,
		logger:        logger.With("adapter", adapterName),
	}
}

// Name returns the adapter name.
func (a *Adapter) Name() string {
	return adapterName
}

// Validate checks NATS connectivity by connecting and immediately closing.
func (a *Adapter) Validate(ctx context.Context) error {
	var opts []natsclient.Option
	if a.credFile != "" {
		opts = append(opts, natsclient.UserCredentials(a.credFile))
	}
	nc, err := natsclient.Connect(a.url, opts...)
	if err != nil {
		return fmt.Errorf("nats connect: %w", err)
	}
	nc.Close()
	return nil
}

// Start connects to NATS and publishes events from the channel. It blocks until
// ctx is cancelled. On NATS disconnection, it reconnects with exponential backoff.
func (a *Adapter) Start(ctx context.Context, events <-chan event.Event) error {
	var attempt int
	for {
		err := a.run(ctx, events)
		if ctx.Err() != nil {
			return ctx.Err()
		}

		delay := backoff.Jitter(attempt, a.backoffBase, a.backoffCap)
		a.logger.Error("nats connection lost, reconnecting",
			"error", err,
			"attempt", attempt+1,
			"delay", delay,
		)
		metrics.NatsErrors.Inc()
		attempt++

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

func (a *Adapter) run(ctx context.Context, events <-chan event.Event) error {
	opts := []natsclient.Option{
		natsclient.Name("pgcdc"),
		natsclient.MaxReconnects(-1),
	}
	if a.credFile != "" {
		opts = append(opts, natsclient.UserCredentials(a.credFile))
	}

	nc, err := natsclient.Connect(a.url, opts...)
	if err != nil {
		return fmt.Errorf("nats connect: %w", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("jetstream init: %w", err)
	}

	// Ensure stream exists with the configured subject filter.
	streamCfg := jetstream.StreamConfig{
		Name:     a.streamName,
		Subjects: []string{a.subjectPrefix + ".>"},
		MaxAge:   a.maxAge,
	}
	_, err = js.CreateOrUpdateStream(ctx, streamCfg)
	if err != nil {
		return fmt.Errorf("create/update stream: %w", err)
	}

	a.logger.Info("nats connected",
		"url", a.url,
		"stream", a.streamName,
		"subject_prefix", a.subjectPrefix,
	)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-events:
			if !ok {
				return nil
			}

			subject := a.eventSubject(ev.Channel)
			data, err := json.Marshal(ev)
			if err != nil {
				a.logger.Error("marshal event failed", "error", err, "event_id", ev.ID)
				continue
			}

			if a.encoder != nil {
				encoded, encErr := a.encoder.Encode(ev, data)
				if encErr != nil {
					metrics.EncodingErrors.Add(1)
					a.logger.Error("encoding failed, skipping event", "error", encErr, "event_id", ev.ID)
					continue
				}
				data = encoded
			}

			// Build NATS message with headers for trace context and dedup.
			msg := &natsclient.Msg{
				Subject: subject,
				Data:    data,
				Header:  natsclient.Header{},
			}
			msg.Header.Set("Nats-Msg-Id", ev.ID)

			// Create delivery span and inject trace context into NATS headers.
			var span trace.Span
			if a.tracer != nil {
				var spanOpts []trace.SpanStartOption
				spanOpts = append(spanOpts,
					trace.WithSpanKind(trace.SpanKindConsumer),
					trace.WithAttributes(
						attribute.String("pgcdc.adapter", adapterName),
						attribute.String("pgcdc.event.id", ev.ID),
						attribute.String("pgcdc.channel", ev.Channel),
						attribute.String("pgcdc.operation", ev.Operation),
					),
				)
				pubCtx := ctx
				if ev.SpanContext.IsValid() {
					spanOpts = append(spanOpts, trace.WithLinks(trace.Link{SpanContext: ev.SpanContext}))
					pubCtx = trace.ContextWithRemoteSpanContext(ctx, ev.SpanContext)
				}
				pubCtx, span = a.tracer.Start(pubCtx, "pgcdc.adapter.deliver", spanOpts...)
				// nats.Header is http.Header — use propagation.HeaderCarrier directly.
				otel.GetTextMapPropagator().Inject(pubCtx, propagation.HeaderCarrier(msg.Header))
			}

			start := time.Now()
			_, err = js.PublishMsg(ctx, msg)
			duration := time.Since(start)
			metrics.NatsPublishDuration.Observe(duration.Seconds())

			if err != nil {
				metrics.NatsErrors.Inc()
				if span != nil {
					span.RecordError(err)
					span.SetStatus(codes.Error, err.Error())
					span.End()
				}
				// Do NOT ack: return error to trigger reconnect (event is lost — pre-existing limitation).
				return pgcdcerr.WrapEvent(err, adapterName, ev)
			}

			if span != nil {
				span.End()
			}
			metrics.NatsPublished.Inc()
			metrics.EventsDelivered.WithLabelValues(adapterName).Inc()
			if a.ackFn != nil && ev.LSN > 0 {
				a.ackFn(ev.LSN)
			}
		}
	}
}

// eventSubject converts an event channel to a NATS subject.
// "pgcdc:orders" -> "pgcdc.orders", "pgcdc:public.orders" -> "pgcdc.public.orders"
func (a *Adapter) eventSubject(channel string) string {
	// Strip "pgcdc:" prefix if present, replace colons with dots.
	subject := strings.ReplaceAll(channel, ":", ".")
	// Ensure it starts with our prefix.
	if !strings.HasPrefix(subject, a.subjectPrefix+".") {
		subject = a.subjectPrefix + "." + subject
	}
	return subject
}
