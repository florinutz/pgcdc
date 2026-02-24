package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/backoff"
	"github.com/florinutz/pgcdc/metrics"
)

const adapterName = "nats"

// Adapter publishes events to a NATS JetStream stream.
type Adapter struct {
	url           string
	subjectPrefix string
	streamName    string
	credFile      string
	maxAge        time.Duration
	backoffBase   time.Duration
	backoffCap    time.Duration
	logger        *slog.Logger
}

// New creates a NATS JetStream adapter. Duration parameters default to sensible
// values when zero.
func New(
	url, subjectPrefix, streamName, credFile string,
	maxAge, backoffBase, backoffCap time.Duration,
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
	opts := []nats.Option{
		nats.Name("pgcdc"),
		nats.MaxReconnects(-1),
	}
	if a.credFile != "" {
		opts = append(opts, nats.UserCredentials(a.credFile))
	}

	nc, err := nats.Connect(a.url, opts...)
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

			start := time.Now()
			_, err = js.Publish(ctx, subject, data,
				jetstream.WithMsgID(ev.ID),
			)
			duration := time.Since(start)
			metrics.NatsPublishDuration.Observe(duration.Seconds())

			if err != nil {
				metrics.NatsErrors.Inc()
				// Return error to trigger reconnect.
				return fmt.Errorf("publish event %s: %w", ev.ID, err)
			}

			metrics.NatsPublished.Inc()
			metrics.EventsDelivered.WithLabelValues(adapterName).Inc()
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
