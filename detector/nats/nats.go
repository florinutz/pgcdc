//go:build !no_nats

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

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/reconnect"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/pgcdcerr"
)

const detectorName = "nats_consumer"

// Detector consumes messages from a NATS JetStream stream and emits them as
// pgcdc events.
type Detector struct {
	url         string
	stream      string
	subjects    []string
	durable     string
	credFile    string
	backoffBase time.Duration
	backoffCap  time.Duration
	logger      *slog.Logger
}

// New creates a NATS JetStream consumer detector. Duration parameters default
// to sensible values when zero.
func New(
	url, stream string,
	subjects []string,
	durable, credFile string,
	backoffBase, backoffCap time.Duration,
	logger *slog.Logger,
) *Detector {
	if logger == nil {
		logger = slog.Default()
	}
	if backoffBase <= 0 {
		backoffBase = 5 * time.Second
	}
	if backoffCap <= 0 {
		backoffCap = 60 * time.Second
	}
	if stream == "" {
		stream = "pgcdc"
	}
	if durable == "" {
		durable = "pgcdc"
	}
	return &Detector{
		url:         url,
		stream:      stream,
		subjects:    subjects,
		durable:     durable,
		credFile:    credFile,
		backoffBase: backoffBase,
		backoffCap:  backoffCap,
		logger:      logger.With("detector", detectorName),
	}
}

// Name returns the detector name.
func (d *Detector) Name() string { return detectorName }

// Start connects to NATS JetStream and consumes messages, emitting them as
// events on the provided channel. It blocks until ctx is cancelled. On
// disconnection it reconnects with exponential backoff. MUST NOT close the
// events channel.
func (d *Detector) Start(ctx context.Context, events chan<- event.Event) error {
	return reconnect.Loop(ctx, detectorName, d.backoffBase, d.backoffCap,
		d.logger, metrics.NatsConsumerErrors,
		func(ctx context.Context) error {
			return d.run(ctx, events)
		})
}

func (d *Detector) run(ctx context.Context, events chan<- event.Event) error {
	opts := []natsclient.Option{
		natsclient.Name("pgcdc-consumer"),
		natsclient.MaxReconnects(-1),
	}
	if d.credFile != "" {
		opts = append(opts, natsclient.UserCredentials(d.credFile))
	}

	nc, err := natsclient.Connect(d.url, opts...)
	if err != nil {
		return &pgcdcerr.NatsConsumeError{Stream: d.stream, Err: fmt.Errorf("connect: %w", err)}
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return &pgcdcerr.NatsConsumeError{Stream: d.stream, Err: fmt.Errorf("jetstream init: %w", err)}
	}

	consCfg := jetstream.ConsumerConfig{
		Durable:       d.durable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	}
	if len(d.subjects) > 0 {
		consCfg.FilterSubjects = d.subjects
	}

	cons, err := js.CreateOrUpdateConsumer(ctx, d.stream, consCfg)
	if err != nil {
		return &pgcdcerr.NatsConsumeError{Stream: d.stream, Err: fmt.Errorf("create consumer: %w", err)}
	}

	iter, err := cons.Messages()
	if err != nil {
		return &pgcdcerr.NatsConsumeError{Stream: d.stream, Err: fmt.Errorf("start message iterator: %w", err)}
	}
	// Stop the iterator when context is cancelled so iter.Next() unblocks.
	go func() {
		<-ctx.Done()
		iter.Stop()
	}()
	defer iter.Stop()

	d.logger.Info("nats consumer started",
		"url", d.url,
		"stream", d.stream,
		"durable", d.durable,
		"subjects", d.subjects,
	)

	for {
		msg, err := iter.Next()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			metrics.NatsConsumerErrors.Inc()
			return &pgcdcerr.NatsConsumeError{Stream: d.stream, Err: fmt.Errorf("fetch message: %w", err)}
		}

		ev, err := d.messageToEvent(msg)
		if err != nil {
			d.logger.Error("convert message to event", "error", err, "subject", msg.Subject())
			metrics.NatsConsumerErrors.Inc()
			continue
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case events <- ev:
		}

		if err := msg.Ack(); err != nil {
			d.logger.Error("ack message", "error", err, "subject", msg.Subject())
		}

		metrics.NatsConsumerReceived.Inc()
	}
}

// messageToEvent converts a NATS JetStream message to a pgcdc event.
func (d *Detector) messageToEvent(msg jetstream.Msg) (event.Event, error) {
	// Build channel: replace dots with colons, prefix with "pgcdc:" if needed.
	channel := "pgcdc:" + strings.ReplaceAll(msg.Subject(), ".", ":")

	// Use message data as payload: if valid JSON, use as-is; otherwise wrap.
	data := msg.Data()
	var payload json.RawMessage
	if json.Valid(data) {
		payload = data
	} else {
		wrapped, err := json.Marshal(string(data))
		if err != nil {
			return event.Event{}, fmt.Errorf("wrap non-JSON payload: %w", err)
		}
		payload = wrapped
	}

	return event.New(channel, "NATS_CONSUME", payload, "nats_consumer")
}
