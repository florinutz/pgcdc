//go:build !no_kafka

package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/reconnect"
	"github.com/florinutz/pgcdc/metrics"
)

const detectorName = "kafka_consumer"

// Detector consumes events from Kafka topics using a consumer group.
type Detector struct {
	opts        []kgo.Opt
	topics      []string
	backoffBase time.Duration
	backoffCap  time.Duration
	logger      *slog.Logger
}

// New creates a Kafka consumer detector. Duration parameters default to
// sensible values when zero.
func New(
	brokers, topics []string,
	group, offset string,
	saslMechanism, saslUser, saslPass, tlsCAFile string,
	tlsEnabled bool,
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
	if len(brokers) == 0 {
		brokers = []string{"localhost:9092"}
	}
	if group == "" {
		group = "pgcdc"
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumerGroup(group),
	}

	// Set initial offset reset policy.
	switch offset {
	case "latest":
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	default: // "earliest"
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	}

	// SASL authentication.
	switch saslMechanism {
	case "plain":
		opts = append(opts, kgo.SASL(plain.Auth{User: saslUser, Pass: saslPass}.AsMechanism()))
	case "scram-sha-256":
		opts = append(opts, kgo.SASL(scram.Auth{User: saslUser, Pass: saslPass}.AsSha256Mechanism()))
	case "scram-sha-512":
		opts = append(opts, kgo.SASL(scram.Auth{User: saslUser, Pass: saslPass}.AsSha512Mechanism()))
	}

	// TLS configuration.
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

	return &Detector{
		opts:        opts,
		topics:      topics,
		backoffBase: backoffBase,
		backoffCap:  backoffCap,
		logger:      logger.With("detector", detectorName),
	}
}

// Name returns the detector name.
func (d *Detector) Name() string { return detectorName }

// Start connects to Kafka and consumes events, sending them to the events
// channel. It blocks until ctx is cancelled. On connection error, it
// reconnects with exponential backoff. MUST NOT close the events channel.
func (d *Detector) Start(ctx context.Context, events chan<- event.Event) error {
	return reconnect.Loop(ctx, detectorName, d.backoffBase, d.backoffCap,
		d.logger, metrics.KafkaConsumerErrors,
		func(ctx context.Context) error {
			return d.run(ctx, events)
		})
}

func (d *Detector) run(ctx context.Context, events chan<- event.Event) error {
	client, err := kgo.NewClient(d.opts...)
	if err != nil {
		return fmt.Errorf("create kafka consumer client: %w", err)
	}
	defer client.Close()

	d.logger.Info("kafka consumer ready", "topics", d.topics)

	for {
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			// Log all fetch errors and return the first to trigger reconnect.
			for _, fe := range errs {
				d.logger.Error("kafka fetch error",
					"topic", fe.Topic,
					"partition", fe.Partition,
					"error", fe.Err,
				)
				metrics.KafkaConsumerErrors.Inc()
			}
			return fmt.Errorf("fetch: %w", errs[0].Err)
		}

		fetches.EachRecord(func(record *kgo.Record) {
			ev, evErr := d.recordToEvent(record)
			if evErr != nil {
				d.logger.Warn("create event from kafka record",
					"error", evErr,
					"topic", record.Topic,
					"partition", record.Partition,
					"offset", record.Offset,
				)
				metrics.KafkaConsumerErrors.Inc()
				return
			}

			select {
			case events <- ev:
				metrics.KafkaConsumerReceived.Inc()
			case <-ctx.Done():
				return
			}
		})

		client.AllowRebalance()
	}
}

func (d *Detector) recordToEvent(record *kgo.Record) (event.Event, error) {
	channel := "pgcdc:" + record.Topic

	var payload json.RawMessage
	if json.Valid(record.Value) {
		payload = json.RawMessage(record.Value)
	} else {
		wrapped := struct {
			Raw string `json:"raw"`
		}{
			Raw: base64.StdEncoding.EncodeToString(record.Value),
		}
		var err error
		payload, err = json.Marshal(wrapped)
		if err != nil {
			return event.Event{}, fmt.Errorf("marshal non-JSON payload: %w", err)
		}
	}

	ev, err := event.New(channel, "KAFKA_CONSUME", payload, "kafka_consumer")
	if err != nil {
		return event.Event{}, err
	}

	if !record.Timestamp.IsZero() {
		ev.CreatedAt = record.Timestamp
	}

	return ev, nil
}
