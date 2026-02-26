package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/dlq"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/backoff"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/pgcdcerr"
	"github.com/florinutz/pgcdc/tracing"
)

const (
	defaultMaxRetries  = 5
	defaultBackoffBase = 1 * time.Second
	defaultBackoffCap  = 32 * time.Second
	defaultTimeout     = 10 * time.Second
	userAgent          = "pgcdc/1.0"
)

// Adapter delivers events as HTTP POST requests to a webhook URL.
type Adapter struct {
	url         string
	headers     map[string]string
	signingKey  string
	maxRetries  int
	backoffBase time.Duration
	backoffCap  time.Duration
	client      *http.Client
	logger      *slog.Logger
	dlq         dlq.DLQ
	ackFn       adapter.AckFunc
	tracer      trace.Tracer
	inflight    sync.WaitGroup
}

// SetTracer implements adapter.Traceable.
func (a *Adapter) SetTracer(t trace.Tracer) { a.tracer = t }

// SetDLQ sets the dead letter queue for failed deliveries.
func (a *Adapter) SetDLQ(d dlq.DLQ) { a.dlq = d }

// SetAckFunc implements adapter.Acknowledger.
func (a *Adapter) SetAckFunc(fn adapter.AckFunc) { a.ackFn = fn }

// New creates a webhook adapter. If maxRetries is <= 0 it defaults to 5.
// If logger is nil a no-op logger is used. Duration parameters default
// to sensible values when zero.
func New(url string, headers map[string]string, signingKey string, maxRetries int, timeout, backoffBase, backoffCap time.Duration, logger *slog.Logger) *Adapter {
	if maxRetries <= 0 {
		maxRetries = defaultMaxRetries
	}
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	if backoffBase <= 0 {
		backoffBase = defaultBackoffBase
	}
	if backoffCap <= 0 {
		backoffCap = defaultBackoffCap
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Adapter{
		url:         url,
		headers:     headers,
		signingKey:  signingKey,
		maxRetries:  maxRetries,
		backoffBase: backoffBase,
		backoffCap:  backoffCap,
		client: &http.Client{
			Timeout: timeout,
		},
		logger: logger,
	}
}

// Start blocks, consuming events from the channel and delivering each one via
// HTTP POST. It returns nil when the channel is closed or ctx.Err() when the
// context is cancelled.
func (a *Adapter) Start(ctx context.Context, events <-chan event.Event) error {
	a.logger.Info("webhook adapter started", "url", a.url)

	for {
		select {
		case <-ctx.Done():
			a.logger.Info("webhook adapter stopping", "reason", "context cancelled")
			return ctx.Err()

		case ev, ok := <-events:
			if !ok {
				a.logger.Info("webhook adapter stopping", "reason", "channel closed")
				return nil
			}

			// Create delivery span linked to the detector span.
			deliverCtx := ctx
			var span trace.Span
			if a.tracer != nil {
				var opts []trace.SpanStartOption
				opts = append(opts,
					trace.WithSpanKind(trace.SpanKindConsumer),
					trace.WithAttributes(
						attribute.String("pgcdc.adapter", "webhook"),
						attribute.String("pgcdc.event.id", ev.ID),
						attribute.String("pgcdc.channel", ev.Channel),
						attribute.String("pgcdc.operation", ev.Operation),
					),
				)
				if ev.SpanContext.IsValid() {
					opts = append(opts, trace.WithLinks(trace.Link{SpanContext: ev.SpanContext}))
					deliverCtx = trace.ContextWithRemoteSpanContext(ctx, ev.SpanContext)
				}
				deliverCtx, span = a.tracer.Start(deliverCtx, "pgcdc.adapter.deliver", opts...)
			}

			a.inflight.Add(1)
			if err := a.deliver(deliverCtx, ev); err != nil {
				if span != nil {
					span.RecordError(err)
					span.SetStatus(codes.Error, err.Error())
				}
				a.logger.Error("delivery failed, skipping event",
					"event_id", ev.ID,
					"channel", ev.Channel,
					"error", err,
				)
				if a.dlq != nil {
					if dlqErr := a.dlq.Record(ctx, ev, "webhook", err); dlqErr != nil {
						a.logger.Error("dlq record failed", "error", dlqErr)
					}
				}
			}
			if span != nil {
				span.End()
			}
			a.inflight.Done()
			// Ack after terminal outcome (delivery, DLQ record, or non-retryable skip).
			if a.ackFn != nil && ev.LSN > 0 {
				a.ackFn(ev.LSN)
			}
		}
	}
}

// Name returns the adapter name.
func (a *Adapter) Name() string {
	return "webhook"
}

// Validate checks that the webhook URL host can be DNS-resolved.
func (a *Adapter) Validate(ctx context.Context) error {
	u, err := url.Parse(a.url)
	if err != nil {
		return fmt.Errorf("parse url: %w", err)
	}
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("empty hostname in url %q", a.url)
	}
	resolver := &net.Resolver{}
	addrs, err := resolver.LookupHost(ctx, host)
	if err != nil {
		return fmt.Errorf("dns resolve %q: %w", host, err)
	}
	if len(addrs) == 0 {
		return fmt.Errorf("dns resolve %q: no addresses", host)
	}
	return nil
}

// Drain waits for all in-flight deliveries to complete.
func (a *Adapter) Drain(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		a.inflight.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// deliver marshals the event to JSON and sends it as an HTTP POST, retrying on
// 5xx and 429 responses with exponential backoff and full jitter. Non-retryable
// 4xx errors are logged and skipped.
func (a *Adapter) deliver(ctx context.Context, ev event.Event) error {
	body, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal event %s: %w", ev.ID, err)
	}

	var lastStatus int
	var lastErr error

	for attempt := range a.maxRetries {
		if attempt > 0 {
			metrics.WebhookRetries.Inc()
			wait := backoff.Jitter(attempt, a.backoffBase, a.backoffCap)
			a.logger.Info("retrying delivery",
				"event_id", ev.ID,
				"attempt", attempt+1,
				"backoff", wait,
			)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.url, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("create request for event %s: %w", ev.ID, err)
		}

		// Standard headers.
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", userAgent)
		req.Header.Set("X-PGCDC-Event-ID", ev.ID)
		req.Header.Set("X-PGCDC-Channel", ev.Channel)

		// Inject W3C trace context (traceparent) if span is active.
		tracing.InjectHTTP(ctx, req.Header)

		// Custom headers from config.
		for k, v := range a.headers {
			req.Header.Set(k, v)
		}

		// HMAC-SHA256 signature.
		if a.signingKey != "" {
			mac := hmac.New(sha256.New, []byte(a.signingKey))
			mac.Write(body)
			sig := hex.EncodeToString(mac.Sum(nil))
			req.Header.Set("X-PGCDC-Signature", "sha256="+sig)
		}

		start := time.Now()
		resp, err := a.client.Do(req)
		metrics.WebhookDuration.Observe(time.Since(start).Seconds())
		if err != nil {
			// Network errors are retryable.
			lastErr = err
			a.logger.Warn("http request failed",
				"event_id", ev.ID,
				"attempt", attempt+1,
				"error", err,
			)
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			metrics.EventsDelivered.WithLabelValues("webhook").Inc()
			a.logger.Debug("event delivered",
				"event_id", ev.ID,
				"status", resp.StatusCode,
			)
			return nil
		}

		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			lastStatus = resp.StatusCode
			a.logger.Warn("retryable response",
				"event_id", ev.ID,
				"status", resp.StatusCode,
				"attempt", attempt+1,
			)
			continue
		}

		// Non-retryable 4xx: log and skip.
		a.logger.Error("non-retryable response, skipping event",
			"event_id", ev.ID,
			"status", resp.StatusCode,
		)
		return nil
	}

	return &pgcdcerr.WebhookDeliveryError{
		EventID:    ev.ID,
		URL:        a.url,
		StatusCode: lastStatus,
		Retries:    a.maxRetries,
		Err:        lastErr,
	}
}
