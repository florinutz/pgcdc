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

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/backoff"
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
//
// Implements adapter.Deliverer — the middleware stack provides retry, circuit
// breaker, rate limiting, DLQ, tracing, metrics, and cooperative checkpoint ack.
// The adapter's own Deliver() handles only the HTTP POST with per-attempt retry
// for transient HTTP errors (5xx/429). The middleware retry wraps around the
// entire Deliver() call for broader failure recovery.
type Adapter struct {
	url         string
	headers     map[string]string
	signingKey  string
	maxRetries  int
	backoffBase time.Duration
	backoffCap  time.Duration
	client      *http.Client
	logger      *slog.Logger
	inflight    sync.WaitGroup
}

// Config holds all parameters for the webhook adapter.
type Config struct {
	URL         string
	Headers     map[string]string
	SigningKey  string
	MaxRetries  int
	Timeout     time.Duration
	BackoffBase time.Duration
	BackoffCap  time.Duration
}

// New creates a webhook adapter. If MaxRetries is <= 0 it defaults to 5.
// If logger is nil a no-op logger is used. Duration parameters default
// to sensible values when zero.
func New(cfg Config, logger *slog.Logger) *Adapter {
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = defaultMaxRetries
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultTimeout
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = defaultBackoffBase
	}
	if cfg.BackoffCap <= 0 {
		cfg.BackoffCap = defaultBackoffCap
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Adapter{
		url:         cfg.URL,
		headers:     cfg.Headers,
		signingKey:  cfg.SigningKey,
		maxRetries:  cfg.MaxRetries,
		backoffBase: cfg.BackoffBase,
		backoffCap:  cfg.BackoffCap,
		client: &http.Client{
			Timeout: cfg.Timeout,
		},
		logger: logger,
	}
}

// Deliver sends a single event as an HTTP POST, retrying on 5xx and 429
// responses with exponential backoff. Implements adapter.Deliverer.
func (a *Adapter) Deliver(ctx context.Context, ev event.Event) error {
	a.inflight.Add(1)
	defer a.inflight.Done()
	return a.deliver(ctx, ev)
}

// Start is the legacy event loop kept for backward compatibility.
// When the middleware detects Deliverer, it uses Deliver() instead.
func (a *Adapter) Start(ctx context.Context, events <-chan event.Event) error {
	a.logger.Info("webhook adapter started (legacy path)", "url", a.url)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-events:
			if !ok {
				return nil
			}
			if err := a.Deliver(ctx, ev); err != nil {
				a.logger.Error("delivery failed", "event_id", ev.ID, "error", err)
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
// 4xx errors are logged and treated as success (skip).
func (a *Adapter) deliver(ctx context.Context, ev event.Event) error {
	body, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal event %s: %w", ev.ID, err)
	}

	var lastStatus int
	var lastErr error

	for attempt := range a.maxRetries {
		if attempt > 0 {
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

		resp, err := a.client.Do(req)
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
