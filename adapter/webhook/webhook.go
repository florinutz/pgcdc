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
	"math"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/florinutz/pgpipe/event"
	"github.com/florinutz/pgpipe/metrics"
	"github.com/florinutz/pgpipe/pgpipeerr"
)

const (
	defaultMaxRetries  = 5
	defaultBackoffBase = 1 * time.Second
	defaultBackoffCap  = 32 * time.Second
	defaultTimeout     = 10 * time.Second
	userAgent          = "pgpipe/1.0"
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
}

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
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
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
			if err := a.deliver(ctx, ev); err != nil {
				a.logger.Error("delivery failed, skipping event",
					"event_id", ev.ID,
					"channel", ev.Channel,
					"error", err,
				)
			}
		}
	}
}

// Name returns the adapter name.
func (a *Adapter) Name() string {
	return "webhook"
}

// deliver marshals the event to JSON and sends it as an HTTP POST, retrying on
// 5xx and 429 responses with exponential backoff and full jitter. Non-retryable
// 4xx errors are logged and skipped.
func (a *Adapter) deliver(ctx context.Context, ev event.Event) error {
	body, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal event %s: %w", ev.ID, err)
	}

	for attempt := range a.maxRetries {
		if attempt > 0 {
			metrics.WebhookRetries.Inc()
			wait := Backoff(attempt, a.backoffBase, a.backoffCap)
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
		req.Header.Set("X-PGPipe-Event-ID", ev.ID)
		req.Header.Set("X-PGPipe-Channel", ev.Channel)

		// Custom headers from config.
		for k, v := range a.headers {
			req.Header.Set(k, v)
		}

		// HMAC-SHA256 signature.
		if a.signingKey != "" {
			mac := hmac.New(sha256.New, []byte(a.signingKey))
			mac.Write(body)
			sig := hex.EncodeToString(mac.Sum(nil))
			req.Header.Set("X-PGPipe-Signature", "sha256="+sig)
		}

		start := time.Now()
		resp, err := a.client.Do(req)
		metrics.WebhookDuration.Observe(time.Since(start).Seconds())
		if err != nil {
			// Network errors are retryable.
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

	return &pgpipeerr.WebhookDeliveryError{
		EventID: ev.ID,
		URL:     a.url,
		Retries: a.maxRetries,
	}
}

// Backoff calculates the wait duration for a given retry attempt using
// exponential backoff with full jitter: random value in [0, min(cap, base*2^attempt)].
// attempt is zero-indexed (0 = first retry wait).
func Backoff(attempt int, base, cap time.Duration) time.Duration {
	if base <= 0 {
		base = defaultBackoffBase
	}
	if cap <= 0 {
		cap = defaultBackoffCap
	}
	exp := math.Pow(2, float64(attempt))
	delayMs := float64(base.Milliseconds()) * exp
	capMs := float64(cap.Milliseconds())
	if delayMs > capMs {
		delayMs = capMs
	}
	jittered := rand.Float64() * delayMs
	return time.Duration(jittered) * time.Millisecond
}
