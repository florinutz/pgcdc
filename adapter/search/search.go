package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
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
	defaultIDColumn      = "id"
	defaultBatchSize     = 100
	defaultBatchInterval = 1 * time.Second
	defaultBackoffBase   = 1 * time.Second
	defaultBackoffCap    = 30 * time.Second
	defaultMaxRetries    = 3
)

// Adapter syncs CDC events to a search engine (Typesense or Meilisearch).
// INSERT/UPDATE events upsert documents; DELETE events remove documents.
// Documents are batched for efficiency.
type Adapter struct {
	engine        string
	url           string
	apiKey        string
	index         string
	idColumn      string
	batchSize     int
	batchInterval time.Duration
	backoffBase   time.Duration
	backoffCap    time.Duration
	maxRetries    int
	client        *http.Client
	logger        *slog.Logger
	dlq           dlq.DLQ
	tracer        trace.Tracer
	ackFn         adapter.AckFunc
}

// SetTracer sets the OpenTelemetry tracer for per-event spans.
func (a *Adapter) SetTracer(t trace.Tracer) { a.tracer = t }

// SetDLQ sets the dead letter queue for failed deliveries.
func (a *Adapter) SetDLQ(d dlq.DLQ) { a.dlq = d }

// SetAckFunc implements adapter.Acknowledger.
func (a *Adapter) SetAckFunc(fn adapter.AckFunc) { a.ackFn = fn }

// Config holds all parameters for the search adapter.
type Config struct {
	Engine        string
	URL           string
	APIKey        string
	Index         string
	IDColumn      string
	BatchSize     int
	BatchInterval time.Duration
	BackoffBase   time.Duration
	BackoffCap    time.Duration
	MaxRetries    int
}

// New creates a search adapter. Engine must be "typesense" or "meilisearch".
func New(cfg Config, logger *slog.Logger) *Adapter {
	switch cfg.Engine {
	case "typesense", "meilisearch":
	default:
		if logger != nil {
			logger.Error("unsupported search engine, defaulting to typesense", "engine", cfg.Engine)
		}
		cfg.Engine = "typesense"
	}
	if cfg.IDColumn == "" {
		cfg.IDColumn = defaultIDColumn
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultBatchSize
	}
	if cfg.BatchInterval <= 0 {
		cfg.BatchInterval = defaultBatchInterval
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = defaultBackoffBase
	}
	if cfg.BackoffCap <= 0 {
		cfg.BackoffCap = defaultBackoffCap
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = defaultMaxRetries
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Adapter{
		engine:        cfg.Engine,
		url:           strings.TrimRight(cfg.URL, "/"),
		apiKey:        cfg.APIKey,
		index:         cfg.Index,
		idColumn:      cfg.IDColumn,
		batchSize:     cfg.BatchSize,
		batchInterval: cfg.BatchInterval,
		backoffBase:   cfg.BackoffBase,
		backoffCap:    cfg.BackoffCap,
		maxRetries:    cfg.MaxRetries,
		client:        &http.Client{Timeout: 30 * time.Second},
		logger:        logger.With("adapter", "search"),
	}
}

func (a *Adapter) Name() string { return "search" }

// Drain implements adapter.Drainer. The search adapter flushes remaining
// batched documents in Start() on context cancellation, so Drain is a no-op.
func (a *Adapter) Drain(_ context.Context) error { return nil }

// Validate checks search engine health endpoint connectivity.
func (a *Adapter) Validate(ctx context.Context) error {
	healthURL := strings.TrimRight(a.url, "/") + "/health"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
	if err != nil {
		return fmt.Errorf("create health request: %w", err)
	}
	if a.apiKey != "" {
		if a.engine == "typesense" {
			req.Header.Set("X-TYPESENSE-API-KEY", a.apiKey)
		} else {
			req.Header.Set("Authorization", "Bearer "+a.apiKey)
		}
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return fmt.Errorf("search health check: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("search health check returned %d", resp.StatusCode)
	}
	return nil
}

// Start consumes events, batches upserts, and flushes to the search engine.
func (a *Adapter) Start(ctx context.Context, events <-chan event.Event) error {
	a.logger.Info("search adapter started", "engine", a.engine, "index", a.index)

	var (
		mu      sync.Mutex
		batch   []map[string]interface{}
		deletes []string
	)

	flush := func() {
		mu.Lock()
		upserts := batch
		dels := deletes
		batch = make([]map[string]interface{}, 0, a.batchSize)
		deletes = nil
		mu.Unlock()

		if len(upserts) > 0 {
			flushCtx := ctx
			var span trace.Span
			if a.tracer != nil {
				flushCtx, span = a.tracer.Start(ctx, "pgcdc.adapter.deliver",
					trace.WithSpanKind(trace.SpanKindConsumer),
					trace.WithAttributes(
						attribute.String("pgcdc.adapter", "search"),
						attribute.String("pgcdc.operation", "batch_upsert"),
						attribute.Int("pgcdc.batch_size", len(upserts)),
					),
				)
			}
			start := time.Now()
			if err := a.upsertDocuments(flushCtx, upserts); err != nil {
				metrics.SearchErrors.Inc()
				a.logger.Error("search upsert failed", "count", len(upserts), "error", err)
				if span != nil {
					span.RecordError(err)
					span.SetStatus(codes.Error, err.Error())
				}
			} else {
				metrics.SearchUpserted.Add(float64(len(upserts)))
				metrics.SearchBatchDuration.Observe(time.Since(start).Seconds())
			}
			if span != nil {
				span.End()
			}
		}
		for _, id := range dels {
			delCtx := ctx
			var span trace.Span
			if a.tracer != nil {
				delCtx, span = a.tracer.Start(ctx, "pgcdc.adapter.deliver",
					trace.WithSpanKind(trace.SpanKindConsumer),
					trace.WithAttributes(
						attribute.String("pgcdc.adapter", "search"),
						attribute.String("pgcdc.operation", "delete"),
					),
				)
			}
			if err := a.deleteDocument(delCtx, id); err != nil {
				metrics.SearchErrors.Inc()
				a.logger.Error("search delete failed", "id", id, "error", err)
				if span != nil {
					span.RecordError(err)
					span.SetStatus(codes.Error, err.Error())
				}
			} else {
				metrics.SearchDeleted.Inc()
			}
			if span != nil {
				span.End()
			}
		}
	}

	ticker := time.NewTicker(a.batchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			flush()
			return ctx.Err()

		case <-ticker.C:
			flush()

		case ev, ok := <-events:
			if !ok {
				flush()
				return nil
			}

			doc, id, isDel, partial := a.extractPayload(ev)
			if id == "" {
				a.logger.Warn("no ID in event payload, skipping", "event_id", ev.ID)
				continue
			}

			switch {
			case isDel:
				mu.Lock()
				deletes = append(deletes, id)
				mu.Unlock()
			case partial:
				// Partial update (unchanged TOAST columns stripped):
				// route to individual update to avoid overwriting existing
				// fields with null in the search engine.
				if err := a.updateDocument(ctx, id, doc); err != nil {
					metrics.SearchErrors.Inc()
					a.logger.Error("search partial update failed", "id", id, "error", err)
				} else {
					metrics.SearchUpserted.Inc()
				}
			default:
				mu.Lock()
				batch = append(batch, doc)
				if len(batch) >= a.batchSize {
					mu.Unlock()
					flush()
					continue
				}
				mu.Unlock()
			}

			metrics.EventsDelivered.WithLabelValues("search").Inc()
			if a.ackFn != nil && ev.LSN > 0 {
				a.ackFn(ev.LSN)
			}
		}
	}
}

func (a *Adapter) extractPayload(ev event.Event) (doc map[string]interface{}, id string, isDel bool, partial bool) {
	extracted := event.ExtractRow(ev, a.idColumn)
	if extracted.Row == nil {
		return nil, extracted.ID, extracted.IsDelete, false
	}

	// Strip unchanged TOAST columns from the document so they don't
	// overwrite existing values in the search engine with null.
	if len(extracted.UnchangedToast) > 0 {
		for _, col := range extracted.UnchangedToast {
			delete(extracted.Row, col)
		}
		partial = true
	}

	return extracted.Row, extracted.ID, extracted.IsDelete, partial
}

func (a *Adapter) upsertDocuments(ctx context.Context, docs []map[string]interface{}) error {
	var url string
	var body []byte
	var err error

	switch a.engine {
	case "typesense":
		// Typesense import: POST /collections/{collection}/documents/import
		url = fmt.Sprintf("%s/collections/%s/documents/import?action=upsert", a.url, a.index)
		// JSONL format for Typesense import
		var buf bytes.Buffer
		for i, doc := range docs {
			if i > 0 {
				buf.WriteByte('\n')
			}
			line, err := json.Marshal(doc)
			if err != nil {
				return fmt.Errorf("marshal doc: %w", err)
			}
			buf.Write(line)
		}
		body = buf.Bytes()
	case "meilisearch":
		// Meilisearch: POST /indexes/{index}/documents
		url = fmt.Sprintf("%s/indexes/%s/documents", a.url, a.index)
		body, err = json.Marshal(docs)
		if err != nil {
			return fmt.Errorf("marshal docs: %w", err)
		}
	default:
		return fmt.Errorf("unsupported search engine: %s", a.engine)
	}

	return a.doRequest(ctx, http.MethodPost, url, body)
}

// updateDocument performs a partial update on a single document. For Typesense,
// this uses PATCH to update only the provided fields. For Meilisearch, the
// standard document endpoint merges by default (missing fields are retained).
func (a *Adapter) updateDocument(ctx context.Context, id string, doc map[string]interface{}) error {
	body, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("marshal doc: %w", err)
	}

	var reqURL string
	var method string
	switch a.engine {
	case "typesense":
		reqURL = fmt.Sprintf("%s/collections/%s/documents/%s", a.url, a.index, id)
		method = http.MethodPatch
	case "meilisearch":
		// Meilisearch merges by default; wrapping in array for single-doc import.
		reqURL = fmt.Sprintf("%s/indexes/%s/documents", a.url, a.index)
		method = http.MethodPost
		body, err = json.Marshal([]map[string]interface{}{doc})
		if err != nil {
			return fmt.Errorf("marshal doc array: %w", err)
		}
	default:
		return fmt.Errorf("unsupported search engine: %s", a.engine)
	}

	return a.doRequest(ctx, method, reqURL, body)
}

func (a *Adapter) deleteDocument(ctx context.Context, id string) error {
	var url string
	switch a.engine {
	case "typesense":
		url = fmt.Sprintf("%s/collections/%s/documents/%s", a.url, a.index, id)
	case "meilisearch":
		url = fmt.Sprintf("%s/indexes/%s/documents/%s", a.url, a.index, id)
	default:
		return fmt.Errorf("unsupported search engine: %s", a.engine)
	}
	return a.doRequest(ctx, http.MethodDelete, url, nil)
}

func (a *Adapter) doRequest(ctx context.Context, method, url string, body []byte) error {
	var lastErr error
	for attempt := range a.maxRetries {
		if attempt > 0 {
			wait := backoff.Jitter(attempt, a.backoffBase, a.backoffCap)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
		}

		var bodyReader io.Reader
		if body != nil {
			bodyReader = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
		if err != nil {
			return fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		tracing.InjectHTTP(ctx, req.Header)

		switch a.engine {
		case "typesense":
			req.Header.Set("X-TYPESENSE-API-KEY", a.apiKey)
		case "meilisearch":
			req.Header.Set("Authorization", "Bearer "+a.apiKey)
		}

		resp, err := a.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
			lastErr = fmt.Errorf("search API returned %d", resp.StatusCode)
			continue
		}
		return &pgcdcerr.SearchSyncError{
			Engine: a.engine,
			Index:  a.index,
			Err:    fmt.Errorf("search API returned %d", resp.StatusCode),
		}
	}
	if lastErr != nil {
		return &pgcdcerr.SearchSyncError{Engine: a.engine, Index: a.index, Err: lastErr}
	}
	return nil
}
