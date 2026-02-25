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

	"github.com/florinutz/pgcdc/dlq"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/backoff"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/tracing"
)

const (
	defaultIDColumn      = "id"
	defaultBatchSize     = 100
	defaultBatchInterval = 1 * time.Second
	defaultBackoffBase   = 1 * time.Second
	defaultBackoffCap    = 30 * time.Second
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
	client        *http.Client
	logger        *slog.Logger
	dlq           dlq.DLQ
	tracer        trace.Tracer
}

// SetTracer implements adapter.Traceable.
func (a *Adapter) SetTracer(t trace.Tracer) { a.tracer = t }

// SetDLQ sets the dead letter queue for failed deliveries.
func (a *Adapter) SetDLQ(d dlq.DLQ) { a.dlq = d }

// New creates a search adapter. engine must be "typesense" or "meilisearch".
func New(
	engine, url, apiKey, index, idColumn string,
	batchSize int,
	batchInterval, backoffBase, backoffCap time.Duration,
	logger *slog.Logger,
) *Adapter {
	switch engine {
	case "typesense", "meilisearch":
	default:
		if logger != nil {
			logger.Error("unsupported search engine, defaulting to typesense", "engine", engine)
		}
		engine = "typesense"
	}
	if idColumn == "" {
		idColumn = defaultIDColumn
	}
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	if batchInterval <= 0 {
		batchInterval = defaultBatchInterval
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
		engine:        engine,
		url:           strings.TrimRight(url, "/"),
		apiKey:        apiKey,
		index:         index,
		idColumn:      idColumn,
		batchSize:     batchSize,
		batchInterval: batchInterval,
		backoffBase:   backoffBase,
		backoffCap:    backoffCap,
		client:        &http.Client{Timeout: 30 * time.Second},
		logger:        logger.With("adapter", "search"),
	}
}

func (a *Adapter) Name() string { return "search" }

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
		batch = nil
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

			doc, id, isDel := a.extractPayload(ev)
			if id == "" {
				a.logger.Warn("no ID in event payload, skipping", "event_id", ev.ID)
				continue
			}

			mu.Lock()
			if isDel {
				deletes = append(deletes, id)
			} else {
				batch = append(batch, doc)
				if len(batch) >= a.batchSize {
					mu.Unlock()
					flush()
					continue
				}
			}
			mu.Unlock()

			metrics.EventsDelivered.WithLabelValues("search").Inc()
		}
	}
}

// payload is the decoded event payload.
type payload struct {
	Op    string                 `json:"op"`
	Table string                 `json:"table"`
	Row   map[string]interface{} `json:"row"`
}

func (a *Adapter) extractPayload(ev event.Event) (doc map[string]interface{}, id string, isDel bool) {
	var p payload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		return nil, "", false
	}
	isDel = p.Op == "DELETE"
	if p.Row == nil {
		return nil, "", isDel
	}
	if v, ok := p.Row[a.idColumn]; ok && v != nil {
		id = fmt.Sprintf("%v", v)
	}
	return p.Row, id, isDel
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
	for attempt := range 3 {
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
		return fmt.Errorf("search API returned %d", resp.StatusCode)
	}
	return lastErr
}
