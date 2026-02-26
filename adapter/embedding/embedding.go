package embedding

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
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
	"github.com/jackc/pgx/v5"
)

const (
	defaultModel       = "text-embedding-3-small"
	defaultIDColumn    = "id"
	defaultTable       = "pgcdc_embeddings"
	defaultDimension   = 1536
	defaultMaxRetries  = 3
	defaultTimeout     = 30 * time.Second
	defaultBackoffBase = 2 * time.Second
	defaultBackoffCap  = 60 * time.Second
)

// Adapter extracts text columns from CDC events, embeds them via an
// OpenAI-compatible API, and UPSERTs the vectors into a pgvector table.
// DELETE events remove the corresponding vector row.
type Adapter struct {
	apiURL      string
	apiKey      string
	model       string
	columns     []string
	idColumn    string
	dbURL       string
	table       string
	dimension   int
	maxRetries  int
	backoffBase time.Duration
	backoffCap  time.Duration
	client      *http.Client
	logger      *slog.Logger
	dlq         dlq.DLQ
	ackFn       adapter.AckFunc
	tracer      trace.Tracer
}

// SetTracer implements adapter.Traceable.
func (a *Adapter) SetTracer(t trace.Tracer) { a.tracer = t }

// SetDLQ sets the dead letter queue for failed deliveries.
func (a *Adapter) SetDLQ(d dlq.DLQ) { a.dlq = d }

// New creates an embedding adapter.
// apiURL is an OpenAI-compatible endpoint (e.g. https://api.openai.com/v1/embeddings).
// columns is the list of payload row fields to concatenate for embedding.
// idColumn is the payload row field used as the source primary key for UPSERT/DELETE.
// Duration parameters default to sensible values when zero.
func New(
	apiURL, apiKey, model string,
	columns []string,
	idColumn string,
	dbURL, table string,
	dimension int,
	maxRetries int,
	timeout, backoffBase, backoffCap time.Duration,
	logger *slog.Logger,
) *Adapter {
	if model == "" {
		model = defaultModel
	}
	if idColumn == "" {
		idColumn = defaultIDColumn
	}
	if table == "" {
		table = defaultTable
	}
	if dimension <= 0 {
		dimension = defaultDimension
	}
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
		apiURL:      apiURL,
		apiKey:      apiKey,
		model:       model,
		columns:     columns,
		idColumn:    idColumn,
		dbURL:       dbURL,
		table:       table,
		dimension:   dimension,
		maxRetries:  maxRetries,
		backoffBase: backoffBase,
		backoffCap:  backoffCap,
		client:      &http.Client{Timeout: timeout},
		logger:      logger.With("adapter", "embedding"),
	}
}

// Name returns the adapter name.
func (a *Adapter) Name() string {
	return "embedding"
}

// SetAckFunc implements adapter.Acknowledger.
func (a *Adapter) SetAckFunc(fn adapter.AckFunc) { a.ackFn = fn }

// Validate checks embedding API base URL reachability and pgvector DB connectivity.
func (a *Adapter) Validate(ctx context.Context) error {
	// Check API URL is reachable.
	apiBase := strings.TrimSuffix(a.apiURL, "/embeddings")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiBase, nil)
	if err != nil {
		return fmt.Errorf("create api request: %w", err)
	}
	if a.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+a.apiKey)
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return fmt.Errorf("embedding api check: %w", err)
	}
	_ = resp.Body.Close()

	// Check pgvector DB connectivity.
	conn, err := pgx.Connect(ctx, a.dbURL)
	if err != nil {
		return fmt.Errorf("pgvector db connect: %w", err)
	}
	_ = conn.Close(ctx)
	return nil
}

// Start blocks, consuming events. It reconnects to the pgvector database with
// backoff on connection loss, and retries embedding API calls per event.
func (a *Adapter) Start(ctx context.Context, events <-chan event.Event) error {
	a.logger.Info("embedding adapter started", "table", a.table, "model", a.model, "columns", a.columns)

	var attempt int
	for {
		runErr := a.run(ctx, events)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if runErr == nil {
			return nil
		}

		delay := backoff.Jitter(attempt, a.backoffBase, a.backoffCap)
		a.logger.Error("db connection lost, reconnecting",
			"error", runErr,
			"attempt", attempt+1,
			"delay", delay,
		)
		attempt++

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

func (a *Adapter) run(ctx context.Context, events <-chan event.Event) error {
	conn, err := pgx.Connect(ctx, a.dbURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	safeTable := pgx.Identifier{a.table}.Sanitize()
	upsertSQL := fmt.Sprintf(`
		INSERT INTO %s (source_id, source_table, content, embedding, event_id, updated_at)
		VALUES ($1, $2, $3, $4::vector, $5, NOW())
		ON CONFLICT (source_id) DO UPDATE SET
		  content = EXCLUDED.content,
		  embedding = EXCLUDED.embedding,
		  event_id = EXCLUDED.event_id,
		  updated_at = NOW()`, safeTable)
	deleteSQL := fmt.Sprintf(`DELETE FROM %s WHERE source_id = $1`, safeTable)

	a.logger.Info("connected to pgvector database", "table", a.table)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case ev, ok := <-events:
			if !ok {
				return nil
			}

			embedCtx, span := a.startDeliverySpan(ctx, ev)
			_ = embedCtx // used for API calls below

			sourceID, sourceTable, content, isDel := a.extractPayload(ev)
			if sourceID == "" {
				a.logger.Warn("could not extract source ID, skipping event",
					"event_id", ev.ID,
					"id_column", a.idColumn,
				)
				if span != nil {
					span.End()
				}
				continue
			}

			if isDel {
				if _, err := conn.Exec(embedCtx, deleteSQL, sourceID); err != nil {
					if conn.IsClosed() {
						if span != nil {
							span.RecordError(err)
							span.SetStatus(codes.Error, err.Error())
							span.End()
						}
						return fmt.Errorf("delete (connection lost): %w", err)
					}
					a.logger.Warn("delete failed, skipping event", "event_id", ev.ID, "error", err)
				} else {
					metrics.EventsDelivered.WithLabelValues("embedding").Inc()
					a.logger.Debug("vector deleted", "source_id", sourceID)
				}
				if span != nil {
					span.End()
				}
				continue
			}

			if content == "" {
				a.logger.Warn("no text content extracted, skipping event",
					"event_id", ev.ID,
					"columns", a.columns,
				)
				if span != nil {
					span.End()
				}
				continue
			}

			vec, err := a.embed(embedCtx, ev.ID, content)
			if err != nil {
				metrics.EmbeddingAPIErrors.Inc()
				if span != nil {
					span.RecordError(err)
					span.SetStatus(codes.Error, err.Error())
					span.End()
				}
				a.logger.Error("embedding failed, skipping event", "event_id", ev.ID, "error", err)
				if a.dlq != nil {
					if dlqErr := a.dlq.Record(ctx, ev, "embedding", err); dlqErr != nil {
						a.logger.Error("dlq record failed", "error", dlqErr)
					}
				}
				continue
			}

			vecStr := formatVector(vec)
			_, err = conn.Exec(embedCtx, upsertSQL, sourceID, sourceTable, content, vecStr, ev.ID)
			if err != nil {
				if conn.IsClosed() {
					if span != nil {
						span.RecordError(err)
						span.SetStatus(codes.Error, err.Error())
						span.End()
					}
					return fmt.Errorf("upsert (connection lost): %w", err)
				}
				a.logger.Warn("upsert failed, skipping event", "event_id", ev.ID, "error", err)
				if span != nil {
					span.End()
				}
				continue
			}

			metrics.EventsDelivered.WithLabelValues("embedding").Inc()
			a.logger.Debug("vector upserted", "source_id", sourceID, "source_table", sourceTable)
			if span != nil {
				span.End()
			}
		}
	}
}

// embeddingRequest is the OpenAI-compatible request body.
type embeddingRequest struct {
	Model string `json:"model"`
	Input string `json:"input"`
}

// embeddingResponse is the OpenAI-compatible response body.
type embeddingResponse struct {
	Data []struct {
		Embedding []float64 `json:"embedding"`
	} `json:"data"`
	Usage struct {
		TotalTokens int `json:"total_tokens"`
	} `json:"usage"`
}

// startDeliverySpan creates a CONSUMER span linked to the detector span on the event.
// Returns the enriched context and span (nil when tracing is disabled).
func (a *Adapter) startDeliverySpan(ctx context.Context, ev event.Event) (context.Context, trace.Span) {
	if a.tracer == nil {
		return ctx, nil
	}
	var opts []trace.SpanStartOption
	opts = append(opts,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("pgcdc.adapter", "embedding"),
			attribute.String("pgcdc.event.id", ev.ID),
			attribute.String("pgcdc.channel", ev.Channel),
			attribute.String("pgcdc.operation", ev.Operation),
		),
	)
	if ev.SpanContext.IsValid() {
		opts = append(opts, trace.WithLinks(trace.Link{SpanContext: ev.SpanContext}))
		ctx = trace.ContextWithRemoteSpanContext(ctx, ev.SpanContext)
	}
	return a.tracer.Start(ctx, "pgcdc.adapter.deliver", opts...)
}

// embed calls the embedding API with per-event retry and returns the vector.
func (a *Adapter) embed(ctx context.Context, eventID, text string) ([]float64, error) {
	body, err := json.Marshal(embeddingRequest{Model: a.model, Input: text})
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	var lastStatus int
	var lastErr error

	for attempt := range a.maxRetries {
		if attempt > 0 {
			wait := backoff.Jitter(attempt, a.backoffBase, a.backoffCap)
			a.logger.Info("retrying embedding API call",
				"event_id", eventID,
				"attempt", attempt+1,
				"backoff", wait,
			)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(wait):
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.apiURL, bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		if a.apiKey != "" {
			req.Header.Set("Authorization", "Bearer "+a.apiKey)
		}
		// Inject W3C trace context if span is active.
		tracing.InjectHTTP(ctx, req.Header)

		start := time.Now()
		resp, err := a.client.Do(req)
		metrics.EmbeddingAPIDuration.Observe(time.Since(start).Seconds())
		metrics.EmbeddingAPIRequests.Inc()

		if err != nil {
			lastErr = err
			a.logger.Warn("embedding API request failed", "event_id", eventID, "attempt", attempt+1, "error", err)
			continue
		}

		var result embeddingResponse
		decodeErr := json.NewDecoder(resp.Body).Decode(&result)
		_ = resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			if decodeErr != nil {
				return nil, fmt.Errorf("decode response: %w", decodeErr)
			}
			if len(result.Data) == 0 || len(result.Data[0].Embedding) == 0 {
				return nil, fmt.Errorf("empty embedding in response")
			}
			metrics.EmbeddingTokensUsed.Add(float64(result.Usage.TotalTokens))
			return result.Data[0].Embedding, nil
		}

		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			lastStatus = resp.StatusCode
			a.logger.Warn("retryable embedding API response",
				"event_id", eventID,
				"status", resp.StatusCode,
				"attempt", attempt+1,
			)
			continue
		}

		// Non-retryable 4xx.
		a.logger.Error("non-retryable embedding API response, skipping event",
			"event_id", eventID,
			"status", resp.StatusCode,
		)
		return nil, nil // signal skip
	}

	return nil, &pgcdcerr.EmbeddingDeliveryError{
		EventID:    eventID,
		Model:      a.model,
		StatusCode: lastStatus,
		Retries:    a.maxRetries,
		Err:        lastErr,
	}
}

// payload is the decoded event payload structure shared across all event sources.
type payload struct {
	Op    string                 `json:"op"`
	Table string                 `json:"table"`
	Row   map[string]interface{} `json:"row"`
}

// extractPayload parses the event payload and returns the source ID, table,
// concatenated text content, and whether this is a DELETE operation.
func (a *Adapter) extractPayload(ev event.Event) (sourceID, sourceTable, content string, isDel bool) {
	var p payload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		a.logger.Warn("failed to parse event payload", "event_id", ev.ID, "error", err)
		return "", "", "", false
	}

	sourceTable = p.Table
	isDel = p.Op == "DELETE"

	if p.Row == nil {
		return "", sourceTable, "", isDel
	}

	// Extract source ID.
	if v, ok := p.Row[a.idColumn]; ok && v != nil {
		sourceID = fmt.Sprintf("%v", v)
	}

	// Concatenate text columns.
	var parts []string
	for _, col := range a.columns {
		if v, ok := p.Row[col]; ok && v != nil {
			parts = append(parts, fmt.Sprintf("%v", v))
		}
	}
	content = strings.Join(parts, " ")

	return sourceID, sourceTable, content, isDel
}

// formatVector converts a float64 slice to the pgvector string format "[v1,v2,...]".
func formatVector(vec []float64) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, v := range vec {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%g", v)
	}
	b.WriteByte(']')
	return b.String()
}
