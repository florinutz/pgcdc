package embedding

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/backoff"
	"github.com/florinutz/pgcdc/internal/reconnect"
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
//
// Implements adapter.Deliverer — CB, rate-limit, retry, DLQ, and tracing are
// provided by the middleware chain configured in the registry entry. The
// adapter itself handles only HTTP embedding and pgvector persistence.
type Adapter struct {
	apiURL        string
	apiKey        string
	model         string
	columns       []string
	idColumn      string
	dbURL         string
	table         string
	dimension     int
	maxRetries    int
	backoffBase   time.Duration
	backoffCap    time.Duration
	skipUnchanged bool
	client        *http.Client
	logger        *slog.Logger
	upsertSQL     string    // precomputed from table name in New()
	deleteSQL     string    // precomputed from table name in New()
	conn          *pgx.Conn // persistent connection; lazily initialized in Deliver/run
}

// New creates an embedding adapter.
// apiURL is an OpenAI-compatible endpoint (e.g. https://api.openai.com/v1/embeddings).
// columns is the list of payload row fields to concatenate for embedding.
// idColumn is the payload row field used as the source primary key for UPSERT/DELETE.
// Duration parameters default to sensible values when zero.
// CB, rate-limit, retry, DLQ, and tracing are handled by the middleware chain — use
// registry.AdapterResult.MiddlewareConfig to configure them.
func New(
	apiURL, apiKey, model string,
	columns []string,
	idColumn string,
	dbURL, table string,
	dimension int,
	maxRetries int,
	timeout, backoffBase, backoffCap time.Duration,
	skipUnchanged bool,
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
	safeTable := pgx.Identifier{table}.Sanitize()
	return &Adapter{
		apiURL:        apiURL,
		apiKey:        apiKey,
		model:         model,
		columns:       columns,
		idColumn:      idColumn,
		dbURL:         dbURL,
		table:         table,
		dimension:     dimension,
		maxRetries:    maxRetries,
		backoffBase:   backoffBase,
		backoffCap:    backoffCap,
		skipUnchanged: skipUnchanged,
		client:        &http.Client{Timeout: timeout},
		logger:        logger.With("adapter", "embedding"),
		upsertSQL: fmt.Sprintf(`
			INSERT INTO %s (source_id, source_table, content, embedding, event_id, updated_at)
			VALUES ($1, $2, $3, $4::vector, $5, NOW())
			ON CONFLICT (source_id) DO UPDATE SET
			  content = EXCLUDED.content,
			  embedding = EXCLUDED.embedding,
			  event_id = EXCLUDED.event_id,
			  updated_at = NOW()`, safeTable),
		deleteSQL: fmt.Sprintf(`DELETE FROM %s WHERE source_id = $1`, safeTable),
	}
}

// Name returns the adapter name.
func (a *Adapter) Name() string {
	return "embedding"
}

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

// Deliver implements adapter.Deliverer. Called by the middleware chain for each event.
// It manages a persistent pgvector connection, lazily initializing and reconnecting
// as needed. CB, rate-limit, retry, DLQ, and tracing are handled by the middleware.
func (a *Adapter) Deliver(ctx context.Context, ev event.Event) error {
	if err := a.ensureConn(ctx); err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	sourceID, sourceTable, content, isDel, p := a.extractPayload(ev)
	if sourceID == "" {
		a.logger.Warn("could not extract source ID, skipping event",
			"event_id", ev.ID,
			"id_column", a.idColumn,
		)
		return nil
	}

	if isDel {
		if _, err := a.conn.Exec(ctx, a.deleteSQL, sourceID); err != nil {
			if a.conn.IsClosed() {
				a.conn = nil
				return fmt.Errorf("delete (connection lost): %w", err)
			}
			a.logger.Warn("delete failed, skipping event", "event_id", ev.ID, "error", err)
			return nil
		}
		metrics.EventsDelivered.WithLabelValues("embedding").Inc()
		a.logger.Debug("vector deleted", "source_id", sourceID)
		return nil
	}

	if !a.embeddingColumnsChanged(p) {
		metrics.EmbeddingSkipped.Inc()
		a.logger.Debug("skipping unchanged embedding columns", "event_id", ev.ID, "source_id", sourceID)
		return nil
	}

	if content == "" {
		a.logger.Warn("no text content extracted, skipping event",
			"event_id", ev.ID,
			"columns", a.columns,
		)
		return nil
	}

	vec, err := a.embed(ctx, ev.ID, content)
	if err != nil {
		metrics.EmbeddingAPIErrors.Inc()
		a.logger.Error("embedding failed", "event_id", ev.ID, "error", err)
		return err
	}
	if vec == nil {
		// Non-retryable API error; already logged in embed(). Skip event.
		return nil
	}

	vecStr := formatVector(vec)
	if _, err = a.conn.Exec(ctx, a.upsertSQL, sourceID, sourceTable, content, vecStr, ev.ID); err != nil {
		if a.conn.IsClosed() {
			a.conn = nil
			return fmt.Errorf("upsert (connection lost): %w", err)
		}
		a.logger.Warn("upsert failed, skipping event", "event_id", ev.ID, "error", err)
		return nil
	}

	metrics.EventsDelivered.WithLabelValues("embedding").Inc()
	a.logger.Debug("vector upserted", "source_id", sourceID, "source_table", sourceTable)
	return nil
}

// Start implements adapter.Adapter for backwards compatibility (e.g. direct calls in tests).
// When the pipeline detects adapter.Deliverer, it uses the middleware path instead and
// Start is never called.
func (a *Adapter) Start(ctx context.Context, events <-chan event.Event) error {
	a.logger.Info("embedding adapter started", "table", a.table, "model", a.model, "columns", a.columns)

	return reconnect.Loop(ctx, "embedding", a.backoffBase, a.backoffCap,
		a.logger, nil,
		func(ctx context.Context) error {
			if err := a.ensureConn(ctx); err != nil {
				return err
			}
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case ev, ok := <-events:
					if !ok {
						return nil
					}
					if err := a.Deliver(ctx, ev); err != nil {
						// If connection was lost, return to trigger reconnect.
						if a.conn == nil {
							return err
						}
						a.logger.Error("delivery failed", "event_id", ev.ID, "error", err)
					}
				}
			}
		})
}

// ensureConn lazily initializes or reconnects the pgvector connection.
func (a *Adapter) ensureConn(ctx context.Context) error {
	if a.conn != nil && !a.conn.IsClosed() {
		return nil
	}
	conn, err := pgx.Connect(ctx, a.dbURL)
	if err != nil {
		return err
	}
	a.conn = conn
	a.logger.Info("connected to pgvector database", "table", a.table)
	return nil
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

// embed calls the embedding API with per-event retry and returns the vector.
// Returns (nil, nil) for non-retryable API errors (event should be skipped).
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
		// Inject W3C trace context if a span is active in ctx (set by Tracing middleware).
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

		// Non-retryable 4xx: skip event.
		a.logger.Error("non-retryable embedding API response, skipping event",
			"event_id", eventID,
			"status", resp.StatusCode,
		)
		return nil, nil
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
	Old   map[string]interface{} `json:"old"`
}

// extractPayload parses the event payload and returns the source ID, table,
// concatenated text content, whether this is a DELETE operation, and the
// parsed payload (for change detection).
func (a *Adapter) extractPayload(ev event.Event) (sourceID, sourceTable, content string, isDel bool, p payload) {
	// Structured record path: zero JSON parsing.
	if rec := ev.Record(); rec != nil && rec.Operation != 0 &&
		(rec.Change.After != nil || rec.Change.Before != nil) {
		isDel = rec.Operation == event.OperationDelete
		sourceTable = rec.Metadata[event.MetaTable]

		var row map[string]any
		var old map[string]any
		if rec.Change.After != nil {
			row = rec.Change.After.ToMap()
		}
		if rec.Change.Before != nil {
			old = rec.Change.Before.ToMap()
		}

		// For DELETE, row data is in Before.
		if isDel && row == nil {
			row = old
			old = nil
		}

		if row == nil {
			return "", sourceTable, "", isDel, p
		}

		// Extract source ID.
		if v, ok := row[a.idColumn]; ok && v != nil {
			if s, ok := v.(string); ok {
				sourceID = s
			} else {
				sourceID = fmt.Sprintf("%v", v)
			}
		}

		// Concatenate text columns.
		var parts []string
		for _, col := range a.columns {
			if v, ok := row[col]; ok && v != nil {
				if s, ok := v.(string); ok {
					parts = append(parts, s)
				} else {
					parts = append(parts, fmt.Sprintf("%v", v))
				}
			}
		}
		content = strings.Join(parts, " ")

		// Populate p for embeddingColumnsChanged compatibility.
		p.Op = rec.Operation.String()
		p.Table = sourceTable
		p.Row = row
		p.Old = old

		return sourceID, sourceTable, content, isDel, p
	}

	// Legacy path: parse payload JSON.
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		a.logger.Warn("failed to parse event payload", "event_id", ev.ID, "error", err)
		return "", "", "", false, p
	}

	sourceTable = p.Table
	isDel = p.Op == "DELETE"

	if p.Row == nil {
		return "", sourceTable, "", isDel, p
	}

	// Extract source ID.
	if v, ok := p.Row[a.idColumn]; ok && v != nil {
		if s, ok := v.(string); ok {
			sourceID = s
		} else {
			sourceID = fmt.Sprintf("%v", v)
		}
	}

	// Concatenate text columns.
	var parts []string
	for _, col := range a.columns {
		if v, ok := p.Row[col]; ok && v != nil {
			if s, ok := v.(string); ok {
				parts = append(parts, s)
			} else {
				parts = append(parts, fmt.Sprintf("%v", v))
			}
		}
	}
	content = strings.Join(parts, " ")

	return sourceID, sourceTable, content, isDel, p
}

// embeddingColumnsChanged returns true if any of the embedding columns
// differ between old and new row values. Returns true when old is nil
// (INSERT) or when skip-unchanged is disabled.
func (a *Adapter) embeddingColumnsChanged(p payload) bool {
	if !a.skipUnchanged {
		return true
	}
	if p.Op != "UPDATE" {
		return true
	}
	if p.Old == nil {
		return true // no old values to compare
	}
	for _, col := range a.columns {
		newVal := sprintValue(p.Row[col])
		oldVal := sprintValue(p.Old[col])
		if newVal != oldVal {
			return true
		}
	}
	return false
}

// sprintValue converts a value to a string, fast-pathing the common string case.
func sprintValue(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

// formatVector converts a float64 slice to the pgvector string format "[v1,v2,...]".
func formatVector(vec []float64) string {
	var b strings.Builder
	b.Grow(len(vec) * 12) // ~12 chars per float
	b.WriteByte('[')
	for i, v := range vec {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatFloat(v, 'f', -1, 64))
	}
	b.WriteByte(']')
	return b.String()
}
