package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/backoff"
	"github.com/florinutz/pgcdc/metrics"
)

const source = "outbox"

const (
	defaultPollInterval = 500 * time.Millisecond
	defaultBatchSize    = 100
	defaultBackoffBase  = 1 * time.Second
	defaultBackoffCap   = 30 * time.Second
)

// Detector implements detector.Detector by polling a transactional outbox table.
// It uses SELECT ... FOR UPDATE SKIP LOCKED for concurrency-safe processing.
type Detector struct {
	dbURL         string
	table         string
	pollInterval  time.Duration
	batchSize     int
	keepProcessed bool
	backoffBase   time.Duration
	backoffCap    time.Duration
	logger        *slog.Logger
	tracer        trace.Tracer
}

// SetTracer sets the OpenTelemetry tracer for creating per-event spans.
func (d *Detector) SetTracer(t trace.Tracer) {
	d.tracer = t
}

// New creates an outbox detector. Duration and size parameters default to
// sensible values when zero.
func New(
	dbURL, table string,
	pollInterval time.Duration,
	batchSize int,
	keepProcessed bool,
	backoffBase, backoffCap time.Duration,
	logger *slog.Logger,
) *Detector {
	if logger == nil {
		logger = slog.Default()
	}
	if pollInterval <= 0 {
		pollInterval = defaultPollInterval
	}
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	if backoffBase <= 0 {
		backoffBase = defaultBackoffBase
	}
	if backoffCap <= 0 {
		backoffCap = defaultBackoffCap
	}
	if table == "" {
		table = "pgcdc_outbox"
	}
	return &Detector{
		dbURL:         dbURL,
		table:         table,
		pollInterval:  pollInterval,
		batchSize:     batchSize,
		keepProcessed: keepProcessed,
		backoffBase:   backoffBase,
		backoffCap:    backoffCap,
		logger:        logger.With("detector", source),
	}
}

// Name returns the detector name.
func (d *Detector) Name() string {
	return source
}

// Start polls the outbox table and emits events. It blocks until ctx is
// cancelled. On connection failure, it reconnects with exponential backoff.
// The caller owns the events channel; Start does NOT close it.
func (d *Detector) Start(ctx context.Context, events chan<- event.Event) error {
	var attempt int
	for {
		err := d.run(ctx, events)
		if ctx.Err() != nil {
			return ctx.Err()
		}

		delay := backoff.Jitter(attempt, d.backoffBase, d.backoffCap)
		d.logger.Error("outbox connection lost, reconnecting",
			"error", err,
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

// outboxRow represents a row from the outbox table.
type outboxRow struct {
	ID        string
	Channel   string
	Operation string
	Payload   json.RawMessage
}

func (d *Detector) run(ctx context.Context, events chan<- event.Event) error {
	conn, err := pgx.Connect(ctx, d.dbURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	safeTable := pgx.Identifier{d.table}.Sanitize()
	d.logger.Info("outbox detector started", "table", d.table, "poll_interval", d.pollInterval)

	ticker := time.NewTicker(d.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			n, err := d.poll(ctx, conn, safeTable, events)
			if err != nil {
				return err
			}
			metrics.OutboxPolled.Inc()
			if n > 0 {
				metrics.OutboxEventsProcessed.Add(float64(n))
				d.logger.Debug("outbox poll", "processed", n)
			}
		}
	}
}

func (d *Detector) poll(ctx context.Context, conn *pgx.Conn, safeTable string, events chan<- event.Event) (int, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// SELECT rows FOR UPDATE SKIP LOCKED — safe for concurrent pgcdc instances.
	query := fmt.Sprintf(
		"SELECT id, channel, operation, payload FROM %s ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED",
		safeTable,
	)
	rows, err := tx.Query(ctx, query, d.batchSize)
	if err != nil {
		return 0, fmt.Errorf("query outbox: %w", err)
	}

	var processed []outboxRow
	for rows.Next() {
		var r outboxRow
		if err := rows.Scan(&r.ID, &r.Channel, &r.Operation, &r.Payload); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan outbox row: %w", err)
		}
		processed = append(processed, r)
	}
	rows.Close()
	if rows.Err() != nil {
		return 0, fmt.Errorf("read outbox rows: %w", rows.Err())
	}

	if len(processed) == 0 {
		return 0, nil
	}

	// Emit events for each row.
	for _, r := range processed {
		ev, evErr := event.New(r.Channel, r.Operation, r.Payload, source)
		if evErr != nil {
			d.logger.Error("create event failed", "error", evErr, "outbox_id", r.ID)
			continue
		}

		if d.tracer != nil {
			_, span := d.tracer.Start(ctx, "pgcdc.detect",
				trace.WithSpanKind(trace.SpanKindProducer),
				trace.WithAttributes(
					attribute.String("pgcdc.event.id", ev.ID),
					attribute.String("pgcdc.channel", ev.Channel),
					attribute.String("pgcdc.operation", r.Operation),
					attribute.String("pgcdc.source", source),
				),
			)
			ev.SpanContext = span.SpanContext()
			span.End()
		}

		select {
		case events <- ev:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}

	// Clean up processed rows.
	ids := make([]string, len(processed))
	for i, r := range processed {
		ids[i] = r.ID
	}

	if d.keepProcessed {
		// Mark as processed instead of deleting.
		_, err = tx.Exec(ctx,
			fmt.Sprintf("UPDATE %s SET processed_at = now() WHERE id = ANY($1)", safeTable),
			ids,
		)
	} else {
		_, err = tx.Exec(ctx,
			fmt.Sprintf("DELETE FROM %s WHERE id = ANY($1)", safeTable),
			ids,
		)
	}
	if err != nil {
		return 0, fmt.Errorf("cleanup outbox rows: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit tx: %w", err)
	}

	return len(processed), nil
}
