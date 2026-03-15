//go:build !no_clickhouse

package clickhouse

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/adapter/batch"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/reconnect"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/pgcdcerr"
)

const (
	defaultBatchSize     = 10000
	defaultFlushInterval = 1 * time.Second
	defaultBackoffBase   = 5 * time.Second
	defaultBackoffCap    = 60 * time.Second
)

// Adapter streams CDC events to ClickHouse via batch INSERT.
type Adapter struct {
	dsn           string
	table         string
	autoCreate    bool
	asyncInsert   bool
	settings      map[string]string
	batchSize     int
	flushInterval time.Duration
	backoffBase   time.Duration
	backoffCap    time.Duration
	logger        *slog.Logger
	conn          ch.Conn
	ackFn         adapter.AckFunc
	runner        *batch.Runner
}

// New creates a ClickHouse adapter.
// Duration parameters default to sensible values when zero.
func New(
	dsn, table string,
	autoCreate, asyncInsert bool,
	settings map[string]string,
	batchSize int,
	flushInterval time.Duration,
	backoffBase, backoffCap time.Duration,
	logger *slog.Logger,
) *Adapter {
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	if flushInterval <= 0 {
		flushInterval = defaultFlushInterval
	}
	if backoffBase <= 0 {
		backoffBase = defaultBackoffBase
	}
	if backoffCap <= 0 {
		backoffCap = defaultBackoffCap
	}
	if table == "" {
		table = "pgcdc_events"
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &Adapter{
		dsn:           dsn,
		table:         table,
		autoCreate:    autoCreate,
		asyncInsert:   asyncInsert,
		settings:      settings,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		backoffBase:   backoffBase,
		backoffCap:    backoffCap,
		logger:        logger.With("adapter", "clickhouse"),
	}
}

// Name returns the adapter name.
func (a *Adapter) Name() string { return "clickhouse" }

// SetAckFunc sets the cooperative checkpoint acknowledgement function.
func (a *Adapter) SetAckFunc(fn adapter.AckFunc) { a.ackFn = fn }

// BatchConfig returns the batch accumulation configuration.
func (a *Adapter) BatchConfig() adapter.BatchConfig {
	return adapter.BatchConfig{
		MaxSize:       a.batchSize,
		FlushInterval: a.flushInterval,
	}
}

// Validate checks ClickHouse connectivity and optionally creates the target table.
func (a *Adapter) Validate(ctx context.Context) error {
	conn, err := a.open()
	if err != nil {
		return fmt.Errorf("open clickhouse: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("ping clickhouse: %w", err)
	}

	if a.autoCreate {
		if err := a.createTable(ctx, conn); err != nil {
			return fmt.Errorf("auto-create table: %w", err)
		}
	}

	return nil
}

// Drain flushes remaining buffered events. The batch.Runner handles this
// internally when its Start returns.
func (a *Adapter) Drain(ctx context.Context) error {
	return nil
}

// Start connects to ClickHouse and delegates to the batch runner.
func (a *Adapter) Start(ctx context.Context, events <-chan event.Event) error {
	a.logger.Info("clickhouse adapter started",
		"dsn", a.dsn,
		"table", a.table,
		"auto_create", a.autoCreate,
		"batch_size", a.batchSize,
		"flush_interval", a.flushInterval,
	)

	return reconnect.Loop(ctx, "clickhouse", a.backoffBase, a.backoffCap,
		a.logger, nil,
		func(ctx context.Context) error {
			conn, err := a.open()
			if err != nil {
				return fmt.Errorf("open clickhouse: %w", err)
			}
			a.conn = conn
			defer func() {
				_ = conn.Close()
				a.conn = nil
			}()

			if err := conn.Ping(ctx); err != nil {
				return fmt.Errorf("ping clickhouse: %w", err)
			}

			if a.autoCreate {
				if err := a.createTable(ctx, conn); err != nil {
					return fmt.Errorf("auto-create table: %w", err)
				}
			}

			a.runner = batch.New("clickhouse", a, a.batchSize, a.flushInterval, a.logger)
			if a.ackFn != nil {
				a.runner.SetAckFunc(a.ackFn)
			}

			return a.runner.Start(ctx, events)
		})
}

// Flush inserts a batch of events into ClickHouse.
func (a *Adapter) Flush(ctx context.Context, events []event.Event) adapter.FlushResult {
	start := time.Now()

	// Prepare batch insert. Table name comes from config (not user input).
	chBatch, err := a.conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", a.table))
	if err != nil {
		metrics.ClickHouseFlushes.WithLabelValues("error").Inc()
		return adapter.FlushResult{
			Err: &pgcdcerr.ClickHouseFlushError{Table: a.table, Err: fmt.Errorf("prepare batch: %w", err)},
		}
	}

	var failed []adapter.FailedEvent
	appended := 0

	// In-batch dedup: skip duplicate event IDs within the same flush.
	seen := make(map[string]struct{}, len(events))
	skipped := 0

	for _, ev := range events {
		if _, dup := seen[ev.ID]; dup {
			skipped++
			continue
		}
		seen[ev.ID] = struct{}{}

		ev.EnsurePayload()
		if err := chBatch.Append(
			ev.ID,
			ev.Channel,
			ev.Operation,
			ev.Source,
			string(ev.Payload),
			ev.LSN,
			ev.CreatedAt,
			uint64(time.Now().UnixMilli()),
		); err != nil {
			failed = append(failed, adapter.FailedEvent{Event: ev, Err: err})
			continue
		}
		appended++
	}

	if appended > 0 {
		if err := chBatch.Send(); err != nil {
			metrics.ClickHouseFlushes.WithLabelValues("error").Inc()
			metrics.ClickHouseFlushDuration.Observe(time.Since(start).Seconds())
			return adapter.FlushResult{
				Err: &pgcdcerr.ClickHouseFlushError{Table: a.table, Err: fmt.Errorf("send batch: %w", err)},
			}
		}
	}

	duration := time.Since(start).Seconds()
	metrics.ClickHouseFlushes.WithLabelValues("ok").Inc()
	metrics.ClickHouseFlushSize.Observe(float64(appended))
	metrics.ClickHouseFlushDuration.Observe(duration)
	metrics.ClickHouseRows.Add(float64(appended))
	metrics.EventsDelivered.WithLabelValues("clickhouse").Add(float64(appended))
	if skipped > 0 {
		metrics.ClickHouseDedupSkipped.Add(float64(skipped))
	}

	return adapter.FlushResult{
		Delivered: appended,
		Failed:    failed,
	}
}

// open parses the DSN and opens a ClickHouse connection.
func (a *Adapter) open() (ch.Conn, error) {
	opts, err := ch.ParseDSN(a.dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	// Apply async insert settings.
	if a.asyncInsert {
		if opts.Settings == nil {
			opts.Settings = make(ch.Settings)
		}
		opts.Settings["async_insert"] = "1"
		opts.Settings["wait_for_async_insert"] = "1"
	}
	// Apply custom query settings.
	for k, v := range a.settings {
		if opts.Settings == nil {
			opts.Settings = make(ch.Settings)
		}
		opts.Settings[k] = v
	}
	conn, err := ch.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	return conn, nil
}

// createTable issues a CREATE TABLE IF NOT EXISTS with a ReplacingMergeTree engine.
func (a *Adapter) createTable(ctx context.Context, conn ch.Conn) error {
	// Table name is a config value, not user input.
	ddl := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    id String,
    channel LowCardinality(String),
    operation LowCardinality(String),
    source LowCardinality(String),
    payload String,
    lsn UInt64,
    created_at DateTime64(3),
    _version UInt64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (channel, id)`, a.table)

	return conn.Exec(ctx, ddl)
}
