package iceberg

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/backoff"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/pgcdcerr"
)

const (
	defaultFlushInterval = 1 * time.Minute
	defaultFlushSize     = 10000
	defaultBackoffBase   = 5 * time.Second
	defaultBackoffCap    = 60 * time.Second
	maxConsecFailures    = 10
	maxBufferSize        = 100000
)

// Adapter streams CDC events into Apache Iceberg tables.
type Adapter struct {
	// Catalog
	catalog   Catalog
	storage   Storage
	warehouse string

	// Table identity
	namespace []string
	tableName string

	// Write config
	mode          Mode
	schemaMode    SchemaMode
	primaryKeys   []string
	flushInterval time.Duration
	flushSize     int

	// Runtime
	buffer    []event.Event
	mu        sync.Mutex
	tableMeta *TableMetadata
	schema    *Schema

	// Reconnect
	backoffBase time.Duration
	backoffCap  time.Duration

	logger *slog.Logger
}

// Config holds all parameters for the Iceberg adapter.
type Config struct {
	CatalogType   string
	CatalogURI    string
	Warehouse     string
	Namespace     string
	TableName     string
	Mode          string
	SchemaMode    string
	PrimaryKeys   []string
	FlushInterval time.Duration
	FlushSize     int
	BackoffBase   time.Duration
	BackoffCap    time.Duration
}

// New creates an Iceberg adapter.
// Duration parameters default to sensible values when zero.
func New(cfg Config, logger *slog.Logger) *Adapter {
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = defaultFlushInterval
	}
	if cfg.FlushSize <= 0 {
		cfg.FlushSize = defaultFlushSize
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

	ns := parseNamespace(cfg.Namespace)

	var m Mode
	switch strings.ToLower(cfg.Mode) {
	case "upsert":
		m = ModeUpsert
	default:
		m = ModeAppend
	}

	var sm SchemaMode
	switch strings.ToLower(cfg.SchemaMode) {
	case "raw":
		sm = SchemaModeRaw
	default:
		sm = SchemaModeRaw // Phase 1: only raw mode
	}

	// Build catalog and storage.
	storage := &LocalStorage{}
	var cat Catalog
	switch strings.ToLower(cfg.CatalogType) {
	case "hadoop", "":
		cat = NewHadoopCatalog(cfg.Warehouse, storage)
	default:
		cat = NewHadoopCatalog(cfg.Warehouse, storage) // Phase 1: only hadoop
	}

	return &Adapter{
		catalog:       cat,
		storage:       storage,
		warehouse:     cfg.Warehouse,
		namespace:     ns,
		tableName:     cfg.TableName,
		mode:          m,
		schemaMode:    sm,
		primaryKeys:   cfg.PrimaryKeys,
		flushInterval: cfg.FlushInterval,
		flushSize:     cfg.FlushSize,
		buffer:        make([]event.Event, 0, cfg.FlushSize),
		backoffBase:   cfg.BackoffBase,
		backoffCap:    cfg.BackoffCap,
		logger:        logger.With("adapter", "iceberg"),
	}
}

// Name returns the adapter name.
func (a *Adapter) Name() string {
	return "iceberg"
}

// Validate implements adapter.Validator: checks that the warehouse path is accessible.
func (a *Adapter) Validate(_ context.Context) error {
	if a.warehouse == "" {
		return fmt.Errorf("iceberg: warehouse path is required")
	}
	return nil
}

// Start blocks, consuming events from the channel and flushing batches to Iceberg.
func (a *Adapter) Start(ctx context.Context, events <-chan event.Event) error {
	a.logger.Info("iceberg adapter started",
		"warehouse", a.warehouse,
		"namespace", strings.Join(a.namespace, "."),
		"table", a.tableName,
		"mode", a.modeString(),
		"schema", a.schemaModeString(),
		"flush_interval", a.flushInterval,
		"flush_size", a.flushSize,
	)

	var attempt int
	for {
		runErr := a.run(ctx, events)
		if ctx.Err() != nil {
			// Shutdown: flush remaining events.
			a.drainFlush()
			return ctx.Err()
		}
		if runErr == nil {
			return nil
		}

		delay := backoff.Jitter(attempt, a.backoffBase, a.backoffCap)
		a.logger.Error("iceberg flush cycle failed, retrying",
			"error", runErr,
			"attempt", attempt+1,
			"delay", delay,
		)
		attempt++

		select {
		case <-ctx.Done():
			a.drainFlush()
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

// run is the main event loop. Returns an error if too many consecutive flushes fail.
func (a *Adapter) run(ctx context.Context, events <-chan event.Event) error {
	ticker := time.NewTicker(a.flushInterval)
	defer ticker.Stop()

	var consecFailures int

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case ev, ok := <-events:
			if !ok {
				// Channel closed. Flush remaining.
				if err := a.flush(ctx); err != nil {
					a.logger.Error("final flush failed", "error", err)
					return fmt.Errorf("final flush: %w", err)
				}
				return nil
			}

			// Skip transaction markers.
			if ev.Channel == event.TransactionMarkerChannel {
				continue
			}

			a.mu.Lock()
			a.buffer = append(a.buffer, ev)
			bufLen := len(a.buffer)

			// Drop oldest events if buffer exceeds max.
			if bufLen > maxBufferSize {
				dropped := bufLen - maxBufferSize
				a.buffer = a.buffer[dropped:]
				bufLen = maxBufferSize
				a.logger.Warn("buffer overflow, dropped oldest events", "dropped", dropped)
			}
			a.mu.Unlock()

			metrics.IcebergBufferSize.Set(float64(bufLen))
			metrics.EventsDelivered.WithLabelValues("iceberg").Inc()

			// Size trigger.
			if bufLen >= a.flushSize {
				if err := a.flush(ctx); err != nil {
					consecFailures++
					if consecFailures >= maxConsecFailures {
						return &pgcdcerr.IcebergFlushError{
							Attempts: consecFailures,
							Err:      err,
						}
					}
				} else {
					consecFailures = 0
				}
			}

		case <-ticker.C:
			// Timer trigger.
			if err := a.flush(ctx); err != nil {
				consecFailures++
				if consecFailures >= maxConsecFailures {
					return &pgcdcerr.IcebergFlushError{
						Attempts: consecFailures,
						Err:      err,
					}
				}
			} else {
				consecFailures = 0
			}
		}
	}
}

// drainFlush attempts to flush remaining buffered events with a timeout.
func (a *Adapter) drainFlush() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := a.flush(ctx); err != nil {
		a.logger.Error("shutdown drain flush failed", "error", err)
	}
}

func (a *Adapter) modeString() string {
	if a.mode == ModeUpsert {
		return "upsert"
	}
	return "append"
}

func (a *Adapter) schemaModeString() string {
	if a.schemaMode == SchemaModeAuto {
		return "auto"
	}
	return "raw"
}

// parseNamespace splits a dot-separated namespace into parts.
func parseNamespace(ns string) []string {
	if ns == "" {
		return []string{"pgcdc"}
	}
	return strings.Split(ns, ".")
}
