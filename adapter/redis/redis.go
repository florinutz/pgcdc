package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/florinutz/pgcdc/dlq"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/reconnect"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/pgcdcerr"
	goredis "github.com/redis/go-redis/v9"
)

const (
	defaultIDColumn    = "id"
	defaultBackoffBase = 1 * time.Second
	defaultBackoffCap  = 30 * time.Second
)

// Adapter invalidates or syncs Redis cache based on CDC events.
// In "invalidate" mode, DEL is issued on any change.
// In "sync" mode, SET for INSERT/UPDATE, DEL for DELETE.
type Adapter struct {
	url         string
	mode        string // "invalidate" or "sync"
	keyPrefix   string
	idColumn    string
	backoffBase time.Duration
	backoffCap  time.Duration
	logger      *slog.Logger
	dlq         dlq.DLQ
	mu          sync.Mutex
	rdb         *goredis.Client
}

// SetDLQ sets the dead letter queue for failed deliveries.
func (a *Adapter) SetDLQ(d dlq.DLQ) { a.dlq = d }

// Config holds all parameters for the Redis adapter.
type Config struct {
	URL         string
	Mode        string
	KeyPrefix   string
	IDColumn    string
	BackoffBase time.Duration
	BackoffCap  time.Duration
}

// New creates a Redis cache invalidation/sync adapter.
func New(cfg Config, logger *slog.Logger) *Adapter {
	if cfg.Mode == "" {
		cfg.Mode = "invalidate"
	}
	if cfg.IDColumn == "" {
		cfg.IDColumn = defaultIDColumn
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
		mode:        cfg.Mode,
		keyPrefix:   cfg.KeyPrefix,
		idColumn:    cfg.IDColumn,
		backoffBase: cfg.BackoffBase,
		backoffCap:  cfg.BackoffCap,
		logger:      logger.With("adapter", "redis"),
	}
}

func (a *Adapter) Name() string { return "redis" }

// Validate checks Redis connectivity via PING.
func (a *Adapter) Validate(ctx context.Context) error {
	opts, err := goredis.ParseURL(a.url)
	if err != nil {
		return fmt.Errorf("parse redis url: %w", err)
	}
	client := goredis.NewClient(opts)
	defer func() { _ = client.Close() }()
	if err := client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}
	return nil
}

// Start connects to Redis and processes events. It reconnects with backoff on
// connection loss.
func (a *Adapter) Start(ctx context.Context, events <-chan event.Event) error {
	a.logger.Info("redis adapter started", "mode", a.mode, "key_prefix", a.keyPrefix)

	return reconnect.Loop(ctx, "redis", a.backoffBase, a.backoffCap,
		a.logger, metrics.RedisErrors,
		func(ctx context.Context) error {
			return a.run(ctx, events)
		})
}

func (a *Adapter) run(ctx context.Context, events <-chan event.Event) error {
	opts, err := goredis.ParseURL(a.url)
	if err != nil {
		return fmt.Errorf("parse redis url: %w", err)
	}
	rdb := goredis.NewClient(opts)
	a.mu.Lock()
	a.rdb = rdb
	a.mu.Unlock()
	defer func() { a.mu.Lock(); a.rdb = nil; a.mu.Unlock() }()
	defer func() { _ = rdb.Close() }()

	// Verify connectivity.
	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}
	a.logger.Info("connected to redis", "url", a.url)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case ev, ok := <-events:
			if !ok {
				return nil
			}

			id, row, isDel, unchangedToast := a.extractPayload(ev)
			if id == "" {
				a.logger.Warn("no ID in event payload, skipping", "event_id", ev.ID)
				continue
			}

			key := a.keyPrefix + id

			switch {
			case a.mode == "invalidate":
				// Any change = DEL
				if err := rdb.Del(ctx, key).Err(); err != nil {
					if isConnectionError(err) {
						return &pgcdcerr.RedisOperationError{Operation: "del", Key: key, Err: err}
					}
					a.logger.Warn("redis del failed, skipping", "key", key, "error", err)
					a.recordDLQ(ctx, ev, err)
					continue
				}
				metrics.RedisOperations.WithLabelValues("del").Inc()

			case isDel:
				// sync mode DELETE
				if err := rdb.Del(ctx, key).Err(); err != nil {
					if isConnectionError(err) {
						return &pgcdcerr.RedisOperationError{Operation: "del", Key: key, Err: err}
					}
					a.logger.Warn("redis del failed, skipping", "key", key, "error", err)
					a.recordDLQ(ctx, ev, err)
					continue
				}
				metrics.RedisOperations.WithLabelValues("del").Inc()

			default:
				// sync mode INSERT/UPDATE
				finalRow := row

				// When unchanged TOAST columns are present, merge with
				// the existing Redis value so we don't overwrite stored
				// fields with null.
				if len(unchangedToast) > 0 {
					existing, getErr := rdb.Get(ctx, key).Result()
					if getErr == nil {
						var existingRow map[string]interface{}
						if json.Unmarshal([]byte(existing), &existingRow) == nil {
							// Start with existing, overlay changed fields.
							toastSet := make(map[string]struct{}, len(unchangedToast))
							for _, col := range unchangedToast {
								toastSet[col] = struct{}{}
							}
							for k, v := range row {
								if _, skip := toastSet[k]; !skip {
									existingRow[k] = v
								}
							}
							finalRow = existingRow
						}
					}
					// If GET fails (key doesn't exist), SET as-is (best effort).
				}

				data, err := json.Marshal(finalRow)
				if err != nil {
					a.logger.Warn("marshal row failed, skipping", "event_id", ev.ID, "error", err)
					continue
				}
				if err := rdb.Set(ctx, key, data, 0).Err(); err != nil {
					if isConnectionError(err) {
						return &pgcdcerr.RedisOperationError{Operation: "set", Key: key, Err: err}
					}
					a.logger.Warn("redis set failed, skipping", "key", key, "error", err)
					a.recordDLQ(ctx, ev, err)
					continue
				}
				metrics.RedisOperations.WithLabelValues("set").Inc()
			}

			metrics.EventsDelivered.WithLabelValues("redis").Inc()
			if !ev.CreatedAt.IsZero() {
				metrics.EventDeliveryLag.WithLabelValues("redis").Observe(time.Since(ev.CreatedAt).Seconds())
			}
		}
	}
}

// Drain closes the Redis connection during graceful shutdown.
func (a *Adapter) Drain(_ context.Context) error {
	a.mu.Lock()
	rdb := a.rdb
	a.mu.Unlock()
	if rdb != nil {
		return rdb.Close()
	}
	return nil
}

func (a *Adapter) extractPayload(ev event.Event) (id string, row map[string]interface{}, isDel bool, unchangedToast []string) {
	extracted := event.ExtractRow(ev, a.idColumn)
	return extracted.ID, extracted.Row, extracted.IsDelete, extracted.UnchangedToast
}

func (a *Adapter) recordDLQ(ctx context.Context, ev event.Event, err error) {
	if a.dlq != nil {
		if dlqErr := a.dlq.Record(ctx, ev, "redis", err); dlqErr != nil {
			a.logger.Error("dlq record failed", "error", dlqErr)
		}
	}
}

func isConnectionError(err error) bool {
	// go-redis returns specific errors for connection issues.
	return goredis.HasErrorPrefix(err, "LOADING") ||
		err.Error() == "redis: client is closed" ||
		err.Error() == "EOF"
}
