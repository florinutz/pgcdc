package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/florinutz/pgcdc/dlq"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/reconnect"
	"github.com/florinutz/pgcdc/metrics"
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
}

// SetDLQ sets the dead letter queue for failed deliveries.
func (a *Adapter) SetDLQ(d dlq.DLQ) { a.dlq = d }

// New creates a Redis cache invalidation/sync adapter.
func New(url, mode, keyPrefix, idColumn string, backoffBase, backoffCap time.Duration, logger *slog.Logger) *Adapter {
	if mode == "" {
		mode = "invalidate"
	}
	if idColumn == "" {
		idColumn = defaultIDColumn
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
		url:         url,
		mode:        mode,
		keyPrefix:   keyPrefix,
		idColumn:    idColumn,
		backoffBase: backoffBase,
		backoffCap:  backoffCap,
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
						return fmt.Errorf("redis del (connection lost): %w", err)
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
						return fmt.Errorf("redis del (connection lost): %w", err)
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
						return fmt.Errorf("redis set (connection lost): %w", err)
					}
					a.logger.Warn("redis set failed, skipping", "key", key, "error", err)
					a.recordDLQ(ctx, ev, err)
					continue
				}
				metrics.RedisOperations.WithLabelValues("set").Inc()
			}

			metrics.EventsDelivered.WithLabelValues("redis").Inc()
		}
	}
}

type payload struct {
	Op             string                 `json:"op"`
	Table          string                 `json:"table"`
	Row            map[string]interface{} `json:"row"`
	UnchangedToast []string               `json:"_unchanged_toast_columns"`
}

func (a *Adapter) extractPayload(ev event.Event) (id string, row map[string]interface{}, isDel bool, unchangedToast []string) {
	// Structured record path: zero JSON parsing.
	if rec := ev.Record(); rec != nil && rec.Operation != 0 &&
		(rec.Change.After != nil || rec.Change.Before != nil) {
		isDel = rec.Operation == event.OperationDelete

		if isDel {
			if rec.Change.Before != nil {
				row = rec.Change.Before.ToMap()
			}
		} else {
			if rec.Change.After != nil {
				row = rec.Change.After.ToMap()
			}
		}

		if row == nil {
			return "", nil, isDel, nil
		}
		if v, ok := row[a.idColumn]; ok && v != nil {
			if s, ok := v.(string); ok {
				id = s
			} else {
				id = fmt.Sprintf("%v", v)
			}
		}

		// Unchanged TOAST columns from metadata.
		if toastCSV, ok := rec.Metadata[event.MetaUnchangedToastCols]; ok && toastCSV != "" {
			unchangedToast = strings.Split(toastCSV, ",")
		}

		return id, row, isDel, unchangedToast
	}

	// Legacy path: parse payload JSON.
	var p payload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		return "", nil, false, nil
	}
	isDel = p.Op == "DELETE"
	if p.Row == nil {
		return "", nil, isDel, nil
	}
	if v, ok := p.Row[a.idColumn]; ok && v != nil {
		if s, ok := v.(string); ok {
			id = s
		} else {
			id = fmt.Sprintf("%v", v)
		}
	}
	return id, p.Row, isDel, p.UnchangedToast
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
