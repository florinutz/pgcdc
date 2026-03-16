// Package presets provides convenience constructors for common pgcdc pipeline
// configurations. Each preset wires a detector and adapter with sensible
// defaults, returning a ready-to-run [pgcdc.Pipeline].
package presets

import (
	"log/slog"
	"os"

	pgcdc "github.com/florinutz/pgcdc"
	"github.com/florinutz/pgcdc/adapter/stdout"
	"github.com/florinutz/pgcdc/adapter/webhook"
	"github.com/florinutz/pgcdc/detector/listennotify"
	"github.com/florinutz/pgcdc/detector/walreplication"
	"github.com/florinutz/pgcdc/transform"
)

// presetConfig holds common options shared across all presets.
type presetConfig struct {
	slotName   string
	transforms []transform.TransformFunc
	logger     *slog.Logger
	busBuffer  int
}

// Option configures a preset pipeline.
type Option func(*presetConfig)

// WithSlotName sets a persistent replication slot name for WAL-based presets.
// When set, the slot survives disconnects and allows resumption. Has no effect
// on LISTEN/NOTIFY presets.
func WithSlotName(name string) Option {
	return func(c *presetConfig) {
		c.slotName = name
	}
}

// WithTransforms appends global transforms to the pipeline.
func WithTransforms(fns ...transform.TransformFunc) Option {
	return func(c *presetConfig) {
		c.transforms = append(c.transforms, fns...)
	}
}

// WithLogger sets the logger for the pipeline. If not set, a text handler
// writing to stderr is used.
func WithLogger(l *slog.Logger) Option {
	return func(c *presetConfig) {
		c.logger = l
	}
}

// WithBusBuffer sets the bus and subscriber channel buffer size. If not set or
// zero, the bus default (1024) is used.
func WithBusBuffer(size int) Option {
	return func(c *presetConfig) {
		c.busBuffer = size
	}
}

func applyOpts(opts []Option) presetConfig {
	var cfg presetConfig
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.logger == nil {
		cfg.logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	return cfg
}

func pipelineOpts(cfg presetConfig, extra ...pgcdc.Option) []pgcdc.Option {
	out := make([]pgcdc.Option, 0, len(extra)+3)
	out = append(out, pgcdc.WithLogger(cfg.logger))
	if cfg.busBuffer > 0 {
		out = append(out, pgcdc.WithBusBuffer(cfg.busBuffer))
	}
	for _, fn := range cfg.transforms {
		out = append(out, pgcdc.WithTransform(fn))
	}
	out = append(out, extra...)
	return out
}

// WALToStdout creates a pipeline that streams WAL logical replication changes
// to stdout as JSON-lines. The connStr must be a valid PostgreSQL connection
// string and publication must name an existing publication.
func WALToStdout(connStr, publication string, opts ...Option) *pgcdc.Pipeline {
	cfg := applyOpts(opts)
	det := walreplication.New(connStr, publication, 0, 0, false, false, cfg.logger)
	if cfg.slotName != "" {
		det.SetPersistentSlot(cfg.slotName)
	}
	a := stdout.New(nil, cfg.logger)
	return pgcdc.NewPipeline(det, pipelineOpts(cfg, pgcdc.WithAdapter(a))...)
}

// WALToWebhook creates a pipeline that streams WAL logical replication changes
// to the given webhook URL via HTTP POST. The connStr must be a valid
// PostgreSQL connection string and publication must name an existing
// publication.
func WALToWebhook(connStr, publication, webhookURL string, opts ...Option) *pgcdc.Pipeline {
	cfg := applyOpts(opts)
	det := walreplication.New(connStr, publication, 0, 0, false, false, cfg.logger)
	if cfg.slotName != "" {
		det.SetPersistentSlot(cfg.slotName)
	}
	a := webhook.New(webhook.Config{URL: webhookURL}, cfg.logger)
	return pgcdc.NewPipeline(det, pipelineOpts(cfg, pgcdc.WithAdapter(a))...)
}

// ListenNotifyToStdout creates a pipeline that streams PostgreSQL
// LISTEN/NOTIFY notifications to stdout as JSON-lines.
func ListenNotifyToStdout(connStr string, channels []string, opts ...Option) *pgcdc.Pipeline {
	cfg := applyOpts(opts)
	det := listennotify.New(connStr, channels, 0, 0, cfg.logger)
	a := stdout.New(nil, cfg.logger)
	return pgcdc.NewPipeline(det, pipelineOpts(cfg, pgcdc.WithAdapter(a))...)
}

// ListenNotifyToWebhook creates a pipeline that streams PostgreSQL
// LISTEN/NOTIFY notifications to the given webhook URL via HTTP POST.
func ListenNotifyToWebhook(connStr string, channels []string, webhookURL string, opts ...Option) *pgcdc.Pipeline {
	cfg := applyOpts(opts)
	det := listennotify.New(connStr, channels, 0, 0, cfg.logger)
	a := webhook.New(webhook.Config{URL: webhookURL}, cfg.logger)
	return pgcdc.NewPipeline(det, pipelineOpts(cfg, pgcdc.WithAdapter(a))...)
}
