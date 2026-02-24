package wasm

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	extism "github.com/extism/go-sdk"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/pgcdcerr"
	"github.com/florinutz/pgcdc/transform"
)

// WasmTransform wraps a Wasm plugin as a transform.TransformFunc.
type WasmTransform struct {
	name     string
	plugin   *extism.Plugin
	encoding string // "json" or "protobuf"
	logger   *slog.Logger
}

// NewTransform compiles and instantiates a Wasm transform plugin.
func NewTransform(ctx context.Context, rt *Runtime, spec config.PluginTransformSpec, logger *slog.Logger) (*WasmTransform, error) {
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With("component", "wasm_transform", "plugin", spec.Name)

	hostFns := HostFunctions(logger, spec.Name)
	compiled, err := rt.LoadModule(ctx, spec.Path, hostFns)
	if err != nil {
		return nil, fmt.Errorf("load transform module %s: %w", spec.Name, err)
	}

	plugin, err := rt.NewInstance(ctx, compiled, spec.Config)
	if err != nil {
		return nil, fmt.Errorf("create transform instance %s: %w", spec.Name, err)
	}

	encoding := spec.Encoding
	if encoding == "" {
		encoding = "json"
	}

	logger.Info("wasm transform loaded", "path", spec.Path, "encoding", encoding)
	return &WasmTransform{
		name:     spec.Name,
		plugin:   plugin,
		encoding: encoding,
		logger:   logger,
	}, nil
}

// TransformFunc returns a transform.TransformFunc closure backed by the Wasm plugin.
func (wt *WasmTransform) TransformFunc() transform.TransformFunc {
	return func(ev event.Event) (event.Event, error) {
		// Preserve LSN across the Wasm boundary (json:"-" tag means it won't serialize).
		savedLSN := ev.LSN

		data, err := json.Marshal(ev)
		if err != nil {
			metrics.PluginErrors.WithLabelValues(wt.name, "transform").Inc()
			return ev, &pgcdcerr.PluginError{Plugin: wt.name, Type: "transform", Err: fmt.Errorf("marshal: %w", err)}
		}

		start := time.Now()
		_, result, callErr := wt.plugin.Call("transform", data)
		duration := time.Since(start)

		metrics.PluginCalls.WithLabelValues(wt.name, "transform").Inc()
		metrics.PluginDuration.WithLabelValues(wt.name, "transform").Observe(duration.Seconds())

		if callErr != nil {
			metrics.PluginErrors.WithLabelValues(wt.name, "transform").Inc()
			wt.logger.Warn("transform plugin error, skipping event",
				slog.String("event_id", ev.ID),
				slog.String("error", callErr.Error()),
			)
			return ev, &pgcdcerr.PluginError{Plugin: wt.name, Type: "transform", Err: callErr}
		}

		// Empty result = drop the event.
		if len(result) == 0 {
			return ev, transform.ErrDropEvent
		}

		var transformed event.Event
		if err := json.Unmarshal(result, &transformed); err != nil {
			metrics.PluginErrors.WithLabelValues(wt.name, "transform").Inc()
			return ev, &pgcdcerr.PluginError{Plugin: wt.name, Type: "transform", Err: fmt.Errorf("unmarshal result: %w", err)}
		}

		// Re-inject LSN that was lost during serialization.
		transformed.LSN = savedLSN
		return transformed, nil
	}
}

// Close releases the plugin instance.
func (wt *WasmTransform) Close(ctx context.Context) {
	_ = wt.plugin.Close(ctx)
}
