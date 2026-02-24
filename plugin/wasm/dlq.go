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
)

// dlqRecord is the JSON structure passed to the plugin's record function.
type dlqRecord struct {
	Event     event.Event `json:"event"`
	Adapter   string      `json:"adapter"`
	Error     string      `json:"error"`
	Timestamp time.Time   `json:"timestamp"`
}

// WasmDLQ wraps a Wasm plugin as a dlq.DLQ.
type WasmDLQ struct {
	name   string
	plugin *extism.Plugin
	logger *slog.Logger
}

// NewDLQ compiles, instantiates, and calls init on a Wasm DLQ plugin.
func NewDLQ(ctx context.Context, rt *Runtime, name string, spec *config.PluginSpec, logger *slog.Logger) (*WasmDLQ, error) {
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With("component", "wasm_dlq", "plugin", name)

	hostFns := HostFunctions(logger, name)
	compiled, err := rt.LoadModule(ctx, spec.Path, hostFns)
	if err != nil {
		return nil, fmt.Errorf("load dlq module %s: %w", name, err)
	}

	plugin, err := rt.NewInstance(ctx, compiled, spec.Config)
	if err != nil {
		return nil, fmt.Errorf("create dlq instance %s: %w", name, err)
	}

	if plugin.FunctionExists("init") {
		cfgBytes, _ := json.Marshal(spec.Config)
		_, result, callErr := plugin.Call("init", cfgBytes)
		if callErr != nil {
			_ = plugin.Close(ctx)
			return nil, fmt.Errorf("init dlq plugin %s: %w", name, callErr)
		}
		if len(result) > 0 {
			_ = plugin.Close(ctx)
			return nil, fmt.Errorf("init dlq plugin %s: %s", name, string(result))
		}
	}

	logger.Info("wasm dlq loaded", "path", spec.Path)
	return &WasmDLQ{
		name:   name,
		plugin: plugin,
		logger: logger,
	}, nil
}

func (d *WasmDLQ) Record(ctx context.Context, ev event.Event, adapterName string, err error) error {
	rec := dlqRecord{
		Event:     ev,
		Adapter:   adapterName,
		Error:     err.Error(),
		Timestamp: time.Now().UTC(),
	}

	data, marshalErr := json.Marshal(rec)
	if marshalErr != nil {
		metrics.DLQErrors.Inc()
		return fmt.Errorf("marshal dlq record: %w", marshalErr)
	}

	start := time.Now()
	_, result, callErr := d.plugin.Call("record", data)
	duration := time.Since(start)

	metrics.PluginCalls.WithLabelValues(d.name, "dlq").Inc()
	metrics.PluginDuration.WithLabelValues(d.name, "dlq").Observe(duration.Seconds())

	if callErr != nil {
		metrics.PluginErrors.WithLabelValues(d.name, "dlq").Inc()
		metrics.DLQErrors.Inc()
		return &pgcdcerr.PluginError{Plugin: d.name, Type: "dlq", Err: callErr}
	}
	if len(result) > 0 {
		metrics.PluginErrors.WithLabelValues(d.name, "dlq").Inc()
		metrics.DLQErrors.Inc()
		return &pgcdcerr.PluginError{Plugin: d.name, Type: "dlq", Err: fmt.Errorf("%s", string(result))}
	}

	metrics.DLQRecords.WithLabelValues(adapterName).Inc()
	return nil
}

func (d *WasmDLQ) Close() error {
	ctx := context.Background()
	if d.plugin.FunctionExists("close") {
		_, _, _ = d.plugin.Call("close", nil)
	}
	_ = d.plugin.Close(ctx)
	return nil
}
