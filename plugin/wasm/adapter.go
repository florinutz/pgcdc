package wasm

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	extism "github.com/extism/go-sdk"
	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/dlq"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/pgcdcerr"
)

// WasmAdapter wraps a Wasm plugin as an adapter.Adapter.
// It also implements adapter.Acknowledger and pgcdc.DLQAware.
type WasmAdapter struct {
	name   string
	plugin *extism.Plugin
	logger *slog.Logger
	dlq    dlq.DLQ
	ackFn  adapter.AckFunc
}

// NewAdapter compiles, instantiates, and calls init on a Wasm adapter plugin.
func NewAdapter(ctx context.Context, rt *Runtime, spec config.PluginAdapterSpec, logger *slog.Logger) (*WasmAdapter, error) {
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With("component", "wasm_adapter", "plugin", spec.Name)

	hostFns := HostFunctions(logger, spec.Name)
	compiled, err := rt.LoadModule(ctx, spec.Path, hostFns)
	if err != nil {
		return nil, fmt.Errorf("load adapter module %s: %w", spec.Name, err)
	}

	plugin, err := rt.NewInstance(ctx, compiled, spec.Config)
	if err != nil {
		return nil, fmt.Errorf("create adapter instance %s: %w", spec.Name, err)
	}

	// Call init if exported.
	if plugin.FunctionExists("init") {
		cfgBytes, _ := json.Marshal(spec.Config)
		_, result, callErr := plugin.Call("init", cfgBytes)
		if callErr != nil {
			_ = plugin.Close(ctx)
			return nil, fmt.Errorf("init adapter plugin %s: %w", spec.Name, callErr)
		}
		if len(result) > 0 {
			_ = plugin.Close(ctx)
			return nil, fmt.Errorf("init adapter plugin %s: %s", spec.Name, string(result))
		}
	}

	logger.Info("wasm adapter loaded", "path", spec.Path)
	return &WasmAdapter{
		name:   spec.Name,
		plugin: plugin,
		logger: logger,
	}, nil
}

func (a *WasmAdapter) Name() string { return a.name }

func (a *WasmAdapter) SetDLQ(d dlq.DLQ) { a.dlq = d }

func (a *WasmAdapter) SetAckFunc(fn adapter.AckFunc) { a.ackFn = fn }

func (a *WasmAdapter) Start(ctx context.Context, events <-chan event.Event) error {
	defer a.close(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-events:
			if !ok {
				return nil
			}

			data, err := json.Marshal(ev)
			if err != nil {
				a.logger.Error("marshal event", "error", err, "event_id", ev.ID)
				a.ack(ev)
				continue
			}

			start := time.Now()
			_, result, callErr := a.plugin.Call("handle", data)
			duration := time.Since(start)

			metrics.PluginCalls.WithLabelValues(a.name, "adapter").Inc()
			metrics.PluginDuration.WithLabelValues(a.name, "adapter").Observe(duration.Seconds())

			if callErr != nil || len(result) > 0 {
				metrics.PluginErrors.WithLabelValues(a.name, "adapter").Inc()
				var errMsg string
				if callErr != nil {
					errMsg = callErr.Error()
				} else {
					errMsg = string(result)
				}

				a.logger.Warn("adapter plugin handle error",
					slog.String("event_id", ev.ID),
					slog.String("error", errMsg),
				)

				if a.dlq != nil {
					_ = a.dlq.Record(ctx, ev, a.name, &pgcdcerr.PluginError{
						Plugin: a.name,
						Type:   "adapter",
						Err:    fmt.Errorf("%s", errMsg),
					})
				}
				a.ack(ev)
				continue
			}

			metrics.EventsDelivered.WithLabelValues(a.name).Inc()
			a.ack(ev)
		}
	}
}

func (a *WasmAdapter) ack(ev event.Event) {
	if a.ackFn != nil && ev.LSN > 0 {
		a.ackFn(ev.LSN)
	}
}

func (a *WasmAdapter) close(ctx context.Context) {
	if a.plugin.FunctionExists("close") {
		_, _, _ = a.plugin.Call("close", nil)
	}
	_ = a.plugin.Close(ctx)
}
