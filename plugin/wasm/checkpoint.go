package wasm

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	extism "github.com/extism/go-sdk"
	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/pgcdcerr"
)

// checkpointLoadResponse is the JSON response from the plugin's load function.
type checkpointLoadResponse struct {
	LSN uint64 `json:"lsn"`
}

// checkpointSaveRequest is the JSON input to the plugin's save function.
type checkpointSaveRequest struct {
	SlotName string `json:"slot_name"`
	LSN      uint64 `json:"lsn"`
}

// WasmCheckpointStore wraps a Wasm plugin as a checkpoint.Store.
type WasmCheckpointStore struct {
	name   string
	plugin *extism.Plugin
	logger *slog.Logger
}

// NewCheckpointStore compiles, instantiates, and calls init on a Wasm checkpoint plugin.
func NewCheckpointStore(ctx context.Context, rt *Runtime, name string, spec *config.PluginSpec, logger *slog.Logger) (*WasmCheckpointStore, error) {
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With("component", "wasm_checkpoint", "plugin", name)

	hostFns := HostFunctions(logger, name)
	compiled, err := rt.LoadModule(ctx, spec.Path, hostFns)
	if err != nil {
		return nil, fmt.Errorf("load checkpoint module %s: %w", name, err)
	}

	plugin, err := rt.NewInstance(ctx, compiled, spec.Config)
	if err != nil {
		return nil, fmt.Errorf("create checkpoint instance %s: %w", name, err)
	}

	if plugin.FunctionExists("init") {
		cfgBytes, _ := json.Marshal(spec.Config)
		_, result, callErr := plugin.Call("init", cfgBytes)
		if callErr != nil {
			_ = plugin.Close(ctx)
			return nil, fmt.Errorf("init checkpoint plugin %s: %w", name, callErr)
		}
		if len(result) > 0 {
			_ = plugin.Close(ctx)
			return nil, fmt.Errorf("init checkpoint plugin %s: %s", name, string(result))
		}
	}

	logger.Info("wasm checkpoint store loaded", "path", spec.Path)
	return &WasmCheckpointStore{
		name:   name,
		plugin: plugin,
		logger: logger,
	}, nil
}

func (s *WasmCheckpointStore) Load(ctx context.Context, slotName string) (uint64, error) {
	start := time.Now()
	_, result, err := s.plugin.Call("load", []byte(slotName))
	duration := time.Since(start)

	metrics.PluginCalls.WithLabelValues(s.name, "checkpoint").Inc()
	metrics.PluginDuration.WithLabelValues(s.name, "checkpoint").Observe(duration.Seconds())

	if err != nil {
		metrics.PluginErrors.WithLabelValues(s.name, "checkpoint").Inc()
		return 0, &pgcdcerr.PluginError{Plugin: s.name, Type: "checkpoint", Err: err}
	}

	if len(result) == 0 {
		return 0, nil
	}

	var resp checkpointLoadResponse
	if err := json.Unmarshal(result, &resp); err != nil {
		return 0, &pgcdcerr.PluginError{Plugin: s.name, Type: "checkpoint", Err: fmt.Errorf("unmarshal load response: %w", err)}
	}

	return resp.LSN, nil
}

func (s *WasmCheckpointStore) Save(ctx context.Context, slotName string, lsn uint64) error {
	input, _ := json.Marshal(checkpointSaveRequest{SlotName: slotName, LSN: lsn})

	start := time.Now()
	_, result, err := s.plugin.Call("save", input)
	duration := time.Since(start)

	metrics.PluginCalls.WithLabelValues(s.name, "checkpoint").Inc()
	metrics.PluginDuration.WithLabelValues(s.name, "checkpoint").Observe(duration.Seconds())

	if err != nil {
		metrics.PluginErrors.WithLabelValues(s.name, "checkpoint").Inc()
		return &pgcdcerr.PluginError{Plugin: s.name, Type: "checkpoint", Err: err}
	}
	if len(result) > 0 {
		metrics.PluginErrors.WithLabelValues(s.name, "checkpoint").Inc()
		return &pgcdcerr.PluginError{Plugin: s.name, Type: "checkpoint", Err: fmt.Errorf("%s", string(result))}
	}

	return nil
}

func (s *WasmCheckpointStore) Close() error {
	ctx := context.Background()
	if s.plugin.FunctionExists("close") {
		_, _, _ = s.plugin.Call("close", nil)
	}
	_ = s.plugin.Close(ctx)
	return nil
}
