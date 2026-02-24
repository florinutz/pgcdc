package wasm

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	extism "github.com/extism/go-sdk"
)

// Runtime manages pre-compiled Wasm modules and creates plugin instances.
type Runtime struct {
	mu      sync.Mutex
	modules map[string]*extism.CompiledPlugin
	logger  *slog.Logger
}

// NewRuntime creates a Wasm runtime for managing plugin modules.
func NewRuntime(logger *slog.Logger) *Runtime {
	if logger == nil {
		logger = slog.Default()
	}
	return &Runtime{
		modules: make(map[string]*extism.CompiledPlugin),
		logger:  logger.With("component", "wasm_runtime"),
	}
}

// LoadModule compiles a .wasm file once and caches it. Thread-safe.
func (r *Runtime) LoadModule(ctx context.Context, path string, hostFns []extism.HostFunction) (*extism.CompiledPlugin, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if compiled, ok := r.modules[path]; ok {
		return compiled, nil
	}

	manifest := extism.Manifest{
		Wasm: []extism.Wasm{
			extism.WasmFile{Path: path},
		},
	}

	compiled, err := extism.NewCompiledPlugin(ctx, manifest, extism.PluginConfig{
		EnableWasi: true,
	}, hostFns)
	if err != nil {
		return nil, fmt.Errorf("compile wasm module %s: %w", path, err)
	}

	r.modules[path] = compiled
	r.logger.Info("wasm module compiled", "path", path)
	return compiled, nil
}

// NewInstance creates a new plugin instance from a compiled module with the
// given configuration. The config map is serialized to JSON and passed as
// string config entries.
func (r *Runtime) NewInstance(ctx context.Context, compiled *extism.CompiledPlugin, cfg map[string]any) (*extism.Plugin, error) {
	stringCfg := make(map[string]string, len(cfg))
	for k, v := range cfg {
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("marshal config key %q: %w", k, err)
		}
		stringCfg[k] = string(b)
	}

	instanceCfg := extism.PluginInstanceConfig{}
	plugin, err := compiled.Instance(ctx, instanceCfg)
	if err != nil {
		return nil, fmt.Errorf("create wasm instance: %w", err)
	}

	plugin.Config = stringCfg
	return plugin, nil
}

// Close releases all compiled modules.
func (r *Runtime) Close(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for path, compiled := range r.modules {
		_ = compiled.Close(ctx)
		delete(r.modules, path)
	}
}
