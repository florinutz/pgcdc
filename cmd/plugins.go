//go:build !no_plugins

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/florinutz/pgcdc"
	"github.com/florinutz/pgcdc/checkpoint"
	"github.com/florinutz/pgcdc/dlq"
	"github.com/florinutz/pgcdc/internal/config"
	wasmplug "github.com/florinutz/pgcdc/plugin/wasm"
	"github.com/spf13/cobra"
)

func anyPluginsConfigured(cfg config.Config, cmd *cobra.Command) bool {
	if cfg.Plugins.DLQ != nil || cfg.Plugins.Checkpoint != nil {
		return true
	}
	if len(cfg.Plugins.Transforms) > 0 || len(cfg.Plugins.Adapters) > 0 {
		return true
	}
	if paths, _ := cmd.Flags().GetStringSlice("plugin-transform"); len(paths) > 0 {
		return true
	}
	if paths, _ := cmd.Flags().GetStringSlice("plugin-adapter"); len(paths) > 0 {
		return true
	}
	if p, _ := cmd.Flags().GetString("dlq-plugin-path"); p != "" {
		return true
	}
	if p, _ := cmd.Flags().GetString("checkpoint-plugin"); p != "" {
		return true
	}
	return false
}

func initPluginRuntime(_ context.Context, logger *slog.Logger) (rt any, cleanup func()) {
	r := wasmplug.NewRuntime(logger)
	return r, func() {}
}

func closePluginRuntime(ctx context.Context, rt any) {
	if r, ok := rt.(*wasmplug.Runtime); ok && r != nil {
		r.Close(ctx)
	}
}

func makePluginDLQ(ctx context.Context, rt any, path string, cfg map[string]any, logger *slog.Logger) (dlq.DLQ, error) {
	r, ok := rt.(*wasmplug.Runtime)
	if !ok || r == nil {
		return nil, fmt.Errorf("wasm runtime not initialized")
	}
	return wasmplug.NewDLQ(ctx, r, "dlq-plugin", &config.PluginSpec{Path: path, Config: cfg}, logger)
}

func makePluginCheckpoint(ctx context.Context, rt any, path string, cfg map[string]any, logger *slog.Logger) (checkpoint.Store, error) {
	r, ok := rt.(*wasmplug.Runtime)
	if !ok || r == nil {
		return nil, fmt.Errorf("wasm runtime not initialized")
	}
	return wasmplug.NewCheckpointStore(ctx, r, "checkpoint-plugin", &config.PluginSpec{Path: path, Config: cfg}, logger)
}

func wirePluginAdapters(ctx context.Context, rt any, cmd *cobra.Command, cfg config.Config, logger *slog.Logger) ([]pgcdc.Option, func(), error) {
	r, ok := rt.(*wasmplug.Runtime)
	if !ok || r == nil {
		return nil, func() {}, nil
	}

	pluginAdapterPaths, _ := cmd.Flags().GetStringSlice("plugin-adapter")
	pluginAdapterNames, _ := cmd.Flags().GetStringSlice("plugin-adapter-name")
	pluginAdapterConfigs, _ := cmd.Flags().GetStringSlice("plugin-adapter-config")

	var pluginAdapterSpecs []config.PluginAdapterSpec
	for i, p := range pluginAdapterPaths {
		name := fmt.Sprintf("wasm-adapter-%d", i)
		if i < len(pluginAdapterNames) && pluginAdapterNames[i] != "" {
			name = pluginAdapterNames[i]
		}
		pluginCfg := map[string]any{}
		if i < len(pluginAdapterConfigs) && pluginAdapterConfigs[i] != "" {
			if err := json.Unmarshal([]byte(pluginAdapterConfigs[i]), &pluginCfg); err != nil {
				return nil, func() {}, fmt.Errorf("parse --plugin-adapter-config[%d]: %w", i, err)
			}
		}
		pluginAdapterSpecs = append(pluginAdapterSpecs, config.PluginAdapterSpec{Name: name, Path: p, Config: pluginCfg})
	}
	pluginAdapterSpecs = append(pluginAdapterSpecs, cfg.Plugins.Adapters...)

	var opts []pgcdc.Option
	for _, spec := range pluginAdapterSpecs {
		wa, waErr := wasmplug.NewAdapter(ctx, r, spec, logger)
		if waErr != nil {
			return nil, func() {}, fmt.Errorf("create wasm adapter %s: %w", spec.Name, waErr)
		}
		opts = append(opts, pgcdc.WithAdapter(wa))
	}

	return opts, func() {}, nil
}

func buildPluginTransformOpts(ctx context.Context, rt any, cmd *cobra.Command, cfg config.Config, logger *slog.Logger) ([]pgcdc.Option, func()) {
	r, ok := rt.(*wasmplug.Runtime)
	if !ok || r == nil {
		return nil, func() {}
	}

	var opts []pgcdc.Option
	var wasmTransforms []*wasmplug.WasmTransform

	// CLI flag plugin transforms.
	pluginTransformPaths, _ := cmd.Flags().GetStringSlice("plugin-transform")
	pluginTransformConfigs, _ := cmd.Flags().GetStringSlice("plugin-transform-config")
	for i, p := range pluginTransformPaths {
		name := fmt.Sprintf("wasm-transform-%d", i)
		pluginCfg := map[string]any{}
		if i < len(pluginTransformConfigs) && pluginTransformConfigs[i] != "" {
			_ = json.Unmarshal([]byte(pluginTransformConfigs[i]), &pluginCfg)
		}
		spec := config.PluginTransformSpec{Name: name, Path: p, Config: pluginCfg, Scope: "global"}
		wt, wtErr := wasmplug.NewTransform(ctx, r, spec, logger)
		if wtErr != nil {
			logger.Error("create wasm transform", "error", wtErr, "plugin", name)
			continue
		}
		wasmTransforms = append(wasmTransforms, wt)
		opts = append(opts, pgcdc.WithTransform(wt.TransformFunc()))
	}

	// Config file: plugin transforms.
	for _, spec := range cfg.Plugins.Transforms {
		if spec.Path == "" {
			continue
		}
		wt, wtErr := wasmplug.NewTransform(ctx, r, spec, logger)
		if wtErr != nil {
			logger.Error("create wasm transform", "error", wtErr, "plugin", spec.Name)
			continue
		}
		wasmTransforms = append(wasmTransforms, wt)

		switch scope := spec.Scope.(type) {
		case string:
			if scope == "global" || scope == "" {
				opts = append(opts, pgcdc.WithTransform(wt.TransformFunc()))
			}
		case map[string]any:
			if adapterName, ok := scope["adapter"].(string); ok && adapterName != "" {
				opts = append(opts, pgcdc.WithAdapterTransform(adapterName, wt.TransformFunc()))
			} else {
				opts = append(opts, pgcdc.WithTransform(wt.TransformFunc()))
			}
		default:
			opts = append(opts, pgcdc.WithTransform(wt.TransformFunc()))
		}
	}

	cleanup := func() {
		for _, wt := range wasmTransforms {
			wt.Close(ctx)
		}
	}

	return opts, cleanup
}
