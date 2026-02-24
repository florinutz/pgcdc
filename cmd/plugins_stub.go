//go:build no_plugins

package cmd

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/florinutz/pgcdc"
	"github.com/florinutz/pgcdc/checkpoint"
	"github.com/florinutz/pgcdc/dlq"
	"github.com/florinutz/pgcdc/internal/config"
	"github.com/spf13/cobra"
)

func anyPluginsConfigured(_ config.Config, _ *cobra.Command) bool {
	return false
}

func initPluginRuntime(_ context.Context, _ *slog.Logger) (rt any, cleanup func()) {
	return nil, func() {}
}

func closePluginRuntime(_ context.Context, _ any) {}

func makePluginDLQ(_ context.Context, _ any, _ string, _ map[string]any, _ *slog.Logger) (dlq.DLQ, error) {
	return nil, fmt.Errorf("plugin DLQ not available (built with -tags no_plugins)")
}

func makePluginCheckpoint(_ context.Context, _ any, _ string, _ map[string]any, _ *slog.Logger) (checkpoint.Store, error) {
	return nil, fmt.Errorf("plugin checkpoint not available (built with -tags no_plugins)")
}

func wirePluginAdapters(_ context.Context, _ any, _ *cobra.Command, _ config.Config, _ *slog.Logger) ([]pgcdc.Option, func(), error) {
	return nil, func() {}, nil
}

func buildPluginTransformOpts(_ context.Context, _ any, _ *cobra.Command, _ config.Config, _ *slog.Logger) ([]pgcdc.Option, func()) {
	return nil, func() {}
}
