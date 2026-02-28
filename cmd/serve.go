package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/florinutz/pgcdc"
	"github.com/florinutz/pgcdc/bus"
	"github.com/florinutz/pgcdc/health"
	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/registry"
	"github.com/florinutz/pgcdc/server"
	"github.com/florinutz/pgcdc/transform"
	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"
)

// ServeConfig is the top-level config for multi-pipeline mode.
type ServeConfig struct {
	Pipelines []server.PipelineConfig `yaml:"pipelines"`
	Addr      string                  `yaml:"addr"`
}

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Run multiple pipelines from a YAML config file",
	Long: `Starts a multi-pipeline server that manages N independent pipelines
defined in a YAML configuration file. Each pipeline has its own detector,
adapters, and transforms. Shared HTTP server provides metrics, health checks,
and a REST API for runtime pipeline management.

Example config (pipelines.yaml):

  pipelines:
    - name: orders-to-kafka
      detector: {type: wal}
      adapters: [{name: kafka, type: kafka}]
    - name: users-to-search
      detector: {type: wal}
      adapters: [{name: search, type: search}]
`,
	RunE: runServe,
}

func init() {
	serveCmd.Flags().String("config", "pipelines.yaml", "pipeline config file")
	serveCmd.Flags().String("addr", ":8080", "HTTP server address for API + metrics + health")
	rootCmd.AddCommand(serveCmd)
}

func runServe(cmd *cobra.Command, args []string) error {
	configFile, _ := cmd.Flags().GetString("config")
	addr, _ := cmd.Flags().GetString("addr")

	logger := slog.Default()

	// Read config file.
	data, err := os.ReadFile(configFile)
	if err != nil {
		return fmt.Errorf("read config %q: %w", configFile, err)
	}

	var cfg ServeConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("parse config %q: %w", configFile, err)
	}

	if len(cfg.Pipelines) == 0 {
		return fmt.Errorf("no pipelines defined in %q", configFile)
	}

	if cfg.Addr != "" && addr == ":8080" {
		addr = cfg.Addr
	}

	mgr := server.NewManager(registryPipelineBuilder, logger)

	// Register all pipelines.
	for _, pcfg := range cfg.Pipelines {
		if err := mgr.Add(pcfg); err != nil {
			return fmt.Errorf("add pipeline %q: %w", pcfg.Name, err)
		}
	}

	// Set up signal handling.
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	g, gCtx := errgroup.WithContext(ctx)

	// Start all pipelines.
	for _, pcfg := range cfg.Pipelines {
		name := pcfg.Name
		g.Go(func() error {
			if err := mgr.Start(gCtx, name); err != nil {
				logger.Error("failed to start pipeline", "pipeline", name, "error", err)
				return err
			}
			return nil
		})
	}

	// Start HTTP server with API + metrics + health.
	r := chi.NewRouter()
	r.Use(chimiddleware.Recoverer)
	r.Handle("/metrics", promhttp.Handler())
	r.Get("/healthz", mgr.Health().ServeHTTP)
	r.Get("/readyz", health.NewReadinessChecker().ServeHTTP)
	r.Mount("/", server.APIHandler(mgr))

	httpServer := &http.Server{
		Addr:    addr,
		Handler: r,
	}

	g.Go(func() error {
		logger.Info("HTTP server started", "addr", addr, "pipelines", len(cfg.Pipelines))
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	})

	// Wait for shutdown signal.
	g.Go(func() error {
		<-gCtx.Done()
		logger.Info("shutting down...")
		mgr.StopAll()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		return httpServer.Shutdown(shutdownCtx)
	})

	return g.Wait()
}

// registryPipelineBuilder constructs a Pipeline from a PipelineConfig using
// the component registry. Detector, adapters, and transforms are resolved via
// registry.CreateDetector, registry.CreateAdapter, and registry.SpecToTransform.
func registryPipelineBuilder(pcfg server.PipelineConfig, logger *slog.Logger) (*pgcdc.Pipeline, error) {
	// Build a config.Config populated from the PipelineConfig's map[string]any fields.
	cfg := config.Default()
	cfg.PipelineName = pcfg.Name

	// Marshal the detector config map into the correct config section via JSON round-trip.
	if pcfg.Detector.Config != nil {
		if err := mapToStruct(pcfg.Detector.Config, &cfg); err != nil {
			return nil, fmt.Errorf("detector config: %w", err)
		}
	}
	cfg.Detector.Type = pcfg.Detector.Type

	// Bus configuration from the pipeline spec.
	if pcfg.Bus.BufferSize > 0 {
		cfg.Bus.BufferSize = pcfg.Bus.BufferSize
	}
	if pcfg.Bus.Mode != "" {
		cfg.Bus.Mode = pcfg.Bus.Mode
	}

	// Routes.
	if len(pcfg.Routes) > 0 {
		cfg.Routes = pcfg.Routes
	}

	// Create detector via registry.
	detCtx := registry.DetectorContext{
		Cfg:    cfg,
		Logger: logger.With("pipeline", pcfg.Name),
	}
	detResult, err := registry.CreateDetector(pcfg.Detector.Type, detCtx)
	if err != nil {
		return nil, fmt.Errorf("create detector %q: %w", pcfg.Detector.Type, err)
	}
	if detResult.Cleanup != nil {
		defer func() {
			// Only clean up on error — on success the pipeline owns the lifecycle.
			if err != nil {
				detResult.Cleanup()
			}
		}()
	}

	var opts []pgcdc.Option
	opts = append(opts, pgcdc.WithLogger(logger.With("pipeline", pcfg.Name)))
	opts = append(opts, pgcdc.WithPipelineName(pcfg.Name))

	if detResult.CheckpointStore != nil {
		opts = append(opts, pgcdc.WithCheckpointStore(detResult.CheckpointStore))
	}

	// Set bus mode.
	if cfg.Bus.Mode == "reliable" {
		opts = append(opts, pgcdc.WithBusMode(bus.BusModeReliable))
	}
	opts = append(opts, pgcdc.WithBusBuffer(cfg.Bus.BufferSize))

	// Create adapters via registry.
	// Each adapter spec's Config map is merged into cfg for the adapter factory.
	adapterNames := make([]string, 0, len(pcfg.Adapters))
	for _, aSpec := range pcfg.Adapters {
		adapterCfg := cfg // copy for per-adapter config overlay
		if aSpec.Config != nil {
			if err := mapToStruct(aSpec.Config, &adapterCfg); err != nil {
				return nil, fmt.Errorf("adapter %q config: %w", aSpec.Name, err)
			}
		}
		adapterCfg.Adapters = []string{aSpec.Type}

		adapterCtx := registry.AdapterContext{
			Cfg:    adapterCfg,
			Logger: logger.With("pipeline", pcfg.Name, "adapter", aSpec.Name),
		}

		result, aErr := registry.CreateAdapter(aSpec.Type, adapterCtx)
		if aErr != nil {
			return nil, fmt.Errorf("create adapter %q (type %q): %w", aSpec.Name, aSpec.Type, aErr)
		}
		opts = append(opts, pgcdc.WithAdapter(result.Adapter))
		if result.MiddlewareConfig != nil {
			opts = append(opts, pgcdc.WithMiddlewareConfig(aSpec.Name, *result.MiddlewareConfig))
		}
		adapterNames = append(adapterNames, aSpec.Name)
	}

	// Build transforms from the pipeline spec.
	if len(pcfg.Transforms) > 0 {
		var globalTfx []transform.TransformFunc
		for _, tSpec := range pcfg.Transforms {
			cfgSpec := config.TransformSpec{Type: tSpec.Type}
			if tSpec.Config != nil {
				if err := mapToStruct(tSpec.Config, &cfgSpec); err != nil {
					return nil, fmt.Errorf("transform %q config: %w", tSpec.Type, err)
				}
			}
			fn := registry.SpecToTransform(cfgSpec)
			if fn != nil {
				globalTfx = append(globalTfx, fn)
			}
		}
		for _, fn := range globalTfx {
			opts = append(opts, pgcdc.WithTransform(fn))
		}
	}

	// Routes from the pipeline config.
	if len(pcfg.Routes) > 0 {
		for adapter, channels := range pcfg.Routes {
			opts = append(opts, pgcdc.WithRoute(adapter, channels...))
		}
	}

	pipeline := pgcdc.NewPipeline(detResult.Detector, opts...)
	return pipeline, nil
}

// mapToStruct marshals a map[string]any to JSON, then unmarshals into the target struct.
// This is used to convert PipelineConfig's flexible config maps into typed config structs.
func mapToStruct(m map[string]any, target any) error {
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal config map: %w", err)
	}
	return json.Unmarshal(data, target)
}
