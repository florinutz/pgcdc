package cmd

import (
	"context"
	_ "embed"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/florinutz/pgcdc"
	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/adapter/stdout"
	"github.com/florinutz/pgcdc/detector/listennotify"
	"github.com/spf13/cobra"
)

//go:embed playground_schema.sql
var playgroundSchema string

const (
	playgroundContainerName = "pgcdc-playground"
	playgroundConnStr       = "postgres://postgres:pgcdc@localhost:5433/pgcdc?sslmode=disable"
	playgroundReadyTimeout  = 30 * time.Second
	playgroundReadyInterval = 500 * time.Millisecond
)

var playgroundCmd = &cobra.Command{
	Use:   "playground",
	Short: "Start a demo environment with PostgreSQL and pgcdc",
	Long: `Starts a PostgreSQL container via Docker, creates a sample schema,
and runs pgcdc streaming events from an orders table. Inserts demo data
at a configurable interval. Ctrl+C stops everything and cleans up.

Requires Docker to be installed and running.`,
	RunE: runPlayground,
}

func init() {
	rootCmd.AddCommand(playgroundCmd)
	f := playgroundCmd.Flags()
	f.String("playground-adapter", "stdout", "adapter to use: stdout")
	f.String("playground-detector", "listen_notify", "detector to use: listen_notify")
	f.Duration("interval", 2*time.Second, "interval between demo inserts")
}

func runPlayground(cmd *cobra.Command, args []string) error {
	adapterName, _ := cmd.Flags().GetString("playground-adapter")
	detectorName, _ := cmd.Flags().GetString("playground-detector")
	interval, _ := cmd.Flags().GetDuration("interval")

	logger := slog.Default().With("component", "playground")

	// Check Docker is available.
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	if err := checkDocker(ctx); err != nil {
		return fmt.Errorf("docker is required but not available: %w\nInstall Docker: https://docs.docker.com/get-docker/", err)
	}

	// Clean up any leftover container from a previous run.
	cleanupContainer(ctx, logger)

	// Start PostgreSQL container.
	fmt.Fprintln(os.Stderr, "Starting PostgreSQL container...")
	if err := startContainer(ctx); err != nil {
		return fmt.Errorf("start postgres container: %w", err)
	}
	defer func() {
		fmt.Fprintln(os.Stderr, "Cleaning up container...")
		cleanupContainer(context.Background(), logger)
	}()

	// Wait for PostgreSQL to be ready.
	fmt.Fprintln(os.Stderr, "Waiting for database...")
	conn, err := waitForPostgres(ctx, logger)
	if err != nil {
		return fmt.Errorf("database not ready: %w", err)
	}
	_ = conn.Close(ctx)

	// Create schema.
	fmt.Fprintln(os.Stderr, "Creating schema...")
	if err := createSchema(ctx); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}

	// Warn about WAL mode limitations.
	if detectorName == "wal" {
		fmt.Fprintln(os.Stderr, "WARNING: WAL mode requires wal_level=logical. The default postgres:16-alpine image uses wal_level=replica.")
		fmt.Fprintln(os.Stderr, "The playground may not work correctly with --playground-detector wal unless you configure the container manually.")
	}

	// Build detector.
	det, err := buildPlaygroundDetector(detectorName, logger)
	if err != nil {
		return err
	}

	// Build adapter.
	adpt, err := buildPlaygroundAdapter(adapterName, logger)
	if err != nil {
		return err
	}

	// Build and start pipeline.
	fmt.Fprintln(os.Stderr, "Starting pipeline...")
	pipeline := pgcdc.NewPipeline(det,
		pgcdc.WithAdapter(adpt),
		pgcdc.WithLogger(logger),
	)

	// Signal handling.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	startTime := time.Now()
	eventCount := 0

	// Run pipeline in background.
	pipelineErr := make(chan error, 1)
	go func() {
		pipelineErr <- pipeline.Run(ctx)
	}()

	// Insert demo rows.
	fmt.Fprintf(os.Stderr, "Inserting demo data every %s... (Ctrl+C to stop)\n", interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case sig := <-sigCh:
			fmt.Fprintf(os.Stderr, "\nReceived %s, shutting down...\n", sig)
			cancel()
			// Wait for pipeline to finish.
			<-pipelineErr
			fmt.Fprintf(os.Stderr, "Summary: %d events in %s\n", eventCount, time.Since(startTime).Truncate(time.Second))
			return nil

		case err := <-pipelineErr:
			if err != nil {
				return fmt.Errorf("pipeline error: %w", err)
			}
			return nil

		case <-ticker.C:
			if err := insertDemoRow(ctx); err != nil {
				logger.Warn("insert demo row failed", "error", err)
				continue
			}
			eventCount++
		}
	}
}

func checkDocker(ctx context.Context) error {
	out, err := exec.CommandContext(ctx, "docker", "info").CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker info: %w (output: %s)", err, string(out))
	}
	return nil
}

func startContainer(ctx context.Context) error {
	out, err := exec.CommandContext(ctx, "docker", "run", "-d",
		"--name", playgroundContainerName,
		"-e", "POSTGRES_PASSWORD=pgcdc",
		"-e", "POSTGRES_DB=pgcdc",
		"-p", "5433:5432",
		"postgres:16-alpine",
	).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w (output: %s)", err, string(out))
	}
	return nil
}

func cleanupContainer(ctx context.Context, logger *slog.Logger) {
	out, err := exec.CommandContext(ctx, "docker", "rm", "-f", playgroundContainerName).CombinedOutput()
	if err != nil {
		logger.Debug("container cleanup", "error", err, "output", string(out))
	}
}

func waitForPostgres(ctx context.Context, logger *slog.Logger) (*pgx.Conn, error) {
	deadline := time.Now().Add(playgroundReadyTimeout)
	for time.Now().Before(deadline) {
		connCtx, connCancel := context.WithTimeout(ctx, 2*time.Second)
		conn, err := pgx.Connect(connCtx, playgroundConnStr)
		connCancel()
		if err == nil {
			return conn, nil
		}
		logger.Debug("waiting for postgres", "error", err)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(playgroundReadyInterval):
		}
	}
	return nil, fmt.Errorf("postgres not ready after %s", playgroundReadyTimeout)
}

func createSchema(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, playgroundConnStr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	_, err = conn.Exec(ctx, playgroundSchema)
	if err != nil {
		return fmt.Errorf("exec schema: %w", err)
	}
	return nil
}

func buildPlaygroundDetector(name string, logger *slog.Logger) (*listennotify.Detector, error) {
	switch name {
	case "listen_notify":
		return listennotify.New(playgroundConnStr, []string{"pgcdc:orders"}, 0, 0, logger), nil
	default:
		return nil, fmt.Errorf("playground only supports listen_notify detector (got %q)", name)
	}
}

func buildPlaygroundAdapter(name string, logger *slog.Logger) (adapter.Adapter, error) {
	switch name {
	case "stdout":
		return stdout.New(os.Stdout, logger), nil
	default:
		return nil, fmt.Errorf("playground supports stdout adapter (got %q)", name)
	}
}

func insertDemoRow(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, playgroundConnStr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	customers := []string{"Alice", "Bob", "Charlie", "Diana", "Eve"}
	regions := []string{"us-east", "us-west", "eu-west", "ap-south"}
	statuses := []string{"pending", "shipped", "delivered"}

	customer := customers[rand.Intn(len(customers))]
	amount := 10.0 + rand.Float64()*490.0
	region := regions[rand.Intn(len(regions))]
	status := statuses[rand.Intn(len(statuses))]

	_, err = conn.Exec(ctx,
		"INSERT INTO orders (customer, amount, region, status) VALUES ($1, $2, $3, $4)",
		customer, fmt.Sprintf("%.2f", amount), region, status,
	)
	return err
}
