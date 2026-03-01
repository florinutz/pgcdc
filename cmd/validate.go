package cmd

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/registry"
)

type validationResult struct {
	component string
	status    string
	message   string
	duration  time.Duration
}

var validateCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validate configuration without starting the pipeline",
	Long:  `Checks database connectivity, adapter configurations, and reports pass/fail status for each component.`,
	RunE:  runValidate,
}

func init() {
	rootCmd.AddCommand(validateCmd)
	f := validateCmd.Flags()
	f.String("db", "", "PostgreSQL connection string (env: PGCDC_DATABASE_URL)")
}

func runValidate(cmd *cobra.Command, args []string) error {
	var results []validationResult
	hasFailure := false

	// Resolve database URL: flag > viper > env.
	dbURL, _ := cmd.Flags().GetString("db")
	if dbURL == "" {
		dbURL = viper.GetString("database_url")
	}

	// Validate database connectivity.
	if dbURL != "" {
		r := validateDatabase(cmd.Context(), dbURL)
		results = append(results, r)
		if r.status == "FAIL" {
			hasFailure = true
		}
	} else {
		results = append(results, validationResult{
			component: "database",
			status:    "SKIP",
			message:   "no connection string provided (--db or PGCDC_DATABASE_URL)",
		})
	}

	// Structural config validation.
	cfg := config.Default()
	if err := viper.Unmarshal(&cfg); err != nil {
		results = append(results, validationResult{
			component: "config",
			status:    "FAIL",
			message:   fmt.Sprintf("unmarshal: %s", err),
		})
		hasFailure = true
	} else if err := cfg.Validate(); err != nil {
		results = append(results, validationResult{
			component: "config",
			status:    "FAIL",
			message:   err.Error(),
		})
		hasFailure = true
	} else {
		results = append(results, validationResult{
			component: "config",
			status:    "OK",
			message:   "structural validation passed",
		})
	}

	// ParamSpec-based validation (driven by registry declarations).
	specErrors := registry.ValidateConfig(cfg)
	if len(specErrors) > 0 {
		for _, ve := range specErrors {
			results = append(results, validationResult{
				component: fmt.Sprintf("spec/%s", ve.Connector),
				status:    "FAIL",
				message:   fmt.Sprintf("%s [%s]: %s", ve.Param, ve.Rule, ve.Message),
			})
		}
		hasFailure = true
	} else {
		results = append(results, validationResult{
			component: "spec",
			status:    "OK",
			message:   "all parameter specs passed",
		})
	}

	// Check configured adapters.
	adapters := viper.GetStringSlice("adapters")
	if len(adapters) == 0 {
		results = append(results, validationResult{
			component: "adapters",
			status:    "SKIP",
			message:   "no adapters configured",
		})
	}
	for _, name := range adapters {
		switch name {
		case "webhook":
			if cfg.Webhook.URL == "" {
				results = append(results, validationResult{
					component: "adapter/webhook",
					status:    "FAIL",
					message:   "webhook.url is required",
				})
				hasFailure = true
			} else {
				results = append(results, validationResult{
					component: "adapter/webhook",
					status:    "OK",
					message:   fmt.Sprintf("url=%s", cfg.Webhook.URL),
				})
			}
		case "embedding":
			switch {
			case cfg.Embedding.APIURL == "":
				results = append(results, validationResult{
					component: "adapter/embedding",
					status:    "FAIL",
					message:   "embedding.api_url is required",
				})
				hasFailure = true
			case len(cfg.Embedding.Columns) == 0:
				results = append(results, validationResult{
					component: "adapter/embedding",
					status:    "FAIL",
					message:   "embedding.columns must not be empty",
				})
				hasFailure = true
			default:
				results = append(results, validationResult{
					component: "adapter/embedding",
					status:    "OK",
					message:   fmt.Sprintf("api_url=%s columns=%v", cfg.Embedding.APIURL, cfg.Embedding.Columns),
				})
			}
		case "kafka":
			if len(cfg.Kafka.Brokers) == 0 {
				results = append(results, validationResult{
					component: "adapter/kafka",
					status:    "FAIL",
					message:   "kafka.brokers must not be empty",
				})
				hasFailure = true
			} else {
				results = append(results, validationResult{
					component: "adapter/kafka",
					status:    "OK",
					message:   fmt.Sprintf("brokers=%v", cfg.Kafka.Brokers),
				})
			}
		case "nats":
			if cfg.Nats.URL == "" {
				results = append(results, validationResult{
					component: "adapter/nats",
					status:    "FAIL",
					message:   "nats.url is required",
				})
				hasFailure = true
			} else {
				results = append(results, validationResult{
					component: "adapter/nats",
					status:    "OK",
					message:   fmt.Sprintf("url=%s", cfg.Nats.URL),
				})
			}
		case "search":
			if cfg.Search.URL == "" {
				results = append(results, validationResult{
					component: "adapter/search",
					status:    "FAIL",
					message:   "search.url is required",
				})
				hasFailure = true
			} else {
				results = append(results, validationResult{
					component: "adapter/search",
					status:    "OK",
					message:   fmt.Sprintf("engine=%s url=%s", cfg.Search.Engine, cfg.Search.URL),
				})
			}
		case "redis":
			if cfg.Redis.URL == "" {
				results = append(results, validationResult{
					component: "adapter/redis",
					status:    "FAIL",
					message:   "redis.url is required",
				})
				hasFailure = true
			} else {
				results = append(results, validationResult{
					component: "adapter/redis",
					status:    "OK",
					message:   fmt.Sprintf("url=%s mode=%s", cfg.Redis.URL, cfg.Redis.Mode),
				})
			}
		case "s3":
			if cfg.S3.Bucket == "" {
				results = append(results, validationResult{
					component: "adapter/s3",
					status:    "FAIL",
					message:   "s3.bucket is required",
				})
				hasFailure = true
			} else {
				results = append(results, validationResult{
					component: "adapter/s3",
					status:    "OK",
					message:   fmt.Sprintf("bucket=%s format=%s", cfg.S3.Bucket, cfg.S3.Format),
				})
			}
		case "pg_table":
			results = append(results, validationResult{
				component: "adapter/pg_table",
				status:    "OK",
				message:   fmt.Sprintf("table=%s", cfg.PGTable.Table),
			})
		case "file":
			if cfg.File.Path == "" {
				results = append(results, validationResult{
					component: "adapter/file",
					status:    "FAIL",
					message:   "file.path is required",
				})
				hasFailure = true
			} else {
				results = append(results, validationResult{
					component: "adapter/file",
					status:    "OK",
					message:   fmt.Sprintf("path=%s", cfg.File.Path),
				})
			}
		case "exec":
			if cfg.Exec.Command == "" {
				results = append(results, validationResult{
					component: "adapter/exec",
					status:    "FAIL",
					message:   "exec.command is required",
				})
				hasFailure = true
			} else {
				results = append(results, validationResult{
					component: "adapter/exec",
					status:    "OK",
					message:   fmt.Sprintf("command=%s", cfg.Exec.Command),
				})
			}
		default:
			results = append(results, validationResult{
				component: fmt.Sprintf("adapter/%s", name),
				status:    "OK",
				message:   "configured",
			})
		}
	}

	// Check detector.
	detectorType := viper.GetString("detector.type")
	if detectorType == "" {
		detectorType = "listen_notify"
	}
	validDetectors := map[string]bool{
		"listen_notify": true, "wal": true, "outbox": true, "mysql": true, "mongodb": true,
		"sqlite": true, "webhookgw": true,
	}
	if !validDetectors[detectorType] {
		results = append(results, validationResult{
			component: fmt.Sprintf("detector/%s", detectorType),
			status:    "FAIL",
			message:   fmt.Sprintf("unknown detector type %q", detectorType),
		})
		hasFailure = true
	} else {
		results = append(results, validationResult{
			component: fmt.Sprintf("detector/%s", detectorType),
			status:    "OK",
			message:   "configured",
		})
	}

	// Check detector feature compatibility.
	if cfg.Detector.Type != "wal" {
		walFeatures := []struct {
			name    string
			enabled bool
		}{
			{"tx-metadata", cfg.Detector.TxMetadata},
			{"tx-markers", cfg.Detector.TxMarkers},
			{"include-schema", cfg.Detector.IncludeSchema},
			{"schema-events", cfg.Detector.SchemaEvents},
			{"toast-cache", cfg.Detector.ToastCache},
			{"snapshot-first", cfg.Detector.SnapshotFirst},
			{"persistent-slot", cfg.Detector.PersistentSlot},
			{"cooperative-checkpoint", cfg.Detector.CooperativeCheckpoint},
			{"incremental-snapshot", cfg.IncrementalSnapshot.Enabled},
			{"backpressure", cfg.Backpressure.Enabled},
		}
		for _, f := range walFeatures {
			if f.enabled {
				results = append(results, validationResult{
					component: fmt.Sprintf("feature/%s", f.name),
					status:    "WARN",
					message:   "requires --detector wal",
				})
			}
		}
	}
	if cfg.Detector.CooperativeCheckpoint && !cfg.Detector.PersistentSlot {
		results = append(results, validationResult{
			component: "feature/cooperative-checkpoint",
			status:    "WARN",
			message:   "requires --persistent-slot",
		})
	}
	if cfg.Backpressure.Enabled && !cfg.Detector.PersistentSlot {
		results = append(results, validationResult{
			component: "feature/backpressure",
			status:    "WARN",
			message:   "requires --persistent-slot",
		})
	}

	// Print results table.
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "COMPONENT\tSTATUS\tDURATION\tMESSAGE")
	_, _ = fmt.Fprintln(w, "---------\t------\t--------\t-------")
	for _, r := range results {
		dur := "-"
		if r.duration > 0 {
			dur = r.duration.Truncate(time.Millisecond).String()
		}
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", r.component, r.status, dur, r.message)
	}
	_ = w.Flush()

	if hasFailure {
		return fmt.Errorf("validation failed")
	}
	return nil
}

func validateDatabase(ctx context.Context, dbURL string) validationResult {
	start := time.Now()
	connCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	conn, err := pgx.Connect(connCtx, dbURL)
	if err != nil {
		return validationResult{
			component: "database",
			status:    "FAIL",
			message:   fmt.Sprintf("connect: %s", err),
			duration:  time.Since(start),
		}
	}
	defer func() { _ = conn.Close(ctx) }()

	if err := conn.Ping(connCtx); err != nil {
		return validationResult{
			component: "database",
			status:    "FAIL",
			message:   fmt.Sprintf("ping: %s", err),
			duration:  time.Since(start),
		}
	}

	return validationResult{
		component: "database",
		status:    "OK",
		message:   "connected",
		duration:  time.Since(start),
	}
}
