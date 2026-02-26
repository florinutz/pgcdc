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
		results = append(results, validationResult{
			component: fmt.Sprintf("adapter/%s", name),
			status:    "OK",
			message:   "configured (deep validation not yet wired)",
		})
	}

	// Check detector.
	detectorType := viper.GetString("detector.type")
	if detectorType == "" {
		detectorType = "listen_notify"
	}
	results = append(results, validationResult{
		component: fmt.Sprintf("detector/%s", detectorType),
		status:    "OK",
		message:   "configured",
	})

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
