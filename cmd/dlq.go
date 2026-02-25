package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/adapter/stdout"
	"github.com/florinutz/pgcdc/adapter/webhook"
	"github.com/florinutz/pgcdc/dlq"
	"github.com/florinutz/pgcdc/event"
	"github.com/spf13/cobra"
)

var dlqCmd = &cobra.Command{
	Use:   "dlq",
	Short: "Manage dead letter queue records",
	Long:  `List, replay, and purge records from the pgcdc dead letter queue table.`,
}

var dlqListCmd = &cobra.Command{
	Use:   "list",
	Short: "List dead letter queue records",
	RunE:  runDLQList,
}

var dlqReplayCmd = &cobra.Command{
	Use:   "replay",
	Short: "Replay dead letter queue records through an adapter",
	RunE:  runDLQReplay,
}

var dlqPurgeCmd = &cobra.Command{
	Use:   "purge",
	Short: "Delete dead letter queue records",
	RunE:  runDLQPurge,
}

func init() {
	dlqCmd.PersistentFlags().String("db", "", "PostgreSQL connection string (env: PGCDC_DATABASE_URL)")
	dlqCmd.PersistentFlags().String("dlq-table", "pgcdc_dead_letters", "DLQ table name")

	// list flags
	dlqListCmd.Flags().String("adapter", "", "filter by adapter name")
	dlqListCmd.Flags().String("since", "", "filter records created after this time (RFC3339)")
	dlqListCmd.Flags().String("until", "", "filter records created before this time (RFC3339)")
	dlqListCmd.Flags().String("id", "", "filter by record ID")
	dlqListCmd.Flags().Int("limit", 50, "maximum number of records to return")
	dlqListCmd.Flags().Bool("pending", false, "only show records not yet replayed")
	dlqListCmd.Flags().String("format", "table", "output format: table or json")

	// replay flags
	dlqReplayCmd.Flags().String("adapter", "", "filter by adapter name")
	dlqReplayCmd.Flags().String("since", "", "filter records created after this time (RFC3339)")
	dlqReplayCmd.Flags().String("until", "", "filter records created before this time (RFC3339)")
	dlqReplayCmd.Flags().String("id", "", "filter by record ID")
	dlqReplayCmd.Flags().Int("limit", 0, "maximum number of records to replay")
	dlqReplayCmd.Flags().Bool("dry-run", false, "show what would be replayed without delivering")
	dlqReplayCmd.Flags().String("webhook-url", "", "override webhook URL for replay")
	dlqReplayCmd.Flags().StringSlice("kafka-brokers", nil, "override Kafka brokers for replay")
	dlqReplayCmd.Flags().String("kafka-topic", "", "override Kafka topic for replay")

	// purge flags
	dlqPurgeCmd.Flags().String("adapter", "", "filter by adapter name")
	dlqPurgeCmd.Flags().String("since", "", "filter records created after this time (RFC3339)")
	dlqPurgeCmd.Flags().String("until", "", "filter records created before this time (RFC3339)")
	dlqPurgeCmd.Flags().Bool("replayed", false, "only purge records that have been replayed")
	dlqPurgeCmd.Flags().Bool("force", false, "skip confirmation prompt")

	dlqCmd.AddCommand(dlqListCmd)
	dlqCmd.AddCommand(dlqReplayCmd)
	dlqCmd.AddCommand(dlqPurgeCmd)
	rootCmd.AddCommand(dlqCmd)
}

func dlqDBURL(cmd *cobra.Command) (string, error) {
	db, _ := cmd.Flags().GetString("db")
	if db == "" {
		db = os.Getenv("PGCDC_DATABASE_URL")
	}
	if db == "" {
		return "", fmt.Errorf("no database URL specified; use --db or export PGCDC_DATABASE_URL")
	}
	return db, nil
}

func dlqTable(cmd *cobra.Command) string {
	table, _ := cmd.Flags().GetString("dlq-table")
	return table
}

func parseDLQListFilter(cmd *cobra.Command) (dlq.ListFilter, error) {
	var f dlq.ListFilter
	f.Adapter, _ = cmd.Flags().GetString("adapter")
	f.ID, _ = cmd.Flags().GetString("id")
	f.Limit, _ = cmd.Flags().GetInt("limit")

	if cmd.Flags().Lookup("pending") != nil {
		f.Pending, _ = cmd.Flags().GetBool("pending")
	}
	if cmd.Flags().Lookup("replayed") != nil {
		f.ReplayedOnly, _ = cmd.Flags().GetBool("replayed")
	}

	sinceStr, _ := cmd.Flags().GetString("since")
	if sinceStr != "" {
		t, err := time.Parse(time.RFC3339, sinceStr)
		if err != nil {
			return f, fmt.Errorf("invalid --since: %w", err)
		}
		f.Since = &t
	}

	untilStr, _ := cmd.Flags().GetString("until")
	if untilStr != "" {
		t, err := time.Parse(time.RFC3339, untilStr)
		if err != nil {
			return f, fmt.Errorf("invalid --until: %w", err)
		}
		f.Until = &t
	}

	return f, nil
}

func runDLQList(cmd *cobra.Command, args []string) error {
	dbURL, err := dlqDBURL(cmd)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store := dlq.NewStore(dbURL, dlqTable(cmd), nil)
	if err := store.EnsureSchema(ctx); err != nil {
		return err
	}

	filter, err := parseDLQListFilter(cmd)
	if err != nil {
		return err
	}

	format, _ := cmd.Flags().GetString("format")
	if format == "json" {
		return dlqListJSON(ctx, store, filter)
	}
	return dlqListTable(ctx, store, filter)
}

func dlqListJSON(ctx context.Context, store *dlq.Store, filter dlq.ListFilter) error {
	records, err := store.List(ctx, filter)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(records)
}

func dlqListTable(ctx context.Context, store *dlq.Store, filter dlq.ListFilter) error {
	records, err := store.List(ctx, filter)
	if err != nil {
		return err
	}

	if len(records) == 0 {
		fmt.Println("No dead letter queue records found.")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "ID\tEVENT_ID\tADAPTER\tERROR\tCREATED_AT\tREPLAYED_AT")

	for _, r := range records {
		errMsg := r.Error
		if len(errMsg) > 60 {
			errMsg = errMsg[:57] + "..."
		}
		replayed := "-"
		if r.ReplayedAt != nil {
			replayed = r.ReplayedAt.Format(time.RFC3339)
		}
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
			r.ID, r.EventID, r.Adapter, errMsg,
			r.CreatedAt.Format(time.RFC3339), replayed,
		)
	}
	_ = w.Flush()

	// Show count summary if limit was applied and there may be more records.
	total, countErr := store.Count(ctx, filter)
	if countErr == nil && total > int64(len(records)) {
		fmt.Printf("\nShowing %d of %d total records. Use --limit to see more.\n", len(records), total)
	}

	return nil
}

func runDLQReplay(cmd *cobra.Command, args []string) error {
	dbURL, err := dlqDBURL(cmd)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	store := dlq.NewStore(dbURL, dlqTable(cmd), nil)
	if err := store.EnsureSchema(ctx); err != nil {
		return err
	}

	filter, err := parseDLQListFilter(cmd)
	if err != nil {
		return err
	}
	filter.Pending = true // only replay unprocessed records

	records, err := store.List(ctx, filter)
	if err != nil {
		return err
	}

	if len(records) == 0 {
		fmt.Println("No pending dead letter queue records to replay.")
		return nil
	}

	// Group records by adapter.
	grouped := make(map[string][]dlq.StoredRecord)
	for _, r := range records {
		grouped[r.Adapter] = append(grouped[r.Adapter], r)
	}

	dryRun, _ := cmd.Flags().GetBool("dry-run")
	if dryRun {
		fmt.Println("Dry run — the following records would be replayed:")
		for adapterName, recs := range grouped {
			fmt.Printf("  %s: %d records\n", adapterName, len(recs))
		}
		return nil
	}

	logger := slog.Default()
	var totalSuccess, totalFailed int

	for adapterName, recs := range grouped {
		a, buildErr := buildReplayAdapter(adapterName, cmd, logger)
		if buildErr != nil {
			fmt.Printf("Skipping adapter %q: %v\n", adapterName, buildErr)
			totalFailed += len(recs)
			continue
		}

		capture := newReplayCaptureDLQ()

		// Wire DLQ capture if adapter supports it.
		if da, ok := a.(interface{ SetDLQ(dlq.DLQ) }); ok {
			da.SetDLQ(capture)
		}

		// Unmarshal events and send to adapter.
		ch := make(chan event.Event, len(recs))
		recordIDs := make(map[string]string) // event ID -> record ID
		for _, r := range recs {
			var ev event.Event
			if unmarshalErr := json.Unmarshal(r.Payload, &ev); unmarshalErr != nil {
				logger.Error("unmarshal dlq payload", "record_id", r.ID, "error", unmarshalErr)
				totalFailed++
				continue
			}
			recordIDs[ev.ID] = r.ID
			ch <- ev
		}
		close(ch)

		// Run adapter with per-adapter timeout.
		adapterCtx, adapterCancel := context.WithTimeout(ctx, 2*time.Minute)
		_ = a.Start(adapterCtx, ch)
		adapterCancel()

		// Determine successes vs failures.
		var successIDs []string
		for eventID, recordID := range recordIDs {
			if !capture.Failed(eventID) {
				successIDs = append(successIDs, recordID)
			}
		}
		failed := len(recs) - len(successIDs)

		if len(successIDs) > 0 {
			if _, markErr := store.MarkReplayed(ctx, successIDs); markErr != nil {
				logger.Error("mark replayed", "error", markErr)
			}
		}

		totalSuccess += len(successIDs)
		totalFailed += failed
		fmt.Printf("  %s: %d succeeded, %d failed\n", adapterName, len(successIDs), failed)
	}

	fmt.Printf("\nReplay complete: %d succeeded, %d failed\n", totalSuccess, totalFailed)
	return nil
}

func buildReplayAdapter(name string, cmd *cobra.Command, logger *slog.Logger) (adapter.Adapter, error) {
	switch name {
	case "stdout":
		return stdout.New(os.Stdout, logger), nil
	case "webhook":
		url, _ := cmd.Flags().GetString("webhook-url")
		if url == "" {
			return nil, fmt.Errorf("--webhook-url required for webhook replay")
		}
		return webhook.New(url, nil, "", 3, 0, 0, 0, logger), nil
	case "kafka":
		return buildKafkaReplayAdapter(cmd, logger)
	default:
		return nil, fmt.Errorf("replay not supported for adapter %q (supported: stdout, webhook, kafka)", name)
	}
}

// replayCaptureDLQ implements dlq.DLQ and captures which events failed.
type replayCaptureDLQ struct {
	mu     sync.Mutex
	failed map[string]bool
}

func newReplayCaptureDLQ() *replayCaptureDLQ {
	return &replayCaptureDLQ{failed: make(map[string]bool)}
}

func (c *replayCaptureDLQ) Record(_ context.Context, ev event.Event, _ string, _ error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failed[ev.ID] = true
	return nil
}

func (c *replayCaptureDLQ) Close() error { return nil }

func (c *replayCaptureDLQ) Failed(eventID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.failed[eventID]
}

func runDLQPurge(cmd *cobra.Command, args []string) error {
	dbURL, err := dlqDBURL(cmd)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store := dlq.NewStore(dbURL, dlqTable(cmd), nil)
	if err := store.EnsureSchema(ctx); err != nil {
		return err
	}

	filter, err := parseDLQListFilter(cmd)
	if err != nil {
		return err
	}

	replayed, _ := cmd.Flags().GetBool("replayed")
	if replayed {
		filter.ReplayedOnly = true
	}

	// Show what will be purged.
	count, err := store.Count(ctx, filter)
	if err != nil {
		return err
	}
	if count == 0 {
		fmt.Println("No matching dead letter queue records to purge.")
		return nil
	}

	force, _ := cmd.Flags().GetBool("force")
	if !force {
		fmt.Printf("About to delete %d DLQ record(s). Continue? [y/N] ", count)
		var answer string
		_, _ = fmt.Scanln(&answer)
		if answer != "y" && answer != "Y" {
			fmt.Println("Aborted.")
			return nil
		}
	}

	deleted, err := store.Purge(ctx, filter)
	if err != nil {
		return err
	}

	fmt.Printf("Purged %d dead letter queue record(s).\n", deleted)
	return nil
}
