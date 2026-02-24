package cmd

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"
)

var slotCmd = &cobra.Command{
	Use:   "slot",
	Short: "Manage pgcdc replication slots",
	Long:  `List, inspect, and drop persistent replication slots created by pgcdc.`,
}

var slotListCmd = &cobra.Command{
	Use:   "list",
	Short: "List pgcdc replication slots",
	RunE:  runSlotList,
}

var slotStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show status of a replication slot",
	RunE:  runSlotStatus,
}

var slotDropCmd = &cobra.Command{
	Use:   "drop",
	Short: "Drop a persistent replication slot",
	RunE:  runSlotDrop,
}

func init() {
	slotCmd.PersistentFlags().String("db", "", "PostgreSQL connection string (env: PGCDC_DATABASE_URL)")

	slotStatusCmd.Flags().String("name", "", "slot name (required)")
	_ = slotStatusCmd.MarkFlagRequired("name")

	slotDropCmd.Flags().String("name", "", "slot name (required)")
	_ = slotDropCmd.MarkFlagRequired("name")

	slotCmd.AddCommand(slotListCmd)
	slotCmd.AddCommand(slotStatusCmd)
	slotCmd.AddCommand(slotDropCmd)
	rootCmd.AddCommand(slotCmd)
}

func slotDBURL(cmd *cobra.Command) (string, error) {
	db, _ := cmd.Flags().GetString("db")
	if db == "" {
		db = os.Getenv("PGCDC_DATABASE_URL")
	}
	if db == "" {
		return "", fmt.Errorf("no database URL specified; use --db or export PGCDC_DATABASE_URL")
	}
	return db, nil
}

func runSlotList(cmd *cobra.Command, args []string) error {
	dbURL, err := slotDBURL(cmd)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	rows, err := conn.Query(ctx, `
		SELECT slot_name, plugin, slot_type, active, restart_lsn, confirmed_flush_lsn
		FROM pg_replication_slots
		WHERE slot_name LIKE 'pgcdc_%'
		ORDER BY slot_name
	`)
	if err != nil {
		return fmt.Errorf("query slots: %w", err)
	}
	defer rows.Close()

	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "SLOT\tPLUGIN\tTYPE\tACTIVE\tRESTART_LSN\tCONFIRMED_FLUSH_LSN")

	for rows.Next() {
		var name, plugin, slotType string
		var active bool
		var restartLSN, confirmedLSN *string

		if err := rows.Scan(&name, &plugin, &slotType, &active, &restartLSN, &confirmedLSN); err != nil {
			return fmt.Errorf("scan row: %w", err)
		}

		rl := "-"
		if restartLSN != nil {
			rl = *restartLSN
		}
		cl := "-"
		if confirmedLSN != nil {
			cl = *confirmedLSN
		}
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%v\t%s\t%s\n", name, plugin, slotType, active, rl, cl)
	}
	_ = w.Flush()

	return rows.Err()
}

func runSlotStatus(cmd *cobra.Command, args []string) error {
	dbURL, err := slotDBURL(cmd)
	if err != nil {
		return err
	}

	name, _ := cmd.Flags().GetString("name")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	var slotName, plugin, slotType string
	var active bool
	var restartLSN, confirmedLSN *string
	var lagBytes *int64

	err = conn.QueryRow(ctx, `
		SELECT
			slot_name, plugin, slot_type, active,
			restart_lsn, confirmed_flush_lsn,
			pg_wal_lsn_diff(pg_current_wal_lsn(), COALESCE(confirmed_flush_lsn, restart_lsn)) AS lag_bytes
		FROM pg_replication_slots
		WHERE slot_name = $1
	`, name).Scan(&slotName, &plugin, &slotType, &active, &restartLSN, &confirmedLSN, &lagBytes)
	if err == pgx.ErrNoRows {
		return fmt.Errorf("slot %q not found", name)
	}
	if err != nil {
		return fmt.Errorf("query slot: %w", err)
	}

	rl := "-"
	if restartLSN != nil {
		rl = *restartLSN
	}
	cl := "-"
	if confirmedLSN != nil {
		cl = *confirmedLSN
	}
	lag := "unknown"
	if lagBytes != nil {
		lag = formatBytes(*lagBytes)
	}

	fmt.Printf("Slot:                %s\n", slotName)
	fmt.Printf("Plugin:              %s\n", plugin)
	fmt.Printf("Type:                %s\n", slotType)
	fmt.Printf("Active:              %v\n", active)
	fmt.Printf("Restart LSN:         %s\n", rl)
	fmt.Printf("Confirmed Flush LSN: %s\n", cl)
	fmt.Printf("WAL Retained:        %s\n", lag)

	return nil
}

func runSlotDrop(cmd *cobra.Command, args []string) error {
	dbURL, err := slotDBURL(cmd)
	if err != nil {
		return err
	}

	name, _ := cmd.Flags().GetString("name")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	_, err = conn.Exec(ctx, "SELECT pg_drop_replication_slot($1)", name)
	if err != nil {
		return fmt.Errorf("drop slot: %w", err)
	}

	fmt.Printf("Slot %q dropped successfully.\n", name)
	return nil
}

func formatBytes(b int64) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
	)
	switch {
	case b >= gb:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(gb))
	case b >= mb:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(mb))
	case b >= kb:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(kb))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
