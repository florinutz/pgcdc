package cmd

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/florinutz/pgcdc/internal/output"
	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"
)

type slotInfo struct {
	Slot              string `json:"slot"`
	Plugin            string `json:"plugin"`
	Type              string `json:"type"`
	Active            bool   `json:"active"`
	RestartLSN        string `json:"restart_lsn"`
	ConfirmedFlushLSN string `json:"confirmed_flush_lsn"`
	WALRetained       string `json:"wal_retained,omitempty"`
}

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

	output.AddOutputFlag(slotListCmd)
	output.AddOutputFlag(slotStatusCmd)

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

	var slots []slotInfo
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
		slots = append(slots, slotInfo{
			Slot:              name,
			Plugin:            plugin,
			Type:              slotType,
			Active:            active,
			RestartLSN:        rl,
			ConfirmedFlushLSN: cl,
		})
	}
	if err := rows.Err(); err != nil {
		return err
	}

	printer := output.FromCommand(cmd)

	if printer.IsJSON() {
		return printer.JSON(slots)
	}

	headers := []string{"SLOT", "PLUGIN", "TYPE", "ACTIVE", "RESTART_LSN", "CONFIRMED_FLUSH_LSN"}
	tableRows := make([][]string, len(slots))
	for i, s := range slots {
		tableRows[i] = []string{s.Slot, s.Plugin, s.Type, fmt.Sprintf("%v", s.Active), s.RestartLSN, s.ConfirmedFlushLSN}
	}
	return printer.Table(headers, tableRows)
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

	info := slotInfo{
		Slot:              slotName,
		Plugin:            plugin,
		Type:              slotType,
		Active:            active,
		RestartLSN:        rl,
		ConfirmedFlushLSN: cl,
		WALRetained:       lag,
	}

	printer := output.FromCommand(cmd)

	if printer.IsJSON() {
		return printer.JSON(info)
	}

	w := printer.Writer()
	_, _ = fmt.Fprintf(w, "Slot:                %s\n", info.Slot)
	_, _ = fmt.Fprintf(w, "Plugin:              %s\n", info.Plugin)
	_, _ = fmt.Fprintf(w, "Type:                %s\n", info.Type)
	_, _ = fmt.Fprintf(w, "Active:              %v\n", info.Active)
	_, _ = fmt.Fprintf(w, "Restart LSN:         %s\n", info.RestartLSN)
	_, _ = fmt.Fprintf(w, "Confirmed Flush LSN: %s\n", info.ConfirmedFlushLSN)
	_, _ = fmt.Fprintf(w, "WAL Retained:        %s\n", info.WALRetained)

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
