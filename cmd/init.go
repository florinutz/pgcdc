package cmd

import (
	"embed"
	"fmt"
	"os"
	"regexp"
	"text/template"

	"github.com/spf13/cobra"
)

//go:embed sql/trigger_template.sql
var triggerSQL embed.FS

//go:embed sql/publication_template.sql
var publicationSQL embed.FS

//go:embed sql/events_table_template.sql
var eventsTableSQL embed.FS

//go:embed sql/embedding_table_template.sql
var embeddingTableSQL embed.FS

//go:embed sql/outbox_table_template.sql
var outboxTableSQL embed.FS

var (
	validTableName       = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	validChannelName     = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_:.\-]*$`)
	validPublicationName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
)

type triggerData struct {
	Table   string
	Channel string
}

type publicationData struct {
	Table       string
	Publication string
}

type eventsTableData struct {
	Table string
}

type embeddingTableData struct {
	Table     string
	Dimension int
}

type outboxTableData struct {
	Table string
}

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Generate SQL setup for a table (trigger, publication, or events table)",
	Long: `Outputs SQL statements to configure a PostgreSQL table for change detection.

By default, generates CREATE FUNCTION and CREATE TRIGGER for LISTEN/NOTIFY.
With --detector wal, generates CREATE PUBLICATION for WAL logical replication.
With --adapter pg_table, generates CREATE TABLE for the pg_table adapter.`,
	RunE: runInit,
}

func init() {
	initCmd.Flags().String("table", "", "table name (required)")
	initCmd.Flags().String("channel", "", "channel name (default: pgcdc:<table>)")
	initCmd.Flags().String("detector", "listen_notify", "detector type: listen_notify or wal")
	initCmd.Flags().String("publication", "", "publication name for WAL detector (default: pgcdc_<table>)")
	initCmd.Flags().String("adapter", "", "adapter type: pg_table, embedding, or outbox (generates table SQL)")
	initCmd.Flags().Int("dimension", 1536, "vector dimension for embedding adapter (default: 1536)")
	_ = initCmd.MarkFlagRequired("table")
}

func runInit(cmd *cobra.Command, args []string) error {
	table, _ := cmd.Flags().GetString("table")
	detectorType, _ := cmd.Flags().GetString("detector")
	adapterType, _ := cmd.Flags().GetString("adapter")

	if !validTableName.MatchString(table) {
		return fmt.Errorf("invalid table name %q: only alphanumeric characters and underscores are allowed", table)
	}

	// If --adapter is specified, generate adapter-specific SQL.
	if adapterType != "" {
		switch adapterType {
		case "pg_table":
			return runInitPGTable(table)
		case "embedding":
			dimension, _ := cmd.Flags().GetInt("dimension")
			return runInitEmbedding(table, dimension)
		case "outbox":
			return runInitOutbox(table)
		default:
			return fmt.Errorf("unknown adapter type %q for init: expected pg_table, embedding, or outbox", adapterType)
		}
	}

	switch detectorType {
	case "wal":
		return runInitWAL(cmd, table)
	case "listen_notify", "":
		return runInitListenNotify(cmd, table)
	default:
		return fmt.Errorf("unknown detector type %q: expected listen_notify or wal", detectorType)
	}
}

func runInitListenNotify(cmd *cobra.Command, table string) error {
	channel, _ := cmd.Flags().GetString("channel")

	if channel == "" {
		channel = "pgcdc:" + table
	}

	if !validChannelName.MatchString(channel) {
		return fmt.Errorf("invalid channel name %q: only alphanumeric characters, underscores, colons, dots, and hyphens are allowed", channel)
	}

	tmplBytes, err := triggerSQL.ReadFile("sql/trigger_template.sql")
	if err != nil {
		return fmt.Errorf("read embedded template: %w", err)
	}

	tmpl, err := template.New("trigger").Parse(string(tmplBytes))
	if err != nil {
		return fmt.Errorf("parse template: %w", err)
	}

	data := triggerData{
		Table:   table,
		Channel: channel,
	}

	if err := tmpl.Execute(os.Stdout, data); err != nil {
		return fmt.Errorf("execute template: %w", err)
	}

	return nil
}

func runInitWAL(cmd *cobra.Command, table string) error {
	publication, _ := cmd.Flags().GetString("publication")
	if publication == "" {
		publication = "pgcdc_" + table
	}

	if !validPublicationName.MatchString(publication) {
		return fmt.Errorf("invalid publication name %q: only alphanumeric characters and underscores are allowed", publication)
	}

	tmplBytes, err := publicationSQL.ReadFile("sql/publication_template.sql")
	if err != nil {
		return fmt.Errorf("read embedded template: %w", err)
	}

	tmpl, err := template.New("publication").Parse(string(tmplBytes))
	if err != nil {
		return fmt.Errorf("parse template: %w", err)
	}

	data := publicationData{
		Table:       table,
		Publication: publication,
	}

	if err := tmpl.Execute(os.Stdout, data); err != nil {
		return fmt.Errorf("execute template: %w", err)
	}

	return nil
}

func runInitPGTable(table string) error {
	tmplBytes, err := eventsTableSQL.ReadFile("sql/events_table_template.sql")
	if err != nil {
		return fmt.Errorf("read embedded template: %w", err)
	}

	tmpl, err := template.New("events_table").Parse(string(tmplBytes))
	if err != nil {
		return fmt.Errorf("parse template: %w", err)
	}

	data := eventsTableData{
		Table: table,
	}

	if err := tmpl.Execute(os.Stdout, data); err != nil {
		return fmt.Errorf("execute template: %w", err)
	}

	return nil
}

func runInitOutbox(table string) error {
	tmplBytes, err := outboxTableSQL.ReadFile("sql/outbox_table_template.sql")
	if err != nil {
		return fmt.Errorf("read embedded template: %w", err)
	}

	tmpl, err := template.New("outbox_table").Parse(string(tmplBytes))
	if err != nil {
		return fmt.Errorf("parse template: %w", err)
	}

	data := outboxTableData{
		Table: table,
	}

	if err := tmpl.Execute(os.Stdout, data); err != nil {
		return fmt.Errorf("execute template: %w", err)
	}

	return nil
}

func runInitEmbedding(table string, dimension int) error {
	if dimension <= 0 {
		dimension = 1536
	}

	tmplBytes, err := embeddingTableSQL.ReadFile("sql/embedding_table_template.sql")
	if err != nil {
		return fmt.Errorf("read embedded template: %w", err)
	}

	tmpl, err := template.New("embedding_table").Parse(string(tmplBytes))
	if err != nil {
		return fmt.Errorf("parse template: %w", err)
	}

	data := embeddingTableData{
		Table:     table,
		Dimension: dimension,
	}

	if err := tmpl.Execute(os.Stdout, data); err != nil {
		return fmt.Errorf("execute template: %w", err)
	}

	return nil
}
