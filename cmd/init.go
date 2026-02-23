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

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Generate SQL setup for a table (trigger or publication)",
	Long: `Outputs SQL statements to configure a PostgreSQL table for change detection.

By default, generates CREATE FUNCTION and CREATE TRIGGER for LISTEN/NOTIFY.
With --detector wal, generates CREATE PUBLICATION for WAL logical replication.`,
	RunE: runInit,
}

func init() {
	initCmd.Flags().String("table", "", "table name (required)")
	initCmd.Flags().String("channel", "", "channel name (default: pgpipe:<table>)")
	initCmd.Flags().String("detector", "listen_notify", "detector type: listen_notify or wal")
	initCmd.Flags().String("publication", "", "publication name for WAL detector (default: pgpipe_<table>)")
	_ = initCmd.MarkFlagRequired("table")
}

func runInit(cmd *cobra.Command, args []string) error {
	table, _ := cmd.Flags().GetString("table")
	detectorType, _ := cmd.Flags().GetString("detector")

	if !validTableName.MatchString(table) {
		return fmt.Errorf("invalid table name %q: only alphanumeric characters and underscores are allowed", table)
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
		channel = "pgpipe:" + table
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
		publication = "pgpipe_" + table
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
