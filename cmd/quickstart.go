package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"text/template"

	"github.com/florinutz/pgcdc/internal/prompt"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

var quickstartCmd = &cobra.Command{
	Use:   "quickstart",
	Short: "Generate a starter pgcdc project with config, SQL, and optional Docker setup",
	Long: `Interactive wizard that generates a pgcdc.yaml configuration file,
database initialization SQL, and optionally a docker-compose.yml. Use
--non-interactive to skip prompts and use flag values directly.`,
	RunE: runQuickstart,
}

func init() {
	rootCmd.AddCommand(quickstartCmd)

	f := quickstartCmd.Flags()
	f.String("db-type", "postgres", "database type: postgres, mysql, mongodb, sqlite")
	f.String("detector", "", "detector type (auto-detected from --db if omitted)")
	f.StringSlice("adapter", nil, "adapters (comma-separated)")
	f.Bool("docker", false, "generate docker-compose.yml")
	f.Bool("non-interactive", false, "skip prompts, use flag values")
	f.StringP("output-dir", "d", ".", "output directory")
}

// quickstartConfig holds the gathered configuration for file generation.
type quickstartConfig struct {
	DB       string
	Detector string
	Adapters []string
	Docker   bool
	OutDir   string
}

func runQuickstart(cmd *cobra.Command, args []string) error {
	db, _ := cmd.Flags().GetString("db-type")
	detector, _ := cmd.Flags().GetString("detector")
	adapters, _ := cmd.Flags().GetStringSlice("adapter")
	docker, _ := cmd.Flags().GetBool("docker")
	nonInteractive, _ := cmd.Flags().GetBool("non-interactive")
	outDir, _ := cmd.Flags().GetString("output-dir")

	qc := quickstartConfig{
		DB:       db,
		Detector: detector,
		Adapters: adapters,
		Docker:   docker,
		OutDir:   outDir,
	}

	// Interactive mode: when stdin is a TTY and required info is missing.
	isTTY := term.IsTerminal(int(os.Stdin.Fd()))
	needsPrompt := !nonInteractive && isTTY && (len(qc.Adapters) == 0 || qc.Detector == "")

	if needsPrompt {
		p := prompt.New(os.Stdin, cmd.OutOrStdout())
		if err := runQuickstartPrompts(p, &qc); err != nil {
			return err
		}
	}

	// Fill in defaults.
	if qc.Detector == "" {
		qc.Detector = defaultDetector(qc.DB)
	}
	if len(qc.Adapters) == 0 {
		qc.Adapters = []string{"stdout"}
	}

	// Create output directory.
	if err := os.MkdirAll(qc.OutDir, 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	// Generate pgcdc.yaml.
	if err := writeQuickstartFile(filepath.Join(qc.OutDir, "pgcdc.yaml"), qsConfigTemplate, qc); err != nil {
		return err
	}
	fmt.Fprintln(cmd.OutOrStdout(), "Wrote pgcdc.yaml")

	// Generate init.sql (postgres only).
	if qc.DB == "postgres" {
		if err := writeQuickstartFile(filepath.Join(qc.OutDir, "init.sql"), qsInitSQLTemplate, qc); err != nil {
			return err
		}
		fmt.Fprintln(cmd.OutOrStdout(), "Wrote init.sql")
	}

	// Generate docker-compose.yml.
	if qc.Docker {
		if err := writeQuickstartFile(filepath.Join(qc.OutDir, "docker-compose.yml"), qsDockerComposeTemplate, qc); err != nil {
			return err
		}
		fmt.Fprintln(cmd.OutOrStdout(), "Wrote docker-compose.yml")
	}

	fmt.Fprintln(cmd.OutOrStdout())
	fmt.Fprintln(cmd.OutOrStdout(), "Next steps:")
	fmt.Fprintf(cmd.OutOrStdout(), "  1. Edit pgcdc.yaml with your database credentials\n")
	if qc.DB == "postgres" {
		fmt.Fprintf(cmd.OutOrStdout(), "  2. Run init.sql against your database\n")
	}
	if qc.Docker {
		fmt.Fprintf(cmd.OutOrStdout(), "  3. docker compose up\n")
	} else {
		fmt.Fprintf(cmd.OutOrStdout(), "  3. pgcdc listen\n")
	}
	return nil
}

func runQuickstartPrompts(p *prompt.Prompter, qc *quickstartConfig) error {
	// 1. Database type.
	dbTypes := []string{"postgres", "mysql", "mongodb", "sqlite"}
	dbDefault := 0
	for i, d := range dbTypes {
		if d == qc.DB {
			dbDefault = i
			break
		}
	}
	idx, err := p.Select("Database type", dbTypes, dbDefault)
	if err != nil {
		return err
	}
	qc.DB = dbTypes[idx]

	// 2. Detector type.
	detectors := detectorsForDB(qc.DB)
	if len(detectors) > 1 {
		didx, err := p.Select("Detector type", detectors, 0)
		if err != nil {
			return err
		}
		qc.Detector = detectors[didx]
	} else {
		qc.Detector = detectors[0]
	}

	// 3. Adapters.
	adapterOpts := []string{"stdout", "webhook", "sse", "ws", "kafka", "nats", "redis", "file", "exec", "pg_table", "grpc", "s3"}
	selected, err := p.MultiSelect("Adapters", adapterOpts)
	if err != nil {
		return err
	}
	if len(selected) > 0 {
		qc.Adapters = make([]string, len(selected))
		for i, s := range selected {
			qc.Adapters[i] = adapterOpts[s]
		}
	}

	// 4. Docker compose.
	docker, err := p.Confirm("Generate docker-compose.yml?", qc.Docker)
	if err != nil {
		return err
	}
	qc.Docker = docker

	return nil
}

func detectorsForDB(db string) []string {
	switch db {
	case "postgres":
		return []string{"wal", "listen_notify", "outbox"}
	case "mysql":
		return []string{"mysql"}
	case "mongodb":
		return []string{"mongodb"}
	case "sqlite":
		return []string{"sqlite"}
	default:
		return []string{"listen_notify"}
	}
}

func defaultDetector(db string) string {
	switch db {
	case "postgres":
		return "wal"
	case "mysql":
		return "mysql"
	case "mongodb":
		return "mongodb"
	case "sqlite":
		return "sqlite"
	default:
		return "listen_notify"
	}
}

func writeQuickstartFile(path, tmplStr string, data quickstartConfig) error {
	funcMap := template.FuncMap{
		"join": func(s []string, sep string) string {
			result := ""
			for i, v := range s {
				if i > 0 {
					result += sep
				}
				result += v
			}
			return result
		},
		"hasAdapter": func(adapters []string, name string) bool {
			for _, a := range adapters {
				if a == name {
					return true
				}
			}
			return false
		},
	}

	tmpl, err := template.New(filepath.Base(path)).Funcs(funcMap).Parse(tmplStr)
	if err != nil {
		return fmt.Errorf("parse template for %s: %w", path, err)
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	if err := tmpl.Execute(f, data); err != nil {
		return fmt.Errorf("render %s: %w", path, err)
	}
	return nil
}

const qsConfigTemplate = `# pgcdc configuration
# Generated by: pgcdc quickstart
{{- if eq .DB "postgres"}}

database_url: "postgres://user:pass@localhost:5432/mydb"
{{- else if eq .DB "mysql"}}

# MySQL is configured in the mysql section below.
{{- else if eq .DB "mongodb"}}

# MongoDB is configured in the mongodb section below.
{{- else if eq .DB "sqlite"}}

# SQLite is configured in the sqlite section below.
{{- end}}
{{- if eq .Detector "listen_notify"}}

channels:
  - orders
  - users
{{- else}}

channels: []
{{- end}}

detector:
  type: {{.Detector}}
{{- if eq .Detector "wal"}}
  # publication: pgcdc_pub
  persistent_slot: false
{{- end}}

adapters:
{{- range .Adapters}}
  - {{.}}
{{- end}}
{{- if hasAdapter .Adapters "webhook"}}

# Adapter-specific configuration:
webhook:
  url: "https://example.com/webhook"
  # secret: ""
  max_retries: 5
  timeout: 10s
{{- end}}
{{- if hasAdapter .Adapters "kafka"}}

kafka:
  brokers:
    - "localhost:9092"
  # topic: ""
{{- end}}
{{- if hasAdapter .Adapters "nats"}}

nats:
  url: "nats://localhost:4222"
  subject: pgcdc
  stream: pgcdc
{{- end}}
{{- if hasAdapter .Adapters "redis"}}

redis:
  url: "redis://localhost:6379"
  mode: invalidate
{{- end}}
{{- if hasAdapter .Adapters "s3"}}

s3:
  bucket: "my-cdc-bucket"
  region: us-east-1
  format: jsonl
{{- end}}
{{- if hasAdapter .Adapters "grpc"}}

grpc:
  addr: ":9090"
{{- end}}
{{- if hasAdapter .Adapters "file"}}

file:
  path: "/var/log/pgcdc/events.jsonl"
{{- end}}
{{- if eq .DB "mysql"}}

mysql:
  addr: "127.0.0.1:3306"
  user: replicator
  password: ""
  server_id: 1001
{{- end}}
{{- if eq .DB "mongodb"}}

mongodb:
  uri: "mongodb://localhost:27017"
  scope: database
  database: mydb
  full_document: updateLookup
{{- end}}
{{- if eq .DB "sqlite"}}

sqlite:
  db_path: "./mydb.sqlite"
  poll_interval: 500ms
  batch_size: 100
{{- end}}

bus:
  buffer_size: 1024
  mode: fast

shutdown_timeout: 5s
log_level: info

dlq:
  type: stderr
`

const qsInitSQLTemplate = `-- pgcdc database initialization
-- Generated by: pgcdc quickstart
-- Run this against your PostgreSQL database.
{{- if eq .Detector "listen_notify"}}

-- LISTEN/NOTIFY trigger function.
-- Creates a trigger that sends notifications on INSERT/UPDATE/DELETE.

CREATE OR REPLACE FUNCTION pgcdc_notify() RETURNS trigger AS $$
DECLARE
  payload json;
BEGIN
  IF TG_OP = 'DELETE' THEN
    payload = json_build_object(
      'operation', TG_OP,
      'table', TG_TABLE_NAME,
      'row', row_to_json(OLD)
    );
  ELSE
    payload = json_build_object(
      'operation', TG_OP,
      'table', TG_TABLE_NAME,
      'row', row_to_json(NEW)
    );
  END IF;

  PERFORM pg_notify('pgcdc:' || TG_TABLE_NAME, payload::text);
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Apply to your tables (replace 'orders' with your table name):
-- CREATE TRIGGER pgcdc_orders_trigger
--   AFTER INSERT OR UPDATE OR DELETE ON orders
--   FOR EACH ROW EXECUTE FUNCTION pgcdc_notify();
{{- else if eq .Detector "wal"}}

-- WAL logical replication setup.
-- Prerequisites: wal_level=logical in postgresql.conf, REPLICATION privilege.

-- Create a publication for your tables:
-- CREATE PUBLICATION pgcdc_pub FOR TABLE orders, users;

-- Or for all tables:
-- CREATE PUBLICATION pgcdc_pub FOR ALL TABLES;
{{- else if eq .Detector "outbox"}}

-- Outbox table for the outbox pattern.

CREATE TABLE IF NOT EXISTS pgcdc_outbox (
  id          BIGSERIAL PRIMARY KEY,
  channel     TEXT NOT NULL,
  payload     JSONB NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  processed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_pgcdc_outbox_pending
  ON pgcdc_outbox (id) WHERE processed_at IS NULL;
{{- end}}
`

const qsDockerComposeTemplate = `version: "3.8"
services:
{{- if eq .DB "postgres"}}
  db:
    image: postgres:16
    environment:
      POSTGRES_USER: user
      POSTGRES_PASSWORD: pass
      POSTGRES_DB: mydb
{{- if eq .Detector "wal"}}
    command: >
      postgres
      -c wal_level=logical
      -c max_replication_slots=4
      -c max_wal_senders=4
{{- end}}
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
{{- else if eq .DB "mysql"}}
  db:
    image: mysql:8
    environment:
      MYSQL_ROOT_PASSWORD: rootpass
      MYSQL_DATABASE: mydb
      MYSQL_USER: replicator
      MYSQL_PASSWORD: replpass
    command: >
      --server-id=1
      --log-bin=mysql-bin
      --binlog-format=ROW
      --binlog-row-image=FULL
    ports:
      - "3306:3306"
    volumes:
      - mysqldata:/var/lib/mysql
{{- else if eq .DB "mongodb"}}
  db:
    image: mongo:7
    command: ["--replSet", "rs0"]
    ports:
      - "27017:27017"
    volumes:
      - mongodata:/data/db
{{- end}}

  pgcdc:
    image: ghcr.io/florinutz/pgcdc:latest
{{- if eq .DB "postgres"}}
    command: listen --db postgres://user:pass@db:5432/mydb --detector {{.Detector}} --adapter {{join .Adapters ","}}
{{- else if eq .DB "mysql"}}
    command: listen --detector mysql --mysql-addr db:3306 --mysql-user replicator --mysql-password replpass --mysql-server-id 1001 --adapter {{join .Adapters ","}}
{{- else if eq .DB "mongodb"}}
    command: listen --detector mongodb --mongodb-uri mongodb://db:27017 --mongodb-database mydb --adapter {{join .Adapters ","}}
{{- end}}
    depends_on:
      - db

volumes:
{{- if eq .DB "postgres"}}
  pgdata:
{{- else if eq .DB "mysql"}}
  mysqldata:
{{- else if eq .DB "mongodb"}}
  mongodata:
{{- end}}
`
