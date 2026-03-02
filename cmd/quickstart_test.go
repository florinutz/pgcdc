package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/florinutz/pgcdc/internal/prompt"
)

func TestRunQuickstart_Postgres(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	qc := quickstartConfig{
		DB:       "postgres",
		Detector: "",
		Adapters: []string{"stdout"},
		Docker:   false,
		OutDir:   dir,
	}

	cmd := quickstartCmd
	cmd.SetOut(&strings.Builder{})

	// Simulate non-interactive by filling required fields.
	qc.Detector = defaultDetector(qc.DB)
	if err := writeQuickstartFiles(t, qc); err != nil {
		t.Fatal(err)
	}

	// Check pgcdc.yaml exists and has correct detector.
	yaml := readFile(t, filepath.Join(dir, "pgcdc.yaml"))
	if !strings.Contains(yaml, "type: wal") {
		t.Errorf("expected detector type 'wal' in pgcdc.yaml:\n%s", yaml)
	}
	if !strings.Contains(yaml, "postgres://") {
		t.Errorf("expected postgres connection string in pgcdc.yaml:\n%s", yaml)
	}

	// Check init.sql exists for postgres.
	if _, err := os.Stat(filepath.Join(dir, "init.sql")); os.IsNotExist(err) {
		t.Error("expected init.sql for postgres")
	}
}

func TestRunQuickstart_MySQL(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	qc := quickstartConfig{
		DB:       "mysql",
		Detector: defaultDetector("mysql"),
		Adapters: []string{"stdout"},
		Docker:   false,
		OutDir:   dir,
	}

	if err := writeQuickstartFiles(t, qc); err != nil {
		t.Fatal(err)
	}

	yaml := readFile(t, filepath.Join(dir, "pgcdc.yaml"))
	if !strings.Contains(yaml, "type: mysql") {
		t.Errorf("expected detector type 'mysql' in pgcdc.yaml:\n%s", yaml)
	}

	// No init.sql for mysql.
	if _, err := os.Stat(filepath.Join(dir, "init.sql")); !os.IsNotExist(err) {
		t.Error("expected no init.sql for mysql")
	}
}

func TestRunQuickstart_Docker(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	qc := quickstartConfig{
		DB:       "postgres",
		Detector: "wal",
		Adapters: []string{"stdout"},
		Docker:   true,
		OutDir:   dir,
	}

	if err := writeQuickstartFiles(t, qc); err != nil {
		t.Fatal(err)
	}

	dc := readFile(t, filepath.Join(dir, "docker-compose.yml"))
	if !strings.Contains(dc, "postgres:16") {
		t.Errorf("expected postgres image in docker-compose.yml:\n%s", dc)
	}
	if !strings.Contains(dc, "wal_level=logical") {
		t.Errorf("expected wal_level=logical in docker-compose.yml:\n%s", dc)
	}
}

func TestRunQuickstart_DetectorType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		db       string
		expected string
	}{
		{"postgres", "wal"},
		{"mysql", "mysql"},
		{"mongodb", "mongodb"},
		{"sqlite", "sqlite"},
		{"unknown", "listen_notify"},
	}

	for _, tt := range tests {
		if got := defaultDetector(tt.db); got != tt.expected {
			t.Errorf("defaultDetector(%q) = %q, want %q", tt.db, got, tt.expected)
		}
	}
}

func TestDetectorsForDB(t *testing.T) {
	t.Parallel()

	pg := detectorsForDB("postgres")
	if len(pg) != 3 || pg[0] != "wal" {
		t.Errorf("detectorsForDB(postgres) = %v", pg)
	}

	my := detectorsForDB("mysql")
	if len(my) != 1 || my[0] != "mysql" {
		t.Errorf("detectorsForDB(mysql) = %v", my)
	}
}

func TestRunQuickstartPrompts(t *testing.T) {
	t.Parallel()

	// Simulate: select postgres (1), wal detector (1), adapters 1 (stdout), confirm docker no (n).
	input := "1\n1\n1\nn\n"
	p := prompt.New(strings.NewReader(input), &strings.Builder{})

	qc := &quickstartConfig{DB: "postgres"}
	if err := runQuickstartPrompts(p, qc); err != nil {
		t.Fatal(err)
	}

	if qc.DB != "postgres" {
		t.Errorf("expected DB 'postgres', got %q", qc.DB)
	}
	if qc.Detector != "wal" {
		t.Errorf("expected detector 'wal', got %q", qc.Detector)
	}
	if len(qc.Adapters) != 1 || qc.Adapters[0] != "stdout" {
		t.Errorf("expected adapter 'stdout', got %v", qc.Adapters)
	}
	if qc.Docker {
		t.Error("expected docker to be false")
	}
}

// writeQuickstartFiles generates the quickstart output files without the cobra command.
func writeQuickstartFiles(t *testing.T, qc quickstartConfig) error {
	t.Helper()

	if err := os.MkdirAll(qc.OutDir, 0o755); err != nil {
		return err
	}

	if err := writeQuickstartFile(filepath.Join(qc.OutDir, "pgcdc.yaml"), qsConfigTemplate, qc); err != nil {
		return err
	}

	if qc.DB == "postgres" {
		if err := writeQuickstartFile(filepath.Join(qc.OutDir, "init.sql"), qsInitSQLTemplate, qc); err != nil {
			return err
		}
	}

	if qc.Docker {
		if err := writeQuickstartFile(filepath.Join(qc.OutDir, "docker-compose.yml"), qsDockerComposeTemplate, qc); err != nil {
			return err
		}
	}

	return nil
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
