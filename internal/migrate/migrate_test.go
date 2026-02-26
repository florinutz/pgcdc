package migrate

import (
	"testing"
)

func TestMigrationsEmbed(t *testing.T) {
	entries, err := migrations.ReadDir("sql")
	if err != nil {
		t.Fatalf("failed to read embedded sql dir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("no SQL migration files found in embedded FS")
	}

	found := false
	for _, e := range entries {
		if e.Name() == "001_initial.sql" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("001_initial.sql not found in embedded migrations")
	}

	// Verify file content is readable.
	data, err := migrations.ReadFile("sql/001_initial.sql")
	if err != nil {
		t.Fatalf("failed to read 001_initial.sql: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("001_initial.sql is empty")
	}
}

func TestParseMigrationVersion(t *testing.T) {
	tests := []struct {
		name    string
		wantV   int
		wantErr bool
	}{
		{"001_initial.sql", 1, false},
		{"002_add_index.sql", 2, false},
		{"100_big_migration.sql", 100, false},
		{"noseparator.sql", 0, true},
		{"abc_notanumber.sql", 0, true},
	}

	for _, tt := range tests {
		v, err := parseMigrationVersion(tt.name)
		if tt.wantErr {
			if err == nil {
				t.Errorf("parseMigrationVersion(%q): expected error, got %d", tt.name, v)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseMigrationVersion(%q): unexpected error: %v", tt.name, err)
			continue
		}
		if v != tt.wantV {
			t.Errorf("parseMigrationVersion(%q): got %d, want %d", tt.name, v, tt.wantV)
		}
	}
}

func TestListMigrations(t *testing.T) {
	migs, err := listMigrations()
	if err != nil {
		t.Fatalf("listMigrations failed: %v", err)
	}
	if len(migs) == 0 {
		t.Fatal("no migrations found")
	}
	if migs[0].version != 1 {
		t.Fatalf("first migration version = %d, want 1", migs[0].version)
	}
}
