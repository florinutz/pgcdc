package config

import (
	"strings"
	"testing"
)

func validConfig() Config {
	c := Default()
	c.DatabaseURL = "postgres://localhost/test"
	c.Channels = []string{"events"}
	return c
}

func TestValidate_MinimalValid(t *testing.T) {
	c := validConfig()
	if err := c.Validate(); err != nil {
		t.Fatalf("expected valid, got: %v", err)
	}
}

func TestValidate_MissingDatabaseURL(t *testing.T) {
	c := validConfig()
	c.DatabaseURL = ""
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for missing database_url")
	}
	if !strings.Contains(err.Error(), "database_url is required") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_MissingDatabaseURL_MySQL(t *testing.T) {
	c := validConfig()
	c.DatabaseURL = ""
	c.Detector.Type = "mysql"
	c.MySQL.Addr = "localhost:3306"
	c.MySQL.ServerID = 1
	err := c.Validate()
	if err != nil && strings.Contains(err.Error(), "database_url is required") {
		t.Error("mysql detector should not require database_url")
	}
}

func TestValidate_MissingChannels_ListenNotify(t *testing.T) {
	c := validConfig()
	c.Channels = nil
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for missing channels")
	}
	if !strings.Contains(err.Error(), "no channels specified") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_UnknownAdapter(t *testing.T) {
	c := validConfig()
	c.Adapters = []string{"stdout", "foobar"}
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for unknown adapter")
	}
	if !strings.Contains(err.Error(), `unknown adapter "foobar"`) {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_OrphanRoute(t *testing.T) {
	c := validConfig()
	c.Routes = map[string][]string{"webhook": {"events"}}
	// webhook is not in c.Adapters (only stdout)
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for orphan route")
	}
	if !strings.Contains(err.Error(), `route references unknown adapter "webhook"`) {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_WebhookRequiresURL(t *testing.T) {
	c := validConfig()
	c.Adapters = []string{"webhook"}
	c.Webhook.URL = ""
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for missing webhook URL")
	}
	if !strings.Contains(err.Error(), "webhook.url is required") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_WALRequiresPublication(t *testing.T) {
	c := validConfig()
	c.Detector.Type = "wal"
	c.Detector.Publication = ""
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for missing WAL publication")
	}
	if !strings.Contains(err.Error(), "WAL detector requires a publication") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_BackpressureThresholds(t *testing.T) {
	c := validConfig()
	c.Detector.Type = "wal"
	c.Detector.Publication = "pub"
	c.Detector.PersistentSlot = true
	c.Backpressure.Enabled = true
	c.Backpressure.WarnThreshold = 1000
	c.Backpressure.CriticalThreshold = 500 // critical < warn
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for bad thresholds")
	}
	if !strings.Contains(err.Error(), "critical_threshold") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_CooperativeCheckpoint_RequiresPersistentSlot(t *testing.T) {
	c := validConfig()
	c.Detector.Type = "wal"
	c.Detector.Publication = "pub"
	c.Detector.CooperativeCheckpoint = true
	c.Detector.PersistentSlot = false
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "cooperative-checkpoint requires --persistent-slot") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_WALOnlyFeature_OnNonWAL(t *testing.T) {
	c := validConfig()
	c.Detector.Type = "listen_notify"
	c.Detector.TxMetadata = true
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for tx-metadata on non-WAL detector")
	}
	if !strings.Contains(err.Error(), "tx-metadata requires --detector wal") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_FileRequiresPath(t *testing.T) {
	c := validConfig()
	c.Adapters = []string{"file"}
	c.File.Path = ""
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for missing file path")
	}
	if !strings.Contains(err.Error(), "file.path is required") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_GraphQLRequiresKeepalive(t *testing.T) {
	c := validConfig()
	c.Adapters = []string{"graphql"}
	c.GraphQL.KeepaliveInterval = 0
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for zero graphql keepalive_interval")
	}
	if !strings.Contains(err.Error(), "graphql.keepalive_interval") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_ArrowRequiresAddr(t *testing.T) {
	c := validConfig()
	c.Adapters = []string{"arrow"}
	c.Arrow.Addr = ""
	c.Arrow.BufferSize = 0
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for missing arrow addr")
	}
	if !strings.Contains(err.Error(), "arrow.addr") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_DuckDBRequiresFlushSize(t *testing.T) {
	c := validConfig()
	c.Adapters = []string{"duckdb"}
	c.DuckDB.FlushSize = 0
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for zero duckdb flush_size")
	}
	if !strings.Contains(err.Error(), "duckdb.flush_size") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_MultipleErrors(t *testing.T) {
	c := validConfig()
	c.DatabaseURL = ""
	c.Channels = nil
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error")
	}
	// Should contain both errors.
	if !strings.Contains(err.Error(), "database_url") || !strings.Contains(err.Error(), "no channels") {
		t.Errorf("expected multiple errors, got: %v", err)
	}
}
