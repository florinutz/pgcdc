package registry

import (
	"fmt"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/internal/config"
)

func TestValidateParam_Required(t *testing.T) {
	spec := ParamSpec{
		Name:     "test-url",
		Type:     "string",
		Required: true,
	}
	viperMap := map[string]string{"test-url": "webhook.url"}

	// Empty string triggers required failure.
	cfg := config.Default()
	cfg.Webhook.URL = ""
	errs := validateParam(cfg, "webhook", spec, viperMap)
	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}
	if errs[0].Rule != "required" {
		t.Errorf("expected rule 'required', got %q", errs[0].Rule)
	}

	// Non-empty string passes.
	cfg.Webhook.URL = "http://example.com"
	errs = validateParam(cfg, "webhook", spec, viperMap)
	if len(errs) != 0 {
		t.Fatalf("expected 0 errors, got %d: %v", len(errs), errs)
	}
}

func TestValidateParam_MinMax(t *testing.T) {
	spec := ParamSpec{
		Name:        "retries",
		Type:        "int",
		Validations: []string{"min:0", "max:50"},
	}
	viperMap := map[string]string{"retries": "webhook.max_retries"}

	// In range is fine.
	cfg := config.Default()
	cfg.Webhook.MaxRetries = 5
	errs := validateParam(cfg, "webhook", spec, viperMap)
	if len(errs) != 0 {
		t.Fatalf("expected 0 errors, got %d: %v", len(errs), errs)
	}

	// Over max.
	cfg.Webhook.MaxRetries = 100
	errs = validateParam(cfg, "webhook", spec, viperMap)
	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d: %v", len(errs), errs)
	}
	if errs[0].Rule != "max:50" {
		t.Errorf("expected rule 'max:50', got %q", errs[0].Rule)
	}
}

func TestValidateParam_MinSlice(t *testing.T) {
	spec := ParamSpec{
		Name:        "kafka-brokers",
		Type:        "[]string",
		Required:    true,
		Validations: []string{"min:1"},
	}
	viperMap := map[string]string{"kafka-brokers": "kafka.brokers"}

	// Non-empty slice passes.
	cfg := config.Default()
	cfg.Kafka.Brokers = []string{"localhost:9092"}
	errs := validateParam(cfg, "kafka", spec, viperMap)
	if len(errs) != 0 {
		t.Fatalf("expected 0 errors, got %d: %v", len(errs), errs)
	}

	// Empty slice triggers required (before min check).
	cfg.Kafka.Brokers = []string{}
	errs = validateParam(cfg, "kafka", spec, viperMap)
	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d: %v", len(errs), errs)
	}
	if errs[0].Rule != "required" {
		t.Errorf("expected rule 'required', got %q", errs[0].Rule)
	}
}

func TestValidateParam_OneOf(t *testing.T) {
	spec := ParamSpec{
		Name:        "mysql-flavor",
		Type:        "string",
		Validations: []string{"oneof:mysql,mariadb"},
	}
	viperMap := map[string]string{"mysql-flavor": "mysql.flavor"}

	// Valid value.
	cfg := config.Default()
	cfg.MySQL.Flavor = "mysql"
	errs := validateParam(cfg, "mysql", spec, viperMap)
	if len(errs) != 0 {
		t.Fatalf("expected 0 errors, got %d: %v", len(errs), errs)
	}

	// Invalid value.
	cfg.MySQL.Flavor = "postgres"
	errs = validateParam(cfg, "mysql", spec, viperMap)
	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d: %v", len(errs), errs)
	}
	if errs[0].Rule != "oneof:mysql,mariadb" {
		t.Errorf("expected rule 'oneof:mysql,mariadb', got %q", errs[0].Rule)
	}

	// Empty value skipped (unless also required).
	cfg.MySQL.Flavor = ""
	errs = validateParam(cfg, "mysql", spec, viperMap)
	if len(errs) != 0 {
		t.Fatalf("expected 0 errors for empty oneof, got %d: %v", len(errs), errs)
	}
}

func TestValidateParam_URL(t *testing.T) {
	spec := ParamSpec{
		Name:        "test-url",
		Type:        "string",
		Validations: []string{"url"},
	}
	viperMap := map[string]string{"test-url": "webhook.url"}

	cfg := config.Default()

	// Valid URL.
	cfg.Webhook.URL = "https://example.com/hook"
	errs := validateParam(cfg, "webhook", spec, viperMap)
	if len(errs) != 0 {
		t.Fatalf("expected 0 errors, got %d: %v", len(errs), errs)
	}

	// Invalid URL.
	cfg.Webhook.URL = "not a url"
	errs = validateParam(cfg, "webhook", spec, viperMap)
	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d: %v", len(errs), errs)
	}
	if errs[0].Rule != "url" {
		t.Errorf("expected rule 'url', got %q", errs[0].Rule)
	}

	// Empty URL skipped.
	cfg.Webhook.URL = ""
	errs = validateParam(cfg, "webhook", spec, viperMap)
	if len(errs) != 0 {
		t.Fatalf("expected 0 errors for empty url, got %d: %v", len(errs), errs)
	}
}

func TestValidateParam_Duration(t *testing.T) {
	spec := ParamSpec{
		Name:        "test-dur",
		Type:        "duration",
		Validations: []string{"duration"},
	}
	viperMap := map[string]string{"test-dur": "webhook.timeout"}

	cfg := config.Default()

	// Positive duration passes.
	cfg.Webhook.Timeout = 10 * time.Second
	errs := validateParam(cfg, "webhook", spec, viperMap)
	if len(errs) != 0 {
		t.Fatalf("expected 0 errors, got %d: %v", len(errs), errs)
	}

	// Zero duration fails.
	cfg.Webhook.Timeout = 0
	errs = validateParam(cfg, "webhook", spec, viperMap)
	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d: %v", len(errs), errs)
	}
	if errs[0].Rule != "duration" {
		t.Errorf("expected rule 'duration', got %q", errs[0].Rule)
	}
}

func TestValidateParam_NoViperKey(t *testing.T) {
	spec := ParamSpec{
		Name:        "unknown-param",
		Type:        "string",
		Required:    true,
		Validations: []string{"min:1"},
	}
	viperMap := map[string]string{} // empty

	cfg := config.Default()
	errs := validateParam(cfg, "test", spec, viperMap)
	if len(errs) != 0 {
		t.Fatalf("expected 0 errors for missing viper key, got %d", len(errs))
	}
}

func TestResolveConfigValue(t *testing.T) {
	cfg := config.Default()
	cfg.Kafka.Brokers = []string{"a:9092", "b:9092"}
	cfg.Webhook.URL = "http://test.com"
	cfg.MySQL.ServerID = 42

	tests := []struct {
		path string
		want string
	}{
		{"kafka.brokers", "[a:9092 b:9092]"},
		{"webhook.url", "http://test.com"},
		{"mysql.server_id", "42"},
	}

	for _, tt := range tests {
		val := resolveConfigValue(cfg, tt.path)
		got := fmt.Sprintf("%v", val)
		if got != tt.want {
			t.Errorf("resolveConfigValue(%q) = %q, want %q", tt.path, got, tt.want)
		}
	}
}

func TestResolveConfigValue_InvalidPath(t *testing.T) {
	cfg := config.Default()
	val := resolveConfigValue(cfg, "nonexistent.field")
	if val != nil {
		t.Errorf("expected nil for invalid path, got %v", val)
	}
}

func TestIsEmpty(t *testing.T) {
	tests := []struct {
		name string
		val  any
		want bool
	}{
		{"nil", nil, true},
		{"empty string", "", true},
		{"non-empty string", "hello", false},
		{"zero int", 0, true},
		{"non-zero int", 42, false},
		{"empty slice", []string{}, true},
		{"non-empty slice", []string{"a"}, false},
		{"zero float", 0.0, true},
		{"non-zero float", 1.5, false},
		{"false bool", false, true},
		{"true bool", true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isEmpty(tt.val)
			if got != tt.want {
				t.Errorf("isEmpty(%v) = %v, want %v", tt.val, got, tt.want)
			}
		})
	}
}

func TestToFloat64(t *testing.T) {
	tests := []struct {
		name   string
		val    any
		want   float64
		wantOK bool
	}{
		{"int", 42, 42.0, true},
		{"float64", 3.14, 3.14, true},
		{"uint32", uint32(10), 10.0, true},
		{"string", "hello", 0, false},
		{"nil", nil, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := toFloat64(tt.val)
			if ok != tt.wantOK {
				t.Errorf("toFloat64(%v) ok = %v, want %v", tt.val, ok, tt.wantOK)
			}
			if ok && got != tt.want {
				t.Errorf("toFloat64(%v) = %v, want %v", tt.val, got, tt.want)
			}
		})
	}
}

func TestValidationError_Error(t *testing.T) {
	e := ValidationError{
		Connector: "kafka",
		Param:     "kafka-brokers",
		Rule:      "min:1",
		Message:   "kafka-brokers must have at least 1 element(s), got 0",
	}
	got := e.Error()
	want := "kafka: kafka-brokers: kafka-brokers must have at least 1 element(s), got 0"
	if got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

func TestValidateConfig_Integration(t *testing.T) {
	// Register test fixtures. Use real registry entries already present
	// from the init() calls in register_*.go (imported by cmd package).
	// Since we're in the registry package, we must register our own test entries.

	// Clean test: register a simple adapter with specs.
	RegisterAdapter(AdapterEntry{
		Name:        "test-adapter",
		Description: "test",
		ViperKeys: [][2]string{
			{"test-val", "webhook.max_retries"},
		},
		Spec: []ParamSpec{
			{
				Name:        "test-val",
				Type:        "int",
				Validations: []string{"min:1", "max:10"},
			},
		},
	})
	defer func() {
		mu.Lock()
		delete(adapters, "test-adapter")
		mu.Unlock()
	}()

	cfg := config.Default()
	cfg.Adapters = []string{"test-adapter"}
	cfg.Webhook.MaxRetries = 5

	// Should pass.
	errs := ValidateConfig(cfg)
	if len(errs) != 0 {
		t.Fatalf("expected 0 errors, got %d: %v", len(errs), errs)
	}

	// Should fail (over max).
	cfg.Webhook.MaxRetries = 20
	errs = ValidateConfig(cfg)
	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d: %v", len(errs), errs)
	}
	if errs[0].Connector != "test-adapter" {
		t.Errorf("expected connector 'test-adapter', got %q", errs[0].Connector)
	}
}

func TestValidateConfig_SkipsInactiveAdapters(t *testing.T) {
	RegisterAdapter(AdapterEntry{
		Name:        "test-inactive",
		Description: "test",
		ViperKeys: [][2]string{
			{"test-req", "webhook.url"},
		},
		Spec: []ParamSpec{
			{
				Name:     "test-req",
				Type:     "string",
				Required: true,
			},
		},
	})
	defer func() {
		mu.Lock()
		delete(adapters, "test-inactive")
		mu.Unlock()
	}()

	cfg := config.Default()
	cfg.Adapters = []string{"stdout"} // not test-inactive
	cfg.Webhook.URL = ""              // would fail if checked

	errs := ValidateConfig(cfg)
	for _, e := range errs {
		if e.Connector == "test-inactive" {
			t.Fatalf("should not validate inactive adapter, got error: %v", e)
		}
	}
}
