package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Plugin scaffold tests use os.Chdir so they must NOT run in parallel.

func TestRunPluginInit_Transform(t *testing.T) {
	testPluginScaffold(t, "myplugin", "transform", []string{
		"//export transform",
	})
}

func TestRunPluginInit_Adapter(t *testing.T) {
	testPluginScaffold(t, "myadapter", "adapter", []string{
		"//export init_adapter",
		"//export deliver",
		"//export close_adapter",
	})
}

func TestRunPluginInit_DLQ(t *testing.T) {
	testPluginScaffold(t, "mydlq", "dlq", []string{
		"//export init_dlq",
		"//export record",
		"//export close_dlq",
	})
}

func TestRunPluginInit_Checkpoint(t *testing.T) {
	testPluginScaffold(t, "myckpt", "checkpoint", []string{
		"//export init_checkpoint",
		"//export save",
		"//export load",
		"//export close_checkpoint",
	})
}

func testPluginScaffold(t *testing.T, name, pluginType string, expectedExports []string) {
	t.Helper()

	dir := t.TempDir()

	origDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chdir(origDir) }()

	cmd := pluginInitCmd
	cmd.SetArgs([]string{name})
	_ = cmd.Flags().Set("type", pluginType)
	cmd.SetOut(&strings.Builder{})

	if err := cmd.RunE(cmd, []string{name}); err != nil {
		t.Fatalf("runPluginInit(%s, %s): %v", name, pluginType, err)
	}

	// Check files exist.
	for _, f := range []string{"main.go", "go.mod", "Makefile", "README.md"} {
		path := filepath.Join(dir, name, f)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("expected file %s to exist", f)
		}
	}

	// Check main.go has expected exports.
	mainContent, err := os.ReadFile(filepath.Join(dir, name, "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, export := range expectedExports {
		if !strings.Contains(string(mainContent), export) {
			t.Errorf("main.go missing export %q for type %s", export, pluginType)
		}
	}

	// Check go.mod has module name.
	gomod, err := os.ReadFile(filepath.Join(dir, name, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(gomod), "module "+name) {
		t.Errorf("go.mod missing module name %q", name)
	}

	// Check README mentions the type.
	readme, err := os.ReadFile(filepath.Join(dir, name, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(readme), pluginType) {
		t.Errorf("README.md missing plugin type %q", pluginType)
	}
}

func TestRunPluginInit_InvalidName(t *testing.T) {
	cmd := pluginInitCmd
	_ = cmd.Flags().Set("type", "transform")

	err := cmd.RunE(cmd, []string{"123invalid"})
	if err == nil {
		t.Fatal("expected error for invalid plugin name")
	}
	if !strings.Contains(err.Error(), "invalid plugin name") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunPluginInit_InvalidType(t *testing.T) {
	cmd := pluginInitCmd
	_ = cmd.Flags().Set("type", "bogus")

	err := cmd.RunE(cmd, []string{"validname"})
	if err == nil {
		t.Fatal("expected error for invalid plugin type")
	}
	if !strings.Contains(err.Error(), "invalid plugin type") {
		t.Errorf("unexpected error: %v", err)
	}
}
