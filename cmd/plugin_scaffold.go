package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"text/template"

	"github.com/spf13/cobra"
)

var pluginCmd = &cobra.Command{
	Use:   "plugin",
	Short: "Plugin management commands",
}

var pluginInitCmd = &cobra.Command{
	Use:   "init <name>",
	Short: "Scaffold a new pgcdc Wasm plugin project",
	Long: `Generates a ready-to-build Wasm plugin project with main.go, go.mod,
Makefile, and README. Requires TinyGo to build.`,
	Args: cobra.ExactArgs(1),
	RunE: runPluginInit,
}

var validPluginName = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_-]*$`)

func init() {
	rootCmd.AddCommand(pluginCmd)
	pluginCmd.AddCommand(pluginInitCmd)

	pluginInitCmd.Flags().String("type", "transform", "plugin type: transform, adapter, dlq, checkpoint")
}

type pluginTemplateData struct {
	Name       string
	Type       string
	ModuleName string
}

func runPluginInit(cmd *cobra.Command, args []string) error {
	name := args[0]
	pluginType, _ := cmd.Flags().GetString("type")

	if !validPluginName.MatchString(name) {
		return fmt.Errorf("invalid plugin name %q: must start with a letter and contain only alphanumeric characters, underscores, and hyphens", name)
	}

	validTypes := map[string]bool{"transform": true, "adapter": true, "dlq": true, "checkpoint": true}
	if !validTypes[pluginType] {
		return fmt.Errorf("invalid plugin type %q: expected transform, adapter, dlq, or checkpoint", pluginType)
	}

	data := pluginTemplateData{
		Name:       name,
		Type:       pluginType,
		ModuleName: name,
	}

	dir := name
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create directory %s: %w", dir, err)
	}

	// Render main.go.
	mainTmplStr, ok := pluginMainTemplates[pluginType]
	if !ok {
		return fmt.Errorf("no template for plugin type %q", pluginType)
	}
	if err := renderTemplate(filepath.Join(dir, "main.go"), mainTmplStr, data); err != nil {
		return err
	}

	// Render go.mod.
	if err := renderTemplate(filepath.Join(dir, "go.mod"), pluginGoModTemplate, data); err != nil {
		return err
	}

	// Render Makefile.
	if err := renderTemplate(filepath.Join(dir, "Makefile"), pluginMakefileTemplate, data); err != nil {
		return err
	}

	// Render README.md.
	if err := renderTemplate(filepath.Join(dir, "README.md"), pluginReadmeTemplate, data); err != nil {
		return err
	}

	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Plugin scaffolded in ./%s/\n", name)
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  cd %s && make build\n", name)
	return nil
}

func renderTemplate(path, tmplStr string, data pluginTemplateData) error {
	tmpl, err := template.New(filepath.Base(path)).Parse(tmplStr)
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

var pluginMainTemplates = map[string]string{
	"transform":  pluginTransformMainTemplate,
	"adapter":    pluginAdapterMainTemplate,
	"dlq":        pluginDLQMainTemplate,
	"checkpoint": pluginCheckpointMainTemplate,
}

const pluginTransformMainTemplate = `package main

import (
	"github.com/extism/go-pdk"
)

//export transform
func transform() int32 {
	input := pdk.Input()
	// TODO: modify the event JSON
	pdk.Output(input)
	return 0
}

func main() {}
`

const pluginAdapterMainTemplate = `package main

import (
	"github.com/extism/go-pdk"
)

//export init_adapter
func initAdapter() int32 {
	// config := pdk.Input()
	// TODO: initialize adapter with config
	return 0
}

//export deliver
func deliver() int32 {
	input := pdk.Input()
	// TODO: deliver the event
	_ = input
	return 0
}

//export close_adapter
func closeAdapter() int32 {
	return 0
}

func main() {}
`

const pluginDLQMainTemplate = `package main

import (
	"github.com/extism/go-pdk"
)

//export init_dlq
func initDLQ() int32 {
	return 0
}

//export record
func record() int32 {
	input := pdk.Input()
	_ = input
	return 0
}

//export close_dlq
func closeDLQ() int32 {
	return 0
}

func main() {}
`

const pluginCheckpointMainTemplate = `package main

import (
	"github.com/extism/go-pdk"
)

//export init_checkpoint
func initCheckpoint() int32 {
	return 0
}

//export save
func save() int32 {
	input := pdk.Input()
	_ = input
	return 0
}

//export load
func load() int32 {
	pdk.Output([]byte{})
	return 0
}

//export close_checkpoint
func closeCheckpoint() int32 {
	return 0
}

func main() {}
`

const pluginGoModTemplate = `module {{.ModuleName}}

go 1.25

require github.com/extism/go-pdk v1.1.1
`

const pluginMakefileTemplate = `.PHONY: build clean

build:
	tinygo build -o {{.Name}}.wasm -target wasi main.go

clean:
	rm -f {{.Name}}.wasm
`

const pluginReadmeTemplate = `# {{.Name}}

A pgcdc {{.Type}} plugin.

## Build

Requires [TinyGo](https://tinygo.org/getting-started/install/).

    make build

## Usage

Add to your pgcdc.yaml:

    plugins:
{{- if eq .Type "transform"}}
      transforms:
        - path: ./{{.Name}}/{{.Name}}.wasm
{{- else if eq .Type "adapter"}}
      adapters:
        - path: ./{{.Name}}/{{.Name}}.wasm
          name: {{.Name}}
{{- else if eq .Type "dlq"}}
      dlq:
        path: ./{{.Name}}/{{.Name}}.wasm
{{- else if eq .Type "checkpoint"}}
      checkpoint:
        path: ./{{.Name}}/{{.Name}}.wasm
{{- end}}
`
