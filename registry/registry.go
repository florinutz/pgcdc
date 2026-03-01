// Package registry provides a self-registering component registry for adapters,
// detectors, and transforms. Components register via init() in their respective
// cmd/register_*.go files. The registry enables:
//   - Replacing hardcoded switch statements in cmd/listen.go with registry lookups
//   - The `pgcdc list adapters|detectors|transforms` CLI command
//   - Build-tag support via separate register/stub files
package registry

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"go.opentelemetry.io/otel/trace"

	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/adapter/middleware"
	"github.com/florinutz/pgcdc/adapter/sse"
	"github.com/florinutz/pgcdc/adapter/ws"
	"github.com/florinutz/pgcdc/checkpoint"
	"github.com/florinutz/pgcdc/detector"
	"github.com/florinutz/pgcdc/encoding"
	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/transform"
	"github.com/spf13/pflag"
)

// AdapterContext carries shared resources needed by adapter factories.
type AdapterContext struct {
	Cfg          config.Config
	Logger       *slog.Logger
	Ctx          context.Context
	KafkaEncoder encoding.Encoder
	NatsEncoder  encoding.Encoder
}

// AdapterResult is returned by adapter factories.
type AdapterResult struct {
	Adapter          adapter.Adapter
	MiddlewareConfig *middleware.Config // non-nil when the adapter needs CB/RL middleware
	SSEBroker        *sse.Broker        // non-nil for the SSE adapter
	WSBroker         *ws.Broker         // non-nil for the WS adapter
}

// AdapterEntry describes a registered adapter.
type AdapterEntry struct {
	Name        string
	Description string
	Create      func(ctx AdapterContext) (AdapterResult, error)
	BindFlags   func(f *pflag.FlagSet)
	ViperKeys   [][2]string // flag-name → viper-key pairs for PreRunE binding
	Spec        []ParamSpec // typed parameter specifications for describe/validation
}

// DetectorContext carries shared resources for detector factories.
type DetectorContext struct {
	Cfg            config.Config
	Logger         *slog.Logger
	Ctx            context.Context
	TracerProvider trace.TracerProvider // non-nil when OTel tracing is configured
	WasmRT         any                  // opaque wasm runtime; non-nil when plugins are configured
}

// DetectorResult is returned by detector factories.
type DetectorResult struct {
	Detector        detector.Detector
	CheckpointStore checkpoint.Store // non-nil for WAL detector with persistent slot
	Cleanup         func()           // deferred cleanup (e.g. progress store close); may be nil
}

// DetectorEntry describes a registered detector.
type DetectorEntry struct {
	Name        string
	Description string
	Create      func(ctx DetectorContext) (DetectorResult, error)
	BindFlags   func(f *pflag.FlagSet)
	ViperKeys   [][2]string // flag-name → viper-key pairs for validation
	Spec        []ParamSpec // typed parameter specifications for describe/validation
}

// TransformEntry describes a registered transform type.
type TransformEntry struct {
	Name        string
	Description string
	Create      func(spec config.TransformSpec) transform.TransformFunc
}

var (
	mu         sync.RWMutex
	adapters   = make(map[string]AdapterEntry)
	detectors  = make(map[string]DetectorEntry)
	transforms = make(map[string]TransformEntry)
)

// RegisterAdapter adds an adapter factory to the registry.
func RegisterAdapter(e AdapterEntry) {
	mu.Lock()
	defer mu.Unlock()
	adapters[e.Name] = e
}

// RegisterDetector adds a detector factory to the registry.
func RegisterDetector(e DetectorEntry) {
	mu.Lock()
	defer mu.Unlock()
	detectors[e.Name] = e
}

// RegisterTransform adds a transform factory to the registry.
func RegisterTransform(e TransformEntry) {
	mu.Lock()
	defer mu.Unlock()
	transforms[e.Name] = e
}

// GetAdapter returns the adapter entry by name and whether it was found.
func GetAdapter(name string) (AdapterEntry, bool) {
	mu.RLock()
	defer mu.RUnlock()
	e, ok := adapters[name]
	return e, ok
}

// GetDetector returns the detector entry by name and whether it was found.
func GetDetector(name string) (DetectorEntry, bool) {
	mu.RLock()
	defer mu.RUnlock()
	e, ok := detectors[name]
	return e, ok
}

// GetTransform returns the transform entry by name and whether it was found.
func GetTransform(name string) (TransformEntry, bool) {
	mu.RLock()
	defer mu.RUnlock()
	e, ok := transforms[name]
	return e, ok
}

// CreateAdapter creates an adapter by name using the registry.
func CreateAdapter(name string, ctx AdapterContext) (AdapterResult, error) {
	e, ok := GetAdapter(name)
	if !ok {
		return AdapterResult{}, fmt.Errorf("unknown adapter %q", name)
	}
	return e.Create(ctx)
}

// CreateDetector creates a detector by name using the registry.
func CreateDetector(name string, ctx DetectorContext) (DetectorResult, error) {
	e, ok := GetDetector(name)
	if !ok {
		return DetectorResult{}, fmt.Errorf("unknown detector %q", name)
	}
	return e.Create(ctx)
}

// Adapters returns all registered adapter entries, sorted by name.
func Adapters() []AdapterEntry {
	mu.RLock()
	defer mu.RUnlock()
	result := make([]AdapterEntry, 0, len(adapters))
	for _, e := range adapters {
		result = append(result, e)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

// Detectors returns all registered detector entries, sorted by name.
func Detectors() []DetectorEntry {
	mu.RLock()
	defer mu.RUnlock()
	result := make([]DetectorEntry, 0, len(detectors))
	for _, e := range detectors {
		result = append(result, e)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

// Transforms returns all registered transform entries, sorted by name.
func Transforms() []TransformEntry {
	mu.RLock()
	defer mu.RUnlock()
	result := make([]TransformEntry, 0, len(transforms))
	for _, e := range transforms {
		result = append(result, e)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

// AdapterNames returns sorted names of all registered adapters.
func AdapterNames() []string {
	entries := Adapters()
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	return names
}

// DetectorNames returns sorted names of all registered detectors.
func DetectorNames() []string {
	entries := Detectors()
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	return names
}

// BindAdapterFlags calls BindFlags on all registered adapters that provide it.
func BindAdapterFlags(f *pflag.FlagSet) {
	mu.RLock()
	defer mu.RUnlock()
	for _, e := range adapters {
		if e.BindFlags != nil {
			e.BindFlags(f)
		}
	}
}

// BindDetectorFlags calls BindFlags on all registered detectors that provide it.
func BindDetectorFlags(f *pflag.FlagSet) {
	mu.RLock()
	defer mu.RUnlock()
	for _, e := range detectors {
		if e.BindFlags != nil {
			e.BindFlags(f)
		}
	}
}

// BindAdapterViperKeys iterates registered adapters and calls bindFn for each adapter's ViperKeys.
// Designed to be called from bindListenFlags (PreRunE).
func BindAdapterViperKeys(f *pflag.FlagSet, bindFn func(*pflag.FlagSet, [][2]string) error) error {
	mu.RLock()
	defer mu.RUnlock()
	for _, e := range adapters {
		if len(e.ViperKeys) > 0 {
			if err := bindFn(f, e.ViperKeys); err != nil {
				return err
			}
		}
	}
	return nil
}

// SpecToTransform converts a config TransformSpec using registered transform factories.
func SpecToTransform(spec config.TransformSpec) transform.TransformFunc {
	e, ok := GetTransform(spec.Type)
	if !ok {
		return nil
	}
	return e.Create(spec)
}
