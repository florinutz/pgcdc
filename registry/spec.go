package registry

import "fmt"

// ParamSpec describes a single configuration parameter for a connector.
type ParamSpec struct {
	Name        string   // parameter name (e.g., "kafka-brokers")
	Type        string   // "string", "int", "duration", "bool", "[]string", "float64"
	Default     any      // default value
	Required    bool     // whether the parameter is required
	Description string   // human-readable description
	Validations []string // validation rules: "min:1", "max:100", "oneof:a,b,c"
}

// ConnectorSpec describes a connector's full specification.
type ConnectorSpec struct {
	Name        string      // connector name
	Type        string      // "adapter" or "detector"
	Description string      // human-readable description
	Params      []ParamSpec // configuration parameters
}

// GetConnectorSpec looks up the spec for a connector by name, searching adapters
// first, then detectors. Returns nil if not found.
func GetConnectorSpec(name string) *ConnectorSpec {
	if e, ok := GetAdapter(name); ok {
		return &ConnectorSpec{
			Name:        e.Name,
			Type:        "adapter",
			Description: e.Description,
			Params:      e.Spec,
		}
	}
	if e, ok := GetDetector(name); ok {
		return &ConnectorSpec{
			Name:        e.Name,
			Type:        "detector",
			Description: e.Description,
			Params:      e.Spec,
		}
	}
	return nil
}

// FormatDefault returns a human-readable string for a default value.
func FormatDefault(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}
