package registry

import (
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/florinutz/pgcdc/internal/config"
)

// ValidationError describes a single spec-based validation failure.
type ValidationError struct {
	Connector string // adapter or detector name
	Param     string // parameter name
	Rule      string // rule that was violated (e.g., "required", "min:1")
	Message   string // human-readable description
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("%s: %s: %s", e.Connector, e.Param, e.Message)
}

// ValidateConfig validates the given config against all registered ParamSpec declarations.
// It only checks specs for adapters listed in cfg.Adapters and the detector in cfg.Detector.Type.
func ValidateConfig(cfg config.Config) []ValidationError {
	var errs []ValidationError

	// Build a set of active adapters.
	activeAdapters := make(map[string]bool, len(cfg.Adapters))
	for _, a := range cfg.Adapters {
		activeAdapters[a] = true
	}

	// Validate active adapter specs.
	for _, entry := range Adapters() {
		if !activeAdapters[entry.Name] {
			continue
		}
		if len(entry.Spec) == 0 {
			continue
		}
		viperMap := buildViperMap(entry.ViperKeys)
		for _, spec := range entry.Spec {
			errs = append(errs, validateParam(cfg, entry.Name, spec, viperMap)...)
		}
	}

	// Validate the active detector specs.
	detectorType := cfg.Detector.Type
	if detectorType == "" {
		detectorType = "listen_notify"
	}
	if entry, ok := GetDetector(detectorType); ok && len(entry.Spec) > 0 {
		viperMap := buildViperMap(entry.ViperKeys)
		for _, spec := range entry.Spec {
			errs = append(errs, validateParam(cfg, entry.Name, spec, viperMap)...)
		}
	}

	return errs
}

// buildViperMap converts ViperKeys ([][2]string) into a map from flag name to viper config path.
func buildViperMap(keys [][2]string) map[string]string {
	m := make(map[string]string, len(keys))
	for _, kv := range keys {
		m[kv[0]] = kv[1]
	}
	return m
}

// validateParam checks a single ParamSpec against the config, returning any validation errors.
func validateParam(cfg config.Config, connector string, spec ParamSpec, viperMap map[string]string) []ValidationError {
	// Resolve the config path for this param.
	viperKey, ok := viperMap[spec.Name]
	if !ok {
		// No viper key mapping — skip validation for this param.
		return nil
	}

	val := resolveConfigValue(cfg, viperKey)

	var errs []ValidationError

	// Check Required from ParamSpec.Required field.
	if spec.Required {
		if isEmpty(val) {
			errs = append(errs, ValidationError{
				Connector: connector,
				Param:     spec.Name,
				Rule:      "required",
				Message:   fmt.Sprintf("%s is required", spec.Name),
			})
			return errs // no point checking further rules
		}
	}

	// Parse and apply validation rules from Validations strings.
	for _, rule := range spec.Validations {
		if verr := applyRule(connector, spec, val, rule); verr != nil {
			errs = append(errs, *verr)
		}
	}

	return errs
}

// applyRule applies a single validation rule string to a value.
func applyRule(connector string, spec ParamSpec, val any, rule string) *ValidationError {
	parts := strings.SplitN(rule, ":", 2)
	ruleName := parts[0]
	ruleArg := ""
	if len(parts) == 2 {
		ruleArg = parts[1]
	}

	switch ruleName {
	case "required":
		if isEmpty(val) {
			return &ValidationError{
				Connector: connector,
				Param:     spec.Name,
				Rule:      rule,
				Message:   fmt.Sprintf("%s is required", spec.Name),
			}
		}

	case "min":
		n, err := strconv.ParseFloat(ruleArg, 64)
		if err != nil {
			return nil
		}
		num, ok := toFloat64(val)
		if !ok {
			// For slices, check length.
			if length, ok := sliceLen(val); ok {
				if float64(length) < n {
					return &ValidationError{
						Connector: connector,
						Param:     spec.Name,
						Rule:      rule,
						Message:   fmt.Sprintf("%s must have at least %s element(s), got %d", spec.Name, ruleArg, length),
					}
				}
			}
			return nil
		}
		if num < n {
			return &ValidationError{
				Connector: connector,
				Param:     spec.Name,
				Rule:      rule,
				Message:   fmt.Sprintf("%s must be >= %s, got %v", spec.Name, ruleArg, val),
			}
		}

	case "max":
		n, err := strconv.ParseFloat(ruleArg, 64)
		if err != nil {
			return nil
		}
		num, ok := toFloat64(val)
		if !ok {
			return nil
		}
		if num > n {
			return &ValidationError{
				Connector: connector,
				Param:     spec.Name,
				Rule:      rule,
				Message:   fmt.Sprintf("%s must be <= %s, got %v", spec.Name, ruleArg, val),
			}
		}

	case "oneof":
		allowed := strings.Split(ruleArg, ",")
		s := fmt.Sprintf("%v", val)
		if s == "" {
			return nil // empty is valid (unless also required)
		}
		found := false
		for _, a := range allowed {
			if s == a {
				found = true
				break
			}
		}
		if !found {
			return &ValidationError{
				Connector: connector,
				Param:     spec.Name,
				Rule:      rule,
				Message:   fmt.Sprintf("%s must be one of [%s], got %q", spec.Name, ruleArg, s),
			}
		}

	case "url":
		s := fmt.Sprintf("%v", val)
		if s == "" {
			return nil
		}
		if _, err := url.ParseRequestURI(s); err != nil {
			return &ValidationError{
				Connector: connector,
				Param:     spec.Name,
				Rule:      rule,
				Message:   fmt.Sprintf("%s must be a valid URL, got %q", spec.Name, s),
			}
		}

	case "duration":
		if d, ok := val.(time.Duration); ok {
			if d <= 0 {
				return &ValidationError{
					Connector: connector,
					Param:     spec.Name,
					Rule:      rule,
					Message:   fmt.Sprintf("%s must be a positive duration, got %v", spec.Name, d),
				}
			}
		}
	}

	return nil
}

// resolveConfigValue navigates the config struct using a dot-separated viper path.
// For example, "kafka.brokers" resolves to cfg.Kafka.Brokers.
func resolveConfigValue(cfg config.Config, path string) any {
	segments := strings.Split(path, ".")
	v := reflect.ValueOf(cfg)

	for _, seg := range segments {
		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				return nil
			}
			v = v.Elem()
		}
		if v.Kind() != reflect.Struct {
			return nil
		}
		v = findFieldByMapstructure(v, seg)
		if !v.IsValid() {
			return nil
		}
	}

	return v.Interface()
}

// findFieldByMapstructure finds a struct field whose mapstructure tag matches the given name.
func findFieldByMapstructure(v reflect.Value, tag string) reflect.Value {
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		ms := field.Tag.Get("mapstructure")
		if ms == tag {
			return v.Field(i)
		}
	}
	return reflect.Value{}
}

// isEmpty returns true if the value is the zero value or an empty string/slice.
func isEmpty(val any) bool {
	if val == nil {
		return true
	}
	v := reflect.ValueOf(val)
	switch v.Kind() {
	case reflect.String:
		return v.String() == ""
	case reflect.Slice:
		return v.Len() == 0
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Bool:
		return !v.Bool()
	default:
		return false
	}
}

// toFloat64 converts numeric types to float64.
func toFloat64(val any) (float64, bool) {
	if val == nil {
		return 0, false
	}
	v := reflect.ValueOf(val)
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(v.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(v.Uint()), true
	case reflect.Float32, reflect.Float64:
		return v.Float(), true
	default:
		return 0, false
	}
}

// sliceLen returns the length of a slice value and true, or 0 and false if not a slice.
func sliceLen(val any) (int, bool) {
	if val == nil {
		return 0, false
	}
	v := reflect.ValueOf(val)
	if v.Kind() == reflect.Slice {
		return v.Len(), true
	}
	return 0, false
}
