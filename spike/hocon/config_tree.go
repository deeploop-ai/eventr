package configspike

import (
	"fmt"
	"strings"
	"time"

	hoconlib "github.com/gurkankaymak/hocon"
)

// ConfigToMap converts a resolved HOCON Config tree into map[string]any for
// PipelineConfig normalization. Spike only — production loader lives in internal/config.
func ConfigToMap(conf *hoconlib.Config) (map[string]any, error) {
	root := conf.GetRoot()
	if root == nil {
		return nil, fmt.Errorf("empty config root")
	}
	out, ok := valueToAny(root).(map[string]any)
	if !ok {
		return nil, fmt.Errorf("root is not an object")
	}
	return out, nil
}

func valueToAny(v hoconlib.Value) any {
	if v == nil {
		return nil
	}
	switch v.Type() {
	case hoconlib.ObjectType:
		obj := v.(hoconlib.Object)
		m := make(map[string]any, len(obj))
		for k, child := range obj {
			m[k] = valueToAny(child)
		}
		return m
	case hoconlib.ArrayType:
		arr := v.(hoconlib.Array)
		slice := make([]any, len(arr))
		for i, child := range arr {
			slice[i] = valueToAny(child)
		}
		return slice
	case hoconlib.StringType:
		if d, ok := v.(hoconlib.Duration); ok {
			return time.Duration(d).String()
		}
		return string(v.(hoconlib.String))
	case hoconlib.BooleanType:
		return bool(v.(hoconlib.Boolean))
	case hoconlib.NumberType:
		switch n := v.(type) {
		case hoconlib.Int:
			return int(n)
		case hoconlib.Float32:
			return float32(n)
		case hoconlib.Float64:
			return float64(n)
		default:
			return v.String()
		}
	case hoconlib.NullType:
		return nil
	default:
		if d, ok := v.(hoconlib.Duration); ok {
			return time.Duration(d).String()
		}
		return v.String()
	}
}

// stepNames returns step names under steps.{name}.
func StepNames(tree map[string]any) ([]string, error) {
	steps, ok := tree["steps"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("steps is not an object")
	}
	names := make([]string, 0, len(steps))
	for name := range steps {
		names = append(names, name)
	}
	return names, nil
}

// routeFromDependsOn reads depends_on.splitter.route from a step object.
func RouteFromDependsOn(step map[string]any, upstream string) (string, error) {
	dep, ok := step["depends_on"].(map[string]any)
	if !ok {
		return "", fmt.Errorf("depends_on is not a map")
	}
	up, ok := dep[upstream].(map[string]any)
	if !ok {
		return "", fmt.Errorf("upstream %q not found in depends_on", upstream)
	}
	route, ok := up["route"].(string)
	if !ok {
		return "", fmt.Errorf("route not a string")
	}
	return route, nil
}

func EnvSubstitutedBrokers(conf *hoconlib.Config, want string) error {
	brokers := conf.GetStringSlice("steps.kafka-source.source.config.brokers")
	if len(brokers) == 0 {
		return fmt.Errorf("brokers missing")
	}
	if strings.Trim(brokers[0], `"`) != want {
		return fmt.Errorf("brokers[0] = %q, want %q", brokers[0], want)
	}
	return nil
}

func OptionalTagAbsent(conf *hoconlib.Config) error {
	if conf.Get("steps.kafka-sink.sink.config.optional_tag") != nil {
		return fmt.Errorf("optional_tag should be absent when env unset")
	}
	return nil
}

func BatchTimeout(tree map[string]any) (time.Duration, error) {
	steps := tree["steps"].(map[string]any)
	sinkStep := steps["kafka-sink"].(map[string]any)
	sink := sinkStep["sink"].(map[string]any)
	batch, ok := sink["batch"].(map[string]any)
	if !ok {
		return 0, fmt.Errorf("batch missing")
	}
	raw, ok := batch["timeout"].(string)
	if !ok {
		return 0, fmt.Errorf("timeout not string: %T", batch["timeout"])
	}
	return time.ParseDuration(raw)
}
