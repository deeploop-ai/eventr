package config

import (
	"fmt"
	"strings"
	"time"

	hoconlib "github.com/gurkankaymak/hocon"
)

func LoadHOCON(path string) (*PipelineConfig, error) {
	conf, err := hoconlib.ParseResource(path)
	if err != nil {
		return nil, fmt.Errorf("hocon parse: %w", err)
	}
	tree, err := ConfigToMap(conf)
	if err != nil {
		return nil, err
	}
	return mapToPipelineConfig(tree)
}

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

func mapToPipelineConfig(tree map[string]any) (*PipelineConfig, error) {
	cfg := &PipelineConfig{}
	if v, ok := tree["apiVersion"].(string); ok {
		cfg.APIVersion = v
	}
	if v, ok := tree["kind"].(string); ok {
		cfg.Kind = v
	}
	if v, ok := tree["metadata"].(map[string]any); ok {
		cfg.Metadata = stringMap(v)
	}
	if v, ok := tree["engine"].(map[string]any); ok {
		cfg.Engine = mapEngine(v)
	}
	if v, ok := tree["edgeDefaults"].(map[string]any); ok {
		cfg.EdgeDefaults = mapEdgeAttrs(v)
	}
	if v, ok := tree["dlq"].(map[string]any); ok {
		cfg.DLQ = mapDLQ(v)
	}
	if v, ok := tree["observability"].(map[string]any); ok {
		cfg.Observability = mapObservability(v)
	}
	if v, ok := tree["steps"].(map[string]any); ok {
		steps, err := mapSteps(v)
		if err != nil {
			return nil, err
		}
		cfg.Steps = steps
	}
	if v, ok := tree["stages"].([]any); ok {
		stages, err := mapStages(v)
		if err != nil {
			return nil, err
		}
		cfg.Stages = stages
	}
	if v, ok := tree["codecs"].([]any); ok {
		cfg.Codecs = mapCodecs(v)
	}
	if v, ok := tree["edges"].([]any); ok {
		cfg.Edges = mapEdges(v)
	}
	return cfg, nil
}

func stringMap(m map[string]any) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = fmt.Sprint(v)
	}
	return out
}

func mapEngine(m map[string]any) EngineConfig {
	return EngineConfig{
		MaxWorkers:   intVal(m["max_workers"]),
		MaxInflight:  intVal(m["max_inflight"]),
		ErrorMode:    strVal(m["error_mode"]),
		DrainTimeout: strVal(m["drain_timeout"]),
	}
}

func mapDLQ(m map[string]any) *DLQConfig {
	return &DLQConfig{
		Sink:                  strVal(m["sink"]),
		IncludeCurrentPayload: boolVal(m["include_current_payload"]),
	}
}

func mapObservability(m map[string]any) ObservabilityConfig {
	var out ObservabilityConfig
	if metrics, ok := m["metrics"].(map[string]any); ok {
		out.Metrics = MetricsConfig{
			Enabled: boolVal(metrics["enabled"]),
			Port:    intVal(metrics["port"]),
			Path:    strVal(metrics["path"]),
		}
	}
	return out
}

func mapSteps(steps map[string]any) (map[string]StepConfig, error) {
	out := make(map[string]StepConfig, len(steps))
	for name, raw := range steps {
		stepMap, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("step %q is not an object", name)
		}
		step, err := mapStep(stepMap)
		if err != nil {
			return nil, fmt.Errorf("step %q: %w", name, err)
		}
		out[name] = step
	}
	return out, nil
}

func mapStep(m map[string]any) (StepConfig, error) {
	step := StepConfig{StepType: strVal(m["step_type"])}
	if deps, ok := m["depends_on"]; ok {
		parsed, err := parseDependsOn(deps)
		if err != nil {
			return step, err
		}
		step.DependsOn = parsed
	}
	if src, ok := m["source"].(map[string]any); ok {
		step.Source = mapSourceBlock(src)
	}
	if tr, ok := m["transform"].(map[string]any); ok {
		step.Transform = mapTransformBlock(tr)
	}
	if sk, ok := m["sink"].(map[string]any); ok {
		step.Sink = mapSinkBlock(sk)
	}
	return step, nil
}

func mapSourceBlock(m map[string]any) *SourceBlock {
	return &SourceBlock{
		Type:    strVal(m["type"]),
		Decoder: mapCodecRef(m["decoder"]),
		Config:  mapAny(m["config"]),
	}
}

func mapTransformBlock(m map[string]any) *TransformBlock {
	return &TransformBlock{
		Type:      strVal(m["type"]),
		Predicate: strVal(m["predicate"]),
		Workers:   intVal(m["workers"]),
		ErrorMode: strVal(m["error_mode"]),
		Config:    mapAny(m["config"]),
	}
}

func mapSinkBlock(m map[string]any) *SinkBlock {
	return &SinkBlock{
		Type:        strVal(m["type"]),
		Encoder:     mapCodecRef(m["encoder"]),
		Batch:       mapBatch(m["batch"]),
		Ordering:    strVal(m["ordering"]),
		MaxInFlight: intVal(m["max_in_flight"]),
		Config:      mapAny(m["config"]),
	}
}

func mapStages(items []any) ([]StageConfig, error) {
	out := make([]StageConfig, 0, len(items))
	for i, raw := range items {
		m, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("stages[%d] is not an object", i)
		}
		st := StageConfig{
			ID:          strVal(m["id"]),
			Kind:        strVal(m["kind"]),
			Type:        strVal(m["type"]),
			Workers:     intVal(m["workers"]),
			Predicate:   strVal(m["predicate"]),
			Ordering:    strVal(m["ordering"]),
			MaxInFlight: intVal(m["max_in_flight"]),
			Decoder:     mapCodecRef(m["decoder"]),
			Encoder:     mapCodecRef(m["encoder"]),
			Batch:       mapBatch(m["batch"]),
			Config:      mapAny(m["config"]),
		}
		if deps, ok := m["depends_on"]; ok {
			parsed, err := parseDependsOn(deps)
			if err != nil {
				return nil, err
			}
			st.DependsOn = parsed
		}
		out = append(out, st)
	}
	return out, nil
}

func parseDependsOn(raw any) (DependsOnList, error) {
	switch v := raw.(type) {
	case []any:
		out := make(DependsOnList, 0, len(v))
		for _, item := range v {
			switch t := item.(type) {
			case string:
				out = append(out, DependsOnEntry{Upstream: t})
			case map[string]any:
				if len(t) != 1 {
					return nil, fmt.Errorf("depends_on sequence item must be single-key object")
				}
				for upstream, attrsRaw := range t {
					attrsMap, _ := attrsRaw.(map[string]any)
					attrs := mapEdgeAttrs(attrsMap)
					out = append(out, DependsOnEntry{Upstream: upstream, Edge: &attrs})
				}
			default:
				return nil, fmt.Errorf("unsupported depends_on list element %T", item)
			}
		}
		return out, nil
	case map[string]any:
		out := make(DependsOnList, 0, len(v))
		for upstream, attrsRaw := range v {
			attrsMap, _ := attrsRaw.(map[string]any)
			attrs := mapEdgeAttrs(attrsMap)
			out = append(out, DependsOnEntry{Upstream: upstream, Edge: &attrs})
		}
		return out, nil
	default:
		return nil, fmt.Errorf("depends_on must be list or map, got %T", raw)
	}
}

func mapEdgeAttrs(m map[string]any) EdgeAttrs {
	if m == nil {
		return EdgeAttrs{}
	}
	var required *bool
	if v, ok := m["required"].(bool); ok {
		required = &v
	}
	return EdgeAttrs{
		Condition: strVal(m["condition"]),
		Route:     strVal(m["route"]),
		Buffer:    mapEdgeBuffer(m["buffer"]),
		Delivery:  mapDelivery(m["delivery"]),
		Required:  required,
	}
}

func mapEdgeBuffer(raw any) *EdgeBufferConfig {
	m, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	return &EdgeBufferConfig{
		Type:             strVal(m["type"]),
		Size:             intVal(m["size"]),
		Strategy:         strVal(m["strategy"]),
		Key:              stringSlice(m["key"]),
		DiskPath:         strVal(m["disk_path"]),
		DiskMaxSize:      int64Val(m["disk_max_size"]),
		DiskSyncInterval: strVal(m["disk_sync_interval"]),
	}
}

func mapDelivery(raw any) *DeliverySpec {
	m, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	var retry *RetryConfig
	if r, ok := m["retry"].(map[string]any); ok {
		retry = &RetryConfig{
			Max:     intVal(r["max"]),
			Backoff: strVal(r["backoff"]),
		}
	}
	return &DeliverySpec{
		Retry:   retry,
		Timeout: strVal(m["timeout"]),
		DLQ:     strVal(m["dlq"]),
	}
}

func mapCodecRef(raw any) *CodecRef {
	if raw == nil {
		return nil
	}
	switch v := raw.(type) {
	case string:
		return &CodecRef{Type: v}
	case map[string]any:
		return &CodecRef{
			Type:   strVal(v["type"]),
			Ref:    strVal(v["ref"]),
			Config: mapAny(v["config"]),
		}
	default:
		return nil
	}
}

func mapBatch(raw any) *BatchConfig {
	m, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	return &BatchConfig{
		Size:     intVal(m["size"]),
		Timeout:  strVal(m["timeout"]),
		MaxBytes: intVal(m["max_bytes"]),
	}
}

func mapCodecs(items []any) []CodecConfig {
	out := make([]CodecConfig, 0, len(items))
	for _, raw := range items {
		m, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		out = append(out, CodecConfig{
			Name:   strVal(m["name"]),
			Type:   strVal(m["type"]),
			Config: mapAny(m["config"]),
		})
	}
	return out
}

func mapEdges(items []any) []EdgeConfig {
	out := make([]EdgeConfig, 0, len(items))
	for _, raw := range items {
		m, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		var required *bool
		if v, ok := m["required"].(bool); ok {
			required = &v
		}
		out = append(out, EdgeConfig{
			From:      strVal(m["from"]),
			To:        strVal(m["to"]),
			Condition: strVal(m["condition"]),
			Route:     strVal(m["route"]),
			Buffer:    mapEdgeBuffer(m["buffer"]),
			Delivery:  mapDelivery(m["delivery"]),
			Required:  required,
		})
	}
	return out
}

func mapAny(m any) map[string]any {
	if m == nil {
		return nil
	}
	src, ok := m.(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]any, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func strVal(v any) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	default:
		return fmt.Sprint(v)
	}
}

func intVal(v any) int {
	switch t := v.(type) {
	case int:
		return t
	case int64:
		return int(t)
	case float64:
		return int(t)
	default:
		return 0
	}
}

func int64Val(v any) int64 {
	switch t := v.(type) {
	case int64:
		return t
	case int:
		return int64(t)
	case float64:
		return int64(t)
	default:
		return 0
	}
}

func boolVal(v any) bool {
	b, _ := v.(bool)
	return b
}

func stringSlice(v any) []string {
	arr, ok := v.([]any)
	if !ok {
		if s, ok := v.(string); ok && s != "" {
			return strings.Split(s, ",")
		}
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, item := range arr {
		out = append(out, strVal(item))
	}
	return out
}
