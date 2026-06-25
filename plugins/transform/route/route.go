package route

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/deeploop-ai/eventr/internal/basestage"
	"github.com/deeploop-ai/eventr/internal/eql"
	"github.com/deeploop-ai/eventr/internal/message"
	"github.com/deeploop-ai/eventr/internal/registry"
	"github.com/deeploop-ai/eventr/internal/stage"
)

func init() {
	registry.RegisterTransform("route", func(id string, cfg map[string]any) (stage.Transform, error) {
		routes, ok := cfg["routes"].(map[string]any)
		if !ok || len(routes) == 0 {
			return nil, fmt.Errorf("route transform: routes is required")
		}
		compiled := make(map[string]*eql.Program, len(routes))
		for name, expr := range routes {
			s, ok := expr.(string)
			if !ok {
				return nil, fmt.Errorf("route %q: expression must be string", name)
			}
			prg, err := eql.CompileFilter(s)
			if err != nil {
				return nil, fmt.Errorf("route %q: %w", name, err)
			}
			compiled[name] = prg
		}
		return &Transform{
			Base:   basestage.Base{IDVal: id, KindVal: stage.KindTransform, TypeVal: "route"},
			routes: compiled,
		}, nil
	})
}

type Transform struct {
	basestage.Base
	routes map[string]*eql.Program
}

func (t *Transform) Process(ctx context.Context, batch []*message.Message) ([]*message.Message, error) {
	out := make([]*message.Message, 0, len(batch))
	for _, msg := range batch {
		cp := msg.ShallowCopy()
		payload := map[string]any{}
		if cp.ParsedData() != nil {
			if m, ok := cp.ParsedData().(map[string]any); ok {
				payload = m
			}
		} else if len(cp.Payload) > 0 {
			_ = json.Unmarshal(cp.Payload, &payload)
		}
		evalCtx := &eql.EvalContext{
			Msg:     msgAdapter{cp},
			Input:   payload,
			Payload: payload,
			Meta:    cp.Metadata,
		}
		matched := "_default"
		for name, prg := range t.routes {
			if name == "_default" {
				continue
			}
			ok, err := prg.EvalFilter(evalCtx)
			if err != nil {
				return nil, err
			}
			if ok {
				matched = name
				break
			}
		}
		if matched == "_default" {
			if prg, ok := t.routes["_default"]; ok {
				ok, err := prg.EvalFilter(evalCtx)
				if err != nil {
					return nil, err
				}
				if !ok {
					continue
				}
			}
		}
		if cp.Metadata == nil {
			cp.Metadata = make(map[string]any)
		}
		cp.Metadata["er-route"] = matched
		out = append(out, cp)
	}
	return out, nil
}

type msgAdapter struct{ *message.Message }

func (m msgAdapter) EnsureWritable()          { m.Message.EnsureWritable() }
func (m msgAdapter) SetParsedData(v any)      { m.Message.SetParsedData(v) }
func (m msgAdapter) Metadata() map[string]any { return m.Message.Metadata }
