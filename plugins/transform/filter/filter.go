package filter

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
	registry.RegisterTransform("filter", func(id string, cfg map[string]any) (stage.Transform, error) {
		dsl, _ := cfg["dsl"].(string)
		if dsl == "" {
			return nil, fmt.Errorf("filter transform: dsl is required")
		}
		prg, err := eql.CompileFilter(dsl)
		if err != nil {
			return nil, err
		}
		return &Transform{
			Base: basestage.Base{IDVal: id, KindVal: stage.KindTransform, TypeVal: "filter"},
			prg:  prg,
		}, nil
	})
}

type Transform struct {
	basestage.Base
	prg *eql.Program
}

func (t *Transform) Process(ctx context.Context, batch []*message.Message) ([]*message.Message, error) {
	out := make([]*message.Message, 0, len(batch))
	for _, msg := range batch {
		payload := map[string]any{}
		if msg.ParsedData() != nil {
			if m, ok := msg.ParsedData().(map[string]any); ok {
				payload = m
			}
		} else if len(msg.Payload) > 0 {
			_ = json.Unmarshal(msg.Payload, &payload)
		}
		ok, err := t.prg.EvalFilter(&eql.EvalContext{
			Msg:     msgAdapter{msg},
			Input:   payload,
			Payload: payload,
			Meta:    msg.Metadata,
		})
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, msg)
		}
	}
	return out, nil
}

type msgAdapter struct{ *message.Message }

func (m msgAdapter) EnsureWritable()          { m.Message.EnsureWritable() }
func (m msgAdapter) SetParsedData(v any)      { m.Message.SetParsedData(v) }
func (m msgAdapter) Metadata() map[string]any { return m.Message.Metadata }
