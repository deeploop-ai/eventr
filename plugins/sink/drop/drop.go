package drop

import (
	"context"

	"github.com/deeploop-ai/eventr/internal/basestage"
	"github.com/deeploop-ai/eventr/internal/message"
	"github.com/deeploop-ai/eventr/internal/registry"
	"github.com/deeploop-ai/eventr/internal/stage"
)

func init() {
	registry.RegisterSink("drop", func(id string, cfg map[string]any) (stage.Sink, error) {
		return &Sink{
			Base: basestage.Base{IDVal: id, KindVal: stage.KindSink, TypeVal: "drop"},
		}, nil
	})
}

type Sink struct {
	basestage.Base
}

func (s *Sink) Write(context.Context, []*message.Message) error { return nil }
func (s *Sink) Flush(context.Context) error                     { return nil }
