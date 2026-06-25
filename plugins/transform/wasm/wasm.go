package wasm

import (
	"fmt"

	"github.com/deeploop-ai/eventr/internal/registry"
	"github.com/deeploop-ai/eventr/internal/stage"
)

func init() {
	registry.RegisterTransform("wasm", func(id string, cfg map[string]any) (stage.Transform, error) {
		return nil, fmt.Errorf("wasm transform: not implemented in v2.0-alpha")
	})
}
