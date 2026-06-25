package engine

import (
	"context"
	"fmt"
	"sync"

	"github.com/deeploop-ai/eventr/internal/registry"
	"github.com/deeploop-ai/eventr/internal/topology"
)

type Engine struct {
	reg       *registry.Registry
	pipelines map[string]*Pipeline
	mu        sync.Mutex
}

func New(reg *registry.Registry) *Engine {
	if reg == nil {
		reg = registry.Default
	}
	return &Engine{
		reg:       reg,
		pipelines: make(map[string]*Pipeline),
	}
}

func (e *Engine) Load(ctx context.Context, ir *topology.TopologyIR) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.pipelines[ir.Name]; exists {
		return fmt.Errorf("pipeline %q already loaded", ir.Name)
	}
	p, err := NewPipeline(ctx, e.reg, ir)
	if err != nil {
		return err
	}
	e.pipelines[ir.Name] = p
	return nil
}

func (e *Engine) Start(ctx context.Context) error {
	e.mu.Lock()
	pipes := make([]*Pipeline, 0, len(e.pipelines))
	for _, p := range e.pipelines {
		pipes = append(pipes, p)
	}
	e.mu.Unlock()
	for _, p := range pipes {
		if err := p.Start(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) Stop(ctx context.Context) error {
	e.mu.Lock()
	pipes := make([]*Pipeline, 0, len(e.pipelines))
	for _, p := range e.pipelines {
		pipes = append(pipes, p)
	}
	e.mu.Unlock()
	var first error
	for _, p := range pipes {
		if err := p.Stop(ctx); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (e *Engine) PipelineCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.pipelines)
}
