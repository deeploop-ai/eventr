package engine

import (
	"context"
	"fmt"
	"sync"

	"github.com/deeploop-ai/eventr/internal/config"
	"github.com/deeploop-ai/eventr/internal/observability"
	"github.com/deeploop-ai/eventr/internal/registry"
	"github.com/deeploop-ai/eventr/internal/topology"
)

type Engine struct {
	reg       *registry.Registry
	pipelines map[string]*Pipeline
	metrics   *observability.Metrics
	obs       *observability.Server
	mu        sync.Mutex
}

func New(reg *registry.Registry) *Engine {
	if reg == nil {
		reg = registry.Default
	}
	return &Engine{
		reg:       reg,
		pipelines: make(map[string]*Pipeline),
		metrics:   observability.NewMetrics(nil),
	}
}

func (e *Engine) Metrics() *observability.Metrics {
	return e.metrics
}

func (e *Engine) Load(ctx context.Context, ir *topology.TopologyIR) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.pipelines[ir.Name]; exists {
		return fmt.Errorf("pipeline %q already loaded", ir.Name)
	}
	p, err := NewPipeline(ctx, e.reg, ir, e.metrics)
	if err != nil {
		return err
	}
	e.pipelines[ir.Name] = p
	e.metrics.SetPipelineCount(len(e.pipelines))
	return nil
}

func (e *Engine) StartObservability(ctx context.Context, cfg config.ObservabilityConfig) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	pipes := make([]observability.PipelineHealth, 0, len(e.pipelines))
	for _, p := range e.pipelines {
		pipes = append(pipes, p)
	}
	checker := observability.NewChecker(pipes...)
	e.obs = observability.NewServer(cfg, e.metrics, checker)
	return e.obs.Start(ctx)
}

func (e *Engine) StopObservability(ctx context.Context) error {
	e.mu.Lock()
	obs := e.obs
	e.mu.Unlock()
	if obs == nil {
		return nil
	}
	return obs.Stop(ctx)
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
