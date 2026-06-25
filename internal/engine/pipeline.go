package engine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/deeploop-ai/eventr/internal/message"
	"github.com/deeploop-ai/eventr/internal/registry"
	"github.com/deeploop-ai/eventr/internal/stage"
	"github.com/deeploop-ai/eventr/internal/topology"
	"github.com/google/uuid"
)

type Pipeline struct {
	ir      *topology.TopologyIR
	reg     *registry.Registry
	stages  map[string]stage.Stage
	graph   *runtimeGraph
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	stageWG sync.WaitGroup
	started atomic.Bool
}

func NewPipeline(ctx context.Context, reg *registry.Registry, ir *topology.TopologyIR) (*Pipeline, error) {
	p := &Pipeline{
		ir:     ir,
		reg:    reg,
		stages: make(map[string]stage.Stage),
	}
	for _, st := range ir.Stages {
		if err := p.instantiateStage(st); err != nil {
			return nil, err
		}
	}
	g, err := buildRuntimeGraph(ir)
	if err != nil {
		return nil, err
	}
	p.graph = g
	return p, nil
}

func (p *Pipeline) instantiateStage(st topology.StageIR) error {
	cfg := map[string]any{}
	if st.Config != nil {
		for k, v := range st.Config {
			cfg[k] = v
		}
	}
	cfg["__decoder"] = st.Decoder
	cfg["__encoder"] = st.Encoder
	cfg["__predicate"] = st.Predicate
	cfg["__workers"] = st.Workers
	cfg["__batch"] = st.Batch
	cfg["__ordering"] = st.Ordering
	cfg["__max_in_flight"] = st.MaxInFlight

	var s stage.Stage
	var err error
	switch st.Kind {
	case "source":
		var src stage.Source
		src, err = p.reg.CreateSource(st.Type, st.ID, cfg)
		s = src
	case "transform":
		var tr stage.Transform
		tr, err = p.reg.CreateTransform(st.Type, st.ID, cfg)
		s = tr
	case "sink":
		var sk stage.Sink
		sk, err = p.reg.CreateSink(st.Type, st.ID, cfg)
		s = sk
	default:
		return fmt.Errorf("unknown stage kind %q", st.Kind)
	}
	if err != nil {
		return fmt.Errorf("stage %q: %w", st.ID, err)
	}
	p.stages[st.ID] = s
	return nil
}

func (p *Pipeline) Start(ctx context.Context) error {
	if !p.started.CompareAndSwap(false, true) {
		return fmt.Errorf("pipeline %q already started", p.ir.Name)
	}
	runCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	for id, st := range p.stages {
		if err := st.Init(runCtx); err != nil {
			cancel()
			return fmt.Errorf("init stage %q: %w", id, err)
		}
	}

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.run(runCtx)
	}()
	return nil
}

func (p *Pipeline) Stop(ctx context.Context) error {
	if p.cancel != nil {
		p.cancel()
	}
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	for _, st := range p.stages {
		if sk, ok := st.(stage.Sink); ok {
			_ = sk.Flush(ctx)
		}
		_ = st.Stop(ctx)
	}
	return nil
}

func (p *Pipeline) startTransformFanIn(node *runtimeNode) {
	go func() {
		for {
			select {
			case msg, ok := <-node.inbound:
				if !ok {
					return
				}
				node.batchIn <- []*message.Message{msg}
			}
		}
	}()
}

func (p *Pipeline) run(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			_ = r
		}
	}()

	for id, node := range p.graph.nodes {
		if node.kind == "transform" {
			p.startTransformFanIn(node)
		}
		_ = id
	}

	for id, node := range p.graph.nodes {
		if node.kind != "sink" {
			continue
		}
		p.stageWG.Add(1)
		go func(sinkID string, n *runtimeNode) {
			defer p.stageWG.Done()
			p.runSink(ctx, sinkID, n)
		}(id, node)
	}

	for id, node := range p.graph.nodes {
		if node.kind != "transform" {
			continue
		}
		p.stageWG.Add(1)
		go func(trID string, n *runtimeNode) {
			defer p.stageWG.Done()
			p.runTransform(ctx, trID, n)
		}(id, node)
	}

	for id, node := range p.graph.nodes {
		if node.kind != "source" {
			continue
		}
		p.stageWG.Add(1)
		go func(srcID string, n *runtimeNode) {
			defer p.stageWG.Done()
			p.runSource(ctx, srcID, n)
		}(id, node)
	}

	<-ctx.Done()
	p.stageWG.Wait()
}

func (p *Pipeline) runSource(ctx context.Context, id string, node *runtimeNode) {
	src := p.stages[id].(stage.Source)
	out := make(chan *message.Message, node.outBuffer)
	errCh := make(chan error, 1)
	go func() {
		errCh <- src.Consume(ctx, out)
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case err := <-errCh:
			if err != nil && ctx.Err() == nil {
				_ = err
			}
			return
		case msg, ok := <-out:
			if !ok {
				return
			}
			if msg.ID == "" {
				msg.ID = uuid.NewString()
			}
			if acking, ok := src.(stage.AckingSource); ok {
				msg.SetAckFn(func(err error) {
					acking.OnAck(msg, err)
				})
			}
			p.dispatchFrom(ctx, id, msg)
		}
	}
}

func (p *Pipeline) dispatchFrom(ctx context.Context, fromID string, msg *message.Message) {
	edges := p.graph.outgoing[fromID]
	if len(edges) == 0 {
		msg.Ack(nil)
		return
	}
	matched := p.matchEdges(ctx, edges, msg)
	if len(matched) == 0 {
		msg.Ack(nil)
		return
	}
	var pending int32 = int32(len(matched))
	parentAck := func(err error) {
		if err != nil {
			msg.Ack(err)
		}
	}
	_ = parentAck
	for _, edge := range matched {
		edge := edge
		child := msg.ShallowCopy()
		child.SetAckFn(func(err error) {
			if atomic.AddInt32(&pending, -1) == 0 {
				msg.Ack(err)
			}
		})
		node := p.graph.nodes[edge.To]
		select {
		case <-ctx.Done():
			child.Ack(ctx.Err())
			return
		case node.inbound <- child:
		}
	}
}

func (p *Pipeline) matchEdges(ctx context.Context, edges []topology.EdgeIR, msg *message.Message) []topology.EdgeIR {
	var matched []topology.EdgeIR
	for _, edge := range edges {
		if edge.Condition == "" {
			matched = append(matched, edge)
			continue
		}
		ok, err := p.graph.evalCondition(edge.Condition, msg)
		if err != nil {
			if edge.Required {
				msg.Ack(err)
				return nil
			}
			continue
		}
		if ok {
			matched = append(matched, edge)
		}
	}
	_ = ctx
	return matched
}

func (p *Pipeline) runTransform(ctx context.Context, id string, node *runtimeNode) {
	tr := p.stages[id].(stage.Transform)
	for {
		select {
		case <-ctx.Done():
			return
		case batch := <-node.batchIn:
			out, err := tr.Process(ctx, batch)
			if err != nil {
				for _, m := range batch {
					m.Ack(err)
				}
				continue
			}
			for _, m := range out {
				p.dispatchFrom(ctx, id, m)
			}
			if len(out) == 0 {
				for _, m := range batch {
					m.Ack(nil)
				}
			}
		}
	}
}

func (p *Pipeline) runSink(ctx context.Context, id string, node *runtimeNode) {
	sk := p.stages[id].(stage.Sink)
	batchSize := 1
	for _, st := range p.ir.Stages {
		if st.ID == id && st.Batch != nil && st.Batch.Size > 0 {
			batchSize = st.Batch.Size
		}
	}
	batch := make([]*message.Message, 0, batchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		err := sk.Write(ctx, batch)
		for _, m := range batch {
			m.Ack(err)
		}
		batch = batch[:0]
	}
	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case msg := <-node.inbound:
			batch = append(batch, msg)
			if len(batch) >= batchSize {
				flush()
			}
		}
	}
}
