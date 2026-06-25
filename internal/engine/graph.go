package engine

import (
	"encoding/json"
	"fmt"

	"github.com/deeploop-ai/eventr/internal/eql"
	"github.com/deeploop-ai/eventr/internal/message"
	"github.com/deeploop-ai/eventr/internal/topology"
)

type runtimeNode struct {
	id         string
	kind       string
	inbound    chan *message.Message
	batchIn    chan []*message.Message
	outBuffer  int
	workers    int
	conditions map[string]*eql.Program
}

type runtimeGraph struct {
	nodes    map[string]*runtimeNode
	outgoing map[string][]topology.EdgeIR
}

func buildRuntimeGraph(ir *topology.TopologyIR) (*runtimeGraph, error) {
	g := &runtimeGraph{
		nodes:    make(map[string]*runtimeNode),
		outgoing: make(map[string][]topology.EdgeIR),
	}
	for _, st := range ir.Stages {
		buf := 64
		workers := st.Workers
		if workers == 0 {
			workers = 1
		}
		node := &runtimeNode{
			id:        st.ID,
			kind:      st.Kind,
			inbound:   make(chan *message.Message, buf),
			batchIn:   make(chan []*message.Message, workers),
			outBuffer: buf,
			workers:   workers,
		}
		g.nodes[st.ID] = node
	}
	for _, edge := range ir.Edges {
		g.outgoing[edge.From] = append(g.outgoing[edge.From], edge)
		if edge.Condition != "" {
			prg, err := eql.CompileCondition(edge.Condition)
			if err != nil {
				return nil, fmt.Errorf("edge %s->%s condition: %w", edge.From, edge.To, err)
			}
			g.nodes[edge.From].conditions = g.nodes[edge.From].conditions
			if g.nodes[edge.From].conditions == nil {
				g.nodes[edge.From].conditions = make(map[string]*eql.Program)
			}
			g.nodes[edge.From].conditions[edge.To] = prg
		}
	}
	return g, nil
}

func (g *runtimeGraph) evalCondition(cond string, msg *message.Message) (bool, error) {
	prg, err := eql.CompileCondition(cond)
	if err != nil {
		return false, err
	}
	payload := map[string]any{}
	if msg.ParsedData() != nil {
		if m, ok := msg.ParsedData().(map[string]any); ok {
			payload = m
		}
	} else if len(msg.Payload) > 0 {
		_ = json.Unmarshal(msg.Payload, &payload)
	}
	ctx := &eql.EvalContext{
		Msg:     msgAdapter{msg},
		Input:   payload,
		Payload: payload,
		Meta:    msg.Metadata,
	}
	return prg.EvalFilter(ctx)
}

type msgAdapter struct{ *message.Message }

func (m msgAdapter) EnsureWritable() { m.Message.EnsureWritable() }
func (m msgAdapter) SetParsedData(v any) {
	m.Message.SetParsedData(v)
}
func (m msgAdapter) Metadata() map[string]any { return m.Message.Metadata }
