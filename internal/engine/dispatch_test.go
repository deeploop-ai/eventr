package engine

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/deeploop-ai/eventr/internal/message"
	"github.com/deeploop-ai/eventr/internal/topology"
)

func TestDispatchTransformOutputs_AcksParentAfterChild(t *testing.T) {
	p := &Pipeline{graph: &runtimeGraph{nodes: map[string]*runtimeNode{}}}

	var parentAcks atomic.Int32
	parent := message.New([]byte(`{"x":1}`), nil)
	parent.ID = "msg-1"
	parent.SetAckFn(func(error) {
		parentAcks.Add(1)
	})

	child := parent.ShallowCopy()
	child.SetAckFn(nil)

	sink := &runtimeNode{inbound: make(chan *message.Message, 1)}
	p.graph.nodes["sink"] = sink
	p.graph.outgoing = map[string][]topology.EdgeIR{
		"map": {{From: "map", To: "sink"}},
	}

	p.dispatchTransformOutputs(context.Background(), "map", []*message.Message{parent}, []*message.Message{child})

	select {
	case got := <-sink.inbound:
		got.Ack(nil)
	default:
		t.Fatal("expected child dispatched to sink")
	}
	if parentAcks.Load() != 1 {
		t.Fatalf("parent ack count = %d, want 1", parentAcks.Load())
	}
}

func TestDispatchTransformOutputs_AcksDroppedInputs(t *testing.T) {
	p := &Pipeline{graph: &runtimeGraph{nodes: map[string]*runtimeNode{}, outgoing: map[string][]topology.EdgeIR{}}}

	var acks atomic.Int32
	parent := message.New(nil, nil)
	parent.ID = "msg-1"
	parent.SetAckFn(func(error) {
		acks.Add(1)
	})

	p.dispatchTransformOutputs(context.Background(), "filter", []*message.Message{parent}, nil)

	if acks.Load() != 1 {
		t.Fatalf("dropped input ack count = %d, want 1", acks.Load())
	}
}

func TestDispatchTransformOutputs_PassThroughUsesExistingAck(t *testing.T) {
	p := &Pipeline{graph: &runtimeGraph{nodes: map[string]*runtimeNode{}}}
	var acks atomic.Int32
	msg := message.New(nil, nil)
	msg.ID = "msg-1"
	msg.SetAckFn(func(error) {
		acks.Add(1)
	})

	sink := &runtimeNode{inbound: make(chan *message.Message, 1)}
	p.graph.nodes["sink"] = sink
	p.graph.outgoing = map[string][]topology.EdgeIR{
		"filter": {{From: "filter", To: "sink"}},
	}

	p.dispatchTransformOutputs(context.Background(), "filter", []*message.Message{msg}, []*message.Message{msg})

	got := <-sink.inbound
	if got == msg {
		t.Fatal("dispatchFrom should fan-out a shallow copy")
	}
	got.Ack(nil)
	if acks.Load() != 1 {
		t.Fatalf("pass-through ack count = %d, want 1", acks.Load())
	}
}

func TestSendToInbound_DropNewest(t *testing.T) {
	ch := make(chan *message.Message, 1)
	ch <- message.New(nil, nil)

	var acks atomic.Int32
	msg := message.New(nil, nil)
	msg.SetAckFn(func(error) {
		acks.Add(1)
	})

	sendToInbound(context.Background(), ch, msg, "drop_newest")
	if acks.Load() != 1 {
		t.Fatalf("expected dropped message to be acked")
	}
}
