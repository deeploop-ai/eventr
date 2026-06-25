package engine

import (
	"context"
	"sync/atomic"

	"github.com/deeploop-ai/eventr/internal/eql"
	"github.com/deeploop-ai/eventr/internal/message"
)

type ackAggregator struct {
	pending   int32
	errStored int32
	firstErr  atomic.Value
	parent    *message.Message
}

// dispatchTransformOutputs routes transform outputs downstream and completes parent
// message Ack when all child branches finish (Benthos split model).
func (p *Pipeline) dispatchTransformOutputs(ctx context.Context, stageID string, inputs, outputs []*message.Message) {
	outCountByID := make(map[string]int, len(outputs))
	for _, m := range outputs {
		outCountByID[m.ID]++
	}

	passThroughDispatched := make(map[string]bool)
	aggs := make(map[string]*ackAggregator)

	for _, m := range inputs {
		count := outCountByID[m.ID]
		if count == 0 {
			m.Ack(nil)
			continue
		}
		if count == 1 {
			for _, o := range outputs {
				if o.ID == m.ID && o == m {
					p.dispatchFrom(ctx, stageID, o)
					passThroughDispatched[m.ID] = true
					break
				}
			}
		}
		if passThroughDispatched[m.ID] {
			continue
		}
		aggs[m.ID] = &ackAggregator{
			pending: int32(count),
			parent:  m,
		}
	}

	for _, o := range outputs {
		if passThroughDispatched[o.ID] {
			continue
		}
		agg := aggs[o.ID]
		if agg == nil {
			continue
		}
		child := o
		parent := agg.parent
		child.SetAckFn(func(err error) {
			if err != nil && atomic.CompareAndSwapInt32(&agg.errStored, 0, 1) {
				agg.firstErr.Store(err)
			}
			if atomic.AddInt32(&agg.pending, -1) == 0 {
				var ackErr error
				if stored := agg.firstErr.Load(); stored != nil {
					ackErr = stored.(error)
				}
				parent.Ack(ackErr)
			}
		})
		p.dispatchFrom(ctx, stageID, child)
	}
}

func sendToInbound(ctx context.Context, ch chan *message.Message, msg *message.Message, strategy string) {
	switch strategy {
	case "drop_newest":
		select {
		case <-ctx.Done():
			msg.Ack(ctx.Err())
		case ch <- msg:
		default:
			msg.Ack(nil)
		}
	case "drop_oldest":
		select {
		case <-ctx.Done():
			msg.Ack(ctx.Err())
		case ch <- msg:
		default:
			select {
			case old := <-ch:
				old.Ack(nil)
			default:
			}
			select {
			case <-ctx.Done():
				msg.Ack(ctx.Err())
			case ch <- msg:
			}
		}
	default:
		select {
		case <-ctx.Done():
			msg.Ack(ctx.Err())
		case ch <- msg:
		}
	}
}

func (p *Pipeline) evalCondition(ctx context.Context, prg *eql.Program, msg *message.Message) (bool, error) {
	if prg == nil {
		return true, nil
	}
	if err := p.ensureParsed(msg); err != nil {
		return false, err
	}
	_ = ctx
	return p.graph.evalCondition(prg, msg)
}
