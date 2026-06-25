package message

import (
	"context"
	"sync"
	"sync/atomic"
)

type Message struct {
	ID       string
	Payload  []byte
	Metadata map[string]any

	parsedData      any
	parsedDirty     bool
	parsedCodec     string
	decoderStageID  string
	readOnly        atomic.Bool
	cowMu           sync.Mutex
	originalPayload []byte
	ctx             context.Context
	ackFn           func(error)
	errCount        int
}

func New(payload []byte, metadata map[string]any) *Message {
	if metadata == nil {
		metadata = make(map[string]any)
	}
	return &Message{
		Payload:  payload,
		Metadata: metadata,
	}
}

func (m *Message) Context() context.Context {
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}

func (m *Message) SetContext(ctx context.Context) {
	m.ctx = ctx
}

func (m *Message) SetAckFn(fn func(error)) {
	m.ackFn = fn
}

func (m *Message) Ack(err error) {
	if m.ackFn != nil {
		m.ackFn(err)
	}
}

func (m *Message) ShallowCopy() *Message {
	cp := &Message{
		ID:              m.ID,
		Payload:         m.Payload, // share reference — COW: only re-allocated on re-serialize
		Metadata:        shallowCopyMap(m.Metadata),
		parsedData:      m.parsedData,
		parsedDirty:     m.parsedDirty,
		parsedCodec:     m.parsedCodec,
		decoderStageID:  m.decoderStageID,
		originalPayload: m.originalPayload,
		ctx:             m.ctx,
		errCount:        m.errCount,
	}
	cp.readOnly.Store(true)
	return cp
}

func shallowCopyMap(src map[string]any) map[string]any {
	if src == nil {
		return make(map[string]any)
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func (m *Message) ParsedCodec() string {
	return m.parsedCodec
}

func (m *Message) SetParsedCodec(name string) {
	m.parsedCodec = name
}

func (m *Message) DecoderStageID() string {
	return m.decoderStageID
}

func (m *Message) SetDecoderStageID(id string) {
	m.decoderStageID = id
}

func (m *Message) ParsedData() any {
	return m.parsedData
}

func (m *Message) SetParsedData(data any) {
	m.parsedData = data
	m.parsedDirty = true
	m.readOnly.Store(false)
}

func (m *Message) MarkParsedDirty() {
	m.parsedDirty = true
}

func (m *Message) ParsedDirty() bool {
	return m.parsedDirty
}

func (m *Message) EnsureWritable() {
	m.cowMu.Lock()
	defer m.cowMu.Unlock()
	if m.readOnly.Load() && m.parsedData != nil {
		m.parsedData = deepCopyValue(m.parsedData)
		m.readOnly.Store(false)
	}
}

func (m *Message) BackupOriginalPayload() {
	if m.originalPayload == nil {
		m.originalPayload = m.Payload
	}
}

func (m *Message) OriginalPayload() []byte {
	if m.originalPayload != nil {
		return m.originalPayload
	}
	return m.Payload
}

func deepCopyValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		cp := make(map[string]any, len(t))
		for k, val := range t {
			cp[k] = deepCopyValue(val)
		}
		return cp
	case []any:
		cp := make([]any, len(t))
		for i, val := range t {
			cp[i] = deepCopyValue(val)
		}
		return cp
	default:
		return v
	}
}
