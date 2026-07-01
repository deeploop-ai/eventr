# AI / Agent 支持

> 版本：v1.0-draft  
> 日期：2026-07-01  
> 状态：规划阶段（v2.1 起分阶段落地）

English summary in [README](../README.md#ai-native-event-pipelines).

---

## 1. 定位与目标

eventr 的核心是 **DAG 事件路由**——消息在 Source → Transform → Sink 之间流动，具备背压、重试、DLQ 与可观测性。AI/Agent 不是独立子系统，而是 **一等公民 Transform / Step 能力**，与 `map`、`route`、`enrich` 等同层集成。

### 1.1 为什么 AI/Agent 是 eventr 的关键特性

| 场景 | eventr 优势 |
|------|-------------|
| **实时 AI  enrichment** | Kafka/Webhook 事件 → LLM 分类/摘要 → 路由到不同 Sink |
| **RAG 数据管道** | 事件触发分块 → embedding → 向量库写入，全链路 at-least-once |
| **多 Agent 编排** | DAG 天然表达「分类 → 专家 Agent → 聚合」；`route` + 专用 pipeline 分支 |
| **Human-in-the-loop** | HTTP Source 收用户输入 → Agent 推理 → HTTP Sink 回调；边级 retry/DLQ 保可靠 |
| **可观测 AI 运维** | `eventr_*` 指标扩展 token/latency/cost；OTLP span 贯穿 LLM 调用 |

竞品参考：Redpanda Connect（原 Benthos）已提供 OpenAI/Bedrock/Ollama 等 16+ AI processor；eventr 以对等能力 + **eql 模板** + **DAG 编排** + **Ack 语义** 形成差异化。

### 1.2 设计原则

1. **引擎无 LLM 感知** — 引擎仍只调度 `Message`；LLM 调用封装在 Transform 插件内
2. **Provider 可插拔** — OpenAI 兼容 API 为默认抽象；Ollama/Bedrock/Vertex 为独立 adapter
3. **声明式配置** — prompt / model / tool 定义在 YAML/HOCON；动态部分用 eql 模板
4. **与现有语义一致** — 错误走 `error_mode` + 边 `delivery` + pipeline `dlq`；慢调用受 `max_inflight` 背压
5. **分阶段交付** — v2.1 LLM Transform → v2.2 Agent Transform → v2.3+ 编排与 MCP

---

## 2. 架构概览

```
                    ┌─────────────────────────────────────┐
                    │           eventr Engine              │
                    │  fanOut / fanIn / Ack / backpressure │
                    └──────────────────┬──────────────────┘
                                       │
         ┌─────────────────────────────┼─────────────────────────────┐
         │                             │                             │
   ┌─────▼─────┐               ┌───────▼───────┐              ┌──────▼──────┐
   │  Source   │               │  Transform    │              │    Sink     │
   │ kafka/http│──────────────▶│ map / route   │─────────────▶│ kafka/http  │
   └───────────┘               │ llm / agent   │              └─────────────┘
                               │ embed         │
                               └───────┬───────┘
                                       │
                               ┌───────▼───────┐
                               │ Provider Layer │
                               │ openai-compat │
                               │ ollama/bedrock │
                               └───────────────┘
```

**关键概念：**

| 概念 | 说明 |
|------|------|
| **LLM Transform** | 单轮 LLM 调用（chat / completion / JSON mode）；一条 Message 进 → 一条（或 split 多条）出 |
| **Embed Transform** | 文本 → 向量；输出写入 payload 或 metadata，供下游向量 Sink 消费 |
| **Agent Transform** | 多轮推理 + tool calling；会话状态存 metadata 或外部 store |
| **Agent Pipeline** | 用 DAG 表达多 Agent：`classify (llm)` → `route` → 专家 branch → `fan-in` |
| **Provider** | 统一 `Complete` / `Embed` / `Stream` 接口；各厂商 adapter 实现 |

---

## 3. 组件规划

### 3.1 Transform：`llm`（P0，v2.1）

单轮 LLM 调用，对标 Redpanda Connect `openai_chat_completion`。

```yaml
steps:
  classify:
    depends_on: [kafka-in]
    transform:
      type: llm
      workers: 4                    # 并发受 engine.max_inflight 约束
      config:
        provider: openai            # openai | ollama | bedrock | vertex | azure
        model: gpt-4o-mini
        api_key: ${OPENAI_API_KEY}
        base_url: ${OPENAI_BASE_URL}   # 可选，兼容 OpenAI API 的网关
        timeout: 30s
        max_retries: 2               # Transform 内瞬时重试；确定性错误不重试
        response_format: json        # text | json | json_schema
        json_schema:                 # response_format=json_schema 时
          name: classification
          schema: { type: object, properties: { category: { type: string } } }
        messages:
          - role: system
            content: |
              Classify the user message into: billing, support, sales.
          - role: user
            content: "${payload.text}"   # eql 模板，编译期校验变量引用
        result:
          field: payload.classification  # 写入 payload 路径
          # 或 metadata_key: er-llm-response
        usage:
          record_tokens: true            # 写入 metadata er-llm-* 
```

**行为语义：**

- `messages[].content` 支持 eql 模板（`${payload.x}`、`${metadata.y}`）
- 成功：按 `result` 配置写入 payload/metadata；可选保留 raw response 到 `metadata['er-llm-raw']`
- 失败：429/5xx → 瞬时错误，走边 retry；400/401 → 确定性错误，直接 DLQ
- 超时：context deadline，计 `eventr_llm_timeout_total`

### 3.2 Transform：`embed`（P1，v2.1）

```yaml
  vectorize:
    depends_on: [chunk]
    transform:
      type: embed
      config:
        provider: ollama
        model: nomic-embed-text
        input: "${payload.chunk_text}"
        result:
          field: payload.embedding
          dimensions: 768
```

### 3.3 Transform：`agent`（P0，v2.2）

多轮 Agent，支持 tool calling 与可选会话持久化。

```yaml
  support-agent:
    depends_on: [classify]
    transform:
      type: agent
      workers: 2
      config:
        provider: openai
        model: gpt-4o
        max_turns: 10
        session:
          key: "${metadata.session_id}"     # 同 key 共享会话
          store: memory                     # memory | redis（v2.2+）
          ttl: 1h
        system: |
          You are a support agent. Use tools when needed.
        tools:
          - name: lookup_order
            description: Look up order by ID
            parameters:
              type: object
              properties:
                order_id: { type: string }
            invoke:
              type: http                     # http | pipeline | mcp
              method: GET
              url: "https://api.example.com/orders/${arguments.order_id}"
              timeout: 5s
          - name: escalate
            invoke:
              type: pipeline                 #  fan-out 到子 pipeline（v2.3）
              pipeline: escalation-handler
        result:
          field: payload.agent_reply
```

**Agent 循环（单 Message 生命周期内）：**

```
1. 组装 messages（system + session history + 当前 user content）
2. 调用 Provider.Complete（含 tools schema）
3. 若 response 含 tool_calls → 执行 tool → 追加 tool result → 回到 2
4. 达到 max_turns 或 final text → 写入 result → 返回
5. 更新 session store
```

**与 DAG 的关系：** 复杂编排优先用 **多 step DAG**（llm classify → route → 专家 llm）；`agent` transform 适合 **单 Message 内闭环**（tool loop）。

### 3.4 eql 扩展（v2.1）

| 函数 | 说明 |
|------|------|
| `template(s, vars)` | 安全字符串模板（内部用于 messages） |
| `truncate(s, n)` | 截断至 token 预算 |
| `token_estimate(s)` | 粗算 token 数（用于前置校验） |

LLM 相关 **不** 在 eql 内直接发起网络调用——保持 eql 纯函数语义。

### 3.5 Sink 扩展（P2，v2.2+）

| Sink | 用途 |
|------|------|
| `pinecone` / `qdrant` / `pgvector` | RAG 向量写入 |
| `langfuse` / `langsmith` | LLM trace 导出（与 OTLP 互补） |

### 3.6 StepType：`task`（v2.3，与 Envelope 对齐）

预留 `steps.{name}.step_type: task`，表示 **异步长任务**（Agent 跑完再 emit 下游）。与 `agent` transform 的区别：

| | `agent` transform | `task` step |
|--|-------------------|-------------|
| 粒度 | 单 Message 同步/半同步 | 可跨 Message、可 checkpoint |
| 超时 | transform config | step 级 `timeout` + 恢复 |
| 状态 | session store | 引擎级 task ledger |

`task` 在 v2.3 与 Decision step 一并评估；v2.1/v2.2 不阻塞。

---

## 4. Provider 抽象

```go
// internal/llm/provider.go（规划接口，v2.1 实现）

type Provider interface {
    Name() string
    Complete(ctx context.Context, req *CompletionRequest) (*CompletionResponse, error)
    Embed(ctx context.Context, req *EmbedRequest) (*EmbedResponse, error)
    // Stream  v2.2+：流式写入 metadata，供 websocket sink 消费
}

type CompletionRequest struct {
    Model       string
    Messages    []Message
    Tools       []ToolDef
    Format      ResponseFormat
    Temperature *float64
    MaxTokens   *int
}

type CompletionResponse struct {
    Content    string
    ToolCalls  []ToolCall
    Usage      TokenUsage
    FinishReason string
}
```

**Adapter 优先级：**

| 阶段 | Provider | 说明 |
|------|----------|------|
| v2.1 | `openai` | 官方 + 任意 OpenAI 兼容网关（Azure/OpenRouter/本地 vLLM） |
| v2.1 | `ollama` | 本地开发 / 边缘部署 |
| v2.2 | `bedrock` | AWS 生产 |
| v2.2 | `vertex` | GCP Gemini |
| v2.3 | `anthropic` | 原生 Messages API |

Provider 注册与 Source/Sink 相同：`registry.RegisterLLMProvider("openai", factory)`。

---

## 5. 配置与 Message 约定

### 5.1 Metadata 前缀 `er-llm-*` / `er-agent-*`

| 字段 | 说明 |
|------|------|
| `er-llm-model` | 实际使用的 model |
| `er-llm-provider` | provider 名 |
| `er-llm-prompt-tokens` | input tokens |
| `er-llm-completion-tokens` | output tokens |
| `er-llm-latency-ms` | 调用耗时 |
| `er-llm-request-id` | 厂商 request id（tracing 关联） |
| `er-agent-turns` | Agent 循环轮数 |
| `er-agent-session-id` | 会话 id |
| `er-agent-tool-calls` | 本轮 tool 调用次数 |

### 5.2 错误类型（扩展 `er-error-type`）

| 值 | 说明 |
|----|------|
| `llm_rate_limit` | 429，可 retry |
| `llm_timeout` | 超时 |
| `llm_auth_error` | 401/403，确定性 |
| `llm_invalid_request` | 400，确定性 |
| `llm_content_filter` | 内容策略拒绝 |
| `agent_max_turns` | 超过 max_turns |
| `agent_tool_error` | tool 执行失败 |

### 5.3 背压与并发

- LLM transform 的 `workers` 受 `engine.max_workers` 约束
- 每条 Message 占用 `max_inflight` 槽位直至 LLM 返回（长调用会自然背压 Source）
- 可选 `config.concurrency_limit` per-stage 限制同时进行中的 LLM 请求数

---

## 6. 可观测性

### 6.1 指标（`eventr_*` 扩展）

| 指标 | 类型 | 标签 |
|------|------|------|
| `eventr_llm_requests_total` | Counter | pipeline, stage_id, provider, model, status |
| `eventr_llm_latency_seconds` | Histogram | pipeline, stage_id, provider, model |
| `eventr_llm_tokens_total` | Counter | pipeline, stage_id, provider, model, direction=prompt\|completion |
| `eventr_llm_errors_total` | Counter | pipeline, stage_id, error_type |
| `eventr_agent_turns_total` | Counter | pipeline, stage_id |
| `eventr_agent_tool_calls_total` | Counter | pipeline, stage_id, tool_name, status |

### 6.2 Tracing

```
Transform Span (eventr.transform.classify)
└── LLM Span (eventr.llm.complete)
    ├── attributes: provider, model, tokens, finish_reason
    └── Child Span (eventr.agent.tool) × N
```

---

## 7. 典型 Pipeline 模式

### 7.1 事件分类 + 路由

```yaml
steps:
  kafka-in:
    source: { type: kafka, decoder: json, config: { topics: [support-tickets] } }

  llm-classify:
    depends_on: [kafka-in]
    transform:
      type: llm
      config:
        provider: openai
        model: gpt-4o-mini
        response_format: json
        messages:
          - role: user
            content: "Classify: ${payload.body}"
        result: { field: payload.category }

  splitter:
    depends_on: [llm-classify]
    transform:
      type: route
      config:
        routes:
          billing: "payload.category == 'billing'"
          support: "payload.category == 'support'"
          _default: "true"

  billing-sink:
    depends_on: { splitter: { route: billing } }
    sink: { type: kafka, config: { topic: billing-queue } }
```

### 7.2 RAG 摄取

```
kafka-in → map (extract text) → split (chunk) → embed → sink (pgvector)
```

示例见 `_examples/09-rag-ingest.yaml`（Phase 1 完成后添加）。

### 7.3 多 Agent DAG

```
                    ┌─► llm (billing expert) ──► sink
kafka → llm (router)─┼─► llm (support expert) ──► sink
                    └─► llm (sales expert)   ──► sink
```

用 `route` 分流，每个 branch 独立 `llm` step——**无需** monolithic super-agent。

---

## 8. 开发路线图

与 [eventr-design.md §12](../eventr-design.md#12-开发路线图) 对齐，AI/Agent 作为 **阶段 3 并行轨** 与生态扩展交织推进。

### Phase A：LLM 基础（v2.1，约 3 周）

| 任务 | 交付物 |
|------|--------|
| A1 Provider 接口 + OpenAI adapter | `internal/llm/` |
| A2 `llm` transform | `plugins/transform/llm/` |
| A3 eql 模板解析（messages content） | `internal/eql/template.go` |
| A4 指标 + tracing span | `internal/observability/` |
| A5 `embed` transform + Ollama adapter | `plugins/transform/embed/` |
| A6 示例 + 文档 | `_examples/09-*.yaml`、`docs/configurations.md` 更新 |

**验收：** `eventr test` 用 mock provider 跑通 classify pipeline；集成测试可选 `-tags=integration` 打真实 Ollama。

### Phase B：Agent + 工具（v2.2，约 4 周）

| 任务 | 交付物 |
|------|--------|
| B1 `agent` transform + tool loop | `plugins/transform/agent/` |
| B2 HTTP tool invoker | `internal/agent/tools/http.go` |
| B3 Session store（memory + redis） | `internal/agent/session/` |
| B4 Bedrock / Vertex adapter | `internal/llm/bedrock/`, `vertex/` |
| B5 `eventr_agent_*` 指标 | observability |
| B6 向量 Sink（pgvector 或 qdrant 二选一） | `plugins/sink/` |

**验收：** support-agent 示例，HTTP tool 调 mock server，session 跨 Message 复用。

### Phase C：编排与生态（v2.3+）

| 任务 | 说明 |
|------|------|
| C1 MCP tool adapter | Agent 调用 MCP server |
| C2 `task` step type | 长任务 checkpoint |
| C3 pipeline-as-tool | Agent 触发子 pipeline |
| C4 Streaming LLM + websocket sink | 流式响应 |
| C5 Langfuse/LangSmith sink | 可选 observability 导出 |
| C6 WASM LLM 本地模型 | 与现有 wasm transform 结合评估 |

---

## 9. 实现计划索引

| 文档 | 范围 |
|------|------|
| [2026-07-01-ai-agent-foundation.md](superpowers/plans/2026-07-01-ai-agent-foundation.md) | Phase A 详细任务分解（TDD、文件清单） |
| [configurations.md](configurations.md) | 用户-facing 配置参考（Phase A 完成后更新） |
| [eventr-design.md §9](../eventr-design.md#9-组件生态规划) | 组件优先级总表 |

---

## 10. 风险与约束

| 风险 | 缓解 |
|------|------|
| LLM 延迟高，占满 inflight | per-stage `concurrency_limit`；独立 AI pipeline 与核心 ETL 隔离 |
| Token 成本不可控 | `token_estimate` 前置校验；metrics 告警；可选 `max_tokens` 硬限 |
| Tool 调用 SSRF | HTTP tool 允许 list 域名；禁止内网段（可配置） |
| Session 内存泄漏 | memory store LRU + TTL；生产用 redis |
| 厂商 API 变更 | Provider adapter 隔离；集成测试锁定行为 |

---

## 11. 非目标（v2.3 前）

- 内置向量数据库（只做 Sink 插件）
- 可视化 Agent 编排 UI（社区 / v2.3+）
- 微调 / 训练 pipeline
- 引擎内嵌 RAG 检索（用 enrich + HTTP 或专用 transform 组合）

---

> 下一步：执行 [Phase A 实现计划](superpowers/plans/2026-07-01-ai-agent-foundation.md)，交付 `llm` transform + OpenAI/Ollama provider。
