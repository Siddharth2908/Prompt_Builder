# AWA Prompt Builder — Software Design

## 1. Executive Summary

The **AWA Prompt Builder** is a runtime prompt construction framework implemented as an event-driven microservice. It assembles structured, LLM-standard-aware prompts from discrete, versioned components before every LLM call, identified at the use-case level (`AWA_Consumer_ID`) and at the individual call level (`AWA_Job_ID`).

---

## 2. Prompt Component Model

| Component | Key | Source | Lifecycle |
|---|---|---|---|
| User / System Query | `p_uq` | Chat turn or request payload | Dynamic — per job |
| Guardrail System Prompt | `p_guard` | Config store | Static — pre-loaded |
| Activity Business Context | `p_act_bus` | Config store | Static — per consumer |
| Activity Instructions | `p_act_ins` | Config store | Static — per consumer |
| Activity Conduct | `p_act_cond` | Config store | Static — per consumer |
| RAG / OCR Context (1..N) | `p_rag_context_n` | Knowledge retrieval pipeline | Dynamic — per job |
| Agent Role, Goal, Backstory | `p_agent_rgb` | Agent profile store | Static — per agent |
| Agent Conduct / Policy | `p_agent_conduct` | Agent policy store | Static — per agent |
| Agent Reflection Context | `p_agent_context` | CoT / ToT / self-reflection engine | Dynamic — per job |
| Assembly Template | `p_template` | Template registry | Static — per consumer, versioned |

### 2.1 Component Payload Schema

```json
{
  "component_id": "p_act_bus",
  "awa_consumer_id": "IDP_INVOICE_US",
  "version": "2.1.0",
  "enabled": true,
  "content": "...",
  "token_count": 412,
  "last_modified": "2026-05-20T10:00:00Z",
  "checksum": "sha256:abc123"
}
```

### 2.2 RAG Context Envelope

Because RAG context is multi-source, each source is a separate slot:

```json
{
  "component_id": "p_rag_context_n",
  "sources": [
    { "source_id": "kb_product_manual", "content": "...", "score": 0.91, "token_count": 620 },
    { "source_id": "ocr_page_3", "content": "...", "score": 0.87, "token_count": 810 }
  ]
}
```

---

## 3. Identity and Scoping

```
AWA_Consumer_ID  ─── identifies the use case (e.g. IDP_INVOICE_US)
    └── AWA_Job_ID  ─── identifies one LLM call within that use case
```

- **Consumer scope**: template, all static components, version pins, feature flags
- **Job scope**: `p_uq`, `p_rag_context_n`, `p_agent_context`, assembled prompt, metrics

---

## 4. System Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│                         Clients / Callers                          │
│   Chat UI  │  Source System API  │  Agent Orchestrator             │
└───────┬────────────────┬────────────────────┬───────────────────┘
        │                │                    │
        ▼                ▼                    ▼
┌───────────────────────────────────────────────────────────────────┐
│                       API Gateway / Ingress                        │
│          REST + gRPC  ──  Auth (JWT/mTLS)  ──  Rate Limit         │
└───────────────────────────────┬───────────────────────────────────┘
                                │
                       ┌────────▼────────┐
                       │   Event Bus      │  (Apache Kafka)
                       │  Topic Registry  │
                       └────────┬────────┘
          ┌─────────────────────┼────────────────────────┐
          ▼                     ▼                        ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────┐
│  Prompt Builder  │  │  Version Manager │  │   Context Observer   │
│     Service      │  │     Service      │  │      Service         │
│                  │  │                  │  │                      │
│ - Section loader │  │ - Version store  │  │ - Token counter      │
│ - Template exec  │  │ - Diff engine    │  │ - Bloat detector     │
│ - Flag resolver  │  │ - Rollback ops   │  │ - Rot scorer         │
│ - LLM formatter  │  │ - Audit log      │  │ - Alert publisher    │
└────────┬─────────┘  └──────────────────┘  └──────────────────────┘
         │
┌────────▼─────────────────────────────────────────────────────────┐
│                        LLM Adapter Registry                        │
│   OpenAI Adapter │ Anthropic Adapter │ Gemini Adapter │ ...       │
└──────────────────────────────────────────────────────────────────┘
         │
┌────────▼─────────────────────────────────────────────────────────┐
│                         Storage Layer                              │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌───────────────┐  │
│  │  Template  │ │  Section   │ │  Version   │ │   Job / Obs   │  │
│  │  Registry  │ │   Store    │ │   Store    │ │    Store      │  │
│  │ (Postgres) │ │ (Postgres) │ │   (S3 +   │ │  (Postgres +  │  │
│  │            │ │            │ │  Postgres) │ │  TimeSeries)  │  │
│  └────────────┘ └────────────┘ └────────────┘ └───────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

---

## 5. Core Services

### 5.1 Prompt Builder Service

The central service. Triggered by a `PromptBuildRequested` event, it:

1. Resolves the `p_template` and `DynamicDependencyConfig` for the `AWA_Consumer_ID`
2. Builds a **wait checklist** for applicable dynamic components and enters `AWAITING_DYNAMIC` state (or skips directly to assembly if none are applicable)
3. On checklist completion — either by event arrival or timeout — loads all enabled static sections
4. Merges arrived dynamic components (`p_uq`, `p_rag_context_n`, `p_agent_context`)
5. Passes the assembled section map to the LLM Adapter for formatting
6. Publishes `PromptBuilt` with the final payload and metadata

**Section Loader Resolution Order:**

```
Job-level override → Consumer-level config → Global default → Disabled (skip)
```

**Internal Pipeline:**

```
PromptBuildRequested
    │
    ├─► Load p_template (versioned) + DynamicDependencyConfig
    │
    ├─► Build wait checklist from DynamicDependencyConfig
    │       p_rag_context_n:  NOT_APPLICABLE → skip | OPTIONAL/REQUIRED → add to checklist
    │       p_agent_context:  NOT_APPLICABLE → skip | OPTIONAL/REQUIRED → add to checklist
    │
    ├─► [Checklist empty?]
    │       YES → jump to READY_TO_ASSEMBLE
    │       NO  → persist checklist to Redis, start timeout timers
    │             enter AWAITING_DYNAMIC state
    │
    │   ... wait for RAGContextRetrieved / AgentContextReady / DynamicComponentTimeout ...
    │
    ├─► [All checklist items resolved?]
    │       All received → READY_TO_ASSEMBLE
    │       Any REQUIRED timed out with on_timeout=FAIL → FAILED (publish PromptBuildFailed)
    │       Any timed out with on_timeout=PROCEED_WITHOUT → READY_TO_ASSEMBLE (omit section)
    │
    ├─► Load static sections; merge arrived dynamic sections + p_uq
    │
    ├─► For each slot in template order:
    │       ├─ Check enabled flag + NOT_APPLICABLE guard
    │       ├─ Load section content
    │       └─ Apply token budget check (pre-format)
    │
    ├─► Publish SectionsAssembled → Context Observer Service
    │
    ├─► LLM Adapter formats for target model standard
    │
    └─► Publish PromptBuilt
```

---

### 5.1.1 Dynamic Dependency Config

Stored per `AWA_Consumer_ID` and versioned alongside other consumer configuration. Declares whether `p_rag_context_n` and `p_agent_context` are applicable to this use case and, if so, how the builder should wait for them.

```json
{
  "awa_consumer_id": "IDP_INVOICE_US",
  "dynamic_dependency_config": {
    "p_rag_context_n": {
      "applicable": true,
      "wait_strategy": "REQUIRED",
      "timeout_ms": 5000,
      "on_timeout": "PROCEED_WITHOUT",
      "min_sources_required": 1
    },
    "p_agent_context": {
      "applicable": false,
      "wait_strategy": "NOT_APPLICABLE",
      "timeout_ms": null,
      "on_timeout": null
    }
  }
}
```

**`wait_strategy` values:**

| Value | Meaning |
|---|---|
| `NOT_APPLICABLE` | Component is not used for this consumer. No wait timer started. Slot skipped at assembly even if present in `p_template`. |
| `OPTIONAL` | Wait up to `timeout_ms`. If it arrives in time, include it. If not, proceed silently without it. `on_timeout` is implicitly `PROCEED_WITHOUT`. |
| `REQUIRED` | Wait up to `timeout_ms`. `on_timeout` governs what happens next. |

**`on_timeout` values (only relevant when `wait_strategy = REQUIRED`):**

| Value | Behaviour |
|---|---|
| `PROCEED_WITHOUT` | Omit the section from the final prompt. Record `"received": false, "reason": "TIMEOUT"` in the section manifest. Emit a `WARN` observation signal. |
| `FAIL` | Abort the build. Publish `PromptBuildFailed`. No prompt is emitted. |

**Job-level override:** The caller can override `wait_strategy` and `on_timeout` per job via `PromptBuildRequested.overrides`. This follows the standard override precedence (see Section 10).

**`min_sources_required`** (RAG only): If RAG arrives but returns fewer sources than this threshold, treat it as `PROCEED_WITHOUT` regardless of `on_timeout`.

---

### 5.1.2 Job State Machine

Because the Prompt Builder is stateless (horizontally scaled), per-job state is held in **Redis** keyed by `AWA_Job_ID` with a TTL equal to `max(timeout_ms values) + 10 s`. Any replica can pick up an incoming event and advance the state.

```
                        ┌─────────────────────────────────────────┐
                        │             INITIATED                    │
                        │  (PromptBuildRequested received,         │
                        │   consumer config loaded)                │
                        └──────────────┬──────────────────────────┘
                                       │
                      ┌────────────────▼────────────────┐
                      │  Checklist empty?                │
                      └────────┬───────────────┬─────────┘
                               │ NO            │ YES
                    ┌──────────▼──────┐        │
                    │ AWAITING_DYNAMIC │        │
                    │ (timers running) │        │
                    └──────┬──────────┘        │
                           │                   │
          ┌────────────────┼───────────────────┘
          │                │
          │   ┌────────────▼─────────────────────────┐
          │   │ All items received OR                 │
          │   │ timed-out with PROCEED_WITHOUT?       │
          │   └──┬─────────────────────────────┬──────┘
          │      │ YES                          │ NO — any REQUIRED
          │      │                             │ timed out with FAIL
          │   ┌──▼──────────────────┐    ┌────▼──────┐
          └──►│  READY_TO_ASSEMBLE  │    │  FAILED   │◄─── Context Observer BLOCK
              └──────────┬──────────┘    └───────────┘
                         │
                   ┌─────▼──────────┐
                   │   ASSEMBLING   │
                   └─────┬──────────┘
                         │
                   ┌─────▼──────────┐
                   │     BUILT      │
                   └────────────────┘
```

**State record in Redis:**

```json
{
  "awa_job_id": "job_abc123",
  "awa_consumer_id": "IDP_INVOICE_US",
  "state": "AWAITING_DYNAMIC",
  "checklist": {
    "p_rag_context_n": {
      "wait_strategy": "REQUIRED",
      "on_timeout": "PROCEED_WITHOUT",
      "deadline": "2026-05-26T08:00:05.000Z",
      "received": false,
      "timed_out": false
    },
    "p_agent_context": {
      "wait_strategy": "NOT_APPLICABLE",
      "deadline": null,
      "received": false,
      "timed_out": false
    }
  },
  "ttl_expires_at": "2026-05-26T08:00:15.000Z"
}
```

**Timeout mechanism:** When the builder enters `AWAITING_DYNAMIC`, it publishes a delayed `DynamicComponentTimeout` event (one per checklist item) to a Kafka topic backed by a delay queue (e.g., using Kafka with a timer-service sidecar or a Redis-based scheduler). When the timeout event fires, the builder reads current job state from Redis, applies `on_timeout` logic, and advances the state machine.

### 5.2 Version Manager Service

Manages semantic versioning for every static component and template.

**Version Record:**

```json
{
  "component_id": "p_act_bus",
  "awa_consumer_id": "IDP_INVOICE_US",
  "version": "2.1.0",
  "previous_version": "2.0.3",
  "change_type": "minor",
  "changed_by": "user@company.com",
  "change_reason": "Added invoice date extraction variant",
  "snapshot_ref": "s3://awa-versions/p_act_bus/IDP_INVOICE_US/2.1.0.json",
  "created_at": "2026-05-20T10:00:00Z",
  "is_active": true
}
```

**Rollback Mechanics:**
- Each version is a full snapshot (not a diff) stored in object storage
- Rollback sets `is_active = false` on current, promotes target version
- Publishes `VersionRolledBack` event so Prompt Builder reloads its cache
- Rollback is audited and cannot be undone silently — requires confirmation token

**Consumer Version Pin:**

A consumer can pin each component to a specific version independently:

```json
{
  "awa_consumer_id": "IDP_INVOICE_US",
  "version_pins": {
    "p_guard":    "1.3.2",
    "p_act_bus":  "2.1.0",
    "p_act_ins":  "LATEST",
    "p_act_cond": "1.0.0",
    "p_template": "3.0.1"
  }
}
```

### 5.3 Context Observer Service

Monitors assembled prompts for quality signals before the LLM call.

#### 5.3.1 Context Bloat Detection

| Signal | Definition | Threshold (configurable) |
|---|---|---|
| Total token count | Sum of all section tokens | > 80% of model context window |
| Single-section dominance | One section > 60% of total tokens | configurable |
| RAG source overflow | n sources > budget | configurable per consumer |

#### 5.3.2 Context Rot Detection

| Signal | Definition | Method |
|---|---|---|
| Staleness | Static section not updated in N days | Timestamp check |
| Semantic drift | Section content relevance vs. p_uq | Embedding cosine similarity < threshold |
| Dead references | Section references entities no longer in system | Entity resolution check |

#### 5.3.3 Observer Actions

```
WARN  → Log alert, attach observation report to PromptBuilt event
TRIM  → Invoke configured trim strategy (drop lowest-scored RAG sources first)
BLOCK → Halt prompt build, publish PromptBuildBlocked event
```

**Observation Report (attached to every job):**

```json
{
  "awa_job_id": "job_abc123",
  "total_tokens": 3812,
  "model_context_limit": 8192,
  "utilization_pct": 46.5,
  "section_breakdown": {
    "p_guard": 210,
    "p_act_bus": 412,
    "p_rag_context_n": 1620,
    "p_uq": 88
  },
  "alerts": [],
  "rot_signals": [
    { "component": "p_act_bus", "days_since_update": 45, "severity": "WARN" }
  ]
}
```

### 5.4 LLM Adapter Registry

Each adapter implements the `ILLMAdapter` interface and transforms the canonical section map into the target model's native prompt format.

**Interface:**

```python
class ILLMAdapter(ABC):
    @abstractmethod
    def format(self, sections: SectionMap, template: PromptTemplate) -> LLMPayload:
        ...

    @abstractmethod
    def estimate_tokens(self, content: str) -> int:
        ...

    @property
    @abstractmethod
    def model_family(self) -> str:
        ...

    @property
    @abstractmethod
    def context_window(self) -> int:
        ...
```

**Registered Adapters:**

| Adapter | Model Family | Format Standard |
|---|---|---|
| `OpenAIAdapter` | GPT-4o, GPT-4, GPT-3.5 | `system` / `user` / `assistant` message array |
| `AnthropicAdapter` | Claude 3.x / 4.x | `system` param + `user`/`assistant` turns, XML tags |
| `GeminiAdapter` | Gemini 1.5 / 2.x | `contents[]` with `role` and `parts` |
| `LlamaAdapter` | Llama 3.x | Special tokens: `<|system|>`, `<|user|>`, `<|assistant|>` |
| `MistralAdapter` | Mistral / Mixtral | `[INST]` / `[/INST]` instruction format |
| `BedrockAdapter` | AWS Bedrock unified | Bedrock Converse API format |

**Adapter Example — OpenAI:**

```json
[
  { "role": "system", "content": "<p_guard>\n...\n</p_guard>\n\n<p_agent_rgb>\n...\n</p_agent_rgb>\n\n<p_act_bus>\n...\n</p_act_bus>" },
  { "role": "user",   "content": "<p_rag_context>\n...\n</p_rag_context>\n\n<p_uq>\n...\n</p_uq>" }
]
```

**Adapter Example — Anthropic:**

```json
{
  "system": "<p_guard>...</p_guard>\n<p_agent_rgb>...</p_agent_rgb>\n<p_act_bus>...</p_act_bus>",
  "messages": [
    { "role": "user", "content": "<documents>...</documents>\n<p_uq>...</p_uq>" }
  ]
}
```

---

## 6. p_template Design

The template defines the ordered assembly of sections and is stored per `AWA_Consumer_ID`. It is versioned independently.

```json
{
  "template_id": "tmpl_idp_invoice_v3",
  "awa_consumer_id": "IDP_INVOICE_US",
  "version": "3.0.1",
  "slots": [
    { "order": 1,  "component": "p_guard",          "enabled": true,  "placement": "system",  "required": true  },
    { "order": 2,  "component": "p_agent_rgb",       "enabled": true,  "placement": "system",  "required": false },
    { "order": 3,  "component": "p_agent_conduct",   "enabled": true,  "placement": "system",  "required": false },
    { "order": 4,  "component": "p_act_bus",         "enabled": true,  "placement": "system",  "required": true  },
    { "order": 5,  "component": "p_act_ins",         "enabled": true,  "placement": "system",  "required": false },
    { "order": 6,  "component": "p_act_cond",        "enabled": true,  "placement": "system",  "required": false },
    { "order": 7,  "component": "p_rag_context_n",   "enabled": true,  "placement": "user",    "required": false },
    { "order": 8,  "component": "p_agent_context",   "enabled": false, "placement": "user",    "required": false },
    { "order": 9,  "component": "p_uq",              "enabled": true,  "placement": "user",    "required": true  }
  ],
  "token_budget": {
    "system_max": 4096,
    "user_max": 4096,
    "rag_context_max_per_source": 1024,
    "rag_source_limit": 5
  }
}
```

`placement` maps to adapter-specific role positions (system vs user message). The adapter uses this to know which message envelope each section lands in.

**Interaction with `DynamicDependencyConfig`:** A slot in the template is silently skipped at assembly time if the corresponding component's `wait_strategy` is `NOT_APPLICABLE`, regardless of whether `enabled` is `true` in the slot. This means the template can be written once for the broadest use case and the dependency config controls which dynamic slots are actually active per consumer — no need to maintain separate templates per consumer solely because of RAG/agent-context variations.

---

## 7. Event Architecture

### 7.1 Kafka Topics

| Topic | Publisher | Subscribers | Purpose |
|---|---|---|---|
| `awa.prompt.build.requested` | API Gateway | Prompt Builder | Trigger prompt assembly |
| `awa.prompt.rag.retrieved` | RAG Pipeline | Prompt Builder | Inject RAG context into waiting job |
| `awa.prompt.agent.context.ready` | Agent Orchestrator / CoT Engine | Prompt Builder | Inject agent reflection context into waiting job |
| `awa.prompt.dynamic.timeout` | Timer Service | Prompt Builder | Fire when a dynamic component deadline is exceeded |
| `awa.prompt.built` | Prompt Builder | LLM Caller, Context Observer, Audit | Final assembled prompt |
| `awa.prompt.build.failed` | Prompt Builder | Alert Service, Caller | Build failed (timeout FAIL or assembly error) |
| `awa.prompt.build.blocked` | Context Observer | Alert Service, Caller | Build halted by context observer |
| `awa.prompt.context.alert` | Context Observer | Alert Service, Dashboard | Bloat/rot warnings |
| `awa.version.changed` | Version Manager | Prompt Builder (cache invalidate) | Config changed |
| `awa.version.rolledback` | Version Manager | Prompt Builder, Audit | Rollback applied |
| `awa.section.flag.changed` | Control Plane API | Prompt Builder | Enable/disable section |
| `awa.dynamic.config.changed` | Control Plane API | Prompt Builder (cache invalidate) | Dynamic dependency config updated |

### 7.2 Core Event Schemas

**PromptBuildRequested:**

```json
{
  "event_type": "PromptBuildRequested",
  "awa_consumer_id": "IDP_INVOICE_US",
  "awa_job_id": "job_abc123",
  "llm_model": "gpt-4o",
  "p_uq": "Extract all line items from the attached invoice.",
  "overrides": {
    "p_rag_context_n": { "wait_strategy": "OPTIONAL", "timeout_ms": 3000 },
    "p_agent_context": { "wait_strategy": "NOT_APPLICABLE" }
  },
  "timestamp": "2026-05-26T08:00:00Z"
}
```

> `overrides` is optional. When absent, the consumer-level `DynamicDependencyConfig` applies. Job-level overrides can change `wait_strategy`, `timeout_ms`, and `on_timeout` for this call only.

**RAGContextRetrieved** (published by RAG Pipeline, consumed by Prompt Builder):

```json
{
  "event_type": "RAGContextRetrieved",
  "awa_job_id": "job_abc123",
  "sources": [
    { "source_id": "kb_product_manual", "content": "...", "score": 0.91, "token_count": 620 },
    { "source_id": "ocr_page_3",        "content": "...", "score": 0.87, "token_count": 810 }
  ],
  "timestamp": "2026-05-26T08:00:02.800Z"
}
```

**AgentContextReady** (published by CoT/ToT engine, consumed by Prompt Builder):

```json
{
  "event_type": "AgentContextReady",
  "awa_job_id": "job_abc123",
  "context_type": "chain_of_thought",
  "content": "...",
  "token_count": 340,
  "timestamp": "2026-05-26T08:00:01.200Z"
}
```

**DynamicComponentTimeout** (published by Timer Service):

```json
{
  "event_type": "DynamicComponentTimeout",
  "awa_job_id": "job_abc123",
  "component_id": "p_rag_context_n",
  "timestamp": "2026-05-26T08:00:05.000Z"
}
```

**PromptBuilt:**

```json
{
  "event_type": "PromptBuilt",
  "awa_consumer_id": "IDP_INVOICE_US",
  "awa_job_id": "job_abc123",
  "llm_model": "gpt-4o",
  "formatted_payload": { "...adapter output..." },
  "section_manifest": {
    "p_guard":         { "version": "1.3.2", "enabled": true,  "tokens": 210, "received": true  },
    "p_act_bus":       { "version": "2.1.0", "enabled": true,  "tokens": 412, "received": true  },
    "p_rag_context_n": { "sources": 2,        "enabled": true,  "tokens": 1620,"received": true  },
    "p_agent_context": { "wait_strategy": "NOT_APPLICABLE",    "tokens": 0,   "received": false },
    "p_uq":            {                       "enabled": true,  "tokens": 88,  "received": true  }
  },
  "observation_report_ref": "obs_job_abc123",
  "build_duration_ms": 42,
  "timestamp": "2026-05-26T08:00:02.842Z"
}
```

**PromptBuildFailed:**

```json
{
  "event_type": "PromptBuildFailed",
  "awa_consumer_id": "IDP_INVOICE_US",
  "awa_job_id": "job_abc123",
  "reason": "DYNAMIC_COMPONENT_TIMEOUT",
  "failed_component": "p_rag_context_n",
  "on_timeout_applied": "FAIL",
  "timestamp": "2026-05-26T08:00:05.001Z"
}
```

---

### 7.3 Event Flows

#### Flow A — No dynamic components applicable (pure static build)

```
Caller
  │──[PromptBuildRequested]──────────────────────────────► Prompt Builder
                                                                 │
                                                   Load consumer DynamicDependencyConfig
                                                   p_rag_context_n: NOT_APPLICABLE
                                                   p_agent_context: NOT_APPLICABLE
                                                   Checklist empty → READY_TO_ASSEMBLE
                                                                 │
                                                   Load static sections + p_uq
                                                                 │
                                                   ┌────[SectionsAssembled]──► Context Observer
                                                   │                                │
                                                   │                    [ContextAlert?]──► Alert Service
                                                   └───────────── (observation_report)
                                                                 │
  ◄──────────────────────────────────────────────[PromptBuilt]───┘
```

#### Flow B — RAG required, agent context not applicable (happy path)

```
Caller ──[PromptBuildRequested]──────────────────────────────► Prompt Builder
                                                                      │
                                                      p_rag_context_n: REQUIRED (5000ms)
                                                      p_agent_context: NOT_APPLICABLE
                                                      Start RAG timeout timer → Timer Service
                                                      State: AWAITING_DYNAMIC
                                                                      │
RAG Pipeline ──[RAGContextRetrieved, t+2.8s]──────────────────► Prompt Builder
                                                                      │
                                                      Mark p_rag_context_n ✓ in Redis
                                                      Checklist complete → READY_TO_ASSEMBLE
                                                                      │
                                                      Load static sections + merge RAG + p_uq
                                                                      │
                                                      ┌────[SectionsAssembled]──► Context Observer
                                                      │                                │
                                                      └───────────── (observation_report)
                                                                      │
Caller  ◄────────────────────────────────────────[PromptBuilt]────────┘
```

#### Flow C — RAG required, agent context optional, both arrive

```
Caller ──[PromptBuildRequested]──────────────────────────────► Prompt Builder
                                                                      │
                                                      p_rag_context_n: REQUIRED (5000ms)
                                                      p_agent_context: OPTIONAL (3000ms)
                                                      Start both timers → Timer Service
                                                      State: AWAITING_DYNAMIC (2 items pending)
                                                                      │
CoT Engine  ──[AgentContextReady, t+1.2s]─────────────────────► Prompt Builder
                                                      Mark p_agent_context ✓ in Redis (1 remaining)
                                                                      │
RAG Pipeline ──[RAGContextRetrieved, t+2.8s]──────────────────► Prompt Builder
                                                      Mark p_rag_context_n ✓ in Redis
                                                      Checklist complete → READY_TO_ASSEMBLE
                                                                      │
                                                      Merge all sections → format → observe
                                                                      │
Caller  ◄─────────────────────────────────────────[PromptBuilt]───────┘
```

#### Flow D — RAG required, timeout with PROCEED_WITHOUT

```
Caller ──[PromptBuildRequested]──────────────────────────────► Prompt Builder
                                                                      │
                                                      p_rag_context_n: REQUIRED, on_timeout: PROCEED_WITHOUT
                                                      Start timer (5000ms) → Timer Service
                                                      State: AWAITING_DYNAMIC
                                                                      │
                                                      (RAG pipeline delayed or unavailable)
                                                                      │
Timer Service ──[DynamicComponentTimeout, t+5s]───────────────► Prompt Builder
                                                      p_rag_context_n: timed_out, on_timeout → PROCEED_WITHOUT
                                                      Checklist resolved → READY_TO_ASSEMBLE
                                                                      │
                                                      Assemble without p_rag_context_n
                                                      section_manifest: { received: false, reason: "TIMEOUT" }
                                                      Observation report: WARN signal
                                                                      │
Caller  ◄─────────────────────────────────────────[PromptBuilt]───────┘  (with timeout warning)
```

#### Flow E — RAG required, timeout with FAIL

```
Caller ──[PromptBuildRequested]──────────────────────────────► Prompt Builder
                                                                      │
                                                      p_rag_context_n: REQUIRED, on_timeout: FAIL
                                                      Start timer (5000ms) → Timer Service
                                                      State: AWAITING_DYNAMIC
                                                                      │
Timer Service ──[DynamicComponentTimeout, t+5s]───────────────► Prompt Builder
                                                      p_rag_context_n: timed_out, on_timeout → FAIL
                                                      State: FAILED
                                                                      │
Caller  ◄───────────────────────────────────[PromptBuildFailed]───────┘
Alert Service ◄──────────────────────────────────────────────────────┘
```

---

## 8. Control Plane API

RESTful API for runtime management of the framework.

### 8.1 Section Flag Control

```
PATCH /api/v1/consumers/{awa_consumer_id}/sections/{component_id}/flag
Body: { "enabled": false, "reason": "Temporarily disabling CoT context", "changed_by": "user@co.com" }
```

Changes take effect on the next `PromptBuildRequested` event. Publishes `SectionFlagChanged` to Kafka for cache invalidation.

### 8.2 Dynamic Dependency Config

```
GET    /api/v1/consumers/{awa_consumer_id}/dynamic-dependency-config
       Returns current config + effective wait_strategy per component

PUT    /api/v1/consumers/{awa_consumer_id}/dynamic-dependency-config
       Body: { full DynamicDependencyConfig object }
       Replaces config; publishes DynamicConfigChanged for cache invalidation

PATCH  /api/v1/consumers/{awa_consumer_id}/dynamic-dependency-config/{component_id}
       Body: { "wait_strategy": "OPTIONAL", "timeout_ms": 3000, "on_timeout": null }
       Updates one component's settings only

GET    /api/v1/consumers/{awa_consumer_id}/dynamic-dependency-config/history
       Returns version history of config changes with actor, timestamp, reason
```

### 8.3 Version Management

```
GET    /api/v1/consumers/{awa_consumer_id}/sections/{component_id}/versions
POST   /api/v1/consumers/{awa_consumer_id}/sections/{component_id}/versions
       Body: { "content": "...", "change_reason": "..." }

GET    /api/v1/consumers/{awa_consumer_id}/sections/{component_id}/versions/{version}
POST   /api/v1/consumers/{awa_consumer_id}/sections/{component_id}/rollback
       Body: { "target_version": "2.0.3", "reason": "...", "confirmation_token": "..." }
```

### 8.4 Template Management

```
GET    /api/v1/consumers/{awa_consumer_id}/templates
POST   /api/v1/consumers/{awa_consumer_id}/templates
PUT    /api/v1/consumers/{awa_consumer_id}/templates/{template_id}
POST   /api/v1/consumers/{awa_consumer_id}/templates/rollback
```

### 8.5 Observation & Monitoring

```
GET  /api/v1/jobs/{awa_job_id}/observation-report
GET  /api/v1/jobs/{awa_job_id}/state                         ← new: live job state machine status
GET  /api/v1/consumers/{awa_consumer_id}/context-metrics?from=...&to=...
GET  /api/v1/consumers/{awa_consumer_id}/rot-alerts
GET  /api/v1/consumers/{awa_consumer_id}/bloat-alerts
GET  /api/v1/consumers/{awa_consumer_id}/timeout-stats        ← new: timeout rate per dynamic component
```

---

## 9. Data Model (Simplified ER)

```
AWA_Consumer
 ├── consumer_id (PK)
 ├── name
 ├── default_llm_model
 └── version_pins (JSON)

DynamicDependencyConfig                          ← NEW
 ├── config_id (PK)
 ├── consumer_id (FK → AWA_Consumer)
 ├── version
 ├── p_rag_context_n_applicable    (bool)
 ├── p_rag_context_n_wait_strategy (NOT_APPLICABLE | OPTIONAL | REQUIRED)
 ├── p_rag_context_n_timeout_ms    (int, nullable)
 ├── p_rag_context_n_on_timeout    (PROCEED_WITHOUT | FAIL, nullable)
 ├── p_rag_context_n_min_sources   (int, nullable)
 ├── p_agent_context_applicable    (bool)
 ├── p_agent_context_wait_strategy (NOT_APPLICABLE | OPTIONAL | REQUIRED)
 ├── p_agent_context_timeout_ms    (int, nullable)
 ├── p_agent_context_on_timeout    (PROCEED_WITHOUT | FAIL, nullable)
 ├── is_active                     (bool)
 ├── changed_by
 └── created_at

PromptSection
 ├── section_id (PK)
 ├── component_id  (p_guard | p_act_bus | ...)
 ├── consumer_id (FK → AWA_Consumer)
 ├── version
 ├── content
 ├── enabled
 ├── token_count
 └── checksum

PromptTemplate
 ├── template_id (PK)
 ├── consumer_id (FK → AWA_Consumer)
 ├── version
 └── slots (JSON array)

AWA_Job
 ├── job_id (PK)
 ├── consumer_id (FK → AWA_Consumer)
 ├── llm_model
 ├── p_uq
 ├── final_state  (BUILT | FAILED | BLOCKED)     ← NEW
 ├── section_manifest (JSON)
 ├── built_prompt_ref (S3)
 └── created_at

JobDynamicChecklist  [persisted in Redis; mirrored to Postgres on terminal state] ← NEW
 ├── job_id (PK, FK → AWA_Job)
 ├── state  (INITIATED | AWAITING_DYNAMIC | READY_TO_ASSEMBLE | ASSEMBLING | BUILT | FAILED)
 ├── checklist (JSON — per-component received/timed_out/deadline)
 └── resolved_at

ObservationReport
 ├── report_id (PK)
 ├── job_id (FK → AWA_Job)
 ├── total_tokens
 ├── utilization_pct
 ├── section_breakdown (JSON)
 ├── alerts (JSON)
 ├── rot_signals (JSON)
 └── dynamic_timeout_warnings (JSON)              ← NEW

VersionHistory
 ├── history_id (PK)
 ├── component_id
 ├── consumer_id
 ├── version
 ├── snapshot_ref
 ├── changed_by
 └── created_at
```

---

## 10. Feature Flag & Override Precedence

```
Priority (highest → lowest):

1. Job-level override   (in PromptBuildRequested.overrides)
2. Consumer-level flag  (in AWA_Consumer config / DynamicDependencyConfig)
3. Global default       (system-wide fallback)
```

This enables per-call experimentation without changing stored configuration.

**Dynamic dependency overrides follow the same precedence.** A caller can override `wait_strategy`, `timeout_ms`, and `on_timeout` per job without touching the consumer's stored config. Example: a batch processing job may send `wait_strategy: NOT_APPLICABLE` for `p_rag_context_n` even though the consumer config has it as `REQUIRED`, bypassing the wait entirely for that call.

**What cannot be overridden at job level:**
- `p_guard` cannot be disabled (enforced by template schema validation)
- `min_sources_required` can only be relaxed (lowered), not tightened, at job level — to prevent callers from weakening guardrails silently

---

## 11. Deployment Architecture

```
┌─────────────────── Kubernetes Cluster ────────────────────┐
│                                                             │
│  ┌─────────────────┐   ┌─────────────────┐                │
│  │  prompt-builder  │   │ version-manager  │               │
│  │  (3 replicas)    │   │  (2 replicas)    │               │
│  └─────────────────┘   └─────────────────┘                │
│                                                             │
│  ┌─────────────────┐   ┌─────────────────┐                │
│  │ context-observer │   │  llm-adapter    │               │
│  │  (2 replicas)    │   │  (2 replicas)   │               │
│  └─────────────────┘   └─────────────────┘                │
│                                                             │
│  ┌──────────────────────────────────────────┐             │
│  │         Apache Kafka (3-broker cluster)   │             │
│  └──────────────────────────────────────────┘             │
│                                                             │
│  ┌───────────────┐  ┌───────────────┐                     │
│  │  PostgreSQL   │  │  S3-compatible│                     │
│  │  (Primary +   │  │  Object Store │                     │
│  │   Replica)    │  │  (snapshots)  │                     │
│  └───────────────┘  └───────────────┘                     │
└─────────────────────────────────────────────────────────────┘
```

**Each service exposes:**
- `/health` — liveness probe
- `/ready` — readiness probe
- `/metrics` — Prometheus metrics endpoint

---

## 12. Observability

| Signal | Tool | Key Metrics |
|---|---|---|
| Traces | OpenTelemetry + Jaeger | Prompt build latency, section load time per component, dynamic wait duration per component |
| Metrics | Prometheus + Grafana | Token utilization %, bloat alert rate, rot alert rate, rollback count, dynamic component timeout rate (by consumer + component), PROCEED_WITHOUT rate, FAIL rate |
| Logs | Structured JSON → ELK | Per-job section manifest, version in use, flag states, job state transitions, checklist resolution reason |
| Alerts | Alertmanager | Bloat > 90%, rot age > 60 days, build failure rate > 1%, RAG timeout rate > 5% (per consumer) |

---

## 13. Security Considerations

1. **Prompt injection guard**: `p_guard` is always loaded first and cannot be disabled (marked `required: true` in template schema validation)
2. **Section content signing**: Each section stores a SHA-256 checksum; Prompt Builder validates before assembly
3. **Secrets exclusion**: Section content is never logged in full; only `section_id`, `version`, and `token_count` appear in structured logs
4. **API auth**: All Control Plane API endpoints require JWT with RBAC claims (`section:write`, `version:rollback`, `flag:write`)
5. **Rollback confirmation token**: Destructive rollbacks require a short-lived TOTP-style confirmation token issued by a separate endpoint
6. **Audit log**: All flag changes, version changes, and rollbacks are written to an append-only audit table with actor identity

---

## 14. Phased Delivery Roadmap

| Phase | Scope | Outcome |
|---|---|---|
| **P1 — Core** | Prompt Builder Service, static section loading, p_template, OpenAI + Anthropic adapters | End-to-end prompt assembly working |
| **P2 — Dynamic** | RAG context integration, p_agent_context injection, DynamicDependencyConfig, job state machine, timeout handling, job-level overrides | Full dynamic prompt construction with configurable wait behaviour |
| **P3 — Versioning** | Version Manager Service, rollback API, snapshot store | Safe config change management |
| **P4 — Observation** | Context Observer Service, bloat/rot detection, trim strategies | Prompt quality monitoring live |
| **P5 — Expand** | Remaining LLM adapters (Gemini, Llama, Mistral, Bedrock), A/B template testing | Multi-model production readiness |

---

*Design version: 1.1.0 — 2026-05-26*
