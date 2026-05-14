# ACP v2 — Implementation Plan

## For AI Agents Reading This

This document describes the **next major evolution** of ACP. Read `README.md` first to understand the current state (a coordination hub with messaging, tasks, file claims, findings). This plan describes transforming ACP into a **recursive orchestration operating system for AI coding runtimes**.

---

## 1. What ACP Is Becoming

### Current State (README.md)

ACP is a local-first coordination hub: an Axum HTTP/SSE server + CLI that lets multiple AI coding agents exchange messages, track tasks, claim files, and share findings. It works, but agents must be manually started and coordinated.

### Target State

ACP becomes an **AI Runtime Operating System** that:

- **Discovers** all AI coding tools on the user's machine (Claude Code, Codex CLI, Gemini CLI, GitHub Copilot)
- **Discovers** all API key providers (OpenRouter, DeepSeek, Z.ai, Ollama, etc.)
- **Presents** a unified model registry showing every available model and its source
- **Orchestrates** multi-agent workflows where heavy models (Claude Opus, GPT-5.5) handle architecture/review and cheap/free models (Qwen3-Coder, DeepSeek) handle implementation
- **Manages** plug-in role slots that survive runtime failures (rate limits, crashes) via hot-swapping
- **Provides** shared memory so a replacement agent continues where the previous one left off
- **Isolates** workspaces via git worktrees to prevent file conflicts between agents
- **Displays** everything in a live ratatui terminal dashboard

### Core Philosophy

```
The real value is not the model. The real value is the runtime.
```

Claude Code proved that tool orchestration, planning loops, patch semantics, and workspace awareness matter more than raw model quality. ACP leverages existing runtime ecosystems instead of rebuilding them.

### Key Architectural Decision

**ACP does NOT write a new agent runtime from scratch.**

ACP composes existing runtimes recursively:

```
ACP Orchestrator (meta-runtime)
├── Claude Code     → subprocess (subscription)
├── Codex CLI       → subprocess (subscription)
├── Gemini CLI      → subprocess (subscription)
├── Copilot CLI     → subprocess (subscription)
└── Claudex Instance → Claude Code binary + env override (API key models)
    ├── Qwen3-Coder     (via OpenRouter)
    ├── DeepSeek-v4     (via DeepSeek API)
    ├── GLM-4.7-Flash   (via Z.ai)
    └── Any local model (via Ollama)
```

For API-key-based models, ACP spawns a **Claudex instance** — the Claude Code binary with `ANTHROPIC_BASE_URL`, `ANTHROPIC_AUTH_TOKEN`, and `ANTHROPIC_MODEL` environment variables overridden. This turns any Anthropic-compatible API model into a full coding agent with tools, planning, MCP support, and workspace awareness — with zero custom runtime engineering.

**Claudex reference project:**: https://github.com/sasdsamatt123/claudex

---

## 2. Core Concepts

### Model
Just an inference endpoint. Knows nothing about tools, git, or planning.
Examples: GPT-5.5, Claude Opus 4.7, Qwen3-Coder, DeepSeek-v4

### Runtime
Agent execution environment. Provides tools, patch system, planning loop, MCP, autonomous execution.
Examples: Claude Code, Codex CLI, Gemini CLI, Claudex instance

### Agent
A live runtime instance assigned to a role.
Example: Agent #42, Runtime: Claudex, Model: Qwen3-Coder, Role: Backend Developer

### Role Slot
A persistent orchestration position. Survives runtime failure.
Example: "Architect" slot — if Claude hits rate limit, GPT-5.5 takes over, slot continues.

### Capability
Hierarchical skill tag. `rust/tokio/axum` satisfies requirements for `rust` and `rust/tokio` but not vice versa.

### Workflow
A YAML-defined task orchestration graph with slots, steps, dependencies, and failure policies.

---

## 3. System Architecture

```
┌─────────────────────────────────────────┐
│              ACP CLI + TUI              │
│         (ratatui dashboard)             │
└────────────────┬────────────────────────┘
                 │
┌────────────────▼────────────────────────┐
│          ACP Orchestrator               │
│  Scheduler · Pipeline · Slots · Memory  │
└────────────────┬────────────────────────┘
                 │
  ┌──────────────┼──────────────┐
  ▼              ▼              ▼
External      Claudex        Workspace
Runtime       Instances       Engine
Adapters      (API models)   (git worktrees)
  │              │
  ├─Claude Code  ├─Qwen (OpenRouter)
  ├─Codex CLI    ├─DeepSeek (API)
  ├─Gemini CLI   ├─GLM (Z.ai)
  └─Copilot CLI  └─Local (Ollama)
                 │
┌────────────────▼────────────────────────┐
│           ACP Hub                       │
│  Event Bus · HTTP/SSE · SQLite · Memory │
└─────────────────────────────────────────┘
```

---

## 4. Crate Structure

```
crates/
├── acp-protocol/       # Shared types, traits, enums (EXISTING → EXTEND)
├── acp-hub/            # Event bus + HTTP/SSE + SQLite (EXISTING → EXTEND)
├── acp-discover/       # Runtime discovery + model registry (NEW)
├── acp-runtime/        # RuntimeAdapter trait + 5 adapters (NEW)
├── acp-workspace/      # Git worktree + file claims + merge (NEW)
├── acp-orchestrator/   # Scheduler + pipeline + slots + memory (NEW)
└── acp-cli/            # CLI commands + ratatui dashboard (NEW, replaces agentctl)
```

---

## 5. Crate Details

### 5.1 acp-protocol (EXTEND existing agent-protocol)

Add these types to the existing crate:

```rust
// Runtime adapter contract — ALL runtimes implement this
trait RuntimeAdapter {
    async fn spawn(&self, spec: AgentSpec) -> Result<AgentHandle>;
    async fn send_task(&self, task: Task) -> Result<TaskHandle>;
    fn stream_events(&self) -> EventStream;
    async fn interrupt(&self) -> Result<()>;
    async fn shutdown(&self) -> Result<()>;
    async fn health(&self) -> RuntimeHealth;
}

enum RuntimeType { ClaudeCode, Codex, Gemini, Copilot, Claudex }
enum RuntimeHealth { Healthy, Degraded, RateLimited, AuthExpired, Crashed }
enum SlotStatus { Empty, Assigned, Active, Working, Waiting, Vacant, Disabled }
enum SchedulerProfile { QualityFirst, BudgetFirst, SpeedFirst }

// Strongly typed events — NOT raw JSON
enum PipelineEvent {
    RuntimeSpawned { .. },
    TaskAssigned { .. },
    PatchApplied { .. },
    RateLimitHit { .. },
    AuthExpired { .. },
    RuntimeCrash { .. },
    MergeConflict { .. },
    ToolFailure { .. },
    ContextOverflow { .. },
    ValidationFailure { .. },
}

struct RoleSlot { id, role, runtime_type, model, status, capabilities }
struct ModelRecord { id, name, runtime_source, tier, context_window, pricing }
struct WorkflowConfig { .. } // parsed from YAML
```

Keep ALL existing types: Message, Task, Finding, FileClaim, Agent, etc.

### 5.2 acp-hub (EXTEND existing agent-hub)

**Modularize** `lib.rs` (currently 1588 lines) into `handlers/`, `db/`, `models/`.

**Add migration v3** with these tables:

```sql
CREATE TABLE models ( -- unified model registry
    id TEXT PRIMARY KEY, name TEXT, runtime_source TEXT,
    tier TEXT, context_window INTEGER, pricing_input REAL, pricing_output REAL
);
CREATE TABLE pipelines (
    id TEXT PRIMARY KEY, workflow_yaml TEXT, status TEXT, profile TEXT,
    created_at TEXT, completed_at TEXT
);
CREATE TABLE slots (
    id TEXT PRIMARY KEY, pipeline_id TEXT, role TEXT, runtime_type TEXT,
    model_id TEXT, agent_id TEXT, status TEXT DEFAULT 'empty'
);
CREATE TABLE pipeline_events ( -- immutable event log
    id INTEGER PRIMARY KEY AUTOINCREMENT, pipeline_id TEXT, agent_id TEXT,
    event_type TEXT, payload JSON, correlation_id TEXT, causation_id TEXT, created_at TEXT
);
CREATE TABLE artifacts (
    id TEXT PRIMARY KEY, pipeline_id TEXT, stage_name TEXT,
    artifact_type TEXT, content TEXT, created_by TEXT, created_at TEXT
);
CREATE TABLE working_context ( -- for runtime hot-swap handoff
    pipeline_id TEXT, role TEXT, summary TEXT, key_decisions JSON,
    active_files JSON, updated_at TEXT, PRIMARY KEY (pipeline_id, role)
);
CREATE TABLE capability_scores ( -- telemetry for learned scoring
    runtime_type TEXT, model_id TEXT, capability TEXT,
    success_count INTEGER DEFAULT 0, failure_count INTEGER DEFAULT 0,
    PRIMARY KEY (runtime_type, model_id, capability)
);
```

**New endpoints:** `/api/models`, `/api/pipelines`, `/api/pipelines/{id}/slots`, `/api/pipelines/{id}/events`, `/api/pipelines/{id}/artifacts`, `/api/memory/{pipeline_id}/{role}`

### 5.3 acp-discover (NEW)

```
src/
├── lib.rs
├── subscription.rs  # `which claude/codex/gemini/copilot` + version + auth check
├── provider.rs      # ~/.acp/providers/*.yaml + API key management
├── registry.rs      # Unified model registry builder
└── health.rs        # Doctor command: full health check
```

### 5.4 acp-runtime (NEW)

```
src/
├── lib.rs
├── adapter/
│   ├── mod.rs
│   ├── claude.rs    # claude -p ... --output-format stream-json --bare
│   ├── codex.rs     # codex exec ...
│   ├── gemini.rs    # gemini -p ...
│   ├── copilot.rs   # copilot -p ... --no-ask-user
│   └── claudex.rs   # Claude binary + ANTHROPIC_BASE_URL/AUTH_TOKEN/MODEL env override
├── process.rs       # Subprocess spawn, monitor, kill, timeout
├── output.rs        # stdout/stderr parsing, rate limit detection
└── error.rs         # RuntimeError types
```

**Critical: Claudex adapter (the core ~50 lines):**

```rust
impl RuntimeAdapter for ClaudexAdapter {
    async fn spawn(&self, spec: AgentSpec) -> Result<AgentHandle> {
        Command::new("claude")
            .env("ANTHROPIC_BASE_URL", &self.provider.base_url)
            .env("ANTHROPIC_AUTH_TOKEN", &self.provider.api_key)
            .env("ANTHROPIC_MODEL", &spec.model)
            .env("CLAUDE_CONFIG_DIR", &self.isolated_config_dir)
            .arg("-p").arg(&spec.task)
            .arg("--output-format").arg("stream-json")
            .arg("--allowedTools").arg(&spec.allowed_tools.join(","))
            .arg("--bare")
            .spawn_async()
    }
}
```

### 5.5 acp-workspace (NEW)

```
src/
├── lib.rs
├── worktree.rs    # git worktree add/remove, branch: acp/<role>/<task-id>
├── claims.rs      # File claim system (extends existing)
├── merge.rs       # Merge simulation + conflict detection + resolver agent
├── validation.rs  # Config-driven: compile → lint → test → merge sim
└── snapshot.rs    # Workspace rollback
```

### 5.6 acp-orchestrator (NEW)

```
src/
├── lib.rs
├── scheduler.rs   # Weighted scoring, semi-automatic mode, capability matching
├── pipeline.rs    # Workflow YAML parser + DAG execution (sequential + parallel)
├── slots.rs       # Slot lifecycle: empty → assigned → active → vacant → reassigned
├── memory.rs      # Event log + artifact store + context compression
└── recovery.rs    # Rate limit → vacate slot → fallback search → reassign
```

**Scheduler scoring:**
```
score = (capability_match × 0.30)
      + (runtime_quality × 0.25)
      + (cost_efficiency × 0.20)
      + (context_fit × 0.15)
      + (latency × 0.10)
```

**Scheduler mode:** Semi-automatic — initial slot assignments ask user approval, recovery/fallback is automatic.

### 5.7 acp-cli (NEW, replaces agentctl)

```
src/
├── main.rs
├── commands/
│   ├── discover.rs   # acp discover
│   ├── models.rs     # acp models [--tier free]
│   ├── runtimes.rs   # acp runtimes
│   ├── provider.rs   # acp provider add/list/validate
│   ├── pipeline.rs   # acp pipeline run/list/status
│   ├── slots.rs      # acp slot assign/vacate/list
│   ├── workspace.rs  # acp workspace status
│   ├── doctor.rs     # acp doctor
│   └── dashboard.rs  # acp dashboard
└── tui/              # ratatui panels: slots, events, health, pipeline graph
```

---

## 6. Workflow YAML Schema

```yaml
workflow:
  id: fullstack-feature
  name: "Full Stack Feature Development"
  profile: quality-first  # quality-first | budget-first | speed-first

  slots:
    architect:
      role: architect
      runtime_mode: external       # external | claudex | auto
      preferred:
        - runtime: claude-code
          model: claude-opus-4.7
        - runtime: codex
          model: gpt-5.5
      required_capabilities: [architecture, system-design]
      optional: false

    backend:
      role: backend-developer
      runtime_mode: claudex
      preferred:
        - runtime: claudex
          model: qwen3-coder
          provider: openrouter
        - runtime: claudex
          model: deepseek-v4-pro
          provider: deepseek
      required_capabilities: [rust]

    reviewer:
      role: code-reviewer
      runtime_mode: external
      preferred:
        - runtime: claude-code
          model: claude-opus-4.7
      optional: true

  steps:
    - architect.plan
    - parallel:
        - backend.implement
    - reviewer.audit

  failure:
    default: retry(3)
    overrides:
      architect: ask_user
      reviewer: skip

  timeouts:
    step_minutes: 30
    pipeline_minutes: 180
```

---

## 7. Key Mechanisms

### Rate Limit Recovery
1. Wrapper parses rate limit warning from CLI stdout/stderr
2. Sends `RateLimitHit` event to orchestrator
3. Orchestrator marks slot as `Vacant`
4. Scheduler searches for same model on different source, or next-best alternative
5. User is notified; in semi-auto mode, recovery is automatic

### Runtime Hot-Swap
1. Current agent's working context is saved (summary, decisions, active files)
2. New runtime is spawned with context injected into system prompt
3. Slot status changes from `Vacant` → `Assigned` → `Active`
4. New agent continues from where the old one left off

### Workspace Isolation
- Each agent gets its own `git worktree` at `.acp/worktrees/<agent-id>/`
- Branch naming: `acp/<role>/<task-id>`
- File claims prevent concurrent edits
- Merge pipeline: validation → tests → merge simulation → conflict detection → merge

### Shared Memory (Event-Sourced)
- **Event log:** Immutable append-only history (pipeline_events table)
- **Artifact store:** Plans, patches, reports produced by agents
- **Working context:** Compressed handoff data for runtime hot-swap
- Context compression uses the cheapest available model to summarize

### MCP System
- ACP manages MCP server lifecycle globally
- Read-only MCP servers (docs, DB queries): shared across agents
- Mutable MCP servers (git, filesystem): per-agent isolation
- Global registry: `~/.acp/mcp.json`

---

## 8. Configuration

```
~/.acp/
├── config.yaml              # Global settings, concurrency limits
├── providers/
│   ├── zai.yaml             # API key providers
│   ├── deepseek.yaml
│   └── openrouter.yaml
├── pipelines/
│   ├── full_dev.yaml        # Workflow templates
│   └── quick_fix.yaml
├── capabilities.yaml        # Model capability overrides
├── mcp.json                 # Shared MCP config
└── skills/                  # Skill definitions (prompt templates)
```

Subscription runtimes (Claude Code, Codex, Gemini, Copilot): ACP does NOT manage their auth. It only detects "installed and logged in?".

API key providers: stored in `~/.acp/providers/<name>.yaml`, key referenced via env variable.

---

## 9. Development Phases

### Phase 1 — MVP (~4-5 weeks)

Everything needed for a working product:

**Week 1:**
- [ ] `acp-protocol` — add all new types (RuntimeAdapter, Event, Slot, Workflow, etc.)
- [ ] `acp-hub` — modularize lib.rs, add migration v3, add new endpoints

**Week 2:**
- [ ] `acp-discover` — subscription detection + provider management + model registry
- [ ] `acp-runtime` — 5 adapters (claude, codex, gemini, copilot, claudex) + rate limit detection

**Week 3:**
- [ ] `acp-orchestrator` — scheduler + pipeline engine + slot management + shared memory
- [ ] `acp-workspace` — worktree creation + file claims + basic merge

**Week 4-5:**
- [ ] `acp-cli` — all commands + ratatui dashboard
- [ ] Template workflows (full_dev, quick_fix, review_only)
- [ ] Integration testing + provider config

### Phase 2 — Advanced Orchestration (~3 weeks)
- Learned capability scoring (adaptive scheduling)
- Recovery engine (auto-heal)
- Skill system + translation layer
- Advanced conflict resolution agent
- Observability (tracing, event replay)

### Phase 3 — Intelligence (~2 weeks)
- Self-optimizing scheduler
- Semantic memory (embeddings)
- Adaptive workflows
- Analytics dashboard

---

## 10. Prerequisites

User's machine needs:
- **Rust toolchain** (for building ACP)
- **Git** (for workspace isolation)
- **claude** binary (`npm install -g @anthropic-ai/claude-code`) — required for Claudex instances
- **Optional:** `codex`, `gemini`, `copilot` CLI tools (for subscription runtimes)
- **Optional:** API keys for Z.ai, DeepSeek, OpenRouter, etc.

---

## 11. Existing Codebase Reference

The current codebase (see `README.md`) has 4 crates totaling ~3,000 lines of Rust:

| Crate | Lines | Role in v2 |
|---|---|---|
| `agent-protocol` | 413 | → becomes `acp-protocol` (extended) |
| `agent-hub` | 1,588 | → becomes `acp-hub` (extended + modularized) |
| `agent-client` | 332 | → used by new crates |
| `agentctl` | 593 | → replaced by `acp-cli` |

All existing functionality (messaging, tasks, file claims, findings, SSE streaming) is preserved and extended.
