# Architecture

## Core Concepts

### Runtime
An AI coding tool that can be invoked as a subprocess. ACP supports four native runtimes (Claude Code, Codex CLI, Gemini CLI, Copilot CLI) and one synthetic runtime — **Claudex** — which runs the Claude Code binary with `ANTHROPIC_BASE_URL`, `ANTHROPIC_AUTH_TOKEN`, and `ANTHROPIC_MODEL` overridden to point at any Anthropic-compatible API endpoint.

### Role Slot
A named position in a workflow (e.g. `architect`, `backend`, `reviewer`). Slots are persistent: if the assigned runtime hits a rate limit or crashes, ACP hot-swaps to the next preferred candidate for that role and continues.

### Pipeline
A single execution of a workflow. A pipeline holds the YAML definition, its current status, all slot lifecycle events, step results, and the event log.

### Capability Score
A learned weight per `(runtime, model, capability)` triple. The scheduler uses these to rank assignment candidates and updates them after each step based on health outcomes. Scores older than 7 days are time-decayed.

### Semantic Memory
A per-pipeline in-process TF cosine-similarity index. After each step, the first 500 characters of agent stdout are indexed. Before the next step, ACP queries the index for relevant prior context and injects the top snippets into the agent's prompt via `HandoffContext.semantic_hints`.

### Adaptive Controller
Monitors pipeline failure rate. When `profile == QualityFirst`, `step_count >= 4`, and `failure_rate > 30%`, it automatically switches to `SpeedFirst` to prefer faster, more available runtimes.

---

## Crate Map

```
acp-cli
  ├── tui/home.rs          Interactive home screen (ratatui)
  ├── tui/draw.rs          Live dashboard renderer
  ├── tui/state.rs         Dashboard state struct
  ├── commands/pipeline.rs pipeline run / list / status
  ├── commands/analytics.rs analytics pipeline
  ├── commands/discover.rs doctor / models / provider
  ├── commands/slot.rs     slot list / assign / vacate
  ├── commands/skill.rs    skill list / show
  ├── commands/mcp.rs      mcp list / start / stop
  ├── commands/memory.rs   memory search
  ├── commands/runtime.rs  runtime interrupt / shutdown
  └── commands/workspace.rs workspace status

acp-hub
  ├── handlers/            Axum route handlers
  ├── db.rs                SQLite query functions
  └── migrations.rs        Schema versioning (4 migrations)

acp-orchestrator
  ├── pipeline.rs          Workflow execution loop
  ├── scheduler.rs         Capability-scored assignment with time decay
  ├── adaptive.rs          Failure-rate-triggered profile switching
  ├── semantic.rs          TF cosine-similarity memory index
  ├── memory.rs            HandoffContext + build_task()
  ├── recovery.rs          Subprocess execution + latency tracking
  ├── slots.rs             Slot lifecycle events and fallback logic
  └── conflict.rs          Merge conflict detection

acp-discover
  ├── lib.rs               Runtime discovery (which/version checks)
  ├── providers.rs         API provider registry (~/.acp/providers.json)
  ├── skills.rs            Skill loading (~/.acp/skills/)
  └── health.rs / subscription.rs

acp-protocol              Shared serde types (no logic)
acp-runtime               Subprocess adapter (spawns runtime binaries)
acp-workspace             Git worktree management for parallel agents
agent-client              Typed async HTTP client for the hub API
```

---

## Orchestration Flow

```
acp pipeline run workflow.yaml --approve-assignments --execute
│
├─ parse_workflow()          validates YAML, deserialises WorkflowConfig
├─ scheduler.candidates()   scores runtimes against each role slot
├─ user approves assignments
│
└─ run_local_pipeline_with_events()
   │
   ├─ MemoryIndex::new()         in-process semantic index
   ├─ AdaptiveController::new()  failure-rate monitor
   │
   └─ for step in workflow.steps:
      │
      ├─ WorkflowStep::Conditional  skip if last_health doesn't match
      ├─ WorkflowStep::Parallel     spawn all actions concurrently
      └─ WorkflowStep::Action
         │
         ├─ inject_semantic_context()  query memory → HandoffContext.semantic_hints
         ├─ build_task()               construct prompt with context + hints
         ├─ Instant::now()
         ├─ adapter.spawn()            run runtime subprocess
         ├─ latency_ms = elapsed
         │
         ├─ memory.add(step, stdout)   index output for future steps
         ├─ controller.record_step()   maybe switch profile
         └─ emit OrchestratorEvent::Step → persist to hub via API
```

---

## Scheduler Scoring

Each `(runtime, model)` candidate for a role gets a score:

```
final_score = base_score + learned_delta * time_decay + profile_boost
```

- **base_score**: 0.5 for capability match, 0.0 otherwise
- **learned_delta**: accumulated from capability score records; halved for each 7-day age window
- **profile_boost**: `+0.2` when model tier matches the active `SchedulerProfile` (quality-first prefers Flagship, speed-first prefers Standard)

The `analytics pipeline` command and the `SchedulerInsights` struct expose the full breakdown per assignment.

---

## Data Persistence

SQLite database (`acp-hub.db`) with 4 schema migrations:

| Migration | Tables added |
|---|---|
| 1 | `pipelines`, `pipeline_events`, `pipeline_slots`, `models`, `capability_scores` |
| 2 | `scheduler_decisions`, `context_compressions`, `semantic_memory_entries`, `pipeline_artifacts`, `mcp_servers`, `mcp_health`, `runtime_commands`, `working_contexts` |
| 3 | `file_claims`, `findings` |
| 4 | `step_metrics`, `last_updated_at` column on `capability_scores` |
